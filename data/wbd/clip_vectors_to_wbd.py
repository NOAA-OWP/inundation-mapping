#!/usr/bin/env python3

import argparse
import json
import logging
import os
import shutil
import sys

import geopandas as gpd
import pandas as pd
import rasterio as rio
from dotenv import load_dotenv
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import nearest_points


gpd.options.io_engine = "pyogrio"

srcDir = os.getenv('srcDir')
projectDir = os.getenv('projectDir')

load_dotenv(f'{srcDir}/bash_variables.env')
load_dotenv(f'{projectDir}/config/params_template.env')

output_filenames = {
    "nwm_lakes": "nwm_lakes_proj_subset.gpkg",
    "nwm_streams": "nwm_subset_streams.gpkg",
    "nwm_headwaters": "nwm_headwater_points_subset.gpkg",
    "wbd_streams_buffer": "wbd_buffered_streams.gpkg",
    "nwm_catchments": "nwm_catchments_proj_subset.gpkg",
    "levee_lines": "nld_subset_levees.gpkg",
    "levee_lines_burned": "3d_nld_subset_levees_burned.gpkg",
    "levee_protected_areas": "LeveeProtectedAreas_subset.gpkg",
    "osm_bridges": "osm_bridges_subset.gpkg",
    "osm_roads": "osm_roads_subset.gpkg",
}


def extend_outlet_streams(streams, wbd_buffered, wbd):
    """
    Extend outlet streams to nearest buffered WBD boundary
    """

    # Select only the streams that are outlets
    levelpath_outlets = streams[streams['to'] == 0]

    levelpath_outlets_columns = [x for x in levelpath_outlets.columns]

    # Select streams that intersect the WBD but not the WBD buffer
    levelpath_outlets = levelpath_outlets.sjoin(wbd)[levelpath_outlets_columns]

    wbd_boundary = wbd.copy()
    wbd_boundary['geometry'] = wbd_boundary.geometry.boundary
    wbd_boundary = gpd.GeoDataFrame(data=wbd_boundary, geometry='geometry')

    wbd_buffered["linegeom"] = wbd_buffered.geometry

    levelpath_outlets = levelpath_outlets[
        ~levelpath_outlets.intersects(wbd_buffered["linegeom"].boundary.iloc[0])
    ]

    levelpath_outlets['nearest_point'] = None
    levelpath_outlets['nearest_point_wbd'] = None
    levelpath_outlets['last'] = None

    levelpath_outlets = levelpath_outlets.explode(index_parts=False)

    for index, row in levelpath_outlets.iterrows():
        coords = [(coords) for coords in list(row['geometry'].coords)]
        last_coord = coords[-1]
        levelpath_outlets.at[index, 'last'] = Point(last_coord)

    wbd_buffered['geometry'] = wbd_buffered.geometry.boundary
    wbd_buffered = gpd.GeoDataFrame(data=wbd_buffered, geometry='geometry')

    errors = 0
    for index, row in levelpath_outlets.iterrows():
        levelpath_geom = row['last']
        nearest_point = nearest_points(levelpath_geom, wbd_buffered)
        nearest_point_wbd = nearest_points(levelpath_geom, wbd_boundary.geometry)

        levelpath_outlets.at[index, 'nearest_point'] = nearest_point[1]['geometry'].iloc[0]
        levelpath_outlets.at[index, 'nearest_point_wbd'] = nearest_point_wbd[1].iloc[0]

        levelpath_outlets_nearest_points = levelpath_outlets.at[index, 'nearest_point']
        levelpath_outlets_nearest_points_wbd = levelpath_outlets.at[index, 'nearest_point_wbd']

        if isinstance(levelpath_outlets_nearest_points, pd.Series):
            levelpath_outlets_nearest_points = levelpath_outlets_nearest_points.iloc[-1]
        if isinstance(levelpath_outlets_nearest_points_wbd, pd.Series):
            levelpath_outlets_nearest_points_wbd = levelpath_outlets_nearest_points_wbd.iloc[-1]

        # Extend outlet stream if outlet point is outside of the WBD or nearest snap point is within 100m of the WBD boundary
        outlet_point = Point(row['geometry'].coords[-1])
        if (outlet_point.distance(levelpath_outlets_nearest_points_wbd) < 100) or (
            ~outlet_point.intersects(wbd.geometry)[0]
        ):
            levelpath_outlets.at[index, 'geometry'] = LineString(
                list(row['geometry'].coords) + list([levelpath_outlets_nearest_points.coords[0]])
            )
        else:
            errors += 1

    levelpath_outlets = gpd.GeoDataFrame(data=levelpath_outlets, geometry='geometry')
    levelpath_outlets = levelpath_outlets.drop(columns=['last', 'nearest_point', 'nearest_point_wbd'])

    # Replace the streams in the original file with the extended streams
    streams = streams[~streams['ID'].isin(levelpath_outlets['ID'])]
    streams = pd.concat([streams, levelpath_outlets], ignore_index=True)

    return streams


def subset_vector_layers(huc, wbd_filename, wbd_buffer_filename, huc_directory, copy_from_dir, copying_flags):
    """
    Subsets vector layers for a given HUC region by either copying from pre-clipped data
    or generating new clipped layers based on flags.

    Args:
        huc (str): HUC number as a string identifier.
        wbd_filename (str): Path to the GeoPackage file containing the HUC boundary.
        wbd_buffer_filename (str): Path to the GeoPackage file containing the buffered HUC boundary.
        huc_directory (str): Output directory for saving the resulting vector layers.
        copy_from_dir (str): Directory containing pre-clipped vector data to copy from (if applicable).
        copying_flags (dict): Dictionary with 8 boolean flags indicating which vector layers to copy
                              vs. clip. Example:
            {
                'copy_nwm_lakes': True,
                'copy_nwm_streams_headwater': True,
                'copy_nwm_catchments': False,
                'copy_levee_lines': False,
                'copy_levee_lines_burned': False,
                'copy_levee_protected_areas': False,
                'copy_osm_bridges': False,
                'copy_osm_roads': False
            }

    Returns:
        None. Saves the subsetted vector layers to the specified `huc_directory`.

    """

    dem_cellsize = float(os.getenv('res'))

    # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
    if huc[:2] == '19':
        nwm_lakes = os.getenv('input_nwm_lakes_Alaska')
        nwm_catchments = os.getenv('input_nwm_catchments_Alaska')
        nld_lines = os.getenv('input_NLD_Alaska')
        nld_lines_preprocessed = os.getenv('input_levees_preprocessed_Alaska')
        nwm_streams = os.getenv('input_nwm_flows_Alaska')
        nwm_headwaters = os.getenv('input_nwm_headwaters_Alaska')
        levee_protected_areas = os.getenv('input_nld_levee_protected_areas_Alaska')
        osm_bridges = os.getenv('osm_bridges_alaska')
        osm_roads = os.getenv('osm_roads_alaska')
        huc_CRS = os.getenv('ALASKA_CRS')
        input_LANDSEA = os.getenv('input_landsea_Alaska')
    else:
        nwm_lakes = os.getenv('input_nwm_lakes')
        nwm_catchments = os.getenv('input_nwm_catchments')
        nld_lines = os.getenv('input_NLD')
        nld_lines_preprocessed = os.getenv('input_levees_preprocessed')
        nwm_streams = os.getenv('input_nwm_flows')
        nwm_headwaters = os.getenv('input_nwm_headwaters')
        levee_protected_areas = os.getenv('input_nld_levee_protected_areas')
        osm_bridges = os.getenv('osm_bridges')
        osm_roads = os.getenv('osm_roads')
        huc_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')

        if huc[:2] == "04":
            input_LANDSEA = os.getenv('input_GL_boundaries')
        else:
            input_LANDSEA = os.getenv('input_landsea')

    # read wbd and wbd_buffered that are needed for clipping
    wbd = gpd.read_file(os.path.join(huc_directory, wbd_filename))
    wbd_buffer = gpd.read_file(os.path.join(huc_directory, wbd_buffer_filename))

    if 'shape_Length' in wbd.columns:
        wbd = wbd.drop(columns=['shape_Length'])

    # for copying, use shutil.copy2 to preserve the orignal files timestamps
    if copying_flags['copy_levee_protected_areas']:
        src = os.path.join(copy_from_dir, huc, output_filenames['levee_protected_areas'])
        dst = os.path.join(huc_directory, output_filenames['levee_protected_areas'])

        if os.path.exists(src):
            logging.info(f"Copying levee-protected areas for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: levee-protected areas for {huc} not found at {src}.")
    else:
        # TODO investigate this old comment : Clip levee-protected areas polygons for future masking ocean areas (where applicable)
        logging.info(f"Clipping levee-protected areas for {huc}")
        levee_protected_areas = gpd.read_file(levee_protected_areas, mask=wbd_buffer, engine="fiona")
        if not levee_protected_areas.empty:
            levee_protected_areas.to_file(
                os.path.join(huc_directory, output_filenames['levee_protected_areas']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        del levee_protected_areas

    if copying_flags['copy_nwm_lakes']:
        src = os.path.join(copy_from_dir, huc, output_filenames['nwm_lakes'])
        dst = os.path.join(huc_directory, output_filenames['nwm_lakes'])
        if os.path.exists(src):
            logging.info(f"Copying nwm_lakes for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: nwm_lakes for {huc} not found at {src}.")
    else:
        # Find intersecting lakes and writeout
        logging.info(f"clipping NWM Lakes for {huc}")
        nwm_lakes = gpd.read_file(nwm_lakes, mask=wbd_buffer, engine="fiona")
        nwm_lakes = nwm_lakes.loc[nwm_lakes.geometry.area < 18990454000.0]

        if not nwm_lakes.empty:
            # Perform fill process to remove holes/islands in the NWM lake polygons
            nwm_lakes = nwm_lakes.explode(index_parts=True)
            nwm_lakes_fill_holes = MultiPolygon(
                Polygon(p.exterior) for p in nwm_lakes['geometry']
            )  # remove donut hole geometries
            # Loop through the filled polygons and insert the new geometry
            for i in range(len(nwm_lakes_fill_holes.geoms)):
                nwm_lakes.loc[i, 'geometry'] = nwm_lakes_fill_holes.geoms[i]
            nwm_lakes.to_file(
                os.path.join(huc_directory, output_filenames['nwm_lakes']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        del nwm_lakes

    if copying_flags['copy_levee_lines']:
        src = os.path.join(copy_from_dir, huc, output_filenames['levee_lines'])
        dst = os.path.join(huc_directory, output_filenames['levee_lines'])
        if os.path.exists(src):
            logging.info(f"Copying NLD levee_lines for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: levee_lines for {huc} not found at {src}.")
    else:
        # Find intersecting levee lines
        logging.info(f"Clipping NLD levee lines for {huc}")
        nld_lines = gpd.read_file(nld_lines, mask=wbd_buffer, engine="fiona")
        if not nld_lines.empty:
            nld_lines.to_file(
                os.path.join(huc_directory, output_filenames['levee_lines']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        del nld_lines

    if copying_flags['copy_levee_lines_burned']:
        src = os.path.join(copy_from_dir, huc, output_filenames['levee_lines_burned'])
        dst = os.path.join(huc_directory, output_filenames['levee_lines_burned'])
        if os.path.exists(src):
            logging.info(f"Copying levee_lines_burned for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: levee_lines_burned for {huc} not found at {src}.")
    else:
        # Preprocessed levee lines for burning
        logging.info(f"Clipping levee_lines_burned for {huc}.")
        nld_lines_preprocessed = gpd.read_file(nld_lines_preprocessed, mask=wbd_buffer, engine="fiona")
        if not nld_lines_preprocessed.empty:
            nld_lines_preprocessed.to_file(
                os.path.join(huc_directory, output_filenames['levee_lines_burned']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        del nld_lines_preprocessed

    if copying_flags['copy_nwm_catchments']:
        src = os.path.join(copy_from_dir, huc, output_filenames['nwm_catchments'])
        dst = os.path.join(huc_directory, output_filenames['nwm_catchments'])
        if os.path.exists(src):
            logging.info(f"Copying nwm_catchments for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: nwm_catchments for {huc} not found at {src}.")
    else:
        # Find intersecting nwm_catchments
        logging.info(f"Clipping nwm_catchments for {huc}.")
        nwm_catchments = gpd.read_file(nwm_catchments, mask=wbd_buffer, engine="fiona")

        if len(nwm_catchments) > 0:
            nwm_catchments.to_file(
                os.path.join(huc_directory, output_filenames['nwm_catchments']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        else:
            logging.info("No NWM catchments within HUC " + str(huc) + " boundaries.")
            sys.exit(0)

        del nwm_catchments

    if copying_flags['copy_osm_bridges']:
        src = os.path.join(copy_from_dir, huc, output_filenames['osm_bridges'])
        dst = os.path.join(huc_directory, output_filenames['osm_bridges'])
        if os.path.exists(src):
            logging.info(f"Copying osm_bridges for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: osm_bridges for {huc} not found at {src}.")
    else:
        # Subset OSM (Open Street Map) bridges
        logging.info(f"Clipping OSM Bridges for {huc}")

        subset_osm_bridges_gdb = gpd.read_file(osm_bridges, mask=wbd_buffer, engine="fiona")
        if subset_osm_bridges_gdb.empty:
            print("-- No applicable bridges for this HUC")
            logging.info("-- No applicable bridges for this HUC")
        else:
            subset_osm_bridges_gdb.to_file(
                os.path.join(huc_directory, output_filenames['osm_bridges']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )

        del subset_osm_bridges_gdb

    if copying_flags['copy_osm_roads']:
        src = os.path.join(copy_from_dir, huc, output_filenames['osm_roads'])
        dst = os.path.join(huc_directory, output_filenames['osm_roads'])
        if os.path.exists(src):
            logging.info(f"Copying osm_roads for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: osm_roads for {huc} not found at {src}.")
    else:
        # Subset OSM (Open Street Map) roads
        logging.info(f"Clipping OSM roads for {huc}")

        subset_osm_roads_gdb = gpd.read_file(osm_roads, mask=wbd_buffer, engine="fiona")
        if subset_osm_roads_gdb.empty:
            print("-- No applicable roads for this HUC")
            logging.info("-- No applicable roads for this HUC")
        else:
            subset_osm_roads_gdb.to_file(
                os.path.join(huc_directory, output_filenames['osm_roads']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )

        del subset_osm_roads_gdb

    if copying_flags['copy_nwm_streams_headwater']:
        for vector_item in ['wbd_streams_buffer', 'nwm_streams', 'nwm_headwaters']:
            src = os.path.join(copy_from_dir, huc, output_filenames[vector_item])
            dst = os.path.join(huc_directory, output_filenames[vector_item])

            if os.path.exists(src):
                logging.info(f"Copying {vector_item} for {huc} (from previous output).")
                shutil.copy2(src, dst)
            else:
                logging.warning(f"Missing file: {vector_item} for {huc} not found at {src}.")

    else:
        # first Make the streams buffer smaller than the wbd_buffer so streams don't reach the edge of the DEM
        logging.info(f"Create stream buffer for {huc}")
        wbd_streams_buffer = wbd_buffer.copy()
        wbd_streams_buffer.geometry = wbd_streams_buffer.geometry.buffer(-8 * dem_cellsize, resolution=32)

        wbd_streams_buffer = wbd_streams_buffer[['geometry']]
        wbd_streams_buffer.to_file(
            os.path.join(huc_directory, output_filenames['wbd_streams_buffer']),
            driver='GPKG',
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )

        # Subset nwm streams
        logging.info(f"Clipping NWM Streams for {huc}")
        nwm_streams = gpd.read_file(nwm_streams, mask=wbd_buffer, engine="fiona")

        # NWM can have duplicate records, but appear to always be identical duplicates
        nwm_streams.drop_duplicates(subset="ID", keep="first", inplace=True)

        nwm_streams = extend_outlet_streams(nwm_streams, wbd_buffer, wbd)

        # Select only the streams that are outlet
        streams_crossing_wbd = gpd.sjoin(nwm_streams, wbd, predicate='crosses')
        if streams_crossing_wbd.empty:
            logging.warning("No streams intersect the WBD. Cannot extend outlet streams.")
            return nwm_streams

        if os.path.exists(input_LANDSEA):
            logging.info(f"Clipping NWM Streams for {huc} to land areas")
            landsea = gpd.read_file(input_LANDSEA, mask=wbd_buffer, engine="fiona")
            nwm_streams = nwm_streams.overlay(landsea, how='difference')
        else:
            logging.info(f"No landsea file provided, using all NWM streams for {huc}")

        if nwm_streams.empty:
            print("No NWM stream segments within HUC " + str(huc) + " boundaries.")
            logging.info("No NWM stream segments within HUC " + str(huc) + " boundaries.")
            sys.exit(0)

        # Filter streams that are outlets
        outlets = streams_crossing_wbd[streams_crossing_wbd['to'] == 0]
        outlets.to_file(
            os.path.join(
                huc_directory, os.path.splitext(output_filenames['nwm_streams'])[0] + '_outlets.gpkg'
            ),
            driver='GPKG',
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )

        # Find all stream IDs downstream of the outlets
        outlet_ids = set()
        for outlet_id in set(outlets['ID']):
            outlet_ids.add(outlet_id)
            downstream_segments = nwm_streams[
                nwm_streams['ID'] == nwm_streams.loc[nwm_streams['ID'] == outlet_id, 'to'].values[0]
            ]
            while not downstream_segments.empty:
                outlet_ids.add(downstream_segments['ID'].values[0])
                downstream_segments = nwm_streams[nwm_streams['ID'].isin(downstream_segments['to'])]
        # Filter the original streams to keep only those that are downstream of the outlets
        nwm_streams_outlets = nwm_streams[nwm_streams['ID'].isin(outlet_ids)]
        nwm_streams_outlets.to_file(
            os.path.join(
                huc_directory,
                os.path.splitext(output_filenames['nwm_streams'])[0] + '_outlets_downstream.gpkg',
            ),
            driver='GPKG',
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )

        nwm_streams_nonoutlets = nwm_streams[~nwm_streams['ID'].isin(outlet_ids)]

        if len(nwm_streams) > 0:
            # Address issue where NWM streams exit the HUC boundary and then re-enter, creating a MultiLineString
            nwm_streams_nonoutlets = (
                gpd.clip(nwm_streams_nonoutlets, wbd_streams_buffer).explode(index_parts=True).reset_index()
            )

            # Find and keep the downstream segment of the NWM stream
            max_parts = nwm_streams_nonoutlets[['level_0', 'level_1']].groupby('level_0').max()

            nwm_streams_nonoutlets = nwm_streams_nonoutlets.merge(
                max_parts, on='level_0', suffixes=('', '_max')
            )

            nwm_streams_nonoutlets = nwm_streams_nonoutlets[
                nwm_streams_nonoutlets['level_1'] == nwm_streams_nonoutlets['level_1_max']
            ]

            nwm_streams_nonoutlets = nwm_streams_nonoutlets.drop(columns=['level_1_max'])

            nwm_streams = pd.concat([nwm_streams_nonoutlets, nwm_streams_outlets])

            nwm_streams.to_file(
                os.path.join(huc_directory, output_filenames['nwm_streams']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        else:
            print("No NWM stream segments within HUC " + str(huc) + " boundaries.")
            logging.info("No NWM stream segments within HUC " + str(huc) + " boundaries.")
            sys.exit(0)
        del nwm_streams

        # Subset NWM headwaters
        logging.info(f"Clipping NWM Headwater Points for {huc}")
        nwm_headwaters = gpd.read_file(nwm_headwaters, mask=wbd_streams_buffer, engine="fiona")

        if len(nwm_headwaters) > 0:
            nwm_headwaters.to_file(
                os.path.join(huc_directory, output_filenames['nwm_headwaters']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        else:
            print("No headwater point(s) within HUC " + str(huc) + " boundaries.")
            logging.info("No headwater point(s) within HUC " + str(huc) + " boundaries.")
            sys.exit(0)

        del nwm_headwaters


if __name__ == '__main__':
    '''
    sample usage:

    python clip_vectors_to_wbd.py \
    --huc 21020001 \
    --wbd_filename wbd.gpkg \
    --wbd_buffer_filename wbd_buffered.gpkg \
    --huc_directory outputs/preclips/test3/21020001/ \
    --copy_from_dir data/inputs/pre_clip_huc8/20250218/ \
    --copying_flags '{"copy_nwm_lakes": true,
        "copy_nwm_streams_headwater": true,
        "copy_nwm_catchments": false,
            "copy_levee_lines": false,
            "copy_levee_lines_burned": false,
            "copy_levee_protected_areas": false,
            "copy_osm_bridges": false,
                "copy_osm_roads": false}'
    '''

    parser = argparse.ArgumentParser(description='Subset vector layers')

    parser.add_argument('--huc', type=str, required=True, help='HUC number (e.g., "03180004")')
    parser.add_argument('--wbd_filename', type=str, required=True, help='name of the HUC boundary gpkg file')
    parser.add_argument(
        '--wbd_buffer_filename', type=str, required=True, help='name of the buffered HUC boundary gpkg file'
    )
    parser.add_argument(
        '--huc_directory',
        type=str,
        required=True,
        help='Directory containing the above GPKG files and HUC-specific output results',
    )
    parser.add_argument(
        '--copy_from_dir', type=str, required=True, help='Directory with pre-clipped data for copying'
    )
    parser.add_argument(
        '--copying_flags',
        type=json.loads,
        required=True,
        help='A dictionary with 8 itesm indicating which layers to copy vs. clip (e.g., \'{"copy_nwm_lakes": true, ...}\')',
    )

    args = vars(parser.parse_args())

    subset_vector_layers(**args)
