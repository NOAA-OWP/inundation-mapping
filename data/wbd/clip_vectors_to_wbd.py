#!/usr/bin/env python3

import argparse
import json
import logging
import os
import shutil
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import rasterio as rio
from dotenv import load_dotenv
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import nearest_points

from src.utils.shared_functions import get_huc_vars


gpd.options.io_engine = "pyogrio"

srcDir = os.getenv('srcDir')
projectDir = os.getenv('projectDir')

load_dotenv(f'{srcDir}/bash_variables.env')
load_dotenv(f'{projectDir}/config/params_template.env')

output_filenames = {
    'lakes': "nwm_lakes_proj_subset.gpkg",
    "nwm_streams": "nwm_subset_streams.gpkg",
    "nwm_headwaters": "nwm_headwater_points_subset.gpkg",
    "wbd_streams_buffer": "wbd_buffered_streams.gpkg",
    'catchments': "nwm_catchments_proj_subset.gpkg",
    "levee_lines": "nld_subset_levees.gpkg",
    "levee_lines_burned": "3d_nld_subset_levees_burned.gpkg",
    "levee_protected_areas": "LeveeProtectedAreas_subset.gpkg",
    "osm_bridges": "osm_bridges_subset.gpkg",
    "osm_roads": "osm_roads_subset.gpkg",
    "buildings": "buildings_subset.gpkg",
}


def extend_outlet_streams(streams, wbd_buffered, wbd, landsea=None):
    """
    Extend outlet streams to nearest buffered WBD boundary
    """
    errors = 0

    # Select only the streams that are outlets
    levelpath_outlets = streams[streams['to'] == 0]

    if 'index_right' in levelpath_outlets.columns:
        levelpath_outlets = levelpath_outlets.drop(columns=['index_right'])
    # levelpath_outlets_columns = [x for x in levelpath_outlets.columns]

    # Select streams that intersect the WBD but not the WBD buffer
    # levelpath_outlets = levelpath_outlets.sjoin(wbd)[levelpath_outlets_columns]
    levelpath_outlets = levelpath_outlets.sjoin(wbd)

    if levelpath_outlets.empty:
        return streams, errors

    if 'index_right' in levelpath_outlets.columns:
        levelpath_outlets = levelpath_outlets.drop(columns=['index_right'])
    # levelpath_outlets = levelpath_outlets[levelpath_outlets_columns]

    # Get the WBD exterior ring
    wbd_boundary = wbd.copy()
    wbd_boundary['geometry'] = wbd_boundary.geometry.boundary
    wbd_boundary = gpd.GeoDataFrame(data=wbd_boundary, geometry='geometry')

    if 'linegeom' not in wbd_buffered.columns:
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
        levelpath_outlets.at[index, 'last'] = Point(coords[-1])

    if not wbd_buffered.geometry.boundary.explode().empty:
        wbd_buffered['geometry'] = wbd_buffered.geometry.boundary
        wbd_buffered = gpd.GeoDataFrame(data=wbd_buffered, geometry='geometry')

    for index, row in levelpath_outlets.iterrows():
        levelpath_geom = row['last']
        nearest_point = nearest_points(levelpath_geom, wbd_buffered)
        nearest_point_wbd = nearest_points(levelpath_geom, wbd_boundary.geometry)

        levelpath_outlets_nearest_points = nearest_point[1]['geometry'].iloc[0]
        levelpath_outlets_nearest_points_wbd = nearest_point_wbd[1].iloc[0]

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

        # Snap levelpath endpoints (mainstem only) to the nearest landsea boundary
        # if they are within 10 km, ensuring outlets align with the landsea.
        if landsea is not None and 'mainstem' in levelpath_outlets.columns:
            landsea_union = landsea.geometry.unary_union
            levelpath_outlets = levelpath_outlets[levelpath_outlets['mainstem'] == 1]
            for index, row in levelpath_outlets.iterrows():
                endpoint = Point(row['geometry'].coords[-1])
                nearest_point_landsea = nearest_points(endpoint, landsea_union)[1]
                distance = endpoint.distance(nearest_point_landsea)
                if distance <= 10000:
                    coords = list(row['geometry'].coords)
                    coords[-1] = nearest_point_landsea.coords[0]
                    levelpath_outlets.at[index, 'geometry'] = LineString(coords)

    levelpath_outlets = gpd.GeoDataFrame(data=levelpath_outlets, geometry='geometry')
    levelpath_outlets = levelpath_outlets.drop(columns=['last', 'nearest_point', 'nearest_point_wbd'])

    # Replace the streams in the original file with the extended streams
    if levelpath_outlets.empty:
        return streams, errors

    streams = streams[~streams['ID'].isin(levelpath_outlets['ID'])]
    streams = pd.concat([streams, levelpath_outlets], ignore_index=True)

    return streams, errors


def subset_vector_layers(
    huc, wbd_filename, wbd_buffer_filename, huc_directory, copy_from_dir, preclipping_flags
):
    """
    Subsets vector layers for a given HUC region by either copying from pre-clipped data
    or generating new clipped layers based on flags.

    Args:
        huc (str): HUC number as a string identifier.
        wbd_filename (str): Path to the GeoPackage file containing the HUC boundary.
        wbd_buffer_filename (str): Path to the GeoPackage file containing the buffered HUC boundary.
        huc_directory (str): Output directory for saving the resulting vector layers.
        copy_from_dir (str): Directory containing pre-clipped vector data to copy from (if applicable).
        preclipping_flags (dict): Dictionary with boolean flags indicating which vector layers to preclip
                              vs. copy.

    Returns:
        None. Saves the subsetted vector layers to the specified `huc_directory`.

    """

    # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
    buildings_parts_path = os.getenv('buildings_parts_path')  # one path is used for all regions

    huc_vars = get_huc_vars(huc)
    huc_CRS = huc_vars['crs']
    nwm_lakes = huc_vars['lakes']
    nwm_catchments = huc_vars['catchments']
    nld_lines = huc_vars['levees']
    nld_lines_preprocessed = huc_vars['levees_preprocessed']
    nwm_streams = huc_vars['streams']
    nwm_headwaters = huc_vars['headwaters']
    levee_protected_areas = huc_vars['levee_protected_areas']
    osm_roads = huc_vars['roads']
    input_LANDSEA = huc_vars['landsea']

    osm_bridges_modified_dir = os.getenv('osm_bridges_modified_dir')

    # read wbd and wbd_buffered that are needed for clipping
    wbd = gpd.read_file(os.path.join(huc_directory, wbd_filename))
    wbd_buffer = gpd.read_file(os.path.join(huc_directory, wbd_buffer_filename))

    wbd = wbd[['HUC8', 'fimid', 'geometry']]
    # if 'shape_Length' in wbd.columns:
    #     wbd = wbd.drop(columns=['shape_Length', 'areasqkm'])

    # for copying, use shutil.copy2 to preserve the orignal files timestamps
    if not preclipping_flags['lakes']:
        src = os.path.join(copy_from_dir, huc, output_filenames['lakes'])
        dst = os.path.join(huc_directory, output_filenames['lakes'])
        if os.path.exists(src):
            logging.info(f"Copying nwm_lakes for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: nwm_lakes for {huc} not found at {src}.")
    else:
        # Find intersecting lakes and writeout
        logging.info(f"clipping NWM Lakes for {huc}")
        logging.info(f"Using nwm_lakes source for {huc}: {nwm_lakes}")
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
                os.path.join(huc_directory, output_filenames['lakes']),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )
        del nwm_lakes

    if not preclipping_flags['levees']:
        for vector_item in ['levee_lines', 'levee_lines_burned', 'levee_protected_areas']:
            src = os.path.join(copy_from_dir, huc, output_filenames[vector_item])
            dst = os.path.join(huc_directory, output_filenames[vector_item])
            if os.path.exists(src):
                logging.info(f"Copying {vector_item} for {huc} (from previous output).")
                shutil.copy2(src, dst)
            else:
                logging.warning(f"Missing file: {vector_item} for {huc} not found at {src}.")
    else:
        # Preprocessed levee lines for burning
        logging.info(f"Clipping levee_lines_burned for {huc}.")
        if nld_lines_preprocessed and os.path.exists(nld_lines_preprocessed):
            logging.info(f"Using levee_lines_burned source for {huc}: {nld_lines_preprocessed}")
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

        logging.info(f"Clipping NLD levee lines for {huc}")
        if nld_lines and os.path.exists(nld_lines):
            logging.info(f"Using levee_lines source for {huc}: {nld_lines}")
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

        logging.info(f"Clipping levee-protected areas for {huc}")
        if levee_protected_areas and os.path.exists(levee_protected_areas):
            logging.info(f"Using levee_protected_areas source for {huc}: {levee_protected_areas}")
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

    if not preclipping_flags['catchments']:
        src = os.path.join(copy_from_dir, huc, output_filenames['catchments'])
        dst = os.path.join(huc_directory, output_filenames['catchments'])
        if os.path.exists(src):
            logging.info(f"Copying nwm_catchments for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: nwm_catchments for {huc} not found at {src}.")
    else:
        # Find intersecting nwm_catchments
        logging.info(f"Clipping nwm_catchments for {huc}.")
        if os.path.exists(nwm_catchments):
            logging.info(f"Using nwm_catchments source for {huc}: {nwm_catchments}")
            nwm_catchments = gpd.read_file(nwm_catchments, engine="fiona")

            nwm_catchments = nwm_catchments.clip(wbd_buffer)

            if len(nwm_catchments) > 0:
                nwm_catchments.to_file(
                    os.path.join(huc_directory, output_filenames['catchments']),
                    driver='GPKG',
                    index=False,
                    crs=huc_CRS,
                    engine="fiona",
                )
            else:
                logging.info("No NWM catchments within HUC " + str(huc) + " boundaries.")
                sys.exit(0)

            del nwm_catchments

    if not preclipping_flags['osm_bridges']:
        src = os.path.join(copy_from_dir, huc, output_filenames['osm_bridges'])
        dst = os.path.join(huc_directory, output_filenames['osm_bridges'])
        if os.path.exists(src):
            logging.info(f"Copying osm_bridges for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: osm_bridges for {huc} not found at {src}.")
    else:
        # Bridge modified files are already HUC-level, so stage them directly.
        dst = os.path.join(huc_directory, output_filenames['osm_bridges'])

        if not osm_bridges_modified_dir:
            logging.warning(
                "Missing osm_bridges_modified_dir environment variable; osm_bridges not transferred."
            )
        else:
            src = os.path.join(osm_bridges_modified_dir, f'huc_{huc}_osm_bridges_modified.gpkg')
            if os.path.exists(src):
                logging.info(f"Transferring recently pulled osm_bridges for {huc} from {src}.")
                shutil.copy2(src, dst)
            else:
                logging.warning(f"Missing file: recently pulled osm_bridges for {huc} not found at {src}.")

    if not preclipping_flags['osm_roads']:
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
        if os.path.exists(osm_roads):
            logging.info(f"Using osm_roads source for {huc}: {osm_roads}")
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

    if not preclipping_flags['buildings']:
        src = os.path.join(copy_from_dir, huc, output_filenames['buildings'])
        dst = os.path.join(huc_directory, output_filenames['buildings'])
        if os.path.exists(src):
            logging.info(f"Copying buildings for {huc} (from previous output).")
            shutil.copy2(src, dst)
        else:
            logging.warning(f"Missing file: buildings for {huc} not found at {src}.")
    else:

        logging.info(f"compiling buildings for {huc}")
        huc_parts_path = Path(buildings_parts_path) / f"huc8_{huc}"
        logging.info(f"Using buildings source directory for {huc}: {huc_parts_path}")
        parquet_parts = list(huc_parts_path.glob("*.parquet")) if huc_parts_path.exists() else []
        if parquet_parts:
            gdfs = [gpd.read_parquet(p) for p in parquet_parts]
            merged = pd.concat(gdfs, ignore_index=True)
            merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=gdfs[0].crs)

            dst = os.path.join(huc_directory, output_filenames['buildings'])
            merged.to_file(dst, driver="GPKG", engine="fiona")

        else:
            logging.info(f"-- No building parquet files for huc {huc}")

    if not preclipping_flags['streams_headwater']:
        for vector_item in ['nwm_streams', 'nwm_headwaters']:
            src = os.path.join(copy_from_dir, huc, output_filenames[vector_item])
            dst = os.path.join(huc_directory, output_filenames[vector_item])

            if os.path.exists(src):
                logging.info(f"Copying {vector_item} for {huc} (from previous output).")
                shutil.copy2(src, dst)
            else:
                logging.warning(f"Missing file: {vector_item} for {huc} not found at {src}.")

    else:
        wbd_streams_buffer = gpd.read_file(
            os.path.join(huc_directory, output_filenames['wbd_streams_buffer'])
        )

        # Subset nwm streams
        logging.info(f"Clipping NWM Streams for {huc}")
        logging.info(f"Using nwm_streams source for {huc}: {nwm_streams}")
        nwm_streams = gpd.read_file(nwm_streams, mask=wbd_buffer, engine="fiona")

        nwm_streams.loc[~nwm_streams['to'].isin(nwm_streams['ID']), 'to'] = 0

        # NWM can have duplicate records, but appear to always be identical duplicates
        nwm_streams = nwm_streams.drop_duplicates(subset="ID", keep="first")

        nwm_streams, errors = extend_outlet_streams(nwm_streams, wbd_buffer, wbd)

        # Select only the streams that are outlet
        if 'index_right' in nwm_streams.columns:
            nwm_streams = nwm_streams.drop(columns=['index_right'])

        # NWM can have duplicate records, but appear to always be identical duplicates
        nwm_streams.drop_duplicates(subset="ID", keep="first", inplace=True)

        streams_in_wbd = gpd.sjoin(nwm_streams, wbd)
        ids_in_wbd = set(streams_in_wbd['ID'])
        if 'index_right' in streams_in_wbd.columns:
            streams_in_wbd = streams_in_wbd.drop(columns=['index_right'])

        if os.path.exists(input_LANDSEA):
            logging.info(f"Using landsea source for stream processing {huc}: {input_LANDSEA}")
            landsea = gpd.read_file(input_LANDSEA, mask=wbd_buffer, engine="fiona")
            if landsea.empty:
                logging.info(f"Landsea file provided but no landsea area found within wbd_buffer for {huc}")
                landsea = None
            else:
                # Dissolve landsea
                landsea = landsea.dissolve()
                logging.info(f"Clipping NWM Streams for {huc} to land areas")
        else:
            logging.info(f"No landsea file provided, using all NWM streams for {huc}")
            landsea = None

        nwm_streams, errors = extend_outlet_streams(nwm_streams, wbd_buffer, wbd, landsea)

        if nwm_streams.empty:
            print("No NWM stream segments within HUC " + str(huc) + " boundaries.")
            logging.info("No NWM stream segments within HUC " + str(huc) + " boundaries.")
            sys.exit(0)

        # Select only the streams that are outlet
        outlets = gpd.sjoin(nwm_streams, gpd.GeoDataFrame(geometry=wbd.boundary)).drop(
            columns=['index_right']
        )

        outlet_ids = set(outlets['ID'].to_list() + nwm_streams[nwm_streams['to'] == 0]['ID'].to_list())
        outlets = nwm_streams[nwm_streams['ID'].isin(outlet_ids)]

        outlets_temp = outlets[~outlets['to'].isin(streams_in_wbd['ID'])]

        if outlets_temp.empty:
            logging.warning("No streams intersect the WBD. Cannot extend outlet streams.")
            return nwm_streams

        if landsea is not None:
            logging.info(f"Clipping NWM Streams for {huc} to land areas")
            nwm_streams = nwm_streams.overlay(landsea, how='difference')

        # Find all stream IDs downstream of the outlets
        outlet_ids = set()
        for outlet_id in set(outlets['ID']):
            skip = False
            temp_list = [outlet_id]
            # outlet_ids.add(outlet_id)
            outlet_streams = nwm_streams.loc[nwm_streams['ID'] == outlet_id]
            if outlet_streams.empty:
                continue
            downstream_segments = nwm_streams[nwm_streams['ID'] == outlet_streams['to'].values[0]]
            while not downstream_segments.empty:
                temp_id = downstream_segments['ID'].values[0]

                if temp_id in ids_in_wbd:
                    # Ignore if downstream segment is within WBD
                    skip = True
                    break

                # outlet_ids.add(downstream_segments['ID'].values[0])
                temp_list.append(temp_id)
                downstream_segments = nwm_streams[nwm_streams['ID'].isin(downstream_segments['to'])]

            if skip:
                continue
            outlet_ids.update(temp_list)

        # Filter the original streams to keep only those that are downstream of the outlets
        nwm_streams_outlets = nwm_streams[nwm_streams['ID'].isin(outlet_ids)]
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

            nwm_streams = nwm_streams.drop(
                columns=['fimid', 'HUC8', 'fimid_left', 'HUC8_left', 'fimid_right', 'HUC8_right'],
                errors='ignore',
            )

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
        logging.info(f"Using nwm_headwaters source for {huc}: {nwm_headwaters}")
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
    --preclipping_flags '{'lakes': true,
        'streams_headwater': true,
        'catchments': false,
            "levees": false,
            "osm_bridges": false,
            "osm_roads": false,
            "buildings": false}'
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
        '--preclipping_flags',
        type=json.loads,
        required=True,
        help='A dictionary with 8 items indicating which layers to preclip vs. copy (e.g., \'{"lakes": true, ...}\')',
    )

    args = vars(parser.parse_args())

    subset_vector_layers(**args)
