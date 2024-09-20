#!/usr/bin/env python3

import argparse
import logging
import os
import sys

import geopandas as gpd
import pandas as pd
import rasterio as rio
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import nearest_points

from utils.shared_functions import getDriver


gpd.options.io_engine = "pyogrio"


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


def subset_vector_layers(
    subset_nwm_lakes,
    subset_nwm_streams,
    hucCode,
    subset_nwm_headwaters,
    wbd_buffer_filename,
    wbd_streams_buffer_filename,
    wbd_filename,
    dem_filename,
    dem_domain,
    nwm_lakes,
    nwm_catchments,
    subset_nwm_catchments,
    nld_lines,
    nld_lines_preprocessed,
    landsea,
    nwm_streams,
    subset_landsea,
    nwm_headwaters,
    subset_nld_lines,
    subset_nld_lines_preprocessed,
    wbd_buffer_distance,
    levee_protected_areas,
    subset_levee_protected_areas,
    osm_bridges,
    subset_osm_bridges,
    is_alaska,
    huc_CRS,
):

    # print(f"Getting Cell Size for {hucCode}", flush=True)
    with rio.open(dem_filename) as dem_raster:
        dem_cellsize = max(dem_raster.res)

    wbd = gpd.read_file(wbd_filename, engine="pyogrio", use_arrow=True)
    dem_domain = gpd.read_file(dem_domain, engine="pyogrio", use_arrow=True)

    # Get wbd buffer
    logging.info(f"Create wbd buffer for {hucCode}")
    wbd_buffer = wbd.copy()
    wbd_buffer.geometry = wbd_buffer.geometry.buffer(wbd_buffer_distance, resolution=32)
    wbd_buffer = gpd.clip(wbd_buffer, dem_domain)

    # Clip ocean water polygon for future masking ocean areas (where applicable)
    logging.info(f"Clip ocean water polygon for {hucCode}")
    landsea = gpd.read_file(landsea, mask=wbd_buffer, engine="fiona")
    if not landsea.empty:
        os.makedirs(os.path.dirname(subset_landsea), exist_ok=True)
        # print(f"Create landsea gpkg for {hucCode}", flush=True)
        landsea.to_file(
            subset_landsea, driver=getDriver(subset_landsea), index=False, crs=huc_CRS, engine="fiona"
        )

        # Exclude landsea area from WBD and wbd_buffer
        wbd = wbd.overlay(landsea, how='difference')
        wbd.to_file(
            wbd_filename,
            layer='WBDHU8',
            driver=getDriver(wbd_filename),
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )

        wbd_buffer = wbd_buffer.overlay(landsea, how='difference')

    del landsea

    # Make the streams buffer smaller than the wbd_buffer so streams don't reach the edge of the DEM
    logging.info(f"Create stream buffer for {hucCode}")
    wbd_streams_buffer = wbd_buffer.copy()
    wbd_streams_buffer.geometry = wbd_streams_buffer.geometry.buffer(-8 * dem_cellsize, resolution=32)

    wbd_buffer = wbd_buffer[['geometry']]
    wbd_streams_buffer = wbd_streams_buffer[['geometry']]
    wbd_buffer.to_file(
        wbd_buffer_filename, driver=getDriver(wbd_buffer_filename), index=False, crs=huc_CRS, engine="fiona"
    )
    wbd_streams_buffer.to_file(
        wbd_streams_buffer_filename,
        driver=getDriver(wbd_streams_buffer_filename),
        index=False,
        crs=huc_CRS,
        engine="fiona",
    )

    # Clip levee-protected areas polygons for future masking ocean areas (where applicable)
    # print(f"Subsetting Levee Protected Areas for {hucCode}", flush=True)
    logging.info(f"Clip levee-protected areas for {hucCode}")
    levee_protected_areas = gpd.read_file(levee_protected_areas, mask=wbd_buffer, engine="fiona")
    if not levee_protected_areas.empty:
        levee_protected_areas.to_file(
            subset_levee_protected_areas,
            driver=getDriver(subset_levee_protected_areas),
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )
    del levee_protected_areas

    # Find intersecting lakes and writeout
    # print(f"Subsetting NWM Lakes for {hucCode}", flush=True)
    logging.info(f"Subsetting NWM Lakes for {hucCode}")
    nwm_lakes = gpd.read_file(nwm_lakes, mask=wbd_buffer, engine="fiona")
    nwm_lakes = nwm_lakes.loc[nwm_lakes.Shape_Area < 18990454000.0]

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
            subset_nwm_lakes, driver=getDriver(subset_nwm_lakes), index=False, crs=huc_CRS, engine="fiona"
        )
    del nwm_lakes

    # Find intersecting levee lines
    logging.info(f"Subsetting NLD levee lines for {hucCode}")
    nld_lines = gpd.read_file(nld_lines, mask=wbd_buffer, engine="fiona")
    if not nld_lines.empty:
        nld_lines.to_file(
            subset_nld_lines, driver=getDriver(subset_nld_lines), index=False, crs=huc_CRS, engine="fiona"
        )
    del nld_lines

    # Preprocessed levee lines for burning
    nld_lines_preprocessed = gpd.read_file(nld_lines_preprocessed, mask=wbd_buffer, engine="fiona")
    if not nld_lines_preprocessed.empty:
        nld_lines_preprocessed.to_file(
            subset_nld_lines_preprocessed,
            driver=getDriver(subset_nld_lines_preprocessed),
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )
    del nld_lines_preprocessed

    # Subset NWM headwaters
    logging.info(f"Subsetting NWM Headwater Points for {hucCode}")
    nwm_headwaters = gpd.read_file(nwm_headwaters, mask=wbd_streams_buffer, engine="fiona")

    if len(nwm_headwaters) > 0:
        nwm_headwaters.to_file(
            subset_nwm_headwaters,
            driver=getDriver(subset_nwm_headwaters),
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )
    else:
        print("No headwater point(s) within HUC " + str(hucCode) + " boundaries.")
        logging.info("No headwater point(s) within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)

    del nwm_headwaters

    # Find intersecting nwm_catchments
    # print(f"Subsetting NWM Catchments for {hucCode}", flush=True)
    nwm_catchments = gpd.read_file(nwm_catchments, mask=wbd_buffer, engine="fiona")

    if len(nwm_catchments) > 0:
        nwm_catchments.to_file(
            subset_nwm_catchments,
            driver=getDriver(subset_nwm_catchments),
            index=False,
            crs=huc_CRS,
            engine="fiona",
        )
    else:
        logging.info("No NWM catchments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)

    del nwm_catchments

    # Subset OSM (Open Street Map) bridges
    if osm_bridges != "":
        logging.info(f"Subsetting OSM Bridges for {hucCode}")

        subset_osm_bridges_gdb = gpd.read_file(osm_bridges, mask=wbd_buffer, engine="fiona")
        if subset_osm_bridges_gdb.empty:
            print("-- No applicable bridges for this HUC")
            logging.info("-- No applicable bridges for this HUC")
        else:
            logging.info(f"Create subset of osm bridges gpkg for {hucCode}")
            if is_alaska is True:
                # we need to reproject
                subset_osm_bridges_gdb = subset_osm_bridges_gdb.to_crs(huc_CRS)

            subset_osm_bridges_gdb.to_file(
                subset_osm_bridges,
                driver=getDriver(subset_osm_bridges),
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )

        del subset_osm_bridges_gdb

    # Subset nwm streams
    logging.info(f"Subsetting NWM Streams for {hucCode}")
    nwm_streams = gpd.read_file(nwm_streams, mask=wbd_buffer, engine="fiona")

    # NWM can have duplicate records, but appear to always be identical duplicates
    nwm_streams.drop_duplicates(subset="ID", keep="first", inplace=True)

    nwm_streams = extend_outlet_streams(nwm_streams, wbd_buffer, wbd)

    nwm_streams_outlets = nwm_streams[~nwm_streams['to'].isin(nwm_streams['ID'])]
    nwm_streams_nonoutlets = nwm_streams[nwm_streams['to'].isin(nwm_streams['ID'])]

    if len(nwm_streams) > 0:
        # Address issue where NWM streams exit the HUC boundary and then re-enter, creating a MultiLineString
        nwm_streams_nonoutlets = (
            gpd.clip(nwm_streams_nonoutlets, wbd_streams_buffer).explode(index_parts=True).reset_index()
        )

        # Find and keep the downstream segment of the NWM stream
        max_parts = nwm_streams_nonoutlets[['level_0', 'level_1']].groupby('level_0').max()

        nwm_streams_nonoutlets = nwm_streams_nonoutlets.merge(max_parts, on='level_0', suffixes=('', '_max'))

        nwm_streams_nonoutlets = nwm_streams_nonoutlets[
            nwm_streams_nonoutlets['level_1'] == nwm_streams_nonoutlets['level_1_max']
        ]

        nwm_streams_nonoutlets = nwm_streams_nonoutlets.drop(columns=['level_1_max'])

        nwm_streams = pd.concat([nwm_streams_nonoutlets, nwm_streams_outlets])

        nwm_streams.to_file(
            subset_nwm_streams, driver=getDriver(subset_nwm_streams), index=False, crs=huc_CRS, engine="fiona"
        )
    else:
        print("No NWM stream segments within HUC " + str(hucCode) + " boundaries.")
        logging.info("No NWM stream segments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nwm_streams


if __name__ == '__main__':
    # print(sys.argv)

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-a', '--subset-nwm-lakes', help='NWM lake subset', required=True)
    parser.add_argument('-b', '--subset-nwm-streams', help='NWM streams subset', required=True)
    parser.add_argument('-d', '--hucCode', help='HUC boundary ID', required=True, type=str)
    parser.add_argument(
        '-e', '--subset-nwm-headwaters', help='NWM headwaters subset', required=True, default=None
    )
    parser.add_argument('-f', '--wbd_buffer_filename', help='Buffered HUC boundary', required=True)
    parser.add_argument(
        '-s', '--wbd_streams_buffer_filename', help='Buffered HUC boundary (streams)', required=True
    )
    parser.add_argument('-g', '--wbd-filename', help='HUC boundary', required=True)
    parser.add_argument('-i', '--dem-filename', help='DEM filename', required=True)
    parser.add_argument('-j', '--dem-domain', help='DEM domain polygon', required=True)
    parser.add_argument('-l', '--nwm-lakes', help='NWM Lakes', required=True)
    parser.add_argument('-m', '--nwm-catchments', help='NWM catchments', required=True)
    parser.add_argument('-n', '--subset-nwm-catchments', help='NWM catchments subset', required=True)
    parser.add_argument('-r', '--nld-lines', help='Levee vectors to use within project path', required=True)
    parser.add_argument(
        '-rp', '--nld-lines-preprocessed', help='Levee vectors to use for DEM burning', required=True
    )
    parser.add_argument('-v', '--landsea', help='LandSea - land boundary', required=True)
    parser.add_argument('-w', '--nwm-streams', help='NWM flowlines', required=True)
    parser.add_argument('-x', '--subset-landsea', help='LandSea subset', required=True)
    parser.add_argument('-y', '--nwm-headwaters', help='NWM headwaters', required=True)
    parser.add_argument('-z', '--subset-nld-lines', help='Subset of NLD levee vectors for HUC', required=True)
    parser.add_argument(
        '-zp',
        '--subset-nld-lines-preprocessed',
        help='Subset of NLD levee vectors for burning elevations into DEMs',
        required=True,
    )
    parser.add_argument(
        '-wb', '--wbd-buffer-distance', help='WBD Mask buffer distance', required=True, type=int
    )
    parser.add_argument(
        '-lpf', '--levee-protected-areas', help='Levee-protected areas filename', required=True
    )
    parser.add_argument(
        '-lps', '--subset-levee-protected-areas', help='Levee-protected areas subset', required=True
    )
    parser.add_argument('-osm', '--osm-bridges', help='Open Street Maps gkpg', required=True)
    parser.add_argument('-osms', '--subset-osm-bridges', help='Open Street Maps subset', required=True)
    parser.add_argument('-ak', '--is-alaska', help='If in Alaska', action='store_true')
    parser.add_argument('-crs', '--huc-CRS', help='HUC crs', required=True)

    args = vars(parser.parse_args())

    subset_vector_layers(**args)
