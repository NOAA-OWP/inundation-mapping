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


def subset_vector_layers(
    subset_nwm_lakes,
    subset_streams,
    hucCode,
    subset_nwm_headwaters,
    wbd_buffer_filename,
    wbd_streams_buffer_filename,
    wbd_filename,
    dem_filename,
    dem_domain,
    nwm_lakes,
    catchments_filename,
    subset_catchments,
    nld_lines,
    nld_lines_preprocessed,
    landsea,
    input_streams,
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
    stream_id_attribute='ID',
    stream_to_attribute='to',
    hr_to_v2=None,
    catchments_layer=None,
    catchment_id_attribute='ID',
):

    def extend_outlet_streams(streams, wbd_buffered, wbd, stream_outlets, stream_id_attribute='ID'):
        """
        Extend outlet streams to nearest buffered WBD boundary
        """

        wbd['geometry'] = wbd.geometry.boundary
        wbd = gpd.GeoDataFrame(data=wbd, geometry='geometry')

        wbd_buffered["linegeom"] = wbd_buffered.geometry

        # Select only the streams that don't intersect the WBD boundary line
        levelpath_outlets = stream_outlets[~stream_outlets.intersects(wbd['geometry'].iloc[0])]

        levelpath_outlets['nearest_point'] = None
        levelpath_outlets['last'] = None

        levelpath_outlets = levelpath_outlets.explode(index_parts=False)

        for index, row in levelpath_outlets.iterrows():
            coords = [(coords) for coords in list(row['geometry'].coords)]
            last_coord = coords[-1]
            levelpath_outlets.at[index, 'last'] = Point(last_coord)

        wbd_buffered['geometry'] = wbd_buffered.geometry.boundary
        wbd_buffered = gpd.GeoDataFrame(data=wbd_buffered, geometry='geometry')

        for index, row in levelpath_outlets.iterrows():
            levelpath_geom = row['last']
            nearest_point = nearest_points(levelpath_geom, wbd_buffered)

            levelpath_outlets.at[index, 'nearest_point'] = nearest_point[1]['geometry'].iloc[0]

            levelpath_outlets_nearest_points = levelpath_outlets.at[index, 'nearest_point']
            if isinstance(levelpath_outlets_nearest_points, pd.Series):
                levelpath_outlets_nearest_points = levelpath_outlets_nearest_points.iloc[-1]

            levelpath_outlets.at[index, 'geometry'] = LineString(
                list(row['geometry'].coords) + list([levelpath_outlets_nearest_points.coords[0]])
            )

        levelpath_outlets = gpd.GeoDataFrame(data=levelpath_outlets, geometry='geometry')
        levelpath_outlets = levelpath_outlets.drop(columns=['last', 'nearest_point'])

        # Replace the streams in the original file with the extended streams
        streams = streams[~streams[stream_id_attribute].isin(levelpath_outlets[stream_id_attribute])]
        streams = pd.concat([streams, levelpath_outlets], ignore_index=True)

        return streams

    # -------------------------------------------------------------------------------------------------------------------
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

    # Subset streams
    logging.info(f"Subsetting NWM Streams for {hucCode}")

    hr_to_v2 = gpd.read_file(
        hr_to_v2,
        ignore_fields=['measure', 'offset', 'totdasqkm', 'factor', 'lp', 'match_type'],
        mask=wbd_buffer,
        engine="fiona",
    )

    hr_to_v2 = hr_to_v2.drop(columns=['geometry'])
    hr_to_v2.rename(columns={'id': 'ID'}, inplace=True)
    hr_to_v2 = hr_to_v2[hr_to_v2['position'] == 'start']

    input_streams = gpd.read_file(input_streams, mask=wbd_buffer, engine="fiona")

    # Find input_streams in lakes
    input_streams_in_lakes = gpd.overlay(input_streams, nwm_lakes, how='intersection')
    input_streams_in_lakes.rename(columns={'newID': 'Lake'}, inplace=True)
    input_streams_in_lakes = input_streams_in_lakes[[stream_id_attribute, 'Lake']]

    input_streams = input_streams.merge(input_streams_in_lakes, how='left', on=stream_id_attribute)

    input_streams['Lake'] = input_streams['Lake'].fillna(-9999)

    # Join crosswalk points
    input_streams = input_streams.merge(
        hr_to_v2, left_on=stream_id_attribute, right_on='point_id', how='inner'
    )

    input_streams = gpd.GeoDataFrame(input_streams, geometry='geometry')

    # NWM can have duplicate records, but appear to always be identical duplicates
    input_streams.drop_duplicates(subset=stream_id_attribute, keep="first", inplace=True)

    input_streams_are_not_outlets = input_streams[stream_to_attribute].isin(
        input_streams[stream_id_attribute]
    )
    input_streams_outlets = input_streams[~input_streams_are_not_outlets]
    input_streams_nonoutlets = input_streams[input_streams_are_not_outlets]

    input_streams = extend_outlet_streams(
        input_streams, wbd_buffer, wbd, input_streams_outlets, stream_id_attribute
    )

    if len(input_streams) == 0:
        print("No NWM stream segments within HUC " + str(hucCode) + " boundaries.")
        logging.info("No NWM stream segments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)

    # Address issue where NWM streams exit the HUC boundary and then re-enter, creating a MultiLineString
    input_streams_nonoutlets = (
        gpd.clip(input_streams_nonoutlets, wbd_streams_buffer).explode(index_parts=True).reset_index()
    )

    # Find and keep the downstream segment of the NWM stream
    max_parts = input_streams_nonoutlets[['level_0', 'level_1']].groupby('level_0').max()

    input_streams_nonoutlets = input_streams_nonoutlets.merge(max_parts, on='level_0', suffixes=('', '_max'))

    input_streams_nonoutlets = input_streams_nonoutlets[
        input_streams_nonoutlets['level_1'] == input_streams_nonoutlets['level_1_max']
    ]

    input_streams_nonoutlets = input_streams_nonoutlets.drop(columns=['level_1_max'])

    input_streams = pd.concat([input_streams_nonoutlets, input_streams_outlets])

    input_streams.to_file(
        subset_streams, driver=getDriver(subset_streams), index=False, crs=huc_CRS, engine="fiona"
    )

    del input_streams

    # Subset catchments
    # print(f"Subsetting NWM Catchments for {hucCode}", flush=True)
    logging.info(f"Subsetting Catchments for {hucCode}")

    if catchments_layer is not None:
        if catchments_layer == 'NHDPlusCatchment':
            catchments_filename = catchments_filename.format(hucCode[:4], hucCode[:4])
            catchments = os.path.join(os.path.split(catchments_filename)[0], catchments_layer + '.gpkg')
            if not os.path.exists(catchments):
                catchments_temp = gpd.read_file(catchments_filename, layer=catchments_layer)
                catchments_temp = catchments_temp.to_crs(huc_CRS)
                catchments_temp = catchments_temp.explode(index_parts=False)
                catchments_temp.to_file(catchments, driver='GPKG')

    catchments = gpd.read_file(catchments)

    # Join crosswalk points
    catchments = catchments.merge(hr_to_v2, left_on=catchment_id_attribute, right_on='point_id', how='inner')

    if catchments.crs != huc_CRS:
        catchments = catchments.to_crs(huc_CRS)

    if len(catchments) > 0:
        catchments.to_file(
            subset_catchments, driver=getDriver(subset_catchments), index=False, crs=huc_CRS, engine="fiona"
        )
    else:
        logging.info("No NWM catchments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)

    del catchments


if __name__ == '__main__':
    # print(sys.argv)

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-a', '--subset-nwm-lakes', help='NWM lake subset', required=True)
    parser.add_argument('-b', '--subset-streams', help='Streams subset', required=True)
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
    parser.add_argument('-m', '--catchments-filename', help='Catchments filename', required=True)
    parser.add_argument('-n', '--subset-catchments', help='Catchments subset', required=True)
    parser.add_argument('-r', '--nld-lines', help='Levee vectors to use within project path', required=True)
    parser.add_argument(
        '-rp', '--nld-lines-preprocessed', help='Levee vectors to use for DEM burning', required=True
    )
    parser.add_argument('-v', '--landsea', help='LandSea - land boundary', required=True)
    parser.add_argument('-w', '--input-streams', help='Input flowlines', required=True)
    parser.add_argument(
        '-wi', '--streams-id-attribute', help='Flowlines ID attribute', required=False, default='ID'
    )
    parser.add_argument(
        '-wt', '--streams-to-attribute', help='Flowlines to attribute', required=False, default='to'
    )
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
    parser.add_argument('-crs', '--huc-CRS', help='HUC crs', required=True)
    parser.add_argument('-hr', '--hr-to-v2', help='HR to V2', required=False, default=None)
    parser.add_argument(
        '-mi', '--catchments-id-attribute', help='Catchments ID attribute', required=False, default='ID'
    )

    args = vars(parser.parse_args())

    subset_vector_layers(**args)
