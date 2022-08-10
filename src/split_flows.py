#!/usr/bin/env python3

'''
Description:
    1) split stream segments based on lake boundaries and input threshold distance
    2) calculate channel slope, manning's n, and LengthKm for each segment
    3) create unique ids using HUC8 boundaries (and unique FIM_ID column)
    4) create network traversal attribute columns (To_Node, From_Node, NextDownID)
    5) create points layer with segment verticies encoded with HydroID's (used for catchment delineation in next step)
'''

import argparse
import build_stream_traversal
import geopandas as gpd
import numpy as np
import os
import pandas as pd
import rasterio
import sys
import time

from collections import OrderedDict
from os import remove,environ,path
from os.path import isfile,split,dirname
from shapely import ops, wkt
from shapely.geometry import Point, LineString, MultiPoint
from shapely.ops import split as shapely_ops_split
from tqdm import tqdm
from utils.fim_enums import FIM_system_exit_codes
from utils.shared_functions import getDriver, mem_profile
from utils.shared_variables import FIM_ID
from utils.fim_enums import FIM_exit_codes

@mem_profile
def split_flows(max_length, 
                slope_min, 
                lakes_buffer_input, 
                flows_filename,
                dem_filename, 
                split_flows_filename, 
                split_points_filename, 
                wbd8_clp_filename, 
                lakes_filename,
                nwm_streams_filename,
                drop_stream_orders=False):

    wbd = gpd.read_file(wbd8_clp_filename)

    toMetersConversion = 1e-3

    print('Loading data ...')
    flows = gpd.read_file(flows_filename)

    if (len(flows) == 0):
        if (drop_stream_orders):
            # this is not an exception, but a custom exit code that can be trapped
            print("No relevant streams within HUC boundaries.")
            sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # will send a 61 back
        else:
            # if we are not dropping stream orders, then something is wrong
            raise Exception("No flowlines exist.")

    wbd8 = gpd.read_file(wbd8_clp_filename)
    dem = rasterio.open(dem_filename,'r')

    if isfile(lakes_filename):
        lakes = gpd.read_file(lakes_filename)
    else:
        lakes = None

    wbd8 = wbd8.filter(items=[FIM_ID, 'geometry'])
    wbd8 = wbd8.set_index(FIM_ID)
    flows = flows.explode()

    # temp
    flows = flows.to_crs(wbd8.crs)

    split_flows = []
    slopes = []
    hydro_id = 'HydroID'

    # If loop addressing: https://github.com/NOAA-OWP/inundation-mapping/issues/560
    # if we are processing branch 0, skip this step
    if (os.path.split(os.path.dirname(nwm_streams_filename))[1] != '0'):
        print ('trimming DEM stream to NWM branch terminus')
        # read in nwm lines, explode to ensure linestrings are the only geometry
        levelpath_lines = gpd.read_file(nwm_streams_filename).explode()

        # Dissolve the linestring (how much faith should I hold that these are digitized with flow?)
        linestring_geo = levelpath_lines.iloc[0]['geometry']
        if (len(levelpath_lines) > 1):
            linestring_geo = ops.linemerge(levelpath_lines.dissolve(by='levpa_id').iloc[0]['geometry'])

        # Identify the end vertex (most downstream, should be last), transform into geodataframe
        terminal_nwm_point = []
        first, last = linestring_geo.boundary
        terminal_nwm_point.append({'ID':'teminal','geometry':last})
        snapped_point = gpd.GeoDataFrame(terminal_nwm_point).set_crs(levelpath_lines.crs)

        # Snap to DEM flows
        snapped_point['geometry'] = snapped_point.apply(lambda row: flows.interpolate(flows.project( row.geometry)), axis=1)

        # Trim flows to snapped point
        # buffer here because python precision issues, print(demDerived_reaches.distance(snapped_point) < 1e-8)
        trimmed_line = shapely_ops_split(flows.iloc[0]['geometry'], snapped_point.iloc[0]['geometry'].buffer(1))
        # Edge cases: line string not split?, nothing is returned, split does not preserve linestring order?
        # Note to dear reader: last here is really the most upstream segmennt (see crevats above).  When we split we should get 3 segments, the most downstream one
        # the tiny 1 meter segment that falls within the snapped point buffer, and the most upstream one.  We want that last one which is why we trimmed_line[len(trimmed_line)-1]
        last_line_segment = pd.DataFrame({'id':['first'],'geometry':[trimmed_line[len(trimmed_line)-1].wkt]})
        last_line_segment['geometry'] = last_line_segment['geometry'].apply(wkt.loads) # can be last_line_segment = gpd.GeoSeries.from_wkt(last_line_segment) when we update geopandas verisons
        last_line_segment_geodataframe = gpd.GeoDataFrame(last_line_segment).set_crs(flows.crs)

        # replace geometry in merged flowine
        flows['geometry'] = last_line_segment_geodataframe.iloc[0]['geometry']

    # split at HUC8 boundaries
    print ('splitting stream segments at HUC8 boundaries')
    flows = gpd.overlay(flows, wbd8, how='union').explode().reset_index(drop=True)

    # check for lake features
    if lakes is not None:
        if len(lakes) > 0:
          print ('splitting stream segments at ' + str(len(lakes)) + ' waterbodies')
          #create splits at lake boundaries
          lakes = lakes.filter(items=['newID', 'geometry'])
          lakes = lakes.set_index('newID')
          flows = gpd.overlay(flows, lakes, how='union').explode().reset_index(drop=True)
          lakes_buffer = lakes.copy()
          lakes_buffer['geometry'] = lakes.buffer(lakes_buffer_input) # adding X meter buffer for spatial join comparison (currently using 20meters)

    print ('splitting ' + str(len(flows)) + ' stream segments based on ' + str(max_length) + ' m max length')

    # remove empty geometries
    flows = flows.loc[~flows.is_empty,:]

    for i,lineString in tqdm(enumerate(flows.geometry),total=len(flows.geometry)):
        # Reverse geometry order (necessary for BurnLines)
        lineString = LineString(lineString.coords[::-1])

        # skip lines of zero length
        if lineString.length == 0:
            continue

        # existing reaches of less than max_length
        if lineString.length < max_length:
            split_flows = split_flows + [lineString]
            line_points = [point for point in zip(*lineString.coords.xy)]

            # Calculate channel slope
            start_point = line_points[0]; end_point = line_points[-1]
            start_elev,end_elev = [i[0] for i in rasterio.sample.sample_gen(dem,[start_point,end_point])]
            slope = float(abs(start_elev - end_elev) / lineString.length)
            if slope < slope_min:
                slope = slope_min
            slopes = slopes + [slope]
            continue

        splitLength = lineString.length / np.ceil(lineString.length / max_length)

        cumulative_line = []
        line_points = []
        last_point = []

        last_point_in_entire_lineString = list(zip(*lineString.coords.xy))[-1]

        for point in zip(*lineString.coords.xy):

            cumulative_line = cumulative_line + [point]
            line_points = line_points + [point]
            numberOfPoints_in_cumulative_line = len(cumulative_line)

            if last_point:
                cumulative_line = [last_point] + cumulative_line
                numberOfPoints_in_cumulative_line = len(cumulative_line)
            elif numberOfPoints_in_cumulative_line == 1:
                continue

            cumulative_length = LineString(cumulative_line).length

            if cumulative_length >= splitLength:

                splitLineString = LineString(cumulative_line)
                split_flows = split_flows + [splitLineString]

                # Calculate channel slope
                start_point = cumulative_line[0]; end_point = cumulative_line[-1]
                start_elev,end_elev = [i[0] for i in rasterio.sample.sample_gen(dem,[start_point,end_point])]
                slope = float(abs(start_elev - end_elev) / splitLineString.length)
                if slope < slope_min:
                    slope = slope_min
                slopes = slopes + [slope]

                last_point = end_point

                if (last_point == last_point_in_entire_lineString):
                    continue

                cumulative_line = []
                line_points = []

        splitLineString = LineString(cumulative_line)
        split_flows = split_flows + [splitLineString]

        # Calculate channel slope
        start_point = cumulative_line[0]; end_point = cumulative_line[-1]
        start_elev,end_elev = [i[0] for i in rasterio.sample.sample_gen(dem,[start_point,end_point])]
        slope = float(abs(start_elev - end_elev) / splitLineString.length)
        if slope < slope_min:
            slope = slope_min
        slopes = slopes + [slope]

    split_flows_gdf = gpd.GeoDataFrame({'S0' : slopes ,'geometry':split_flows}, crs=flows.crs, geometry='geometry')
    split_flows_gdf['LengthKm'] = split_flows_gdf.geometry.length * toMetersConversion
    if lakes is not None:
        split_flows_gdf = gpd.sjoin(split_flows_gdf, lakes_buffer, how='left', op='within') #options: intersects, within, contains, crosses
        split_flows_gdf = split_flows_gdf.rename(columns={"index_right": "LakeID"}).fillna(-999)
    else:
        split_flows_gdf['LakeID'] = -999

    # need to figure out why so many duplicate stream segments for 04010101 FR
    split_flows_gdf = split_flows_gdf.drop_duplicates()

    # Create Ids and Network Traversal Columns
    addattributes = build_stream_traversal.build_stream_traversal_columns()
    tResults=None
    tResults = addattributes.execute(split_flows_gdf, wbd8, hydro_id)
    if tResults[0] == 'OK':
        split_flows_gdf = tResults[1]
    else:
        print ('Error: Could not add network attributes to stream segments')

    # remove single node segments
    split_flows_gdf = split_flows_gdf.query("From_Node != To_Node")

    # Get all vertices
    split_points = OrderedDict()
    for index, segment in split_flows_gdf.iterrows():
        lineString = segment.geometry

        for point in zip(*lineString.coords.xy):
            if point in split_points:
                if segment.NextDownID == split_points[point]:
                    pass
                else:
                    split_points[point] = segment[hydro_id]
            else:
                split_points[point] = segment[hydro_id]

    hydroIDs_points = [hidp for hidp in split_points.values()]
    split_points = [Point(*point) for point in split_points]

    split_points_gdf = gpd.GeoDataFrame({'id': hydroIDs_points , 'geometry':split_points}, crs=flows.crs, geometry='geometry')

    print('Writing outputs ...')

    if isfile(split_flows_filename):
        remove(split_flows_filename)
    if isfile(split_points_filename):
        remove(split_points_filename)

    if (len(split_flows_gdf) == 0):
        if (drop_stream_orders):
            # this is not an exception, but a custom exit code that can be trapped
            print("There are no flowlines after stream order filtering.")
            sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # will send a 61 back
        else:
            # if we are not dropping stream orders, then something is wrong
            raise Exception("No flowlines exist.")
    split_flows_gdf.to_file(split_flows_filename,driver=getDriver(split_flows_filename),index=False)

    if len(split_points_gdf) == 0:
        raise Exception("No points exist.")
    split_points_gdf.to_file(split_points_filename,driver=getDriver(split_points_filename),index=False)


if __name__ == '__main__':
    max_length             = float(environ['max_split_distance_meters'])
    slope_min              = float(environ['slope_min'])
    lakes_buffer_input     = float(environ['lakes_buffer_dist_meters'])

    # Parse arguments.
    parser = argparse.ArgumentParser(description='splitflows.py')
    parser.add_argument('-f', '--flows-filename', help='flows-filename',required=True)
    parser.add_argument('-d', '--dem-filename', help='dem-filename',required=True)
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename',required=True)
    parser.add_argument('-p', '--split-points-filename', help='split-points-filename',required=True)
    parser.add_argument('-w', '--wbd8-clp-filename', help='wbd8-clp-filename',required=True)
    parser.add_argument('-l', '--lakes-filename', help='lakes-filename',required=True)
    parser.add_argument('-n', '--nwm-streams-filename', help='nwm-streams-filename',required=True)
    parser.add_argument('-ds', '--drop-stream-orders', help='Drop stream orders 1 and 2', type=int, required=False, default=False)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    split_flows(max_length, slope_min, lakes_buffer_input, **args)
