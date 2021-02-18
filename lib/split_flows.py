#!/usr/bin/env python3

'''
Description:
    1) split stream segments based on lake boundaries and input threshold distance
    2) calculate channel slope, manning's n, and LengthKm for each segment
    3) create unique ids using HUC8 boundaries (and unique FIM_ID column)
    4) create network traversal attribute columns (To_Node, From_Node, NextDownID)
    5) create points layer with segment verticies encoded with HydroID's (used for catchment delineation in next step)
'''

import sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, MultiPoint
import rasterio
import numpy as np
import argparse
from tqdm import tqdm
import time
from os.path import isfile
from os import remove,environ
from collections import OrderedDict
import build_stream_traversal
from utils.shared_functions import getDriver
from utils.shared_variables import FIM_ID

flows_fileName         = sys.argv[1]
dem_fileName           = sys.argv[2]
split_flows_fileName   = sys.argv[3]
split_points_fileName  = sys.argv[4]
wbd8_clp_filename      = sys.argv[5]
lakes_filename         = sys.argv[6]
max_length             = float(environ['max_split_distance_meters'])
slope_min              = float(environ['slope_min'])
lakes_buffer_input     = float(environ['lakes_buffer_dist_meters'])

wbd = gpd.read_file(wbd8_clp_filename)

toMetersConversion = 1e-3

print('Loading data ...')
flows = gpd.read_file(flows_fileName)

if not len(flows) > 0:
    print ("No relevant streams within HUC boundaries.")
    sys.exit(0)

wbd8 = gpd.read_file(wbd8_clp_filename)
dem = rasterio.open(dem_fileName,'r')

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

# Get Outlet Point Only
#outlet = OrderedDict()
#for i,segment in split_flows_gdf.iterrows():
#    outlet[segment.geometry.coords[-1]] = segment[hydro_id]

#hydroIDs_points = [hidp for hidp in outlet.values()]
#split_points = [Point(*point) for point in outlet]

# Get all vertices
split_points = OrderedDict()
for row in split_flows_gdf[['geometry',hydro_id, 'NextDownID']].iterrows():
    lineString = row[1][0]

    for point in zip(*lineString.coords.xy):
        if point in split_points:
            if row[1][2] == split_points[point]:
                pass
            else:
                split_points[point] = row[1][1]
        else:
            split_points[point] = row[1][1]

hydroIDs_points = [hidp for hidp in split_points.values()]
split_points = [Point(*point) for point in split_points]

split_points_gdf = gpd.GeoDataFrame({'id': hydroIDs_points , 'geometry':split_points}, crs=flows.crs, geometry='geometry')

print('Writing outputs ...')

if isfile(split_flows_fileName):
    remove(split_flows_fileName)
split_flows_gdf.to_file(split_flows_fileName,driver=getDriver(split_flows_fileName),index=False)

if isfile(split_points_fileName):
    remove(split_points_fileName)
split_points_gdf.to_file(split_points_fileName,driver=getDriver(split_points_fileName),index=False)
