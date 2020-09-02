#!/usr/bin/env python3

'''
Script objectives:
    1) split stream segments based on lake boundaries (defined with ID to avoid in croswalk)
    2) split stream segments based on threshold distance
    3) calculate channel slope, manning's n, and LengthKm, and Waterbody value
    4) create unique ids (ideally globally)
    5) create vector points encoded with HydroID's
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
from os import remove
from collections import OrderedDict
import buildstreamtraversal

flows_fileName         = sys.argv[1] # $outputDataDir/demDerived_reaches.gpkg
dem_fileName           = sys.argv[2] # $outputDataDir/dem_thalwegCond.tif
split_flows_fileName   = sys.argv[3] # $outputDataDir/demDerived_reaches_split.gpkg 
split_points_fileName  = sys.argv[4] # $outputDataDir/demDerived_reaches_split_points.gpkg
maxLength              = float(sys.argv[5])
slope_min              = float(sys.argv[6])
huc8_filename          = sys.argv[7] # $outputDataDir/wbd8_projected.gpkg
lakes_filename         = sys.argv[8] # $outputDataDir/nwm_lakes_proj_clp.gpkg

toMetersConversion = 1e-3

print('Loading data ...')
flows = gpd.read_file(flows_fileName)
WBD8 = gpd.read_file(huc8_filename)
#dem = Raster(dem_fileName)
dem = rasterio.open(dem_fileName,'r')
if isfile(lakes_filename):
    lakes = gpd.read_file(lakes_filename)
else:
    lakes = None

WBD8 = WBD8.filter(items=['fossid', 'geometry'])
WBD8 = WBD8.set_index('fossid')
flows = flows.explode()

# temp
flows = flows.to_crs(WBD8.crs)

split_flows = []
slopes = []
HYDROID = 'HydroID'
split_endpoints = OrderedDict()
# check for lake features
if lakes is not None:
    if len(lakes) > 0:
      print ('splitting stream segments at ' + str(len(lakes)) + ' waterbodies')
      #create splits at lake boundaries
      lakes = lakes.filter(items=['newID', 'geometry'])
      lakes = lakes.set_index('newID')
      flows = gpd.overlay(flows, lakes, how='union').explode().reset_index(drop=True)

print ('splitting ' + str(len(flows)) + ' stream segments based on ' + str(maxLength) + ' m max length')

# remove empty geometries
flows = flows.loc[~flows.is_empty,:]

for i,lineString in tqdm(enumerate(flows.geometry),total=len(flows.geometry)):
    # Reverse geometry order (necessary for BurnLines)
    lineString = LineString(lineString.coords[::-1])

    # skip lines of zero length
    if lineString.length == 0:
        continue

    # existing reaches of less than maxLength
    if lineString.length < maxLength:
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

    splitLength = lineString.length / np.ceil(lineString.length / maxLength)

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
    split_flows_gdf = gpd.sjoin(split_flows_gdf, lakes, how='left', op='within')
else:
    split_flows_gdf['LakeID'] = -999
split_flows_gdf = split_flows_gdf.rename(columns={"index_right": "LakeID"}).fillna(-999)

# Create Ids and Network Traversal Columns
addattributes = buildstreamtraversal.BuildStreamTraversalColumns()
tResults=None
tResults = addattributes.execute(split_flows_gdf, WBD8, HYDROID)
if tResults[0] == 'OK':
    split_flows_gdf = tResults[1]
else:
    print ('Error: Could not add network attributes to stream segments')

# Get Outlet Point Only
#outlet = OrderedDict()
#for i,segment in split_flows_gdf.iterrows():
#    outlet[segment.geometry.coords[-1]] = segment[HYDROID]

#hydroIDs_points = [hidp for hidp in outlet.values()]
#split_points = [Point(*point) for point in outlet]

# Get all vertices
split_points = OrderedDict()
for row in split_flows_gdf[['geometry',HYDROID, 'NextDownID']].iterrows():
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
split_flows_gdf.to_file(split_flows_fileName,driver='GPKG',index=False)

if isfile(split_points_fileName):
    remove(split_points_fileName)
split_points_gdf.to_file(split_points_fileName,driver='GPKG',index=False)


# def findIntersectionPoints(flows):
#
#     line_points = np.array([],dtype=np.object)
#     for i,g in enumerate(flows.geometry):
#
#         g_points = set((x,y) for x,y in zip(*g.coords.xy))
#         line_points = np.append(line_points,g_points)
#
#     intersectionPoints = set()
#     for i,g in tqdm(enumerate(flows.geometry),total=len(flows.geometry)):
#         boolean_of_lines_that_intersect_with_g = flows.geometry.intersects(g)
#         boolean_of_lines_that_intersect_with_g[i] = False
#
#         if sum(boolean_of_lines_that_intersect_with_g) <= 1:
#             continue
#
#         lines_that_intersect_with_g = flows.geometry[boolean_of_lines_that_intersect_with_g]
#         # print(list(boolean_of_lines_that_intersect_with_g))
#         line_points_that_intersect_with_g = line_points[boolean_of_lines_that_intersect_with_g]
#
#         g_points = line_points[i]
#
#         for ii,gg in enumerate(line_points_that_intersect_with_g):
#
#             for iii,ggg in enumerate(gg):
#                     if ggg in g_points:
#                         intersectionPoints.add(ggg)
#
#
#         # g_points = [(x,y) for x,y in zip(*g.coords.xy)]
#         # for line in lines_that_intersect_with_g:
#             # line_points = set((x,y) for x,y in zip(*line.coords.xy))
#             # print(line_points);exit()
#
#         # convert to point geometries
#         intersectionPoints_geometries = np.array([Point(*ip) for ip in intersectionPoints],dtype=np.object)
#
#
#     return(intersectionPoints,intersectionPoints_geometries)
