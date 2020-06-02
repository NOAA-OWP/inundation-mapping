#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, MultiPoint
from raster import Raster
import numpy as np
import argparse
import sys
from tqdm import tqdm
import time
from os.path import isfile
from os import remove
from collections import OrderedDict

flows_fileName = sys.argv[1]
projection = sys.argv[2]
dem_fileName = sys.argv[3]
split_flows_fileName = sys.argv[4]
split_points_fileName = sys.argv[5]
maxLength = float(sys.argv[6])
manning = float(sys.argv[7])
slope_min = float(sys.argv[8])

# maxLength = 2000
toMetersConversion = 1e-3
# manning = 0.06
# slope_min = 0.001

print('Loading data ...')
flows = gpd.read_file(flows_fileName)
dem = Raster(dem_fileName)

flows.to_crs(projection)
flows = flows.explode()

split_flows = []
split_points = OrderedDict()
# all_link_no = []
LengthKm = []
hydroIDs_flows = []
slopes = []
hydroID_count = 1

for i,lineString in tqdm(enumerate(flows.geometry),total=len(flows.geometry)):

    if lineString.length < maxLength:
        lineStringList = [lineString]

        split_flows = split_flows + lineStringList
        LengthKm = LengthKm + [float(lineString.length * toMetersConversion)]
        # all_link_no = all_link_no + [linkNO]
        hydroIDs_flows = hydroIDs_flows + [hydroID_count]

        line_points = [point for point in zip(*lineString.coords.xy)]

        for point in line_points:
            split_points[point] = hydroID_count

        hydroID_count += 1

        start_point = line_points[0]; end_point = line_points[-1]

        start_elev = dem.sampleFromCoordinates(*start_point,returns='value')
        end_elev = dem.sampleFromCoordinates(*end_point,returns='value')
        slope = float(abs(start_elev - end_elev) / lineString.length)

        if slope < slope_min:
            slope = slope_min
        slopes = slopes + [slope]

        continue

    splitLength = lineString.length / np.ceil(lineString.length / maxLength)

    cumulative_line = []
    line_points = []
    last_point = []

    # linkNO = flows['LINKNO'][i]

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
            LengthKm = LengthKm + [float(splitLineString.length * toMetersConversion)]
            # all_link_no = all_link_no + [linkNO]
            hydroIDs_flows = hydroIDs_flows + [hydroID_count]

            for point in cumulative_line:
                split_points[point] = hydroID_count

            hydroID_count += 1

            start_point = cumulative_line[0]; end_point = cumulative_line[-1]

            start_elev = dem.sampleFromCoordinates(*start_point,returns='value')
            end_elev = dem.sampleFromCoordinates(*end_point,returns='value')
            slope = float(abs(start_elev - end_elev) / splitLineString.length)

            if slope < slope_min:
                slope = slope_min

            slopes = slopes + [slope]

            last_point = end_point

            cumulative_line = []
            line_points = []

    splitLineString = LineString(cumulative_line)
    split_flows = split_flows + [splitLineString]
    LengthKm = LengthKm + [float(splitLineString.length * toMetersConversion)]
    # all_link_no = all_link_no + [linkNO]
    hydroIDs_flows = hydroIDs_flows + [hydroID_count]

    for point in cumulative_line:
        split_points[point] = hydroID_count

    start_point = cumulative_line[0]; end_point = cumulative_line[-1]

    start_elev = dem.sampleFromCoordinates(*start_point,returns='value')
    end_elev = dem.sampleFromCoordinates(*end_point,returns='value')
    slope = float(abs(start_elev - end_elev) / splitLineString.length)

    if slope < slope_min:
        slope = slope_min
    slopes = slopes + [slope]

    hydroID_count += 1

hydroIDs_points = [hidp for hidp in split_points.values()]
split_points = [Point(*point) for point in split_points]

split_flows_gdf = gpd.GeoDataFrame({'HydroID' : hydroIDs_flows , 'LengthKm' : LengthKm , 'ManningN' : [manning] * len(split_flows) ,
                                    'S0' : slopes ,'geometry':split_flows}, crs=flows.crs, geometry='geometry')
split_points_gdf = gpd.GeoDataFrame({'id': hydroIDs_points , 'geometry':split_points}, crs=flows.crs, geometry='geometry')
# print(split_flows_gdf)
# print(split_flows_gdf.iloc[0,:])
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

# def findHeadWaterPoints(flows):
#
#     headwater_points = np.array([],dtype=np.object)
#     starting_points = set() ; end_points = set()
#     for i,g in enumerate(flows.geometry):
#
#         g_points = [(x,y) for x,y in zip(*g.coords.xy)]
#
#         starting_point = g_points[0]
#         end_point = g_points[-1]
#
#         starting_points.add(starting_point)
#         end_points.add(end_point)
#
#         # line_points = np.append(line_points,g_points)
#
#     for i,sp in enumerate(starting_points):
#         if sp not in end_points:
#             headwater_points = np.append(headwater_points,sp)
#
#     print(headwater_points)
#     headwater_points_geometries = np.array([Point(*hwp) for hwp in headwater_points],dtype=np.object)
#
#     return(headwater_points,headwater_points_geometries)

# points,point_geometries = findIntersectionPoints(flows)
# points_gdf = gpd.GeoDataFrame(gpd.GeoSeries(point_geometries,crs=flows.crs,name='geometry'),crs=flows.crs)

# print(points_gdf)
# print(len(points_gdf))

# points_gdf.to_file('test.gpkg',driver='GPKG')

# hw_points, hw_geom = findHeadWaterPoints(flows)
# print(hw_geom)
#
# hw_gdf = gpd.GeoDataFrame(gpd.GeoSeries(hw_geom,crs=flows.crs,name='geometry'),crs=flows.crs)
# hw_gdf.to_file('hw_test.gpkg',driver='GPKG')
