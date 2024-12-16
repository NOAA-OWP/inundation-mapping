#!/usr/bin/env python3

import os
from os import listdir
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, Point
from shapely.geometry.polygon import Polygon
import rasterio
import numpy as np


# -----
# Inputs
lid = 'bltn7'
huc = '06010105'
catfim_inputs_path = '/data/previous_fim/fim_4_5_2_11'
catfim_outputs_path = '/data/catfim/emily_test/hand_4_5_11_1_catfim_datavis_flow_based/' # this also specifies flow- vs stage-based?
plot = True # True or False

dem_path = 'data/inputs/3dep_dems/10m_5070/HUC6_060101_dem.tif' # TODO: make this filepath automatically generated?  

# -----
# Processing

# Read in DEM
dem_raster = rasterio.open(dem_path, 'r')

# Read in HAND output flowlines
flowline_path = os.path.join(catfim_inputs_path, huc,'nwm_subset_streams_levelPaths_dissolved.gpkg')
flowline_gdf = gpd.read_file(flowline_path)

# Read in CatFIM outputs
catfim_outputs_mapping_path = os.path.join(catfim_outputs_path, 'mapping')
points_path = os.path.join(catfim_outputs_mapping_path, 'flow_based_catfim_sites.gpkg')
points_gdf = gpd.read_file(points_path)

print('HAND-FIM and CatFIM outputs have been read in.')

# temp debug print column names
print('flowlines columns:')
print(flowline_gdf.columns)
print()
print('points columns:')
print(points_gdf.columns)


for file in os.listdir(catfim_outputs_mapping_path):
    if file.endswith('catfim_library.gpkg'):
        catfim_library_path = os.path.join(catfim_outputs_path, 'mapping', file)
    elif file.endswith('catfim_sites.gpkg'):
        catfim_points_path = os.path.join(catfim_outputs_mapping_path, 'flow_based_catfim_sites.gpkg')

try:
    catfim_library = gpd.read_file(catfim_library_path)
    catfim_points = gpd.read_file(catfim_points_path)

except IOError:
    print(f'Error opening CatFIM outputs from {catfim_outputs_path}:')
    print(IOError)
    sys.exit()

# Filter points to LID
points_filt_gdf = points_gdf[points_gdf['ahps_lid'] == lid] 
	
if len(points_filt_gdf) > 1:
    print(f'ERROR: Multiple points found for lid {lid}.')
    sys.exit()

# Put the point into the projection of the flowlines
points_filt_gdf = points_filt_gdf.to_crs(flowline_gdf.crs) 

# Find the flowline nearest to the point 
flowline_filt_gdf = gpd.sjoin_nearest(points_filt_gdf, flowline_gdf, max_distance=100)

print('flowline_filt_gdf') ## DEBUG
print(flowline_filt_gdf.geometry) ## DEBUG

flowline_filt_gdf.plot()

# Create cross-section of flowline

# Extract the line and the point
line = flowline_filt_gdf.geometry.iloc[0]
point = points_filt_gdf.geometry.iloc[0]


# Find the segment of the line near the point of interest
segment_length = 5 # meters?
segment_start_distance = max(0, line.project(point) - segment_length / 2)
segment_end_distance = min(line.length, segment_start_distance + segment_length)

# Create a shorter segment from the original line
short_segment = LineString([
    line.interpolate(segment_start_distance),
    line.interpolate(segment_end_distance)
])

# Calculate the slope of the shorter line segment
line_vector = np.array(short_segment.xy[1]) - np.array(short_segment.xy[0])
line_vector /= np.linalg.norm(line_vector)  # Normalize
perpendicular_vector = np.array([-line_vector[1], line_vector[0]])  # Perpendicular vector

# Create the cross section line (10 meters long)
length = 10
half_length = length / 2
start_point = (point.x + half_length * perpendicular_vector[0], point.y + half_length * perpendicular_vector[1])
end_point = (point.x - half_length * perpendicular_vector[0], point.y - half_length * perpendicular_vector[1])
new_line = LineString([start_point, end_point])

# Create a GeoDataFrame for the new line
new_line_gdf = gpd.GeoDataFrame({'geometry': [new_line]})



print('new_line_gdf') ## TEMP DEBUG
print(new_line_gdf) ## TEMP DEBUG


# # 

# # Convert cross-section to points
# point_gdf =

# # Get DEM value for each point

# point_gdf['dem_val'] # make dem_val column in point_gdf?

# for point in point_gdf['geometry']:
#   x = point.xy[0][0]
#   y = point.xy[1][0]
#   row, col = dem_raster.index(x,y)
# 	point_gdf['dem_val'] = dem_raster.read(1)[row,col]

# # Get CatFIM classification for each point (True/False for each stage value)
# for magnitude in list_magnitudes:
# 	polygon = catfim_library[magnitude == magnitude]
	
	
# 	# make column for magnitude? 
	
# 	for point in point_gdf: 
		
# 		point_gdf[magnitude] = polygon.contains(point)
# # Plot stage value
# if plot == True:
# # plot stage value with the CatFIM data
