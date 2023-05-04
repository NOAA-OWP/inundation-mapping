#!/usr/bin/env python3

import os
import time
import datetime as dt
import pandas as pd
import geopandas as gpd
import numpy as np
from dotenv import load_dotenv


import pyarrow as pa
import pyarrow.parquet as pq

###############################################################################################
# Overview:
# Read two .gpkg files into two GeoDataFrames
# Create a new GeoDataFrame per HUC8 with all the point data
# Write each HUC8 w/points GeoDataFrame to a GeoParquet file
###############################################################################################

load_dotenv()
outputsDir = os.getenv("outputsDir")
inputsDir = os.getenv("inputsDir")


# cp /efs-drives/fim-dev-team-efs/fim-data/inputs/rating_curve/water_edge_database/usgs_nws_benchmark_points_cleaned.gpkg /home/rdp-user/outputs/rob_build_4_3_9_0-test/usgs_nws_benchmark_points_cleaned.gpkg
fim_obs_pnt_data_file= f"{outputsDir}/rob_build_4_3_9_0-test/usgs_nws_benchmark_points_cleaned.gpkg"

# cp /efs-drives/fim-dev-team-efs/fim-data/inputs/wbd/WBD_National.gpkg /home/rdp-user/outputs/rob_build_4_3_9_0-test/WBD_National.gpkg 
WBD_National_data_file = f'{outputsDir}/rob_build_4_3_9_0-test/WBD_National.gpkg'

# Use EPSG:5070 instead of the default ESRI:102039 (gdal pyproj throws an error with crs 102039)
# Appears that EPSG:5070 is functionally equivalent to ESRI:102039: https://gis.stackexchange.com/questions/329123/crs-interpretation-in-qgis
DEFAULT_FIM_PROJECTION_CRS = "EPSG:5070"


###############################################################################################
# PSEUDOCODE OVERVIEW - Path Forward


# Load huc8 boundaries into GeoDataFrame , ignoring irrevelant fidlds
    # include_fields=['HUC8', 'geometry']
    # WBD_National_df = gpd.read_file(WBD_National_data_file, layer='WBDHU8', ignore_fields=['tnmid', 'metasourceid', 'sourcedatadesc', 'sourceoriginator',
    #                                                                                         'sourcefeatureid', 'loaddate', 'referencegnis_ids', 'areaacres',
    #                                                                                         'areasqkm', 'states', 'name', 'globalid', 'shape_Length',
    #                                                                                         'shape_Area', 'fimid', 'fossid'])

# Load points layer (usgs_nws_benchmark_points) into GeoDataFrame , ignoring irrelevant fields
    # include_fields=['flow', 'magnitude', 'submitter', 'coll_time', 'flow_unit', 'layer', 'path', 'geometry']
    # fim_obs_pnt_df = gpd.read_file(fim_obs_pnt_data_file, layer='usgs_nws_benchmark_points', ignore_fields=['Join_Count', 'TARGET_FID',
    #                                                                                                         'DN', 'ORIG_FID', 'ID', 'AreaSqKM', 'Shape_Leng', 'Shape_Area'] )


#TODO Find HUCS with points?
# hucs_with_points = df .....


# Iterate over the HUCS with points
# for row in hucs_with_points.iterrows()
#   

    # Create a new GeoDataFrame for each huc boundary (set the bbox)

    # Populate the geometry column with points 

    # GeoDataFrame Spatial Join (left inner join) (from this join, the HUC8 column is added, and we can verify all points are same huc8)
        # x = points.sjoin(one_huc, how='inner', predicate='within')
    
    # Drop columns from join that aren't used/needed (only index_right)
        # drop_column_list = ['Join_Count', 'TARGET_FID', 'DN', 'ORIG_FID', 'ID', 'AreaSqKM', 'Shape_Leng', 'Shape_Area', 'index_right', 'tnmid', 'metasourceid', 'sourcedatadesc', 'sourceoriginator', 'sourcefeatureid', 'loaddate', 'referencegnis_ids', 'areaacres', 'areasqkm', 'states', 'name', 'globalid', 'shape_Length', 'shape_Area', 'fimid', 'fossid' ] # 'path'? 
        # <gdf>.drop(['index_right'], axis=1, inplace=True)

    # Save each HUC GeoDataFrame to a geoparquet file containing all of the points per huc 
        # huc_number =  points_and_huc_joined['HUC8'].values[0]
        # parquet_filepath = os.path.join('/home/outputs/usgs_nws_benchmark_points/' + huc_number + '.parquet')
        # points_and_huc_joined.to_parquet(parquet_filepath, index=False)


###############################################################################################
# Performance

# benchmark FIM extent points and HUC attributes
# def load_WBDHU8():
#     WBDHU8_WBD_National_df = gpd.read_file(WBD_National_data_file, layer="WBDHU8")
#     # return WBDHU8_WBD_National_df

# tstart = time.perf_counter()
# load_WBDHU8()
# tend = time.perf_counter()

# print("Time of importing WBD National into a gpd :", tend - tstart)

# FIM obs point data

# def load_benchmark_points():
#     fim_obs_pnt_df = gpd.read_file(fim_obs_pnt_data_file, layer="usgs_nws_benchmark_points")
#     # fim_obs_pnt_df = gpd.read_file(fim_obs_pnt_data_file, where="huc8=12040103") * needs fiona 1.9+
#     # return fim_obs_pnt_df

# tstart = time.perf_counter()
# load_benchmark_points()
# tend = time.perf_counter()

# print("Time of importing benchmark points into a gpd :", tend - tstart)


# #########################################################################################################

# READ PARQUET Using pyarrow library's parquet subpackage (pq)


# Whole file
# Output parquet file
# parquet_file = f'{outputsDir}/rob_build_4_3_9_0-test/12040103/waters_edge_df_12040103.parquet'

# parquet_data = pq.read_table(parquet_file)
# print("Parquet file read table \n", parquet_data)
# print("\n")
# metadata = pq.read_metadata(parquet_file)
# print("Parquet file metadata \n", metadata)
# print("\n")

# Only certain columns
# thee_columns_parquet_data = pq.read_table(parquet_file, columns=['geom', 'flow', 'submitter'])
# print("\n")
# print(thee_columns_parquet_data)
# print("\n")
