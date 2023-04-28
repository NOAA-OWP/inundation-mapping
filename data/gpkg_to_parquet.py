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

# How the PostgreSQL database was loaded previously:

# "Populate PostgreSQL database with benchmark FIM extent points and HUC attributes (the calibration database)"
# ogr2ogr -overwrite -nln hucs -t_srs $DEFAULT_FIM_PROJECTION_CRS -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=$CALIBRATION_DB_NAME user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $inputsDir/wbd/WBD_National.gpkg WBDHU8

# fim_obs_pnt_data="/data/inputs/rating_curve/water_edge_database/usgs_nws_benchmark_points_cleaned.gpkg"

# "Loading Point Data"
# ogr2ogr -overwrite -t_srs $DEFAULT_FIM_PROJECTION_CRS -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=$CALIBRATION_DB_NAME user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $fim_obs_pnt_data usgs_nws_benchmark_points -nln points

###############################################################################################

# Tried running this ogr2ogr command from inside a running Docker container, in the /outputs/rob_build_4_3_9_0-test directory  

# ogr2ogr -overwrite -nln hucs -t_srs EPSG:5070 -f Parquet WBD_National.gpkg WBDHU8

# Fails since the Parquet driver is only supported in GDAL v 3.5 (GeoParquet beta release 1.0 in GDAL v3.6)
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


###############################################################################################
# PSEUDOCODE OVERVIEW
# Path Forward
###############################################################################################

# Load huc8 boundaries into GeoDataFrame
# WBDHU8_WBD_National_df = gpd.read_file(WBD_National_data_file, layer="WBDHU8")

# Load points layer (usgs_nws_benchmark_points) into GeoDataFrame
# fim_obs_pnt_df = gpd.read_file(fim_obs_pnt_data_file, layer="usgs_nws_benchmark_points")

# GeoDataFrame Spatial Join (left inner join)


# Create a GeoDataFrame for each HUC with all of the points associated with it


# Save the GeoDataFrame to a geoparquet file with all of the points for each huc 




























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
