#!/usr/bin/env python3

import os
import time
import datetime as dt
import pandas as pd
import geopandas as gpd
import numpy as np
from dotenv import load_dotenv
import argparse


import pyarrow as pa
import pyarrow.parquet as pq

def find_points_in_huc(wbd_df:gpd.GeoDataFrame, points:gpd.GeoDataFrame):
    huc_gdf = []


    return huc_gdf


def create_geoparquet_directory(*args):
    print("Call to create_geoparquet_directory \n", )
    for arg in args:
        print(arg, "\n")
    return

def create_geoparquet_file(*args):
    print("\n Call to create_geoparquet_file \n", )
    for arg in args:
        print(arg, "\n")
    return

if __name__ == '__main__':

    # Sample Usage:  python3 /foss_fim/data/create_calibration_points_geoparquet.py                              \
    #                    -p /data/inputs/rating_curve/water_edge_database/usgs_nws_benchmark_points_cleaned.gpkg \
    #                    -wbd /data/inputs/wbd/WBD_National.gpkg                                                 \
    #                    -h "12040103"                                                                           \
    #                    -o /data/inputs/rating_curve/calibration_points/

    parser = argparse.ArgumentParser(description='Create a geoparquet file/files with calibration points')
    
    parser.add_argument('-p','--points_data_file_name', help='Name of the .gpkg file (complete relative path) to gather calibration poitns from. ', \
                        required=True)

    parser.add_argument('-wbd','--wbd_layer', help='A directory of where the .tif files '\
                        'files exist. If the -f (tif-file) param is empty then all .tif files '\
                        'in this directory will be used.', 
                        required=False)
    
    parser.add_argument('-u','--huc_number', help='HUC number (with updated points in .gpkg file) ', \
                        required=False)
    
    parser.add_argument('-o', '--output_dir', help='Optional path to send geoparquet file/files ', \
                        required=False)
    
    args = vars(parser.parse_args())

    # Finalize Variables
    p = args['points_data_file_name']
    wbd = args['wbd_layer']
    u = args['huc_number']
    o = args['output_dir']


    # Account for optional arguments
    create_geoparquet_file(p, wbd, u, o)

    # create_geoparquet_directory(*args)