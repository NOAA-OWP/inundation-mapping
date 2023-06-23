#!/usr/bin/env python3

import os
import time
import datetime as dt
import pandas as pd
import geopandas as gpd
import numpy as np
import argparse

from dotenv import load_dotenv
from multiprocessing import Pool


import pyarrow as pa
import pyarrow.parquet as pq

###############################################################################################
# Overview:
# Read two .gpkg files into two separate GeoDataFrames
# Create a new GeoDataFrame per HUC8 with all the point data
# Write each new GeoDataFrame with HUC id and calibration points to a Parquet/GeoParquet file
###############################################################################################


def load_WBD_gpkg_into_GDF(WBD_National_gpkg_file):
    huc_polygons_df = gpd.read_file(WBD_National_gpkg_file, layer='WBDHU8', ignore_fields=['tnmid', 'metasourceid', 'sourcedatadesc', 'sourceoriginator',
                                                                                        'sourcefeatureid', 'loaddate', 'referencegnis_ids', 'areaacres',
                                                                                        'areasqkm', 'states', 'name', 'globalid', 'shape_Length',
                                                                                        'shape_Area', 'fimid', 'fossid'])

    return huc_polygons_df


def load_fim_obs_points_into_GDF(fim_obs_points_data_file):
    fim_obs_point_df = gpd.read_file(fim_obs_points_data_file, layer='usgs_nws_benchmark_points', ignore_fields=['Join_Count', 'TARGET_FID', 'DN', 'ORIG_FID',
                                                                                                                'ID', 'AreaSqKM', 'Shape_Leng', 'Shape_Area'] )

    return fim_obs_point_df


def create_single_huc_gdf_and_write_parquet_file(huc, output_dir, wbd_GDF:gpd.GeoDataFrame, pnt_GDF:gpd.GeoDataFrame):
    
    one_huc = wbd_GDF.loc[wbd_GDF['HUC8'] == huc]
    
    huc_with_points_gdf = pnt_GDF.sjoin(one_huc, how='inner', predicate='within')
    
    if len(huc_with_points_gdf) == 0: 
        print(f'Huc {huc} does not contain any calibration points, skipping {huc}')
        return 
    
    huc_with_points_gdf.drop(['index_right'], axis=1, inplace=True)
    
    parquet_filepath = os.path.join(output_dir, huc, '.parquet')
    
    huc_with_points_gdf.to_parquet(parquet_filepath, index=False)
    
    print(f'Done writing {huc}.parquet to {output_dir}')



def create_parquet_directory(output_dir):
    if os.path.isdir(output_dir) == False:
        os.mkdir(output_dir)
        print(f"Created directory: {output_dir}, .parquet files will be written there.")
    elif os.path.isdir(output_dir) == True:
        print(f"{output_dir} exists, .parquet files will be written there.")


def create_parquet_files(points_data_file_name,
                            wbd_layer,
                            output_dir,
                            number_of_jobs,
                            huc_list=None,
                            all_hucs=False):
    
    # Validation
    total_cpus_available = os.cpu_count() -1
    
    if number_of_jobs > total_cpus_available:
        raise ValueError(f'Provided: -j {number_of_jobs}, which is greater than than amount of available cpus -1: {total_cpus_available}, ' \
                        'please provide a lower value to -j')
    
    # Print start time
    start_time = dt.datetime.now()
    dt_string = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print (f"Started: {dt_string}")
    
    create_parquet_directory(output_dir)
    
    # Load .gpkg files into GeoDataFrames
    huc_polygons_df= load_WBD_gpkg_into_GDF(wbd_layer)
    fim_obs_point_df= load_fim_obs_points_into_GDF(points_data_file_name)
    
    # Set the list of arguments to each process for parallelization
    procs_list = []
    
    # Define the default list of HUCS based on all
    all_hucs_in_WBD = huc_polygons_df.iloc[:,0]
    
    if huc_list is not None:
        hucs_to_parquet_list = list(huc_list)
    elif bool(all_hucs):
        hucs_to_parquet_list = list(all_hucs_in_WBD)
    else:
        hucs_to_parquet_list = os.listdir(output_dir)
    
    # Build arguments (procs_list) to pass to create_single_huc_gdf_and_write_parquet_file
    for huc in hucs_to_parquet_list:
        procs_list.append([huc, output_dir, huc_polygons_df, fim_obs_point_df])
    
    # Parallelize each huc in hucs_to_parquet_list
    with Pool(processes=number_of_jobs) as pool:
            pool.map(create_single_huc_gdf_and_write_parquet_file, procs_list)
    
    # Get time metrics
    end_time = dt.datetime.now()
    dt_string = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print (f"Ended: {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    print(f"Completed writing all .parquet files \n" \
        f"TOTAL RUN TIME: {str(time_duration).split('.')[0]}")


if __name__ == '__main__':
    '''
    Sample Usage:  python3 /foss_fim/data/create_calibration_points_geoparquet.py                              \
                        -p /data/inputs/rating_curve/water_edge_database/usgs_nws_benchmark_points_cleaned.gpkg \
                        -wbd /data/inputs/wbd/WBD_National.gpkg                                                 \
                        -u "12040103, 01010004"                                                                           \
                        -o /data/inputs/rating_curve/calibration_points/                                        \ 
                        -j 6
                        -nh true
    '''

    parser = argparse.ArgumentParser(description='Create a geoparquet file/files with calibration points')
    
    parser.add_argument('-p','--points_data_file_name', help='REQUIRED: Complete relative filepath of a .gpkg file with fim calibration points.', 
                        required=True)

    parser.add_argument('-wbd','--wbd_layer', help='REQUIRED: A directory of where a .gpkg file exists, containing HUC boundary polygons', 
                        required=False, default=f'/data/inputs/wbd/WBD_National.gpkg')
        
    parser.add_argument('-o', '--output_dir', help='OPTIONAL: path to send .parquet file/files ', required=False)
    
    parser.add_argument('-j','--number_of_jobs', help='OPTIONAL: number of cores/processes (default=6)', required=False, default=6, type=int)
    
    parser.add_argument('-u','--huc_list', help='OPTIONAL: HUC list (with updated points in .gpkg file)', required=False, default=None)
    
    parser.add_argument('-a', '--all_hucs', help='OPTIONAL: Provide a value of <true> if new calibration points were added to a HUC which currently' \
        'doesn\'t have a .parquet file. All HUC polygons in the provided <wbd_layer file> will be checked for calibration points in the <points_data_file_name>.' , required=False, default=False)
    
    args = vars(parser.parse_args())


    create_parquet_files(**args)
