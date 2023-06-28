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

load_dotenv('/foss_fim/src/bash_variables.env')
# inputsDir = os.getenv('inputsDir')
# CALIBRATION_DB_PATH = os.getenv(f'{inputsDir}/rating_curve/water_edge_database/calibration_points')
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
input_WBD_gdb              = os.getenv('input_WBD_gdb')
input_calib_points_dir     = os.getenv('input_calib_points_dir')

###############################################################################################
# Overview:
#   Read two .gpkg files into two separate GeoDataFrames
#   Create a new GeoDataFrame per HUC8 with all the point data
#   Write each new GeoDataFrame with HUC id and calibration points to a Parquet/GeoParquet file
# Usage:
#   This script must be run in a Docker container with the correct volume mount to the /data
#   directory containing the input files, and output path.
#       eg: -v /efs-drives/fim-dev-efs/fim-data/:/data

###############################################################################################


def load_WBD_gpkg_into_GDF(WBD_National_gpkg_file):
    huc_polygons_df = gpd.read_file(WBD_National_gpkg_file, layer='WBDHU8',
                                    ignore_fields=['tnmid', 'metasourceid', 'sourcedatadesc', 'sourceoriginator',
                                                    'sourcefeatureid', 'loaddate', 'referencegnis_ids', 'areaacres',
                                                    'areasqkm', 'states', 'name', 'globalid', 'shape_Length',
                                                    'shape_Area', 'fimid', 'fossid'])

    return huc_polygons_df


def load_fim_obs_points_into_GDF(fim_obs_points_data_file):
    fim_obs_point_df = gpd.read_file(fim_obs_points_data_file, layer='usgs_nws_benchmark_points',
                                     ignore_fields=['Join_Count', 'TARGET_FID', 'DN', 'ORIG_FID',
                                                    'ID', 'AreaSqKM', 'Shape_Leng', 'Shape_Area'] )

    return fim_obs_point_df


def create_single_huc_gdf_and_write_parquet_file(args):
    # We have to explicitly unpack the args from pool.map()
    huc        = args[0]
    output_dir = args[1]
    wbd_GDF    = args[2]
    pnt_GDF    = args[3]
    
    one_huc = wbd_GDF.loc[wbd_GDF['HUC8'] == huc]

    huc_with_points_gdf = pnt_GDF.sjoin(one_huc, how='inner', predicate='within')
    
    # If there are no calibration points within the HUC boundary, skip current HUC
    if len(huc_with_points_gdf) == 0: 
        print(f'HUC # {huc} does not contain any calibration points, skipping...')
        return 
    
    # Drop index_right column created by join
    huc_with_points_gdf.drop(['index_right'], axis=1, inplace=True)
    
    # Set the crs projection
    huc_gdf = huc_with_points_gdf.set_crs(DEFAULT_FIM_PROJECTION_CRS, allow_override=True)

    # Set filepath and write file
    parquet_filepath = os.path.join(output_dir, f'{huc}.parquet')
    huc_gdf.to_parquet(parquet_filepath, index=False)
    
    print(f'HUC # {huc} calibartion points written to file: \n' \
        f' \t {output_dir}/{huc}.parquet')


def create_parquet_directory(output_dir):
    if os.path.isdir(output_dir) == False:
        os.mkdir(output_dir)
        print(f"Created directory: {output_dir}, .parquet files will be written there.")
    elif os.path.isdir(output_dir) == True:
        print(f"Output Direcrtory: {output_dir} exists, .parquet files will be written there.")


def create_parquet_files(points_data_file_name,
                            wbd_layer,
                            output_dir,
                            number_of_jobs,
                            huc_list=None,
                            all_hucs=False):
    
    # Validation
    total_cpus_available = os.cpu_count() -1
    
    if number_of_jobs > total_cpus_available:
        raise ValueError(f'Provided: -j {number_of_jobs}, which is greater than than amount of available cpus -1: ' \
                        f'{total_cpus_available}, please provide a lower value for -j')
    
    # Print start time
    start_time = dt.datetime.now()
    dt_string = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f'==========================================================================')
    print (f"\n write_parquet_from_calib_pts.py")
    print(f"\n \t Started: {dt_string} \n")
    
    create_parquet_directory(output_dir)
    
    print(f'Loading .gpkg files into GeoDataFrame....')
    huc_polygons_df  = load_WBD_gpkg_into_GDF(wbd_layer)
    fim_obs_point_df = load_fim_obs_points_into_GDF(points_data_file_name)
    
    # Verify same projection
    if (huc_polygons_df.crs == fim_obs_point_df.crs) is not True:
        raise ValueError(f'Provided: -p {points_data_file_name} crs & -wbd {wbd_layer} crs do not match, ' \
                        f'please make adjustments and re-run.')
    
    # Print timing
    load_gdf_end_time = dt.datetime.now()
    load_duration = load_gdf_end_time - start_time
    print(f"Finished loading input .gpkg files into GeoDataFrames.\n",
            f"\t TIME: {str(load_duration).split('.')[0]} \n")
    
    # Only use provided HUCs
    if huc_list is not None:
        hucs_to_parquet_list = list(huc_list.split(','))
        
    # Use all HUCS in WBD file 
    elif all_hucs:
        # Define the list of HUCS based on all HUCS in WBD File
        all_hucs_in_WBD = huc_polygons_df.iloc[:,0]
        hucs_to_parquet_list = list(all_hucs_in_WBD)
        
    # Default HUC list comes from calibration point .parquet files that are pre-existing in current /inputs location
    else:
        # Get a list of all files in output_dir
        current_hucs_in_output_dir = os.listdir(output_dir)
        # Slice off the .parquet file format to get just the HUC number of all files with the .parquet extension
        hucs_to_parquet_list = [i[:-8] for i in current_hucs_in_output_dir if i.endswith('.parquet')]
    
    # Build arguments (procs_list) for each process (create_single_huc_gdf_and_write_parquet_file)
    procs_list = []
    for huc in hucs_to_parquet_list:
        procs_list.append([huc, output_dir, huc_polygons_df, fim_obs_point_df])
    
    # Parallelize each huc in hucs_to_parquet_list
    print(f'Parallelizing HUC level GeoDataFrame creation and .parquet file writes')
    with Pool(processes=number_of_jobs) as pool:
            pool.map(create_single_huc_gdf_and_write_parquet_file, procs_list)
    
    # Get time metrics
    end_time = dt.datetime.now()
    dt_string = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print (f"\n Ended: {dt_string} \n")

    # Calculate duration
    time_duration = end_time - start_time
    print(f'==========================================================================')
    print(f"\t Completed writing all .parquet files \n" \
        f"\t \t TOTAL RUN TIME: {str(time_duration).split('.')[0]}")
    print(f'==========================================================================')


if __name__ == '__main__':
    '''
    Sample Usage:  python3 /foss_fim/data/write_parquet_from_calib_pts.py                                       \
                        -p /data/inputs/rating_curve/water_edge_database/usgs_nws_benchmark_points_cleaned.gpkg \
                        -wbd /data/inputs/wbd/WBD_National.gpkg                                                 \
                        -o /data/inputs/rating_curve/water_edge_database/calibration_points                     \
                        -j 6                                                                                    \
                        -u "12040103,04100003"                            **OR**                                \
                        -a
    '''

    parser = argparse.ArgumentParser(description='Create a parquet file/files with calibration points')
    
    parser.add_argument('-p','--points_data_file_name', help='REQUIRED: Complete relative filepath of a .gpkg file with fim calibration points.', 
                        type=str, required=True)

    parser.add_argument('-wbd','--wbd_layer', help='REQUIRED: A directory of where a .gpkg file exists, containing HUC boundary polygons', 
                        type=str, required=False, default=input_WBD_gdb)
    
    parser.add_argument('-o', '--output_dir', help='REQUIRED: path to send .parquet file/files ', type=str, required=True, default=input_calib_points_dir)
        
    parser.add_argument('-j','--number_of_jobs', help='OPTIONAL: number of cores/processes (default=6)', type=int, required=False, default=6)
    
    parser.add_argument('-u','--huc_list', help='OPTIONAL: HUC list - String with comma seperated HUC numbers. *DO NOT include spaces*. Provide certain HUCs if' \
                        'points were added/updated only within a few known HUCs.', type=str, required=False, default=None)
    
    parser.add_argument('-a', '--all_hucs', help='OPTIONAL: Provide -a flag if new calibration points were added to many HUCs which currently' \
        'do not have .parquet files.  All HUC polygons in the provided <wbd_layer file> will be checked for calibration points in the <points_data_file_name>.' \
        'If not provided, either the HUCs in the huc_list argument or the current HUCs with files in the output_dir will be used.', required=False, action='store_true')
    
    args = vars(parser.parse_args())


    create_parquet_files(**args)
