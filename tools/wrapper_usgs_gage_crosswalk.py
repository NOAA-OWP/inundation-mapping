#!/usr/bin/env python3

# Created: 1/11/2021
# Primary developer(s): ryan.spies@noaa.gov
# Purpose: This script provides the user to input a customized flow entry to produce
# inundation outputs using outputs from fim_run. Note that the flow csv must be
# formatted with "feature_id" & "discharge" columns. Flow must be in cubic m/s

import os
import argparse
import sys
import multiprocessing
from multiprocessing import Pool
  
# adding sys to the system path
sys.path.insert(0, '/foss_fim/src')
from usgs_gage_crosswalk import crosswalk_usgs_gage
from utils.shared_functions import mem_profile

ENDC = '\033[m'
TGREEN_BOLD = '\033[32;1m'
TGREEN = '\033[32m'
TRED_BOLD = '\033[31;1m'
TWHITE = '\033[37m'
WHITE_BOLD = '\033[37;1m'
CYAN_BOLD = '\033[36;1m'

if __name__ == '__main__':

    available_cores = multiprocessing.cpu_count()

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Wrapper for Crosswalk USGS sites to HydroID and get elevations')
    parser.add_argument('-fim_dir','--fim-directory',help='Directory containing FIM outputs by HUC',required=True)
    parser.add_argument('-gages','--usgs-gages-filename', help='USGS gages (gpkg point layer)', required=True)
    parser.add_argument('-e', '--extent', help="extent configuration entered by user when running fim_run.sh (MS or FR)", required = True)
    parser.add_argument('-j','--job-number',help='Number of jobs to use',required=False,default=1)


    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    fim_dir = args['fim_directory']
    usgs_gages_filename = args['usgs_gages_filename']
    extent = args['extent']
    job_number = int(args['job_number'])
    procs_list = []
    exit_flag = False  # Default to False.

    # Ensure fim_run_dir exists.
    if not os.path.exists(fim_dir):
        print(TRED_BOLD + "Warning: " + WHITE_BOLD + "The provided fim_run_dir (-r) " + CYAN_BOLD + fim_dir + WHITE_BOLD + " could not be located in the 'outputs' directory." + ENDC)
        print(WHITE_BOLD + "Please provide the parent directory name for fim_run.sh outputs. These outputs are usually written in a subdirectory, e.g. data/outputs/XXXXXX" + ENDC)
        print()
        exit_flag = True


    if not os.path.exists(usgs_gages_filename):
        print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided usgs_gages_filename " + CYAN_BOLD + usgs_gages_filename + WHITE_BOLD + " could not be located." + ENDC)
        exit_flag = True

    if job_number > available_cores:
        job_number = available_cores - 1
        print("Provided job number exceeds the number of available cores. " + str(job_number) + " max jobs will be used instead.")

    if exit_flag:
        print()
        sys.exit()
    else:
        huc_list  = os.listdir(fim_dir)
        for huc in huc_list:
            print(huc)
            # Define path to FIM output variables
            dem_filename = os.path.join(fim_dir, huc, 'rem_zeroed_masked.tif') ## Should use dem_meters.tif (not available in -p FIM outputs
            input_flows_filename = os.path.join(fim_dir, huc, 'demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg')
            input_catchment_filename = os.path.join(fim_dir, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
            wbd_buffer_filename = os.path.join(fim_dir, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg') ## Should use wbd_buffered.gpkg (not available in -p FIM outputs)
            dem_adj_filename = os.path.join(fim_dir, huc, 'rem_zeroed_masked.tif') ## Should use dem_thalwegCond.tif (not available in -p FIM outputs
            output_table_filename = os.path.join(fim_dir, huc, 'usgs_elev_table.csv')
            if os.path.exists(input_catchment_filename):
                procs_list.append([usgs_gages_filename,dem_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename,dem_adj_filename,output_table_filename,extent])
                #crosswalk_usgs_gage(usgs_gages_filename,dem_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename,dem_adj_filename,output_table_filename,extent)
                #print('Complete')
            else:
                print(TRED_BOLD + "Warning: " + WHITE_BOLD + "HUC output dir: " + CYAN_BOLD + huc + WHITE_BOLD + " does not contain the necessary FIM outputs... skipping this HUC in the processing." + ENDC)

    
    print(f"Performing usgs gage crosswalk for {len(procs_list)} hucs using {job_number} jobs")
    with Pool(processes=job_number) as pool:
        pool.starmap(crosswalk_usgs_gage, procs_list)
    print('Completed')
    

#parser.add_argument('-gages','--usgs-gages-filename', help='USGS gages', required=True)
#parser.add_argument('-dem','--dem-filename',help='DEM',required=True)
#parser.add_argument('-flows','--input-flows-filename', help='DEM derived streams', required=True)
#parser.add_argument('-cat','--input-catchment-filename', help='DEM derived catchments', required=True)
#parser.add_argument('-wbd','--wbd-buffer-filename', help='WBD buffer', required=True)
#parser.add_argument('-dem_adj','--dem-adj-filename', help='Thalweg adjusted DEM', required=True)
#parser.add_argument('-outtable','--output-table-filename', help='Table to append data', required=True)
#parser.add_argument('-e', '--extent', help="extent configuration entered by user when running fim_run.sh", required = True)