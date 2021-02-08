#!/usr/bin/env python3

# Created: 1/11/2021
# Primary developer(s): ryan.spies@noaa.gov
# Purpose: This script provides the user to input a customized flow entry to produce
# inundation outputs using outputs from fim_run. Note that the flow csv must be
# formatted with "feature_id" & "discharge" columns. Flow must be in cubic m/s

import os
import sys
import pandas as pd
import geopandas as gpd
import rasterio
import json
import csv
import argparse
import shutil

# insert python path at runtime for accessing scripts in foss_fim/tests dir (e.g. inundation.py)
sys.path.insert(1, 'foss_fim/tests')
from utils.shared_functions import get_contingency_table_from_binary_rasters, compute_stats_from_contingency_table
from inundation import inundate

TEST_CASES_DIR = r'/data/inundation_review/inundation_custom_flow/'  # Will update.
INPUTS_DIR = r'/data/inputs'
OUTPUTS_DIR = os.environ['outputDataDir']

ENDC = '\033[m'
TGREEN_BOLD = '\033[32;1m'
TGREEN = '\033[32m'
TRED_BOLD = '\033[31;1m'
TWHITE = '\033[37m'
WHITE_BOLD = '\033[37;1m'
CYAN_BOLD = '\033[36;1m'

def run_recurr_test(fim_run_dir, branch_name, huc_id, input_flow_csv, mask_type='huc'):

    # Construct paths to development test results if not existent.
    huc_id_dir_parent = os.path.join(TEST_CASES_DIR, huc_id)
    if not os.path.exists(huc_id_dir_parent):
        os.mkdir(huc_id_dir_parent)
    branch_test_case_dir_parent = os.path.join(TEST_CASES_DIR, huc_id, branch_name)

    # Delete the entire directory if it already exists.
    if os.path.exists(branch_test_case_dir_parent):
        shutil.rmtree(branch_test_case_dir_parent)

    print("Running the NWM recurrence intervals for HUC: " + huc_id + ", " + branch_name + "...")

    assert os.path.exists(fim_run_dir), "Cannot locate " + fim_run_dir

    # Create paths to fim_run outputs for use in inundate().
    if "previous_fim" in fim_run_dir and "fim_2" in fim_run_dir:
        rem = os.path.join(fim_run_dir, 'rem_clipped_zeroed_masked.tif')
        catchments = os.path.join(fim_run_dir, 'gw_catchments_reaches_clipped_addedAttributes.tif')
    else:
        rem = os.path.join(fim_run_dir, 'rem_zeroed_masked.tif')
        catchments = os.path.join(fim_run_dir, 'gw_catchments_reaches_filtered_addedAttributes.tif')
    if mask_type == 'huc':
        catchment_poly = ''
    else:
        catchment_poly = os.path.join(fim_run_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    hydro_table = os.path.join(fim_run_dir, 'hydroTable.csv')

    # Map necessary inputs for inundation().
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

    #benchmark_category = huc_id.split('_')[1]
    current_huc = huc_id.split('_')[0]  # Break off HUC ID and assign to variable.

    if not os.path.exists(branch_test_case_dir_parent):
        os.mkdir(branch_test_case_dir_parent)


    #branch_test_case_dir = os.path.join(branch_test_case_dir_parent)

    #os.makedirs(branch_test_case_dir)  # Make output directory for branch.

    # Define paths to inundation_raster and forecast file.
    inundation_raster = os.path.join(branch_test_case_dir_parent, branch_name + '_inund_extent.tif')
    forecast = os.path.join(TEST_CASES_DIR,"_input_flow_files", input_flow_csv)

    # Copy forecast flow file into the outputs directory to all viewer to reference the flows used to create inundation_raster
    shutil.copyfile(forecast,os.path.join(branch_test_case_dir_parent,input_flow_csv))

    # Run inundate.
    print("-----> Running inundate() to produce modeled inundation extent for the " + input_flow_csv)
    inundate(
             rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
             subset_hucs=current_huc,num_workers=1,aggregate=False,inundation_raster=inundation_raster,inundation_polygon=None,
             depths=None,out_raster_profile=None,out_vector_profile=None,quiet=True
            )

    print("-----> Inundation mapping complete.")


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM using a user supplied flow data file. Inundation outputs are stored in the /inundation_review/inundation_custom_flow/ directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh (e.g. data/ouputs/dev_abc/12345678_dev/12345678)',required=True)
    parser.add_argument('-b', '--branch-name',help='The name of the working branch in which features are being tested (used to name the output inundation directory) -> type=str',required=True,default="")
    parser.add_argument('-t', '--huc-id',help='The huc id to use (single huc). Format as: xxxxxxxx, e.g. 12345678',required=True,default="")
    parser.add_argument('-m', '--mask-type', help='Optional: specify \'huc\' (FIM < 3) or \'filter\' (FIM >= 3) masking method', required=False,default="huc")
    parser.add_argument('-y', '--input-flow-csv',help='Filename of the user generated (customized) csv. Must contain nwm feature ids and flow value(s) (units: cms) --> put this file in the "_input_flow_files" directory',required=True, default="")


    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    valid_huc_id_list = ['nwm_recurr']

    exit_flag = False  # Default to False.
    print()

    # Ensure fim_run_dir exists.
    if not os.path.exists(args['fim_run_dir']):
        print(TRED_BOLD + "Warning: " + WHITE_BOLD + "The provided fim_run_dir (-r) " + CYAN_BOLD + args['fim_run_dir'] + WHITE_BOLD + " could not be located in the 'outputs' directory." + ENDC)
        print(WHITE_BOLD + "Please provide the parent directory name for fim_run.sh outputs. These outputs are usually written in a subdirectory, e.g. data/outputs/123456/123456." + ENDC)
        print()
        exit_flag = True


    if args['input_flow_csv'] == '':
        print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided input_flow_csv (-y) " + CYAN_BOLD + args['input_flow_csv'] + WHITE_BOLD + " is not provided. Please provide a csv file with nwm featureid and flow values" + ENDC)
        exit_flag = True


    if exit_flag:
        print()
        sys.exit()


    else:

        run_recurr_test(**args)
