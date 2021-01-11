#!/usr/bin/env python3

import os
import sys
import pandas as pd
import geopandas as gpd
import rasterio
import json
import csv
import argparse
import shutil

from utils.shared_functions import get_contingency_table_from_binary_rasters, compute_stats_from_contingency_table
from inundation import inundate

TEST_CASES_DIR = r'/data/test_cases_nwm_recurr/'  # Will update.
INPUTS_DIR = r'/data/inputs'
OUTPUTS_DIR = os.environ['outputDataDir']

ENDC = '\033[m'
TGREEN_BOLD = '\033[32;1m'
TGREEN = '\033[32m'
TRED_BOLD = '\033[31;1m'
TWHITE = '\033[37m'
WHITE_BOLD = '\033[37;1m'
CYAN_BOLD = '\033[36;1m'

def run_recurr_test(fim_run_dir, branch_name, test_id, magnitude, mask_type='huc'):

    # Construct paths to development test results if not existent.
    test_id_dir_parent = os.path.join(TEST_CASES_DIR, test_id)
    if not os.path.exists(test_id_dir_parent):
        os.mkdir(test_id_dir_parent)
    branch_test_case_dir_parent = os.path.join(TEST_CASES_DIR, test_id, branch_name)

    # Delete the entire directory if it already exists.
    if os.path.exists(branch_test_case_dir_parent):
        shutil.rmtree(branch_test_case_dir_parent)

    print("Running the NWM recurrence intervals for test_id: " + test_id + ", " + branch_name + "...")

    fim_run_parent = os.path.join(os.environ['outputDataDir'], fim_run_dir)
    assert os.path.exists(fim_run_parent), "Cannot locate " + fim_run_parent

    # Create paths to fim_run outputs for use in inundate().
    rem = os.path.join(fim_run_parent, 'rem_zeroed_masked.tif')
    catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes.tif')
    if mask_type == 'huc':
        catchment_poly = ''
    else:
        catchment_poly = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    hydro_table = os.path.join(fim_run_parent, 'hydroTable.csv')

    # Map necessary inputs for inundation().
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

    #benchmark_category = test_id.split('_')[1]
    current_huc = test_id.split('_')[0]  # Break off HUC ID and assign to variable.

    if not os.path.exists(branch_test_case_dir_parent):
        os.mkdir(branch_test_case_dir_parent)

    # Check if magnitude is list of magnitudes or single value.
    magnitude_list = magnitude
    if type(magnitude_list) != list:
        magnitude_list = [magnitude_list]

    for magnitude in magnitude_list:
        # Construct path to validation raster and forecast file.

        branch_test_case_dir = os.path.join(branch_test_case_dir_parent, magnitude)

        os.makedirs(branch_test_case_dir)  # Make output directory for branch.

        # Define paths to inundation_raster and forecast file.
        inundation_raster = os.path.join(branch_test_case_dir, 'inundation_extent.tif')
        forecast = os.path.join(TEST_CASES_DIR, 'nwm_recurr_flow_data', 'recurr_' + magnitude + '_cms.csv')

        # Run inundate.
        print("-----> Running inundate() to produce modeled inundation extent for the " + magnitude + " magnitude...")
        inundate(
                 rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=current_huc,num_workers=1,aggregate=False,inundation_raster=inundation_raster,inundation_polygon=None,
                 depths=None,out_raster_profile=None,out_vector_profile=None,quiet=True
                )

        print("-----> Inundation mapping complete.")


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM using streamflow recurrence interflow data. Results are stored in the test directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh (starting inside the /output/ dir)',required=True)
    parser.add_argument('-b', '--branch-name',help='The name of the working branch in which features are being tested (used to name the output directory) -> type=str',required=True,default="")
    parser.add_argument('-t', '--test-id',help='The test_id to use. Format as: HUC_TESTTYPE, e.g. 12345678_nwm_recurr.',required=True,default="")
    parser.add_argument('-m', '--mask-type', help='Specify \'huc\' (FIM < 3) or \'filter\' (FIM >= 3) masking method', required=False,default="huc")
    parser.add_argument('-y', '--magnitude',help='The magnitude (reccur interval) to run. Leave blank to use default intervals.',required=False, default="")


    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    valid_test_id_list = ['nwm_recurr']

    exit_flag = False  # Default to False.
    print()

    # Ensure fim_run_dir exists.
    if not os.path.exists(os.path.join(os.environ['outputDataDir'], args['fim_run_dir'])):
        print(TRED_BOLD + "Warning: " + WHITE_BOLD + "The provided fim_run_dir (-r) " + CYAN_BOLD + os.environ['outputDataDir'] + args['fim_run_dir'] + WHITE_BOLD + " could not be located in the 'outputs' directory." + ENDC)
        print(WHITE_BOLD + "Please provide the parent directory name for fim_run.sh outputs. These outputs are usually written in a subdirectory, e.g. outputs/123456/123456." + ENDC)
        print()
        exit_flag = True

    # Ensure valid flow recurr intervals
    default_flow_intervals = ['1_5','5_0','10_0']
    if args['magnitude'] == '':
        args['magnitude'] = default_flow_intervals
        print(TRED_BOLD + "Using default flow reccurence intervals: " + WHITE_BOLD + str(default_flow_intervals)[1:-1])
    else:
        if set(default_flow_intervals).issuperset(set(args['magnitude'])) == False:
            print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided magnitude (-y) " + CYAN_BOLD + args['magnitude'] + WHITE_BOLD + " is invalid. NWM Recurrence Interval options include: " + str(default_flow_intervals)[1:-1] + ENDC)
            exit_flag = True


    if exit_flag:
        print()
        sys.exit()


    else:

        run_recurr_test(**args)
