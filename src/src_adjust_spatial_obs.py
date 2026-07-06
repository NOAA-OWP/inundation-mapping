#!/usr/bin/env python3

import argparse
import datetime as dt
import multiprocessing
import os
import sys
from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import rasterio
from dotenv import load_dotenv

from src_roughness_optimization import update_rating_curve
from utils.shared_variables import (
    DEFAULT_FIM_PROJECTION_CRS,
    DOWNSTREAM_THRESHOLD,
    ROUGHNESS_MAX_THRESH,
    ROUGHNESS_MIN_THRESH,
)


gpd.options.io_engine = "pyogrio"


#################################
# CRITICAL TODO: July 4, 2026:  In the event of an exception, the log file will not exist
# and its details as well. Besides, we really do not want to leave a file writer
# open. Can leave memory leaks.
# This needs a try/except with printing to log and at least a one liner
# saying including the word "exception", which can be picked up automatically
# by the rollup to fim_process_huc.sh or process_rerun_calibration_huc.sh
################################


# Import variables from .env file
load_dotenv('/foss_fim/src/bash_variables.env')
outputsDir = os.getenv("outputsDir")
input_calib_points_dir = os.getenv("input_calib_points_dir")

'''
The script imports .parquet files per HUC8 containing observed FIM extent points and associated flow data.
This script attributes the point data with its hydroid and HAND values before passing a dataframe to the
    src_roughness_optimization.py workflow.

Processing
- Define CRS to use for initial geoprocessing and read wbd_path and points_layer.
- Define paths to hydroTable.csv, HAND raster, catchments raster, and synthetic rating curve JSON.
- Clip the points water_edge_df to the huc cathments polygons (for faster processing?)
- Define coords variable to be used in point raster value attribution and use point geometry to
    determine catchment raster pixel values
- Check that there are valid obs in the water_edge_df (not empty) and convert pandas series to dataframe
    to pass to update_rating_curve
- Call update_rating_curve() to perform the rating curve calibration.

Inputs
- points_layer:            .gpkg layer containing observed/truth FIM extent points and associated flow value
- fim_directory:           fim directory containing individual HUC output dirs
- wbd_path:                path the watershed boundary dataset layer (HUC polygon boundaries)
- branch_jobs:              number of multi-processing branches jobs to use
- debug_outputs_option:    optional flag to output intermediate files for reviewing/debugging

Outputs
- water_edge_median_df:    dataframe containing:
                                "hydroid", "flow", "submitter", "coll_time", "flow_unit",
                                "layer", and median "HAND" value
'''


def process_points(args):
    '''
    This function ingests geodataframe and attributes the point data with its hydroid and HAND values
    before passing a dataframe to the src_roughness_optimization.py workflow.

    Processing
    - Extract x,y coordinates from geometry
    - Projects the point data to matching CRS for HAND and hydroid rasters
    - Samples the hydroid and HAND raster values for each point and stores the values in dataframe
    - Calculates the median HAND value for all points by hydroid
    '''

    branch_dir = args[0]
    huc = args[1]
    branch_id = args[2]
    hand_path = args[3]
    catchments_path = args[4]
    catchments_poly_path = args[5]
    water_edge_df = args[6]
    htable_path = args[7]
    optional_outputs = args[8]
    hydroid_prefixpath = args[9]

    ## Define coords variable to be used in point raster value attribution.
    coords = [(x, y) for x, y in zip(water_edge_df.X, water_edge_df.Y)]

    water_edge_df = water_edge_df.to_crs(DEFAULT_FIM_PROJECTION_CRS)

    process_int16 = True
    try:
        with open(hydroid_prefixpath, 'r') as file:
            hydroid_prefix = file.read()
            int_hid_prefix = int(hydroid_prefix) * 10000
    except FileNotFoundError:
        process_int16 = False
        int_hid_prefix = 0

    ## Use point geometry to determine HAND raster pixel values.
    with rasterio.open(hand_path) as hand_src, rasterio.open(catchments_path) as catchments_src:

        water_edge_df['hand'] = (
            [np.float32(h[0]) / 1000 for h in hand_src.sample(coords)]
            if process_int16
            else [h[0] for h in hand_src.sample(coords)]
        )

        hydroids = []

        for c in catchments_src.sample(coords):
            c[0] = c[0] if c[0] >= 0 else c[0] * -1
            hid = int_hid_prefix + c[0] if process_int16 else c[0]
            hydroids.append(hid)

        water_edge_df['hydroid'] = hydroids

    water_edge_df = water_edge_df[
        (water_edge_df['hydroid'].notnull())
        & (water_edge_df['hand'] > 0)
        & (water_edge_df['hand'] != 32.767)
        & (water_edge_df['hydroid'] > int_hid_prefix)
    ]

    ## Check that there are valid obs in the water_edge_df (not empty)
    if water_edge_df.empty:
        log_text = (
            'NOTE --> skipping HUC: '
            + str(huc)
            + '  Branch: '
            + str(branch_id)
            + ': no valid observation points found within the branch catchments'
        )
    else:
        ## Intermediate output for debugging
        if optional_outputs:
            branch_debug_pts_out_gpkg = os.path.join(
                branch_dir, 'export_water_edge_df_' + branch_id + '.gpkg'
            )
            water_edge_df.to_file(branch_debug_pts_out_gpkg, driver='GPKG', index=False, engine='fiona')

        # print('Processing points for HUC: ' + str(huc) + '  Branch: ' + str(branch_id))
        ## Get median HAND value for appropriate groups.
        water_edge_median_ds = water_edge_df.groupby(
            ["hydroid", "flow", "submitter", "coll_time", "flow_unit", "layer"]
        )['hand'].median()

        ## Write user_supplied_n_vals to CSV for next step.
        pt_n_values_csv = os.path.join(branch_dir, 'user_supplied_n_vals_' + branch_id + '.csv')
        water_edge_median_ds.to_csv(pt_n_values_csv)
        ## Convert pandas series to dataframe to pass to update_rating_curve
        water_edge_median_df = water_edge_median_ds.reset_index()
        water_edge_median_df['coll_time'] = water_edge_median_df.coll_time.astype(str)
        del water_edge_median_ds

        ## Additional arguments for src_roughness_optimization
        source_tag = 'point_obs'  # tag to use in source attribute field
        merge_prev_adj = True  # merge in previous SRC adjustment calculations

        ## Call update_rating_curve() to perform the rating curve calibration.
        log_text = update_rating_curve(
            branch_dir,
            water_edge_median_df,
            htable_path,
            huc,
            branch_id,
            catchments_poly_path,
            optional_outputs,
            source_tag,
            merge_prev_adj,
            DOWNSTREAM_THRESHOLD,
        )

        ## Still testing: use code below to print out any exceptions.
        '''
        try:
            log_text = update_rating_curve(branch_dir, water_edge_median_df, htable_path, huc,
                catchments_poly_path, optional_outputs, source_tag, merge_prev_adj, DOWNSTREAM_THRESHOLD)
        except Exception as e:
            print(str(huc) + ' --> ' + str(e))
            log_text = 'ERROR!!!: HUC ' + str(huc) + ' --> ' + str(e)
        '''
    return log_text


def ingest_points_layer(huc_dir, branch_jobs, debug_outputs_option, log_file):
    '''

    Inputs
    - huc_dir:               directory of huc ouputs
    - branch_jobs:           number of multiprocessing branches jobs to use for processing hucs
    - debug_outputs_option: optional flag to output intermediate files
    - log_file:             where stdout/stderr will be logged

    Outputs
    - log_file:             where stdout/stderr will be logged
    '''

    huc = os.path.basename(os.path.normpath(huc_dir))

    # check to see if there is any parquet file for this huc
    water_edge_filepath = os.path.join(input_calib_points_dir, f'{huc}.parquet')
    if not os.path.exists(water_edge_filepath):
        print(f"Water edge calibration parquet file missing for huc: {huc}")
        log_file.write(f"Water edge calibration parquet file missing for huc: {huc}" + '\n')
        return

    water_edge_df = gpd.read_parquet(water_edge_filepath)
    print(f"{len(water_edge_df)} Water edge calibration points found in " + str(huc))
    log_file.write(f"{len(water_edge_df)} Water edge calibration points found in " + str(huc) + '\n')

    ## Create X and Y location columns by extracting from geometry.
    water_edge_df['X'] = water_edge_df['geometry'].x
    water_edge_df['Y'] = water_edge_df['geometry'].y

    ## Intermediate output for debugging
    if debug_outputs_option:
        huc_debug_pts_out = os.path.join(huc_dir, 'debug_water_edge_df_' + huc + '.csv')
        water_edge_df.to_csv(huc_debug_pts_out)
        huc_debug_pts_out_gpkg = os.path.join(huc_dir, 'export_water_edge_df_' + huc + '.gpkg')
        water_edge_df.to_file(huc_debug_pts_out_gpkg, driver='GPKG', index=False, engine='fiona')
        # write parquet file using ".to_parquet() method"
        parquet_filepath = os.path.join(huc_dir, 'debug_water_edge_df_' + huc + '.parquet')
        water_edge_df.to_parquet(parquet_filepath, index=False)

    procs_list = []
    huc_branches_dir = os.path.join(huc_dir, 'branches')
    for branch_id in os.listdir(huc_branches_dir):
        branch_dir = os.path.join(huc_branches_dir, branch_id)
        ## Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
        hand_path = os.path.join(branch_dir, 'rem_zeroed_masked_' + branch_id + '.tif')
        catchments_path = os.path.join(
            branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_' + branch_id + '.tif'
        )
        htable_path = os.path.join(branch_dir, 'hydroTable_' + branch_id + '.csv')
        catchments_poly_path = os.path.join(
            branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked_' + branch_id + '.gpkg'
        )
        hydroid_prefix_path = os.path.join(branch_dir, 'hydroid_prefix.txt')

        # Check to make sure the fim output files exist. Continue to next iteration if not and warn user.
        if not os.path.exists(hand_path):
            print(
                "WARNING: HAND grid does not exist (skipping): "
                + str(huc)
                + ' - branch-id: '
                + str(branch_id)
            )
            log_file.write(
                "WARNING: HAND grid does not exist (skipping): "
                + str(huc)
                + ' - branch-id: '
                + str(branch_id)
                + '\n'
            )
        elif not os.path.exists(catchments_path):
            print(
                "WARNING: Catchments grid does not exist (skipping): "
                + str(huc)
                + ' - branch-id: '
                + str(branch_id)
            )
            log_file.write(
                "WARNING: Catchments grid does not exist (skipping): "
                + str(huc)
                + ' - branch-id: '
                + str(branch_id)
                + '\n'
            )
        elif not os.path.exists(htable_path):
            print(
                "WARNING: hydroTable does not exist (skipping): "
                + str(huc)
                + ' - branch-id: '
                + str(branch_id)
            )
            log_file.write(
                "WARNING: hydroTable does not exist (skipping): "
                + str(huc)
                + ' - branch-id: '
                + str(branch_id)
                + '\n'
            )
        else:
            procs_list.append(
                [
                    branch_dir,
                    huc,
                    branch_id,
                    hand_path,
                    catchments_path,
                    catchments_poly_path,
                    water_edge_df,
                    htable_path,
                    debug_outputs_option,
                    hydroid_prefix_path,
                ]
            )

    with Pool(processes=branch_jobs) as pool:
        log_output = pool.map(process_points, procs_list)
        log_file.writelines(["%s\n" % item for item in log_output])

    log_file.write('#########################################################\n')


def run_prep(huc_dir, debug_outputs_option, ds_thresh_override, DOWNSTREAM_THRESHOLD, branch_jobs):
    if ds_thresh_override != DOWNSTREAM_THRESHOLD:
        print(
            'ALERT!! - Using a downstream distance threshold value ('
            + str(float(ds_thresh_override))
            + 'km) different than the default ('
            + str(DOWNSTREAM_THRESHOLD)
            + 'km) - interpret results accordingly'
        )
        DOWNSTREAM_THRESHOLD = float(ds_thresh_override)

    ## Create output dir for log file
    output_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## Create log file for processing records
    print('This may take a few minutes...')
    sys.__stdout__ = sys.stdout
    log_file = open(os.path.join(output_dir, 'log_spatial_src_adjust.log'), "w")
    log_file.write('#########################################################\n')
    log_file.write(
        'Parameter Values:\n'
        + 'DOWNSTREAM_THRESHOLD = '
        + str(DOWNSTREAM_THRESHOLD)
        + '\n'
        + 'ROUGHNESS_MIN_THRESH = '
        + str(ROUGHNESS_MIN_THRESH)
        + '\n'
        + 'ROUGHNESS_MAX_THRESH = '
        + str(ROUGHNESS_MAX_THRESH)
        + '\n'
    )
    log_file.write('#########################################################\n\n')
    log_file.write('START TIME: ' + str(begin_time) + '\n')

    ingest_points_layer(huc_dir, branch_jobs, debug_outputs_option, log_file)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()


if __name__ == '__main__':
    ## Parse arguments.
    parser = argparse.ArgumentParser(
        description=f'Adjusts rating curve based on files in {input_calib_points_dir}, '
        'containing points of known water boundary.'
    )
    parser.add_argument('-huc_dir', '--huc_dir', help='directory of a HUC output directory.', required=True)
    parser.add_argument(
        '-debug',
        '--extra-outputs',
        help='OPTIONAL flag: Use this to keep intermediate output files for debugging/testing',
        default=False,
        required=False,
        action='store_true',
    )
    parser.add_argument(
        '-dthresh',
        '--downstream-thresh',
        help='OPTIONAL Override: distance in km to propogate modified roughness values downstream',
        default=DOWNSTREAM_THRESHOLD,
        required=False,
    )
    parser.add_argument(
        '-jb',
        '--branch_jobs',
        help='OPTIONAL: Number of branches jobs to use',
        type=int,
        required=False,
        default=2,
    )

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    huc_dir = args['huc_dir']
    debug_outputs_option = args['extra_outputs']
    ds_thresh_override = args['downstream_thresh']
    branch_jobs = args['branch_jobs']

    run_prep(huc_dir, debug_outputs_option, ds_thresh_override, DOWNSTREAM_THRESHOLD, branch_jobs)
