#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import sys
from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import rasterio
from dotenv import load_dotenv

import utils.shared_variables as sv
from src_roughness_optimization import update_rating_curve
from src.utils.shared_functions import setup_file_logger
from utils.io import write_geodataframe



gpd.options.io_engine = "pyogrio"

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


# TODO: Nov 2026: Until we get updated usage of MP that can handle logging
# we have to stay with log_text inside child workers only.
def process_points(branch_dir, huc, branch_id, hand_path, catchments_path, catchments_poly_path, water_edge_df, htable_path, is_debug, hydroid_prefix_path):
    '''
    This function ingests geodataframe and attributes the point data with its hydroid and HAND values
    before passing a dataframe to the src_roughness_optimization.py workflow.

    Processing
    - Extract x,y coordinates from geometry
    - Projects the point data to matching CRS for HAND and hydroid rasters
    - Samples the hydroid and HAND raster values for each point and stores the values in dataframe
    - Calculates the median HAND value for all points by hydroid
    
    Returns:
        - in_error: If true, it means stop downsteam coding processing, else continue
        - log_text: A list of strings to go to a log file.

    '''

    log_text = []
    in_error = False

    ## Define coords variable to be used in point raster value attribution.
    coords = [(x, y) for x, y in zip(water_edge_df.X, water_edge_df.Y)]

    water_edge_df = water_edge_df.to_crs(sv.DEFAULT_FIM_PROJECTION_CRS)

    process_int16 = True
    try:
        with open(hydroid_prefix_path, 'r') as file:
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
        log_text.append(
            'NOTE --> skipping HUC: '
            + str(huc)
            + '  Branch: '
            + str(branch_id)
            + ': no valid observation points found within the branch catchments'
        )
        in_error = False  # Do not stop the huc processing
        return log_text, in_error

    ## Intermediate output for debugging
    if is_debug:
        branch_debug_pts_out_gpkg = os.path.join(
            branch_dir, 'export_water_edge_df_' + branch_id + '.parquet'
        )
        write_geodataframe(water_edge_df, branch_debug_pts_out_gpkg, index=False)
        log_text.append(f"{branch_debug_pts_out_gpkg} saved")

    # print('Processing points for HUC: ' + str(huc) + '  Branch: ' + str(branch_id))
    ## Get median HAND value for appropriate groups.
    water_edge_median_ds = water_edge_df.groupby(
        ["hydroid", "flow", "submitter", "coll_time", "flow_unit", "layer"]
    )['hand'].median()

    ## Write user_supplied_n_vals to CSV for next step.
    pt_n_values_csv = os.path.join(branch_dir, 'user_supplied_n_vals_' + branch_id + '.csv')
    water_edge_median_ds.to_csv(pt_n_values_csv)
    log_text.append(f"{pt_n_values_csv} saved")

    ## Convert pandas series to dataframe to pass to update_rating_curve
    water_edge_median_df = water_edge_median_ds.reset_index()
    water_edge_median_df['coll_time'] = water_edge_median_df.coll_time.astype(str)
    del water_edge_median_ds

    ## Additional arguments for src_roughness_optimization
    source_tag = 'point_obs'  # tag to use in source attribute field
    merge_prev_adj = True  # merge in previous SRC adjustment calculations

    ## Call update_rating_curve() to perform the rating curve calibration.
    # TODO: Nov 2026: Until we get logging fully implemented including MP management
    # for all py files that call this function, we have to stick with log_text.
    # log_text, in_error = update_rating_curve(
    #     branch_dir,
    #     water_edge_median_df,
    #     htable_path,
    #     huc,
    #     branch_id,
    #     catchments_poly_path,
    #     is_debug,
    #     source_tag,
    #     merge_prev_adj,
    #     DOWNSTREAM_THRESHOLD,
    # )

    try:
        log_msgs, in_error = update_rating_curve(branch_dir, water_edge_median_df, htable_path, huc, branch_id, 
            catchments_poly_path, is_debug, source_tag, merge_prev_adj, sv.DOWNSTREAM_THRESHOLD)
        log_text.extend(log_msgs)
    except Exception as e:
        in_error = True
        print(str(huc) + ' --> ' + str(e))
        log_text.append(f"ERROR!!!: HUC {huc} --> {str(e)}")

    return log_text, in_error


def ingest_points_layer(huc_dir, branch_jobs, is_debug):
    '''

    Inputs
    - huc_dir:               directory of huc ouputs
    - branch_jobs:           number of multiprocessing branches jobs to use for processing hucs
    - debug_outputs_option: optional flag to output intermediate files
    '''

    '''
    If one of the child workers in MP returns an "in_errors" as true, then shut
    down the entire code processing. ie) sys.exit(1) will work here as it is outside
    an MP.
    '''

    huc = os.path.basename(os.path.normpath(huc_dir))

    # check to see if there is any parquet file for this huc
    water_edge_filepath = os.path.join(input_calib_points_dir, f'{huc}.parquet')
    if not os.path.exists(water_edge_filepath):
        logging.info(f"Water edge calibration parquet file missing for huc: {huc}")
        return

    water_edge_df = gpd.read_parquet(water_edge_filepath)
    logging.info(f"{len(water_edge_df)} Water edge calibration points found in {huc}")

    ## Create X and Y location columns by extracting from geometry.
    water_edge_df['X'] = water_edge_df['geometry'].x
    water_edge_df['Y'] = water_edge_df['geometry'].y

    ## Intermediate output for debugging
    if is_debug:
        huc_debug_pts_out = os.path.join(huc_dir, 'debug_water_edge_df_' + huc + '.csv')
        water_edge_df.to_csv(huc_debug_pts_out)
        logging.debug(f"{huc_debug_pts_out} saved")
        # write file
        parquet_filepath = os.path.join(huc_dir, 'debug_water_edge_df_' + huc + '.parquet')
        write_geodataframe(water_edge_df, parquet_filepath, index=False)
        logging.debug(f"{parquet_filepath} saved")        

    procs_list = []
    huc_branches_dir = os.path.join(huc_dir, 'branches')
    for branch_id in os.listdir(huc_branches_dir):

        branch_dir = os.path.join(huc_branches_dir, branch_id)
        ## Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.

        # ----------- validate paths ---------------
        # If a branch failed previously, it will not have a hydrotable  and that is ok.
        htable_path = os.path.join(branch_dir, 'hydroTable_' + branch_id + '.csv')
        if not os.path.exists(htable_path):
            logging.warning(f"WARNING: {htable_path} missing (skipping).. might be ok if branch previously failed")
            continue

        # If the hydrotable is there, then the rest of these should be
        catchments_path = os.path.join(
            branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_' + branch_id + '.tif'
        )
        if not os.path.exists(catchments_path):
            raise Exception(f"{catchments_path} is missing")

        hand_path = os.path.join(branch_dir, 'rem_zeroed_masked_' + branch_id + '.tif')

        catchments_poly_path = os.path.join(
            branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked_' + branch_id + '.parquet'
        )
        hydroid_prefix_path = os.path.join(branch_dir, 'hydroid_prefix.txt')

        args = {
            "branch_dir": branch_dir,
            "huc": huc,
            "branch_id": branch_id,
            "hand_path": hand_path,
            "catchments_path": catchments_path,
            "catchments_poly_path": catchments_poly_path,
            "water_edge_df": water_edge_df,
            "htable_path": htable_path,
            "is_debug": is_debug,
            "hydroid_prefix_path": hydroid_prefix_path
        }
        procs_list.append(args)

    results = []
    # TODO: Nov 2026: This should be ugpraded, mostly for tracing, debugging and logs
    with Pool(processes=branch_jobs) as pool:
        for map_output in pool.imap(process_points, procs_list):
            results.append(map_output)

    logging.debug("branches processed, now updating the logs")
    in_error = False

    # TODO: Nov 2026: This is temp as with this MP setup, child workers can not write to logs
    # so we will send back log text and an error bool so we can shut down the downstream tools if it fails.
    # Once we do, we can use logging.info, .debug, .warning, etc
    for log_text_list, has_error in results:
        if has_error:
            for item in log_text_list:
                logging.error(item)  # Note.. error type
            # As we are outside an MP, we can use sys.exit(1) to stop downstream code processing
            in_error = True
        else:
            for item in log_text_list:
                logging.debug(item)  # Note.. debug type (logs only)

    # logging.debug("Processing completed with log updates")

    if in_error:
        print("Stopping processing")
        sys.exit(1)


def run_prep(huc_dir, is_debug, ds_thresh_override, branch_jobs):

    downstream_theshold = sv.DOWNSTREAM_THRESHOLD
    if ds_thresh_override != downstream_theshold:
        print(
            'ALERT!! - Using a downstream distance threshold value ('
            + str(float(ds_thresh_override))
            + 'km) different than the default ('
            + str(downstream_theshold)
            + 'km) - interpret results accordingly'
        )
        downstream_theshold = float(ds_thresh_override)

    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    begin_time = dt.datetime.now()
    # Create log file for processing records
    log_file_path = setup_file_logger(log_dir, "log_spatial_src_adjust")
    logging.info("Start src adjust spatial obs")
    print(f"Logs will be saved to {log_file_path}")    
    logging.debug('START TIME: ' + str(begin_time))
    logging.debug('#########################################################')
    ## Create log file for processing records
    
    print('This may take a few minutes...')
    logging.debug(
        'Parameter Values:\n'
        + 'DOWNSTREAM_THRESHOLD = '
        + str(downstream_theshold)
        + '\n'
        + 'ROUGHNESS_MIN_THRESH = '
        + str(sv.ROUGHNESS_MIN_THRESH)
        + '\n'
        + 'ROUGHNESS_MAX_THRESH = '
        + str(sv.ROUGHNESS_MAX_THRESH)
    )
    logging.debug('#########################################################')
    logging.debug('START TIME: ' + str(begin_time))
    logging.debug("")

    ingest_points_layer(huc_dir, branch_jobs, is_debug)

    # Record run time
    logging.debug('########################################################')
    end_time = dt.datetime.now()
    logging.debug('END TIME: ' + str(end_time))
    tot_run_time = end_time - begin_time
    logging.debug('TOTAL RUN TIME: ' + str(tot_run_time))


if __name__ == '__main__':
    ## Parse arguments.
    parser = argparse.ArgumentParser(
        description=f'Adjusts rating curve based on files in {input_calib_points_dir}, '
        'containing points of known water boundary.'
    )
    parser.add_argument('-huc-dir', '--huc-dir', help='directory of a HUC output directory.', required=True)
    parser.add_argument(
        '-debug',
        '----is-debug',
        help='OPTIONAL flag: Use this to keep intermediate output files for debugging/testing',
        default=False,
        required=False,
        action='store_true',
    )
    parser.add_argument(
        '-dthresh',
        '--ds-thresh-override',
        help='OPTIONAL Override: distance in km to propogate modified roughness values downstream',
        default=sv.DOWNSTREAM_THRESHOLD,
        required=False,
    )
    parser.add_argument(
        '-jb',
        '--branch-jobs',
        help='OPTIONAL: Number of branches jobs to use',
        type=int,
        required=False,
        default=2,
    )

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    run_prep(**args)
