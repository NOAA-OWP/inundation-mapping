#!/usr/bin/env python3

import argparse
import datetime as dt
import multiprocessing
import os
import sys
from multiprocessing import Pool
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from dotenv import load_dotenv
from rasterstats import point_query

from src_roughness_optimization import update_rating_curve
from utils.shared_variables import (
    DEFAULT_FIM_PROJECTION_CRS,
    DOWNSTREAM_THRESHOLD,
    ROUGHNESS_MAX_THRESH,
    ROUGHNESS_MIN_THRESH,
)


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
- job_number:              number of multi-processing jobs to use
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
    use_usgs_hwm = args[10]

    # Reproject to FIM CRS
    water_edge_df = water_edge_df.to_crs(DEFAULT_FIM_PROJECTION_CRS)

    ## Define coords variable to be used in point raster value attribution.
    coords = [(x, y) for x, y in zip(water_edge_df.X, water_edge_df.Y)]

    ## OWP Version - merge v4.8.7.3
    ## hydroid_prefixpath file does not exist
    print(f" Not using {hydroid_prefixpath}")
    # with open(hydroid_prefixpath, 'r') as file:
    #     hydroid_prefix = file.read()
    #     int_hid_prefix = int(hydroid_prefix) * 10000

    ## Use point geometry to determine HAND raster pixel values.
    with rasterio.open(hand_path) as hand_src, rasterio.open(catchments_path) as catchments_src:
        ## OWP Version - merge v4.8.7.3
        #     water_edge_df['hand'] = [np.float32(h[0]) / 1000 for h in hand_src.sample(coords)]
        #     hydroids = []
        #
        #     for c in catchments_src.sample(coords):
        #         hid = int_hid_prefix * -1 + c[0] if c[0] < 0 else int_hid_prefix + c[0]
        #         hydroids.append(hid)
        # water_edge_df['hydroid'] = hydroids

        ## NGWPC Version - merge v4.8.7.3
        raw_hand_vals = [h[0] for h in hand_src.sample(coords)]
        hydroid_vals = [c[0] for c in catchments_src.sample(coords)]

    # Assign to dataframe
    water_edge_df['hydroid'] = hydroid_vals

    # If usgs_usgs_hwm is True, and if the height_above_gnd column exists, adjust HAND values
    if use_usgs_hwm and 'height_above_gnd' in water_edge_df.columns:
        # Assume null values are the actual water edge, so fill them with 0.
        water_edge_df['height_above_gnd'] = pd.to_numeric(
            water_edge_df['height_above_gnd'], errors='coerce'
        ).fillna(0)
        adjusted_hand = []
        for hand, height_above_gnd in zip(raw_hand_vals, water_edge_df['height_above_gnd']):
            height_above_gnd = height_above_gnd * 0.3048  # ft to m
            if hand is None or np.isnan(hand):
                adjusted_hand.append(np.nan)
            elif pd.notnull(height_above_gnd) and height_above_gnd >= 0:
                adjusted_hand.append(hand + height_above_gnd)
            else:
                adjusted_hand.append(hand)
        water_edge_df['hand'] = adjusted_hand
    else:
        water_edge_df['hand'] = raw_hand_vals

    # Clean up the dataframe
    water_edge_df = water_edge_df[
        (water_edge_df['hydroid'].notnull()) & (water_edge_df['hand'] > 0) & (water_edge_df['hydroid'] > 0)
    ]

    # ## OWP Version - merge v4.8.7.3
    # water_edge_df = water_edge_df[
    #     (water_edge_df['hydroid'].notnull())
    #     & (water_edge_df['hand'] > 0)
    #     & (water_edge_df['hand'] != 32.767)
    #     & (water_edge_df['hydroid'] > int_hid_prefix)
    # ]

    if use_usgs_hwm:
        # Reassign 'submitter' values to reflect all submitters for each hydroid
        submitter_labels = water_edge_df.groupby('hydroid')['submitter'].apply(
            lambda s: ', '.join(sorted(set(s))) if 'usgs_hwm' in s.values else s.iloc[0]
        )

        # Map the combined label back to each row by hydroid
        water_edge_df['submitter'] = water_edge_df['hydroid'].map(submitter_labels)

        # Group hydroids by unique submitter values (as sets)
        submitter_sets = water_edge_df.groupby('hydroid')['submitter'].apply(lambda x: set(x))

        # Identify hydroids with ONLY 'usgs_hwm' as submitter
        # These are dropped so as to not bias RCs to high end
        hydroids_to_drop = submitter_sets[submitter_sets == {'usgs_hwm'}].index
        water_edge_df = water_edge_df[
            ~water_edge_df['hydroid'].isin(hydroids_to_drop)
        ]  # Drop all rows with those hydroids

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

        ## Get median HAND value for appropriate groups.
        water_edge_median_ds = water_edge_df.groupby(
            ["hydroid", "flow", "coll_time", "submitter", "flow_unit", "layer"]
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


def find_points_in_huc(huc_id, use_usgs_hwm, log_file):
    '''
    This function loads the .parquet file containing points attributed with the input huc id into a GDataFrame

    Processing
    - Query the <input_calib_points_dir> directory for a <HUC8>.parquet file containing calibration points.
    - Reads points contained in .parquet file into a pandas geodataframe

    Inputs
    - huc_id:        HUC id to find in <input_calib_points_dir>

    Outputs
    - water_edge_df: geodataframe with point data
    '''

    water_edge_filepath = os.path.join(input_calib_points_dir, f'{huc_id[:8]}.parquet')

    # Read original water edge points
    water_edge_df = gpd.read_parquet(water_edge_filepath)

    # If use_usgs_hwm is True, then check if corresponding parquet file exists for HUC.
    # If it does, merge it with the water_edge_df. Filtering performed later.
    if use_usgs_hwm:
        usgs_hwm_parquet_dir = os.getenv("input_calib_points_usgs_hwm_dir")
        if usgs_hwm_parquet_dir:
            potential_usgs_water_edge_filepath = Path(usgs_hwm_parquet_dir) / Path(water_edge_filepath).name
            if potential_usgs_water_edge_filepath.exists():
                try:
                    usgs_hwm_water_edge_df = gpd.read_parquet(potential_usgs_water_edge_filepath)
                    if usgs_hwm_water_edge_df.crs != water_edge_df.crs:
                        usgs_hwm_water_edge_df = usgs_hwm_water_edge_df.to_crs(water_edge_df.crs)

                    water_edge_df = gpd.GeoDataFrame(
                        pd.concat([water_edge_df, usgs_hwm_water_edge_df], ignore_index=True, sort=False)
                    )
                    water_edge_df.set_geometry('geometry', inplace=True)
                    log_file.write(f"USGS HWM points merged for {huc_id}")
                except Exception as e:
                    log_file.write(f"Failed to process USGS HWM for {huc_id}: {str(e)}\n")
            else:
                log_file.write(f"No USGS HWM file found for {huc_id} — skipping.\n")
        else:
            log_file.write(
                "Environment variable 'input_calib_points_usgs_hwm_dir' not set — skipping USGS merge.\n"
            )

    # Read WBD geometry as a full GeoDataFrame (retaining CRS)
    wbd_gdf = gpd.read_file(os.path.join(fim_directory, huc_id, 'wbd.gpkg'))

    # Reproject WBD geometry to match points if needed
    if wbd_gdf.crs != water_edge_df.crs:
        wbd_gdf = wbd_gdf.to_crs(water_edge_df.crs)

    # Intersect
    water_edge_df = water_edge_df[water_edge_df.intersects(wbd_gdf.geometry.union_all())].reset_index(
        drop=True
    )

    return water_edge_df


def find_hucs_with_points(points_file_dir, fim_out_huc_list):
    '''
    This function queries a directory with .parquet files of HUCs containing calibration points
    (generated from /data/write_parquet_from_calib_pts.py) and returns a list of all the HUCs in
    <fim_out_huc_list> that contain calibration point data.
    '''

    try:
        files_in_points_file_dir = os.listdir(points_file_dir)
    except FileNotFoundError:
        return []

    # Use list comprehension to slice .parquet off filename, and also prune non-parquet files in directory
    hucs_in_points_file_dir = [i[:-8] for i in files_in_points_file_dir if i.endswith('.parquet')]

    # make sets
    hucs_in_points_file_dir_set = set(hucs_in_points_file_dir)
    fim_out_huc_list_huc8s_set = set([f[:8] for f in fim_out_huc_list])

    # Use set operations to only keep hucs in both the points_file_dir & fim_out_huc_list
    fim_out_huc_list_with_points = hucs_in_points_file_dir_set & fim_out_huc_list_huc8s_set

    # Get the list of fim_out_huc_list that have points
    hucs_wpoints = [f for f in fim_out_huc_list if f[:8] in fim_out_huc_list_with_points]

    return hucs_wpoints


def ingest_points_layer(fim_directory, job_number, debug_outputs_option, log_file, use_usgs_hwm):
    '''
    The function obtains all points within a given huc, locates the corresponding FIM output files
    for each huc (confirms all necessary files exist), and then passes a proc list of
    huc organized data to process_points function.

    Inputs
    - fim_directory:        parent directory of fim ouputs (contains HUC directories)
    - job_number:           number of multiprocessing jobs to use for processing hucs
    - debug_outputs_option: optional flag to output intermediate files
    - log_file:             where stdout/stderr will be logged

    Processing
    - Query the <input_calib_points_dir> for all unique huc ids that have calb points
    - Loop through all HUCs with calib data and locate necessary fim output files to pass to calib workflow

    - procs_list:           passes multiprocessing list of input args for process_points function input

    Outputs
    - log_file:             where stdout/stderr will be logged
    '''

    print("Finding all fim_output hucs that contain calibration points...")
    fim_out_huc_list = [
        item for item in os.listdir(fim_directory) if os.path.isdir(os.path.join(fim_directory, item))
    ]

    # Remove logs, unit_errors, and branch_errors folders if they exist in <fim_directory>
    fim_out_huc_list.remove('logs')
    if 'unit_errors' in fim_out_huc_list:
        fim_out_huc_list.remove('unit_errors')
    if 'branch_errors' in fim_out_huc_list:
        fim_out_huc_list.remove('branch_errors')

    # get huc_level
    huc_level = max(len(o) for o in fim_out_huc_list)

    ## Record run time and close log file
    run_time_start = dt.datetime.now()
    log_file.write('Finding all hucs that contain calibration points...' + '\n')
    huc_list_db = find_hucs_with_points(input_calib_points_dir, fim_out_huc_list)

    run_time_end = dt.datetime.now()
    task_run_time = run_time_end - run_time_start

    log_file.write('HUC SEARCH TASK RUN TIME: ' + str(task_run_time) + '\n')
    print(f"{len(huc_list_db)} hucs found in point file directory" + '\n')
    log_file.write(f"{len(huc_list_db)} hucs found in point file directory" + '\n')
    log_file.write('#########################################################\n')

    # Ensure HUC id has huc_level characters
    huc_list = []
    for huc in huc_list_db:
        ## zfill to the appropriate scale to ensure leading zeros are present, if necessary.
        if len(huc) == 7:
            huc = huc.zfill(huc_level)
        if huc not in huc_list:
            huc_list.append(huc)
            log_file.write(str(huc) + '\n')

    # Initialize process list for multiprocessing.
    procs_list = []

    # huc_list = ['07080205'] # Uncomment for testing
    # Sort huc_list for helping track progress in future print statments
    huc_list.sort()
    ## Define paths to relevant HUC HAND data.
    for huc in huc_list:
        huc_branches_dir = os.path.join(fim_directory, huc, 'branches')
        water_edge_df = find_points_in_huc(huc, use_usgs_hwm, log_file)
        print(f"{len(water_edge_df)} points found in " + str(huc))
        log_file.write(f"{len(water_edge_df)} points found in " + str(huc) + '\n')

        ## Create X and Y location columns by extracting from geometry.
        water_edge_df['X'] = water_edge_df['geometry'].x
        water_edge_df['Y'] = water_edge_df['geometry'].y

        ## Check to make sure the HUC directory exists in the current fim_directory
        if not os.path.exists(os.path.join(fim_directory, huc)):
            log_file.write(
                "FIM Directory for huc: "
                + str(huc)
                + " does not exist --> skipping SRC adjustments for this HUC (obs points found)\n"
            )

        ## Intermediate output for debugging
        if debug_outputs_option:
            huc_debug_pts_out = os.path.join(fim_directory, huc, 'debug_water_edge_df_' + huc + '.csv')
            water_edge_df.to_csv(huc_debug_pts_out)
            huc_debug_pts_out_gpkg = os.path.join(fim_directory, huc, 'export_water_edge_df_' + huc + '.gpkg')
            water_edge_df.to_file(huc_debug_pts_out_gpkg, driver='GPKG', index=False, engine='fiona')
            # write parquet file using ".to_parquet() method"
            parquet_filepath = os.path.join(fim_directory, huc, 'debug_water_edge_df_' + huc + '.parquet')
            water_edge_df.to_parquet(parquet_filepath, index=False)

        for branch_id in os.listdir(huc_branches_dir):
            branch_dir = os.path.join(huc_branches_dir, branch_id)
            ## Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
            hand_path = os.path.join(branch_dir, 'rem_zeroed_masked_' + branch_id + '.tif')
            catchments_path = os.path.join(
                branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_' + branch_id + '.tif'
            )
            htable_path = os.path.join(branch_dir, 'hydroTable_' + branch_id + '.csv')
            catchments_poly_path = os.path.join(
                branch_dir,
                'gw_catchments_reaches_filtered_addedAttributes_crosswalked_' + branch_id + '.gpkg',
            )

            ## Below is from the v4.8.7.3 merge and introduces a breaking change to NGWPC's HLP functionality.
            ## Since we commented out the call to $toolsDir/convert_to_int16.py in delineate_hydros_and_produce_HAND.sh
            ##      this file does not exist, but does not need to be commented out here, we commented the reading of
            ##      the file above.
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
                        use_usgs_hwm,
                    ]
                )

    # with Pool(processes=job_number) as pool:
    #     log_output = pool.map(process_points, procs_list)
    #     log_file.writelines(["%s\n" % item for item in log_output])

    try:
        with Pool(processes=job_number) as pool:
            log_output = pool.map(process_points, procs_list)
            log_file.writelines(["%s\n" % item for item in log_output])
    except Exception as e:
        print(str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + str(e))
        log_file.write(
            'ERROR!!!: HUC ' + str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + str(e) + '\n'
        )

    log_file.write('#########################################################\n')


def run_prep(
    fim_directory, debug_outputs_option, ds_thresh_override, DOWNSTREAM_THRESHOLD, job_number, usgs_hwm
):
    '''
    Main function to call the processing functions defined above, with validation, logging, and timing

    Validation:
        - fim_directory exists
        - job_number does not exceed available cpus.
        - ds_thresh_override value is different than defualy and warn user
    '''

    assert os.path.isdir(fim_directory), 'ERROR: could not find the input fim_dir location: ' + str(
        fim_directory
    )

    available_cores = multiprocessing.cpu_count()
    if job_number > available_cores:
        job_number = available_cores - 1
        print(
            "Provided job number exceeds the number of available cores. "
            + str(job_number)
            + " max jobs will be used instead."
        )

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
    output_dir = os.path.join(fim_directory, "logs", "src_optimization")
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

    ingest_points_layer(fim_directory, job_number, debug_outputs_option, log_file, usgs_hwm)

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
    parser.add_argument(
        '-fim_dir', '--fim-directory', help='Parent directory of FIM-required datasets.', required=True
    )
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
        '-j', '--job-number', help='OPTIONAL: Number of jobs to use', type=int, required=False, default=2
    )
    parser.add_argument(
        '--use-usgs-hwm',
        help='OPTIONAL: Use if USGS High Water Mark data are desired to supplement spatial obs.',
        default=False,
        required=False,
        action="store_true",
    )

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    fim_directory = args['fim_directory']
    debug_outputs_option = args['extra_outputs']
    ds_thresh_override = args['downstream_thresh']
    job_number = args['job_number']
    use_usgs_hwm = args['use_usgs_hwm']

    run_prep(
        fim_directory,
        debug_outputs_option,
        ds_thresh_override,
        DOWNSTREAM_THRESHOLD,
        job_number,
        use_usgs_hwm,
    )
