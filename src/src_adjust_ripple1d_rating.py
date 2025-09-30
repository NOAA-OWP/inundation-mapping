#!/usr/bin/env python3

import argparse
import datetime as dt
import multiprocessing
import os
import sys
from collections import deque
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

from src_roughness_optimization import update_rating_curve
from utils.shared_functions import check_file_age, concat_huc_csv, find_matching_subdirectories


'''
The script ingests a ripple1d rating curve parquet file and a NWM flow recurrence interval database.
The gage location will be associated to the corresponding hydroID and attributed with the HAND elevation value

Processing
- Read in ripple1d rating curve from parquet and convert WSE navd88 values to meters
- Read in the aggregate ripple elev table csv from the HUC fim directory
- Filter null entries and convert ripple1d flow from cfs to cms
- Calculate HAND elevation value for each gage location (NAVD88 elevation - NHD DEM thalweg elevation)
- Read in the NWM recurr csv file and convert flow to cfs
- Calculate the closest SRC discharge value to the NWM flow value
- Create dataframe with crosswalked ripple1d flow and NWM recurr flow and assign metadata attributes
- Calculate flow difference (variance) to check for large discrepancies btw NWM flow and ripple1d closest flow
- Log signifant differences (or negative HAND values) btw the NWM flow value and closest ripple1d rating flow
- Produce log file
- Call update_rating_curve() to perform the rating curve calibration.

Inputs
- run_dir:                  fim directory containing individual HUC output dirs
- ripple1d inputs dir:      input directory with HUC level ripple1d rating curves
- ripple1d RC filename:     ripple1d rating curve database filename
- nwm_recurr_filepath:      NWM flow recurrence interval dataset
- huc_level:                HUC level used (8, 10, or 12)
- debug_outputs_option:     optional flag to output intermediate files for reviewing/debugging
- job_number:               number of multi-processing jobs to use

Outputs
- water_edge_median_ds: dataframe containing 'location_id','hydroid','reach_id','huc','hand',
                        'discharge_cms','nwm_recur_flow_cms','nwm_recur','layer'
'''


def create_ripple1d_rating_database(
    huc_ripple1d_input_file, ripple1d_elev_df, nwm_recurr_filepath, log_dir, huc_level
):
    start_time = dt.datetime.now()
    print('Reading ripple1d rating curves from parquet...')
    log_text = 'Processing database for ripple1d flow/WSE at NWM flow recur intervals...\n'
    # Note that we are using flow_cfs not flow_cms (error in raw data)
    col_filter = ["reach_id", "flow_cfs", "wse_m", "ras_xs_station"]
    ras_rc_df = pd.read_parquet(huc_ripple1d_input_file, columns=col_filter)
    run_time = dt.datetime.now() - start_time
    print(f"Duration (read ripple1d rating curve parquet): {str(run_time).split('.')[0]}")
    # Cast ras_xs_station to float, then integer to remove decimal values
    ras_rc_df = ras_rc_df.astype({'ras_xs_station': 'float'}).astype({'ras_xs_station': 'int'})

    # Assign fid_xs column
    ras_rc_df['fid_xs'] = ras_rc_df['reach_id'].astype(str) + '_' + ras_rc_df['ras_xs_station'].astype(str)

    # rename fid_xs & WSE columns
    ras_rc_df.rename(columns={'fid_xs': 'location_id'}, inplace=True)

    ras_rc_df.rename(columns={'wse_m': 'wse_navd88_m'}, inplace=True)

    # Need to use the flow_cfs because there is an error in the raw flow_cms
    ras_rc_df['discharge_cms'] = ras_rc_df['flow_cfs'] * 0.0283168
    ras_rc_df = ras_rc_df.drop(columns=["flow_cfs"])

    # read in the aggregate Ripple1d elev table csv
    start_time = dt.datetime.now()

    cross_df = ripple1d_elev_df[
        ["location_id", "HydroID", "feature_id", "levpa_id", f"HUC{huc_level}", "dem_adj_elevation", "source"]
    ].copy()
    cross_df.rename(
        columns={'dem_adj_elevation': 'hand_datum', 'HydroID': 'hydroid', f'HUC{huc_level}': 'huc'},
        inplace=True,
    )

    # filter null location_id rows from cross_df
    cross_df = cross_df[cross_df.location_id.notnull()]

    # merge ripple1d ratings with crosswalk attributes
    ras_rc_df = ras_rc_df.merge(cross_df, how='left', on='location_id')
    ras_rc_df = ras_rc_df[ras_rc_df['hydroid'].notna()]

    # calculate hand elevation
    ras_rc_df['hand'] = ras_rc_df['wse_navd88_m'] - ras_rc_df['hand_datum']
    ras_rc_df = ras_rc_df[
        ['location_id', 'reach_id', 'hydroid', 'levpa_id', 'huc', 'hand', 'discharge_cms', 'source']
    ]

    ras_rc_df['feature_id'] = ras_rc_df['reach_id'].astype(int)

    # read in the NWM recurr csv file
    nwm_recur_df = pd.read_csv(nwm_recurr_filepath, dtype={'feature_id': int})
    if "Unnamed: 0" in nwm_recur_df.columns:
        nwm_recur_df = nwm_recur_df.drop(columns=["Unnamed: 0"])
    nwm_recur_df.rename(
        columns={
            '2_0_year_recurrence_flow_17C': '2_0_year',
            '5_0_year_recurrence_flow_17C': '5_0_year',
            '10_0_year_recurrence_flow_17C': '10_0_year',
            '25_0_year_recurrence_flow_17C': '25_0_year',
            '50_0_year_recurrence_flow_17C': '50_0_year',
        },
        inplace=True,
    )

    # convert cfs to cms (x 0.028317)
    nwm_recur_df.loc[:, ['2_0_year', '5_0_year', '10_0_year', '25_0_year', '50_0_year']] *= 0.028317

    # merge nwm recurr with ras_rc_df
    merge_df = ras_rc_df.merge(nwm_recur_df, how='left', on='feature_id')

    # NWM recurr intervals
    recurr_intervals = ["2", "5", "10", "25", "50"]  # "2","5","10","25","50"
    final_df = pd.DataFrame()  # create empty dataframe to append flow interval dataframes
    for interval in recurr_intervals:
        log_text += '\n\nProcessing: ' + str(interval) + '-year NWM recurr intervals\n'
        print('Processing: ' + str(interval) + '-year NWM recurr intervals')
        ## Calculate the closest SRC discharge value to the NWM flow value
        merge_df['Q_find'] = (merge_df['discharge_cms'] - merge_df[interval + "_0_year"]).abs()

        ## Check for any missing/null entries in the input SRC
        # there may be null values for lake or coastal flow lines
        # (need to set a value to do groupby idxmin below)
        if merge_df['Q_find'].isnull().values.any():
            log_text += (
                'HUC: '
                + str(merge_df['huc'])
                + ' : feature_id'
                + str(merge_df['feature_id'])
                + ' --> Null values found in "Q_find" calc. These will be filled with 999999 () \n'
            )
            ## Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
            merge_df['Q_find'] = merge_df['Q_find'].fillna(999999)
        if merge_df['hydroid'].isnull().values.any():
            log_text += 'HUC: ' + str(merge_df['huc']) + ' --> Null values found in "hydroid"... \n'

        # Create dataframe with crosswalked ripple1d flow and NWM recurr flow
        calc_df = merge_df.loc[merge_df.groupby(['location_id', 'levpa_id'])['Q_find'].idxmin()].reset_index(
            drop=True
        )  # find the index of the Q_1_5_find (closest matching flow)
        # Calculate flow difference (variance) to check for large discrepancies between
        # NWM flow and ripple1d rating closest flow
        calc_df['check_variance'] = (
            (calc_df['discharge_cms'] - calc_df[interval + "_0_year"]) / calc_df['discharge_cms']
        ).abs()
        # Assign new metadata attributes
        calc_df['nwm_recur'] = interval + "_0_year"
        calc_df['layer'] = '_ripple1d-gage____' + interval + "-year"
        calc_df.rename(columns={interval + "_0_year": 'nwm_recur_flow_cms'}, inplace=True)
        # Subset calc_df for final output
        calc_df = calc_df[
            [
                'location_id',
                'hydroid',
                'feature_id',
                'levpa_id',
                'huc',
                'hand',
                'discharge_cms',
                'check_variance',
                'nwm_recur_flow_cms',
                'nwm_recur',
                'layer',
                'source',
            ]
        ]
        final_df = pd.concat([final_df, calc_df], ignore_index=True)
        # Log any negative HAND elev values and remove from database
        log_text += 'Warning: Negative HAND stage values -->\n'
        log_text += calc_df[calc_df['hand'] < 0].to_string() + '\n'
        final_df = final_df[final_df['hand'] > 0]
        # Log any signifant differences btw the NWM flow value and closest ripple1d rating flow
        # (this ensures that we consistently sample the ripple1d rating curves at
        # known intervals - NWM recur flow)
        log_text += 'Warning: Large variance (>10%) between NWM flow and closest ripple1d flow -->\n'
        log_text += calc_df[calc_df['check_variance'] > 0.1].to_string() + '\n'
        final_df = final_df[final_df['check_variance'] < 0.1]
        # Get datestamp from ripple1d rating curve file to use as coll_time attribute in hydroTable.csv
        datestamp = check_file_age(ripple_rc_filepath)
        final_df['coll_time'] = str(datestamp)[:15]

    # Rename attributes (for ingest to update_rating_curve) and output csv with the ripple1d RC database
    final_df.rename(columns={'discharge_cms': 'flow', 'source': 'submitter'}, inplace=True)
    final_df.to_csv(os.path.join(log_dir, "ripple1d_rc_nwm_recurr.csv"), index=False)

    # Output log text to log file
    log_text += '#########\nTotal entries per ripple1d point location (feature_id) -->\n'
    loc_id_df = final_df.groupby(['location_id']).size().reset_index(name='count')
    log_text += loc_id_df.to_string() + '\n'
    log_text += '#########\nTotal entries per NWM recur value -->\n'
    recur_count_df = final_df.groupby(['nwm_recur']).size().reset_index(name='count')
    log_text += recur_count_df.to_string() + '\n'
    log_ras_db = open(os.path.join(log_dir, 'log_ripple1d_rc_database.log'), "w")
    log_ras_db.write(log_text)
    log_ras_db.close()
    return final_df


def branch_proc_list(ripple1d_df, huc_run_dir, debug_outputs_option, log_file):
    procs_list = []  # Initialize list for mulitprocessing.

    # loop through all unique level paths that have a ripple1d data points
    huc_branch_dict = ripple1d_df.groupby('huc')['levpa_id'].apply(set).to_dict()

    for huc in sorted(
        huc_branch_dict.keys()
    ):  # sort huc_list for helping track progress in future print statments
        branch_set = huc_branch_dict[huc]
        for branch_id in branch_set:
            # Define paths to branch HAND data.
            # Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
            # Outputs can be HUC8, HUC10, or HUC12
            branch_dir = os.path.join(huc_run_dir, 'branches', branch_id)
            hand_path = os.path.join(branch_dir, 'rem_zeroed_masked_' + branch_id + '.tif')
            catchments_path = os.path.join(
                branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_' + branch_id + '.tif'
            )
            catchments_poly_path = os.path.join(
                branch_dir,
                'gw_catchments_reaches_filtered_addedAttributes_crosswalked_' + branch_id + '.gpkg',
            )
            htable_path = os.path.join(branch_dir, 'hydroTable_' + branch_id + '.csv')
            water_edge_median_ds = ripple1d_df[
                (ripple1d_df['huc'] == huc) & (ripple1d_df['levpa_id'] == branch_id)
            ]

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
                ## Additional arguments for src_roughness_optimization
                source_tag = 'ripple1d_rating'  # tag to use in source attribute field
                merge_prev_adj = True  # merge in previous SRC adjustment calculations

                print('Will perform SRC adjustments for huc: ' + str(huc) + ' - branch-id: ' + str(branch_id))
                procs_list.append(
                    [
                        branch_dir,
                        water_edge_median_ds,
                        htable_path,
                        huc,
                        branch_id,
                        catchments_poly_path,
                        debug_outputs_option,
                        source_tag,
                        merge_prev_adj,
                    ]
                )

    # multiprocess all available branches
    print(f"Calculating new SRCs for {len(procs_list)} branches using {job_number} jobs...")
    try:
        with Pool(processes=job_number) as pool:
            log_output = pool.starmap(update_rating_curve, procs_list)
            log_file.writelines(["%s\n" % item for item in log_output])
    except Exception as e:
        print(str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + str(e))
        log_file.write(
            '\n ERROR!!!: HUC ' + str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + ' ' + str(e) + '\n'
        )


def run_prep(
    run_dir,
    ripple_input_dir,
    ripple_rc_filepath,
    nwm_recurr_filepath,
    huc_level,
    debug_outputs_option,
    job_number,
):
    ## Check input args are valid
    assert os.path.isdir(run_dir), 'ERROR: could not find the input fim_dir location: ' + str(run_dir)

    available_cores = multiprocessing.cpu_count()
    if job_number > available_cores:
        job_number = available_cores - 1
        print(
            "Provided job number exceeds the number of available cores. "
            + str(job_number)
            + " max jobs will be used instead."
        )

    ## Create output dir for log and ripple1d rc database
    log_dir = os.path.join(run_dir, "logs", "src_optimization")
    print("Log file output here: " + str(log_dir))
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    ## Create a time var to log run time
    begin_time = dt.datetime.now()
    # Create log file for processing records
    log_file = open(os.path.join(log_dir, 'log_ripple1d_rc_src_adjust.log'), "w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    hucs_with_data = find_matching_subdirectories(run_dir, ripple_input_dir)
    if len(hucs_with_data) == 0:
        print('ALERT: Did not find any HUCs with ripple1d data to perform adjustments')
        log_file.write('ALERT: Did not find any HUCs with ripple1d data to perform adjustments\n')
        return

    log_file.write('ripple1d data available and will perform SRC adjustments for hucs:\n')
    log_file.write(str(hucs_with_data))
    log_file.write('\n#########################################################\n\n')

    for huc in hucs_with_data:
        huc_run_dir = os.path.join(run_dir, huc)
        huc_ripple1d_input_file = os.path.join(huc_run_dir, ripple_rc_filepath)

        ## Create an aggregate dataframe with all ripple1d_elev_table.csv entries for hucs in fim_dir
        print(f'\n Reading ripple1d point loc HAND elevation from {huc} ripple1d_elev_table.csv files...')
        csv_elev = (
            'ripple1d_elev_table.csv'  # file name to search ripple1d location data (in the huc/branch dirs)
        )
        # ripple1d_elev_df = concat_huc_csv(huc_run_dir, huc_level, csv_elev)
        if os.path.isfile(os.path.join(huc_run_dir, csv_elev)):
            ripple1d_elev_df = pd.read_csv(
                os.path.join(huc_run_dir, csv_elev),
                dtype={
                    f'HUC{huc_level}': object,
                    'location_id': object,
                    'feature_id': int,
                    'levpa_id': object,
                },
            )
        else:
            print(f" Processing errors for HUC : {huc}, it does not have the necessary {csv_elev} file. \n")
            ripple1d_elev_df = None

        ## Create an aggregate dataframe with all ripple1d rating curve csv files
        # print('Reading ripple1d rating curves csv files from the input directory...')
        # ras_rating_df = concat_huc_csv(ripple_input_dir, huc_level, ripple_rc_filepath)

        if ripple1d_elev_df is None:
            warn_err = (
                'WARNING: ripple1d_elev_df not created - check that ' + csv_elev + ' files exist in fim_dir!'
            )
            print(warn_err)
            log_file.write(warn_err)

        elif ripple1d_elev_df.empty:
            warn_err = (
                'WARNING: ripple1d_elev_df is empty - check that ' + csv_elev + ' files exist in fim_dir!'
            )
            print(warn_err)
            log_file.write(warn_err)

        else:
            print('This may take a few minutes...')
            log_file.write("Starting create ripple1d rating db")
            ripple1d_df = create_ripple1d_rating_database(
                huc_ripple1d_input_file, ripple1d_elev_df, nwm_recurr_filepath, log_dir, huc_level
            )

            ## Create huc proc_list for multiprocessing and execute the update_rating_curve function
            branch_proc_list(ripple1d_df, huc_run_dir, debug_outputs_option, log_file)

    ## Record run time and close log file
    log_file.write('#########################################################\n\n')
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()


if __name__ == '__main__':
    ## Parse arguments.
    parser = argparse.ArgumentParser(
        description='Adjusts rating curve with database of ripple1d reach average rating curves'
        '(calculated WSE/flow).'
    )
    parser.add_argument('-run_dir', '--run-dir', help='Parent directory of FIM run.', required=True)
    parser.add_argument(
        '-ripple1d_input',
        '--ripple1d-dir',
        help='Path to ripple1d rating curve input directory',
        required=True,
    )
    parser.add_argument(
        '-ripple1d_rc',
        '--ripple1d-ratings',
        help='Parquet file name for ripple1d rating curve',
        required=True,
    )
    parser.add_argument(
        '-nwm_recur',
        '--nwm_recur',
        help='Path to NWM recur file (multiple NWM flow intervals). NOTE: assumes flow units are cfs!!',
        required=True,
    )
    parser.add_argument('-huc_level', '--huc-level', help='HUC level to use', required=True)

    parser.add_argument(
        '-debug',
        '--extra-outputs',
        help='Optional flag: Use this to keep intermediate output files for debugging/testing',
        default=False,
        required=False,
        action='store_true',
    )
    parser.add_argument('-j', '--job-number', help='Number of jobs to use', required=False, default=1)

    ## Assign variables from arguments.
    args = vars(parser.parse_args())
    run_dir = args['run_dir']
    ripple_input_dir = args['ripple1d_dir']
    ripple_rc_filepath = args['ripple1d_ratings']
    nwm_recurr_filepath = args['nwm_recur']
    huc_level = args['huc_level']
    debug_outputs_option = args['extra_outputs']
    job_number = int(args['job_number'])

    ## Prepare/check inputs, create log file, and spin up the proc list
    run_prep(
        run_dir,
        ripple_input_dir,
        ripple_rc_filepath,
        nwm_recurr_filepath,
        huc_level,
        debug_outputs_option,
        job_number,
    )
