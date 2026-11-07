#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import sys
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

from src_roughness_optimization import update_rating_curve
from utils.shared_functions import check_file_age
from utils.shared_variables import DOWNSTREAM_THRESHOLD

from src.utils.shared_functions import setup_file_logger


'''
The script ingests a RAS2FIM rating curve csv and a NWM flow recurrence interval database.
The gage location will be associated to the corresponding hydroID and attributed with the HAND elevation value

Processing
- Read in RAS2FIM rating curve from csv and convert WSE navd88 values to meters
- Read in the aggregate RAS elev table csv from the HUC fim directory (output from ras_gage_crosswalk.py)
- Filter null entries and convert RAS2FIM flow from cfs to cms
- Calculate HAND elevation value for each gage location (NAVD88 elevation - NHD DEM thalweg elevation)
- Read in the NWM recurr csv file and convert flow to cfs
- Calculate the closest SRC discharge value to the NWM flow value
- Create dataframe with crosswalked RAS2FIM flow and NWM recurr flow and assign metadata attributes
- Calculate flow difference (variance) to check for large discrepancies btw NWM flow and RAS2FIM closest flow
- Log signifant differences (or negative HAND values) btw the NWM flow value and closest RAS2FIM rating flow
- Produce log file
- Call update_rating_curve() to perform the rating curve calibration.

Inputs
- branch_dir:           fim directory containing individual HUC output dirs
- ras_rc_filename:      Name of RAS2FIM rating curve database referenced in src/bash_variables.env
- nwm_recurr_filepath:  NWM flow recurrence interval dataset
- debug_outputs_option: optional flag to output intermediate files for reviewing/debugging
- branch_jobs:           number of multi-processing branches jobs to use

Outputs
- water_edge_median_ds: dataframe containing 'location_id','hydroid','feature_id','huc','hand',
                        'discharge_cms','nwm_recur_flow_cms','nwm_recur','layer'
'''

# Unpacks the dictionary in each list items. Only needed as we are using imap and dictionaries
def _run_update_rating_curve(args):
    return update_rating_curve(**args)


def create_ras2fim_rating_database(huc_ras_input_file, ras_elev_df, nwm_recurr_filepath, log_dir, ras_rc_filename):
    section_start_time = dt.datetime.now()

    logging.debug('Processing database for RAS2FIM flow/WSE at NWM flow recur intervals...')
    # Note that we are using flow_cfs not flow_cms (error in raw data)
    col_filter = ["fid_xs", "flow_cfs", "wse_m"]
    ras_rc_df = pd.read_csv(
        huc_ras_input_file, dtype={'fid_xs': object}, usecols=col_filter, encoding="unicode_escape"
    )  # , nrows=30000)
    ras_rc_df = ras_rc_df.rename(columns={'fid_xs': 'location_id'})
    # ras_rc_df['location_id'] = ras_rc_df['feature_id'].astype(object)
    run_time = dt.datetime.now() - section_start_time
    logging.debug(f"Duration (read ras_rc_csv): {str(run_time).split('.')[0]}")

    # read in the aggregate RAS elev table csv
    section_start_time = dt.datetime.now()

    # rename WSE column
    ras_rc_df = ras_rc_df.rename(columns={'wse_m': 'wse_navd88_m'})

    # Need to use the flow_cfs because there is an error in the raw flow_cms
    ras_rc_df['discharge_cms'] = ras_rc_df['flow_cfs'] * 0.0283168
    ras_rc_df = ras_rc_df.drop(columns=["flow_cfs"])

    cross_df = ras_elev_df[
        ["location_id", "HydroID", "feature_id", "levpa_id", "HUC8", "dem_adj_elevation", "source"]
    ].copy()
    cross_df = cross_df.rename(
        columns={'dem_adj_elevation': 'hand_datum', 'HydroID': 'hydroid', 'HUC8': 'huc'})

    # filter null location_id rows from cross_df
    cross_df = cross_df[cross_df.location_id.notnull()]

    # merge ras2fim ratings with crosswalk attributes
    ras_rc_df = ras_rc_df.merge(cross_df, how='left', on='location_id')
    ras_rc_df = ras_rc_df[ras_rc_df['hydroid'].notna()]

    # calculate hand elevation
    ras_rc_df['hand'] = ras_rc_df['wse_navd88_m'] - ras_rc_df['hand_datum']
    ras_rc_df = ras_rc_df[
        ['location_id', 'feature_id', 'hydroid', 'levpa_id', 'huc', 'hand', 'discharge_cms', 'source']
    ]
    ras_rc_df['feature_id'] = ras_rc_df['feature_id'].astype(int)

    # read in the NWM recurr csv file
    nwm_recur_df = pd.read_csv(nwm_recurr_filepath, dtype={'feature_id': int})
    if "Unnamed: 0" in nwm_recur_df.columns:
        nwm_recur_df = nwm_recur_df.drop(columns=["Unnamed: 0"])
    nwm_recur_df = nwm_recur_df.rename(
        columns={
            '2_0_year_recurrence_flow_17C': '2_0_year',
            '5_0_year_recurrence_flow_17C': '5_0_year',
            '10_0_year_recurrence_flow_17C': '10_0_year',
            '25_0_year_recurrence_flow_17C': '25_0_year',
            '50_0_year_recurrence_flow_17C': '50_0_year',
        }
    )

    # convert cfs to cms (x 0.028317)
    nwm_recur_df.loc[:, ['2_0_year', '5_0_year', '10_0_year', '25_0_year', '50_0_year']] *= 0.028317

    # merge nwm recurr with ras_rc_df
    merge_df = ras_rc_df.merge(nwm_recur_df, how='left', on='feature_id')

    # NWM recurr intervals
    recurr_intervals = ["2", "5", "10", "25", "50"]  # "2","5","10","25","50","100"
    final_df = pd.DataFrame()  # create empty dataframe to append flow interval dataframes
    for interval in recurr_intervals:
        logging.debug('Processing: ' + str(interval) + '-year NWM recurr intervals')
        ## Calculate the closest SRC discharge value to the NWM flow value
        merge_df['Q_find'] = (merge_df['discharge_cms'] - merge_df[interval + "_0_year"]).abs()

        ## Check for any missing/null entries in the input SRC
        # there may be null values for lake or coastal flow lines
        # (need to set a value to do groupby idxmin below)
        if merge_df['Q_find'].isnull().values.any():

            # TODO: Nov 2026: Should this raise an exception? or at least return? or at least a continue?

            logging.debug(
                'HUC: '
                + str(merge_df['huc'])
                + ' : feature_id'
                + str(merge_df['feature_id'])
                + ' --> Null values found in "Q_find" calc. These will be filled with 999999 ()'
            )
            ## Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
            merge_df['Q_find'] = merge_df['Q_find'].fillna(999999)
        if merge_df['hydroid'].isnull().values.any():
            logging.debug('HUC: ' + str(merge_df['huc']) + ' --> Null values found in "hydroid"... ')

        # Create dataframe with crosswalked RAS2FIM flow and NWM recurr flow
        calc_df = merge_df.loc[merge_df.groupby(['location_id', 'levpa_id'])['Q_find'].idxmin()].reset_index(
            drop=True
        )  # find the index of the Q_1_5_find (closest matching flow)
        # Calculate flow difference (variance) to check for large discrepancies between
        # NWM flow and RAS2FIM rating closest flow
        calc_df['check_variance'] = (
            (calc_df['discharge_cms'] - calc_df[interval + "_0_year"]) / calc_df['discharge_cms']
        ).abs()
        # Assign new metadata attributes
        calc_df['nwm_recur'] = interval + "_0_year"
        calc_df['layer'] = '_ras2fim-gage____' + interval + "-year"
        calc_df = calc_df.rename(columns={interval + "_0_year": 'nwm_recur_flow_cms'})
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
        logging.debug('Warning: Negative HAND stage values -->')
        logging.debug(calc_df[calc_df['hand'] < 0].to_string())
        logging.debug("")

        final_df = final_df[final_df['hand'] > 0]
        # Log any signifant differences btw the NWM flow value and closest RAS2FIM rating flow
        # (this ensures that we consistently sample the RAS2FIM rating curves at
        # known intervals - NWM recur flow)

        logging.debug("Warning: Large variance (>10%) between NWM flow and closest RAS2FIM flow -->")
        logging.debug(calc_df[calc_df['check_variance'] > 0.1].to_string())
        logging.debug("")

        final_df = final_df[final_df['check_variance'] < 0.1]
        # Get datestamp from ras2fim rating curve file to use as coll_time attribute in hydroTable.csv
        # TODO below needs update since now it is a file name and not a path and will return None
        datestamp = check_file_age(ras_rc_filename)
        final_df['coll_time'] = str(datestamp)[:15]

    # Rename attributes (for ingest to update_rating_curve) and output csv with the RAS2FIM RC database
    final_df = final_df.rename(columns={'discharge_cms': 'flow', 'source': 'submitter'})
    final_df.to_csv(os.path.join(log_dir, "ras2fim_rc_nwm_recurr.csv"), index=False)
    logging.debug("ras2fim_rc_nwm_recurr.csv saved")

    # Output log text to log file
    logging.debug('#########')
    logging.debug("Total entries per RAS2FIM point location (feature_id) -->")
    loc_id_df = final_df.groupby(['location_id']).size().reset_index(name='count')
    logging.debug(loc_id_df.to_string())
    logging.debug("Total entries per NWM recur value -->")
    recur_count_df = final_df.groupby(['nwm_recur']).size().reset_index(name='count')
    logging.debug(recur_count_df.to_string())
    return final_df


def branch_proc_list(ras_df, huc_dir, is_debug, branch_jobs):
    huc = os.path.basename(os.path.normpath(huc_dir))

    procs_list = []  # Initialize list for mulitprocessing.

    ## Additional arguments for src_roughness_optimization
    source_tag = 'ras2fim_rating'  # tag to use in source attribute field
    merge_prev_adj = True  # merge in previous SRC adjustment calculations

    huc_branches_dir = os.path.join(huc_dir, 'branches')
    for branch_id in os.listdir(huc_branches_dir):
        # Define paths to branch HAND data.
        # Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
        branch_dir = os.path.join(huc_dir, 'branches', branch_id)

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
                
        catchments_poly_path = os.path.join(
            branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked_' + branch_id + '.parquet'
        )

        water_edge_median_ds = ras_df[(ras_df['huc'] == huc) & (ras_df['levpa_id'] == branch_id)]

        if water_edge_median_ds.empty:
            logging.warning(f"WARNING: there are no level path ids for branch id {branch_id}")
            continue   

        logging.debug('Will perform SRC adjustments for huc: ' + str(huc) + ' - branch-id: ' + str(branch_id))

        args = {
                "fim_directory": branch_dir,
                "water_edge_median_df": water_edge_median_ds,
                "htable_path": htable_path,
                "huc": huc,
                "branch_id": branch_id,
                "catchments_poly_path": catchments_poly_path,
                "debug_outputs_option": is_debug,
                "source_tag": source_tag,
                "merge_prev_adj=False": merge_prev_adj,
                "down_dist_thresh": DOWNSTREAM_THRESHOLD
                }
        procs_list.append(args)

    # multiprocess all available branches
    logging.debug(f"Calculating new SRCs for {len(procs_list)} branches using {branch_jobs} jobs...")

    map_output = None
    # TODO: Nov 2026: This should be ugpraded, mostly for tracing, debugging and logs    
    with Pool(processes=branch_jobs) as pool:
        map_output = pool.imap(_run_update_rating_curve, procs_list)

    # TODO: Nov 2026: This is temp as with this MP setup, child workers can not write to logs
    # so we will send back log text and an error bool so we can shut down the downstream tools if it fails.
    # Once we do, we can use logging.info, .debug, .warning, etc
    for log_text_list, has_error in map_output:
        if has_error:
            for item in log_text_list:
                logging.error(item)  # Note.. error type
            # As we are outside an MP, we can use sys.exit(1) to stop downstream code processing
            sys.exit(1)
        else:
            for item in log_text_list:
                logging.info(item)  # Note.. info type


def run_prep(huc_dir, ras_rc_filename, nwm_recurr_filepath, is_debug, branch_jobs):
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## Create output dir for log and ras2fim rc database
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    log_file_path = setup_file_logger(log_dir, "log_ras2fim_rc_src_adjust")
    logging.info("Start src adjust ras2fim rating")
    print(f"Logs will be saved to {log_file_path}")
    logging.debug('START TIME: ' + str(begin_time))
    logging.debug('#########################################################')    

    # since we already copied the ras2fim data, if it's available inside tempHucDataDir, we can just check for its availablity to do the job
    huc = os.path.basename(os.path.normpath(huc_dir))
    csv_elev = 'ras_elev_table.csv'
    ras_elev_path = os.path.join(huc_dir, csv_elev)

    if not os.path.exists(ras_elev_path):
        logging.info(f'Skipping... RAS2FIM data is not available for huc {huc}')
        return

    # huc_run_dir = os.path.join(run_dir, huc)
    huc_ras_input_file = os.path.join(huc_dir, ras_rc_filename)
    if not os.path.exists(huc_ras_input_file):
        logging.info(f'Skipping... RAS2FIM rating curve data does not exist ({ras_rc_filename})')
        return

    logging.debug('Reading RAS2FIM point loc HAND elevation from ras_elev_table csv file...')
    ras_elev_df = pd.read_csv(
        ras_elev_path, dtype={'HUC8': object, 'location_id': object, 'feature_id': int, 'levpa_id': object}
    )

    if ras_elev_df is None:
        warn_err = 'WARNING: ras_elev_df not created - check that ' + csv_elev + ' files exist in huc_dir!'
        logging.warning(warn_err)
        return

    if ras_elev_df.empty:
        warn_err = 'WARNING: ras_elev_df is empty - check that ' + csv_elev + ' files exist in huc_dir!'
        logging.warning(warn_err)
        return

    print('This may take a few minutes...')
    logging.debug("Starting create RAS2FIM rating db")
    ras_df = create_ras2fim_rating_database(huc_ras_input_file, ras_elev_df, nwm_recurr_filepath, log_dir, ras_rc_filename)

    ## Create huc proc_list for multiprocessing and execute the update_rating_curve function
    branch_proc_list(ras_df, huc_dir, is_debug, branch_jobs)

    ## Record run time and close log file
    logging.debug('########################################################')
    end_time = dt.datetime.now()
    logging.debug('END TIME: ' + str(end_time))
    tot_run_time = end_time - begin_time
    logging.debug('TOTAL RUN TIME: ' + str(tot_run_time))


if __name__ == '__main__':
    ## Parse arguments.
    parser = argparse.ArgumentParser(
        description='Adjusts rating curve with database of RAS2FIM reach average rating curves'
        '(calculated WSE/flow).'
    )
    parser.add_argument('-huc_dir', '--huc-dir', help='directory of a HUC run.', required=True)
    parser.add_argument(
        '-ras_rc',
        '--ras-rc-filename',
        help='CSV file name for RAS2FIM rating curve (reach avg)',
        required=True,
    )
    parser.add_argument(
        '-nwm_recur',
        '--nwm-recurr-filepath',
        help='Path to NWM recur file (multiple NWM flow intervals). NOTE: assumes flow units are cfs!!',
        required=True,
    )
    parser.add_argument(
        '-debug',
        '--is-debug',
        help='Optional flag: Use this to keep intermediate output files for debugging/testing',
        default=False,
        required=False,
        action='store_true',
    )
    parser.add_argument(
        '-jb', '--branch-jobs', help='Number of branch jobs to use', required=False, default=1, type=int
    )

    ## Assign variables from arguments.
    args = vars(parser.parse_args())

    ## Prepare/check inputs, create log file, and spin up the proc list
    run_prep(**args)
