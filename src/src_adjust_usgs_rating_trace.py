import argparse
import datetime as dt
import logging
import os
import sys
from multiprocessing import Pool

import geopandas as gpd
import pandas as pd

from src_roughness_optimization import update_rating_curve
from src.utils.shared_functions import setup_file_logger
from tools.tools_shared_functions import filter_usgs_by_acceptance_criteria
from utils.shared_functions import check_file_age
from utils.shared_variables import USGS_CALB_TRACE_DIST, DOWNSTREAM_THRESHOLD


'''
The script ingests a USGS rating curve csv and a NWM flow recurrence interval database.
The gage location will be associated to the corresponding hydroID and attributed with the HAND elevation value

Processing
- Read in USGS rating curve from csv and convert WSE navd88 values to meters
- Read in the aggregate USGS elev table csv from the HUC fim directory (output from usgs_gage_crosswalk.py)
- Filter null entries and convert usgs flow from cfs to cms
- Calculate HAND elevation value for each gage location (NAVD88 elevation - NHD DEM thalweg elevation)
- Read in the NWM recurr csv file and convert flow to cfs
- Calculate the closest SRC discharge value to the NWM flow value
- Create dataframe with crosswalked USGS flow and NWM recurr flow and assign metadata attributes
- Calculate flow difference (variance) to check for large discrepancies btw NWM flow and USGS closest flow
- Log any signifant differences (or negative HAND values) btw the NWM flow value and closest USGS rating flow
- Produce log file
- Call update_rating_curve() to perform the rating curve calibration.

Inputs
- branch_dir:           fim directory containing individual HUC output dirs
- usgs_rc_filepath:     USGS rating curve database (produced by get_usgs_rating_curves.py)
- nwm_recurr_filepath:  NWM flow recurrence interval dataset
- debug_outputs_option: optional flag to output intermediate files for reviewing/debugging
- branch_jobs:           number of multi-processing branches jobs to use

Outputs
- water_edge_median_ds: dataframe containing:
                            'location_id', 'hydroid', 'feature_id', 'huc', 'hand', 'discharge_cms',
                            'nwm_recur_flow_cms', 'nwm_recur', 'layer'
'''

# Unpacks the dictionary in each list items. Only needed as we are using imap and dictionaries
def _run_update_rating_curve(args):
    return update_rating_curve(**args)


def create_usgs_rating_database(
    usgs_rc_filepath, usgs_sites_filepath, usgs_elev_df, nwm_recurr_filepath, log_dir):
    start_time = dt.datetime.now()
    logging.info('Reading USGS rating curve from csv...')
    logging.debug('Processing database for USGS flow/WSE at NWM flow recur intervals...')
    col_usgs = ["location_id", "flow", "stage", "elevation_navd88"]
    usgs_rc_df = pd.read_csv(
        usgs_rc_filepath, dtype={'location_id': object}, usecols=col_usgs
    )  # , nrows=30000)
    logging.debug('Duration (read usgs_rc_csv): {}'.format(dt.datetime.now() - start_time))

    acceptable_sites = pd.read_csv(usgs_sites_filepath, dtype={'location_id': object})
    acceptable_sites_filt = filter_usgs_by_acceptance_criteria(acceptable_sites)
    location_ids_to_keep = acceptable_sites_filt['location_id'].drop_duplicates().tolist()

    # rm acceptable_sites_filt, acceptable_sites

    # Only keep rating curves from acceptable sites
    usgs_rc_df = usgs_rc_df[usgs_rc_df['location_id'].isin(location_ids_to_keep)]

    # convert WSE navd88 values to meters
    usgs_rc_df['elevation_navd88_m'] = usgs_rc_df['elevation_navd88'] / 3.28084

    # read in the aggregate USGS elev table csv
    # start_time = dt.datetime.now()
    cross_df = usgs_elev_df[
        ["location_id", "HydroID", "feature_id", "levpa_id", "HUC8", "dem_adj_elevation"]
    ]
    cross_df = cross_df.rename(
        columns={'dem_adj_elevation': 'hand_datum', 'HydroID': 'hydroid', 'HUC8': 'huc'}
    )

    # filter null location_id rows from cross_df
    # (removes ahps lide entries that aren't associated with USGS gage)
    cross_df = cross_df[cross_df.location_id.notnull()]

    # convert usgs flow from cfs to cms
    usgs_rc_df['discharge_cms'] = usgs_rc_df.flow / 35.3147
    usgs_rc_df = usgs_rc_df.drop(columns=["flow"])

    # merge usgs ratings with crosswalk attributes
    usgs_rc_df = usgs_rc_df.merge(cross_df, how='left', on='location_id')
    usgs_rc_df = usgs_rc_df[usgs_rc_df['hydroid'].notna()]

    # If all records filtered out, write log and return empty dataframe
    if len(usgs_rc_df) == 0:
        # msg already handed in run_prep
        # logging.warning("[WARNING] All gages were removed after filtering criteria.")
        return pd.DataFrame()
    
    # calculate hand elevation
    usgs_rc_df['hand'] = usgs_rc_df['elevation_navd88_m'] - usgs_rc_df['hand_datum']
    usgs_rc_df = usgs_rc_df[
        ['location_id', 'feature_id', 'hydroid', 'levpa_id', 'huc', 'hand', 'discharge_cms']
    ]
    usgs_rc_df['feature_id'] = usgs_rc_df['feature_id'].astype(int)

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

    # merge nwm recurr with usgs_rc
    merge_df = usgs_rc_df.merge(nwm_recur_df, how='left', on='feature_id')

    # NWM recurr intervals
    recurr_intervals = ("2", "5", "10", "25", "50")
    final_df = pd.DataFrame()  # create empty dataframe to append flow interval dataframes
    for interval in recurr_intervals:
        logging.debug('Processing: ' + str(interval) + '-year NWM recurr intervals')
        # Calculate the closest SRC discharge value to the NWM flow value
        merge_df['Q_find'] = (merge_df['discharge_cms'] - merge_df[interval + "_0_year"]).abs()

        # Check for any missing/null entries in the input SRC
        # There may be null values for lake or coastal flow lines
        # (need to set a value to do groupby idxmin below)
        if merge_df['Q_find'].isnull().values.any():
            logging.debug(
                'HUC: '
                + str(merge_df['huc'])
                + ' : feature_id'
                + str(merge_df['feature_id'])
                + ' --> Null values found in "Q_find" calc. These will be filled with 999999 ()'
            )
            # Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
            merge_df['Q_find'] = merge_df['Q_find'].fillna(999999)
        if merge_df['hydroid'].isnull().values.any():

            # TODO: Nov 2026: Should this raise an exception? or at least return? or at least a continue?

            logging.debug('HUC: ' + str(merge_df['huc']) + ' --> Null values found in "hydroid"')

        # Create dataframe with crosswalked USGS flow and NWM recurr flow &
        # find the index of the closest matching flow btw USGS rating and NWM recur
        calc_df = merge_df.loc[merge_df.groupby(['location_id', 'levpa_id'])['Q_find'].idxmin()].reset_index(
            drop=True
        )
        # Calculate flow difference (variance) to check for large discrepancies between
        # NWM flow and USGS closest flow
        calc_df['check_variance'] = (
            (calc_df['discharge_cms'] - calc_df[interval + "_0_year"]) / calc_df['discharge_cms']
        ).abs()
        # Assign new metadata attributes
        calc_df['nwm_recur'] = interval + "_0_year"
        calc_df['layer'] = '_usgs-gage____' + interval + "-year"
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
            ]
        ]
        final_df = pd.concat([final_df, calc_df], ignore_index=True)
        # Log any negative HAND elev values and remove from database

        logging.debug('Warning: Negative HAND stage values -->')
        logging.debug(calc_df[calc_df['hand'] < 0].to_string())
        logging.debug("")

        final_df = final_df[final_df['hand'] > 0]
        # Log any signifant differences btw the NWM flow value and closest USGS rating flow
        # This ensures that we consistently sample the USGS rating curves at known intervals - NWM recur flow
        logging.debug("Large variance (>10%) between NWM flow and closest USGS flow -->")
        logging.debug(calc_df[calc_df['check_variance'] > 0.1].to_string())
        logging.debug("")

        final_df = final_df[final_df['check_variance'] < 0.1]
        final_df['submitter'] = 'usgs_rating_wrds_api_' + final_df['location_id'].astype(str)
        # Get datestamp from usgs rating curve file to use as coll_time attribute in hydroTable.csv
        datestamp = check_file_age(usgs_rc_filepath)
        final_df['coll_time'] = str(datestamp)[:15]

    # Rename attributes (for ingest to update_rating_curve) and output csv with the USGS RC database
    final_df = final_df.rename(columns={'discharge_cms': 'flow'})
    # Saved for debugging only
    final_df.to_csv(os.path.join(log_dir, "usgs_rc_nwm_recurr.csv"), index=False)
    logging.debug("Saving usgs_rc_nwm_recurr.csv")

    logging.debug("#########")
    logging.debug('-- Total entries per USGS gage location -->')
    loc_id_df = final_df.groupby(['location_id']).size().reset_index(name='count')    
    logging.debug(loc_id_df.to_string())

    logging.debug("#########")
    logging.debug("Total entries per NWM recur value -->")
    recur_count_df = final_df.groupby(['nwm_recur']).size().reset_index(name='count')
    logging.debug(recur_count_df.to_string())
    return final_df


def trace_network(df, start_id):
    # This function creates a list of all upstream & downstream hydroids
    # Input: df --> dataframe of demDerived_reaches with network attribs
    # Input: start_id --> hydroid value where the trace routine will start
    current_id = start_id
    trace_up = []
    trace_down = []
    start_order = None  # Variable to store the start_order
    accumulated_length = 0

    while True:
        current_row = df[df['HydroID'] == current_id]

        if current_row.empty:
            break

        next_id = current_row['NextDownID'].values[0]
        order = current_row['order_'].values[0]
        length = current_row['LengthKm'].values[0]
        lake = current_row['LakeID'].values[0]

        # Assign start_order when first encountered
        if start_order is None:
            start_order = order

        if order != start_order:
            break

        accumulated_length += length
        if accumulated_length >= float(USGS_CALB_TRACE_DIST):
            break

        if lake > 0:
            break

        # not dropping the HydroID that has the gauge location (need later)
        trace_down.append(int(current_id))

        current_id = next_id

    current_id = start_id  # Reset current_id for tracing down
    accumulated_length = 0

    while True:
        current_row = df[(df['NextDownID'] == current_id) & (df['order_'] == start_order)]
        if current_row.empty:
            break

        next_id = current_row['HydroID'].values[0]
        order = current_row['order_'].values[0]
        length = current_row['LengthKm'].values[0]
        lake = current_row['LakeID'].values[0]

        if order != start_order:
            break

        accumulated_length += length
        if accumulated_length >= float(USGS_CALB_TRACE_DIST):
            break

        if lake > 0:
            break

        if current_id != start_id:
            trace_up.append(current_id)

        current_id = next_id

    return trace_up, trace_down


def branch_proc_list(usgs_df, huc_dir, is_debug, branch_jobs):

    '''
    If one of the child workers in MP returns an "in_errors" as true, then shut
    down the entire code processing. ie) sys.exit(1) will work here as it is outside
    an MP.
    '''

    huc = os.path.basename(os.path.normpath(huc_dir))
    usgs_df['huc'] = usgs_df['huc'].astype(int).astype(str).str.zfill(8)
    usgs_df['levpa_id'] = usgs_df['levpa_id'].astype(int).astype(str)
    usgs_df['levpa_id'] = usgs_df['levpa_id'].astype(int).astype(str)

    huc_branch_dict = usgs_df.groupby('huc')['levpa_id'].apply(set).to_dict()

    # Check if this HUC has any gages after all filterings
    if huc not in huc_branch_dict:
        logging.warning(
            f"WARNING: All USGS gages for HUC {huc} was filtered out during processing."
            f"Skipping USGS rating curve adjustments."
        )
        return
    
    # Additional arguments for src_roughness_optimization
    source_tag = 'usgs_rating'  # tag to use in source attribute field
    merge_prev_adj = False  # merge in previous SRC adjustment calculations

    branch_set = huc_branch_dict[huc]
    procs_list = []  # Initialize list for mulitprocessing.
    for branch_id in branch_set:
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

        dem_reaches_path = os.path.join(
            branch_dir,
            'demDerived_reaches_split_filtered_addedAttributes_crosswalked_' + branch_id + '.parquet',
        )

        # --------------------------
        df = gpd.read_parquet(dem_reaches_path)
        usgs_elev = usgs_df[((usgs_df['huc'] == huc) & (usgs_df['levpa_id'] == branch_id))]

        # Yes, some hucs and branches may not have usgs data
        if usgs_elev.empty:
            logging.warning(f"WARNING: usgs_elev tables does not have HUC and/or Branch Data. Skipping...")
            continue

        # Calculate updstream/downstream trace ()
        df = df[['HydroID', 'order_', 'LengthKm', 'NextDownID', 'LakeID']]

        # Change the data type of 'HydroID' and 'NextDownID' to int
        df['HydroID'] = df['HydroID'].astype(int)
        df['NextDownID'] = df['NextDownID'].astype(int)

        # Loop through every row in the "usgs_elev" dataframe
        for index, row in usgs_elev.iterrows():
            start_id = row['hydroid']

            # Trace the network for each row
            up, down = trace_network(df, start_id)

            # Append the results to the "usgs_elev" dataframe
            usgs_elev = usgs_elev.copy()
            usgs_elev.loc[index, 'up'] = ','.join(map(str, up))
            usgs_elev.loc[index, 'down'] = ','.join(map(str, down))

        # Handle NaN values and ignore rows where up/down trace list is empty
        usgs_elev['up'] = (
            usgs_elev['up']
            .astype(str)
            .apply(lambda x: [num.strip() for num in x.split(',')] if pd.notna(x) else [])
        )
        usgs_elev['down'] = (
            usgs_elev['down']
            .astype(str)
            .apply(lambda x: [num.strip() for num in x.split(',')] if pd.notna(x) else [])
        )

        # Combine the up & down hydroid lists into a new column
        usgs_elev['trace_hydroid'] = [lst1 + lst2 for lst1, lst2 in zip(usgs_elev['up'], usgs_elev['down'])]

        # Drop up & down columns
        columns_to_drop = ['up', 'down']
        usgs_elev = usgs_elev.drop(columns=columns_to_drop)

        # Explode the trace column
        usgs_elev_trace = usgs_elev.explode('trace_hydroid')

        # Check for empty or nan trace lists and convert the column to integers
        usgs_elev_trace['trace_hydroid'] = usgs_elev_trace['trace_hydroid'].replace('nan', 0)
        usgs_elev_trace['trace_hydroid'] = usgs_elev_trace['trace_hydroid'].replace('', 0)
        usgs_elev_trace['trace_hydroid'] = usgs_elev_trace['trace_hydroid'].astype(int)

        # Drop rows where 'trace_hydroid' column is empty
        # Addresses backpool removals and lake gauges
        usgs_elev_trace = usgs_elev_trace[usgs_elev_trace['trace_hydroid'].astype(int) != 0]

        # Check that there are still valid entries in the usgs_elev
        # May have filtered out all if all locs were lakes
        if usgs_elev_trace.empty:
            logging.warning(
                "WARNING: did not find any valid hydroids to process: "
                + str(huc)
                + ' - branch-id: '
                + str(branch_id)
            )
            continue

        # Rename columns (yes.. do them one at a time)
        usgs_elev_trace = usgs_elev_trace.rename(columns={'hydroid': 'hydroid_gauge'})
        usgs_elev_trace = usgs_elev_trace.rename(columns={'trace_hydroid': 'hydroid'})

        if is_debug:
            usgs_elev_trace.to_csv(
                os.path.join(branch_dir, 'water_edge_trace_' + str(branch_id) + '.csv'), index=False
            )
            logging.debug(f"Saving water_edge_trace_{str(branch_id)}.csv'")

        logging.debug('Will perform SRC adjustments for huc: ' + str(huc) + ' - branch-id: ' + str(branch_id))
        args = {
                "fim_directory": branch_dir,
                "water_edge_median_df": usgs_elev_trace,
                "htable_path": htable_path,
                "huc": huc,
                "branch_id": branch_id,
                "catchments_poly_path": catchments_poly_path,
                "debug_outputs_option": is_debug,
                "source_tag": source_tag,
                "merge_prev_adj": merge_prev_adj,
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

def run_prep(
    huc_dir, usgs_rc_filepath, usgs_sites_filepath, nwm_recurr_filepath, is_debug, branch_jobs
):
    # Check input args are valid
    # assert os.path.isdir(run_dir), 'ERROR: could not find the input fim_dir location: ' + str(run_dir)

    # usgs_elev_file = os.path.join(branch_dir,'usgs_elev_table.csv')
    # usgs_elev_df = pd.read_csv(
    #     usgs_elev_file, dtype={'HUC8': object, 'location_id': object, 'feature_id': int}
    # )
    huc = os.path.basename(os.path.normpath(huc_dir))

    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    begin_time = dt.datetime.now()
    # Create log file for processing records
    log_file_path = setup_file_logger(log_dir, "log_usgs_rc_src_adjust")
    logging.info("Start USGS rating curve adj")
    print(f"Logs will be saved to {log_file_path}")    
    logging.debug('START TIME: ' + str(begin_time))
    logging.debug('#########################################################')

    # It is fine that it is a hardcoded name.
    # It will always be this name at the huc level and fine if it does not exist
    csv_name = 'usgs_elev_table.csv'
    if not os.path.exists(os.path.join(huc_dir, csv_name)):
        logging.info(f'Skipping... No USGS calibration points exist (usgs_elev_table.csv) for huc {huc}')
        return

    logging.info('Reading USGS gage HAND elevation from usgs_elev_table.csv file...')
    usgs_elev_df = pd.read_csv(
        os.path.join(huc_dir, csv_name), dtype={'location_id': object, 'levpa_id': object}
    )

    if usgs_elev_df.empty:
        logging.warning(f"WARNING: usgs_elev_table.csv for {huc} is empty")
        return
    else:
        print('This may take a few minutes...')
        usgs_df = create_usgs_rating_database(
            usgs_rc_filepath, usgs_sites_filepath, usgs_elev_df, nwm_recurr_filepath, log_dir
        )

        # if usgs_df is empty--no acceptable gage, then end the process early.
        if usgs_df is None or usgs_df.empty:
            warn_msg = (
                f"WARNING: All USGS gage data for HUC {huc} was filtered out during processing. "
                "Check log_usgs_rc_database.log for details. Skipping USGS rating curve adjustments."
            )
            logging.warning(warn_msg)
            return
        
        else:
            # Create huc proc_list for multiprocessing and execute the update_rating_curve function
            branch_proc_list(usgs_df, huc_dir, is_debug, branch_jobs)

    # Record run time and close log file
    logging.debug('########################################################')
    end_time = dt.datetime.now()
    logging.debug('END TIME: ' + str(end_time))
    tot_run_time = end_time - begin_time
    logging.debug('TOTAL RUN TIME: ' + str(tot_run_time))


if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(
        description='Adjusts rating curve with database of USGS rating curve (calculated WSE/flow).'
    )
    parser.add_argument('-huc_dir', '--huc_dir', help='directory of a HUC run', required=True)
    parser.add_argument(
        '-usgs_rc', '--usgs-rc-filepath', help='Path to USGS rating curve csv file', required=True
    )
    parser.add_argument(
        '-usgs_sites',
        '--usgs-sites-filepath',
        help='Path to USGS acceptable sites for rating curves file',
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
        '-jb', '--branch-jobs', help='Number of branches jobs to use', required=False, default=1, type=int
    )

    # Assign variables from arguments.
    args = vars(parser.parse_args())

    # Prepare/check inputs, create log file, and spin up the proc list
    run_prep(**args)
