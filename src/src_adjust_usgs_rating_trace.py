import argparse
import datetime as dt
import multiprocessing
import os
import sys
import ast
from multiprocessing import Pool

import geopandas as gpd
import pandas as pd

from power_law_src_optimization import update_rating_curve
from tools.tools_shared_functions import filter_usgs_by_acceptance_criteria
from utils.shared_functions import check_file_age, concat_huc_csv
from utils.shared_variables import USGS_CALB_TRACE_DIST


'''
Purpose: 
Builds a filtered and preprocessed USGS rating curve that will be used for SRC calibration.

Processing
- Read in USGS rating curve from csv and convert WSE navd88 values to meters
- Read in the aggregate USGS elev table csv from the HUC fim directory (output from usgs_gage_crosswalk.py)
- Filter null entries and convert usgs flow from cfs to cms
- Calculate HAND elevation value for each gage location (NAVD88 elevation - NHD DEM thalweg elevation)
- Log and remove negative HAND values
- Produce log file
- Call update_rating_curve() to perform the rating curve calibration.

Inputs
- branch_dir:           fim directory containing individual HUC output dirs
- usgs_rc_filepath:     USGS rating curve database (produced by rating_curve_get_usgs_curves.py)
- debug_outputs_option: optional flag to output intermediate files for reviewing/debugging
- job_number:           number of multi-processing jobs to use

Outputs
- water_edge_median_ds: dataframe containing:
                            'location_id', 'hydroid', 'feature_id', 'huc', 'hand', 'discharge_cms', 'submitter'
'''


def create_usgs_rating_database(
    usgs_rc_filepath, usgs_sites_filepath, usgs_elev_df, log_dir
):
    start_time = dt.datetime.now()
    print('Reading USGS rating curve from csv...')
    log_text = 'Processing database for USGS flow/WSE at NWM flow recur intervals...\n'
    col_usgs = ["location_id", "flow", "stage", "elevation_navd88"]
    usgs_rc_df = pd.read_csv(
        usgs_rc_filepath, dtype={'location_id': object}, usecols=col_usgs
    )  # , nrows=30000)
    print('Duration (read usgs_rc_csv): {}'.format(dt.datetime.now() - start_time))

    # Read in and filter acceptable sites file
    # acceptable_sites_path = "/data/inputs/usgs_gages/acceptable_sites_for_rating_curves_20250603.csv"
    # TODO: Make an input variable

    acceptable_sites = pd.read_csv(usgs_sites_filepath, dtype={'location_id': object})
    acceptable_sites_filt = filter_usgs_by_acceptance_criteria(acceptable_sites)
    location_ids_to_keep = acceptable_sites_filt['location_id'].drop_duplicates().tolist()

    # rm acceptable_sites_filt, acceptable_sites

    # Only keep rating curves from acceptable sites
    usgs_rc_df = usgs_rc_df[usgs_rc_df['location_id'].isin(location_ids_to_keep)]

    # convert WSE navd88 values to meters
    usgs_rc_df['elevation_navd88_m'] = usgs_rc_df['elevation_navd88'] / 3.28084

    # read in the aggregate USGS elev table csv
    start_time = dt.datetime.now()
    cross_df = usgs_elev_df[
        ["location_id", "HydroID", "feature_id", "levpa_id", "HUC8", "dem_adj_elevation"]
    ].copy()
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

    # calculate hand elevation
    usgs_rc_df['hand'] = usgs_rc_df['elevation_navd88_m'] - usgs_rc_df['hand_datum']
    usgs_rc_df = usgs_rc_df[
        ['location_id', 'feature_id', 'hydroid', 'levpa_id', 'huc', 'hand', 'discharge_cms']
    ]
    usgs_rc_df['feature_id'] = usgs_rc_df['feature_id'].astype(int)

   
    # Log any negative HAND elev values and remove from database
    log_text += 'Warning: Negative HAND stage values -->\n'
    log_text += usgs_rc_df[usgs_rc_df['hand'] < 0].to_string() + '\n'
    final_df = usgs_rc_df[usgs_rc_df['hand'] > 0]

    final_df['submitter'] = 'usgs_rating_wrds_api_' + final_df['location_id']
    # Get datestamp from usgs rating curve file to use as coll_time attribute in hydroTable.csv
    datestamp = check_file_age(usgs_rc_filepath)
    final_df['coll_time'] = str(datestamp)[:15]

    # Rename attributes (for ingest to update_rating_curve) and output csv with the USGS RC database
    # final_df = final_df.rename(columns={'discharge_cms': 'flow'})
    # final_df.to_csv(os.path.join(log_dir, "usgs_rc_full.csv"), index=False)

    # Output log text to log file
    log_text += '#########\nTotal entries per USGS gage location -->\n'
    loc_id_df = final_df.groupby(['location_id']).size().reset_index(name='count')
    log_text += loc_id_df.to_string() + '\n'
    log_usgs_db = open(os.path.join(log_dir, 'log_usgs_rc_database.log'), "w")
    log_usgs_db.write(log_text)
    log_usgs_db.close()
    return final_df


def trace_network(df, start_id):
    # This function creates a list of all upstream & downstream hydroids
    # Input: df --> dataframe of demDerived_reaches with network attribs
    # Input: start_id --> hydroid value where the trace routine will start
    # Store HydroIDs and accumulated lengths

    current_id = start_id
    trace_up = []
    trace_down = []
    # Store accumulated length
    up_length = {int(start_id): 0}  # Starting gage has 0 upstream
    down_length = {}
    start_order = None  # Variable to store the start_order
    accumulated_length = 0

    # Downstream trace
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
        down_length[int(current_id)] = accumulated_length

        current_id = next_id

    # Upstream trace
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
            up_length[int(current_id)] = accumulated_length

        current_id = next_id

    return trace_up, trace_down, down_length, up_length


def branch_proc_list(usgs_df, run_dir, debug_outputs_option, log_file):
    procs_list = []  # Initialize list for mulitprocessing.

    # loop through all unique level paths that have a USGS gage
    # branch_huc_dict = pd.Series(usgs_df.levpa_id.values,index=usgs_df.huc).to_dict('list')
    # branch_huc_dict = usgs_df.set_index('huc').T.to_dict('list')
    huc_branch_dict = usgs_df.groupby('huc')['levpa_id'].apply(set).to_dict()

    for huc in sorted(
        huc_branch_dict.keys()
    ):  # sort huc_list for helping track progress in future print statments
        branch_set = huc_branch_dict[huc]
        for branch_id in branch_set:
            # Define paths to branch HAND data.
            # Define paths to HAND raster, catchments raster, and synthetic rating curve JSON.
            # Assumes outputs are for HUC8 (not HUC6)
            branch_dir = os.path.join(run_dir, huc, 'branches', branch_id)
            hand_path = os.path.join(branch_dir, 'rem_zeroed_masked_' + branch_id + '.tif')
            catchments_path = os.path.join(
                branch_dir, 'gw_catchments_reaches_filtered_addedAttributes_' + branch_id + '.tif'
            )
            catchments_poly_path = os.path.join(
                branch_dir,
                'gw_catchments_reaches_filtered_addedAttributes_crosswalked_' + branch_id + '.gpkg',
            )
            htable_path = os.path.join(branch_dir, 'hydroTable_' + branch_id + '.csv')
            dem_reaches_path = os.path.join(
                branch_dir,
                'demDerived_reaches_split_filtered_addedAttributes_crosswalked_' + branch_id + '.gpkg',
            )
            df = gpd.read_file(dem_reaches_path)
            usgs_elev = usgs_df[(usgs_df['huc'] == huc) & (usgs_df['levpa_id'].astype(int) == int(branch_id))]

            # Calculate updstream/downstream trace ()
            df = df[['HydroID', 'order_', 'LengthKm', 'NextDownID', 'LakeID']]

            # Change the data type of 'HydroID' and 'NextDownID' to int
            df['HydroID'] = df['HydroID'].astype(int)
            df['NextDownID'] = df['NextDownID'].astype(int)

            # Loop through every row in the "usgs_elev" dataframe
            for index, row in usgs_elev.iterrows():
                start_id = row['hydroid']

                # Trace the network for each row
                up, down , down_length, up_length = trace_network(df, start_id)

                # Append the results to the "usgs_elev" dataframe
                usgs_elev.loc[index, 'up'] = ','.join(map(str, up))
                usgs_elev.loc[index, 'down'] = ','.join(map(str, down))
                usgs_elev.loc[index, 'up_length'] = str(up_length)
                usgs_elev.loc[index, 'down_length'] = str(down_length)

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
            usgs_elev['trace_hydroid'] = [
                lst1 + lst2 for lst1, lst2 in zip(usgs_elev['up'], usgs_elev['down'])
            ]
            # Explode the trace column
            usgs_elev_trace = usgs_elev.explode('trace_hydroid')



            # Check for empty or nan trace lists and convert the column to integers
            usgs_elev_trace['trace_hydroid'] = usgs_elev_trace['trace_hydroid'].replace('nan', 0)
            usgs_elev_trace['trace_hydroid'] = usgs_elev_trace['trace_hydroid'].replace('', 0)
            usgs_elev_trace['trace_hydroid'] = usgs_elev_trace['trace_hydroid'].astype(int)

            # Drop rows where 'trace_hydroid' column is empty
            # Addresses backpool removals and lake gauges
            usgs_elev_trace = usgs_elev_trace[usgs_elev_trace['trace_hydroid'].astype(int) != 0]

            # Map accumulated length
            def get_accum_length(row):
                hyd = row['trace_hydroid']
                # convert string dicts to actual dicts
                if pd.isna(row['up_length']):
                    return 0
                up_dict = ast.literal_eval(row['up_length'])
                down_dict = ast.literal_eval(row['down_length'])
                if hyd in up_dict:
                    return up_dict[hyd]
                elif hyd in down_dict:
                    return down_dict[hyd]
                else:
                    return 0

            usgs_elev_trace['accum_length'] = usgs_elev_trace.apply(get_accum_length, axis=1)
            usgs_elev_trace.drop(columns=['up', 'down', 'up_length', 'down_length'], inplace=True)
            # Check that there are still valid entries in the usgs_elev
            # May have filtered out all if all locs were lakes
            if usgs_elev_trace.empty:
                print(
                    "ALERT: did not find any valid hydroids to process: "
                    + str(huc)
                    + ' - branch-id: '
                    + str(branch_id)
                )
                log_file.write(
                    "ALERT: did not find any valid hydroids to process: "
                    + str(huc)
                    + ' - branch-id: '
                    + str(branch_id)
                    + '\n'
                )
                continue

            # Rename columns
            usgs_elev_trace.rename(columns={'hydroid': 'hydroid_gauge'}, inplace=True)
            usgs_elev_trace.rename(columns={'trace_hydroid': 'hydroid'}, inplace=True)

            if debug_outputs_option:
                usgs_elev_trace.to_csv(
                    os.path.join(branch_dir, 'water_edge_trace_' + str(branch_id) + '.csv'), index=False
                )

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
                # Additional arguments for src_roughness_optimization
                # Keep this tag for now, it may use when applying power law for all calibration methods
                source_tag = 'usgs_rating'  # tag to use in source attribute field
                merge_prev_adj = False  # merge in previous SRC adjustment calculations

                print('Will perform SRC adjustments for huc: ' + str(huc) + ' - branch-id: ' + str(branch_id))
                procs_list.append(
                    [
                        branch_dir,
                        usgs_elev_trace,
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
    with Pool(processes=job_number) as pool:
        log_output = pool.starmap(update_rating_curve, procs_list)
        log_file.writelines(["%s\n" % item for item in log_output])
    # TO-DO update the error handling to properly capture issues in the multiprocessing
    # try statement for debugging
    # try:
    #     with Pool(processes=job_number) as pool:
    #         log_output = pool.starmap(update_rating_curve, procs_list)
    #         log_file.writelines(["%s\n" % item for item in log_output])
    # except Exception as e:
    #     print(str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + str(e))
    #     log_file.write(
    #         'ERROR!!!: HUC ' + str(huc) + ' --> ' + '  branch id: ' + str(branch_id) + str(e) + '\n'
    #     )


def run_prep(
    run_dir, usgs_rc_filepath, usgs_sites_filepath, debug_outputs_option, job_number
):
    # Check input args are valid
    assert os.path.isdir(run_dir), 'ERROR: could not find the input fim_dir location: ' + str(run_dir)

    # Create an aggregate dataframe with all usgs_elev_table.csv entries for hucs in fim_dir
    print('Reading USGS gage HAND elevation from usgs_elev_table.csv files...')
    # usgs_elev_file = os.path.join(branch_dir,'usgs_elev_table.csv')
    # usgs_elev_df = pd.read_csv(
    #     usgs_elev_file, dtype={'HUC8': object, 'location_id': object, 'feature_id': int}
    # )
    csv_name = 'usgs_elev_table.csv'  # TODO: Get this from a variable?

    available_cores = multiprocessing.cpu_count()
    if job_number > available_cores:
        job_number = available_cores - 1
        print(
            "Provided job number exceeds the number of available cores. "
            + str(job_number)
            + " max jobs will be used instead."
        )

    # Create output dir for log and usgs rc database
    log_dir = os.path.join(run_dir, "logs", "src_optimization")
    print("Log file output here: " + str(log_dir))
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    # Create a time var to log run time
    begin_time = dt.datetime.now()
    # Create log file for processing records
    log_file = open(os.path.join(log_dir, 'log_usgs_rc_src_adjust.log'), "w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.write('#########################################################\n\n')

    usgs_elev_df = concat_huc_csv(run_dir, csv_name)

    if usgs_elev_df is None:
        warn_err = (
            'WARNING: usgs_elev_df not created - check that usgs_elev_table.csv files exist in fim_dir!'
        )
        print(warn_err)
        log_file.write(warn_err)

    elif usgs_elev_df.empty:
        warn_err = 'WARNING: usgs_elev_df is empty - check that usgs_elev_table.csv files exist in fim_dir!'
        print(warn_err)
        log_file.write(warn_err)

    else:
        print('This may take a few minutes...')
        log_file.write("starting create usgs rating db\n")
        usgs_df = create_usgs_rating_database(
            usgs_rc_filepath, usgs_sites_filepath, usgs_elev_df, log_dir
        )
        # Create huc proc_list for multiprocessing and execute the update_rating_curve function
        branch_proc_list(usgs_df, run_dir, debug_outputs_option, log_file)

    # Record run time and close log file
    log_file.write('########################################################\n\n')
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    sys.stdout = sys.__stdout__
    log_file.close()


if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(
        description='Adjusts rating curve with database of USGS rating curve (calculated WSE/flow).'
    )
    parser.add_argument('-run_dir', '--run-dir', help='Parent directory of FIM run.', required=True)
    parser.add_argument(
        '-usgs_rc', '--usgs-ratings', help='Path to USGS rating curve csv file', required=True
    )
    parser.add_argument(
        '-usgs_sites',
        '--usgs-sites',
        help='Path to USGS acceptable sites for rating curves file',
        required=True,
    )

    parser.add_argument(
        '-debug',
        '--extra-outputs',
        help='Optional flag: Use this to keep intermediate output files for debugging/testing',
        default=False,
        required=False,
        action='store_true',
    )
    parser.add_argument('-j', '--job-number', help='Number of jobs to use', required=False, default=1)

    # Assign variables from arguments.
    args = vars(parser.parse_args())
    run_dir = args['run_dir']
    usgs_rc_filepath = args['usgs_ratings']
    usgs_sites_filepath = args['usgs_sites']
    # nwm_recurr_filepath = args['nwm_recur']
    debug_outputs_option = args['extra_outputs']
    job_number = int(args['job_number'])

    # Prepare/check inputs, create log file, and spin up the proc list
    run_prep(
        run_dir, usgs_rc_filepath, usgs_sites_filepath, debug_outputs_option, job_number
    )
