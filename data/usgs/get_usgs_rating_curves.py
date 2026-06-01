#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv

import src.utils.shared_functions as sf
import tools.tools_shared_functions as tsf
import tools.tools_shared_variables as tsv
from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_variables import PREP_PROJECTION
from tools.tools_shared_variables import acceptable_site_type_list


gpd.options.io_engine = "pyogrio"


'''
This script calls the NOAA Tidal API for datum conversions. Experience shows that
    running script outside of business hours seems to be most consistent way
    to avoid API errors. Currently configured to get rating curve data within
    CONUS.

    Tidal API call may need to be modified to get datum conversions for AK ??
'''


def __get_all_active_usgs_sites():
    '''
    Compile a list of all active usgs gage sites.
    Return a GeoDataFrame of all sites.

    Arguments
    ---------
    None

    Returns
    -------
    gdf : GeoDataFrame
        A geospatial layer containing all active USGS gage sites. This layer is used for mapping and spatial joins.

    metadata_list : LIST
        A list of metadata for all active USGS gage sites. This is used for pulling rating curve data and other site-specific information.

    '''
    # Get metadata for all usgs_site_codes that are active in the U.S.
    # Note: GU (Guam) and AS (American Somoa) do not come in as they fail the "active" test.
    metadata_url = f'{API_BASE_URL}/metadata'
    # Define arguments to retrieve metadata and then get metadata from WRDS
    select_by = 'usgs_site_code'
    selector = ['all']
    must_include = 'usgs_data.active'
    metadata_list, ___, err_msg = tsf.get_metadata(
        metadata_url,
        select_by,
        selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=None,
    )

    if err_msg != '':
        logging.error(f'Error occurred while getting metadata: {err_msg}')

    # Get a geospatial layer (gdf) for all acceptable sites
    logging.info("Aggregating WBD HUCs...")
    ___, gdf = tsf.aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)

    # Rename gdf fields
    gdf.columns = gdf.columns.str.replace('identifiers_', '')

    return gdf, metadata_list


# We are not using the screen queue at this time but it is available for console outputs in mps
# where we do not want it in the logs. Enforced by run_with_mp at this time.
def __mp_get_flows_for_site(
    site_data_json, nws_lid, usgs_site_code, threshold_url, file_logger, screen_queue, task_id
):
    '''
    Gets flow values for each flood category (action, minor, moderate, major) for a given site.
    These flow values are used by Sierra Testing (rating_curve_comparison.py).

    Arguments
    ---------
    site_data_json : DICT
        A dictionary of metadata for a given site. This is used to pull the feature_id for the site, which
        is required for pulling flow data.
    nws_lid : STR
        The NWS location id for the site. This is used to pull the flow thresholds for the site.
    usgs_site_code : STR
        The USGS site code for the site. This is used for logging and to populate the output flow file.
    threshold_url : STR
        The URL for the NWS threshold API. This is used to pull the flow thresholds for the site.
    file_logger : LOGGER
        The logger for the multi-processing function. This is used to log messages for each site.
    screen_queue : MP_QUEUE
        The screen queue for the multi-processing function. This is used to print messages to the console for each site.
    task_id : STR
        The task id for the multi-processing function. This is used for logging and to identify the site being
        processed in the logs and console messages.

    Returns
    -------
    status_code : INT
        A status code indicating the result of the function. 1 indicates success, 0 indicates a non-critical error
        that resulted in skipping the site, and -1 indicates a critical error that should shut down the entire tool.
    site_flows_df : DataFrame
        A DataFrame containing the flow values for each flood category for the site. This is used by Sierra Testing
        (rating_curve_comparison.py). The DataFrame contains the following columns: feature_id, discharge_cms,
        recurr_interval, nws_lid, location_id.
    '''

    try:
        # sf.l_print(f"Processing flow data for lid: {task_id}", file_logger, "debug", screen_queue)

        # Note: There is no try catch because we want any errors to shut down the entire tool
        feature_id = site_data_json.get('identifiers').get('nwm_feature_id')

        # Get the stages and flows
        ___, flows, ___ = tsf.get_thresholds(
            threshold_url,
            select_by='nws_lid',
            selector=nws_lid,
            threshold='all',
            source_crs_availability=None,
        )

        if flows is None:
            sf.l_print(f"{task_id}: No flow data found for site", file_logger, "warning", screen_queue)
            # TODO: This doesn't get printed, which isn't the end of the world but also might be good to log this info
            return 0, None  # log and continue the next task

        site_flows_df = pd.DataFrame()
        # For each flood category
        for category in ['action', 'minor', 'moderate', 'major']:
            # Get flow
            flow = flows.get(category, None)

            # If flow or feature id are not valid, skip to next site
            if flow is None:
                continue

            # Otherwise, write 'guts' of a flow file and append to a master DataFrame.
            else:
                flow_df = tsf.flow_data([feature_id], flow, convert_to_cms=True)
                flow_df['recurr_interval'] = category
                flow_df['nws_lid'] = nws_lid
                flow_df['location_id'] = usgs_site_code
                flow_df = flow_df.rename(columns={'discharge': 'discharge_cms'})

                site_flows_df = pd.concat([site_flows_df, flow_df], ignore_index=True)
        return 1, site_flows_df

    except Exception:
        sf.l_print(f"❌ Critical error while processing {task_id}", file_logger, "critical", screen_queue)
        sf.l_print(traceback.format_exc(), file_logger, "critical", screen_queue)
        return -1, None  # shut the program down.


# Generate categorical flows for each category across all sites.
def __write_categorical_flow_files(
    metadata_list, site_status_df, output_dir, file_datetime_string, parent_log_file, num_jobs
):
    '''
    Writes flow files of each category for every feature_id in the input metadata.
    Written to supply input flow files of all gage sites for each flood category.

    Arguments
    ---------
    metadata_list : LIST
        A list of metadata for all active USGS gage sites. This is used for pulling flow data and other site-specific information.
    site_status_df : DataFrame
        A DataFrame to track the status of each site throughout the process.
    output_dir : STR
        Path to output_dir where flow files will be saved.
    file_datetime_string : STR
        A string of the current date and time, used for naming the output flow files and log files.
    parent_log_file : STR
        Path to the parent log file. This function uses its own mp log file. This arg allows the mp logs to be
        appended to the parent when it is done.
    num_jobs : INT
        Number of jobs (workers) used for multi-proc.

    Returns
    -------
    None

    Note: For now.. any error files created anywhere are not removed.

    '''

    logging.info("Writing categorical flow file...")

    threshold_url = f'{API_BASE_URL}/nws_threshold'
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # For each site in metadata_trimmed
    num_sites = len(metadata_list)
    logging.info(f"Number of sites to process: {num_sites}")

    # Check that there is an acceptable nws_lid and feature_id val and create the arg list for mp
    task_args_list = []
    bogus_id_site_list, no_nws_lid_site_list, no_feature_id_site_list = [], [], []
    processed_USGS_site_codes = []

    for i in range(num_sites):
        skip_site = False
        site_data_json = metadata_list[i]

        feature_id = site_data_json.get('identifiers').get('nwm_feature_id')
        nws_lid = site_data_json.get('identifiers').get('nws_lid')
        usgs_site_code = site_data_json.get('identifiers').get('usgs_site_code')

        # Check for a test site ID
        if nws_lid == 'Bogus_ID':  # Legacy test?
            bogus_id_site_list.append(usgs_site_code)
            skip_site = True

        # Check for a nws_lid
        if nws_lid is None or nws_lid == "":
            no_nws_lid_site_list.append(usgs_site_code)
            skip_site = True

        # Check for a NWM feature_id
        if feature_id is None:
            no_feature_id_site_list.append(usgs_site_code)
            skip_site = True

        # Skip site if missing nws_lid, NWM feature_id, or if Bogus_ID is found
        if skip_site == True:
            continue

        # If proper data is avail, add to the arg list for mp
        task_args_list.append(
            {
                "site_data_json": site_data_json,
                "nws_lid": nws_lid,
                "usgs_site_code": usgs_site_code,
                "threshold_url": threshold_url,
            }
        )
        processed_USGS_site_codes.append(usgs_site_code)

    sorted_tasks_args_list = sorted(task_args_list, key=lambda x: ['nws_lid'])

    # Log which sites will be removed due to missing data
    # It is expected that some sites will be removed at this stage
    if len(bogus_id_site_list) > 0:
        logging.warning(f"Found {len(bogus_id_site_list)} site(s) with bogus ID values: {bogus_id_site_list}")
        site_status_df.loc[
            site_status_df['usgs_site_code'].isin(bogus_id_site_list), 'categorical_flows_avail'
        ] = 'no (due to placeholder ID)'

    if len(no_nws_lid_site_list) > 0:
        logging.warning(
            f"Found {len(no_nws_lid_site_list)} site(s) with no nws_lid available: {no_nws_lid_site_list}"
        )
        site_status_df.loc[
            site_status_df['usgs_site_code'].isin(no_nws_lid_site_list), 'categorical_flows_avail'
        ] = 'no (due to missing NWS LID)'

    if len(no_feature_id_site_list) > 0:
        logging.warning(
            f"Found {len(no_feature_id_site_list)} site(s) with no feature_id available: {no_feature_id_site_list}"
        )
        site_status_df.loc[
            site_status_df['usgs_site_code'].isin(no_feature_id_site_list), 'categorical_flows_avail'
        ] = 'no (due to missing NWM Feature ID)'

    all_removed_sites_list = list(set(bogus_id_site_list + no_nws_lid_site_list + no_feature_id_site_list))
    if len(all_removed_sites_list) > 0:
        logging.warning(f"Removed {len(all_removed_sites_list)} site(s)")

    # ---
    # Initiate multiproc for getting the flows for the remaining sites
    logging.info(f"Processing flow data for {len(sorted_tasks_args_list)} remaining sites")

    # Set up MP logger
    mp_log_file_path = os.path.join(output_dir, f"get_rating_curves-mp-flow-{file_datetime_string}.log")
    mp_logger = sf.setup_mp_file_logger(mp_log_file_path, logger_name="mp_flows")

    # We get a list of dictionaries
    flow_dfs = sf.run_with_mp(
        task_function=__mp_get_flows_for_site,
        tasks_args_list=sorted_tasks_args_list,
        file_logger=mp_logger,
        max_workers=num_jobs,
        task_id_key='nws_lid',
        show_progress=False,
    )

    # Roll the mp log into the master log.
    sf.rollup_log_files(mp_log_file_path, parent_log_file)
    # for now.. let's leave the error files alone, even the mp ones

    # roll up the list of df's into one master df
    # run_with_mp returns a list of dictionaries keyed with a huc. We don't care about the keys, just the values
    # which are df's
    all_flows_data = pd.DataFrame()
    for i, value in enumerate(flow_dfs.values()):
        if i == 0:
            all_flows_data = value
        else:
            all_flows_data = pd.concat([all_flows_data, value])

    # Write usgs stage discharge data, used by Sierra tests (rating_curve_comparison.py)
    logging.info("Writing for USGS discharge data for each usgs stage (ie. action, minor, etc)")
    if not all_flows_data.empty:
        usgs_discharge_file_name = os.path.join(output_dir, 'usgs_stage_discharge_cms.csv')
        final_data = all_flows_data[['feature_id', 'discharge_cms', 'recurr_interval']]
        final_data.to_csv(usgs_discharge_file_name, index=False)
        logging.info(f"Saved USGS discharge file to {usgs_discharge_file_name}")

        # Update site_status_df 'categorical_flows_avail' column with 'yes' if there is categorical flow data available for the site
        site_status_df.loc[
            site_status_df['usgs_site_code'].isin(all_flows_data['location_id']), 'categorical_flows_avail'
        ] = 'yes'

        # If USGS site code is in processed_USGS_site_codes but there is no categorical flow data for the site, update site_status_df 'categorical_flows_avail' column with 'no'
        site_status_df.loc[
            (site_status_df['usgs_site_code'].isin(processed_USGS_site_codes))
            & (~site_status_df['usgs_site_code'].isin(all_flows_data['location_id'])),
            'categorical_flows_avail',
        ] = 'no'

    else:
        logging.info("No flow data was found. Saving of usgs_stage_discharge_cms file skipped")

        # Update site_status_df 'categorical_flows_avail' column with 'no' if there is no categorical flow data for the site (but only if the status says 'not set')
        site_status_df.loc[
            site_status_df['categorical_flows_avail'] == 'not set', 'categorical_flows_avail'
        ] = 'no (no flow data found for any sites)'

    return site_status_df


def set_global_env(env_file):
    '''
    Sets the global environment variables for the USGS rating curve retrieval.

    Args:
        env_file (str): Path to the environment file containing the necessary variables.

    Returns:
        None
    '''
    global API_BASE_URL, WBD_LAYER, NWM_FLOWS_MS
    load_dotenv(env_file)

    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    NWM_FLOWS_MS = os.getenv("NWM_FLOWS_MS")


# ++++++++++++++++++++++++++++++++
# TODO: likely don't need usgs_site_code if we have the task id
# Yes... usgs_site_code and task_id are redundant for now
def __mp_get_site_rating_curve(metadata_json, rating_curve_url, file_logger, screen_queue, task_id):
    '''
    Gets the rating curve for a given site and converts it to elevation (NAVD88).

    Arguments
    ---------
    metadata_json : DICT
        A dictionary of metadata for a given site. This is used to pull datum information for the site, which is required for converting the rating curve to elevation.
    rating_curve_url : STR
        The URL for the rating curve API. This is used to pull the rating curve for the site.
    usgs_site_code : STR # TODO: Remove input? I think it's already included in the metadata input etc.. # TODO: Clean up
        The USGS site code for the site. This is used for logging and to populate the output rating curve DataFrame.
    file_logger : LOGGER
        The logger for the multi-processing function. This is used to log messages for each site.
    screen_queue : MP_QUEUE
        The screen queue for the multi-processing function. This is used to print messages to the console for each site.
    task_id : STR
        The task id for the multi-processing function. This is used for logging and to identify the site being processed in the logs and console messages.

    Returns
    -------
    status_code : INT
        A status code indicating the result of the function. 1 indicates success, 0 indicates a non-critical error that resulted in skipping the site, and -1 indicates a critical error that should shut down the entire tool.
    curve : DataFrame
        A DataFrame containing the rating curve for the site, with the stage converted to elevation (NAVD88). This is used for mapping and analysis. The DataFrame contains the following columns: stage, discharge, active, datum, datum_vcs, navd88_datum, elevation_navd88.

    '''

    try:

        # Get datum information for site (only need usgs_data)

        # Note: debug... if you are in mp and want screen only, use screen_queue
        # screen_queue.put(f"testing my mp code {task_id}")
        # for debugging, shut off when in real mode so it uses the progress bar.
        # screen_queue.put(f"Getting rating curves for usgs location id of {task_id}")

        ___, usgs = tsf.get_datum(metadata_json)
        location_id = usgs['usgs_site_code']  # in theory we get one and exactly one here
        # sf.l_print(f"{location_id}: Getting rating curves...", file_logger, "info")  # too verbose

        # If no vertical or horizontal datum, skip site
        # if usgs['crs'] in tsv.UNKNOWN_DATUM_SPELLINGS: # TODO: Make sure we actually need to error out in this situation (no CRS?)... or should we let it try?
        #     sf.l_print(f"{location_id}: Removed due to unknown CRS (CRS: {usgs['crs']})", file_logger, "warning")
        #     return 0, None  # log and continue the next task

        if usgs['vcs'] in tsv.UNKNOWN_DATUM_SPELLINGS:
            sf.l_print(
                f"{location_id}: Removed due to unknown/blank VCS (VCS: {usgs['vcs']})",
                file_logger,
                "warning",
            )
            return 0, None  # log and continue the next task

        # Get rating curve for site using the USGS site code
        curve, rc_err_msg = tsf.get_rating_curve(rating_curve_url, location_ids=[location_id])

        if rc_err_msg != "":
            sf.l_print(
                f"{location_id}: Rating curve API call returned error message: {rc_err_msg}",
                file_logger,
                "error",
            )

        # If no rating curve was returned, skip site.
        if curve.empty:
            sf.l_print(
                f'{location_id}: Removed because it has no rating curves', file_logger, "info", screen_queue
            )
            return 0, None  # log and continue the next task

        # -----------
        # Only continue if a rating curve is found and the CRS and VCS are not None/UNK/""/etc.

        # Correct datum spelling if needed
        crs_corrected, vcs_corrected, uncorrected_crs_error, uncorrected_vcs_error, msgs = (
            tsf.correct_datum_typos(usgs['crs'], usgs['vcs'])
        )

        if crs_corrected is not None:
            usgs['crs'] = crs_corrected
        if vcs_corrected is not None:
            usgs['vcs'] = vcs_corrected

        # An uncorrected error likely means an error in the future, but we will just log it here in case it goes through
        if uncorrected_crs_error is True:
            sf.l_print(
                f"{location_id}: Unable to correct CRS spelling (CRS: {usgs['crs']})", file_logger, "warning"
            )
        if uncorrected_vcs_error is True:
            sf.l_print(
                f"{location_id}: Unable to correct VCS spelling (VCS: {usgs['vcs']})", file_logger, "warning"
            )

        # Log information about whether the datums were corrected
        # This is probably too verbose for large runs, but keeping it in for now (maybe keeping at debug level is good)
        if len(msgs) > 0:
            for msg in msgs:
                sf.l_print(f'{location_id}: {msg}', file_logger, "debug")

        # If the site is in PR, VI, or HI, keep datum in LMSL (local mean sea level)
        # because our 3DEP dems are also in LMSL for these areas.
        # Sept 2025, GU and AS (Guam and American Samoa don't come in as they
        #  fail the "must_include = 'usgs_data.active'" test.) # TODO: Does this need to be changed?
        if usgs['state'] in ['Puerto Rico', 'Virgin Islands', 'Hawaii']:
            if usgs['vcs'] == 'LMSL':
                navd88_datum = usgs['datum']
                # left of the screen queue so it goes to log only
                sf.l_print(
                    f'{location_id}: Site is in PR, VI, or HI, so datum kept as LMSL', file_logger, "debug"
                )
            else:
                # If the site is in PR, VI, or HI, and has a datum other than LMSL, return an error.
                datum_name = usgs['vcs']
                message = (
                    f'{location_id}: Removed because site is located in PR,'
                    f'VI, or HI but has a datum other than LMSL ({datum_name})'
                )
                sf.l_print(message, file_logger, "warning")
                return 0, None  # log and continue the next task

        # If the state is not PR, VI, or HI, then we want to adjust the datum to NAVD88 if needed.
        # If the datum is unknown, skip site.
        else:
            if usgs['vcs'] == 'NGVD29':

                try:
                    # Get the datum adjustment to convert NGVD to NAVD.
                    datum_adj_ft, err_msg = tsf.ngvd_to_navd_ft(datum_info=usgs)

                    # Log an error message if it exists (but not erroring out automatically because it's
                    # possible an error is returned but datum adjustment was still sucessful)
                    if err_msg != "":
                        sf.l_print(f'{location_id}: {err_msg}', file_logger, "error")

                except Exception as ex:
                    ex = str(ex)

                    if 'HTTPSConnectionPool' in ex:
                        http_msg = (
                            "VDatum API Error: HTTPSConnectionPool. Waiting 10 seconds and rerunning API call"
                        )
                        sf.l_print(f'{location_id}: {http_msg}', file_logger, "error")
                        time.sleep(10)  # Maybe the API needs a break, so wait 10 seconds
                        try:
                            datum_adj_ft, err_msg = tsf.ngvd_to_navd_ft(datum_info=usgs)

                            if err_msg != "":
                                sf.l_print(f'{location_id}: {err_msg}', file_logger, "error")

                        except Exception as ex:
                            msg = 'VDatum API Error, possible API issue (tried again after 10 second break)'
                            sf.l_print(f'{location_id}: {msg}: {ex}', file_logger, "error")

                    elif 'Invalid projection' in ex:
                        msg = f'VDatum API Error, invalid projection: crs={usgs["crs"]}'
                        sf.l_print(f'{location_id}: {msg}', file_logger, "error")

                    else:
                        msg = 'VDatum API Error, unrecognized exception'
                        sf.l_print(f'{location_id}: {msg}: {ex}', file_logger, "error")

                # If datum API failed, print message and skip site.
                if datum_adj_ft is None:
                    sf.l_print(
                        f'{location_id}: Removed because datum adjustment failed!', file_logger, "warning"
                    )
                    return 0, None  # log and continue the next task

                # If datum adjustment succeeded, calculate datum in NAVD88
                navd88_datum = round(usgs['datum'] + datum_adj_ft, 2)
                sf.l_print(f'{location_id}: Successfully converted NGVD29 to NAVD88', file_logger, "debug")

            elif usgs['vcs'] == 'NAVD88':
                navd88_datum = usgs['datum']
                # sf.l_print(f'{location_id}: Vertical datum is already NAVD88, no vdatum conversion needed', file_logger, "debug")  # too verbose

            elif usgs['vcs'] == 'LMSL':
                # If the site has a vdatum of LMSL and is not in PR, VI or HI, skip site.
                sf.l_print(
                    f'{location_id}: Removed because LMSL vertical datum found outside of PR, VI, or HI',
                    file_logger,
                    "warning",
                )
                return 0, None  # log and continue the next task

            else:
                # If the site has an unrecognized datum, skip site.
                datum_name = usgs['vcs']
                sf.l_print(f'{location_id}: Removed due to unknown vertical datum', file_logger, "warning")
                return 0, None  # log and continue the next task

        # Populate rating curve with metadata and use navd88 datum to convert stage to elevation.
        # If you came in looking for all sites, then "active" will be true. A filtered set, this might be True or False
        curve['active'] = usgs['active']
        curve['datum'] = usgs['datum']
        curve['datum_vcs'] = usgs['vcs']
        curve['navd88_datum'] = navd88_datum
        curve['elevation_navd88'] = round(curve['stage'] + navd88_datum, 2)

        # sf.l_print(f"{location_id}: Done retrieving rating curves", file_logger, "debug")  # too verbose!
        return 1, curve

    except Exception:
        sf.l_print(
            f'❌ Critical error while processing {task_id} (location ID: {location_id})',
            file_logger,
            "critical",
            screen_queue,
        )
        sf.l_print(traceback.format_exc(), file_logger, "critical", screen_queue)
        return -1, None  # shut the program down.


def __get_usgs_metadata(list_of_gage_sites, metadata_url):
    '''
    Retrieves metadata for the specified USGS gage sites.

    Arguments
    ---------
    list_of_gage_sites : LIST
        A list of USGS gage site codes for which to retrieve metadata. If the list contains 'all', metadata for all active USGS gage sites will be retrieved.
    metadata_url : STR
        The URL for the metadata API endpoint. This is used to pull metadata for the specified sites.

    Returns
    -------
    sites_gdf : GeoDataFrame
        A geospatial layer containing the USGS gage sites for which metadata was retrieved. This layer is used for mapping and spatial joins.
    metadata_list : LIST
        A list of metadata for the USGS gage sites for which metadata was retrieved. This is used for pulling rating curve data and other site-specific information.
    site_status_df : DataFrame
        A dataframe tracking the status of each site during the metadata retrieval process.
    '''

    sites_gdf = pd.DataFrame()  # create blank dataframes
    site_status_df = pd.DataFrame()

    if list_of_gage_sites == ['all']:
        logging.info('Getting metadata for all sites')
        sites_gdf, metadata_list = __get_all_active_usgs_sites()

    # Otherwise, if a list of sites is passed, retrieve sites from WRDS.
    else:
        # Define arguments to retrieve metadata and then get metadata from WRDS
        select_by = 'usgs_site_code'
        selector = list_of_gage_sites

        # Sept 2025: Is this right? If we send in a list of sites, it does not
        # include the filter of "must_include = 'usgs_data.active'" which is used
        # when we use the "get_all_active_usgs_sites"
        print()
        logging.info(f"Getting metadata for selected sites : {selector}")
        print()

        # Since there is a limit to number characters in url, split up selector if too many sites.
        max_sites = 20  # Can we go more than 20? do we want to?
        metadata_list = []
        error_gages_list = []

        # TODO: Do we even need to keep the chunk functions? I don't feel like we're prioritizing large-scale runs with a list input...
        if len(selector) > max_sites:
            chunks = [selector[i : i + max_sites] for i in range(0, len(selector), max_sites)]
            # Get metadata for each chunk
            # metadata_df = pd.DataFrame()  # Feb 2026: metadata_df is not in use and has not been since at least Jun 2025
            for chunk in chunks:
                chunk_list, __, err_msg = tsf.get_metadata(
                    metadata_url,
                    select_by,
                    chunk,
                    must_include=None,
                    upstream_trace_distance=None,
                    downstream_trace_distance=None,
                )
                if err_msg != '':
                    logging.error(f'Error occurred while getting metadata for {chunk}')
                    logging.error(err_msg)

                # If one of the gage IDs is incorrect, then it will error out the entire chunk/list of gages
                # If the metadata list is zero, rerun sites individually to ensure that is not preventing data from being retrieved
                if len(chunk_list) == 0:
                    print()
                    logging.warning(
                        f'No metadata collected for the {len(chunk)} sites. Re-running each site individually.'
                    )

                    for gage in chunk:
                        try:
                            gage_list, __, err_msg = tsf.get_metadata(
                                metadata_url,
                                select_by,
                                [gage],
                                must_include=None,
                                upstream_trace_distance=None,
                                downstream_trace_distance=None,
                            )
                            if err_msg != '':
                                logging.error(f'Error occurred while getting metadata for {gage}:')
                                logging.error(err_msg)
                                error_gages_list.append(gage)

                            # Append chunk data to metadata_list/df
                            chunk_list.extend(gage_list)

                        except Exception as ex:
                            logging.error(f'Exception occurred while metadata metadata for {gage}: {ex}')
                            error_gages_list.append(gage)

                # Append chunk data to metadata_list/df
                metadata_list.extend(chunk_list)

                # Feb 2026: metadata_df is not in use and has not been since at least Jun 2025
                # metadata_df = pd.concat([metadata_df, chunk_df])
        else:
            # If selector has less than max sites, then get metadata.
            metadata_list, __, err_msg = tsf.get_metadata(
                metadata_url,
                select_by,
                selector,
                must_include=None,
                upstream_trace_distance=None,
                downstream_trace_distance=None,
            )
            if err_msg != '':
                logging.error('Error occurred while getting metadata:')
                logging.error(err_msg)

            # If one of the gage IDs is incorrect, then it will error out the entire chunk/list of gages
            # If the metadata list is zero, rerun sites individually to ensure that is not preventing data from being retrieved
            if len(metadata_list) == 0:
                logging.warning(
                    f'No metadata collected for the {len(selector)} sites. Re-running each site individually.'
                )

                for gage in list_of_gage_sites:
                    try:
                        gage_list, __, err_msg = tsf.get_metadata(
                            metadata_url,
                            select_by,
                            [gage],
                            must_include=None,
                            upstream_trace_distance=None,
                            downstream_trace_distance=None,
                        )
                        if err_msg != '':
                            logging.error(f'Error occurred while getting metadata for {gage}:')
                            logging.error(err_msg)
                            error_gages_list.append(gage)

                        # Append chunk data to metadata_list/df
                        metadata_list.extend(gage_list)

                    except Exception as ex:
                        logging.error(f'Exception occurred while getting metadata for site {gage}: {ex}')
                        error_gages_list.append(gage)

        # Log which gages failed (if any)
        if len(error_gages_list) > 0:
            logging.warning(f"Failed to get metadata for the following input site(s) : {error_gages_list}")

        # Get a geospatial layer (gdf) for all acceptable sites
        logging.info("Aggregating WBD HUCs...")
        ___, sites_gdf = tsf.aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)

        if sites_gdf is not None and not sites_gdf.empty:
            # Rename gdf fields
            sites_gdf.columns = sites_gdf.columns.str.replace('identifiers_', '')
        else:
            logging.info("Sites GDF is empty or None after aggregation.")
            return sites_gdf, metadata_list, site_status_df

    if len(metadata_list) == 0:
        logging.info("Length of metadata list is zero after aggregation.")
        return sites_gdf, metadata_list, site_status_df

    # ##### TEMP DEBUG SECTION #####
    # # TEMP DEBUG: Keep first n sites to speed up testing. Remove this when not needed.
    # n = 300
    # logging.info(f"DEBUG MODE: Only keeping metadata for first {n} sites")
    # metadata_list = metadata_list[:n]
    # sample_usgs_list = []
    # for i in range(len(metadata_list)):
    #     site_data_json = metadata_list[i]
    #     usgs_site_code = site_data_json.get('identifiers').get('usgs_site_code')
    #     sample_usgs_list.append(usgs_site_code)

    # # Filter the sites gdf to only have the sites in sample_usgs_list (will be less than number of sites provided)
    # sites_gdf = sites_gdf[sites_gdf['usgs_site_code'].isin(sample_usgs_list)]
    ##### END TEMP DEBUG SECTION #####

    # Drop a few columns to prevent errors downstream while saving GPKG
    sites_gdf = sites_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')
    sites_gdf = sites_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')

    # Create a site status DF to track why different sites are getting removed
    # Should be a copy of sites_gdf but with these columns: nws_lid,location_id,feature_id, and status (a new column)
    site_status_df = sites_gdf[['nws_lid', 'usgs_site_code', 'nwm_feature_id']].copy()
    site_status_df['metadata_avail'] = 'yes'  # Every site in this table at this point has metadata available

    # Make rows for the sites that were in the input list but did not have metadata returned (if the input was not "all").
    # These sites will be marked as "no metadata" in the site_status_df and will be included in the final output to show that they were processed but had no metadata available.
    if list_of_gage_sites != ['all']:
        for site in list_of_gage_sites:
            if site not in sites_gdf['usgs_site_code'].values:
                new_row = {
                    'nws_lid': None,
                    'usgs_site_code': site,
                    'nwm_feature_id': None,
                    'metadata_avail': 'no',
                }
                site_status_df = pd.concat([site_status_df, pd.DataFrame([new_row])], ignore_index=True)

    # Create additional columns with a placeholder
    new_cols = ['rating_curves_avail', 'correct_site_type', 'categorical_flows_avail']
    site_status_df[new_cols] = 'not set'

    return sites_gdf, metadata_list, site_status_df


def __attrib_mainstems_filter_sites(sites_gdf, all_rating_curves, site_status_df, output_dir):
    '''
    Attributes mainstem segments in the sites_gdf. This is used for mapping and spatial joins.

    Arguments
    ---------
    sites_gdf : GeoDataFrame
        A geospatial layer containing the USGS gage sites for which metadata was retrieved.
    all_rating_curves : DataFrame
        A DataFrame containing all available rating curves.
    site_status_df : DataFrame
        A DataFrame to track the status of each site.
    output_dir : str
        The directory where output files will be saved.

    Returns
    -------
    acceptable_sites_list : LIST
        A list of location_ids for sites that have rating curves and are of an acceptable site type.
    all_rating_curves : DataFrame
        A DataFrame containing only the rating curves for sites that have rating curves and are of an acceptable site type.
    site_status_df : DataFrame
        A DataFrame to track the status of each site.
    '''

    # Rename columns and add attribute indicating if rating curve exists
    sites_gdf.rename(columns={'nwm_feature_id': 'feature_id', 'usgs_site_code': 'location_id'}, inplace=True)
    sites_with_data = pd.DataFrame({'location_id': all_rating_curves['location_id'].unique(), 'curve': 'yes'})
    sites_gdf = sites_gdf.merge(sites_with_data, on='location_id', how='left')
    sites_gdf.fillna({'curve': 'no'}, inplace=True)

    # Import mainstems segments to be used in run_by_unit.sh
    ms_df = gpd.read_file(NWM_FLOWS_MS)
    ms_segs = ms_df.ID.astype(str).to_list()  # Yes.. required (sites_gdf.eval below)

    # Populate mainstems attribute field
    sites_gdf['mainstem'] = 'no'
    sites_gdf.loc[sites_gdf.eval('feature_id in @ms_segs'), 'mainstem'] = 'yes'

    # Previously we had code here to filter sites using the following lists:
    # acceptable_coord_acc_code_list, acceptable_coord_method_code_list, acceptable_alt_meth_code_list,
    # acceptable_site_type_list, # usgs_data_coord_accuracy_code is in acceptable_coord_acc_code_list
    # This filtering wasn't effective here and will now take place in the scripts that use the data.

    # Filter out non stream sites (the other acceptance criteria will be filtered out the scripts where the data is used)
    num_sites_before_filtering = len(sites_gdf)
    sites_gdf['acceptable_site_type'] = sites_gdf['usgs_data_site_type'].isin(acceptable_site_type_list)

    # Get a list of sites that are being removed due to unacceptable site type
    unacceptable_site_type_sites = sites_gdf[sites_gdf['acceptable_site_type'] == False][
        'location_id'
    ].tolist()

    # Update the 'correct_site_type' column in site_status_df to 'yes' if the site is of an acceptable site type, and 'no' if it is not
    site_status_df['correct_site_type'] = np.where(
        site_status_df['usgs_site_code'].isin(unacceptable_site_type_sites), 'no', 'yes'
    )

    # Filter to acceptable sites
    acceptable_sites_gdf = sites_gdf[sites_gdf['acceptable_site_type'] == True]

    # Get the number of sites removed because they're missing rating curves
    num_acceptable_sites_missing_rc = len(acceptable_sites_gdf[acceptable_sites_gdf['curve'] == 'no'])
    logging.info(f"Removed {num_acceptable_sites_missing_rc} site(s) due to missing rating curve")

    # Only keep sites where the rating curve is available  # TODO: this step might be obselete because I filter out the none sites earlier now
    acceptable_sites_gdf = acceptable_sites_gdf[acceptable_sites_gdf['curve'] == 'yes']

    # Log the site len change
    num_sites_removed = len(acceptable_sites_gdf) - len(sites_gdf)
    if num_sites_removed > 0:
        logging.info(
            f"Removed {num_sites_removed} site(s) due to unacceptable site type and/or missing rating curve"
        )

    # Save filtered sites file for viewing
    logging.info("Saving acceptable rating curve files")

    acceptable_sites_csv_path = os.path.join(output_dir, 'acceptable_sites_for_rating_curves.csv')
    logging.info(f"...to CSV at path {acceptable_sites_csv_path}")
    acceptable_sites_gdf.to_csv(acceptable_sites_csv_path)

    acceptable_sites_gpkg_path = os.path.join(output_dir, 'acceptable_sites_for_rating_curves.gpkg')
    logging.info(f"...and to GeoPackage at path {acceptable_sites_gpkg_path}")
    acceptable_sites_gdf.to_file(acceptable_sites_gpkg_path, driver='GPKG', engine='fiona')

    # Make list of acceptable sites
    acceptable_sites_list = acceptable_sites_gdf['location_id'].tolist()

    # Filter all rating curves to only the rating curves for sites in the acceptable sites list
    all_rating_curves = all_rating_curves[all_rating_curves['location_id'].isin(acceptable_sites_list)]

    return all_rating_curves, site_status_df


def __run_rating_curve_retrieval(
    metadata_list, site_status_df, rating_curve_url, output_dir, file_datetime_string, num_jobs, log_file_path
):
    '''
    New wrapper for  __mp_get_site_rating_curve (June 2026)

    This function is responsible for running the multiprocessing function to get rating curves for each site,
    and then compiling the results into a single DataFrame. It also updates the site_status_df with the availability
    of rating curves for each site.

    Arguments
    ---------
    metadata_list : LIST
        A list of metadata for the USGS gage sites for which metadata was retrieved. This is used for
        pulling rating curve data and other site-specific information.
    site_status_df : DataFrame
        A DataFrame to track the status of each site.
    rating_curve_url : STR
        The URL for the rating curve API. This is used to pull the rating curve for each site.
    output_dir : str
        The directory where output files will be saved.
    file_datetime_string : str
        A string representing the date and time, used for naming output files.
    num_jobs : int
        The number of parallel jobs to run in the multiprocessing function.
    log_file_path : str
        The file path for the log file where messages will be recorded.

    Returns
    -------
    rating_curves_dfs : dict
        A dictionary containing the rating curve DataFrames for each site, keyed by location_id.
        Some values may be None if the rating curve retrieval failed for those sites.
    all_rating_curves : DataFrame
        A DataFrame containing the rating curves for all sites that had successful retrievals.
    site_status_df : DataFrame
        A DataFrame to track the status of each site, updated with the availability of rating curves for each site.

    '''
    # Create a list of USGS sites to get RC's for
    tasks_args_list = []
    for i in range(len(metadata_list)):
        metadata_json = metadata_list[i]

        # usgs_site_code = metadata_json['identifiers']['usgs_site_code'] # TODO: Clean up after testing
        tasks_args_list.append(
            {
                "metadata_json": metadata_json,
                "rating_curve_url": rating_curve_url,
                # "usgs_site_code": usgs_site_code, # TODO: Test removal of usgs site code input
            }
        )

    # Sort task args (not a great way to sort, but it is as least something)
    sorted_tasks_args_list = sorted(tasks_args_list, key=lambda x: ['usgs_site_code'])

    # Set up logging for multiprocessing
    mp_log_file_path = os.path.join(output_dir, f"get_rating_curves-mp-{file_datetime_string}.log")
    mp_logger = sf.setup_mp_file_logger(mp_log_file_path, logger_name="mp_rcs")

    # Run multiprocessing and produce a list of dictionaries
    rating_curves_dfs = sf.run_with_mp(
        task_function=__mp_get_site_rating_curve,
        tasks_args_list=sorted_tasks_args_list,
        file_logger=mp_logger,
        max_workers=num_jobs,
        task_id_key='usgs_site_code',
        show_progress=False,
    )
    # TODO: not sure but might be file collisons with many MP's trying to write to one file
    # at the same time. For now, we will jsut let MP have its own in case it gets hung up
    # if the MP gets hung up or throws and exception, if mp log was used script wide
    # it can hang up the entire script as it has a reference from inside the mp to this
    # parent script.

    # Roll the mp log into the master log (for now.. let's leave the error files alone, even the mp ones)
    sf.rollup_log_files(mp_log_file_path, log_file_path)

    # Get the dfs from the multiproc output
    # Note: run_with_mp returns a list of dictionaries keyed with a huc. We don't care about the keys (HUCs), just the values (DFs).

    # If they all fail, rating_curves_dfs will be something like this: {'14238800': None, '01464000': None, '01464907': None}
    # Remove any None values from the dictionary
    rating_curves_dfs_filtered = {k: v for k, v in rating_curves_dfs.items() if v is not None}

    logging.info(f"Retrieved rating curves for {len(rating_curves_dfs_filtered)} sites")
    logging.info(
        f"Failed to retrieve rating curves for {len(rating_curves_dfs) - len(rating_curves_dfs_filtered)} sites"
    )

    # If there's no valid rating curves in the dictionary, make a blank df to return (will get caught in next step)
    if len(rating_curves_dfs_filtered) == 0:
        all_rating_curves = pd.DataFrame()

    elif len(rating_curves_dfs_filtered) > 0:  # Changed to only iterate through the RC's that aren't none
        logging.info(f"Creating DataFrame for {len(rating_curves_dfs_filtered)} retrieved rating curves")

        loop_section_start_dt = datetime.now(timezone.utc)
        for i, value in enumerate(rating_curves_dfs_filtered.values()):
            # Print an update every 500 sites w/duration of that section
            if i % 500 == 0:
                section_dur_msg = fh.print_date_time_duration(
                    loop_section_start_dt, datetime.now(timezone.utc), False
                )
                logging.info(
                    f"Compiling rating curve {i} of {len(rating_curves_dfs_filtered)} - {section_dur_msg}"
                )
                loop_section_start_dt = datetime.now(timezone.utc)  # Reset timestamp

            # Create the output dataframe with all of the valid rating curves
            if i == 0:
                all_rating_curves = value
            else:
                all_rating_curves = pd.concat([all_rating_curves, value])

        # Get a list of sites that have rating curves and sites that don't have rating curves (but had metadata) and update the site_status_df accordingly
        sites_with_rating_curves = all_rating_curves['location_id'].unique().tolist()
        sites_without_rating_curves = site_status_df[
            (site_status_df['metadata_avail'] == 'yes')
            & (~site_status_df['usgs_site_code'].isin(sites_with_rating_curves))
        ]['usgs_site_code'].tolist()

        # Update the site_status_df with the availability of rating curves
        site_status_df.loc[
            site_status_df['usgs_site_code'].isin(sites_with_rating_curves), 'rating_curves_avail'
        ] = 'yes'
        site_status_df.loc[
            site_status_df['usgs_site_code'].isin(sites_without_rating_curves), 'rating_curves_avail'
        ] = 'no'

    # TODO: Should we return rating_curves_dfs, or rating_curves_dfs_filtered?
    return rating_curves_dfs, all_rating_curves, site_status_df


def __write_rc_and_site_files(all_rating_curves, sites_gdf, list_of_gage_sites, output_dir):
    '''
    Write the rating curves and site information to files.

    Arguments
    --------
    all_rating_curves : DataFrame
        A DataFrame containing the rating curves for all sites that were successfully retrieved and processed.
        This DataFrame includes the stage, discharge, datum information, and elevation in NAVD88 for each site
    sites_gdf : GeoDataFrame
        A geospatial layer containing the USGS gage sites for which metadata was retrieved. This layer includes
        attributes such as the site type, coordinate accuracy, and other metadata that can be used for filtering and mapping.
    list_of_gage_sites : LIST
        A list of USGS gage site codes that were input by the user. This is used to determine whether to save
        the full list of sites (if 'all' was selected) or
        just the sites that were processed (if a specific list was provided).
    output_dir : str
        The directory where output files will be saved. This includes the rating curve CSV, site status CSV,
        and potentially the geospatial file of sites if 'all' was selected.

    Returns
    -------
    msg : str
        A message indicating the outcome of the file writing process, including where files were saved and
        what files were saved. This message can be used for logging and user feedback.

    '''

    # Write rating curve dataframe to file
    usgs_rating_curve_file = os.path.join(output_dir, "usgs_rating_curves.csv")
    all_rating_curves.to_csv(usgs_rating_curve_file, index=False)
    logging.info(f"Saved rating curve dataframe to {usgs_rating_curve_file}")

    # If 'all' option specified, reproject then write out shapefile of acceptable sites.
    if list_of_gage_sites == ['all']:  # TODO: Should it also do something if 'all' isn't specified?
        sites_gdf = sites_gdf.to_crs(PREP_PROJECTION)
        usgs_gages_file = os.path.join(output_dir, "usgs_gages.gpkg")
        sites_gdf.to_file(usgs_gages_file, layer='usgs_gages', driver='GPKG', engine='fiona')

        logging.info(f"Saved sites GDF to to {usgs_gages_file}")
        msg = "USGS gage and rating curve files saved"

    else:  # TODO: Decide if this is the desired behavior (why not just save above for list as well?)
        msg = "Rating curve files saved"

    return msg


def make_status_summary(row):
    '''
    Make the status summary column.

    For each row,
    - if metadata_avail is no, add 'no metadata available',
    - if rating_curves_avail is no, add 'no rating curves available',
    - if correct_site_type is no, add 'incorrect site type',
    - if categorical_flows_avail is no, add 'no categorical flows available'
    - if all are yes, status will be 'good'

    Arguments
    ---------
    row : Series
        A row from the site_status_df DataFrame, containing the columns 'metadata_avail',
        'rating_curves_avail', 'correct_site_type', and 'categorical_flows_avail',
        which indicate the status of each of these criteria for a given site.

    Returns
    -------
    status : str
        A summary status string that indicates the overall status of the site based on the criteria.
        This string will be a combination of messages indicating which criteria are not met (e.g., "No metadata
        available; No rating curves available") or "Good" if all criteria are met.

    '''
    status_list = []
    if 'no' in row['metadata_avail']:
        status_list.append('no metadata found')
        # metadata_avail will never be 'not set'

    if 'no' in row['rating_curves_avail']:
        status_list.append('no rating curves found')
    elif 'not set' in row['rating_curves_avail']:
        status_list.append('rating curves not queried for site')

    if 'no' in row['correct_site_type']:
        status_list.append('incorrect site type')
    elif 'not set' in row['correct_site_type']:
        status_list.append('site type not queried for site')

    if 'no' in row['categorical_flows_avail']:
        status_list.append('no categorical flows found')
    elif 'not set' in row['categorical_flows_avail']:
        status_list.append('categorical flows not queried for site')

    if status_list == []:
        status = 'Good'
    else:
        status = '; '.join(status_list)
        status = status.capitalize()

    return status


def get_usgs_rating_curves(list_of_gage_sites, env_file, num_jobs, output_dir):
    '''
    Main function (was usgs_rating_to_elev).

    Returns rating curves, for a set of sites, adjusted to elevation NAVD.
    Currently configured to get rating curve data within CONUS.
    Workflow as follows:
        1a. If 'all' option passed, get metadata for all acceptable USGS sites.
        1b. If a list of sites passed, get metadata for all sites supplied by user.
        2.  Extract datum information for each site.
        3.  If site is in CONUS or AK: convert datum if NGVD88. If site is in PR, VI, or HI, keep datum as LMSL.
        4.  Get rating curve for each site individually
        5.  Convert rating curve to absolute elevation (NAVD) and store in DataFrame
        6.  Append all rating curves to a master DataFrame.

    Outputs:
        Note: The output folder will automatically add a subfolder with today's date and
        add all files to it. ie)/data/inputs/usgs_curves/ (auto addes /20250912/)

        usgs_rating_curves.csv -- A csv containing USGS rating curve as well
        as datum adjustment and rating curve expressed as an elevation (NAVD88).
        ONLY SITES IN CONUS ARE CURRENTLY LISTED IN THIS CSV. To get
        additional sites, the Tidal API will need to be reconfigured and tested.

        log_{date}.csv -- A csv containing gage-specific messages.

        (if all option passed) usgs_gages.gpkg -- a point layer containing ALL USGS gage sites that meet
        certain criteria. In the attribute table is a 'curve' column that will indicate if a rating
        curve is provided in "usgs_rating_curves.csv"

        acceptable_sites_for_rating_curves.csv -- A csv containing all acceptable sites
        that have a rating curve.

        acceptable_sites_for_rating_curves.gpkg -- A geopackage containing all acceptable sites
        that have a rating curve.

        usgs_stage_discharge_cms.csv -- A csv containing the flow values for each flood category
        (action, minor, moderate, major) for each site.  Used by Seirra Testing (rating_curve_comparison)

        TODO: deprecated as of 5/14/25... remove? or do we use this?
        sites_bool_flags.gpkg -- A geopackage containing all acceptable sites


    Arguments
    ---------
    list_of_gage_sites : LIST
        List of all gage site IDs. If all acceptable sites in CONUS are desired
        list_of_gage_sites can be passed 'all' and it will use the get_all_active_usgs_sites
        function to filter out sites that meet certain requirements across CONUS.
    env_file : STR
        Path to environment file containing necessary variables. See README for details on required variables.
    num_jobs : INT
        Number of jobs (workers) used for multi-proc.
    output_dir : STR
        Directory, if specified, where output csv is saved.

    Returns
    -------
    all_rating_curves : Pandas DataFrame
        DF of the rating curves (or blank df if an error occurred or no valid sites available).

    '''

    # Initialize output dfs
    all_rating_curves = pd.DataFrame()
    site_status_df = pd.DataFrame()

    # Validate CPU availability
    total_cpus_available = os.cpu_count()
    if num_jobs > total_cpus_available - 1:
        raise ValueError(
            f'Provided: -j {num_jobs}, which is greater than than amount of available cpus -1: '
            f'{total_cpus_available - 1} will be used instead.'
        )

    # Import variables from .env file and set global variables (if avail.)
    if not os.path.exists(env_file):
        print(f"ERROR: Environment file does not exist: {env_file}")
        sys.exit()
    else:
        print(f'Loading environment file: {env_file}')
        print("")
        set_global_env(env_file)

    if list_of_gage_sites != 'all':
        print(
            "\n"
            "*** NOTICE: You have provide a list of specific usgs site codes to process.\nPlease note that when getting all sites,"
            " it filters to only sites that are active. But when using specific codes, it will not use the 'is active' filter.\n\n"
            "To continue, hit your enter key or CTRL-C to abort"
        )
        input(
            ">> "
        )  # If the user does want to continue, we won't check the value here, just the fact that it was not aborted

    # Check if sites CSV is supplied
    if list_of_gage_sites.endswith('.csv'):
        # Convert csv list to python list
        with open(list_of_gage_sites) as f:
            sites = f.read().splitlines()
        list_of_gage_sites = sites
    else:
        list_of_gage_sites = args['list_of_gage_sites'].split(' ')

    # Define URLs for metadata and rating curve
    metadata_url = f'{API_BASE_URL}/metadata'
    rating_curve_url = f'{API_BASE_URL}/rating_curve'

    # Create output_dir directory if it doesn't exist
    if output_dir == "":
        raise ValueError("Output dir parameter can not be empty")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Set up the main logger
    log_file_path = sf.setup_file_logger(output_dir, "get_rating_curves")

    # Record start time
    overall_start_dt = datetime.now(timezone.utc)
    file_datetime_string = overall_start_dt.strftime("%Y%m%d-%H%M")
    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")

    try:
        print("")
        print("=========================================================================")
        logging.info(f"Get New USGS Rating Curves - {display_dt_string} (UTC)")
        logging.info("")
        print(f"Logs will be saved to {log_file_path}")
        print(f"Saving results in {output_dir}")
        print("")
        logging.info("-------------------------------------------------")
        logging.info("")

        # ------------------------------
        # Get USGS metadata and aggregate by HUC

        section_start_dt = datetime.now(timezone.utc)
        logging.info("Begin retrieving metadata and aggregating by HUCs...")
        logging.info("")

        sites_gdf, metadata_list, site_status_df = __get_usgs_metadata(list_of_gage_sites, metadata_url)
        # If 'all' option passed to list of gages sites, it retrieves all sites within CONUS.
        # This part usually only takes a few mins (up to 8 mins(ish) )

        logging.info("")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Finished retrieving metadata and aggregating by HUCs - {dur_msg}")
        logging.info("-------------------------------------------------")
        logging.info("")

        # Error out if no sites or metadata were retrieved
        if len(sites_gdf) == 0:
            msg = 'No valid sites found. Program aborting.'
            raise Exception(msg)

        if len(metadata_list) == 0:
            msg = 'No metadata was found for any of the sites. Program aborting.'
            raise Exception(msg)

        # ------------------------------
        # Set up and run the multiproc to get rating curves for sites

        section_start_dt = datetime.now(timezone.utc)
        logging.info("Begin retrieving rating curves...")
        logging.info("")
        logging.info(f"Number of sites to process: {len(metadata_list)}")

        rating_curves_dfs, all_rating_curves, site_status_df = __run_rating_curve_retrieval(
            metadata_list,
            site_status_df,
            rating_curve_url,
            output_dir,
            file_datetime_string,
            num_jobs,
            log_file_path,
        )

        logging.info("")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Finished retrieving rating curves - {dur_msg}")
        logging.info("-------------------------------------------------")
        logging.info("")

        # Error out with messages if no rating curves made it past the datum checks
        if len(all_rating_curves) == 0:
            msg = 'No rating curves to compile. Program aborting.'
            raise Exception(msg)

        if len(rating_curves_dfs) == 0:
            msg = "No rating curve DFs to compile. Program aborting."
            raise Exception(msg)

        # ------------------------------
        # Get mainstems attribute and filter out unacceptable sites

        section_start_dt = datetime.now(timezone.utc)
        logging.info("Begin getting mainstem attribute and filtering sites...")
        logging.info("")

        all_rating_curves, site_status_df = __attrib_mainstems_filter_sites(
            sites_gdf, all_rating_curves, site_status_df, output_dir
        )

        logging.info("")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Finished getting mainstem attribute and filtering sites - {dur_msg}")
        logging.info("-------------------------------------------------")
        logging.info("")

        # ------------------------------
        # Save rating curve file, site status file, and USGS gages file (if 'all' is selected)

        section_start_dt = datetime.now(timezone.utc)
        logging.info("Begin saving output files...")
        logging.info("")

        msg = __write_rc_and_site_files(all_rating_curves, sites_gdf, list_of_gage_sites, output_dir)

        logging.info("")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"{msg} - {dur_msg}")
        logging.info("-------------------------------------------------")
        logging.info("")

        # ------------------------------
        # Write out categorical flow files for each threshold across all available sites

        section_start_dt = datetime.now(timezone.utc)
        logging.info("Begin getting stage discharge values...")
        logging.info("")

        site_status_df = __write_categorical_flow_files(
            metadata_list, site_status_df, output_dir, file_datetime_string, log_file_path, num_jobs
        )

        logging.info("")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Finished getting stage discharge values - {dur_msg}")
        logging.info("-------------------------------------------------")
        logging.info("")

    except Exception as ex:
        logging.critical(f"Exception occured: {ex}")
        logging.critical(traceback.format_exc())

    # If site status df is not empty, save it to file. If it is empty, log that it is empty and skip saving.
    if len(site_status_df) == 0:
        logging.warning("Site status dataframe is empty. Skipping saving site status file.")
    else:

        section_start_dt = datetime.now(timezone.utc)
        logging.info("Begin summarizing NWS LID site statuses and saving table...")
        logging.info("")

        site_status_df['status'] = site_status_df.apply(make_status_summary, axis=1)

        # site_status_df['status'] = site_status_df['status'].astype(str).str.strip('"')

        # Save the site status df to file
        site_status_file = os.path.join(output_dir, "nws_site_status.csv")
        site_status_df.to_csv(site_status_file, index=False)
        logging.info(f"Saved site status dataframe to {site_status_file}")

        logging.info("")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Finished summarizing NWS LID site statuses and saving table - {dur_msg}")
        logging.info("-------------------------------------------------")
        logging.info("")

    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    dur_msg = fh.print_date_time_duration(overall_start_dt, datetime.now(timezone.utc), False)

    logging.info(f"Program complete - {display_dt_string} (UTC)")
    print(f"Logs saved to {log_file_path}")
    logging.info(f"{dur_msg}")
    print("=========================================================================")

    return all_rating_curves


if __name__ == '__main__':
    '''
    Retrieve USGS rating curves adjusted to elevation (NAVD88).
    Recommend running outside of business hours to reduce API related errors.
    If error occurs try increasing sleep time (from default of 1).

    Arguments:
    -l, --list_of_gage_sites: REQUIRED. Gage sites to process. Can be a space-delineated list of
                                gage sites, a CSV (one site per line), or use “all” to get all USGS
                                gage sites. Use numerical USGS site codes not NWS LIDS.
    -o, --output_dir:         OPTIONAL. Directory to save outputs.

    Example usage:

    Download all sites to outputs folder
        python /foss_fim/data/usgs/get_usgs_rating_curves.py -l 'all' \
            -o '/data/inputs/usgs_gages' -j 30

    Download certain sites to outputs folder
        python /foss_fim/data/usgs/get_usgs_rating_curves.py -l '04228500 04228502' \
            -o '/data/inputs/usgs_gages' -j 30

    The program will automatically add a subfolder with todays date: ie) /20250912/

    **********************
    Don't worry if you see a very large number of lines showing Error or Warning reported for {usgs site id}.
    The majority of the sites are rejected for various reasons, but mostly becuase they don't have rating curves.
    **********************

    '''

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Retrieve USGS rating curves adjusted to elevation (NAVD88).\n'
        'Currently configured to get rating curves within CONUS.\n'
        'Recommend running outside of business hours to reduce API related errors.'
    )
    parser.add_argument(
        '-l',
        '--list-of-gage-sites',
        help='REQUIRED: "all" for all active usgs sites, specify individual sites separated by space, '
        'or provide a csv of sites (one per line).',
        required=True,
    )
    parser.add_argument(
        '-o',
        '--output-dir',
        help='REQUIRED: Directory where all outputs will be stored.',
        default="",
        required=True,
    )
    parser.add_argument('-j', "--num-jobs", help="OPTIONAL: Number of processes", type=int, default=1)
    parser.add_argument(
        '-e',
        '--env-file',
        help='OPTIONAL: Docker mount path to the environment file.'
        'default = /data/config/fim_enviro_values.env',
        required=False,
        default='/data/config/fim_enviro_values.env',
    )

    args = vars(parser.parse_args())
    get_usgs_rating_curves(**args)  # was usgs_rating_to_elev
