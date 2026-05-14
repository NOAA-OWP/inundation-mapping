#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv

import src.utils.shared_functions as sf
import tools.tools_shared_functions as tsf
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

    Returns
    -------
    None.

    '''
    # Get metadata for all usgs_site_codes that are active in the U.S.
    # Note: GU (Guam) and AS (American Somoa) do not come in as they fail the "active" test.
    metadata_url = f'{API_BASE_URL}/metadata'
    # Define arguments to retrieve metadata and then get metadata from WRDS
    select_by = 'usgs_site_code'
    selector = ['all']
    must_include = 'usgs_data.active'
    metadata_list, ___ = tsf.get_metadata(
        metadata_url,
        select_by,
        selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=None,
    )
    # Get a geospatial layer (gdf) for all acceptable sites
    print("Aggregating WBD HUCs...")
    ___, gdf = tsf.aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)

    # Rename gdf fields
    gdf.columns = gdf.columns.str.replace('identifiers_', '')

    return gdf, metadata_list


# We are not using the screen queue at this time but it is available for console outputs in mps
# where we do not want it in the logs. Enforced by run_with_mp at this time.
def __mp_get_flows_for_site(
    site_data_json, nws_lid, usgs_site_code, threshold_url, file_logger, screen_queue, task_id
):

    try:
        sf.l_print(f"Processing flow data for lid: {task_id}", file_logger, "debug", screen_queue)

        # there is no try catch as I want any errors to shut down the entire tool
        # try:
        feature_id = site_data_json.get('identifiers').get('nwm_feature_id')

        # Get the stages and flows
        ___, flows, ___ = tsf.get_thresholds(
            threshold_url,
            select_by='nws_lid',
            selector=nws_lid,
            threshold='all',
            source_crs_availability=None,
        )

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
    metadata_list, output_dir, file_datetime_string, parent_log_file, num_jobs
):
    '''
    Writes flow files of each category for every feature_id in the input metadata.
    Written to supply input flow files of all gage sites for each flood category.

    Parameters
    ----------
    metadata : DICT
        Dictionary of metadata from WRDS (e.g. output from get_all_active_usgs_sites).
    output_dir : STR
        Path to output_dir where flow files will be saved.

    parent_log_file:
        This function uses its own mp log file. This arg allows the mp logs to be
        appended to the parent when it is done.
        Note: For now.. any error files created anywhere are not removed.
    '''

    threshold_url = f'{API_BASE_URL}/nws_threshold'
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Remove sites that have value of None for the nws_lid
    # Not required, but helps reduce the number of sites to process
    # metadata = [site for site in metadata if site.get('identifiers').get('nws_lid') != None]

    # For each site in metadata_trimmed
    num_sites = len(metadata_list)
    logging.info(f"Number of sites to process: {num_sites}")

    # we now only process them if they have nws data
    task_args_list = []
    for i in range(num_sites):
        site_data_json = metadata_list[i]

        feature_id = site_data_json.get('identifiers').get('nwm_feature_id')
        nws_lid = site_data_json.get('identifiers').get('nws_lid')
        usgs_site_code = site_data_json.get('identifiers').get('usgs_site_code')

        # thresholds only provided for valid nws_lid.
        if nws_lid == 'Bogus_ID':  # Legacy test?
            logging.warning("Bogus_ID value found")
            continue

        # if invalid feature_id skip to next site
        if nws_lid is None or nws_lid == "":
            logging.warning(f"usgs site code of {usgs_site_code} does not have a nws_lid value")
            continue

        # if invalid feature_id skip to next site
        if feature_id is None:
            logging.warning(f"usgs site code of {usgs_site_code} does not have a feature id value")
            continue

        task_args_list.append(
            {
                "site_data_json": site_data_json,
                "nws_lid": nws_lid,
                "usgs_site_code": usgs_site_code,
                "threshold_url": threshold_url,
            }
        )

    sorted_tasks_args_list = sorted(task_args_list, key=lambda x: ['nws_lid'])

    # Again.. we make a special mp for this set
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
    else:
        logging.info("No flow data was found. Saving of usgs_stage_discharge_cms file skipped")


def set_global_env(env_file):
    global API_BASE_URL, WBD_LAYER, NWM_FLOWS_MS
    load_dotenv(env_file)

    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    NWM_FLOWS_MS = os.getenv("NWM_FLOWS_MS")


# Can not use the one from Shared_functions as we need to build up a
# concat of dataframes


# ++++++++++++++++++++++++++++++++
# TODO: likely don't need usgs_site_code if we have the task id
# Yes... usgs_site_code and task_id are redundant for now
def __mp_get_site_rating_curve(
    metadata_json, rating_curve_url, usgs_site_code, file_logger, screen_queue, task_id
):

    try:
        # Get datum information for site (only need usgs_data)
        sf.l_print(f"Getting rating curves for usgs location id of {usgs_site_code}", file_logger, "info")

        # debug... if you are in mp and want screen only, use screen_queue
        # screen_queue.put(f"testing my mp code {usgs_site_code}")

        # for debugging, shut off when in real mode so it uses the progress bar.
        # screen_queue.put(f"Getting rating curves for usgs location id of {usgs_site_code}")

        ___, usgs = tsf.get_datum(metadata_json)

        # Get rating curve for site
        location_id = usgs['usgs_site_code']  # in theory we get one and exactly one here

        # # Filter out sites that are not in contiguous US. If this section is removed be sure to test with
        # #   datum adjustment section (region will need changed)
        # if usgs['state'] in ['Puerto Rico', 'Virgin Islands', 'Hawaii']: # Removed May 2025
        #     continue

        curve = tsf.get_rating_curve(rating_curve_url, location_ids=[location_id])

        # If no rating curve was returned, skip site.
        if curve.empty:
            sf.l_print(
                f'{location_id}: Removed because it has no rating curves', file_logger, "info", screen_queue
            )
            return 0, None  # log and continue the next task

        # If the site is in PR, VI, or HI, keep datum in LMSL (local mean sea level)
        # because our 3DEP dems are also in LMSL for these areas.
        # Sept 2025, GU and AS (Guam and American Samoa don't come in as they
        #  fail the "must_include = 'usgs_data.active'" test.
        if usgs['state'] in ['Puerto Rico', 'Virgin Islands', 'Hawaii']:
            if usgs['vcs'] == 'LMSL':
                navd88_datum = usgs['datum']
                # left of the screen queue so it goes to log only
                sf.l_print(
                    f'{location_id}: site is in PR, VI, or HI, so datum kept as LMSL', file_logger, "debug"
                )
            else:
                # If the site is in PR, VI, or HI, and has a datum other than LMSL, return an error.
                datum_name = usgs['vcs']
                message = (
                    f'{location_id}: Removed because site is located PR,'
                    f'VI, or HI but has a datum other than LMSL ({datum_name})'
                )
                # file_logger.warning(message)
                sf.l_print(message, file_logger, "warning")
                return 0, None  # log and continue the next task

        # If the state is not PR, VI, or HI, then we want to adjust the datum to NAVD88 if needed.
        # If the datum is unknown, skip site.
        else:
            if usgs['vcs'] == 'NGVD29':

                # Get the datum adjustment to convert NGVD to NAVD.
                datum_adj_ft, err_msg = tsf.ngvd_to_navd_ft(datum_info=usgs)

                if err_msg != "":
                    sf.l_print(f'{location_id}: {err_msg}', file_logger, "error")
                    return 0, None  # log and continue the next task

                # If datum API failed, print message and skip site.
                if datum_adj_ft is None:
                    # file_logger.warning(f'{location_id}: Removed because datum adjustment failed!!')
                    sf.l_print(
                        f'{location_id}: Removed because datum adjustment failed!!', file_logger, "warning"
                    )
                    return 0, None  # log and continue the next task

                # If datum adjustment succeeded, calculate datum in NAVD88
                navd88_datum = round(usgs['datum'] + datum_adj_ft, 2)
                # file_logger.debug(f'{location_id}: successfully converted NGVD29 to NAVD88')
                sf.l_print(f'{location_id}: successfully converted NGVD29 to NAVD88', file_logger, "debug")

            elif usgs['vcs'] == 'NAVD88':
                navd88_datum = usgs['datum']
                # file_logger.debug(f'{location_id}: already NAVD88')
                sf.l_print(f'{location_id}: already NAVD88', file_logger, "debug")

            elif usgs['vcs'] == 'LMSL':
                # If the site has a vdatum of LMSL and is not in PR, VI or HI, skip site.
                # file_logger.warning(
                #     f'{location_id}: Removed because LMSL datum found outside of PR, VI, or HI'
                # )
                sf.l_print(
                    f'{location_id}: Removed because LMSL datum found outside of PR, VI, or HI',
                    file_logger,
                    "warning",
                )
                return 0, None  # log and continue the next task

            else:
                # If the site has an unrecognized datum, skip site.
                datum_name = usgs['vcs']
                # file_logger.warning(f'{location_id}: Removed due to unknown datum ({datum_name})')
                sf.l_print(
                    f'{location_id}: Removed due to unknown datum ({datum_name})', file_logger, "warning"
                )
                return 0, None  # log and continue the next task

        # Populate rating curve with metadata and use navd88 datum to convert stage to elevation.
        # If you came in looking for all sites, then "active" will be true. A filtered set, this might be True or False
        curve['active'] = usgs['active']
        curve['datum'] = usgs['datum']
        curve['datum_vcs'] = usgs['vcs']
        curve['navd88_datum'] = navd88_datum
        curve['elevation_navd88'] = round(curve['stage'] + navd88_datum, 2)

        # file_logger.debug(f"Done rating curves for usgs location id of {usgs_site_code}")
        sf.l_print(f"Done rating curves for usgs location id of {usgs_site_code}", file_logger, "debug")
        return 1, curve

    except Exception:
        # file_logger.critical(f"❌ Critical error while processing {task_id}")
        # file_logger.critical(traceback.format_exc())
        sf.l_print(f'❌ Critical error while processing {task_id}', file_logger, "critical", screen_queue)
        sf.l_print(traceback.format_exc(), file_logger, "critical", screen_queue)
        # screen_queue.put(f"❌ Critical error while processing {task_id}")
        # screen_queue.put(traceback.format_exc())
        return -1, None  # shut the program down.


def __get_usgs_metadata(list_of_gage_sites, metadata_url):

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

        # Since there is a limit to number characters in url, split up selector if too many sites.
        max_sites = 20  # Can we go more than 20? do we want to?
        metadata_list = []

        if len(selector) > max_sites: # TODO: Do we even need to keep this? I don't feel like we're prioritizing large-scale runs with a list input...
            chunks = [selector[i : i + max_sites] for i in range(0, len(selector), max_sites)]
            # Get metadata for each chunk
            # metadata_df = pd.DataFrame()  # Feb 2026: metadata_df is not in use and has not been since at least Jun 2025
            for chunk in chunks:
                chunk_list, __ = tsf.get_metadata(
                    metadata_url,
                    select_by,
                    chunk,
                    must_include=None,
                    upstream_trace_distance=None,
                    downstream_trace_distance=None,
                )

                # If one of the gage IDs is incorrect, then it will error out the entire chunk/list of gages
                # If the metadata list is zero, rerun sites individually to ensure that is not preventing data from being retrieved
                if len(chunk_list) == 0:
                    print()
                    logging.warning(f'No metadata collected for the {len(chunk)} sites. Re-running each site individually.')

                    for gage in chunk:
                        try:
                            gage_list, __ = tsf.get_metadata(
                                metadata_url,
                                select_by,
                                [gage],
                                must_include=None,
                                upstream_trace_distance=None,
                                downstream_trace_distance=None,
                            )
                            # Append chunk data to metadata_list/df
                            chunk_list.extend(gage_list)

                        except Exception as ex:
                            logging.error(f'Exception occurred while pulling data for site {gage}: {ex}')
                
                # Append chunk data to metadata_list/df
                metadata_list.extend(chunk_list)

                # Feb 2026: metadata_df is not in use and has not been since at least Jun 2025
                # metadata_df = pd.concat([metadata_df, chunk_df])
        else:
            # If selector has less than max sites, then get metadata.
            metadata_list, __ = tsf.get_metadata(
                metadata_url,
                select_by,
                selector,
                must_include=None,
                upstream_trace_distance=None,
                downstream_trace_distance=None,
            )

            # If one of the gage IDs is incorrect, then it will error out the entire chunk/list of gages
            # If the metadata list is zero, rerun sites individually to ensure that is not preventing data from being retrieved
            if len(metadata_list) == 0:
                logging.warning(f'No metadata collected for the {len(selector)} sites. Re-running each site individually.')

                for gage in list_of_gage_sites:
                    try:
                        print()
                        logging.info(f'Re-running site {gage}')
                        gage_list, __ = tsf.get_metadata(
                            metadata_url,
                            select_by,
                            [gage],
                            must_include=None,
                            upstream_trace_distance=None,
                            downstream_trace_distance=None,
                        )
                        # Append chunk data to metadata_list/df
                        metadata_list.extend(gage_list)

                    except Exception as ex:
                        logging.error(f'Exception occurred while pulling data for site {gage}: {ex}')

        # Get a geospatial layer (gdf) for all acceptable sites
        print()
        logging.info("Aggregating WBD HUCs...")
        ___, sites_gdf = tsf.aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)
        if sites_gdf is not None and not sites_gdf.empty:
            # Rename gdf fields
            sites_gdf.columns = sites_gdf.columns.str.replace('identifiers_', '')
        else:
            logging.error("There are no acceptable sites.")
            sys.exit()

    if len(metadata_list) == 0:
        logging.error("No metadata was found for any of the sites")
        sys.exit()

    sites_gdf = sites_gdf.astype({'metadata_sources': str})
    sites_gdf = sites_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')
    sites_gdf = sites_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')

    return sites_gdf, metadata_list


def __attrib_mainstems(sites_gdf, all_rating_curves, output_dir):


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

    # Debugging tool
    # sites_gdf.to_csv(os.path.join(output_dir, f'acceptable_sites_pre.csv'))

    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    # logging.info(f"Recasting... {dt_string} (UTC) ")

    # TODO: : Jun 2025: - Update Sep 9, 2025: Does not yet seem to be used.
    # Figure out if we have a use for sites_bool_flags.gpkg and add this back in if needed.
    # # -- Filter all_rating_curves according to acceptance criteria -- #
    # # -- We only want acceptable gages in the rating curve CSV -- #
    # sites_gdf['acceptable_codes'] = (
    #     sites_gdf['usgs_data_coord_accuracy_code'].isin(acceptable_coord_acc_code_list)
    #     & sites_gdf['usgs_data_coord_method_code'].isin(acceptable_coord_method_code_list)
    #     & sites_gdf['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list)
    #     & sites_gdf['usgs_data_site_type'].isin(acceptable_site_type_list)
    # )
    # sites_gdf = sites_gdf.astype({'usgs_data_alt_accuracy_code': float})
    # sites_gdf['acceptable_alt_error'] = np.where(
    #     sites_gdf['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh, True, False
    # )

    # sites_gdf.to_file(os.path.join(output_dir, 'sites_bool_flags.gpkg'), driver='GPKG', engine='fiona')

    # -- Filter out non stream sites-- #
    # -- The other acceptance criteria will be filtered out the scripts where the data is used -- #
    sites_gdf['acceptable_site_type'] = sites_gdf['usgs_data_site_type'].isin(acceptable_site_type_list)

    # Filter to acceptable sites and save filtered sites file for viewing
    acceptable_sites_gdf = sites_gdf[sites_gdf['acceptable_site_type'] == True]
    acceptable_sites_gdf = acceptable_sites_gdf[acceptable_sites_gdf['curve'] == 'yes']

    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    logging.info(f"Saving acceptable rating curve files... {display_dt_string} (UTC) ")
    acceptable_sites_gdf.to_csv(os.path.join(output_dir, 'acceptable_sites_for_rating_curves.csv'))
    acceptable_sites_gdf.to_file(
        os.path.join(output_dir, 'acceptable_sites_for_rating_curves.gpkg'), driver='GPKG', engine='fiona'
    )

    # Make list of acceptable sites
    acceptable_sites_list = acceptable_sites_gdf['location_id'].tolist()

    return acceptable_sites_list


# Generate USGS rating curves
def usgs_rating_to_elev(list_of_gage_sites, env_file, num_jobs, output_dir):
    '''

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


    Parameters
    ----------
    list_of_gage_sites : LIST
        List of all gage site IDs. If all acceptable sites in CONUS are desired
        list_of_gage_sites can be passed 'all' and it will use the get_all_active_usgs_sites
        function to filter out sites that meet certain requirements across CONUS.

    output_dir : STR
        Directory, if specified, where output csv is saved.

    num_jobs : INT
        Number of jobs (workers) used for multi-proc.

    Returns
    -------
    all_rating_curves : Pandas DataFrame
        DF of the rating curves (or blank df if an error occurred or no valid sites available).

    '''

    # Initialize output df for rating curves
    all_rating_curves = pd.DataFrame()

    # Validation
    total_cpus_available = os.cpu_count()
    if num_jobs > total_cpus_available - 1:
        raise ValueError(
            f'Provided: -j {num_jobs}, which is greater than than amount of available cpus -1: '
            f'{total_cpus_available - 1} will be used instead.'
        )

    # Import variables from .env file
    if not os.path.exists(env_file):
        print(f"ERROR: Environment file does not exist: {env_file}")
        sys.exit()
    else:
        print(f'Loading environment file: {env_file}')
        print("")
        # Set global variables
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
        )  # if they user does want to continue, we won't check the value here, just the fact that it was not aborted

    # Check if csv is supplied
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

    overall_start_dt = datetime.now(timezone.utc)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    file_datetime_string = overall_start_dt.strftime("%Y%m%d-%H%M")
    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")

    log_file_path = sf.setup_file_logger(output_dir, "get_rating_curves")
    # mp_file_logger = sf.setup_mp_file_logger(log_file_path)

    try:
        print("\n=========================================================================\n")
        logging.info(f"Get New USGS Rating Curves - {display_dt_string} (UTC)\n")
        print()
        print(f"Logs will be saved to {log_file_path}")
        print(f"Saving results in {output_dir}")
        print()
        logging.info("\n-------------------------------------------------\n")

        # ------------------------------
        # Get USGS metadata and aggregate by HUC

        logging.info("Retrieving metadata and aggregating by HUCs...\n")
        section_start_dt = datetime.now(timezone.utc)

        # If 'all' option passed to list of gages sites, it retrieves all sites within CONUS.
        # This part usually only takes a few mins (up to 8 mins(ish) )
        sites_gdf, metadata_list = __get_usgs_metadata(list_of_gage_sites, metadata_url)

        # Save an interium copy of the metadata # TODO: Clean up?
        # sites_gdf = sites_gdf.to_crs(PREP_PROJECTION)
        # usgs_metadata_file = os.path.join(output_dir, "usgs_metadata.gpkg")
        # print(f"Saving a copy of the raw usgs metadata to {usgs_metadata_file}")
        # sites_gdf.to_file(usgs_metadata_file, layer='usgs_gages', driver='GPKG', engine='fiona')

        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"\nFinished retrieving metadata and aggregating by HUCs - {dur_msg}")
        logging.info("\n-------------------------------------------------\n")

        # ------------------------------
        # Process metadata (creates DF to store all appended rating curves)

        logging.info("Begin processing metadata...\n")
        section_start_dt = datetime.now(timezone.utc)

        num_sites = len(metadata_list)
        logging.info(f"Number of sites to process: {num_sites}")
        print("-- Note: some locations will be skipped") # TODO: What do we mean by this? Should clarify or remove

        tasks_args_list = []
        for i in range(len(metadata_list)):
            metadata_json = metadata_list[i]

            usgs_site_code = metadata_json['identifiers']['usgs_site_code']
            tasks_args_list.append(
                {
                    "metadata_json": metadata_json,
                    "rating_curve_url": rating_curve_url,
                    "usgs_site_code": usgs_site_code,
                }
            )

        # Run multiprocessing
        # not a great way to sort, but it is as least something
        sorted_tasks_args_list = sorted(tasks_args_list, key=lambda x: ['usgs_site_code'])

        # TODO: not sure but might be file collisons with many MP's trying to write to one file
        # at the same time. For now, we will jsut let MP have its own in case it gets hung up
        # if the MP gets hung up or throws and exception, if mp log was used script wide
        # it can hang up the entire script as it has a reference from inside the mp to this
        # parent script.
        mp_log_file_path = os.path.join(output_dir, f"get_rating_curves-mp-{file_datetime_string}.log")
        mp_logger = sf.setup_mp_file_logger(mp_log_file_path, logger_name="mp_rcs")
        # We get a list of dictionaries
        rating_curves_dfs = sf.run_with_mp(
            task_function=__mp_get_site_rating_curve,
            tasks_args_list=sorted_tasks_args_list,
            file_logger=mp_logger,
            max_workers=num_jobs,
            task_id_key='usgs_site_code',
            show_progress=False,
        )

        # Roll the mp log into the master log.
        sf.rollup_log_files(mp_log_file_path, log_file_path)
        # for now.. let's leave the error files alone, even the mp ones

        # more processing of rating curves
        if len(rating_curves_dfs) == 0:
            logging.error("There are no acceptable sites. Stopping program.")
            sys.exit(1)

        # run_with_mp returns a list of dictionaries keyed with a huc. We don't care about the keys, just the values
        # which are df's
        for i, value in enumerate(rating_curves_dfs.values()):
            if i == 0:
                all_rating_curves = value
            else:
                all_rating_curves = pd.concat([all_rating_curves, value])

        logging.info(f"Number of sites to processes with metadata: {len(all_rating_curves)}")

        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"\nFinished processing metadata - {dur_msg}")
        logging.info("\n-------------------------------------------------\n")

        # Error out with messages if no rating curves made it past the datum checks
        if len(all_rating_curves) == 0:
            logging.error('ERROR: No rating curves to compile. Program aborting.')
            sys.exit(1)

        # ------------------------------
        # Attribute mainstem sites (Add mainstems attribute to acceptable sites)

        logging.info("Begin attributing mainstem sites...\n")
        section_start_dt = datetime.now(timezone.utc)

        # Filter out all_rating_curves by list
        acceptable_sites_list = __attrib_mainstems(sites_gdf, all_rating_curves, output_dir)
        all_rating_curves = all_rating_curves[all_rating_curves['location_id'].isin(acceptable_sites_list)]

        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"\nFinished attributing mainstems sites - {dur_msg}")
        logging.info("\n-------------------------------------------------\n")

        # ------------------------------
        # Save rating curve file and USGS gages file (if 'all' is selected)

        logging.info("Saving output files...\n")
        section_start_dt = datetime.now(timezone.utc)

        # Write rating curve dataframe to file
        usgs_rating_curve_file = os.path.join(output_dir, "usgs_rating_curves.csv")
        all_rating_curves.to_csv(usgs_rating_curve_file, index=False)

        # If 'all' option specified, reproject then write out shapefile of acceptable sites.
        if list_of_gage_sites == ['all']: # TODO: Should it also do something if 'all' isn't specified?
            sites_gdf = sites_gdf.to_crs(PREP_PROJECTION)
            usgs_gages_file = os.path.join(output_dir, "usgs_gages.gpkg")
            sites_gdf.to_file(usgs_gages_file, layer='usgs_gages', driver='GPKG', engine='fiona')

            msg = "USGS gage and rating curve files saved"

        else: # TODO: Decide if this is the desired behavior (why not just save above for list as well?)
            msg = "Rating curve files saved"

        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"\n{msg} - {dur_msg}")
        logging.info("\n-------------------------------------------------\n")

        # ------------------------------
        # Write categorical flow files

        # Write out flow files for each threshold across all sites
        logging.info("Getting stage discharge values...\n")
        section_start_dt = datetime.now(timezone.utc)

        __write_categorical_flow_files(
            metadata_list, output_dir, file_datetime_string, log_file_path, num_jobs
        )

        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"\nFinished getting stage discharge values - {dur_msg}")
        logging.info("\n-------------------------------------------------\n")

    except Exception as ex:
        logging.critical(f"Exception occured inside usgs_rating_to_elev: {ex}")
        logging.critical(traceback.format_exc())

    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    dur_msg = fh.print_date_time_duration(overall_start_dt, datetime.now(timezone.utc), False)
    logging.info(f"\nProgram complete - {display_dt_string} (UTC)")
    logging.info(f"{dur_msg}")
    print("\n=========================================================================\n")

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
    usgs_rating_to_elev(**args)
