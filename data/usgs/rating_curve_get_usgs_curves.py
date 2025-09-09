#!/usr/bin/env python3
import argparse
import logging
import os
import shutil
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv

import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_variables import PREP_PROJECTION

import tools.tools_shared_functions as tsf
from tools.tools_shared_variables import acceptable_site_type_list

gpd.options.io_engine = "pyogrio"


'''
This script calls the NOAA Tidal API for datum conversions. Experience shows that
    running script outside of business hours seems to be most consistent way
    to avoid API errors. Currently configured to get rating curve data within
    CONUS.

    Tidal API call may need to be modified to get datum conversions for AK. # TODO: Alaska updates?
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
def __mp_get_flows_for_site(site_data_json, nws_lid, usgs_site_code, threshold_url, file_logger, screen_queue, task_id):

    file_logger.debug(f"Processing flow data for lid: {task_id}")
    
    feature_id = site_data_json.get('identifiers').get('nwm_feature_id')

    # Get the stages and flows
    ___, flows, ___ = tsf.get_thresholds(
        threshold_url, select_by='nws_lid', selector=nws_lid, threshold='all'
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
            
    return site_flows_df


##############################################################################
# Generate categorical flows for each category across all sites.
##############################################################################
def __write_categorical_flow_files(metadata_list, output_dir, file_date_append, file_datetime_string, master_log_file, num_jobs):
    '''
    Writes flow files of each category for every feature_id in the input metadata.
    Written to supply input flow files of all gage sites for each flood category.

    Parameters
    ----------
    metadata : DICT
        Dictionary of metadata from WRDS (e.g. output from get_all_active_usgs_sites).
    output_dir : STR
        Path to output_dir where flow files will be saved.

    Returns
    -------
    all_data : DataFrame
        A dataframe of categorical flow for every feature ID in the input metadata.

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
    list_flow_dfs = sf.run_with_mp(
        task_function=__mp_get_flows_for_site,
        tasks_args_list=sorted_tasks_args_list,
        file_logger=mp_logger,
        max_workers=num_jobs,
        task_id_key='nws_lid',
        exit_on_failure=True,
        show_progress=True,
    )

    # Roll the mp log into the master log.
    with open(mp_log_file_path, 'r') as src_file:
        with open(master_log_file, 'a') as master_log_file:
            shutil.copyfileobj(src_file, master_log_file)
    # for now.. let's leave the error files alone, even the mp ones            
    os.remove(mp_log_file_path)
    
    # roll up the list of df's into one master df
    all_flows_data = pd.concat(list_flow_dfs, ignore_index=True)

    # Write usgs stage discharge data, used by Sierra tests (rating_curve_comparison.py)
    logging.info("Writing for USGS discharge data for each usgs stage (ie. action, minor, etc)")
    if not all_flows_data.empty:
        usgs_discharge_file_name = os.path.join(
            output_dir, f'usgs_stage_discharge_cms_{file_date_append}.csv'
        )
        final_data = all_flows_data[['feature_id', 'discharge_cms', 'recurr_interval']]
        final_data.to_csv(usgs_discharge_file_name, index=False)

    return all_flows_data


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
def __mp_get_site_rating_curve(metadata_json, rating_curve_url, usgs_site_code, file_logger, screen_queue, task_id):
    
    # Get datum information for site (only need usgs_data)
    file_logger.info(f"Getting rating curves for usgs location id of {usgs_site_code}")
    
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
        file_logger.warning(f'{location_id}: Removed because it has no rating curves')
        return None

    # If the site is in PR, VI, or HI, keep datum in LMSL (local mean sea level)
    # because our 3DEP dems are also in LMSL for these areas.
    # Sept 2025, GU and AS (Guam and American Samoa don't come in as they
    #  fail the "must_include = 'usgs_data.active'" test.
    if usgs['state'] in ['Puerto Rico', 'Virgin Islands', 'Hawaii']:
        if usgs['vcs'] == 'LMSL':
            navd88_datum = usgs['datum']
            file_logger.debug(f'{location_id}: site is in PR, VI, or HI, so datum kept as LMSL')
        else:
            # If the site is in PR, VI, or HI, and has a datum other than LMSL, return an error.
            datum_name = usgs['vcs']
            message = f'{location_id}: Removed because site is located PR,' \
            f'VI, or HI but has a datum other than LMSL ({datum_name})'
            file_logger.warning(message)
            return None

    # If the state is not PR, VI, or HI, then we want to adjust the datum to NAVD88 if needed.
    # If the datum is unknown, skip site.
    else:
        if usgs['vcs'] == 'NGVD29':

            # Get the datum adjustment to convert NGVD to NAVD.
            datum_adj_ft = tsf.ngvd_to_navd_ft(datum_info=usgs)

            # If datum API failed, print message and skip site.
            if datum_adj_ft is None:
                file_logger.warning(f'{location_id}: Removed because datum adjustment failed!!')
                return None

            # If datum adjustment succeeded, calculate datum in NAVD88
            navd88_datum = round(usgs['datum'] + datum_adj_ft, 2)
            file_logger.debug(f'{location_id}: succesfully converted NGVD29 to NAVD88')

        elif usgs['vcs'] == 'NAVD88':
            navd88_datum = usgs['datum']
            file_logger.debug(f'{location_id}: already NAVD88')

        elif usgs['vcs'] == 'LMSL':
            # If the site has a vdatum of LMSL and is not in PR, VI or HI, skip site.
            file_logger.warning(
                f'{location_id}: Removed because LMSL datum found outside of PR, VI, or HI'
            )
            return None

        else:
            # If the site has an unrecognized datum, skip site.
            datum_name = usgs['vcs']
            file_logger.warning(f'{location_id}: Removed due to unknown datum ({datum_name})')
            return None

    # Populate rating curve with metadata and use navd88 datum to convert stage to elevation.
    # If you came in looking for all sites, then "active" will be true. A filtered set, this might be True or False
    curve['active'] = usgs['active']
    curve['datum'] = usgs['datum']
    curve['datum_vcs'] = usgs['vcs']
    curve['navd88_datum'] = navd88_datum
    curve['elevation_navd88'] = curve['stage'] + navd88_datum

    file_logger.debug(f"Done rating curves for usgs location id of {usgs_site_code}")
    
    # for debugging, shut off when in real mode so it uses the progress bar.    
    # screen_queue.put(f"Done rating curves for usgs location id of {usgs_site_code}")

    return curve


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
        logging.info(f"Getting metadata for selected sites : {selector}")

        # Since there is a limit to number characters in url, split up selector if too many sites.
        max_sites = 20  # Can we go more than 20? do we want to?
        if len(selector) > max_sites:
            chunks = [selector[i : i + max_sites] for i in range(0, len(selector), max_sites)]
            # Get metadata for each chunk
            metadata_list = []
            metadata_df = pd.DataFrame()
            for chunk in chunks:
                chunk_list, chunk_df = tsf.get_metadata(
                    metadata_url,
                    select_by,
                    chunk,
                    must_include=None,
                    upstream_trace_distance=None,
                    downstream_trace_distance=None,
                )
                # Append chunk data to metadata_list/df
                metadata_list.extend(chunk_list)
                metadata_df = pd.concat([metadata_df, chunk_df])
        else:
            # If selector has less than max sites, then get metadata.
            metadata_list, metadata_df = tsf.get_metadata(
                metadata_url,
                select_by,
                selector,
                must_include=None,
                upstream_trace_distance=None,
                downstream_trace_distance=None,
            )
        # Get a geospatial layer (gdf) for all acceptable sites
        logging.info("Aggregating WBD HUCs...")
        ___, sites_gdf = tsf.aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)
        if not sites_gdf.empty:
            # Rename gdf fields
            sites_gdf.columns = sites_gdf.columns.str.replace('identifiers_', '')
        else:
            logging.error("There are no acceptable sites.")
            sys.exit()

    if len(metadata_list) == 0:
        logging.error("No metadata was found for any of the sites")
        sys.exit()
            
    return sites_gdf, metadata_list


def __attrib_mainstems(sites_gdf, all_rating_curves, output_dir, file_date_append):

    # Add mainstems attribute to acceptable sites
    section_start_dt = datetime.now(timezone.utc)
    display_dt_string = section_start_dt.strftime("%m/%d/%Y %H:%M:%S")
    logging.info(f"Attributing mainstems sites started: {display_dt_string} (UTC)")
    # Rename columns and add attribute indicating if rating curve exists
    sites_gdf.rename(
        columns={'nwm_feature_id': 'feature_id', 'usgs_site_code': 'location_id'}, inplace=True
    )
    sites_with_data = pd.DataFrame(
        {'location_id': all_rating_curves['location_id'].unique(), 'curve': 'yes'}
    )
    sites_gdf = sites_gdf.merge(sites_with_data, on='location_id', how='left')
    sites_gdf.fillna({'curve': 'no'}, inplace=True)

    # Import mainstems segments to be used in run_by_unit.sh
    ms_df = gpd.read_file(NWM_FLOWS_MS)
    ms_segs = ms_df.ID.astype(str).to_list()  # Yes.. required (sites_gdf.eval below)

    # Populate mainstems attribute field
    sites_gdf['mainstem'] = 'no'
    sites_gdf.loc[sites_gdf.eval('feature_id in @ms_segs'), 'mainstem'] = 'yes'

    # Debugging tool
    # sites_gdf.to_csv(os.path.join(output_dir, f'acceptable_sites_pre_{file_date_append}.csv'))

    sites_gdf = sites_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')
    sites_gdf = sites_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')

    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    # logging.info(f"Recasting... {dt_string} (UTC) ")

    sites_gdf = sites_gdf.astype({'metadata_sources': str})

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
    acceptable_sites_gdf.to_csv(
        os.path.join(output_dir, f'acceptable_sites_for_rating_curves_{file_date_append}.csv')
    )
    acceptable_sites_gdf.to_file(
        os.path.join(output_dir, f'acceptable_sites_for_rating_curves_{file_date_append}.gpkg'),
        driver='GPKG',
        engine='fiona',
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
        Note: All files have today's date appended.

        usgs_rating_curves_{date}.csv -- A csv containing USGS rating curve as well
        as datum adjustment and rating curve expressed as an elevation (NAVD88).
        ONLY SITES IN CONUS ARE CURRENTLY LISTED IN THIS CSV. To get
        additional sites, the Tidal API will need to be reconfigured and tested.

        log_{date}.csv -- A csv containing gage-specific messages.

        (if all option passed) usgs_gages_{date}.gpkg -- a point layer containing ALL USGS gage sites that meet
        certain criteria. In the attribute table is a 'curve' column that will indicate if a rating
        curve is provided in "usgs_rating_curves_{date}.csv"

        acceptable_sites_for_rating_curves_{date}.csv -- A csv containing all acceptable sites
        that have a rating curve.

        acceptable_sites_for_rating_curves_{date}.gpkg -- A geopackage containing all acceptable sites
        that have a rating curve.

        acceptable_sites_pre_{date}.csv -- A csv containing all acceptable sites that have a rating curve.

        usgs_stage_discharge_cms_{date}.csv -- A csv containing the flow values for each flood category
        (action, minor, moderate, major) for each site.  Used by Seirra Testing (rating_curve_comparison)

        sites_bool_flags.gpkg -- A geopackage containing all acceptable sites
        TODO: deprecated as of 5/14/25... remove? or do we use this?

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
    A number of files are saved, including...

    '''

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
        # Set global variables
        set_global_env(env_file)

    # Check if csv is supplied
    
    if list_of_gage_sites != 'all':
        print("*** You have provide a list of specific usgs site codes to process.\nPlease note that when getting all sites," \
            " it filters to only sites that are active. But when using specific codes, it will not use the 'is active' filter.\n\n" \
            "To continue, hit your enter key or CTRL-C to abort")
        input(">> ")  # if they user does want to continue, we won't check the value here, just the fact that it was not aborted
        
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

    overall_start_dt = datetime.now(timezone.utc)
    file_date_append = overall_start_dt.strftime("%Y%m%d")
    file_datetime_string = overall_start_dt.strftime("%Y%m%d-%H%M")    
    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    
    log_file_name = f"get_rating_curves-{file_datetime_string}.log"
    log_file_path = os.path.join(output_dir, log_file_name)    
    sf.setup_file_logger(log_file_path)
    # file_logger = sf.setup_mp_file_logger(log_file_path)

    try:
        logging.info("Retrieving new USGS rating curves")
        logging.info(f"Started {display_dt_string} (UTC)")
        print()
        print(f"Saving results in {output_dir}")
        print()

        # If 'all' option passed to list of gages sites, it retrieves all sites within CONUS.
        section_start_dt = datetime.now(timezone.utc)
        logging.info(f"Retrieving metadata")

        # This part usually only takes a few mins (up to 8 mins(ish) )
        sites_gdf, metadata_list = __get_usgs_metadata(list_of_gage_sites, metadata_url)

        display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Retrieving metadata and agg by HUCs complete: {display_dt_string} (UTC); {dur_msg}")

        # Create DataFrame to store all appended rating curves
        # print('Processing metadata...')
        section_start_dt = datetime.now(timezone.utc)
        display_dt_string = section_start_dt.strftime("%m/%d/%Y %H:%M:%S")
        logging.info(f"Processing metadata started: {display_dt_string} (UTC)")
        all_rating_curves = pd.DataFrame()

        # For each site in metadata_list
        # for metadata in metadata_list:
        num_sites = len(metadata_list)
        logging.info(f"Number of sites to process: {num_sites}")
        print("-- Note: some locations will be skipped")

        # TODO: This part needs MP, as is, it takes appx 21 hours
        # Use MP and not Threading. Why? We do a tons of WRDS calls and we 
        # don't want to overload the network pipe. Threading is best for high computational
        # and not when calling external or making multiple calls to the disks.
        # If we need to later, we can make a seperate log per mp and roll them up togehter
        # see FIM_logger.merge_log_files
        tasks_args_list = []
        for i in range(len(metadata_list)):
            metadata_json = metadata_list[i]
            
            # DEBUG
            # print("+++++++++++++++")
            # print(metadata_json)
            # print("+++++++++++++++")

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
               
        # setup the mp logger
        # TODO: test with exceptions and see if this still works with multiple loggers in place.
        # TODO: not sure but might be file collisons with many MP's trying to write to one file
        # at the same time. For now, we will jsut let MP have its own in case it gets hung up
        # if the MP gets hung up or throws and exception, if mp log was used script wide
        # it can hang up the entire script as it has a reference from inside the mp to this
        # parent script.
        mp_log_file_path = os.path.join(output_dir, f"get_rating_curves-mp-{file_datetime_string}.log")
        mp_logger = sf.setup_mp_file_logger(mp_log_file_path, logger_name="mp_rcs")
        list_rating_curves_dfs = sf.run_with_mp(
            task_function=__mp_get_site_rating_curve,
            tasks_args_list=sorted_tasks_args_list,
            # file_logger=fim_logger,
            file_logger=mp_logger,
            # file_logger=logging.getLogger(),
            max_workers=num_jobs,
            task_id_key='usgs_site_code',
            exit_on_failure=True,
            show_progress=True,
        )

        # Roll the mp log into the master log.
        with open(mp_log_file_path, 'r') as src_file:
            with open(log_file_path, 'a') as master_log_file:
                shutil.copyfileobj(src_file, master_log_file)
        os.remove(mp_log_file_path)
        # for now.. let's leave the error files alone, even the mp ones
        
        # more processing of rating curves
        if len(list_rating_curves_dfs) == 0:
            logging.error("There are no acceptable sites. Stopping program.")
            sys.exit(1)
                
        all_rating_curves = pd.concat(list_rating_curves_dfs)
        logging.info(f"Number of sites to processes with metadata: {len(all_rating_curves)}")

        display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Processing metadata complete: {display_dt_string} (UTC); {dur_msg}")
        logging.info("=============")

        # Error out with messages if no rating curves made it past the datum checks
        if len(all_rating_curves) == 0:
            logging.error('ERROR: No rating curves to compile. Program aborting.')
            sys.exit(1)

        # Filter out all_rating_curves by list
        acceptable_sites_list = __attrib_mainstems(sites_gdf, all_rating_curves, output_dir, file_date_append)
        all_rating_curves = all_rating_curves[all_rating_curves['location_id'].isin(acceptable_sites_list)]

        # dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        logging.info(f"Attributing mainstems sites done: {display_dt_string} (UTC)")
        logging.info("=============")

        # Write rating curve dataframe to file
        usgs_rating_curve_file = os.path.join(output_dir, f"usgs_rating_curves_{file_date_append}.csv")
        all_rating_curves.to_csv(usgs_rating_curve_file, index=False)

        # If 'all' option specified, reproject then write out shapefile of acceptable sites.
        # TODO: Should it also do something if 'all' isn't specified?
        if list_of_gage_sites == ['all']:
            sites_gdf = sites_gdf.to_crs(PREP_PROJECTION)
            usgs_gages_file = os.path.join(output_dir, f"usgs_gages_{file_date_append}.gpkg")

            sites_gdf.to_file(usgs_gages_file, layer='usgs_gages', driver='GPKG', engine='fiona')

        display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        logging.info(f"usgs gage files created: {display_dt_string} (UTC)")

        # Write out flow files for each threshold across all sites
        section_start_dt = datetime.now(timezone.utc)
        display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        logging.info(f"Getting stage discharge values - started: {display_dt_string} (UTC)")


        # TODO... ADD MP here as well

        # what do we want back here? anything?
        __write_categorical_flow_files(metadata_list, output_dir, file_date_append, file_datetime_string, log_file_path, num_jobs)
       
        

        display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f"Getting stage discharge values - complete: {display_dt_string} (UTC); {dur_msg}")

    except Exception:
        logging.critical(traceback.format_exc())

    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    dur_msg = fh.print_date_time_duration(overall_start_dt, datetime.now(timezone.utc), False)
    logging.info(f"Program complete: {display_dt_string} (UTC)")
    logging.info(dur_msg)
    print("-------------------------------------------------------------------------")

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
        /foss_fim/data/usgs/rating_curve_get_usgs_curves.py -l 'all' -o '/data/inputs/usgs_gages/'

    Download certain sites to outputs folder
        /foss_fim/data/usgs/rating_curve_get_usgs_curves.py -l '04228500 04228502' -o '/data/inputs/usgs_gages'

    '''

    # Parse arguments
    # TODO: Check whether this is still true. Update if needed.
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
    parser.add_argument('-j', "--num-jobs",
        help="OPTIONAL: Number of processes",
        type=int,
        default=1)
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
