#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
# from multiprocessing import Pool
from pathlib import Path
import traceback

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from tools_shared_functions import (
    aggregate_wbd_hucs,
    flow_data,
    get_datum,
    get_metadata,
    get_rating_curve,
    get_thresholds,
    ngvd_to_navd_ft,
)
from tools_shared_variables import (acceptable_site_type_list,)

from utils.shared_variables import PREP_PROJECTION
from utils.shared_functions import FIM_Helpers as fh

gpd.options.io_engine = "pyogrio"


'''
This script calls the NOAA Tidal API for datum conversions. Experience shows that
    running script outside of business hours seems to be most consistent way
    to avoid API errors. Currently configured to get rating curve data within
    CONUS. Tidal API call may need to be modified to get datum conversions for
    AK, HI, PR/VI.
'''


def get_all_active_usgs_sites():
    '''
    Compile a list of all active usgs gage sites.
    Return a GeoDataFrame of all sites.

    Returns
    -------
    None.

    '''
    # Get metadata for all usgs_site_codes that are active in the U.S.
    metadata_url = f'{API_BASE_URL}/metadata'
    # Define arguments to retrieve metadata and then get metadata from WRDS
    select_by = 'usgs_site_code'
    selector = ['all']
    must_include = 'usgs_data.active'
    metadata_list, metadata_df = get_metadata(
        metadata_url,
        select_by,
        selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=None,
    )
    # Get a geospatial layer (gdf) for all acceptable sites
    print("Aggregating WBD HUCs...")
    dictionary, gdf = aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)

    # # Get a list of all sites in gdf
    # list_of_sites = gdf['identifiers_usgs_site_code'].to_list() # TODO: Removed because unused... fully remove after testing.

    # Rename gdf fields
    gdf.columns = gdf.columns.str.replace('identifiers_', '')

    return gdf, metadata_list


##############################################################################
# Generate categorical flows for each category across all sites.
##############################################################################
def write_categorical_flow_files(metadata, output_dir, file_date_append):
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

    # For each site in metadata
    all_data = pd.DataFrame()

    for site in metadata:
        # Get the feature_id and usgs_site_code
        feature_id = site.get('identifiers').get('nwm_feature_id')
        usgs_code = site.get('identifiers').get('usgs_site_code')
        nws_lid = site.get('identifiers').get('nws_lid')

        # thresholds only provided for valid nws_lid.
        if nws_lid == 'Bogus_ID' or nws_lid is None:
            continue

        # if invalid feature_id skip to next site
        if feature_id is None:
            continue

        # Get the stages and flows
        stages, flows = get_thresholds(threshold_url, select_by='nws_lid', selector=nws_lid, threshold='all')

        # For each flood category
        for category in ['action', 'minor', 'moderate', 'major']:
            # Get flow
            flow = flows.get(category, None)

            # If flow or feature id are not valid, skip to next site
            if flow is None:
                continue

            # Otherwise, write 'guts' of a flow file and append to a master DataFrame.
            else:
                data = flow_data([feature_id], flow, convert_to_cms=True)
                data['recurr_interval'] = category
                data['nws_lid'] = nws_lid
                data['location_id'] = usgs_code
                data = data.rename(columns={'discharge': 'discharge_cms'})
                # Append site data to master DataFrame
                all_data = pd.concat([all_data, data], ignore_index=True)

    # Write usgs stage discharge data, used by Sierra tests (rating_curve_comparison.py)
    print("Writing for USGS discharge data for each usgs stage (ie. action, minor, etc)")
    if not all_data.empty:
        usgs_discharge_file_name = os.path.join(output_dir, f'usgs_stage_discharge_cms_{file_date_append}.csv')
        final_data = all_data[['feature_id', 'discharge_cms', 'recurr_interval']]
        final_data.to_csv(usgs_discharge_file_name, index=False)

    return all_data

def set_global_env(env_file):
    global API_BASE_URL, WBD_LAYER, NWM_FLOWS_MS 
    load_dotenv(env_file)

    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    NWM_FLOWS_MS = os.getenv("NWM_FLOWS_MS")

def usgs_rating_to_elev(list_of_gage_sites, env_file, output_dir=False, sleep_time=1.0):
    '''

    Returns rating curves, for a set of sites, adjusted to elevation NAVD.
    Currently configured to get rating curve data within CONUS. Tidal API
    call may need to be modified to get datum conversions for AK, HI, PR/VI.
    Workflow as follows:
        1a. If 'all' option passed, get metadata for all acceptable USGS sites.
        1b. If a list of sites passed, get metadata for all sites supplied by user.
        2.  Extract datum information for each site.
        3.  If site is in CONUS or AK: convert datum if NGVD88. If site is in PR, VI, or HI, keep datum as LMSL.
        4.  Get rating curve for each site individually
        5.  Convert rating curve to absolute elevation (NAVD) and store in DataFrame
        6.  Append all rating curves to a master DataFrame.


    Outputs, if an output_dir is specified, are:
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
        Directory, if specified, where output csv is saved. OPTIONAL, Default is False.

    sleep_time: FLOAT
        Amount of time to rest between API calls. The Tidal API appears to
        error out more during business hours. Increasing sleep_time may help.

    Returns
    -------
    all_rating_curves : Pandas DataFrame
        DataFrame containing USGS rating curves adjusted to elevation for
        all input sites. Additional metadata also contained in DataFrame

    '''

    # Import variables from .env file
    if not os.path.exists(env_file):
        print(f"ERROR: Environment file does not exist: {env_file}")
        sys.exit()
    else:
        print(f'Loading environment file: {env_file}')
        # Set global variables
        set_global_env(env_file)

    # Define URLs for metadata and rating curve
    metadata_url = f'{API_BASE_URL}/metadata'
    rating_curve_url = f'{API_BASE_URL}/rating_curve'

    # Create output_dir directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    overall_start_dt = datetime.now(timezone.utc)
    file_date_append = overall_start_dt.strftime("%Y%m%d")
        
    dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
   
    __setup_logger(output_dir)
    
    try:
        logging.info("Retrieving new USGS rating curves")
        logging.info(f"Started {dt_string} (UTC)")
        print()    
        print(f"Saving results in {output_dir}")    
        print()

        # If 'all' option passed to list of gages sites, it retrieves all sites within CONUS.
        start_dt = datetime.now(timezone.utc)
        logging.info(f"Getting metadata: {dt_string} (UTC)")
        if list_of_gage_sites == ['all']:
            logging.info('Getting metadata for all sites')
            sites_gdf, metadata_list = get_all_active_usgs_sites()

        # Otherwise, if a list of sites is passed, retrieve sites from WRDS.
        else:
            
            # TODO: Jun 2, 2025: if you send in more than one site code, it fails
            # It attempts to call WRDS URL with more than one code instead of calling
            # for each code, then concatenation them.
            # Error: Message: Bad Request
            # api/location/v3.0/metadata/usgs_site_code/04228500%2C04228502/ (notice.. I tried for two codes)
            # We can fix this on a future release.
            
            # Define arguments to retrieve metadata and then get metadata from WRDS
            select_by = 'usgs_site_code'
            selector = list_of_gage_sites
            logging.info(f"Selected sites : {selector}")

            # Since there is a limit to number characters in url, split up selector if too many sites.
            max_sites = 150
            if len(selector) > max_sites:
                chunks = [selector[i : i + max_sites] for i in range(0, len(selector), max_sites)]
                # Get metadata for each chunk
                metadata_list = []
                metadata_df = pd.DataFrame()
                for chunk in chunks:
                    chunk_list, chunk_df = get_metadata(
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
                metadata_list, metadata_df = get_metadata(
                    metadata_url,
                    select_by,
                    selector,
                    must_include=None,
                    upstream_trace_distance=None,
                    downstream_trace_distance=None,
                )

            # Get a geospatial layer (gdf) for all acceptable sites
            logging.info("Aggregating WBD HUCs...")
            _, sites_gdf = aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=True)
            if not sites_gdf.empty:
                # Get a list of all sites in gdf
                list_of_sites = sites_gdf['identifiers_usgs_site_code'].to_list() # TODO: Do we need this?
                # Rename gdf fields
                sites_gdf.columns = sites_gdf.columns.str.replace('identifiers_', '')
            else:
                logging.info("There are no acceptable sites.")
                sys.exit()

        dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        dur_msg = fh.print_date_time_duration(start_dt, datetime.now(timezone.utc), False)    
        logging.info(f"Getting metadata done: {dt_string} (UTC)")
        logging.info(dur_msg)
        logging.info("=============")

        # Create DataFrame to store all appended rating curves
        # print('Processing metadata...')
        start_dt = datetime.now(timezone.utc)
        dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")    
        logging.info(f"Processing metadata started: {dt_string} (UTC)")
        all_rating_curves = pd.DataFrame()

        # For each site in metadata_list
        # for metadata in metadata_list:
        for i in range(len(metadata_list)):
            metadata = metadata_list[i]

            # Print progress every 50 sites
            if i % 50 == 0:
                logging.info(f"Processing site {i}/{len(metadata_list)}, {round((i/len(metadata_list))*100, 2)}%")

            # Get datum information for site (only need usgs_data)
            ___, usgs = get_datum(metadata)

            # # Filter out sites that are not in contiguous US. If this section is removed be sure to test with
            # #   datum adjustment section (region will need changed)
            # if usgs['state'] in ['Puerto Rico', 'Virgin Islands', 'Hawaii']: # Removed May 2025
            #     continue

            # Get rating curve for site
            location_ids = usgs['usgs_site_code']
            if location_ids is None:  # Some sites don't have a value for usgs_site_code, skip them
                continue

            curve = get_rating_curve(rating_curve_url, location_ids=[location_ids])

            # If no rating curve was returned, skip site.
            if curve.empty:
                logging.info(f'{location_ids}: Removed because it has no rating curve')
                continue

            # If the site is in PR, VI, or HI, keep datum in LMSL (local mean sea level) 
            # because our 3DEP dems are also in LMSL for these areas. 
            if usgs['state'] in ['Puerto Rico', 'Virgin Islands', 'Hawaii']:
                if usgs['vcs'] == 'LMSL':
                    navd88_datum = usgs['datum']
                    logging.info(f'{location_ids}: site is in PR, VI, or HI, so datum kept as LMSL')
                else:
                    # If the site is in PR, VI, or HI, and has a datum other than LMSL, return an error. 
                    datum_name = usgs['vcs']
                    message = f'{location_ids}: Removed because site is located PR,'
                    f'VI, or HI but has a datum other than LMSL ({datum_name})'
                    logging.info(message)
                    continue

            # If the state is not PR, VI, or HI, then we want to adjust the datum to NAVD88 if needed.
            # If the datum is unknown, skip site.
            else:
                if usgs['vcs'] == 'NGVD29':

                    # To prevent time-out errors
                    time.sleep(sleep_time)

                    # Get the datum adjustment to convert NGVD to NAVD. Region needs changed if not in CONUS.
                    datum_adj_ft = ngvd_to_navd_ft(datum_info=usgs, region='contiguous')

                    # If datum API failed, print message and skip site.
                    if datum_adj_ft is None:
                        logging.info(f'ERROR: {location_ids}: Removed because datum adjustment failed!!')
                        continue

                    # If datum adjustment succeeded, calculate datum in NAVD88
                    navd88_datum = round(usgs['datum'] + datum_adj_ft, 2)
                    logging.info(f'{location_ids}: succesfully converted NGVD29 to NAVD88')

                elif usgs['vcs'] == 'NAVD88':
                    navd88_datum = usgs['datum']
                    logging.info(f'{location_ids}: already NAVD88')

                elif usgs['vcs'] == 'LMSL':
                    # If the site has a vdatum of LMSL and is not in PR, VI or HI, skip site. 
                    logging.info(f'{location_ids}: Removed because LMSL datum found outside of PR, VI, or HI')
                    continue

                else:
                    # If the site has an unrecognized datum, skip site. 
                    datum_name = usgs['vcs']
                    logging.info(f'{location_ids}: Removed due to unknown datum ({datum_name})')
                    continue

            # Populate rating curve with metadata and use navd88 datum to convert stage to elevation.
            curve['active'] = usgs['active']
            curve['datum'] = usgs['datum']
            curve['datum_vcs'] = usgs['vcs']
            curve['navd88_datum'] = navd88_datum
            curve['elevation_navd88'] = curve['stage'] + navd88_datum

            # Append all rating curves to a dataframe
            all_rating_curves = pd.concat([all_rating_curves, curve])

        end_time = datetime.now(timezone.utc)
        dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")        
        dur_msg = fh.print_date_time_duration(start_dt, end_time, False)   
        logging.info(f"Processing metadata done: {dt_string} (UTC)")
        logging.info(dur_msg)
        logging.info("=============")
        
        # Error out with messages if no rating curves made it past the datum checks
        if len(all_rating_curves) == 0:
            logging.info('ERROR: No rating curves to compile.')        
            sys.exit()

        # Add mainstems attribute to acceptable sites
        start_dt = datetime.now(timezone.utc)
        dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")    
        logging.info(f"Attributing mainstems sites started: {dt_string} (UTC)")
        
        # Rename columns and add attribute indicating if rating curve exists
        sites_gdf.rename(columns={'nwm_feature_id': 'feature_id', 'usgs_site_code': 'location_id'}, inplace=True)
        sites_with_data = pd.DataFrame({'location_id': all_rating_curves['location_id'].unique(), 'curve': 'yes'})
        sites_gdf = sites_gdf.merge(sites_with_data, on='location_id', how='left')
        sites_gdf.fillna({'curve': 'no'}, inplace=True)

        # Import mainstems segments to be used in run_by_unit.sh
        ms_df = gpd.read_file(NWM_FLOWS_MS)
        ms_segs = ms_df.ID.astype(str).to_list()

        # Populate mainstems attribute field
        sites_gdf['mainstem'] = 'no'
        sites_gdf.loc[sites_gdf.eval('feature_id in @ms_segs'), 'mainstem'] = 'yes'
        
        # Debugging tool
        # sites_gdf.to_csv(os.path.join(output_dir, f'acceptable_sites_pre_{file_date_append}.csv'))

        sites_gdf = sites_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')
        sites_gdf = sites_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')

        dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        logging.info(f"Recasting... {dt_string} (UTC) ")
        
        sites_gdf = sites_gdf.astype({'metadata_sources': str})

        # TODO: Figure out if we have a use for sites_bool_flags.gpkg and add this back in if needed.
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
        
        dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")    
        logging.info(f"Saving acceptable rating curve files... {dt_string} (UTC) ")
        acceptable_sites_gdf.to_csv(
            os.path.join(output_dir, f'acceptable_sites_for_rating_curves_{file_date_append}.csv'))
        acceptable_sites_gdf.to_file(
            os.path.join(output_dir, f'acceptable_sites_for_rating_curves_{file_date_append}.gpkg'), driver='GPKG', engine='fiona'
        )

        # Make list of acceptable sites
        acceptable_sites_list = acceptable_sites_gdf['location_id'].tolist()

        # Filter out all_rating_curves by list
        all_rating_curves = all_rating_curves[all_rating_curves['location_id'].isin(acceptable_sites_list)]

        dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        dur_msg = fh.print_date_time_duration(start_dt, datetime.now(timezone.utc), False)    
        logging.info(f"Attributing mainstems sites done: {dt_string} (UTC)")
        logging.info(dur_msg)
        logging.info("=============")

        # If output_dir is specified, write data to file.
        if output_dir:
            # Write rating curve dataframe to file
            usgs_rating_curve_file = os.path.join(output_dir, f"usgs_rating_curves_{file_date_append}.csv")
            all_rating_curves.to_csv(usgs_rating_curve_file, index=False)
            
            # If 'all' option specified, reproject then write out shapefile of acceptable sites. 
            # TODO: Should it also do something if 'all' isn't specified?
            if list_of_gage_sites == ['all']:
                sites_gdf = sites_gdf.to_crs(PREP_PROJECTION)
                usgs_gages_file = os.path.join(output_dir, f"usgs_gages_{file_date_append}.gpkg")
            
                sites_gdf.to_file(usgs_gages_file, layer='usgs_gages', driver='GPKG', engine='fiona')

            dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
            logging.info(f"usgs guage files created: {dt_string} (UTC)")
            
            # Write out flow files for each threshold across all sites
            start_dt = datetime.now(timezone.utc)
            dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")    
            logging.info(f"Creating stage discharge values started: {dt_string} (UTC)")
            
            write_categorical_flow_files(metadata_list, output_dir, file_date_append)
            
            dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
            dur_msg = fh.print_date_time_duration(start_dt, datetime.now(timezone.utc), False)    
            logging.info(f"Creating stage discharge values done: {dt_string} (UTC)")
            logging.info(dur_msg)
            logging.info("=============")

        else:  
            logging.info("No output_dir specified, no output files written.")
    except Exception:
        logging.critical(traceback.format_exc())

    dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    dur_msg = fh.print_date_time_duration(overall_start_dt, datetime.now(timezone.utc), False)    
    logging.info(f"Program complete: {dt_string} (UTC)")
    logging.info(dur_msg)
    print("-------------------------------------------------------------------------")

    return all_rating_curves


def __setup_logger(output_folder_path):
    '''
    Prints to log file and screen at the same time.
    
    Note: This does not work well for MP if it is trying to write to a shared log file
    Best to let each MP create its own log, then merge at the end of each cycle
    '''
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"get_rating_curves-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create a formatter to define the log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Create a file handler to write logs to a file
    file_handler = logging.FileHandler(log_file_path)
    # You can set the desired log level for file output, but can be a different level to
    # Whatever level you set is that level PLUS all higher levels.
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Create a stream handler to print logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Now you can log messages with different levels
    # logger.debug('This is a debug message')
    # logger.info('This is an info message')
    # logger.warning('This is a warning message')
    # logger.error('This is an error message')


if __name__ == '__main__':
    '''
    Retrieve USGS rating curves adjusted to elevation (NAVD88). 
    Currently configured to get rating curves within CONUS. # TODO: Check whether this is still true. Update if needed.
    Recommend running outside of business hours to reduce API related errors. 
    If error occurs try increasing sleep time (from default of 1).

    Arguments: 
    -l, --list_of_gage_sites: REQUIRED. Gage sites to process. Can be a space-delineated list of 
                                gage sites, a CSV (one site per line), or use “all” to get all USGS 
                                gage sites. Use numerical USGS site codes not NWS LIDS.
    -o, --output_dir:          OPTIONAL. Directory to save outputs.
    -t, --sleep_timer:        OPTIONAL. Length of time to rest between datum API calls. Defaults to 1.

    Example usage:

    Download all sites to outputs folder
        /foss_fim/data/usgs/rating_curve_get_usgs_curves.py -l 'all' -w '/outputs' 

    Download certain sites to outputs folder
        /foss_fim/data/usgs/rating_curve_get_usgs_curves.py -l '04228500 04228502' -w '/outputs' 

    '''

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Retrieve USGS rating curves adjusted to elevation (NAVD88).\n'
        'Currently configured to get rating curves within CONUS.\n' # TODO: Check whether this is still true. Update if needed.
        'Recommend running outside of business hours to reduce API related errors.\n'
        'If error occurs try increasing sleep time (from default of 1).'
    )
    parser.add_argument(
        '-l',
        '--list-of-gage-sites',
        help='REQUIRED: "all" for all active usgs sites, specify individual sites separated by space, '
        'or provide a csv of sites (one per line).',
        required=True,
    )
    parser.add_argument(
        '-o', '--output-dir', help='Directory where all outputs will be stored.',
        default=False,
        required=False
    )
    parser.add_argument(
        '-t', '--sleep-timer', help='How long to rest between datum API calls', 
        default=1.0,
        required=False
    )
    parser.add_argument(
    '-e',
    '--env-file',
    help='OPTIONAL: Docker mount path to the environment file. ie) data/config/fim_enviro_values.env',
    required=False,
    default= '/data/config/fim_enviro_values.env'
    )
    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # Check if csv is supplied
    if args['list_of_gage_sites'].endswith('.csv'):
        # Convert csv list to python list
        with open(args['list_of_gage_sites']) as f:
            sites = f.read().splitlines()
        args['list_of_gage_sites'] = sites
        list_of_gage_sites = args['list_of_gage_sites']

    else:
        list_of_gage_sites = args['list_of_gage_sites'].split(' ')

    output_dir = args['output_dir']
    sleep_timer = float(args['sleep_timer'])
    env_file = args['env_file']

    # Generate USGS rating curves

    usgs_rating_to_elev(list_of_gage_sites, env_file,
                        output_dir=output_dir, sleep_time=sleep_timer)
    
