#!/usr/bin/env python3

import argparse
import os
import urllib3
import pickle
import pandas as pd
import requests
from dotenv import load_dotenv

from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import date, datetime, timezone

from tools_shared_functions import(
    get_metadata,
    get_thresholds
)


def label_data_file(label, lst_hucs):

    # If a list of HUCs is provided, add 'subset' to the label
    subset = '' if 'all' in lst_hucs else '_subset'

    # Add a leading underscore to the label if it's not empty
    label = f'_{label}' if label != '' else label

    date_formatted = date.today().strftime("%d%m%Y")
    label_with_date = f'{label}{subset}_{date_formatted}'

    return label_with_date

def get_huc_dictionary(metadata_list, lst_hucs):

    '''
    huc_lid_dict, lid_list = get_huc_dictionary(metadata_list, lst_hucs)
    '''

    lid_list = []
    huc_lid_dict = {}

    # Iterate through the metadata to get a list of LIDs and HUCs
    for site_entry in metadata_list:
        lid_i = site_entry['identifiers']['nws_lid']
        huc_nws_i = site_entry['nws_preferred']['huc']
        huc_usgs_i = site_entry['usgs_preferred']['huc']
    
        huc_i = huc_usgs_i if huc_nws_i is None else huc_nws_i

        lid_list.append(lid_i)
        huc_lid_dict[lid_i] = huc_i

    if 'all' not in lst_hucs:
        # Filter huc_lid_dict to only include HUCs in huc_lst
        huc_lid_dict = {lid: huc for lid, huc in huc_lid_dict.items() if huc in lst_hucs}
        lid_list = list(huc_lid_dict.keys())

    print(f'Number of sites to download thresholds for: {len(lid_list)}')

    return huc_lid_dict

# ----- 

# TODO: Remove this function from the generate_categorical_fim_flows.py and replace it with code that can read it in 
def download_all_metadata(metadata_filepath, metadata_url, search):

    print('Starting metadata download from WRDS...')

    nwm_us_search, nwm_ds_search = search, search
    output_meta_list = []

    # This function currently does not use the HUC list functionality because the get_metadata function
    # does not get all of the forecast points when using HUCs. When we use nws_lid = all, we get 4810 forecast points,
    # whereas when we use huc = all, we only get 3686 forecast points.
    # So for now, we are just going to get all forecast points using nws_lid = all, and then filter them later if needed. 
    # However, we can still filter the thresholds using the HUC list if it is provided.

    # Get all forecast points
    forecast_point_meta_list, ___ = get_metadata( 

        metadata_url,
        select_by='nws_lid',
        selector=['all'],
        must_include='nws_data.rfc_forecast_point',
        upstream_trace_distance=nwm_us_search,
        downstream_trace_distance=nwm_ds_search,
    )

    # Get all sites for OCONUS regions (HI, PR, and AK)
    oconus_meta_list, ___ = get_metadata(
        metadata_url,
        select_by='state',
        selector=['HI', 'PR', 'AK'],
        must_include=None,
        upstream_trace_distance=nwm_us_search,
        downstream_trace_distance=nwm_ds_search,
    )

    # Append the lists
    unfiltered_meta_list = forecast_point_meta_list + oconus_meta_list

    # Filter the metadata list
    output_meta_list = []
    unique_lids_list, duplicate_lids_list = [], []
    duplicate_meta_list, nonelid_metadata_list = [], []

    for i, site in enumerate(unfiltered_meta_list):
        nws_lid = site['identifiers']['nws_lid']

        if nws_lid is None:
            # No LID available
            nonelid_metadata_list.append(site)

        elif nws_lid in unique_lids_list:
            # Duplicate LID
            duplicate_lids_list.append(nws_lid)
            duplicate_meta_list.append(site)

        else:
            # Unique/unseen LID that's not None
            unique_lids_list.append(nws_lid)
            output_meta_list.append(site)

    print(f'Total number of unique LIDs: {len(unique_lids_list)}')

    try:

        with open(metadata_filepath, "wb") as p_handle:
            pickle.dump(output_meta_list, p_handle, protocol=pickle.HIGHEST_PROTOCOL)

        print(f"Metadata saved at {metadata_filepath}")

        file_size_bytes = os.path.getsize(metadata_filepath)
        file_size_kb = round(file_size_bytes / 1024, 2)
        file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
        print(f"File size: {file_size_kb} kb or {file_size_mb} mb")

    except Exception as e:
        print(f"Error saving pickle file {metadata_filepath}: {e}")


# moved over from inundation-mapping/tools/tools_shared_functions.py
# TODO: Re-route all files that use this function to get it from here
def download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict):
    """
    TODO: add a note about why .pkl for thresholds.

    Download all thresholds from the WRDS API for a list of LIDs and save them as CSV files.
    Combine all CSV files into a single pickle file.

    This function can be run in a Jupyter notebook or as a standalone script in a Python environment
    to predownload thresholds for all sites in the metadata pickle file.

    Parameters:
    - thresholds_filepath: str, filepath where output files will be saved.
    - threshold_url: str, URL of the WRDS API endpoint for thresholds.
    - huc_lid_dict: dict, dictionary mapping LIDs to HUCs.
    
    Outputs:
    - Saves a combined pickle file 'all_thresholds.pkl' containing all thresholds.
    """
    thresholds_start_time = datetime.now(timezone.utc)

    print('Starting threshold download from WRDS...')

    # Iterate through LIDs in huc_lid_dict and get thresholds from the WRDS API
    list_threshold_dfs = []
    for lid, huc in huc_lid_dict.items():
        try:
            stages, flows, status = get_thresholds(
                    threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
                )
        except Exception as e:
            print(f"Error retrieving thresholds for LID {lid}: {e}")
            print(status)
            continue

        # Combine and label thresholds
        thresholds_dict = [{'threshold_type': 'stages', 'huc': huc, **stages}, 
                    {'threshold_type': 'flows', 'huc': huc, **flows}]
        
        # Format into a dataframe and add to the df list
        thresholds_df = pd.DataFrame(thresholds_dict)
        list_threshold_dfs.append(thresholds_df)

    # Combine all the DataFrames in the list into a single, final DataFrame
    all_thresholds_df = pd.concat(list_threshold_dfs, ignore_index=True)

    # Save the combined DataFrame to a pickle file
    try:
        with open(thresholds_filepath, 'wb') as f:
            pickle.dump(all_thresholds_df, f)
        print(f"Thresholds saved at {thresholds_filepath}")

        file_size_bytes = os.path.getsize(thresholds_filepath)
        file_size_kb = round(file_size_bytes / 1024, 2)
        file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
        print(f"File size: {file_size_kb} kb or {file_size_mb} mb")

    except Exception as e:
        print(f"Error saving pickle file {thresholds_filepath}: {e}")

    thresholds_end_time = datetime.now(timezone.utc)
    thresholds_duration = thresholds_end_time - thresholds_start_time
    print(f"Finished downloading thresholds - Duration: {str(thresholds_duration).split('.')[0]}")
    print()


def __load_nwm_metadata(metadata_filepath, API_BASE_URL, search, metadata_download, lst_hucs):
    '''
    Downloads or reads in the NWM metadata and then returns the data as a list and a HUC dictionary. 

    1. Loads the NWM metadata list using the method specified by metadata_download.
        If metadata_download is True: it downloads metadata from the specified URL using two API calls: one for all 
        forecast points and another for all points in OCONUS regions (HI, PR, AK). Results are combined
        and duplicate or None-valued NWS LIDs are filtered out.
        
        If metadata_download is False: the metadata is loaded from the file (function errors out if filepath isn't provided
        and download metadata is False).

    2. Uses the NWM metadata to create a huc/lid dictionary.

    Args:
        metadata_filepath (str) : Filepath where the metadata pickle is or will be stored. If it does not exist, 
                                it will be created. If it does exist and metadata_download is True, it will be overwritten.
        API_BASE_URL (str) : WRDS API URL for retrieving NWM metadata.
        search (int) : Distance for upstream and downstream metadata search.
        metadata_download (bool) : Whether metadata should be downloaded (True) or not (False)
        lst_hucs (list of string) : List of HUCs to process or a list containing the value 'all' to process all HUCs.

    Returns:
        output_meta_list (list) : Filtered list of metadata dictionaries, each representing a unique NWS LID site.
        huc_lid_dict (dict) : dictionary mapping LIDs to HUCs.
    '''

    output_meta_list = []

    if metadata_download == True:
        metadata_url = f'{API_BASE_URL}/metadata'

        # Give a warning if the file will be overwritten
        if os.path.isfile(metadata_filepath):
            print(f"WARNING: NWM metadata file already exists at {metadata_filepath} and metadata_download is set to True. It will be overwritten.")
        else:
            print(f"Meta file will be downloaded and saved at {metadata_filepath}")

        # Download metadata and save metadata to pkl file 
        metadata_start_time = datetime.now(timezone.utc)
        download_all_metadata(metadata_filepath, metadata_url, search)

        metadata_end_time = datetime.now(timezone.utc)
        metadata_duration = metadata_end_time - metadata_start_time
        print(f"Finished downloading metadata - Duration: {str(metadata_duration).split('.')[0]}")
        print()

    else:
        # Error if metafile is not there
        if not os.path.isfile(metadata_filepath):
            raise ValueError(f"NWM metadata file not found at {metadata_filepath} and metadata_download is set to False. Provide a valid NWM metafile or set metadata_download to True.")
            # this error really should only occur in command line running of this tool or CatFIM development
            # (and not regular CatFIM runs), so we can keep for now. 
        else:
            print(f"Meta file already downloaded and exists at {metadata_filepath}")

    # Open metadata file
    with open(metadata_filepath, "rb") as p_handle:
        output_meta_list = pickle.load(p_handle)
    
    # Get the HUC dictionary
    huc_lid_dict = get_huc_dictionary(output_meta_list, lst_hucs)
    
    return output_meta_list, huc_lid_dict

# TODO: THIS SHOULD BE MOVED TO A CATFIM-SPECIFIC SCRIPT 
def __load_site_thresholds(threshold_file, lid):
    '''
    Loads threshold stage and flow data for a given site (LID) from a local pickle file.

    Parameters
    ----------
        threshold_file (str): Path to the local pickle file containing threshold data.
        lid (str): NWS Location Identifier (LID) for the site.

    Returns:
    ----------
        stages (dict or None): Dictionary of stage thresholds for the site, or None if not found.
        flows (dict or None): Dictionary of flow thresholds for the site, or None if not found.
    '''

    if os.path.isfile(threshold_file) == True:
        # Read pickle file and get the stages and flows dictionary for the site
        with open(threshold_file, 'rb') as f:
            loaded_data = pickle.load(f)
            site_data = loaded_data[loaded_data['nws_lid'] == lid.upper()] # TODO: Check whether we need an upper or lower case conversion

        # Error if site_data is empty
        if site_data.empty:
            FLOG.error(f"No threshold data found for LID {lid} in the provided threshold file.")
            return None, None, 0

        # Make output dictionaries for stages and flows
        # Assuming there's only one record per threshold_type per lid
        stages = site_data.loc[site_data['threshold_type'] == 'stages'].to_dict(orient='records')[0]
        del stages['threshold_type']
        del stages['huc']

        flows = site_data.loc[site_data['threshold_type'] == 'flows'].to_dict(orient='records')[0]
        del flows['threshold_type']
        del flows['huc']

        # # Print out the stages and flows for debugging  ## TEMP DEBUG
        # FLOG.lprint(f"Stages for LID {lid}: {stages}")  ## TEMP DEBUG
        # FLOG.lprint(f"Flows for LID {lid}: {flows}")  ## TEMP DEBUG

        FLOG.lprint('Thresholds loaded from .pkl file.')

    return stages, flows


def main(env_file, workspace, label, lst_hucs, search, metadata_download, threshold_download, input_metadata_file):

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    print('================================')
    print('Starting processing to obtain WRDS data')
    print(f'{dt_string} (UTC)')
    print()

    # Validate workspace
    if not os.path.exists(workspace):
        raise ValueError(f'Workspace path {workspace} does not exist. Please provide a valid path.')

    # Validate inputs
    if metadata_download == False and threshold_download == False:
        raise ValueError('At least one of -m (get metadata) or -t (get thresholds) must be specified as True.')
    elif metadata_download == True and threshold_download == True:
        print('Both metadata and thresholds will be downloaded and saved.')
    elif metadata_download == True and threshold_download == False:
        print('Only metadata will be saved.')
    elif threshold_download == True and metadata_download == False:
        print('Only threshold data will be saved, valid metadata pkl file must be provided.')
    
    lst_hucs = lst_hucs.split()

    if 'all' in lst_hucs:
        print('No HUC list provided, downloading data for all HUCs.')
    else:
        print(f'Downloading data for {len(lst_hucs)} HUCs.')
    print()

    # Set up API URLs
    load_dotenv(env_file)
    API_BASE_URL = os.getenv('API_BASE_URL')
    if API_BASE_URL is None:
        raise ValueError(
            'API base url not found. Ensure inundation_mapping/tools/ has an .env file with the API_BASE_URL.'
        )

    # If no metafile is provided, generate filepath and filename
    if input_metadata_file == '':
        label_with_date = label_data_file(label, lst_hucs)
        output_metadata_filename = f'metadata{label_with_date}.pkl'
        metadata_filepath = os.path.join(workspace, output_metadata_filename)
    
    # If metadata filepath is provided, use it
    else:
        metadata_filepath = input_metadata_file

    ## ===== START SECTION OF CODE TO COPY INTO CATFIM PREPROCESSING =====

    # Load NWM metadata (either by downloading it or pulling it from WRDS)
    # Note: This is the function that we will put into CatFIM code
    ___, huc_lid_dict= __load_nwm_metadata(metadata_filepath, API_BASE_URL, search, metadata_download, lst_hucs)

    # Load thresholds if specified
    if threshold_download == True:
        threshold_url = f'{API_BASE_URL}/nws_threshold'

        label_with_date = label_data_file(label, lst_hucs)
        output_thresholds_filename = f'thresholds{label_with_date}.pkl'
        thresholds_filepath = os.path.join(workspace, output_thresholds_filename)

        # Download thresholds
        download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict)

    ## ===== END SECTION OF CODE TO COPY INTO CATFIM PREPROCESSING =====

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    time_duration = overall_end_time - overall_start_time

    print('Processing complete.')
    print(f"Total duration: {str(time_duration).split('.')[0]}") 
    print('================================')

    # TODO: bolt in Ali's new logging system


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='PLACEHOLDER.') # TODO: Add description

    parser.add_argument(
        '-e',
        '--env-file',
        help='OPTIONAL: Docker mount path to the environment file.'
        'default = /data/config/fim_enviro_values.env',
        required=False,
        default='/data/config/fim_enviro_values.env',
    )

    parser.add_argument('-w',
        '--workspace',
        help='OPTIONAL: Workspace where all outputs will be saved.',
        required=False,
        default = '/data/inputs/wrds/'
    )

    parser.add_argument('-l',
        '--label',
        help='OPTIONAL: Label for filenames. Stucture will be metadata_<label>_ddmmyy.pkl and thresholds_<label>_ddmmyy.pkl).',
        required=False
    )

    parser.add_argument(
        '-lh',
        '--lst-hucs',
        help="OPTIONAL: Space-delimited list of HUCs to get WRDS data for. Defaults to all HUCs. e.g. '12090301 19020301'",
        required=False,
        default='all',
    )

    parser.add_argument(
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. Defaults to 5.',
        required=False,
        default='5',
    )

    parser.add_argument(
        '-m',
        '--metadata-download',
        help="OPTIONAL: Create the metadata.pkl file. Must have at least one of -m or -t selected.",
        required=False,
        default=False,
        action='store_true'
    )

    parser.add_argument(
        '-t',
        '--threshold-download',
        help="OPTIONAL: Create the thresholds.pkl file. Must have at least one of -m or -t selected.",
        required=False,
        default=False,
        action='store_true'
        )

    parser.add_argument(
        '-mf',
        '--input-metadata-file',
        help="OPTIONAL: Input metadata file to use for pulling thresholds. Will error if -m flag is also used.",
        required=False,
        default='',
    )

    args = vars(parser.parse_args())

    # Main function call
    main(**args)
