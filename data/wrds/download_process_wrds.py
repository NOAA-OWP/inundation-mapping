#!/usr/bin/env python3
import argparse
import json
import os
import pickle
import sys
from datetime import date, datetime, timezone

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from tools_shared_functions import get_metadata, get_thresholds
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry



# TODO: We have both prints and msgs added to the messages list. We need to rethink this
# as catfim needs to iterate all messages coming back and log them. With prints
# and messages, they screen shows everythign twice.
# Check all functions for this problem. And how do we handle this for runnign this tool
# at command line versus CatFIM. Maybe an arg for show pritns or create messages?



def label_data_file(label, lst_hucs):
    '''
    Generate a filename-style label that optionally indicates a subset and appends the current date.

    Parameters
    ----------
    label : str
        Base label to include in the output. If non-empty, a leading underscore is prepended (e.g. "foo" -> "_foo").
    lst_hucs : Sequence[str]
        Sequence of HUC identifiers. If the sequence contains the string 'all' (membership tested with `in`),
        no subset marker is added; otherwise '_subset' is appended.

    Returns
    -------
    str
        Composed label in the form:
            "{label}{subset}_{date}"
        where:
          - `label` is either the empty string or the input label prefixed with an underscore,
          - `subset` is either '' (when 'all' in lst_hucs) or '_subset',
          - `date` is the current date formatted as YYYYMMDD.

    Examples
    --------
    label_data_file("survey", ["HUC1"])  -> "_survey_subset_20251206"
    label_data_file("", ["all"])        -> "_20251206"

    
    Rob: maybe a few more notes what this is function is doing. :)
    Maybe some output examples?
    
    '''

    # If a list of HUCs is provided, add 'subset' to the label
    subset = '' if 'all' in lst_hucs else '_subset'

    # Add a leading underscore to the label if it's not empty
    label = f'_{label}' if label != '' else label

    date_formatted = date.today().strftime("%Y%m%d")
    label_with_date = f'{label}{subset}_{date_formatted}'

    return label_with_date


def get_huc_dictionary(metadata_list, lst_hucs):
    '''
    Example usage:
    huc_lid_dict, lid_list = get_huc_dictionary(metadata_list, lst_hucs)

    returns: a dictionary [('00BRD', '18060005'), ('AANG1', '03130001'), ...]
       - sorted by upper case site ids.
       - May contain test or invalid sites at this point. Calling code can sort that out.
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

    # Todo: Might not be used for thresholds, maybe just getting a Huc list?
    
    print(f'Number of sites to download thresholds for: {len(lid_list)}')

    return huc_lid_dict


# -----


def download_all_metadata(metadata_filepath, metadata_url, search):
    '''
    Example usage:
    messages = download_all_metadata(metadata_filepath, metadata_url, search)
    '''

    print('Starting metadata download from WRDS...')

    nwm_us_search, nwm_ds_search = search, search
    output_meta_list = []
    messages = []

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

    msg = f'Total number of unique LIDs: {len(unique_lids_list)}'
    messages.append(msg)
    print(msg)

    try:
        with open(metadata_filepath, "wb") as p_handle:
            pickle.dump(output_meta_list, p_handle, protocol=pickle.HIGHEST_PROTOCOL)

        msg = f"New metadata file saved at {metadata_filepath}"
        messages.append(msg)
        print(msg)

    except Exception as e:
        msg = f"Error saving meta data pickle file {metadata_filepath}: {e}"
        messages.append(msg)
        print(msg)
        raise (e)

    return messages


def download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict):
    '''
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

    Note: The output is saved as a pickle file instead of CSV becuase that is the file type we
    chose for saving the metadata. The metadata is a list of dictionaries, which is not easily
    saved as a CSV file. To keep the file types consistent, we are saving the thresholds
    as a pickle file as well.

    Example usage:
        messages = download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict)

    '''
    messages = []
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
            msg = f"Error retrieving thresholds for LID {lid}: {e}"
            # TODO: Could change phrasing (to remove 'Error')... or just have CatFIM handle by not
            # throwing this as a critical error
            messages.append(msg)
            print(msg)

            messages.append(status)
            print(status)
            continue

        # Combine and label thresholds
        thresholds_dict = [
            {'threshold_type': 'stages', 'huc': huc, **stages},
            {'threshold_type': 'flows', 'huc': huc, **flows},
        ]

        # Format into a dataframe and add to the df list
        thresholds_df = pd.DataFrame(thresholds_dict)
        list_threshold_dfs.append(thresholds_df)

    # Combine all the DataFrames in the list into a single, final DataFrame
    all_thresholds_df = pd.concat(list_threshold_dfs, ignore_index=True)

    # Save the combined DataFrame to a pickle file
    try:
        with open(thresholds_filepath, 'wb') as f:
            pickle.dump(all_thresholds_df, f)

        msg = f"Thresholds file saved at {thresholds_filepath}"
        messages.append(msg)
        print(msg)

    except Exception as e:
        msg = f"Error saving pickle file {thresholds_filepath}: {e}"
        messages.append(msg)
        print(msg)
        raise (e)

    thresholds_end_time = datetime.now(timezone.utc)
    thresholds_duration = thresholds_end_time - thresholds_start_time
    print(f"Finished downloading thresholds - Duration: {str(thresholds_duration).split('.')[0]}")
    print()

    return messages


def load_nwm_metadata(metadata_filepath, API_BASE_URL, search, metadata_download, lst_hucs):
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
        - output_meta_list (list) : Filtered list of metadata dictionaries, each representing a unique NWS LID site.
        - huc_lid_dict (dict) : dictionary mapping LIDs to HUCs.
            ie) a dictionary [('00BRD', '18060005'), ('AANG1', '03130001'), ...]
                - Sorted by upper case site ids.
                - May contain test or invalid sites at this point. Calling code can sort that out.
        - messages (list of string) : Logging (because print statements won't show up in CatFIM.)

    Example usage:

    output_meta_list, huc_lid_dict, messages = load_nwm_metadata(
        metadata_filepath, API_BASE_URL, search, metadata_download, lst_hucs
    )
    
    NOTES:
       - If this function finds warning data, it will include the phrase (case-senstive) "WARNING"
         and same is true for "ERROR". Error means that the calling script can decide if it wants to shut down
         log it, continue, etc.
         It is also possible that you might get multiple messages returned in the "messages" list and some
         may be warnings, others just messages. Could be a mix and match returned.
         If something catestrophic happens, this function will thrown an exception.
    
    '''

    # TODO: We have both prints and msgs added to the messages list. We need to rethink this
    # as catfim needs to iterate all messages coming back and log them. With prints
    # and messages, they screen shows everythign twice.
    # Check all functions for this problem. And how do we handle this for runnign this tool
    # at command line versus CatFIM. Maybe an arg for show pritns or create messages?

    output_meta_list = []
    messages = []
    huc_lid_dict = {}

    if metadata_download == True:
        metadata_url = f'{API_BASE_URL}/metadata'

        # Give a warning if the file will be overwritten
        if os.path.isfile(metadata_filepath):
            msg = f"WARNING: NWM metadata file already exists at {metadata_filepath}, but metadata_download"
            " is set to True. File will be overwritten."
            messages.append(msg)
            print(msg)
        else:
            msg = f"NWM metadata file does not exist at {metadata_filepath}, metadata will be downloaded."
            messages.append(msg)
            print(msg)

        # Download metadata and save metadata to pkl file
        metadata_start_time = datetime.now(timezone.utc)
        messages_me = download_all_metadata(metadata_filepath, metadata_url, search)
        messages = messages + messages_me

        metadata_end_time = datetime.now(timezone.utc)
        metadata_duration = metadata_end_time - metadata_start_time
        print(f"Finished downloading metadata - Duration: {str(metadata_duration).split('.')[0]}")
        print()

    else:
        msg = f"Loading NWM metadata from {metadata_filepath}."
        messages.append(msg)
        print(msg)

    # Check metadata file exists and error if metafile is not there
    if not os.path.isfile(metadata_filepath):
        msg = f"NWM metadata file not found at {metadata_filepath}."
        messages.append(msg)
        print(msg)

        msg = "ERROR: Cannot proceed without NWM metadata."
        messages.append(msg)
        print(msg)
        
        return (
            output_meta_list,
            huc_lid_dict,
            messages,
        )  # HUC lid dict will be empty, so we can use that to indicate an error
        # TODO: handle error in CatFIM if empty huc lid dict is returned? or raise exception here?

    # Open metadata file (either the one we just downloaded or pre-existing)
    with open(metadata_filepath, "rb") as p_handle:
        output_meta_list = pickle.load(p_handle)
        # print(f"NWM metadata file loaded from {metadata_filepath}.")  # TEMP DEBUG


    # filter by the incoming lst_hucs, which may be one or all

    # Filter for dictionaries where 'name' is 'Alice'
    # filtered_list_by_name = [item for item in data_list if item.get('name') == 'Alice']
    # print(filtered_list_by_name)

    # filtered_meta_list = [item for item in output_meta_list if item.get('HUC8') in lst_hucs]
        # Find lid metadata from master list of metadata dictionaries.
    # filtered_meta_list = next(
    #     (item for item in output_meta_list if item['identifiers']['HUC8'] in lst_hucs), False
    # )

    huc_lid_dict = {}
    filtered_meta_list = []
    for site_entry in output_meta_list:
        lid_i = site_entry['identifiers']['nws_lid']
        huc_nws_i = site_entry['nws_preferred']['huc']
        huc_usgs_i = site_entry['usgs_preferred']['huc']

        huc_i = huc_usgs_i if huc_nws_i is None else huc_nws_i

        if 'all' in lst_hucs or huc_i in lst_hucs:
            huc_lid_dict[lid_i] = huc_i
            filtered_meta_list.append(site_entry)

    # Get the HUC dictionary
    # ROB: Do we need to make sure the output_meta_list is not empty? Woudl it ever be?
    # huc_lid_dict = get_huc_dictionary(filtered_meta_list, lst_hucs)

    # Check if huc_lid_dict is empty and log message (unlikely but possible)
    if not huc_lid_dict:
        if "all" not in lst_hucs:
            msg = "WARNING: No valid HUC/LID pairs found in the metadata for the specified HUC list."
        else:
            msg = "WARNING: No valid HUC/LID pairs found in the metadata."
        messages.append(msg)
        print(msg)

    print()

    return filtered_meta_list, huc_lid_dict, messages

# TODO: THIS SHOULD BE MOVED TO A CATFIM-SPECIFIC SCRIPT 
# Rob: maybe not.. TBD...
def load_site_thresholds(threshold_file, lid):
    '''
    Loads threshold stage and flow data for a given site (LID) from a local pickle file.

    Parameters:
    ----------
        threshold_file (str): Path to the local pickle file containing threshold data.
        lid (str): NWS Location Identifier (LID) for the site.

    Returns:
    --------
        stages (dict): Dictionary of stage thresholds for the site, or None if not found.
        flows (dict): Dictionary of flow thresholds for the site, or None if not found.
        messages (list of string): Status print messages.

    Example usage:
    -------------
    stages, flows, messages = load_site_thresholds('path/to/thresholds.pkl', 'FLOX1')
    '''
    stages, flows = {}, {}
    messages = []

    if os.path.isfile(threshold_file) == True:
        # Read pickle file and get the stages and flows dictionary for the site
        with open(threshold_file, 'rb') as f:
            loaded_data = pickle.load(f)
            site_data = loaded_data[loaded_data['nws_lid'] == lid.upper()]

        # Error if site_data is empty
        if site_data.empty:
            msg = f"No threshold data found for LID {lid} in the provided threshold file."
            messages.append(msg)
            print(msg)
            return stages, flows, messages

        # Make output dictionaries for stages and flows
        # Assuming there's only one record per threshold_type per lid
        stages = site_data.loc[site_data['threshold_type'] == 'stages'].to_dict(orient='records')[0]
        del stages['threshold_type']
        del stages['huc']

        flows = site_data.loc[site_data['threshold_type'] == 'flows'].to_dict(orient='records')[0]
        del flows['threshold_type']
        del flows['huc']

        # # Print out the stages and flows for debugging
        # msg = f"Stages for LID {lid}: {stages}; Flows for LID {lid}: {flows}"  # DEBUG
        # messages.append(msg)  # DEBUG
        # print(msg)  # DEBUG
        print('Thresholds loaded from .pkl file.')  # TODO: maybe print the number of thresholds avail?

    else:
        msg = f'Threshold file not found at {threshold_file}, unable to load thresholds for site {lid}.'
        messages.append(msg)
        print(msg)

    return stages, flows, messages


def main(
    env_file,
    output_folder,
    label,
    lst_hucs,
    search,
    metadata_download,
    threshold_download,
    input_metadata_file,
):

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    print('================================')
    print('Starting processing to obtain WRDS data')
    print(f'{dt_string} (UTC)')
    print()

    # Validate output folder path
    if not os.path.exists(output_folder):
        raise ValueError(f'Output folder path {output_folder} does not exist. Please provide a valid path.')

    # Validate inputs
    if metadata_download == False and threshold_download == False:
        raise ValueError(
            'At least one of -m (get metadata) or -t (get thresholds) must be specified as True.'
        )
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
        print('HUC list only limits the thresholds downloaded, all metadata will stil be downloaded.')
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
        metadata_filepath = os.path.join(output_folder, output_metadata_filename)
        
        # Rob: Tell the user the name and location of the file
    
    # If metadata filepath is provided, use it
    else:
        metadata_filepath = input_metadata_file

    ## ===== START SECTION OF CODE TO COPY INTO CATFIM PREPROCESSING =====

    # Load NWM metadata (either by downloading it or pulling it from WRDS)
    # Note: This is the function that we will put into CatFIM code
    __, huc_lid_dict, __ = load_nwm_metadata(
        metadata_filepath, API_BASE_URL, search, metadata_download, lst_hucs
    )

    if not huc_lid_dict:
        sys.exit('Error occurred in metadata download.')

    # Load thresholds if specified
    if threshold_download == True:
        threshold_url = f'{API_BASE_URL}/nws_threshold'

        label_with_date = label_data_file(label, lst_hucs)
        output_thresholds_filename = f'thresholds{label_with_date}.pkl'
        thresholds_filepath = os.path.join(output_folder, output_thresholds_filename)
        
        # Rob: Tell the user the name and location of the file

        # Download thresholds
        download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict)

    # TODO: Should there be an "else"?

    ## ===== END SECTION OF CODE TO COPY INTO CATFIM PREPROCESSING =====

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    time_duration = overall_end_time - overall_start_time

    print('Processing complete.')
    print(f"Total duration: {str(time_duration).split('.')[0]}")
    print('================================')

    # TODO: bolt in Ali's new logging system


if __name__ == '__main__':
    '''
    Run examples:
    1. Download both metadata and thresholds for all HUCs
    python download_process_wrds.py -m -t

    2. Download metadata only for specific HUCs
    python download_process_wrds.py -m -lh "12090301 19020301"

    3. Download thresholds only using an existing metadata file
    python download_process_wrds.py -t -mf "path/to/metadata.pkl"

    4. Specify a custom output folder and label
    python download_process_wrds.py -w "/custom/output/folder" -l "my_label"

    5. Set a custom search distance
    python download_process_wrds.py -s 10 -m -t

    '''
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='This script automates the downloading and processing of datasets from WRDS. '
        'It retrieves metadata and threshold data for specified HUCs and saves them as pickle files in the designated output folder.'
    )

    parser.add_argument(
        '-e',
        '--env-file',
        help='OPTIONAL: Docker mount path to the environment file.'
        'default = /data/config/fim_enviro_values.env',
        required=False,
        default='/data/config/fim_enviro_values.env',
    )
    parser.add_argument(
        '-w',
        '--output-folder',
        help='OPTIONAL: Folder where all outputs will be saved.',
        required=False,
        default='/data/inputs/wrds/',
    )

    parser.add_argument(
        '-l',
        '--label',
        help='OPTIONAL: Label for filenames. Stucture will be metadata_<label>_yyyymmdd.pkl and thresholds_<label>_yyyymmdd.pkl).',
        required=False,
        default='',
    )

    parser.add_argument(
        '-lh',
        '--lst-hucs',
        help="OPTIONAL: Space-delimited list of HUCs to get WRDS data for. Defaults to all HUCs. e.g. '12090301 19020301'"
        "Only limits the thresholds downloaded, all metadata will stil be downloaded.",
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
        action='store_true',
    )

    parser.add_argument(
        '-t',
        '--threshold-download',
        help="OPTIONAL: Create the thresholds.pkl file. Must have at least one of -m or -t selected.",
        required=False,
        default=False,
        action='store_true',
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
