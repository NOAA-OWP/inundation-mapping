#!/usr/bin/env python3
import argparse
import os
import pickle
from datetime import date, datetime, timezone
import pandas as pd
from dotenv import load_dotenv
from tools_shared_functions import aggregate_wbd_hucs, get_datum, get_metadata, get_thresholds



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


def download_all_metadata(metadata_filepath, metadata_url, search):
    '''
    Download all metadata from the WRDS API and save it as a pickle file.

    Parameters
    ----------
    metadata_filepath : str
        Path to the output pickle file for the metadata.
    metadata_url : str
        URL for the WRDS metadata API.
    search : int
        Search distance for upstream and downstream tracing (default is 5).

    Returns
    -------
    list
        List of messages indicating the progress and results of the download.

    Notes
    -----
    This function currently does not use the HUC list functionality because the get_metadata function
    does not get all of the forecast points when using HUCs. When we use nws_lid = all, we get 4810 forecast points,
    whereas when we use huc = all, we only get 3686 forecast points.
    So for now, we are just going to get all forecast points using nws_lid = all, and then filter them later if needed.
    However, we can still filter the thresholds later using the HUC list if it is provided.

    '''

    nwm_us_search, nwm_ds_search = search, search
    output_meta_list = []
    messages = []

    messages.append('Starting metadata download from WRDS...')

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

    try:
        with open(metadata_filepath, "wb") as p_handle:
            pickle.dump(output_meta_list, p_handle, protocol=pickle.HIGHEST_PROTOCOL)

        msg = f"New metadata file saved at {metadata_filepath}"
        messages.append(msg)

    except Exception as e:
        msg = f"Error saving meta data pickle file {metadata_filepath}: {e}"
        messages.append(msg)
        raise (e)

    return messages


def download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict, lid_source_dict):
    '''
    Download all thresholds from the WRDS API for a list of LIDs and save them as CSV files.
    Combine all CSV files into a single pickle file.

    This function can be run in a Jupyter notebook or as a standalone script in a Python environment
    to predownload thresholds for all sites in the metadata pickle file.

    Parameters
    ----------
    thresholds_filepath - str
        filepath where output files will be saved
    threshold_url - str
        URL of the WRDS API endpoint for thresholds
    huc_lid_dict - dict
        dictionary mapping LIDs to HUCs
    lid_source_dict - dict
        dictionary with NWS LID as key and list of available data sources (NRLDB, USGS Rating Depot)
        as value. This is used to create a source preference list for the get_thresholds function.
        If there is no projection available for either source, the value will be an empty list.

    Returns
    -------
    messages - list
        List of messages indicating the progress and results of the download

    Outputs
    -------
    Saves a combined pickle file 'all_thresholds.pkl' containing all thresholds.

    Notes 
    -----
    The output is saved as a pickle file instead of CSV becuase that is the file type we
    chose for saving the metadata. The metadata is a list of dictionaries, which is not easily
    saved as a CSV file. To keep the file types consistent, we are saving the thresholds
    as a pickle file as well.

    Example usage:
        messages = download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict)

    '''
    messages = []
    thresholds_start_time = datetime.now(timezone.utc)

    messages.append('Starting threshold download from WRDS...')

    # Iterate through LIDs in huc_lid_dict and get thresholds from the WRDS API
    list_threshold_dfs = []

    for huc, lids in huc_lid_dict.items():
        for lid in lids:

            try:
                source_crs_availability = lid_source_dict[lid.upper()]

                stages, flows, status = get_thresholds(
                    threshold_url=threshold_url,
                    select_by='nws_lid',
                    selector=lid,
                    threshold='all',
                    source_crs_availability=source_crs_availability,
                )

            except Exception as e:
                msg = f"Unable to retrieve thresholds for LID {lid}, exception occurred: {e}"
                messages.append(msg)
                messages.append(status)
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

    except Exception as e:
        msg = f"Error saving pickle file {thresholds_filepath}: {e}"
        messages.append(msg)
        raise (e)

    thresholds_end_time = datetime.now(timezone.utc)
    thresholds_duration = thresholds_end_time - thresholds_start_time
    messages.append(f"Finished downloading thresholds - Duration: {str(thresholds_duration).split('.')[0]}")

    # TODO: Why not return the dataset as well?
    return messages



def load_nwm_metadata(metadata_filepath, API_BASE_URL, search, metadata_download):
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

    Returns:
        - output_meta_list (list) : Filtered list of metadata dictionaries, each representing a unique NWS LID site.
        - huc_lid_dict (dict) : dictionary mapping LIDs to HUCs.
            ie) a dictionary [('00BRD', '18060005'), ('AANG1', '03130001'), ...]
                - Sorted by upper case site ids.
                - May contain test or invalid sites at this point. Calling code can sort that out.
        - messages (list of string) : Logging (because print statements won't show up in CatFIM.)

    Example usage:

    output_meta_list, huc_lid_dict, messages = load_nwm_metadata(
        metadata_filepath, API_BASE_URL, search, metadata_download
    )

    NOTES:
       - If this function finds warning data, it will include the phrase (case-senstive) "WARNING"
         and same is true for "ERROR". Error means that the calling script can decide if it wants to shut down
         log it, continue, etc.
         It is also possible that you might get multiple messages returned in the "messages" list and some
         may be warnings, others just messages. Could be a mix and match returned.
         If something catestrophic happens, this function will thrown an exception.

        - This function does not filter by HUC list because the HUC information in the metadata is not
         complete and we get more sites by pulling all metadata and then filtering by lat/long intersection
         with the WBD later (aggregate_wbd_hucs). If we filter by HUC list here, we will miss a lot of sites
         that do not have the HUC information in the metadata but are still in the WBD polygons for the HUCs
         we want to process.

    '''
    output_meta_list = []
    messages = []

    if metadata_download == True:
        metadata_url = f'{API_BASE_URL}/metadata'

        # Give a warning if the file will be overwritten
        if os.path.isfile(metadata_filepath):
            msg = f"WARNING: File already exists at {metadata_filepath} & metadata_download is set to True. File will be overwritten."
            messages.append(msg)
        else:
            msg = f"NWM metadata file does not exist at {metadata_filepath}, metadata will be downloaded."
            messages.append(msg)

        # Download metadata and save metadata to pkl file
        metadata_start_time = datetime.now(timezone.utc)
        messages_me = download_all_metadata(metadata_filepath, metadata_url, search)
        messages = messages + messages_me

        metadata_end_time = datetime.now(timezone.utc)
        metadata_duration = metadata_end_time - metadata_start_time

        msg = f"Finished downloading metadata - Duration: {str(metadata_duration).split('.')[0]}"
        messages.append(msg)

    else:
        msg = f"Loading NWM metadata from {metadata_filepath}."
        messages.append(msg)

    # Check metadata file exists and error if metafile is not there
    if not os.path.isfile(metadata_filepath):
        msg = f"NWM metadata file not found at {metadata_filepath}."
        messages.append(msg)

        msg = "ERROR: Cannot proceed without NWM metadata. Set metadata_download to True or provide a valid metadata_filepath."
        messages.append(msg)

        return (output_meta_list, messages)

    # Open metadata file (either the one we just downloaded or pre-existing)
    with open(metadata_filepath, "rb") as p_handle:
        output_meta_list = pickle.load(p_handle)

        msg = f"NWM metadata file loaded from {metadata_filepath}."
        messages.append(msg)

    return output_meta_list, messages


# TODO: Move to CatFIM shared functions?
# Rob: maybe not.. TBD...
def load_site_thresholds(threshold_file, lid):
    '''
    Loads threshold stage and flow data for a given site (LID) from a local pickle file.

    Parameters:
    ----------
        threshold_file -  STR 
            Path to the local pickle file containing threshold data.
        lid -  STR 
            NWS site ID (LID) for which to load thresholds.

    Returns:
    --------
        stages -  DICT or None
            Dictionary of stage thresholds for the site, or None if not found.
        flows -  DICT or None
            Dictionary of flow thresholds for the site, or None if not found.
        messages -  LIST OF STRING 
            Status print messages.

    Example:
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
        # print('Thresholds loaded from .pkl file.')  # TODO: maybe print the number of thresholds avail?

    else:
        msg = f'Threshold file not found at {threshold_file}, unable to load thresholds for site {lid}.'
        messages.append(msg)

    return stages, flows, messages


def check_metadata_CRS_availability(output_meta_list):
    '''
    Iterate down the output meta list to see what projections are available for each site
    and create an output dictionary that has the NWS LID as the key and the preferred data
    source (NRLDB or USGS) as the value. This will be used to create a source preference list
    for the get_thresholds function.

    Parameters
    ----------
    output_meta_list - LIST
        List of dictionaries containing metadata for each NWM site. (From get_metadata function.)

    Returns
    ----------
    lid_source_dict - DICT     
        Dictionary with NWS LID as key and list of available data sources (NRLDB, USGS Rating Depot) as value.
        If there is no projection available for either source, the value will be an empty list.


    Example usage:
    lid_source_dict = check_metadata_CRS_availability(output_meta_list)

    '''

    # This list also includes the misspellings that the code knows how to handle # TODO: Add to a shared variables file?
    correct_spellings = ['NAD27', 'NAD83', 'LMSL']
    nad27_misspellings = [
        'NGVD 1929',
        'NAD 1927',
        'NAD 1929',
        'NAD-27',
        '1929',
        'NAD1927',
        'NGVD29',
        '1927',
        'NGVD',
        'NVGD',
        'NAD 27',
        'NGDV 1929',
        'NGVD1929',
        'NAAD27',
        'NAD29',
        '1927 NGVD',
        '1929',
        'NVD 1929',
        'NAD1929',
        'NGVD1927',
        '1929 NGV',
        '1929 NGVD',
        'NA 1927',
        'NAVD27',
    ]
    nad83_misspellings = [
        'NAD 1983',
        'NAVD88',
        'NAD 83',
        'NAD1983',
        'WGS84',
        'NADA 1983',
        'NAV83',
        'NAVD 1988',
        '1988',
        'NAD 88',
        'NAVD 88',
        'nad83',
        'NAV-88',
        'NAVD83',
        'NGVD1988',
        'NAD84',
        'NAD 1988',
        '1988',
        'NAV83',
        'NAVD-88',
        'NAD88',
        'NAD87',
        'NAD893',
    ]

    acceptable_horizontal_projections = correct_spellings + nad27_misspellings + nad83_misspellings

    lid_source_dict = {}

    for output_meta_i in output_meta_list:

        projections_available_i = []
        nws_lid = output_meta_i['identifiers']['nws_lid']
        nws_datum_info, usgs_datum_info = get_datum(output_meta_i)

        if nws_datum_info['crs'] in acceptable_horizontal_projections:
            projections_available_i.append('NRLDB')
        if usgs_datum_info['crs'] in acceptable_horizontal_projections:
            projections_available_i.append('USGS Rating Depot')

        lid_source_dict[nws_lid] = projections_available_i

        # print(f'nws_lid: {nws_lid}')  ## TEMP DEBUG
        # print(f'nws CRS:           {nws_datum_info["crs"]}')  ## TEMP DEBUG
        # print(f'usgs CRS:           {usgs_datum_info["crs"]}')  ## TEMP DEBUG
        # print(f'projections available:            {projections_available_i}')  ## TEMP DEBUG
        # print()  ## TEMP DEBUG

    return lid_source_dict


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

    # Import variables from .env file
    load_dotenv(env_file)
    API_BASE_URL = os.getenv('API_BASE_URL')
    WBD_LAYER = os.getenv("WBD_LAYER")

    if API_BASE_URL is None:
        raise ValueError(
            f'API base url not found. Ensure the .env file ({env_file}) has the API_BASE_URL.'
        )
    
    if WBD_LAYER is None and threshold_download == True:
        raise ValueError(
            f'WBD layer name not found in env file ({env_file}). This is required for threshold download and aggregation of HUCs.'
        )

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
        print('Only threshold data will be saved.') 
        # For this setup, a valid metadata pkl file must be provided (check will occur for this later on)

    # Format HUC list
    lst_hucs = lst_hucs.split()
    if 'all' in lst_hucs:
        print('No HUC list provided, downloading data for all HUCs.')
    else:
        print(f'Downloading data for {len(lst_hucs)} HUCs.')
        print('HUC list only limits the thresholds downloaded, all metadata will stil be downloaded.')

    print()
    print("----- METADATA -----")

    # If no metafile is provided, generate filepath and filename
    if input_metadata_file == '':
        label_with_date = label_data_file(label, lst_hucs)
        output_metadata_filename = f'metadata{label_with_date}.pkl'
        metadata_filepath = os.path.join(output_folder, output_metadata_filename)

    # If metadata filepath is provided, use it
    else:
        metadata_filepath = input_metadata_file
    
    print("Begin loading metadata...")
    if metadata_download == True:
        print("Metadata will be downloaded from WRDS, could take around 10 minutes.")
    print(f"Metadata filepath: {metadata_filepath}.")

    # Load NWM metadata (either by loading file or downloading from WRDS)
    output_meta_list, messages = load_nwm_metadata(
        metadata_filepath, API_BASE_URL, search, metadata_download
    )

    for msg in messages:
        print(msg)

    print()
    print("----- THRESHOLDS -----")

    # Load thresholds if specified
    if threshold_download == True:
    
        # Get the HUC dictionary
        print('Getting HUC information from the metadata using the WBD layer...')
        huc_lid_dict, nwm_sites_all_gdf = aggregate_wbd_hucs(output_meta_list, WBD_LAYER, retain_attributes=True)

        # Filter huc_lid_dict to only include HUCs in huc_lst
        if 'all' not in lst_hucs:
            keep = set(lst_hucs)
            for huc in list(huc_lid_dict):
                if huc not in keep:
                    del huc_lid_dict[huc]

        if len(huc_lid_dict) == 0:
            raise Exception("No metadata left after filtering the metadata pickle file by the HUC list.")

        threshold_url = f'{API_BASE_URL}/nws_threshold'

        label_with_date = label_data_file(label, lst_hucs)
        output_thresholds_filename = f'thresholds{label_with_date}.pkl'
        thresholds_filepath = os.path.join(output_folder, output_thresholds_filename)

        print(f"Thresholds will be downloaded for sites in {len(huc_lid_dict)} HUCs")
    
        # Get a dictionary of which sources have valid CRS's for each site
        lid_source_dict = check_metadata_CRS_availability(output_meta_list)

        # Download thresholds
        messages = download_all_thresholds(thresholds_filepath, threshold_url, huc_lid_dict, lid_source_dict)

        for msg in messages:
            print(msg)
        print()

    else:
        print('Threshold download not selected, skipping threshold download.')
        print()

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    time_duration = overall_end_time - overall_start_time

    print('Processing complete.')
    print(f"Total duration: {str(time_duration).split('.')[0]}")
    print('================================')

    # TODO: bolt in Ali's new logging system


if __name__ == '__main__':
    '''

    Arguments:
    -----------
    -m, --metadata-download (Optional) - Whether to create the metadata.pkl file.
                                Must have at least one of -m or -t provided.

    -t, --threshold-download (Optional) - Whether to create the thresholds.pkl file.
                                Must have at least one of -m or -t provided.

    -lh, --lst-hucs (Optional) - Space-delimited list of HUCs for which to download
                                thresholds (e.g. '12090301 19020301').
                                Defaults to all HUCs if no list is provided.
                                Note: Only limits the thresholds downloaded, all
                                metadata will stil be downloaded.
    
    -mf, --input-metadata-file (Optional) - Input metadata file to use for pulling
                                thresholds. Will error if -m flag is also used (must
                                be used with -t flag).

    -e, --env-file (Optional) - Docker mount filepath to the environment file.
                                Default is /data/config/fim_enviro_values.env

    -w, --output-folder (Optional) - Folder where all outputs will be saved.
                                Default is /data/inputs/wrds/

    -l, --label (Optional) - Label for to add additional info to filenames.
                                Stucture will be metadata_<label>_yyyymmdd.pkl
                                and thresholds_<label>_yyyymmdd.pkl).
                                Default is empty string, which will just give you
                                metadata_yyyymmdd.pkl and thresholds_yyyymmdd.pkl.

    -s, --search (Optional) - Upstream and downstream search in miles.
                                Defaults to 5 if no number is provided.

    Run examples:

    - Download BOTH metadata and thresholds for specific HUCs and a custom output folder

        python /foss_fim/data/wrds/download_process_wrds.py -m -t -lh "12090301 19020301" -w '/data/catfim/emily_test'

    - Download metadata ONLY for specific HUCs

        python /foss_fim/data/wrds/download_process_wrds.py -m

    - Download thresholds ONLY (must use an existing metadata file

        python /foss_fim/data/wrds/download_process_wrds.py -t -mf "path/to/metadata.pkl"

    - Download BOTH thresholds and metadata and specify a custom output folder, label, search distance, and HUC list

        python /foss_fim/data/wrds/download_process_wrds.py -m -t -w "/custom/output/folder" -l "my_label" -s 10 -lh "12090301 19020301"

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
