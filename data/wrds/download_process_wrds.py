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

# moved over from inundation-mapping/tools/tools_shared_functions.py
# TODO: Remove this function from the tools shared functions file
# TODO: Re-route all files that use this function to get it from here
def get_metadata(
    metadata_url,
    select_by,
    selector,
    must_include=None,
    upstream_trace_distance=None,
    downstream_trace_distance=None,
):
    '''
    Retrieve metadata for a site or list of sites.

    Parameters
    ----------
    metadata_url : STR
        metadata base URL.
    select_by : STR
        Location search option. Options include: 'state', TODO: test 'nws_lid'
    selector : LIST
        Value to match location data against. Supplied as a LIST.
    must_include : STR, optional
        What attributes are required to be valid response. The default is None.
    upstream_trace_distance : INT, optional
        Distance in miles upstream of site to trace NWM network. The default is None.
    downstream_trace_distance : INT, optional
        Distance in miles downstream of site to trace NWM network. The default is None.

    Returns
    -------
    metadata_list : LIST
        Dictionary or list of dictionaries containing metadata at each site.
    metadata_dataframe : Pandas DataFrame
        Dataframe of metadata for each site.

    '''

    # Format selector variable in case multiple selectors supplied
    format_selector = '%2C'.join(selector)
    # Define the url
    url = f'{metadata_url}/{select_by}/{format_selector}/'
    # Assign optional parameters to a dictionary
    params = {}
    params['must_include'] = must_include
    params['upstream_trace_distance'] = upstream_trace_distance
    params['downstream_trace_distance'] = downstream_trace_distance
    # Suppress Insecure Request Warning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    # Request data from url
    response = requests.get(url, params=params, verify=False)
    #    print(response)
    #    print(url)
    if response.ok:
        # Convert data response to a json
        metadata_json = response.json()
        # Get the count of returned records
        location_count = metadata_json['_metrics']['location_count']
        # Get metadata
        metadata_list = metadata_json['locations']
        # Add timestamp of WRDS retrieval
        timestamp = response.headers['Date']
        # Add timestamp of sources retrieval
        timestamp_list = metadata_json['data_sources']['metadata_sources']

        # Default timestamps to "Not available" and overwrite with real values if possible.
        nwis_timestamp, nrldb_timestamp = "Not available", "Not available"
        for timestamp in timestamp_list:
            if "NWIS" in timestamp:
                nwis_timestamp = timestamp
            if "NRLDB" in timestamp:
                nrldb_timestamp = timestamp

        #        nrldb_timestamp, nwis_timestamp = metadata_json['data_sources']['metadata_sources']
        # get crosswalk info (always last dictionary in list)
        crosswalk_info = metadata_json['data_sources']
        # Update each dictionary with timestamp and crosswalk info also save to DataFrame.
        for metadata in metadata_list:
            metadata.update({"wrds_timestamp": timestamp})
            metadata.update({"nrldb_timestamp": nrldb_timestamp})
            metadata.update({"nwis_timestamp": nwis_timestamp})
            metadata.update(crosswalk_info)
        metadata_dataframe = pd.json_normalize(metadata_list)
        # Replace all periods with underscores in column names
        metadata_dataframe.columns = metadata_dataframe.columns.astype(str).str.replace('.', '_')
    else:
        # if request was not succesful, print error message.
        # TODO: Output this as a status string because the print is getting suppressed
        print(f'Code: {response.status_code}\nMessage: {response.reason}\nURL: {response.url}')
        # Return empty outputs
        metadata_list = []
        metadata_dataframe = pd.DataFrame()
    return metadata_list, metadata_dataframe


# moved over from inundation-mapping/tools/tools_shared_functions.py
# TODO: Remove this function from the tools shared functions file
# TODO: Re-route all files that use this function to get it from here
def get_thresholds(threshold_url, select_by, selector, threshold='all'):
    '''
    Get nws_lid threshold stages and flows (i.e. bankfull, action, minor,
    moderate, major). Returns a dictionary for stages and one for flows.

    Parameters
    ----------
    threshold_url : STR
        WRDS threshold API.
    select_by : STR
        Type of site (nws_lid, usgs_site_code etc).
    selector : STR
        Site for selection. Must be a single site.
    threshold : STR, optional
        Threshold option. The default is 'all'.

    Returns
    -------
    stages : DICT
        Dictionary of stages at each threshold.
    flows : DICT
        Dictionary of flows at each threshold.
    status_msg : STR
        Status of API call and data availability.

    '''
    params = {}
    params['threshold'] = threshold
    url = f'{threshold_url}/{select_by}/{selector}'

    # Initialize status message
    status_msg = f"Selector: {selector}: "

    # response = requests.get(url, params=params, verify=False)

    # Call the API
    session = requests.Session()

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)

    response = session.get(url, params=params, verify=False)

    if response.status_code == 200:
        thresholds_json = response.json()

        # Get metadata
        thresholds_info = thresholds_json['value_set']
        threshold_count = thresholds_json['_metrics']['threshold_count']
        status_msg += f"WRDS response sucessful. {threshold_count} threshold types available. "

        # Initialize stages/flows dictionaries
        stages = {}
        flows = {}
        # Check if thresholds information is populated. If site is non-existent thresholds info is blank
        if thresholds_info:
            # Get all rating sources and corresponding indexes in a dictionary
            rating_sources = {
                i.get('calc_flow_values').get('rating_curve').get('source'): index
                for index, i in enumerate(thresholds_info)
            }
            # Get threshold data use USGS Rating Depot (priority) otherwise NRLDB.
            if 'USGS Rating Depot' in rating_sources:
                threshold_data = thresholds_info[rating_sources['USGS Rating Depot']]
            elif 'NRLDB' in rating_sources:
                threshold_data = thresholds_info[rating_sources['NRLDB']]
            # If neither USGS or NRLDB is available use first dictionary to get stage values.
            else:
                threshold_data = thresholds_info[0]
            # Get stages and flows for each threshold
            if threshold_data:
                status_msg += "Thresholds available. "

                stages = threshold_data['stage_values']
                flows = threshold_data['calc_flow_values']
                # Add source information to stages and flows. Flows source inside a nested dictionary. Remove key once source assigned to flows.
                stages['source'] = threshold_data.get('metadata').get('threshold_source')
                flows['source'] = flows.get('rating_curve', {}).get('source')
                flows.pop('rating_curve', None)
                # Add timestamp WRDS data was retrieved.
                stages['wrds_timestamp'] = response.headers['Date']
                flows['wrds_timestamp'] = response.headers['Date']
                # Add Site information
                stages['nws_lid'] = threshold_data.get('metadata').get('nws_lid')
                flows['nws_lid'] = threshold_data.get('metadata').get('nws_lid')
                stages['usgs_site_code'] = threshold_data.get('metadata').get('usgs_site_code')
                flows['usgs_site_code'] = threshold_data.get('metadata').get('usgs_site_code')
                stages['units'] = threshold_data.get('metadata').get('stage_units')
                flows['units'] = threshold_data.get('metadata').get('calc_flow_units')
        return stages, flows, status_msg
    else:
        status_msg += "WRDS response error." 
        print(status_msg)
        stages = None
        flows = None

        return stages, flows, status_msg


# ----- 

# TODO: Remove this function from the generate_categorical_fim_flows.py and replace it with code that can read it in
# was __load_nwm_metadata
def download_all_metadata(workspace, metadata_url, search, metadata_download, label_with_date):

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

    print(f'    Total number of unique LIDs: {len(unique_lids_list)}')

    meta_filepath = None

    if metadata_download == True:
        try:
            filename = f'nwm_metafile{label_with_date}.pkl'
            meta_filepath = os.path.join(workspace, filename)

            with open(meta_filepath, "wb") as p_handle:
                pickle.dump(output_meta_list, p_handle, protocol=pickle.HIGHEST_PROTOCOL)

            print(f"    Metadata saved at {meta_filepath}")

            file_size_bytes = os.path.getsize(meta_filepath)
            file_size_kb = round(file_size_bytes / 1024, 2)
            file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
            print(f"    File size: {file_size_kb} kb or {file_size_mb} mb")

        except Exception as e:
            print(f"Error saving pickle file {meta_filepath}: {e}")


    return output_meta_list, meta_filepath # so CatFIM can read it in


# moved over from inundation-mapping/tools/tools_shared_functions.py
# TODO: Remove this function from the tools shared functions file
# TODO: Re-route all files that use this function to get it from here
def download_all_thresholds(workspace, threshold_url, metadata_list, label_with_date, lst_hucs):
    """
    Download all thresholds from the WRDS API for a list of LIDs and save them as CSV files.
    Combine all CSV files into a single pickle file.

    This function can be run in a Jupyter notebook or as a standalone script in a Python environment
    to predownload thresholds for all sites in the metadata pickle file.

    Parameters:
    - threshold_url: str, URL of the WRDS API endpoint for thresholds.
    - output_folder: str, path to the folder where output files will be saved.
    - metadata_list: 

    Returns:
    - None
    
    Outputs:
    - CSV files for each LID in a subfolder 'threshold_download' within the output_folder.
    - A combined pickle file 'all_thresholds.pkl' containing all thresholds.
    """
    print('Starting threshold download from WRDS...')

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

    print(f'    Number of sites to download thresholds for: {len(lid_list)}')

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

    filename = f'thresholds{label_with_date}.pkl'
    thresholds_filepath = os.path.join(workspace, filename)

    # Save the combined DataFrame to a pickle file
    try:
        with open(thresholds_filepath, 'wb') as f:
            pickle.dump(all_thresholds_df, f)
        print(f"    Thresholds saved at {thresholds_filepath}")

        file_size_bytes = os.path.getsize(thresholds_filepath)
        file_size_kb = round(file_size_bytes / 1024, 2)
        file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
        print(f"    File size: {file_size_kb} kb or {file_size_mb} mb")

    except Exception as e:
        print(f"Error saving pickle file {thresholds_filepath}: {e}")

    return thresholds_filepath # so CatFIM can read it in

    # return huc_lid_dict and safe as file? TODO: add in, add a note that it's just for tracing

# def emulate_wrds_data(data_csv) # actually I think that emulate wrds data really might need to be its own file in the wrds folder... # TODO

# this function will also be able to be run from within CatFIM
def obtain_wrds_data(env_file,
                    workspace,
                    label,
                    lst_hucs,
                    search,
                    metadata_download,
                    threshold_download):

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    print('================================')
    print('Starting processing to obtain WRDS data')
    print(f'{dt_string} (UTC)')
    print()

    # Validate workspace
    if not os.path.exists(workspace):
        raise ValueError(f'Workspace path {workspace} does not exist. Please provide a valid path.')
    

    if metadata_download == False and threshold_download == False:
        raise ValueError('At least one of -m (get metadata) or -t (get thresholds) must be specified as True.')
    elif metadata_download == True and threshold_download == True:
        print('Both metadata and thresholds will be downloaded and saved.')
    elif metadata_download == True and threshold_download == False:
        print('Only metadata will be downloaded and saved.')
    elif threshold_download == True and metadata_download == False:
        print('Only threshold data will be saved (but metadata will still be downloaded).')
    
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
    metadata_url = f'{API_BASE_URL}/metadata'
    threshold_url = f'{API_BASE_URL}/nws_threshold'

    # If a list of HUCs is provided, add 'subset' to the label
    subset = '' if 'all' in lst_hucs else '_subset'

    # Add a leading underscore to the label if it's not empty
    label = f'_{label}' if label != '' else label

    date_formatted = date.today().strftime("%d%m%Y")
    label_with_date = f'{label}{subset}_{date_formatted}'

    # Download metadata and save metadata to pkl file 
    # (If thresholds_only == True, metadata will be downloaded but not saved)
    metadata_start_time = datetime.now(timezone.utc)
    output_meta_list, ___ = download_all_metadata(workspace, metadata_url, search, metadata_download, label_with_date)
    
    metadata_end_time = datetime.now(timezone.utc)
    metadata_duration = metadata_end_time - metadata_start_time
    print(f"    Finished downloading metadata - Duration: {str(metadata_duration).split('.')[0]}")
    print()

    if threshold_download == True:
        # Download thresholds
        thresholds_start_time = datetime.now(timezone.utc)

        ___ = download_all_thresholds(workspace, threshold_url, output_meta_list, label_with_date, lst_hucs)

        thresholds_end_time = datetime.now(timezone.utc)
        thresholds_duration = thresholds_end_time - thresholds_start_time
        print(f"    Finished downloading thresholds - Duration: {str(thresholds_duration).split('.')[0]}")
        print()

        # TODO: add a note about why .pkl for thresholds


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

    args = vars(parser.parse_args())

    # Main function call
    obtain_wrds_data(**args)
