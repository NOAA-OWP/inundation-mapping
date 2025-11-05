#!/usr/bin/env python3
 
import os
import logging
import pickle

from datetime import datetime, timezone

from dotenv import load_dotenv

from tools_shared_functions import (
    aggregate_wbd_hucs,
    get_metadata,
)

# gpd.options.io_engine = "pyogrio"

# Shared by both generate_categorical_fim.py and catfim_process_hucs.py
def get_meta_and_huc_data(output_catfim_dir,
                          nwm_us_search,
                          nwm_ds_search,
                          nwm_meta_file_path,
                          get_new_meta_data,
                          lst_hucs,
                          env_file):

    '''
    Returns:
        - huc_dictionary: list: Filtered list of metadata dictionaries, each representing a unique NWS LID site.
        - meta_gdf: all meta data for valid HUCs and all sites associated with it.
        
        Note: Could come back with an empty huc_dictionary, meta_gdf if just one huc was submitted and it was
        invalid or did not find any matching ahps data.
    '''

    all_meta_lists = load_nwm_metadata(
        output_catfim_dir, nwm_us_search, nwm_ds_search, nwm_meta_file_path, get_new_meta_data, env_file)

    # logging.info("+++++++++++++++++++")
    # logging.info(f"all_meta_lists is {all_meta_lists}")
    # logging.info("+++++++++++++++++++")

    print("")

    # Assign HUCs to all sites using a spatial join of the FIM 4 HUC layer.
    # Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
    # of all sites used later in script.
    logging.info("Start aggregate_wbd_hucs")

    huc_dictionary, meta_gdf = aggregate_wbd_hucs(all_meta_lists, os.getenv("input_wbd_layer"), True, lst_hucs)

    # Could come back with an empty huc_dictionary, out_gdf if just one huc was submitted and it was
    # invalid or did not find any matching ahps data.
    return huc_dictionary, meta_gdf


def load_nwm_metadata(output_catfim_dir,
                      metadata_url,
                      nwm_us_search,
                      nwm_ds_search,
                      nwm_meta_file,
                      get_new_meta_data,
                      env_file):
    '''
    Runs for both stage and flow. Loads and filters NWM metadata.

    This function checks if a local metadata pickle file exists. If it does, the metadata is loaded from the file.
    Otherwise, it downloads metadata from the specified URL using two API calls: one for all forecast points and
    another for all points in OCONUS regions (HI, PR, AK). The results are combined, and duplicate or None-valued
    NWS LIDs are filtered out. The filtered metadata is then saved to a pickle file for future use.
    
    This function has three modes:
       1) get_new_meta_data is True, which means call WRDS directly (or Emily's tool function to do the same thing)
          It might error out if this was included on a non OWP server. How can we trap that?
       2) get_new_meta_data is False and nwm_meta_file is empty. That means use the bash_variables default to load
          the pickle file for the meta data.
       3) get_new_meta_data is False and nwm_meta_file has a path. This means use the submitted meta file path
          for pickle data.

    Args:
        - output_catfim_dir (str): Directory where output files, including the metadata pickle, are stored.
             Note: This value might be empty or None, meaning don't save the pickle file.
        - metadata_url (str): URL endpoint for retrieving NWM metadata.
        - nwm_us_search (int): Upstream trace distance for metadata search.
        - nwm_ds_search (int): Downstream trace distance for metadata search.
        - nwm_metafile (str): Path to the local metadata pickle file.
        - get_new_meta_data (bool):  TODO
        - env_file(str):  TODO

    Returns:
        list: Filtered list of metadata dictionaries, each representing a unique NWS LID site.
    '''

    logging.info("Loading nwm metadata")
    start_dt = datetime.now(timezone.utc)

    # -----------
    # TODO: eventually someday add input validation. Can leave for another release.

    output_meta_list = []
    
    # Check to see if meta file already exists
    # This feature means we can copy the pickle file to another enviro (AWS?) as it won't need to call
    # WRDS unless we need a smaller or modified version. This one likely has all nws_lid data.

    save_pickle_file = False
    if not get_new_meta_data:
        if not nwm_meta_file:
            # get the bash_variables value
            nwm_meta_file = os.getenv("nwm_meta_file")
            save_pickle_file = True  # Save a copy if it came from bash_variables
            
        if os.path.isfile(nwm_meta_file):
            with open(nwm_meta_file, "rb") as p_handle:
                output_meta_list = pickle.load(p_handle)
        else:
            raise Exception("nwm_meta_file at {nwm_meta_file} does not exist")
    else:  # get new data
        api_base_url = load_env_values(env_file)
        metadata_url = f'{api_base_url}/metadata'    

        save_pickle_file = True  # Save a copy if it was newly loaded

        # Get all forecast points
        forecast_point_meta_list, ___ = get_metadata(
            metadata_url,
            select_by='nws_lid',
            selector=['all'],
            must_include='nws_data.rfc_forecast_point',
            upstream_trace_distance=nwm_us_search,
            downstream_trace_distance=nwm_ds_search,
        )

        # Get all points for OCONUS regions (HI, PR, and AK)
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
        unique_lids, duplicate_lids = [], []
        duplicate_meta_list = []
        nonelid_metadata_list = []

        for i, site in enumerate(unfiltered_meta_list):
            nws_lid = site['identifiers']['nws_lid']

            if nws_lid is None:
                # No LID available
                nonelid_metadata_list.append(site)

            elif nws_lid in unique_lids:
                # Duplicate LID
                duplicate_lids.append(nws_lid)
                duplicate_meta_list.append(site)

            else:
                # debug
                # if nws_lid.upper() not in ['PNTA3', 'PWBA3']:
                #     continue

                # Unique/unseen LID that's not None
                unique_lids.append(nws_lid)
                output_meta_list.append(site)

        logging.info(f'{len(duplicate_lids)} duplicate points removed.')
        # logging.info(f'Duplicate point LIDs: {duplicate_lids}')
        logging.info(f'{len(nonelid_metadata_list)} points with value of None for nws_lid removed.')
        logging.info(f'Filtered metadatada downloaded for {len(output_meta_list)} points.')

    # ----------
    if save_pickle_file and output_catfim_dir is not None and output_catfim_dir != "":
        meta_file = os.path.join(output_catfim_dir, "nwm_metafile.pkl")
        logging.info(f"Meta file will be downloaded and saved at {meta_file}")

        with open(meta_file, "wb") as p_handle:
            pickle.dump(output_meta_list, p_handle, protocol=pickle.HIGHEST_PROTOCOL)

    end_dt = datetime.now(timezone.utc)
    time_duration = end_dt - start_dt
    logging.info(f"Retrieving metadata - Duration: {str(time_duration).split('.')[0]}")

    return output_meta_list


# used by stage based only.    
#def get_threshold_data():
    
 
# used by flow based only. hummm... likely not
# def get_flow_data():  use generate_categorical_fim_flows to get this.


def load_env_values(env_file):
    '''
    Loads environment variables from a .env file.
    Expects the .env file to contain API_BASE_URL
    
    Parameters
    ----------
        env_file (str): Path to the .env file.

    '''
    if os.path.exists(env_file) == False:
        raise Exception(f"The environment file of {env_file} does not seem to exist")

    load_dotenv(env_file)
    # import variables from .env file
    api_base_url = os.getenv("API_BASE_URL")
    
    # At this point, we only have one value to return.
    return api_base_url

