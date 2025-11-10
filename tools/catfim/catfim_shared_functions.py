#!/usr/bin/env python3
 
import os
import logging
import pickle

import pandas as pd

from datetime import datetime, timezone

from dotenv import load_dotenv

from tools_shared_functions import (
    aggregate_wbd_hucs,
    get_metadata,
)

# gpd.options.io_engine = "pyogrio"

# Shared by both generate_categorical_fim.py and catfim_process_hucs.py

# def emily_get_meta_and_huc_data(output_catfim_dir,
#                                 search,
#                                 nwm_meta_file_path,
#                                 get_new_meta_data,
#                                 lst_hucs,
#                                 api_base_url):


#     '''
    
#     For Catfim code, it is used by two scripts.
#         - One is generate_categorical_fim which may be working with more than one huc and is aiming to get list
#           of hucs and their sites for validation purposes. It does not need to use the metadata returned but that is ok.
#         - Another is catfim_process_huc which does want the list of sites, but will pass in just it's own huc.
#           However, it does need to use the incoming meta data.

#     Returns:
#         - huc_dictionary: list: Filtered list of metadata dictionaries, each representing a unique NWS LID site.
#         - meta_gdf: all meta data for valid HUCs and all sites associated with it.
        
#         Note: Could come back with an empty huc_dictionary, meta_gdf if just one huc was submitted and it was
#         invalid or did not find any matching ahps data.
#     '''

#     # Emily's will always load the meta data, either live, by file, but I have to give her
#     # the nwm_meta_file_path

#     if get_new_meta_data is False:
        
#         use_default_meta_file = False
#         if nwm_meta_file_path is None or nwm_meta_file_path == "":
#             nwm_meta_file_path = os.getenv("nwm_meta_file")  # get it from bash_variables 
#             use_default_meta_file = True           
#             # dw.load_nwm_metadata will load this file for us
#         # else:  # a meta file path has been submitted by the user (likely an test meta file)

#         if not os.path.isfile(nwm_meta_file_path):
#             if use_default_meta_file:
#                 errMsg = f"The default nwm_meta_file of {nwm_meta_file_path} does not exist."
#                 "Check bash_variables and pathing."
#             else:
#                 errMsg = f"The nwm meta file path of {nwm_meta_file_path} does not exist."
#                 "Check your inputs, including case, and pathing."

#             raise Exception(errMsg)
    
#     # output_meta_list is a list of json blocks
#     # nwm_meta_file_path will never be empty, but may or may not exist. I assume that if dw.load_nwm_metadata finds
#     # the file, it will load it
    
#     logging.info("Loading nwm metadata")
#     start_dt = datetime.now(timezone.utc)
    
#     # In this case, I do not need the huc_list_dictionary returned from load_nwm_metadata as the 
#     # second return object.
#     # we will submit output_meta_list to aggregate_wbd_hucs to validate, clean it up,
#     # etc
    
#     # dw.load_nwm_metadata can a number of things:
#     #   1) valid data. coudl the output_meta_list be empty? We coudl get valid data and a message saying
#     #      that the pickle file was newly saved or something.
#     #   2) an exception
#     #   3) Some way to tell me that the file was not found ?? ie.. shoudl we let it do the sys.exit? that won't work in MP.
#     #      Depending on the return_msg, output_meta_list might be empty.
#     output_meta_list, __, return_msg = dw.load_nwm_metadata(nwm_meta_file_path,
#                                                             api_base_url,
#                                                             search,
#                                                             get_new_meta_data,
#                                                             lst_hucs)

#     # return_msg might be a warning, or a line saying where it was saved ??  Hummm..
#     # maybe have it just return something like 
#     if return_msg != "":
#         logging.info(return_msg)

#     end_dt = datetime.now(timezone.utc)
#     time_duration = end_dt - start_dt
#     logging.info(f"Retrieving metadata - Duration: {str(time_duration).split('.')[0]}")

#     print("")

#     # Assign HUCs to all sites using a spatial join of the FIM 4 HUC layer.
#     # Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
#     # of all sites used later in script.
#     logging.info("Start aggregate_wbd_hucs")
      # I don't think we need a duration on this as I think it is pretty fast.

#     huc_dictionary, meta_gdf = aggregate_wbd_hucs(output_meta_list, os.getenv("input_wbd_layer"), True, lst_hucs)

#     # Could come back with an empty huc_dictionary, out_gdf if just one huc was submitted and it was
#     # invalid or did not find any matching ahps data.
#     return huc_dictionary, meta_gdf


def get_meta_and_huc_data(output_catfim_dir,
                          metadata_url,
                          search,
                          nwm_meta_file_path,
                          get_new_meta_data,
                          lst_hucs):

    '''
    Returns:
        - huc_dictionary: list: Filtered list of metadata dictionaries, each representing a unique NWS LID site.
        - meta_gdf: all meta data for valid HUCs and all sites associated with it.
        
        Note: Could come back with an empty huc_dictionary, meta_gdf if just one huc was submitted and it was
        invalid or did not find any matching ahps data.
    '''

    logging.info("Loading nwm metadata")
    start_dt = datetime.now(timezone.utc)

    all_meta_lists = __load_nwm_metadata(
        output_catfim_dir, metadata_url, search, nwm_meta_file_path, get_new_meta_data)

    end_dt = datetime.now(timezone.utc)
    time_duration = end_dt - start_dt
    logging.info(f"Retrieving metadata - Duration: {str(time_duration).split('.')[0]}")

    print("")

    # Assign HUCs to all sites using a spatial join of the FIM 4 HUC layer.
    # Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
    # of all sites used later in script.
    logging.info("Start aggregate_wbd_hucs")
    # I don't think we need a duration on this as I think it is pretty fast.
    
    huc_dictionary, meta_gdf = aggregate_wbd_hucs(all_meta_lists, os.getenv("input_wbd_layer"), True, lst_hucs)

    # Could come back with an empty huc_dictionary, out_gdf if just one huc was submitted and it was
    # invalid or did not find any matching ahps data.
    return huc_dictionary, meta_gdf


# this is a temp one, soon to be replaced by emily's version above.
def __load_nwm_metadata(output_catfim_dir,
                      metadata_url,
                      search,
                      nwm_meta_file,
                      get_new_meta_data):
    '''
    Runs for both stage and flow. Loads and filters NWM metadata.

    
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

    '''  NEEDS UPDATING
        This function checks if a local metadata pickle file exists. If it does, the metadata is loaded from the file.
    Otherwise, it downloads metadata from the specified URL using two API calls: one for all forecast points and
    another for all points in OCONUS regions (HI, PR, AK). The results are combined, and duplicate or None-valued
    NWS LIDs are filtered out. The filtered metadata is then saved to a pickle file for future use.
    
    '''

    # -----------
    # TODO: eventually, someday, kinda, somewhat, basically, add input validation.

    output_meta_list = []
    
    # Check to see if meta file already exists
    # This feature means we can copy the pickle file to another enviro (AWS?) as it won't need to call
    # WRDS unless we need a smaller or modified version. This one likely has all nws_lid data.

    save_pickle_file = False
    if get_new_meta_data is True:
        if not nwm_meta_file:  # means it is either None or empty
            # get the bash_variables value
            nwm_meta_file = os.getenv("nwm_meta_file")
            save_pickle_file = True  # Save a copy if it came from bash_variables ???
            
        if os.path.isfile(nwm_meta_file):
            logging.info(f"Loading nwm_meta data file from {nwm_meta_file}")
            with open(nwm_meta_file, "rb") as p_handle:
                output_meta_list = pickle.load(p_handle)
        else:
            raise Exception(f"nwm_meta_file at {nwm_meta_file} does not exist")
        
        # Does that pickle file have this huc in it? maybe more then it? For now,
        # just check that it has some records, we will filter to the HUC later as this 
        # tool is reused by pre-processing for now
        
    else:  # get new data
        save_pickle_file = True  # Save a copy if it was newly retrieved

        # Get all forecast points
        forecast_point_meta_list, ___ = get_metadata(
            metadata_url,
            select_by='nws_lid',
            selector=['all'],
            must_include='nws_data.rfc_forecast_point',
            upstream_trace_distance=search,
            downstream_trace_distance=search,
        )

        # Get all points for OCONUS regions (HI, PR, and AK)
        oconus_meta_list, ___ = get_metadata(
            metadata_url,
            select_by='state',
            selector=['HI', 'PR', 'AK'],
            must_include=None,
            upstream_trace_distance=search,
            downstream_trace_distance=search,
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
        logging.info(f'{len(nonelid_metadata_list)} points with value of None for {nws_lid} removed.')
        logging.info(f'Filtered metadatada downloaded for {len(output_meta_list)} points.')

    # ----------
    # TODO: check that we have any records. Lots of ways it could be empty. Humm.... 

    # ----------
    if save_pickle_file is True and output_catfim_dir != "":
        meta_file = os.path.join(output_catfim_dir, "nwm_metafile.pkl")
        logging.info(f"Meta file saved at {meta_file}")

        # Overwrite it even though it is not saved at the huc level
        with open(meta_file, "wb") as p_handle:
            pickle.dump(output_meta_list, p_handle, protocol=pickle.HIGHEST_PROTOCOL)

    return output_meta_list


def load_fim_global_env_values(env_file):
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


def load_restricted_sites(catfim_type):
    """
    Previously, only stage based used this. It is now being used by stage-based and flow-based (1/24/25)

    The 'catfim_type' column can have three different values: 'stage', 'flow', and 'both'. This determines
    whether the site should be filtered out for stage-based CatFIM, flow-based CatFIM, or both of them.

    Returns: a dataframe for the restricted lid and the reason why:
        'nws_lid', 'restricted_reason'
    """

    file_name = "ahps_restricted_sites.csv"
    current_script_folder = os.path.dirname(__file__)
    file_path = os.path.join(current_script_folder, file_name)

    df_restricted_sites = pd.read_csv(file_path, dtype=str)

    df_restricted_sites['nws_lid'].fillna("", inplace=True)
    df_restricted_sites['restricted_reason'].fillna("", inplace=True)
    df_restricted_sites['catfim_type'].fillna("", inplace=True)

    # remove extra empty spaces on either side of all cellls
    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.strip()
    df_restricted_sites['restricted_reason'] = df_restricted_sites['restricted_reason'].str.strip()
    df_restricted_sites['catfim_type'] = df_restricted_sites['catfim_type'].str.strip()

    # Need to drop the comment lines before doing any more processing
    df_restricted_sites.drop(
        df_restricted_sites[df_restricted_sites.nws_lid.str.startswith("#")].index, inplace=True
    )

    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.upper()

    # Clean up dataframe
    for ind, row in df_restricted_sites.iterrows():
        nws_lid = row['nws_lid']
        restricted_reason = row['restricted_reason']

        if restricted_reason == "":
            restricted_reason = "From the ahps_restricted_sites,"
            " the site will not be mapped, but a reason has not be provided."
            df_restricted_sites.at[ind, 'restricted_reason'] = restricted_reason

            # FLOG.warning(f"{restricted_reason}. Lid is '{nws_lid}'")            
            # Humm.. how do we log this? screen is ok, but log isn't (MP versus non MP)
            # can we try just using the "logging" instance? Let's try it and see what happens
            logging.warning(f"{restricted_reason}. Lid is '{nws_lid}'")     
                        
        continue
    # end loop

    # Filter df_restricted_sites by CatFIM type
    if catfim_type == 'sb':  # Keep rows where 'catfim_type' is either 'stage' or 'both'
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['stage', 'both'])]

    else:
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['flow', 'both'])]

    # Remove catfim_type column
    df_restricted_sites.drop('catfim_type', axis=1, inplace=True)

    return df_restricted_sites

