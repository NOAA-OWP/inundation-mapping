#!/usr/bin/env python3

import logging
import os
import random
import shutil
import time
import pickle

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

# Global vars, shared by all related py files.
MAGNITUDES_TYPES = ['action', 'minor', 'moderate', 'major', 'record']

MAX_STAGE_THRESHOLD = 250

def load_fim_global_env_values(env_file):
    '''
    Loads environment variables from a .env file.
    Expects the .env file to contain API_BASE_URL

    Arguments
    ---------
    env_file - STR
        Path to the .env file.

    Returns
    -------
    api_base_url - STR
        Base URL for the WRDS API.
    '''
    if os.path.exists(env_file) == False:
        raise Exception(f"The environment file of {env_file} does not seem to exist")

    load_dotenv(env_file)
    # import variables from .env file
    api_base_url = os.getenv("API_BASE_URL")

    # At this point, we only have one value to return.
    return api_base_url


def get_huc_metadata(huc, huc_path):
    '''
    Load already-downloaded metadata for a specific HUC.

    Arguments
    ---------
    huc - STR
        The HUC number.
    huc_path - STR
        The path to the HUC directory.

    Returns
    -------
    metadata_json_list - LIST
        List of metadata JSON objects (filtered to the HUC).
    huc_sites_gdf - Geopandas GeoDataFrame
        A GDF of the same metadata (filtered to the site list for the HUC).

    '''

    # Get filepaths for the metadata
    nwm_meta_file = os.getenv('NWM_METAFILE_PATH')
    nwm_sites_path = os.getenv('NWM_SITES_PATH')

    # Check input filepaths
    if not os.path.isfile(nwm_meta_file):
        raise FileNotFoundError(f"Error: Expected metafile at {nwm_meta_file}")

    if not os.path.isfile(nwm_sites_path):
        raise FileNotFoundError(f"Error: Expected NWM sites parquet at {nwm_sites_path}")

    # A bit of start staggering to help not overload the MP (0.1 milliseconds to 2 secs)
    time_delay_mms = random.randint(100, 2000) / 1000
    time.sleep(time_delay_mms)

    # -----
    # Copy metadata over to HUC path, then read it in

    # Set source metadata file and make a new filepath in the HUC folder
    src_nwm_meta_file = nwm_meta_file
    meta_file_name = os.path.basename(nwm_meta_file)
    nwm_meta_file = os.path.join(huc_path, meta_file_name)
    shutil.copyfile(src_nwm_meta_file, nwm_meta_file)

    # Read metadata file
    with open(nwm_meta_file, "rb") as p_handle:
        metadata_json_list = pickle.load(p_handle)

    # -----
    # Copy the NWM sites file over to HUC path, then read it in and format it

    # Set source nwm sites file and make a new filepath in the HUC folder
    src_nwm_sites_path = nwm_sites_path
    nwm_sites_name = os.path.basename(nwm_sites_path)
    nwm_sites_path = os.path.join(huc_path, nwm_sites_name)
    shutil.copyfile(src_nwm_sites_path, nwm_sites_path)

    # Read NWM sites file
    all_sites_gdf = gpd.read_parquet(nwm_sites_path)

    if len(all_sites_gdf) == 0:
        msg = f"Sites table empty from filepath {nwm_sites_path}"
        logging.critical(msg)
        raise Exception()

    # Filter sites table to the given HUC
    huc_sites_gdf = all_sites_gdf[all_sites_gdf['HUC8'] == huc].copy()

    if len(huc_sites_gdf) == 0:
        msg = f"Sites table empty after filtering to HUC {huc}"
        logging.error(msg)

    # There appears to be actual column named "index" at this point, remove it
    huc_sites_gdf.reset_index(drop=True, inplace=True)
    huc_sites_gdf.rename(columns={"identifiers_nws_lid": "nws_lid"}, inplace=True)

    # -----
    # Get the site list and use it to filter the metadata JSON list

    # Keep everyhing uppercase for processing as the json files are uppercase for that file
    huc_sites_gdf['nws_lid'] = huc_sites_gdf['nws_lid'].str.upper()

    # Now that we have a list of HUCs to lids from the geoparquet, given to use from generate_categorical_fim.
    # we can filter the meta_json_list down
    nwm_lids = huc_sites_gdf['nws_lid'].tolist()

    # Find lid metadata from master list of metadata dictionaries (line 66).
    huc_metadata_json_list = []
    for lid_site_data in metadata_json_list:
        lid = lid_site_data['identifiers']['nws_lid']
        if lid in nwm_lids:
            huc_metadata_json_list.append(lid_site_data)

    if len(huc_metadata_json_list) == 0:
        msg = f"Metadata JSON list empty after filtering to lid list {nwm_lids} for HUC {huc}"
        logging.error(msg)

    # -----
    # Clean up: Remove the copied-over files once we've read this data in

    os.remove(nwm_meta_file)
    os.remove(nwm_sites_path)

    return huc_metadata_json_list, huc_sites_gdf


def check_for_restricted_sites(sites_gdf, catfim_type):
    '''
    Checks for restricted sites and updates the sites GeoDataFrame accordingly.

    Compares the provided sites GeoDataFrame against a list of restricted sites
    loaded from a CSV file. It updates the 'status' and 'mapped' columns of
    the GeoDataFrame for any restricted sites and returns a list of valid NWM LIDs.

    Arguments
    ----------
    sites_gdf - geopandas.GeoDataFrame
        A GeoDataFrame containing site information with columns such as 'nws_lid'.
    catfim_type - str
        The type of CATFIM processing, 'sb' or 'fb'.

    Note: Previously had huc and sites_file_path also as inputs, removed 1/13/26 because they weren't used

    Returns
    -------
    valid_nwm_lids - list
        A list of valid NWM LIDs after excluding restricted sites.
    sites_gdf - geopandas.GeoDataFrame
        The updated GeoDataFrame with restricted sites marked accordingly.

    Notes
    -----

    meta_gdf is likely pretty small by now, only sites for this HUC
    Likely a smarter way to do this as well.. lambda? Could do a join but we have
    dup column names we would have to cleanup.

    '''

    # Load restricted sites for the given catfim_type
    df_restricted_sites = load_restricted_sites(catfim_type)

    # Check whether LIDs are in restricted sites list, update sites_gdf accordingly
    valid_nwm_lids = []
    for index, row in sites_gdf.iterrows():
        lid = row["nws_lid"].upper()
        is_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]

        if len(is_restrict_lid) > 0:
            # what if it comes back with more than one? if so.. it is a bug in the list

            # sites_gdf.at[index, "status"] = restricted_reason # TODO: clean up after testing
            # sites_gdf.at[index, "mapped"] = "no"

            restricted_reason = is_restrict_lid.iloc[0]['restricted_reason']
            sites_gdf = update_line_status_or_warning(lid, sites_gdf, restricted_reason, set_mapped_to_no = True)

        else:
            valid_nwm_lids.append(lid)

    return valid_nwm_lids, sites_gdf


def load_restricted_sites(catfim_type):
    '''
    Reads and interprets the ahps_restricted_sites.csv (from the inundation_mapping repo) to
    return a list of restricted sites specific to the given CatFIM type (SB or FB).

    The 'catfim_type' column in the CSV can have three different values: 'stage', 'flow', and 'both.'
    This determines whether the site should be filtered out for SB, FB, or both.

    We used to require that the LID was 5 characters, but we removed that requirement in Fall 2025
    because there actually are a few LIDs that might be valid but aren't 5 chars. And if they're
    invalid, we will filter elsewhere.

    Arguments
    ---------
    catfim_type - STR
        'sb' or 'fb'

    Returns
    -------
    df_restricted_sites - Pandas DataFrame
        Dataframe containing the restricted lids and the reasons why (columns are 'nws_lid', 'restricted_reason')
    '''

    file_name = "ahps_restricted_sites.csv"
    current_script_folder = os.path.dirname(__file__)
    file_path = os.path.join(current_script_folder, file_name)

    df_restricted_sites = pd.read_csv(file_path, dtype=str)

    df_restricted_sites['nws_lid'].fillna("", inplace=True)
    df_restricted_sites['restricted_reason'].fillna("", inplace=True)
    df_restricted_sites['catfim_type'].fillna("", inplace=True)

    # Remove extra empty spaces on either side of all cellls
    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.strip()
    df_restricted_sites['restricted_reason'] = df_restricted_sites['restricted_reason'].str.strip()
    df_restricted_sites['catfim_type'] = df_restricted_sites['catfim_type'].str.strip()

    # Need to drop the comment lines before doing any more processing
    df_restricted_sites.drop(
        df_restricted_sites[df_restricted_sites.nws_lid.str.startswith("#")].index, inplace=True
    )

    # Filter df_restricted_sites by CatFIM type
    if catfim_type == 'sb':  # Keep rows where 'catfim_type' is either 'stage' or 'both'
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['stage', 'both'])]
    else:
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['flow', 'both'])]

    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.upper()

    # Clean up dataframe
    for ind, row in df_restricted_sites.iterrows():
        nws_lid = row['nws_lid']
        restricted_reason = row['restricted_reason']

        if restricted_reason == "":
            restricted_reason = "From the ahps_restricted_sites,"
            " the site will not be mapped, but a reason has not be provided."
            df_restricted_sites.at[ind, 'restricted_reason'] = "Restricted Site - " + restricted_reason

            logging.warning(f"{restricted_reason}. Lid is '{nws_lid}'")
            # TODO: Test that this actually makes it into the warnings log
        continue

    # Remove catfim_type column
    df_restricted_sites = df_restricted_sites.drop('catfim_type', axis=1)

    return df_restricted_sites


def load_runtime_args(output_folder):
    '''
    Loads CatFIM run arguments from the runtime_args.env file.

    Variables loaded into memory (example):
        CATFIM_TYPE=fb
        ENV_FILE="/data/config/fim_enviro_values.env"
        SEARCH=5
        NWM_METAFILE_PATH=""
        NWM_SITES_PATH=""
        GET_NEW_META_DATA=False
        THRESHOLD_FILE_PATH=""
        GET_NEW_THRESHOLD_DATA=False
        FIM_RUN_DIR="/data/previous_fim/hand_4_8_7_2"
        PAST_MAJOR_INTERVAL_CAP=5

    Arguments
    ---------
    output_folder - STR
        CatFIM output folder filepath.

    TODO: Let's change GET_NEW_META_DATA and GET_NEW_THRESHOLD_DATA to true booleans
    '''
    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)

    if not os.path.isfile(args_file):
        raise ValueError(f"Unable to find the runtime_args.env at {output_folder}")

    # Use load_env to pull out just the variables it needs.
    load_dotenv(args_file)

    return


def make_huc_mapping_filepaths(huc, catfim_type, huc_path):
    '''
    Used for both flow- and stage-based CatFIM.

    Used in catfim_process_huc.py and in the command line functioning of
    generate_categorical_fim_mapping.py.

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code (i.e. '12090301').
    catfim_type - STR
        'FB' or 'SB'
    huc_path - STR
        Filepath to the HUC-specific CatFIM outputs.

    Returns
    -------
    output_mapping_dir - STR
        Filepath to the HUC-specific CatFIM mapping directory.
    output_temp_dir - STR
        Filepath to the HUC-specific CatFIM temp directory.
    log_file_dir - STR
        Filepath to the HUC-specific CatFIM log directory.
    sites_pre_mapping_file_path - STR
        Filepath to the pre-mapping sites file.
    sites_post_mapping_file_path - STR
        Filepath to the post-mapping sites file.
    library_pre_mapping_file_path - STR
        Filepath to the pre-mapping library file.
    library_post_mapping_file_path - STR
        Filepath to the post-mapping library file.

    '''
    # Make the CatFIM type label
    catfim_type_label = None
    if catfim_type == 'fb':
        catfim_type_label = 'flow_based'
    elif catfim_type == 'sb':
        catfim_type_label = 'stage_based'

    # Create filepaths for mapping and temp folders
    output_mapping_dir = os.path.join(huc_path, "mapping")
    output_temp_dir = os.path.join(huc_path, "temp")
    log_file_dir = os.path.join(huc_path, "logs")

    # Create filepaths for the sites and library Geopackages
    sites_pre_mapping_file_path = os.path.join(output_temp_dir, f"{catfim_type_label}_sites_pre_mapping_{huc}.gpkg")
    sites_post_mapping_file_path = os.path.join(output_mapping_dir, f"{catfim_type_label}_sites_{huc}.gpkg")

    # library_pre_inun_file_path changed to library_pre_mapping_file_path
    library_pre_mapping_file_path = os.path.join(output_temp_dir, f"{catfim_type_label}_library_pre_mapping_{huc}.csv")
    library_post_mapping_file_path = os.path.join(output_mapping_dir, f"{catfim_type_label}_library_{huc}.gpkg")

    return output_mapping_dir, output_temp_dir, log_file_dir, sites_pre_mapping_file_path, sites_post_mapping_file_path, library_pre_mapping_file_path, library_post_mapping_file_path

def finalize_sites_mapping_status(
    huc,
    catfim_type,
    sites_post_mapping_file_path,
    library_post_mapping_file_path,
    sites_input,
    library_input,
):
    '''
    Used in both stage- and flow-based CatFIM.
    Updates the mapping status and status messages for CatFIM sites based on the presence of valid inundation GeoPackage files.

    Function assumes that we should have only two values for mapped, either no, or "not set."

    If we have a value in the warning column and the mapped is 'not set', then copy messages to status, then mapped becomes 'Good'.
    If no value in warning, and mapped is 'not set', then status becomes 'Good'. # TODO: Confirm logic

    Arguments
    ---------
    huc - ST
        Hydrologic Unit Code (i.e. '12090301')
    catfim_type - STR
        'sb' or 'fb'
    sites_post_mapping_file_path - STR
        final huc-level filepath (if mapping was completed, will be the same path variable as sites_input)
    library_post_mapping_file_path - STR
        final huc-level filepath (if mapping was completed, will be the same path variable as library_input)
    sites_input - STR  GDF
        Either sites_gdf or the sites post mapping filepath (str)
    library_input - STR  None
        either None or library_post_mapping_file_path

    Example
    -------
    Syntax when it is used BEFORE mapping has completed:
        finalize_sites_mapping_status(
            huc,
            catfim_type,
            sites_post_mapping_file_path,
            library_post_mapping_file_path,
            sites_gdf,
            None,
        )

    Syntax when it is used AFTER mapping has completed:
        finalize_sites_mapping_status(
            huc,
            catfim_type,
            sites_post_mapping_file_path,
            library_post_mapping_file_path,
            sites_post_mapping_file_path,
            library_post_mapping_file_path,
        )

    CatFIM Reorg Jan 2026: TODO: Clean up

    This needs to be rethought.

      - Any time we deliminate all needs to make it to mapping, update the sites file.
      - What do we want to do if we have a catestrophic fail? do we rename the {huc}_sites.gpkg so
        it is not included in the final catfim post processing?
      - When we start mapping, a copy of the sites file will be made calling it someting like
        sites_pre_mapping.gpkg. While mapping is processing, it will never update the
        sites_pre_mapping so the mapping code can be run multiple time. However, when mapping
        is done, it will finish with a sites_post_mapping. It will contain any updated statuses.
      - When we come back from mapping, we check to see if the sites_post_mapping.gkpg and load it in
        which becomes our new final {huc}_sites.gpkg
      - Regardless when we hit __updated_sites.. (or a similar) function, we need to update that
        file only once for column renamed, status updates, etc.
      - But.. how do we handle it while go through all processing?  Do we have a different WIP
        sites file that we keep building on as the master?  Then when we get here we look figure
        out if we aborted / stopped prior to mapping, then use the most recent temp sites.gpkg
        as a starting point for the final?  Do we look for the post procesing version of it?
      - Do we try to do that all in this function? or do we break it up for something like a function
        called __sites_finalization?
      - Do we make sure mapping updates the sites_post_processing to update the "mapped" = yes
        and status = good?  Do we let it update the status column from the warnings column?
        Come to think of it.. that might be good. That way, in theory if we get here and we find
        some that are "not set", that might indicate a code fail.. .humm.... moo-ha-ha (lol)
        That is probably better than looking for the existiance of a library file.

    At a min... we need to plug in the mapping code to make it's own sites_post_mapping so this
    function can decide what to do with it.

    Note: We do have some last minute library finalization we will need. Humm... how does that
    work if we re-run mapping.  What about the last minute case change for the nws_lid column and
    rerun of mapping? hummm.

    Note: the nws_lid column will never get renamed here. Let catfim_post_processing rename
      those columns to ahps_lid when it gets there.
        
    '''

    logging.info(f"{huc} - Begin updating sites mapping status")

    huc_function_tag = f"{huc} - Update Sites Mapping Status -"

    # ------------------------------------
    # Validate site_inputs (can be a filepath or a GDF)

    if isinstance(sites_input, str):

        if not os.path.exists(sites_input):
            msg = f'{huc_function_tag} Unable to finalize HUC, no file exists at sites filepath: {sites_input}'
            logging.error(msg)  # TODO: Is critical + exception correct here? maybe not... since other HUCs still might've been sucessful
            raise Exception(msg)

        # Read in sites_input as a gdf
        logging.info(f"{huc_function_tag} Finalizing sites_input from path {sites_input}") # TEMP DEBUG
        sites_gdf = gpd.read_file(sites_input, engine='fiona')

    elif isinstance(sites_input, gpd.GeoDataFrame):
        logging.info(f"{huc_function_tag} sites_input is a GeoDataFrame") # TEMP DEBUG
        sites_gdf = sites_input

    else:
        # Error out if sites_input is not string or a gdf
        msg = f"{huc_function_tag} Unable to finalize HUC, sites_input is not a GDF or string."
        logging.error(msg)
        raise Exception(msg)

    # Once sites_gdf has been created, check that it has stuff in it
    if len(sites_gdf) == 0:
        msg = f"{huc_function_tag} Unable to finalize HUC, sites_gdf is empty."
        logging.error(msg)
        raise Exception(msg)

    # ------------------------------------
    # Update mapping status in sites_gdf (the only sites that should be updated here are the unmapped sites,
    # the mapped sites should have an updated status from the end of post_process_huc_mapping)

    # Initialize values
    if library_input == None: library_input = '' # change none to an empty string 
    mapped_sites_list, huc_library_gdf = [], []
    mapping_completed = False

    if os.path.exists(library_input):
        # Read in the HUC library and get a list of sites that have at least one mapped geometry
        huc_library_gdf = gpd.read_file(library_input, engine='fiona') 
        huc_library_gdf['nws_lid'] = huc_library_gdf['nws_lid'].str.lower() # TODO: Make this consistent earlier in the process?
        sites_with_valid_geoms_gdf = huc_library_gdf[~huc_library_gdf.geometry.is_empty & huc_library_gdf.geometry.notna()]
        mapped_sites_list = sites_with_valid_geoms_gdf['nws_lid'].unique().tolist()

        if len(mapped_sites_list) != 0:
            mapping_completed = True

    if not mapping_completed:
        logging.warning(f"{huc} has no mapped sites. It is possible that a HUC-level error occurred that prevented mapping from finishing processing.")

    # Set a consistent site status error message
    site_status_error_message = 'Possible site status error occurred during processing.'

    # Make sure the lid columns are the same case for the comparison (lowercase in both)
    sites_gdf['nws_lid'] = sites_gdf['nws_lid'].str.lower() # TODO: Make this consistent earlier in the process?

    # Update mapping status in the sites gdf
    for index, row in sites_gdf.iterrows():
        lid = row["nws_lid"]
        lid_mapped = row["mapped"]
        lid_status = row["status"]
        lid_warning = row["warnings"]

        lid_status_new, lid_mapped_new = None, None

        # Create the new mapped and status values for this site based on whether it is in the mapped sites list
        if lid in mapped_sites_list:  # If site is in mapped list, update mapped to "yes" and update status if it is not already
            # If we have mapped sites, we know mapping_completed is True so we don't need to check for it

            lid_mapped_new = "yes"

            if lid_status != "not set": # Status val available (possible error)
                lid_status_new = f"{lid_status}; {site_status_error_message}"
                logging.error(f"{huc_function_tag} {lid} is mapped but had a status before post-processing. {site_status_error_message}")

            elif lid_warning != "": # No status val available, but warning val IS available
                lid_status_new = lid_warning

            elif lid_warning == "": # No val for status or warning, inundation completed
                lid_status_new = "Good"

            else:  # Uncaught variant - status not available, warning not available (possible error)
                lid_status_new = f"Good ; {site_status_error_message}"
                logging.error(f"{huc_function_tag} {lid} Site is mapped but site status/warning vals are abnormal; {site_status_error_message}")
                logging.error(f"{huc_function_tag} {lid} Status: {lid_status}, Warning: {lid_warning}")

        else: # If site is not in mapped list, update mapped to "no" if it is not already and update status

            lid_mapped_new = "no"

            if lid_mapped == "yes": # Likely an error if mapped says 'yes' but we have no geoms for it.
                logging.error(f"{huc_function_tag} {lid} is not mapped but had a mapped val of 'yes' before post-processing; {site_status_error_message}")
                logging.error(f"{huc_function_tag} {lid} Status: {lid_status}, Warning: {lid_warning}")

                if lid_status != "not set": # Status val available
                    lid_status_new = f"{lid_status}; {site_status_error_message}"

                elif lid_warning != "": # No status val available, but warning val IS available
                    lid_status_new = f"{lid_warning}; {site_status_error_message}"

                elif mapping_completed is True: # No val for status or warning, inundation completed
                    lid_status_new = f"Mapping completed but no inundated files found. Site had mapped val of 'yes' before post-processing. {site_status_error_message}"

                else: # No val for status or warning, inundation not completed
                    lid_status_new = f"Mapping not completed, no inundated files found. Site had mapped val of 'yes' before post-processing. {site_status_error_message}"

            elif lid_mapped == "no":  # Expected behavior

                if lid_status != "not set": # Status val available (this is the expected behavior)
                    lid_status_new = lid_status

                elif lid_warning != "": # No status val available, but warning val IS available (indicates possible error)
                    lid_status_new = f"{lid_warning}; {site_status_error_message}"
                    logging.error(f"{huc_function_tag} {lid} had a mapped value of 'no' but no site status before post-processing. {site_status_error_message}")

                elif mapping_completed is True: # No val for status or warning, inundation completed (indicates possible error)
                    lid_status_new = f"Mapping completed but no inundated files found; {site_status_error_message}"
                    logging.error(f"{huc_function_tag} {lid} had a mapped value of no but no site status or warnings before post-processing. Mapping completed but no inundated files found. {site_status_error_message}")

                else:  # No val for status or warning, inundation not completed (indicates possible error)
                    lid_status_new = f"Mapping not completed, inundated polygons ; {site_status_error_message}"
                    logging.error(f"{huc_function_tag} {lid} had a mapped value of no but no site status or warnings before post-processing. Mapping not completed, no inundated files found. {site_status_error_message}")

            elif lid_mapped == "not set":  # Expected behavior

                if lid_status != "not set": # Status val available (possible error)
                    lid_status_new = f"{lid_status}; {site_status_error_message}"
                    logging.error(f"{huc_function_tag} {lid} is not mapped but had a status before post-processing. {site_status_error_message}")

                elif lid_warning != "": # No status val available, but warning val IS available (Expected behavior)
                    lid_status_new = lid_warning

                elif mapping_completed is True: # No val for status or warning, inundation completed
                    lid_status_new = "Mapping completed but no inundated files found"

                else: # No val for status or warning, inundation not completed
                    lid_status_new = "Mapping not completed, no inundated files found"

            else: # If status is not 'not set' 'no' or 'yes' at this stage then something weird happened... Unlikely, indicates an error
                msg = f"{huc_function_tag} {lid} Site had an irregular mapped value of '{lid_mapped}' before post-processing. {site_status_error_message}"
                logging.error(msg)
                raise Exception(msg)

        # Error out if either of the new output vals are empty (very unlikely)
        if None in (lid_status_new, lid_mapped_new) or "" in (lid_status_new, lid_mapped_new):
            msg = f"{huc_function_tag} {lid} - lid_status_new and/or lid_mapped_new is None or empty at the end of finalize_sites_mapping_status. lid_mapped_new: {lid_mapped_new}; lid_status_new: {lid_status_new}"
            logging.error(msg)
            raise Exception(msg)

        # Update the sites gdf with the new mapped and status values for this site
        sites_gdf.at[index, "status"] = lid_status_new
        sites_gdf.at[index, "mapped"] = lid_mapped_new
    # End of literating sites gdf loop

    # At this point, we should have a sites_gdf that has updated values for the 'mapped' and 'status' columns

    # ------------------------------------
    # Update any other sites columns that are needed

    # Re-insert the geometry column at the end
    geom = sites_gdf.pop(sites_gdf.geometry.name)
    sites_gdf[geom.name] = geom
    sites_gdf = sites_gdf.set_geometry(geom.name)

    # Rename columns in the sites GDF, then drop any unnecessary or confusing columns
    sites_gdf = rename_output_columns(sites_gdf)
    sites_gdf = drop_output_columns(sites_gdf, catfim_type)

    # Save updated sites GDF
    logging.info(f"{huc_function_tag} Saving updated HUC sites GDF to {sites_post_mapping_file_path}")
    sites_gdf.to_file(sites_post_mapping_file_path, driver='GPKG', engine="fiona", index=False)

    # ------------------------------------
    # Process HUC library if it is available

    # Exit the function here if there is no library input, if the library GDF is empty, or if the path is faulty
    if len(huc_library_gdf) == 0:
        logging.info(f"{huc_function_tag} No library input available, no final library gdf will be saved to {library_post_mapping_file_path}")
        return

    # if library_input == None: # TODO: Clean up
    # if len(huc_library_gdf) == 0:
    #     msg = f"{huc_function_tag} Unable to finalize HUC library, library table is empty at {library_input}"
    #     logging.error(msg)
    #     return

    # if not os.path.exists(library_input):
    #     msg = f"{huc_function_tag} Unable to finalize HUC library, no file exists at: {library_input}"
    #     logging.error(msg)
    #     return

    # Otherwise, HUC library should already be read in as huc_library_gdf
    logging.info(f"{huc_function_tag} Processing library path {library_input}")

    # Rename columns in the library GDF, then drop any unnecessary or confusing columns
    huc_library_gdf = rename_output_columns(huc_library_gdf)  # nws_lid is now ahps_lid
    huc_library_gdf = drop_output_columns(huc_library_gdf, catfim_type)

    # Library needs the following metadata from the sites GDF: wfo, rfc, state, county, and status
    # Note: We are merging using the newly renamed columns!!!
    huc_library_gdf = pd.merge(
        huc_library_gdf,
        sites_gdf[['ahps_lid', 'wfo', 'rfc', 'state', 'county', 'status']],
        on='ahps_lid',
        how='left'
    )

    # TODO: For stage-based, do we want to specify what the elevation source was?
    # -> that exists already, it's s_src (can rename if needed)

    # Re-insert the geometry column at the end
    geom = huc_library_gdf.pop(huc_library_gdf.geometry.name)
    huc_library_gdf[geom.name] = geom
    huc_library_gdf = huc_library_gdf.set_geometry(geom.name)

    # Save updated library gdf here
    logging.info(f"{huc_function_tag} Saving updated HUC library to {library_post_mapping_file_path}")
    huc_library_gdf.to_file(library_post_mapping_file_path, driver='GPKG', engine="fiona", index=False)

    return

def feet_to_meters(feet):
    '''
    Converts feet to meters.

    Arguments
    ---------
    feet - FLOAT
        Value in feet to be converted to meters.

    Returns
    -------
    meters - FLOAT
        Converted value in meters.
    '''
    meters = feet * 0.3048
    return meters

def get_huc_output_folder(huc, output_folder):
    '''
    Validates the output folder path and returns the normalized output folder path.

    Arguments
    ---------
    huc - STR
        HUC identifier for the hydrologic unit code.
    output_folder - STR
        Root output folder path where HUC subdirectories are stored.

    Returns
    -------
    huc_path - STR
        Full path to the HUC subfolder in output directory.
    output_folder - STR
        Normalized output folder path (trailing slashes removed).

    '''

    # Valdiate HUC value (must be a string with 8 numeric digits and start with 0, 1, or 2)
    if not (isinstance(huc, str) and len(huc) == 8 and huc.isdigit() and huc[0] in ['0', '1', '2']):
        raise ValueError(f"HUC must be an 8-digit string starting with 0, 1, or 2. Received: {huc}")

    # Check that the output folder was provided
    if not output_folder or output_folder == "":
        raise ValueError("output_folder argument can not be None or empty.")
    
    # If applicable, take slash off filepath end
    if output_folder.endswith("/"):  # strip it off the end
        output_folder = output_folder[:-1]

    # If applicable, strip HUCs subfolder off filepath end (for now, temporarily)
    if output_folder.endswith("hucs"):
        output_folder = output_folder[:-4]
        # output_folder path goes from '/dir/output_path/hucs' to '/dir/output_path/'

    # Check that the output folder exists
    if not os.path.exists(
        output_folder
    ):  # the hucs subfolder may/may not exist but the root output folder must
        raise ValueError(
            f"output_folder of {output_folder} does not exist. Please check pathing including case."
        )

    # No need to validate any of the runtime_args as they were validated when it was created. (likely)

    # Make HUC path (ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301)
    huc_path = os.path.join(output_folder, "hucs", huc)

    return huc_path, output_folder


def drop_output_columns(df, catfim_type):
    '''
    Rename the CatFIM output column names.

    Arguments
    ---------
    df - Pandas Dataframe
        Dataframe or geodataframe that potentially needs column renaming.
    catfim_type - STR
        'sb' or 'fb'

    Returns
    -------
    df_new - Pandas DataFrame or GeoDataFrame
        Dataframe or geodataframe with removed columns.
    
    '''
    df_new = df.copy()

    # List of columns to remove for both stage-based AND flow-based CatFIM
    cols_to_remove = [ # TODO: Decide what columns should be removed
        'warnings',
        'env_can_gage_data_name',	
        'env_can_gage_data_latitude',	
        'env_can_gage_data_longitude',	
        'env_can_gage_data_map_link',
        'env_can_gage_data_drainage_area',
        'env_can_gage_data_contrib_drainage_area',
        'env_can_gage_data_water_course',
        'level_0'
    ]

    # List of columns to only remove for fb or sb CatFIM
    fb_specific_cols_to_remove = ['interval_stage', 'is_interval']
    sb_specific_cols_to_remove = []

    # Assemble column list
    if catfim_type == 'fb':
        cols_to_remove += fb_specific_cols_to_remove
    elif catfim_type == 'sb':
        cols_to_remove += sb_specific_cols_to_remove
    else:
        msg = f'Expected value of sb or fb for catfim_type. Received value of {catfim_type} instead. Unable to drop columns.'
        logging.error(msg)
        raise Exception(msg)

    # Remove listed columns if they are found
    for colname in cols_to_remove:
        if colname in df_new.columns:
            df_new.drop(columns=[colname], errors='ignore', inplace=True)

    return df_new

def rename_output_columns(df):
    '''
    Rename the CatFIM output column names.

    Arguments
    ---------
    df - Pandas Dataframe or Geopandas GeoDataFrame
        Dataframe or geodataframe that potentially needs column renaming.

    Returns
    -------
    df_new - Pandas DataFrame or GeoDataFrame
        Dataframe or geodataframe with renamed columns.
    
    '''

    df_new = df.copy()

    # TODO: Decide if we want to have uniform column names between sites and library 
    # or if we want to keep with previous patterns of having different colnames

    # Dictionary of old name and new names for the final outputs
    column_rename_dictionary = {
        # 'old_name': 'new_name',
        # 'identifiers_nwm_feature_id': 'nwm_seg', # TODO: Decide if we want these renamed?
        # 'identifiers_usgs_site_code': 'usgs_gage',
        'nws_lid': 'ahps_lid',
        'huc': 'huc8',
        'HUC8': 'huc8',
        'nws_data_wfo': 'wfo',
        'nws_data_rfc': 'rfc',
        'nws_data_state': 'state',
        'nws_data_county': 'county',
        'wrds_timestamp': 'wrds_time',
        'nrldb_timestamp': 'nrldb_time',
        'nwis_timestamp': 'nwis_time',
        # 'rfc_stage': '', # TODO: Decide which of these SB library columns I want to rename
        # 'datum_adj_ft': '',
        # 'datum_adj_wse_ft':'',
        # 'datum_adj_wse_m': '',
        # 'lid_alt_ft': '',
        # 'lid_alt_m': '',
        # 'is_interval': '',
        # 'interval_stage': '',
        # 'lid_usgs_elev': '',
        # 'datum_adj_ft': 'dtm_adj_ft', # TODO: Decide. Removed the SB column name change for now because they're out of date and less clear than the current output names
                                        # These will be the col names if we don't make this change: datum_adj_ft, datum_adj_wse_ft, datum_adj_wse_m
        # 'dadj_w_ft': 'datum_adj_wse_ft',
        # 'dadj_w_m': 'dadj_w_m',
        # TODO: Fill in the new column names after they've been decided on
    }

    # Iterate down the colname dictionary and rename whichever ones are there
    for old_name, new_name in column_rename_dictionary.items():
        if old_name in df_new.columns:
            df_new.rename(columns={old_name: new_name}, inplace=True)

            # logging.info(f"Renamed column '{old_name}' to '{new_name}' in the output dataframe.")  # TEMP DEBUG

    return df_new


def update_line_status_or_warning(site, sites_gdf, message, set_mapped_to_no):
    '''
    Updates the status, warnings, and mapped columns in the sites_gdf for a specific site.

    Enforces the rule that we set the status only when we are changing the mapped column.

    Appends the message to the 'warnings' column if we are not changing the mapped status.

    Arguments
    ---------

    site - str
        Site ID or "all" if we want to update all sites in the HUC.
    sites_gdf - Geopandas GeoDataFrame
        Table of sites.
    message - str
        Message that should be added to the sites_gdf table for the site
    set_mapped_to_no - bool
        Whether we are setting mapped to no (True) or not (False).

    Returns
    ------
    sites_gdf - Geopandas GeoDataFrame
        Table of sites, with the 'status', 'warnings', and 'mapped' columns updated.

    
    '''

    all_huc_sites = sites_gdf["nws_lid"].tolist()

    logging.info(f"Updating site(s): {site} with message: {message}")  # TEMP DEBUG? Verbose

    if site == "all":
        site_list = all_huc_sites
    else:
        site_list = [site]

    # Iterate sites and update the columns as needed
    for site_i in site_list:

        site_i = site_i.upper()  # Before post-processing, sites are uppercase

        if site_i not in all_huc_sites:
            logging.error(f"{site} -  - Updating sites table, site not in sites_gdf... unable to update table")
            return sites_gdf

        # Get initial values and print value changes
        mapped_i = sites_gdf.loc[sites_gdf["nws_lid"] == site_i, "mapped"].iloc[0]
        status_i = sites_gdf.loc[sites_gdf["nws_lid"] == site_i, "status"].iloc[0]
        warnings_i = sites_gdf.loc[sites_gdf["nws_lid"] == site_i, "warnings"].iloc[0]

        # Give a warning if mapped is already 'no', because that could indicate an error
        # Because we don't keep processing sites after mapped is 'no'
        if mapped_i == "no":
            logging.warning(f"{site} - Updating sites table, 'mapped' value is already 'no' for this site")

        if set_mapped_to_no is True:
            # We are unmapping the site, so the message gets put in the "status" column
            sites_gdf.loc[sites_gdf["nws_lid"] == site_i, ['mapped', 'status']] = ['no', message]

            logging.info(f"{site} - Updating sites table, changed mapped from '{mapped_i}' to 'no'")  # Verbose, maybe debug?
            logging.info(f"{site} - Updating sites table, changed status from '{status_i}' to '{message}'")  # Verbose, maybe debug?

        elif set_mapped_to_no is False:
            # We are NOT yet unmapping the site, so the messages gets put in the "warnings" column
            
            # Check whether there is already a warning
            prev_warning = sites_gdf.loc[sites_gdf["nws_lid"] == site_i, 'warnings'].item()

            # If there's no previous warning for the site, set this message as the warning
            if prev_warning == "":
                warning_message = message

            # If there is already a warning, add this message to the end of it
            elif prev_warning != "":
                warning_message = f"{prev_warning}; {message}"

            # Update the warnings column with the new message
            # (but we are NOT changing the mapped column here)
            sites_gdf.loc[sites_gdf["nws_lid"] == site_i, 'warnings'] = warning_message

            logging.info(f"{site} - Updating sites table, changed status from '{warnings_i}' to '{warning_message}'")  # Verbose, maybe debug?

        else:
            logging.error(f"Updating sites table, error updating line status/warnings. Invalid value for set_mapped_to_no: {set_mapped_to_no}")

    return sites_gdf


def validate_huc_inputs(huc, output_folder):
    '''
    Validate HUC input parameters and return normalized paths.

    Checks that:
        - HUC data is available in FIM_RUN_DIR
        - required directory structure (branches) exists for the HUC

    Arguments
    ---------
    huc - STR
        HUC identifier for the hydrologic unit code.
    output_folder - STR
        Root output folder path where HUC subdirectories are stored.

    Returns
    -------
    huc_path - STR
        Full path to the HUC subfolder in output directory.
    output_folder - STR
        Normalized output folder path (trailing slashes removed).

    Raises
    ------
    ValueError: If output_folder is None/empty or doesn't exist, or if FIM_RUN_DIR environment variable is not set.
    FileNotFoundError: If HUC path or branches directory doesn't exist in FIM_RUN_DIR.
    '''

    huc_path, output_folder = get_huc_output_folder(huc, output_folder)

    # Validate data exists in the fim_run_dir and it includes this HUC.
    # This may seem reduntant as it was checked (sort of) in generate_categorical_fim.py.
    # However, this one is HUC specific and it is possible that a HUC
    # can be run after generate_categorical_fim.py was run and add more HUC on the fly.
    # If this HUC did not previous exist or failed, you can re-run this script independantly
    # then run post processing again.
    fim_run_dir = os.getenv("FIM_RUN_DIR")  # Only works if you run from within catfim_process_huc.py
    if not fim_run_dir:
        raise ValueError(
            "The enviro value for FIM_RUN_DIR does not exist or is empty. It was loaded"
            " and included in the runtime_arg enviro file. Check pathing and variables."
        )

    # Validate that the FIM run path exists for the HUC
    fim_run_huc_path = os.path.join(fim_run_dir, huc)
    if not os.path.exists(fim_run_huc_path):
        raise FileNotFoundError(
            "This script needs to talk to its HUC in the fim_run_dir, but the folder"
            f" {fim_run_huc_path} does not exist. Please check pathing (with case) and fim run"
            " error logs or huc list."
        )

    # Validate that the branch dir exists and is not empty
    branch_dir = os.path.join(fim_run_huc_path, 'branches')
    if not os.path.exists(branch_dir):
        raise FileNotFoundError(
            "This script needs to talk to branches in its fim_run_dir / HUC in the fim_run_dir,"
            f" but the folder {branch_dir} does not exist. Please check pathing (with case) and"
            " it's error logs."
        )

    if not os.listdir(branch_dir):
        raise FileNotFoundError(
            f"The branches directory at {branch_dir} is empty."
            " Please check that FIM data has been properly generated for this HUC."
        )

    # No need to validate any of the runtime_args as they were validated when it was created. (likely)

    # Make HUC path (ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301)
    huc_path = os.path.join(output_folder, "hucs", huc)

    return huc_path, output_folder
