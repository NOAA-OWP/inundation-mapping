#!/usr/bin/env python3

import logging
import os
import pickle
import random
import shutil
import time

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv


# Global vars, shared by all related py files.
MAGNITUDES_TYPES = ['action', 'minor', 'moderate', 'major', 'record']

# Default search distance
DEFAULT_SEARCH = 5

# Max stage value (anything higher is probably WSE and treated as such)
MAX_STAGE_THRESHOLD = 250  # feet

# Max allowable difference between HAND DEM elevation and APHS site elevation
MAX_ELEV_DIFFERENCE_THRESHOLD = 10  # meters

# A NoData value for the datum adjustment, used in multiple functions across the codebase
ELEV_NODATA_VALUE = -9999.0

# A NoData value for the threshold values
THRESH_NODATA_VALUE = -1

# Order of first columns for the sites and library output files
OUTPUT_COLUMN_ORDER = ['ahps_lid', 'huc8', 'mapped', 'status', 'wfo', 'rfc', 'state', 'county']

# Dictionary of old name and new names for the sites and library output files (Format: 'old_name': 'new_name')
COLUMN_RENAME_DICT = {
    'nws_lid': 'ahps_lid',
    'huc': 'huc8',
    'HUC8': 'huc8',
    's_src': 'stage_src',
    'lid_alt_ft': 'gage_zero_elev_ft',
    'lid_usgs_elev': 'hand_dem_elev_ft',
    'nws_data_wfo': 'wfo',
    'nws_data_rfc': 'rfc',
    'nws_data_state': 'state',
    'nws_data_county': 'county',
    'wrds_timestamp': 'wrds_time',
    'nrldb_timestamp': 'nrldb_time',
    'nwis_timestamp': 'nwis_time',
    'identifiers_nwm_feature_id': 'nwm_seg',
    'identifiers_usgs_site_code': 'usgs_gage',
    'crosswalk_datasets_location_nwm_crosswalk_dataset_location_nwm_crosswalk_dataset_id': 'location_nwm_crosswalk_dataset_id',
    'crosswalk_datasets_location_nwm_crosswalk_dataset_name': 'location_nwm_crosswalk_dataset_name',
    'crosswalk_datasets_location_nwm_crosswalk_dataset_description': 'location_nwm_crosswalk_dataset_description',
    'crosswalk_datasets_nws_usgs_crosswalk_dataset_nws_usgs_crosswalk_dataset_id': 'nws_usgs_crosswalk_dataset_id',
    'crosswalk_datasets_nws_usgs_crosswalk_dataset_name': 'nws_usgs_crosswalk_dataset_name',
    'crosswalk_datasets_nws_usgs_crosswalk_dataset_description': 'nws_usgs_crosswalk_dataset_description',
    # 'rfc_stage': '',
    # 'datum_adj_ft': 'dtm_adj_ft',
    # 'dadj_w_ft': 'datum_adj_wse_ft',
    # 'dadj_w_m': 'dadj_w_m',
    # TODO: Decide. Removed the SB column name change for now because they're out of date and less clear than the current output names
    # These will be the col names if we don't make this change: datum_adj_ft, datum_adj_wse_ft, datum_adj_wse_m
}


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
    huc_metadata_json_list = []
    huc_sites_gdf = gpd.GeoDataFrame()

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
        msg = f"{huc} - Sites table empty after filtering to HUC"
        logging.warning(msg)

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

    if len(nwm_lids) > 0:

        for lid_site_data in metadata_json_list:
            lid = lid_site_data['identifiers']['nws_lid']
            if lid in nwm_lids:
                huc_metadata_json_list.append(lid_site_data)

        if len(huc_metadata_json_list) == 0:
            msg = f"{huc} - Metadata JSON list empty after filtering to lid list {nwm_lids}"
            logging.error(msg)

    else:
        msg = f"{huc} - No valid LIDs found in lid list, skipping filtering metadata JSON"
        logging.warning(msg)

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
            restricted_reason = is_restrict_lid.iloc[0]['restricted_reason']
            sites_gdf = update_line_status_or_warning(
                lid, sites_gdf, restricted_reason, set_mapped_to_no=True
            )

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

    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].fillna("")
    df_restricted_sites['restricted_reason'] = df_restricted_sites['restricted_reason'].fillna("")
    df_restricted_sites['catfim_type'] = df_restricted_sites['catfim_type'].fillna("")

    # Remove extra empty spaces on either side of all cells
    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.strip()
    df_restricted_sites['restricted_reason'] = df_restricted_sites['restricted_reason'].str.strip()
    df_restricted_sites['catfim_type'] = df_restricted_sites['catfim_type'].str.strip()

    # Need to drop the comment lines before doing any more processing
    df_restricted_sites.drop(
        df_restricted_sites[df_restricted_sites.nws_lid.str.startswith("#")].index, inplace=True
    )

    # Filter df_restricted_sites by CatFIM type
    if catfim_type == 'sb':  # SB CatFIM: Keep rows where 'catfim_type' is either 'stage' or 'both'
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['stage', 'both'])]
    else:  # FB CatFIM: Keep rows where 'catfim_type' is either 'flow' or 'both'
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
    sites_pre_mapping_file_path = os.path.join(
        output_temp_dir, f"{catfim_type_label}_sites_pre_mapping_{huc}.gpkg"
    )
    sites_post_mapping_file_path = os.path.join(output_mapping_dir, f"{catfim_type_label}_sites_{huc}.gpkg")

    # library_pre_inun_file_path changed to library_pre_mapping_file_path
    library_pre_mapping_file_path = os.path.join(
        output_temp_dir, f"{catfim_type_label}_library_pre_mapping_{huc}.csv"
    )
    library_post_mapping_file_path = os.path.join(
        output_mapping_dir, f"{catfim_type_label}_library_{huc}.gpkg"
    )

    return (
        output_mapping_dir,
        output_temp_dir,
        log_file_dir,
        sites_pre_mapping_file_path,
        sites_post_mapping_file_path,
        library_pre_mapping_file_path,
        library_post_mapping_file_path,
    )


def finalize_sites_mapping_status(
    huc, catfim_type, sites_post_mapping_file_path, library_post_mapping_file_path, sites_input, library_input
):
    '''
    Used in both stage- and flow-based CatFIM.
    Updates the mapping status and status messages for CatFIM sites based on the presence of valid inundation GeoPackage files.

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

    '''

    logging.info(f"{huc} - Begin updating sites mapping status")

    huc_function_tag = f"{huc} - Update Sites Mapping Status -"

    # ------------------------------------
    # Validate site_inputs (can be a filepath or a GDF)

    if isinstance(sites_input, str):
        try:
            # Read in sites_input as a gdf
            with open(sites_input, "r"):
                sites_gdf = gpd.read_file(sites_input, engine='fiona')
            logging.info(f"{huc_function_tag} Finalizing sites_input from path {sites_input}")

        except FileNotFoundError as e:
            logging.error(
                f"{huc_function_tag} Unable to finalize HUC, sites GDF file does not exist at {sites_input}"
            )
            raise Exception(e)

        except Exception as e:
            # Fallback for any other unexpected errors
            logging.error(f"{huc_function_tag} Unable to finalize HUC, an unexpected error occurred: {e}")
            raise Exception(e)

    elif isinstance(sites_input, gpd.GeoDataFrame):
        logging.info(f"{huc_function_tag} sites_input is a GeoDataFrame")
        sites_gdf = sites_input

    else:
        # Error out if sites_input is not string or a gdf
        msg = f"{huc_function_tag} Unable to finalize HUC, sites_input is not a GDF or string."
        logging.error(msg)
        raise Exception(msg)

    # Once sites_gdf has been created, check that it has stuff in it
    if len(sites_gdf) == 0:
        msg = f"{huc_function_tag} Unable to finalize sites mapping status, sites_gdf is empty."
        logging.warning(msg)
        return

    # ------------------------------------
    # Update mapping status in sites_gdf (the only sites that should be updated here are the unmapped sites,
    # the mapped sites should have an updated status from the end of post_process_huc_mapping)

    # Initialize values
    if library_input is None:
        library_input = ''  # change none to an empty string
    mapped_sites_list, huc_library_gdf = [], []
    mapping_completed = False

    if os.path.exists(library_input):
        # Read in the HUC library and get a list of sites that have at least one mapped geometry
        huc_library_gdf = gpd.read_file(library_input, engine='fiona')
        huc_library_gdf['nws_lid'] = huc_library_gdf[
            'nws_lid'
        ].str.lower()  # TODO: Make this consistent earlier in the process?

        sites_with_valid_geoms_gdf = huc_library_gdf[
            ~huc_library_gdf.geometry.is_empty & huc_library_gdf.geometry.notna()
        ]

        mapped_sites_list = sites_with_valid_geoms_gdf['nws_lid'].unique().tolist()

        if len(mapped_sites_list) != 0:
            mapping_completed = True

    if mapping_completed is False:
        logging.warning(
            f"{huc} has no mapped sites. It is possible that a HUC-level error occurred that prevented mapping from finishing processing."
        )

    # Set a consistent site status error message
    site_status_error_message = 'Possible site status error occurred during processing.'

    # Make sure the lid columns are the same case for the comparison (lowercase in both)
    sites_gdf['nws_lid'] = sites_gdf[
        'nws_lid'
    ].str.lower()  # TODO: Make this consistent earlier in the process?

    # Update mapping status in the sites gdf
    for index, row in sites_gdf.iterrows():
        lid = row["nws_lid"]
        lid_mapped = row["mapped"]
        lid_status = row["status"]
        lid_warning = row["warnings"]

        lid_status_new, lid_mapped_new = None, None

        # Create the new mapped and status values for this site based on whether it is in the mapped sites list
        if lid in mapped_sites_list:
            # If site is in mapped list, update mapped to "yes" and update status if it is not already
            # If we have mapped sites, we know mapping_completed is True so we don't need to check for it

            lid_mapped_new = "yes"

            if lid_status != "not set":  # Status val available (possible error)
                lid_status_new = f"{lid_status}; {site_status_error_message}"
                logging.error(
                    f"{huc_function_tag} {lid} is mapped but had a status before post-processing. {site_status_error_message}"
                )

            elif lid_warning != "":  # No status val available, but warning val IS available
                lid_status_new = lid_warning

            elif lid_warning == "":  # No val for status or warning, inundation completed
                lid_status_new = "Good"

            else:  # Uncaught variant - status not available, warning not available (possible error)
                lid_status_new = f"Good ; {site_status_error_message}"
                logging.error(
                    f"{huc_function_tag} {lid} Site is mapped but site status/warning vals are abnormal; {site_status_error_message}"
                )
                logging.error(f"{huc_function_tag} {lid} Status: {lid_status}, Warning: {lid_warning}")

        else:  # If site is not in mapped list, update mapped to "no" if it is not already and update status

            lid_mapped_new = "no"

            if lid_mapped == "yes":  # Likely an error if mapped says 'yes' but we have no geoms for it.
                logging.error(
                    f"{huc_function_tag} {lid} is not mapped but had a mapped val of 'yes' before post-processing; {site_status_error_message}"
                )
                logging.error(f"{huc_function_tag} {lid} Status: {lid_status}, Warning: {lid_warning}")

                if lid_status != "not set":  # Status val available
                    lid_status_new = f"{lid_status}; {site_status_error_message}"

                elif lid_warning != "":  # No status val available, but warning val IS available
                    lid_status_new = f"{lid_warning}; {site_status_error_message}"

                elif mapping_completed is True:  # No val for status or warning, inundation completed
                    lid_status_new = f"Mapping completed but no inundated files found. Site had mapped val of 'yes' before post-processing. {site_status_error_message}"

                else:  # No val for status or warning, inundation not completed
                    lid_status_new = f"Mapping not completed, no inundated files found. Site had mapped val of 'yes' before post-processing. {site_status_error_message}"

            elif lid_mapped == "no":  # Expected behavior

                if lid_status != "not set":  # Status val available (this is the expected behavior)
                    lid_status_new = lid_status

                elif lid_warning != "":  # No status val, but warning val IS available (possible error)
                    lid_status_new = f"{lid_warning}; {site_status_error_message}"
                    logging.error(
                        f"{huc_function_tag} {lid} had a mapped value of 'no' but no site status before post-processing. {site_status_error_message}"
                    )

                elif mapping_completed is True:  # No status/warning, inundation completed (error?)
                    lid_status_new = (
                        f"Mapping completed but no inundated files found; {site_status_error_message}"
                    )
                    logging.error(
                        f"{huc_function_tag} {lid} had a mapped value of no but no site status or warnings before post-processing. Mapping completed but no inundated files found. {site_status_error_message}"
                    )

                else:  # No status or warning val, inundation not completed (possible error)
                    lid_status_new = (
                        f"Mapping not completed, inundated polygons ; {site_status_error_message}"
                    )
                    logging.error(
                        f"{huc_function_tag} {lid} had a mapped value of no but no site status or warnings before post-processing. Mapping not completed, no inundated files found. {site_status_error_message}"
                    )

            elif lid_mapped == "not set":  # Expected behavior

                if lid_status != "not set":  # Status val available (possible error)
                    lid_status_new = f"{lid_status}; {site_status_error_message}"
                    logging.error(
                        f"{huc_function_tag} {lid} is not mapped but had a status before post-processing. {site_status_error_message}"
                    )

                elif lid_warning != "":  # No status available, but a warning IS (expected)
                    lid_status_new = lid_warning

                elif mapping_completed is True:  # No status or warning, inundation completed
                    lid_status_new = "Mapping completed but no inundated files found"

                else:  # No status or warning, inundation not completed
                    lid_status_new = "Mapping not completed, no inundated files found"

            else:  # If status is not 'not set' 'no' or 'yes' at this stage then something weird happened... Unlikely, indicates an error
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
    sites_gdf = rename_output_columns(sites_gdf, COLUMN_RENAME_DICT)  # nws_lid is now ahps_lid
    sites_gdf = drop_output_columns(sites_gdf, catfim_type)
    sites_gdf = round_output_columns(sites_gdf)
    sites_gdf = reorder_output_columns(sites_gdf, OUTPUT_COLUMN_ORDER)

    # Save updated sites GDF
    logging.info(f"{huc_function_tag} Saving updated HUC sites GDF to {sites_post_mapping_file_path}")
    sites_gdf.to_file(sites_post_mapping_file_path, driver='GPKG', engine="fiona", index=False)

    # ------------------------------------
    # Process HUC library if it is available

    # Exit the function here if there is no library input, if the library GDF is empty, or if the path is faulty
    if len(huc_library_gdf) == 0:
        logging.info(
            f"{huc_function_tag} No library input available, no final library gdf will be saved to {library_post_mapping_file_path}"
        )
        return

    # Otherwise, HUC library should already be read in as huc_library_gdf
    logging.info(f"{huc_function_tag} Processing library path {library_input}")

    # Rename columns in the library GDF, then drop any unnecessary or confusing columns
    huc_library_gdf = rename_output_columns(huc_library_gdf, COLUMN_RENAME_DICT)  # nws_lid is now ahps_lid
    huc_library_gdf = drop_output_columns(huc_library_gdf, catfim_type)
    huc_library_gdf = round_output_columns(huc_library_gdf)
    huc_library_gdf = reorder_output_columns(huc_library_gdf, OUTPUT_COLUMN_ORDER)

    # Library needs the following metadata from the sites GDF: wfo, rfc, state, county, and status
    # Note: We are merging using the newly renamed columns!!!
    huc_library_gdf = pd.merge(
        huc_library_gdf,
        sites_gdf[['ahps_lid', 'wfo', 'rfc', 'state', 'county', 'status']],
        on='ahps_lid',
        how='left',
    )

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
    # Check that the col rename has occurred
    if 'ahps_lid' not in df.columns:
        msg = (
            "Expected column 'ahps_lid' not found in dataframe columns."
            "This likely means that the rename_output_columns function has not been applied yet."
            "Additional column formatting should not be applied before the column renaming."
        )
        logging.error(msg)
        raise Exception(msg)

    df_new = df.copy()

    # List of columns to remove for both stage-based AND flow-based CatFIM
    cols_to_remove = [
        'warnings',
        'level_0',
        'lid_alt_m',
        'datum_adj_wse_m',
        'nws_data_geo_rfc',
        'nws_data_huc',
        'usgs_data_huc',
        'usgs_data_state',
        'identifiers_env_can_gage_id',
        'env_can_gage_data_name',
        'env_can_gage_data_latitude',
        'env_can_gage_data_longitude',
        'env_can_gage_data_map_link',
        'env_can_gage_data_drainage_area',
        'env_can_gage_data_contrib_drainage_area',
        'env_can_gage_data_water_course',
    ]

    # List of columns to only remove for fb or sb CatFIM
    fb_specific_cols_to_remove = ['interval_stage', 'is_interval', 'stage', 'stage_uni', 'stage_src']
    sb_specific_cols_to_remove = ['q', 'q_src', 'q_uni']
    # TODO: Eventually can remove the stage intermediate vals, but keeping for now
    # bcs it’s useful for debugging at the moment (datum_adj_ft, datum_adj_wse_ft, lid_alt_ft, hand_stage)

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


def rename_output_columns(df, column_rename_dict):
    '''
    Rename the CatFIM output column names.

    Arguments
    ---------
    df - Pandas Dataframe or Geopandas GeoDataFrame
        Dataframe or geodataframe that potentially needs column renaming.
    column_rename_dict - Dictionary
        A dictionary mapping old column names to new column names.

    Returns
    -------
    df_new - Pandas DataFrame or GeoDataFrame
        Dataframe or geodataframe with renamed columns.

    '''

    df_new = df.copy()

    # Previous versions of CatFIM had different column names for the sites and library outputs,
    # but we are moving towards having more uniform column names between the two outputs.

    # Iterate down the colname dictionary and rename whichever ones are there
    for old_name, new_name in column_rename_dict.items():
        if old_name in df_new.columns:
            df_new.rename(columns={old_name: new_name}, inplace=True)
            # logging.info(f"Renamed column '{old_name}' to '{new_name}' in the output dataframe.")  # Verbose

    return df_new


def reorder_output_columns(df, column_order_list):
    '''
    Reorders the output columns based on an input list.
    Uses renamed column names, must be used after rename_output_columns.

    Arguments
    ---------
    df - Pandas Dataframe or Geopandas GeoDataFrame
        Dataframe or geodataframe that potentially needs column rounding.
    column_order_list - List of STR


    Returns
    -------
    df_new - Pandas DataFrame or GeoDataFrame
        Dataframe or geodataframe with rounded columns.

    '''
    # Check that the col rename has occurred
    if 'ahps_lid' not in df.columns:
        msg = (
            "Expected column 'ahps_lid' not found in dataframe columns."
            "This likely means that the rename_output_columns function has not been applied yet."
            "Additional column formatting should not be applied before the column renaming."
        )
        logging.error(msg)
        raise Exception(msg)

    df_new = df.copy()

    # Identify which of the target colnames are actually in the df
    cols_to_move = [c for c in column_order_list if c in df_new.columns]

    # Get all other columns that are not in our column order list list
    remaining_cols = [c for c in df_new.columns if c not in cols_to_move]

    # Reorder the columns
    df_new = df_new[cols_to_move + remaining_cols]

    return df_new


def round_output_columns(df):
    '''
    Round the values in the specified columns of the input DataFrame.
    Uses renamed column names, must be used after rename_output_columns.

    Arguments
    ---------
    df - Pandas Dataframe or Geopandas GeoDataFrame
        Dataframe or geodataframe that potentially needs column rounding.

    Returns
    -------
    df_new - Pandas DataFrame or GeoDataFrame
        Dataframe or geodataframe with rounded columns.

    '''
    # Check that the col rename has occurred
    if 'ahps_lid' not in df.columns:
        msg = (
            "Expected column 'ahps_lid' not found in dataframe columns."
            "This likely means that the rename_output_columns function has not been applied yet."
            "Additional column formatting should not be applied before the column renaming."
        )
        logging.error(msg)
        raise Exception(msg)

    df_new = df.copy()

    # Dictionary of old name and new names for the final outputs (Format: 'new_name': number of decimal places to round to)
    column_round_dictionary = {
        'datum_adj_ft': 2,
        'datum_adj_wse_ft': 2,
        'datum_adj_wse_m': 2,
        'lid_alt_ft': 2,
        'lid_alt_m': 2,
        'lid_usgs_elev': 2,
    }

    # Iterate down the colname dictionary and round whichever ones are there
    for new_name, round_val in column_round_dictionary.items():
        if new_name in df_new.columns:
            df_new[new_name] = df_new[new_name].round(round_val)

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

    if site == "all":
        site_list = all_huc_sites
    else:
        site_list = [site]

    # Iterate sites and update the columns as needed
    for site_i in site_list:
        site_i = site_i.upper()  # Before post-processing, sites are uppercase

        if site_i not in all_huc_sites:
            logging.error(f"{site} - Updating sites table - Site not in sites_gdf... unable to update table")
            return sites_gdf

        # Get initial values and print value changes
        mapped_i = sites_gdf.loc[sites_gdf["nws_lid"] == site_i, "mapped"].iloc[0]
        status_i = sites_gdf.loc[sites_gdf["nws_lid"] == site_i, "status"].iloc[0]
        warnings_i = sites_gdf.loc[sites_gdf["nws_lid"] == site_i, "warnings"].iloc[0]

        # Give a warning if mapped is already 'no', because that could indicate an error
        # because we don't keep processing sites after mapped is 'no'.
        # We also will not update any of the columns because we don't want to overwrite the actual reason
        # a site became unmapped.
        if mapped_i == "no":
            logging.warning(
                f"{site} - Updating sites table - 'Mapped' value is already 'no', will not be updating sites table with new message: '{message}'"
            )
            logging.warning(
                f"{site} - Updating sites table - Current status: '{status_i}', Current warnings: '{warnings_i}'"
            )
            continue

        if set_mapped_to_no is True:
            # We are unmapping the site, so the message gets put in the "status" column
            sites_gdf.loc[sites_gdf["nws_lid"] == site_i, ['mapped', 'status']] = ['no', message]

            logging.info(
                f"{site} - Updating sites table - Changed 'Mapped' from '{mapped_i}' to 'no'"
            )  # Verbose
            logging.info(
                f"{site} - Updating sites table - Changed status from '{status_i}' to '{message}'"
            )  # Verbose

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

            logging.info(
                f"{site} - Updating sites table - Changed warnings from '{warnings_i}' to '{warning_message}'"
            )  # Verbose

        else:
            logging.error(
                f"Updating sites table - Error updating line status/warnings. Invalid value for set_mapped_to_no: {set_mapped_to_no}"
            )

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
