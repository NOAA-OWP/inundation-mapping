#!/usr/bin/env python3

import logging
import os
import pickle
import random
import shutil
import time
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

import data.wrds.download_process_wrds as dpw
import src.utils.shared_functions as sf
from src.utils.shared_variables import VIZ_PROJECTION
from tools.tools_shared_functions import aggregate_wbd_hucs
# TODO: Clean up unused imports

# Global vars, shared by all related py files.
MAGNITUDES_TYPES = ['action', 'minor', 'moderate', 'major', 'record']


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


# TODO: This should probably be moved into flows.py  ??
def get_metadata(huc, huc_path, output_folder):
    """
    Get metadata for a specific HUC.

    Parameters
    ----------
        huc (str): The HUC number.
        huc_path (str): The path to the HUC directory.
        output_folder (str): The output folder path.

    Returns
    -------
        metadata_json_list (list): List of metadata JSON objects.
        return_msgs (list): List of messages from the metadata retrieval process.

    """

    # If we are not getting new metadata, then we assume that the runtime args has the path
    # to a valid pkl file. We just need to copy it over to this dir and load it so we don't
    # have a file collision.
    nwm_meta_file = os.getenv('NWM_METAFILE_PATH')

    # We really only need to load this env if we are going to let the script call WRDS directly.
    api_base_url = ""
    if os.getenv('GET_NEW_META_DATA') is True:
        api_base_url = load_fim_global_env_values(os.getenv('ENV_FILE'))

        # Figure out pathing for the new file to be created, but we need it to be saved in this huc dir
        # If we load our own, add the huc number in front.
        nwm_meta_file = os.path.join(huc_path, f'{huc}_nwm_metadata.pkl')
    else:
        # We need to make a copy of it and put it into the local dir temporaily
        # to save against MP file collisions.
        # then pass that into
        if os.path.isfile(nwm_meta_file) is False:
            raise FileNotFoundError(f"Error: Expected metafile at {nwm_meta_file}")

        # Make a copy of it and put it in our local dir, but give it a few second random delay to help
        # with MP and all of the first set of hucs grabbing a copy at the exact same time.

        # A bit of start staggering to help not overload the MP (0.1 milliseconds to 2 secs)
        time_delay_mms = random.randint(100, 2000) / 1000
        time.sleep(time_delay_mms)
        src_nwm_meta_file = os.path.join(output_folder, nwm_meta_file)
        meta_file_name = os.path.basename(nwm_meta_file)
        nwm_meta_file = os.path.join(huc_path, meta_file_name)  # Now using the new huc copy
        shutil.copyfile(src_nwm_meta_file, nwm_meta_file)

    # Either way, we should have a meta file by now, already validated
    # TODO: see notes on load_nwm_metadata about missing sites related to huc value
    #  we will get meta for all sites for now, not filtered.
    metadata_json_list, return_msgs = dpw.load_nwm_metadata(
        nwm_meta_file, api_base_url, os.getenv('SEARCH'), os.getenv('GET_NEW_META_DATA'), list()
    )

    # return_msgs is a list and might have some warnings, some messages and/or errors
    if len(return_msgs) > 0:
        # TODO: This seems a bit bumpy but good enough for now. No idea on a better answer short of
        # custom exceptions.

        # also.. we get duplicate info to the script as download_process_wrds.py has both prints
        # and returns as a message.  Hummmm. See notes in download_process_wrds.py

        for msg in return_msgs:
            if "warning" in msg.lower():
                logging.warning(msg)
            elif "error" in msg.lower():
                raise Exception(msg)
            else:
                logging.info(msg)

    # TODO: Clean up temp code and comments below (once we no longer need the reference)

    # What does the metatable look like when flattened into a df considering its multiple layers
    # test_df = pd.dataframe(metadata_json_list) # TODO: Clean up
    # test_df = pd.json_normalize(metadata_json_list)
    # test_df.to_csv(os.path.join(output_folder, "df_all_metadata.csv"))

    # Note:
    # aggregate_wbd_hucs takes in a meta json and a list of hucs.
    # DO NOT attempt to run aggregate_wbd_hucs it does not seem to work with a clipped huc wbd,
    # not sure why. And if we try to run aggreg for every huc, the full size WBD takes anywhere
    # from 6 to 20 mins to come back from agg. agg uses the points from each json site, then
    # adds them overtop of the WBD to figure out the HUCs, but the huc values do not come in
    # reliably enough from WRDS. Ultimately, if we generate our own (or get a list)
    # of HUCs to sites, we can filter this json down much easier.

    # In the meantime, we let generate_categorical_fim, talk to agg for all HUCs and put that into a

    # TODO: We need a faster answer
    # how do we handle not loading the entire WBD? Can't really use clips but maybe
    # it is ok to fully load it (well.. a smaller filtered HUC8 (102739 ???  - check crs inside aggre)
    # wbd_file = os.getenv("input_wbd_layer")
    # NOTE: If we stick with the shared one, we need to make very quick copy of to a huc path before
    # loading as there will be a data collision if all HUCs are tryign to open the same file

    # wbd_file = '/data/inputs/wbd/WBD_National_CatFIM_tests.gpkg'  # a small one with just a few hucs
    # wbd_file = f"{os.getenv('pre_clip_huc_dir')}/{huc}/wbd8_clp.gpkg"
    # how can we speed this up? change crs somehow?
    # also.. when I tried a huc pre-clip wbd, it lost a point on one of the HUCS (buffer)? 01050004

    # A bit of start staggering to help not overload the MP (0.1 milliseconds to 10 secs)
    # Its big and might take a few seconds to copy over

    # TODO: update... they delay is not the loading of the wbd, but the iterating of it.

    # but I only need the time delay if I am copying a shared wbd
    # time_delay_mms = random.randint(100, 10000) / 1000
    # time.sleep(time_delay_mms)
    # wbd_file_name = os.path.basename(wbd_file)
    # # huc_wbd_file = os.path.join(huc_path, wbd_file_name)  # Now using the new huc copy
    # shutil.copyfile(wbd_file, huc_wbd_file)

    # huc_dictionary, sites_gdf = aggregate_wbd_hucs(meta_json_list, huc_wbd_file, True, [huc])
    # huc_dictionary, sites_gdf = aggrgate_wbd_hucs(meta_json_list, wbd_file, True, [huc])
    # if len(huc_dictionary) == 0:
    #     raise Exception(f"Error: {huc} does not appears to have any nwm sites")

    # # Drop list fields if invalid
    # sites_gdf = sites_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')
    # sites_gdf = sites_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')

    # if 'metadata_sources' in sites_gdf.columns:  # TODO: Is this column needed/used? Changed to accomodate Guam?
    #     sites_gdf = sites_gdf.astype({'metadata_sources': str})

    # viz_sites_gdf = sites_gdf.to_crs(VIZ_PROJECTION)

    # Debug Temp. Lets make a copy as a checkpoint
    # raw_sites_file = os.path.join(huc_path, "raw_sites.gpkg")
    # viz_sites_gdf.to_file(raw_sites_file, driver='GPKG', crs=VIZ_PROJECTION, engine='fiona')

    # Filter the meta_json to just the HUC we want. meta_json_list still has the full list (ie.. not filtered)
    # the list of dictionary items are {huc, [multiple lids]}
    # nwm_lids = []
    # # nwm_list = nwm_lids.extendlist(huc_dictionary.values())  # the "value" column is a list of nwm_lists
    # for val in huc_dictionary:
    #     nwm_lids = nwm_lids.extend(val)

    # filtered_meta_list = []
    # for site_entry in meta_json_list:
    #     lid = site_entry['identifiers']['nws_lid']
    #     if lid in nwm_lids:
    #         filtered_meta_list.append(site_entry)

    all_sites_gdf = gpd.read_parquet(os.getenv('NWM_SITES_PATH'))

    huc_sites_gdf = all_sites_gdf[all_sites_gdf['HUC8'] == huc].copy()

    if len(huc_sites_gdf) == 0:
        raise Exception(f"Error. The HUC of {huc} does not exist in the all sites dataset.")

    # There appears to be actual column named "index" at this point, remove it
    huc_sites_gdf.reset_index(drop=True, inplace=True)

    huc_sites_gdf.rename(columns={"identifiers_nws_lid": "nws_lid"}, inplace=True)

    # Keep everyhing uppercase for processing as the json files are uppercase for that file
    huc_sites_gdf['nws_lid'] = huc_sites_gdf['nws_lid'].str.upper()

    # TODO: now that we have a list of the sites applicable to this huc, filter the metadata_json
    # todo: We want a list of dictionary from huc_sites_gdf of {nws_lid, huc}

    # Now that we have a list of HUCs to lids from the geoparquet, given to use from generate_categorical_fim.
    # we can filter the meta_json_list down
    nwm_lids = huc_sites_gdf['nws_lid'].tolist()

    # Find lid metadata from master list of metadata dictionaries (line 66).
    huc_metadata_json_list = []
    for lid_site_data in metadata_json_list:
        lid = lid_site_data['identifiers']['nws_lid']
        if lid in nwm_lids:
            huc_metadata_json_list.append(lid_site_data)

    # what do we do if the huc_site_gdf and/or huc_metadata_list is empty
    # TODO: Error if no data found

    return huc_metadata_json_list, huc_sites_gdf


def check_for_restricted_sites(sites_gdf, catfim_type):
    """
    Checks for restricted sites and updates the sites GeoDataFrame accordingly.

    Compares the provided sites GeoDataFrame against a list of restricted sites
    loaded from a CSV file. It updates the 'status' and 'mapped' columns of
    the GeoDataFrame for any restricted sites and returns a list of valid NWM LIDs.

    Parameters
    ----------
    sites_gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing site information with columns such as 'nws_lid'.
    catfim_type : str
        The type of CATFIM processing, 'sb' or 'fb'.

    Note: Previously had huc and sites_file_path also as inputs, removed 1/13/26 because they weren't used

    Returns
    -------
    valid_nwm_lids : list
        A list of valid NWM LIDs after excluding restricted sites.
    sites_gdf : geopandas.GeoDataFrame  
        The updated GeoDataFrame with restricted sites marked accordingly.


    Notes:
    -----

    meta_gdf is likely pretty small by now, only sites for this HUC
    Likely a smarter way to do this as well.. lambda? Could do a join but we have
    dup column names we would have to cleanup.

    """
    # Load restricted sites for the given catfim_type
    df_restricted_sites = load_restricted_sites(catfim_type)

    # Check whether LIDs are in restricted sites list, update sites_gdf accordingly
    valid_nwm_lids = []
    for index, row in sites_gdf.iterrows():
        lid = row["nws_lid"].upper()
        is_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]
        if len(is_restrict_lid) > 0:
            # what if it comes back with more than one? if so.. it is a bug in the list
            sites_gdf.at[index, "status"] = is_restrict_lid.iloc[0]['restricted_reason']
            sites_gdf.at[index, "mapped"] = "no"
        else:
            valid_nwm_lids.append(lid)

    return valid_nwm_lids, sites_gdf


def load_restricted_sites(catfim_type):
    """
    Reads and interprets the ahps_restricted_sites.csv (from the inundation_mapping repo) to
    return a list of restricted sites specific to the given CatFIM type (SB or FB).

    The 'catfim_type' column in the CSV can have three different values: 'stage', 'flow', and 'both.' 
    This determines whether the site should be filtered out for SB, FB, or both.

    We used to require that the LID was 5 characters, but we removed that requirement in Fall 2025
    because there actually are a few LIDs that might be valid but aren't 5 chars. And if they're 
    invalid, we will filter elsewhere.

    Args
        catfim_type: (str) 'sb' or 'fb'

    Returns
       df_restricted_sites (DataFrame) containing the restricted lids and the reasons why: 
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

            # FLOG.warning(f"{restricted_reason}. Lid is '{nws_lid}'") # TODO: Update logging?
            # Humm.. how do we log this? screen is ok, but log isn't (MP versus non MP)
            # can we try just using the "logging" instance? Let's try it and see what happens
            logging.warning(f"{restricted_reason}. Lid is '{nws_lid}'")

        continue
    # end loop

    # Remove catfim_type column
    df_restricted_sites = df_restricted_sites.drop('catfim_type', axis=1)

    return df_restricted_sites


def load_runtime_args(output_folder):
    """
    Loads CatFIM run arguments from the runtime_args.env file.

    Args
        output_folder: (str) CatFIM output folder filepath

    Variables loaded into memory (example)
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
    """

    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)

    if not os.path.isfile(args_file):
        raise ValueError(f"Unable to find the runtime_args.env at {output_folder}")

    # Use load_env to pull out just the variables it needs.
    load_dotenv(args_file)

    # TODO: Let's change GET_NEW_META_DATA and GET_NEW_THRESHOLD_DATA to true booleans 


def validate_inputs(huc, output_folder):
    """
    Validate input parameters and return normalized paths.

    Checks that:
        - output_folder exists
        - HUC data is available in FIM_RUN_DIR
        - required directory structure (branches) exists for the HUC

    Args:
        huc (str): HUC identifier for the hydrologic unit code.
        output_folder (str): Root output folder path where HUC subdirectories are stored.

    Returns:
        tuple: (huc_path, output_folder)
            - huc_path (str): Full path to the HUC subfolder in output directory.
            - output_folder (str): Normalized output folder path (trailing slashes removed).

    Raises:
        ValueError: If output_folder is None/empty or doesn't exist, or if FIM_RUN_DIR environment variable is not set.
        FileNotFoundError: If HUC path or branches directory doesn't exist in FIM_RUN_DIR.

    """
    
    # TODO: valdiate huc value (8 numeric maybe and starts with 0, 1, or 2)

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

    # Validate data exists in the fim_run_dir and it includes this HUC.
    # This may seem reduntant as it was checked (sort of) in generate_categorical_fim.py.
    # However, this one is HUC specific and it is possible that a HUC
    # can be run after generate_categorical_fim.py was run and add more HUC on the fly.
    # If this HUC did not previous exist or failed, you can re-run this script independantly
    # then run post processing again.
    fim_run_dir = os.getenv("FIM_RUN_DIR")
    if not fim_run_dir:
        raise ValueError(
            "The enviro value for FIM_RUN_DIR does not exist or is empty. It was loaded"
            " and included in the runtime_arg enviro file. Check pathing and variables."
        )

    fim_run_huc_path = os.path.join(fim_run_dir, huc)
    if not os.path.exists(fim_run_huc_path):
        raise FileNotFoundError(
            "This script needs to talk to its HUC in the fim_run_dir, but the folder"
            f" {fim_run_huc_path} does not exist. Please check pathing (with case) and fim run"
            " error logs or huc list."
        )

    branch_dir = os.path.join(fim_run_huc_path, 'branches')
    if not os.path.exists(branch_dir):
        raise FileNotFoundError(
            "This script needs to talk to branches in its fim_run_dir / HUC in the fim_run_dir,"
            f" but the folder {branch_dir} does not exist. Please check pathing (with case) and"
            " it's error logs."
        )

    # TODO: Validate we have some folders in it.

    # do we validate other key files? branches exist? what if it was a bad huc in the first place?
    # TODO: Validate key bash_variable values? path the meta and threshold files?  Better yet, Emily's tool shoudl do that when we call her things

    # No need to validate any of the runtime_args as they were validated when it was created. (likely)

    # ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301
    huc_path = os.path.join(output_folder, "hucs", huc)

    return huc_path, output_folder
