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


def get_metadata(huc, huc_path, output_folder):

    # this can get filtered meta data based on HUC if you want it.

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

    # either way, we should have a meta file by now, already validated
    # TODO: see notes on load_nwm_metadata about missing sites related to huc value
    #  we will get metaor all sites for now, not filtered.
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

    # What does the metatable look like when flattened into a df considering its multiple layers
    # test_df = pd.dataframe(metadata_json_list)
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
    # 

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

    huc_sites_gdf = all_sites_gdf[all_sites_gdf['HUC8'] == huc]

    if len(huc_sites_gdf) == 0:
        raise Exception("No ")

    huc_sites_gdf.rename(columns={"identifiers_nws_lid": "nws_lid"}, inplace=True)
     # Keep everyhing upper for processing as the json files are upper for that filed
    huc_sites_gdf['nws_lid'] = huc_sites_gdf['nws_lid'].str.upper()

    # TODO: now that we have a list of the sites applicable to this huc, filter the metadata_json
    # todo: We want a list of dictionary from huc_sites_gdf of {nws_lid, huc}


    # Now that we have a list of HUCs to lids from the parquet, given to use from generate_categorical_fim.
    # we can filter the meta_json_list down
    nwm_lids = huc_sites_gdf['nws_lid']

    # let's hold off on this now
    huc_metadata_json_list = []
    for nwm_site_json in metadata_json_list:
        lid = nwm_site_json['identifiers']['nws_lid']
        if lid in nwm_lids:
            huc_metadata_json_list.append(nwm_site_json)

    # what do we do if the huc_site_gdf and/or huc_metadata_list is empty

    return huc_metadata_json_list, huc_sites_gdf
