#!/usr/bin/env python3

import copy
import csv
import glob
import logging
import os
import pickle
import random
import shutil
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone

# from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from tools_shared_functions import (
    filter_nwm_segments_by_stream_order,
    flow_data,
    get_nwm_segs,
    get_thresholds,
)

# foss_fim imports
import data.wrds.download_process_wrds as dpw
import tools.catfim.catfim_shared_functions as csf
from src.utils.shared_variables import VIZ_PROJECTION

# global vars
MAGNITUDES = ['action', 'minor', 'moderate', 'major', 'record']

gpd.options.io_engine = "pyogrio"


"""
Oct/Nov 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing

Tenative notes:
    - Some of the functions in here may move or be split to smaller functions.
    
    - This can be use by any other file when applicable, but anything needed by catfim_post_processing.py
      should be moved into that file.
      
    - Many or all of the functions in here, may move into catfim_process_huc.py potentially. Possible
      some may move into generate_categorical_fim.py, but more likely some of it's logic relating to flows,
      meta and thresholds wil move here.
      
    - This can be responsible for getting meta data, threashold and flow data for both FB and SB
    
    - For now, some data may come from
      WRDS, but will save pickle / parquet files when applicable for all HUCs to use for it's processing.
      Later, the process of getting data from WRDS will be split to an independant tools in our code "data"
      folders. When that happens, this may no longer be needed here, other than flow data? or making version
      copied of the WRDS files?
      
    - Meta and threshold data loaded here, are applicable to all HUCs and sites and will not be filtered in any
      way. It will still, of course, continue to honor WRDS filters that apply to non CONUS sites/HUCs.

"""

'''

Aug 2024
This script was upgraded significantly with lots of misc TODO's embedded.
Lots of inline documenation needs updating as well

Oct 2025
Doc strings and improved documentation was added.

'''


def get_threshold_data(huc, huc_path, valid_nwm_lids):

    # A bit of start staggering to help not overload the MP (0.1 milliseconds to 4 secs)
    time_delay_mms = random.randint(100, 4000) / 1000
    time.sleep(time_delay_mms)

    huc_threshold_data_file_path = os.path.join(huc_path, f"{huc}_threshold.csv")

    # We really only need to load this env if we are going to let the script call WRDS directly.
    api_base_url = ""
    if os.getenv('GET_NEW_THRESHOLD_DATA') is True:
        api_base_url = csf.load_fim_global_env_values(os.getenv('ENV_FILE'))

        threshold_url = f'{api_base_url}/nws_threshold'

        # Download thresholds applicable to this huc list.
        # Build a new huc_lic_dict as we know only valid sites we want after restriction tests
        huc_lid_dict = {}
        for lid in valid_nwm_lids:
            huc_lid_dict[lid] = huc

        date_formatted = datetime.now(timezone.utc).strftime("%Y%m%d")

        # Note: We have to do this as not all threshhold data has a huc value, but we already
        # have figured out a list of nws_lids for this huc
        local_copy_threshold_file_path = os.path.join(huc_path, f'nwm_threshold_data_{date_formatted}.pkl')
        return_msgs = dpw.download_all_thresholds(local_copy_threshold_file_path, threshold_url, huc_lid_dict)

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
    else:
        # We need to make a copy of it and put it into the local dir temporaily
        # to save against MP file collisions.
        threshold_file_path = os.getenv('THRESHOLD_FILE_PATH')
        if os.path.isfile(threshold_file_path) is False:
            raise FileNotFoundError(f"Error: Expected the threshold file at {threshold_file_path}")

        # Make a copy of it and put it in our local dir, but give it a few second random delay to help
        # with MP and all of the first set of hucs grabbing a copy at the exact same time.
        src_file_name = os.path.basename(threshold_file_path)
        local_copy_threshold_file_path = os.path.join(huc_path, src_file_name)  # Now using the new huc copy
        shutil.copyfile(threshold_file_path, local_copy_threshold_file_path)

    # TODO: Rob: what is this "manual_input" thing about?  need some details here

    # Either way, we have an existing threshold file to load and filter
    # Get the source (important for differentiating processing for manual input vs wrds)
    # we will need to filter out just the site records we want as it depends what file was
    # loaded in the first place.
    # The threshold files has all site and huc data, so we need to filter it out
    threshold_all_sites_df = pd.DataFrame()
    with open(local_copy_threshold_file_path, "rb") as p_handle:

        # even though the threshold file is a pickle file, it has a df in it
        threshold_all_sites_df = pickle.load(p_handle)
        # source_list = thresh_json_data['source']

        # If manual input is in source list, set data source to manual input
        # Assumes that if one is manual input, then all are manual input
        # if 'Manual_Input' in source_list:
        #     print("Manual input found in threshold source list.")
        #     data_source = 'Manual_Input'

        # Otherwise, compile unique sources into a comma-separated string
        # else:
        #     data_source = set(thresh_json_data['source'])

        #     # TODO: Nov 2025: Fix this. The data source line below with the join has a bug.
        #     # When the source comes in with a slash at the front, we get:
        #     #     TypeError: sequence item 0: expected str instance, NoneType found

        #     # When the source comes in without a slash at the front, we get:
        #     #     TypeError: sequence item 0: expected str instance, float found
        #     # data_source = ', '.join(data_source)

        #     # temp workaround
        #     data_source = 'TEST'

    # This is for threshold filtered to this huc as the loaded threshold file may have more than
    # just this huc
    # Humm... do we sort out stages (for FB? what about SB and its interavles here?)
    # And update sites_db if it is missing something

    # I think it is possible to not have threshold data for all of this huc's list.
    # change all nws_lid values to lower
    # threshold_all_sites_df['nws_lid'] = threshold_all_sites_df['nws_lid'].str.lower()

    threshold_huc_df = threshold_all_sites_df[threshold_all_sites_df['nws_lid'].isin(valid_nwm_lids)].copy()
    if len(threshold_huc_df) == 0:
        # TODO: make sure to updates the sites mapped and status for all lid records for this HUC
        return []
    
    # threshold_huc_df.reset_index()

    stage_types = ['action', 'minor', 'moderate', 'major', 'record']

    threshold_huc_df[stage_types] = threshold_huc_df[stage_types].fillna(value=-1).astype("float")

    # save a copy of the filtered version
    threshold_huc_df.to_csv(huc_threshold_data_file_path, index=False)

    # drop columns we don't use at all at this point.
    threshold_huc_df = threshold_huc_df.drop(columns=[
        'low', 'bankfull', 'huc', 'flood'],
        errors='ignore')

    # move the nws_lid column to the start to make it easier to read the outputs
    nws_lid_col = threshold_huc_df.pop('nws_lid')
    threshold_huc_df.insert(0, 'nws_lid', nws_lid_col)

    # ========================
    # Make df for stages and flows
    stages_df = threshold_huc_df.loc[threshold_huc_df['threshold_type'] == 'stages'].copy()
    flows_df = threshold_huc_df.loc[threshold_huc_df['threshold_type'] == 'flows'].copy()

    # Flatten this into one nwm row for now.
    # Why flatten? Sometimes when processing flow, the flow it wants some stage data even though
    # it does not use it, just columns in the flow library data
    # and visa-versa.
    # Some lids may have stage data, flow data or both. We want to make sure that any of the 10 columns
    # (action, minor, moder... ) x (stage or flow)
    # so we come out with:
    # nws_lid, action_stage, minor_stage, moderate_stage.... action_flow, minor_flow, moderate_flow...  etc
    # All ten cells will have -1 or a value
    thresholds_merged_df = pd.merge(stages_df, flows_df, on='nws_lid', suffixes=('_stage', '_flow'), how='outer')

    # cleanup df
    # Drop duplicate columns (all will have the same values)
    # usgs_site_code, nws_lid, wrds_timestamp, threshold_type_stages
    # we do not need the flow columns long term. Just need them for processing
    thresholds_merged_df.drop(
        columns=['threshold_type_stage', 'threshold_type_flow', 'wrds_timestamp_flow', 'usgs_site_code_flow'],
        errors='ignore', inplace=True)
    thresholds_merged_df.rename(
        columns={
            'wrds_timestamp_stage': 'wrds_timestamp',
            'usgs_site_code_stage': 'usgs_site_code',
        },
        inplace=True
    )

    # It is possible that we only had stage rec for this huc, or only flow record for this huc. If so.
    # makes sure we have all 10 columns no matter what.
    # we can just check one column for each flow and stage and assume the other 5 is missing
    # And just because a set of stages exists but not flows, does not auto reject the record.
    for stage_type in stage_types:
        col_name = f"{stage_type}_stage"
        if  col_name not in thresholds_merged_df.columns:
            thresholds_merged_df[col_name] = -1

        col_name = f"{stage_type}_flow"
        if  col_name not in thresholds_merged_df.columns:
            thresholds_merged_df[col_name] = -1

    # The main columns come out as:
    #  action_stage, action_flow, minor_stage, minor_flow.... etc
    # some columns may have None or 0? Do we make sure they are floats and set to zero or -1 if not there?
    # save a copy of the filtered flattened version
    merged_file_name = huc_threshold_data_file_path.replace('.csv', '_merged.csv')
    thresholds_merged_df.to_csv(merged_file_name, index=False)

    # add some extra columns for processing
    thresholds_merged_df["is_valid"] = True
    thresholds_merged_df["err_msg"] = ""
    thresholds_merged_df["missing_stages"] = ""

    return thresholds_merged_df


def process_theshold_data(catfim_type, valid_nwm_lids, sites_gdf, huc, huc_path, thresholds_merged_df, metadata_json):

    # Basically.... the same thing as the original generate_flows, but thresholds have already been loaded


    # For now, we still need the json to help pull out nodes like segments. (more complex stuff)
    # Eventually we can get it all df row based as it is all there already which means we
    #    dont' really need the metadata_json.
    # The threshold_memrged_df is for this huc only and has some extra columns but the main ones are in the pattern of
    #    action_stage, action_flow, minor_stage, minor_flow.... etc
    # This keeps clearly defined valuse for all columns instead of two seperate dictionaries.
    # FB needs some meta data from sb stage colums. Not for processing, just data going into the library rows.

    huc_library_df = pd.DataFrame()

    # ++++++++++++++++++++++++++++
    # TODO: This HAS TO be change to loading somethign at the HUC level for flow data, it is just too large to do here. (1.6 GiB)

    # Get the correct nwm_flows_region_df based on the HUC
    if huc[:4] == '2201':  # Guam
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nhd_flows_Guam'))
    elif huc[:4] == '2203':  # American Samoa
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nhd_flows_AmericanSamoa'))
    elif huc[:2] == '19':  # Alaska
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nwm_flows_Alaska'))
    else:  # CONUS + Hawaii + Puerto Rico
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nwm_flows'))   # this might be slow, it is 1.8 GiB

    # TODO: check if the metadata_json is empty and fail
    # also check that other values such as valid_nwm_lids, thresholds_merge_df is not empty and fail


    if catfim_type == "sb":
        sites_gdf, library_df = __process_stage_based_sites(valid_nwm_lids, huc, sites_gdf, thresholds_merged_df, metadata_json, nwm_flows_region_df)

    else:
        sites_gdf, library_df = __process_flow_based_sites(valid_nwm_lids, huc, sites_gdf, thresholds_merged_df, metadata_json, nwm_flows_region_df)


def __process_stage_based_sites(valid_nwm_lids, huc, sites_gdf, thresholds_merged_df, metadata_json, nwm_flows_region_df):

    data_source = lid_threshold_data["source_stage"]

    usgs_elev_table = os.path.join(os.getenv("FIM_RUN_DIR"), huc, 'usgs_elev_table.csv')

    usgs_elev_df = None
    if data_source != 'Manual_Input' and not os.path.exists(usgs_elev_table):
        msg = "Internal Error: Missing key data from HUC record (usgs_elev_table missing)"
        raise Exception(msg)

    if data_source != 'Manual_Input':
        usgs_elev_df = pd.read_csv(usgs_elev_table)

    for lid in valid_nwm_lids:

        # ---------------------------
        # TODO: low priority:
        #   this is duplicated in __process_flow_based... see if we can make a function out of it
        logging.info(f"Processing threshold data for {lid}")

        # Let's get the indexs number so we can update. We can assume the rec exists
        # lid_site_gdf_index = sites_gdf.loc[sites_gdf["nws_lid"] == lid].index

        lid_threshold_data = thresholds_merged_df.loc[thresholds_merged_df['nws_lid'] == lid].copy()
        if lid_threshold_data.empty:
            msg = 'WRDS response sucessful but no stage values available'
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
            logging.warning(f"{lid}: {msg}")
            continue

        # if lid_threshold_data  action_stage

        metadata = next(
            (item for item in metadata_json if item['identifiers']['nws_lid'] == lid.upper()), False
        )

        # ---------------------------
        has_error, sites_gdf, segments = __get_segments(lid, sites_gdf, metadata)  # Updates the sites_gdf for us
        if has_error is True:
            continue


    # return sites_gdf, library_df


def __process_flow_based_sites(valid_nwm_lids, huc, sites_gdf, thresholds_merged_df, metadata_json, nwm_flows_region_df):

    for lid in valid_nwm_lids:

        # Processing data and tests the lid level before processing at the magnitude level
        print("")        
        logging.info(f"threshold data processing for {lid}")


        lid_threshold_data = thresholds_merged_df.loc[thresholds_merged_df['nws_lid'] == lid].copy()
        if lid_threshold_data.empty:
            msg = 'Missing all stage data'
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
            # sites_gdf.iloc[lid_site_gdf_index, ['mapped', 'status']] = ['no', msg]
            logging.warning(f"{lid}: {msg}")
            continue
        
        lid_metadata = next(
            (item for item in metadata_json if item['identifiers']['nws_lid'] == lid), False
        )

        # ---------------------------
        has_error, sites_gdf, segments = __get_segments(lid, sites_gdf, lid_metadata, nwm_flows_region_df)  # Updates the sites_gdf for us
        if has_error is True:
            continue

        # procesing each magnitude in here, now that the tests that are not mag specific are done
        sites_gdf, library_df = __build_library_for_mags("fb", lid, sites_gdf, lid_threshold_data, lid_metadata, nwm_flows_region_df, segments)

    # return sites_gdf, library_df


'''
    # for lid in valid_nwm_lids:


    #     # ==============================
    #     # We have a good handful of lid level tests we can do before sorting out magnitude level.

    #     metadata = next(
    #         (item for item in metadata_json if item['identifiers']['nws_lid'] == lid.upper()), False
    #     )

    #     # -----------------------------------------------------
    #     # Tests for both fb and sb

    #     # SB only tests
    #     if catfim_type == "sb":
    #         data_source = lid_threshold_data["source_stage"]

    #         # err_msg = 

    #         lid_altitude = lid_sites_gdf["usgs_data_altitude"]
    #         if lid_altitude is None or lid_altitude == 0:
    #             msg = 'AHPS site altitude value is invalid'
    #             sites_gdf.loc[site_gdf_index, 'mapped'] = "no"
    #             sites_gdf.loc[site_gdf_index, 'status'] = msg
    #             logging.warning(f"{lid}: {msg}")
    #             continue


    #     if data_source != 'Manual_Input' and not os.path.exists(usgs_elev_table):
    #         msg = ":Internal Error: Missing key data from HUC record (usgs_elev_table missing)"
    #         # all_messages.append(huc + msg)
    #         MP_LOG.warning(huc + msg)
    #         skip_lid_process = True

    #             # Look for acceptable elevations
    #             acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id)

    #             if acceptable_usgs_elev_df is None or len(acceptable_usgs_elev_df) == 0:
    #                 msg = ":Unable to find gage data"  # TODO: USGS Gage Method: Update this error message to be more descriptive
    #                 all_messages.append(lid + msg)
    #                 MP_LOG.warning(huc_lid_id + msg)
    #                 continue

    #             # Get the dem_adj_elevation value from usgs_elev_table.csv.
    #             # Prioritize the value that is not from branch 0.
    #             lid_usgs_elev, dem_eval_messages = __adj_dem_evalation_val(
    #                 acceptable_usgs_elev_df, lid, huc_lid_id
    #             )
    #             all_messages = all_messages + dem_eval_messages
    #             if len(dem_eval_messages) > 0:
    #                 continue

    #             # Filter out sites that don't have "good" data
    #             # TODO: USGS Gage Method: It doens't seem like the below error messages are performing as expected....
    #             try:
    #                 if not metadata['usgs_data']['alt_method_code'] in acceptable_alt_meth_code_list:
    #                     MP_LOG.warning(f"{huc_lid_id}: Not in acceptable alt method codes")
    #                     continue
    #                 if not metadata['usgs_data']['site_type'] in acceptable_site_type_list:
    #                     MP_LOG.warning(f"{huc_lid_id}: Not in acceptable site type codes")
    #                     continue
    #                 if not float(metadata['usgs_data']['alt_accuracy_code']) <= acceptable_alt_acc_thresh:
    #                     MP_LOG.warning(f"{huc_lid_id}: Not in acceptable threshold range")
    #                     continue
    #             except Exception:
    #                 MP_LOG.error(f"{huc_lid_id}: Filtering out 'bad' data in the usgs data")
    #                 MP_LOG.error(traceback.format_exc())
    #                 continue

    #             # Adjust datum of HAND grid based on elevation data from usgs_elev_table.csv.
    #             datum_adj_ft, datum_messages = __adjust_datum_ft(flows, metadata, lid, huc_lid_id)
    #             all_messages = all_messages + datum_messages
    #             if datum_adj_ft is None:
    #                 MP_LOG.warning(f"{huc_lid_id}: datum_adj_ft is None")
    #                 continue

    #         else:  # if source is manual input, we skip the above elevation filtering
    #             MP_LOG.lprint(
    #                 f"{huc_lid_id}: Skipping elevation checks and datum adjustment for Manual Input source"
    #             )

    #             lid_altitude = float(lid_altitude)  # LID altitude is expected to be in meters
    #             lid_usgs_elev = (
    #                 lid_altitude * 0.3048
    #             )  # lid_altitude is now in meters to match non-manual input units
    #             # TODO: Automate conversion?

    #             datum_adj_ft = 0  # no datum adjustment for manual input



    #     # We are passing in a copy of the lid_sites_gdf which we can use to replace it in the original df
    #     # when it comes back. But above we can sort out if all 5 stages or flows are missing.
    #     # if at least one flow and one stages is available, go in. Or do we? 
    #     # we can detect messages or lack of library records if all flows or stages are missing?
    #     # Inside this can sort out which stage type and flow values are missing or invalid.
    #     # For SB, this will also add the interval records
    #     lid_sites_gdf, lid_library_df = __build_library_data(catfim_type,
    #                                                  lid,
    #                                                  lid_site_data,
    #                                                  huc,
    #                                                  huc_path,
    #                                                  lid_threshold_data,
    #                                                  metadata_json,
    #                                                  )
        
    #     # depending on the return results
    #     # update what we have for the lid site, but we can still keep usign it.
    #     # sites_gdf.loc[site_gdf_index] = lid_sites_gdf


    #     if lid_library_df is None or lid_library_df.empty:
    #         msg = ""  # hummmmm. do we need this? are the messages aleady covered in the return lid_sites_gdf?
    #         continue

    #     pd.concat(huc_library_df, lid_library_df)

    # # # load segments (FB)

    # # # Find lid metadata from master list of metadata dictionaries.
    # # metadata = next(
    # #     (item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False
    # # )

    # return sites_gdf, huc_library_df
'''



def __build_library_for_mags(catfim_type, lid, sites_gdf, lid_threshold_data, lid_metadata, nwm_flows_region_df, segments):

    # ======================
    # Create the basic five library recs when applicable, based on their stage and flow values
    # We will do SB intervals later.

    logging.info(f"Building library data for {lid}")

    # As always, each error found at any level, will update the master sites_gdf as each is processed.
    # ie) a failed, then mapped becomes "no", and status gets a message.

    # As we progress, we are building up all of the library records for each stage / magnitude level

    # Based on some conditions, some library recs won't be created at all (ie.. missing stage or flow values)

    # Each initial row (per stage), not including interval records, will have all columns filled in.
    # Most field values are filled directly from the sites gdf. Some library rows (stage), will 
    # be added, then later removed when it fails other tests. The rows will not contain library geometry.
    # We will add that when we get to mapping / inundation.

    # Later, some fields in the library columns will be updated as it goes such as altitude ??

    # Each row will be an applicable stages or interval record (if SB) as seen in the current final library
    # files, just without geometry. Down the road, more of these rows for this lid, might be dropped based
    # on how inundation goes.

    # We will fill in most of the columns in the library records at the end of this function.

    # In the end of this function, we have a fully complete set of library records for this lid
    # and updated sites, mapped and status fields. In some cases, there may be no remaining valid
    # library files for this lid.

    # lid_library_df = __create_library_df(catfim_type)
    # Could build magnitude candidate library recs and might end up being 0 recs.
    # Up to five recs for SB and possibly more if SB (adding intervals)
    lid_library_df = pd.DataFrame()


    # ------------------------
    # We are going to create magnitude records  (action, minor, etc)
    # build up the warning messages, but if an error messages comes out, we change mapping to no on sites
    # and return
    err_msg = ""
    warning_msg = ""
    invalid_flows = []

    # test
    msg = "testing mapped abort"
    sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
    return sites_gdf, lid_library_df

    # -----------------------------------
    # Check for missing mags first
    for magnitude_type in MAGNITUDES:

        # The column will exist but does it have a valid value (might be -1). (ie. action_stage and action_flow)
        # It has also already been filtered to one rec just for this lid
        stage_type_idx = lid_threshold_data.columns.get_loc(f"{magnitude_type}_stage")
        stage_value = lid_threshold_data.iloc[0][stage_type_idx]
        flow_type_idx = lid_threshold_data.columns.get_loc(f"{magnitude_type}_flow")
        flow_value = lid_threshold_data.iloc[0][flow_type_idx]

        if catfim_type == "sb" and stage_value == -1:
            invalid_flows.append(magnitude_type)
            continue
        elif catfim_type == "fb" and flow_value == -1:  # notice.. flow_value instead of stage_value
            invalid_flows.append(magnitude_type)
            continue

    # debug test
    invalid_flows.append("rob missing")

    if len(invalid_flows) == 5:
        if catfim_type == "sb":
            msg =  "No thresholds for required categories found on WRDS API"
        else:
            # TODO: in earlier gen_cat_fim_flows.py -> generate_flows_for_huc, it has this message
            #   if all(stages.get(category, None) is None for category in categories):
            #      message = f'{lid}:Missing all stage data'
            # Why? It doesnt' use it to process anything and only uses in the flows library
            # for the "q" columns. Shouldn't it be ok if those are blank?
            # TBD
            msg = "Missing all calculated flows for all stages"

        #  Update the sites_gdf
        sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
        logging.warning(f"{lid}: {msg}")
        return sites_gdf, lid_library_df


    elif len(invalid_flows) > 0:
        warning_mags = ', '.join(invalid_flows)
        if catfim_type == "sb":
            warning_message = f"Missing stage data for {warning_mags}"
        else:
            warning_message = f"Missing flow data for {warning_mags}"

        # might have been a warning from something else by this point.
        prev_warning = sites_gdf.loc[sites_gdf["nws_lid"] == lid, 'warnings'].item()
        if prev_warning != "":
            warning_message = f"{prev_warning}; {warning_message}"

        # sites_gdf.at[lid_site_gdf_index, 'warnings'] = warning_message
        sites_gdf.loc[sites_gdf["nws_lid"] == lid, 'warnings'] = warning_message
        # sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]

    # -----------------------------------
    # We have at least one mag to process

    # Process the lid level tests first, then we can do mag level next. Most tests are done at the
    # lid level and not the mag level.



    # if lid_library_df goes back empty, then no stages or flows were able to be processed.
    # return err_msg, warning_msg, lid_library_df
    return sites_gdf, lid_library_df



def __create_lid_library_rec(catfim_type, stage_type, lid_sites_gdf, stage_value, flow_value):

    err_msg = ""
    warning_msg = ""

    # We likely read some values from lid_sites_gdf columns but do we update any? Do we need to send a copy back?

    if flows_dict is None or len(flows_dict) == 0:  # Changed to flows Sept' 25
        err_msg = "WRDS response sucessful but no flow values available"
        return err_msg, warning_msg, None


    # Maybe we jsut return an error and warning instead of try to update the sites here?
    return err_msg, warning_msg, lid_stage_library_df


def __create_library_df(catfim_type):

    # Sart the library df. It does not have an geometry yet. Some records
    # will be added, but later removed if they fail anywhere
    # We will just start with basic columns and add more as we go if applicable.
    # At this point, these are applicable to both FB and SB
    df_cols = {
        "nws_lid": pd.Series(dtype='str'),
        "name": pd.Series(dtype='str'),
        "magnitude": pd.Series(dtype='str'),
        "huc": pd.Series(dtype='str'),
        "interval_stage": pd.Series(dtype='str'),
        "is_interval": pd.Series(dtype='bool'),
        "stage": pd.Series(dtype='float'),
        "stage_uni": pd.Series(dtype='str'),
        "WFO": pd.Series(dtype='str'),
        "rfc": pd.Series(dtype='str'),
        "state": pd.Series(dtype='str'),
        "county": pd.Series(dtype='str'),
        "q": pd.Series(dtype='str'),
        "q_uni": pd.Series(dtype='str'),
        "q_src": pd.Series(dtype='str'),
        "s_src": pd.Series(dtype='str'),
        "wrds_time": pd.Series(dtype='str'),
        "nrldb_time": pd.Series(dtype='str'),
        "nwis_time": pd.Series(dtype='str'),
        "lat": pd.Series(dtype='float'),
        "lon": pd.Series(dtype='float'),
    }

    # SB has a few extra columns that FB does not
    if catfim_type == 'sb':
        sb_cols = {    
            "dtm_adj_ft": pd.Series(dtype='float'),
            "dadj_w_ft": pd.Series(dtype='float'),
            "dadj_w_m": pd.Series(dtype='float'),
            "lid_alt_ft": pd.Series(dtype='float'),
            "lid_alt_m": pd.Series(dtype='float'),
            "rfs_stage": pd.Series(dtype='float'),
        }
        df_cols.update(sb_cols)

    return pd.DataFrame(df_cols)  # creates the schema for all library columns


def __get_segments(lid, sites_gdf, lid_metadata, nwm_flows_region_df):

    in_error = False

    # --------------
    # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
    unfiltered_segments = list(set(get_nwm_segs(lid_metadata)))

    desired_order = lid_metadata['nwm_feature_data']['stream_order']

    # Filter segments to be of like stream order.
    segments = filter_nwm_segments_by_stream_order(
        unfiltered_segments, desired_order, nwm_flows_region_df
    )
    # Previous input was nwm_flows_df, but now it is region specific df (9/25/25)

    # If there are no segments, write message and exit out
    if not segments or len(segments) == 0:
        msg = 'Missing nwm stream segments'
        sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]        
        logging.warning(f"{lid}: {msg}")
        in_error = True

    return in_error, sites_gdf, segments


# Can not be called from command line.
