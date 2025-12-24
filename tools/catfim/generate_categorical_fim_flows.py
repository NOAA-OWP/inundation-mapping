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


gpd.options.io_engine = "pyogrio"


"""
Oct/Nov/Dec 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing

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

"""
Sites_gdf:
    As with all catfim files, after the initial sites_gdf is created with geometry, each file
    and function, will update it as it moves along. There are a wide number of reasons that a
    site may be changed to mapped = "no".  Each function will know why and will update
    the HUC sites.gdf and will handle "mapped, status, and warnings if applicable"

    At this point, the warning column is only for string saying things like: 
    missing acion and minor stage data, etc. Later the messages column will be rolled
    into the final status column, same as before
"""

# TODO: This files shoudl be renamed to somethign like generate_categorical_fim_thresholds
# as "flows" is not descriptive enough. Note: REnaming it will maintian its history.

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
        source_list = threshold_all_sites_df['source']

        # TODO: There is a bug in the data_source that needs to fixed.
        # If manual input is in source list, set data source to manual input
        # Assumes that if one is manual input, then all are manual input
        if 'Manual_Input' in source_list:
            logging.info("Manual input found in threshold source list.")
            data_source = 'Manual_Input'

        # Otherwise, compile unique sources into a comma-separated string
        else:
            data_source = set(threshold_all_sites_df['source'])

            # TODO: Nov 2025: Fix this. The data source line below with the join has a bug.
            # When the source comes in with a slash at the front, we get:
            #     TypeError: sequence item 0: expected str instance, NoneType found

            # When the source comes in without a slash at the front, we get:
            #     TypeError: sequence item 0: expected str instance, float found
            # data_source = ', '.join(data_source)

            # temp workaround
            data_source = 'TEST'

    # This is for threshold filtered to this huc as the loaded threshold file may have more than
    # just this huc

    # TODO: upper case or lower case always? (see other notes around the code about lower or upper)
    # I think it is possible to not have threshold data for all of this huc's list.
    # change all nws_lid values to lower
    # threshold_all_sites_df['nws_lid'] = threshold_all_sites_df['nws_lid'].str.lower()

    threshold_huc_df = threshold_all_sites_df[threshold_all_sites_df['nws_lid'].isin(valid_nwm_lids)].copy()
    if len(threshold_huc_df) == 0:
        # TODO: make sure to updates the sites mapped and status for all lid records for this HUC
        return []
    
    # TODO: humm.. do we reset? or does copy() above do that for us?
    # threshold_huc_df.reset_index()

    # Ensure the mag type columns are floats and already rounded to 2 decimals.
    stage_types = ['action', 'minor', 'moderate', 'major', 'record']
    threshold_huc_df[stage_types] = threshold_huc_df[stage_types].fillna(value=-1).astype(float).round(2)

    # Ensure the units and source columns are strings and not empty
    str_columns = ['units', 'source']
    threshold_huc_df[str_columns] = threshold_huc_df[str_columns].fillna(value="").astype(str)

    # save a copy of the filtered version
    threshold_huc_df.to_csv(huc_threshold_data_file_path, index=False)

    # drop columns we don't use at all at this point.
    threshold_huc_df = threshold_huc_df.drop(columns=[
        'low', 'bankfull', 'huc', 'flood'],
        errors='ignore')

    # move the nws_lid column to the start to make it easier to read the outputs
    nws_lid_col = threshold_huc_df.pop('nws_lid')
    threshold_huc_df.insert(0, 'nws_lid', nws_lid_col)

    return threshold_huc_df, data_source


# More / less equiv to generate_flows in previous versions but some parts have already been done
# before we get here, such as dropping lids for restricted sites. Threshold data is more refined
# by now from previous versions.
def process_theshold_data(catfim_type, valid_lids, sites_gdf, huc, huc_path, threshold_huc_df, metadata_json):

    """
        By this point some lids have been dropped such as one from the restricted sites list.

        At the end, we will have a HUC level library file, which is all of the sites / mag types up to this
        point. Many will be dropped as each lid / mag type is processed. It will be saved to a library but as
        it will be a csv as it does not have geometry yet, which comes later.

        For FB, it will also create a flow_discharge.csv file which is the rolled up to HUC level data
        previously seen the flows dir.

        There is no longer a need for attributes files as it is data that already existed in the sites_gdf
        and theshold data file. The attributes file had a bunch of duplicate data and mostly only needed
        the stage level data for each mag.  It also includes the three "q" columns which is the flow data.

        TODO: why are they called 'q'. ROB: check HV meta data mapx and data load scripts to see if we rename
        it there?  Also.. what does the catfim compare do with the fields?
    """

    # Basically.... the same thing as the original generate_flows, but thresholds have already been loaded

    # For now, we still need the json to help pull out nodes like segments. (more complex stuff)

    # Eventually we can get it all df row based as it is all there already which means we
    #    don't really need the metadata_json.

    # FB needs some meta data from sb stage colums. Not for processing, just data going into the library rows.

    # ++++++++++++++++++++++++++++
    # TODO: This should be changed to loading something_path at the HUC level for flow data,
    # The CONUS flow file it is 1.6 GiB and is a bit slow to load.

    # Emily is looking into it

    # Get the correct nwm_flows_region_df based on the HUC
    if huc[:4] == '2201':  # Guam
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nhd_flows_Guam'))
    elif huc[:4] == '2203':  # American Samoa
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nhd_flows_AmericanSamoa'))
    elif huc[:2] == '19':  # Alaska
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nwm_flows_Alaska'))
    else:  # CONUS + Hawaii + Puerto Rico
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nwm_flows'))   # this might be slow, it is 1.8 GiB
        # TODO: else assume CONUS + HI or PR? What if it is an invalid huc?  Do we validate it earlier? or let
        # it fall out later with non matching lids or fim huc folders.
        # HUMMM...        



    # TODO: check if the metadata_json is empty and fail
    # also check that other values such as valid_nwm_lids, thresholds_merge_df is not empty and fail

    discharge_file_path = os.path.join(huc_path, "flow_discharges.csv")  # only needed for FB  (shoudl it be here then?)
    
    # initial library file post threshold processing
    huc_library_threshold_file_path = os.path.join(huc_path, f"{huc}_library_threshold.csv")

    # These will save intermediate library data for all lids and mag types that are valid by this point.
    # More logic will be done later, which may drop some of the library recs.
    if catfim_type == "sb":
        # This will not create the interval records at this point. It will do it much farther down the road
        # after it has passed a number of tests per mag.
        sites_gdf = __create_sb_library_data(valid_lids,
                                             sites_gdf,
                                             threshold_huc_df,
                                             metadata_json,
                                             nwm_flows_region_df,
                                             huc_path,
                                             huc,
                                             huc_library_threshold_file_path)

    else:
        # This also creates and processes FB discharge data and saves it to disk.
        sites_gdf = __create_fb_library_data(valid_lids,
                                             sites_gdf,
                                             threshold_huc_df,
                                             metadata_json,
                                             nwm_flows_region_df,
                                             huc_library_threshold_file_path,
                                             discharge_file_path)

    return sites_gdf

# This is here are we are still talking about raw threshold data at this point
def __create_sb_library_data(valid_lids,
                             sites_gdf,
                             threshold_huc_df,
                             metadata_json,
                             nwm_flows_region_df,
                             huc_path,
                             huc,
                             huc_initial_library_file_path):

    # is this in the threshold_huc_df dataset? Maybe just do the Manual_input search here
    data_source = threshold_huc_df["source_stage"]

    return sites_gdf


# This is here are we are still talking about raw threshold data at this point
def __create_fb_library_data(valid_lids,
                             sites_gdf,
                             threshold_huc_df,
                             metadata_json,
                             nwm_flows_region_df,
                             huc_initial_library_file_path,
                             discharge_file_path):

    """
    Attribute files are no longer needed as the data in those files were already present in sites_gdf,
    metadata and threshold files. This gives us everything we need to start library records.
    Each lid will create 0 to 5 library records initially (one per mag type).
    During processing some mags (flows/stages) might be rejected which will either drop a library
    record or not make it in the first place.

    Each lid, then mag will be processed, eventually rolling up all valid library files to a huc
    level file and will include all columns needed for the rest of program processing.

    In later steps, more of those library records may be dropped based on further logic.

    This master HUC level sites_gdf will be updated at all stages along the way to update
    the mapped, status and warning columns as applicable. At this time, the warning column
    is just for listing when a mag is missing for a lid but has at least one valid mag.
    ie) Missing stage (or flow) data for action; minor; moderate; major. That will be merged
    into the status column at the end if still applicable.

    Results / Returns:
        - an updated sites_gdf, also saved to disk
    """

    # Yes... catfim_process_huc.py creates these files but only removing for restarts.
    # Maybe it is ok to keep redefining as it won't change and helps for one less
    # arg to pass around. It is not used by SB.


    huc_library_df = pd.DataFrame()
    huc_discharges_df = pd.DataFrame()  # formerly flow files in the flow dir

    for lid in valid_lids:

        # ---------------------
        # Check the lid to see if it is missing all threshold (stage, flow) data.
        # Processing data and tests the lid level before processing at the magnitude level
        print("")        
        logging.info(f"threshold data processing for {lid}")

        lid_threshold_data = threshold_huc_df.loc[threshold_huc_df['nws_lid'] == lid].copy()
        if lid_threshold_data.empty:
            msg = 'Missing all flow data'
            logging.warning(f"{lid}: {msg}")            
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
            continue
        
        # -------------------------
        # check if all stage values are here for this lid
        # Even though FB does not actually use stage data in any calcs, it must have all 5 stage values to be valid
        # and in the FB library file, there are no records without "stage" values.
        stages = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'stages'].to_dict(orient='records')[0]

        # We do want to reject if they are all missing which woudl indicate a code problem ??
        if all(stages.get(magnitude_type, None) is None for magnitude_type in csf.MAGNITUDES_TYPES):
            msg = 'Error getting flows values from WRDS API'
            logging.warning(f"{lid}: {msg}")
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
            continue

        # -------------------------
        #  check if all flow threshold values are here for this lid
        flows = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'flows'].to_dict(orient='records')[0]
        if all(flows.get(magnitude_type, None) is None for magnitude_type in csf.MAGNITUDES_TYPES):            
            msg = "Missing all calculated flows for all stages"
            logging.warning(f"{lid}: {msg}")
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
            continue

        # -------------------------
        lid_metadata = next(
            (item for item in metadata_json if item['identifiers']['nws_lid'] == lid), False
        )

        # ---------------------------
        # As always, it updates, if applicable, the sites_gdf as it moves along (ie.. mapped, status)
        has_error, sites_gdf, segments = __get_segments(lid, sites_gdf, lid_metadata, nwm_flows_region_df)
        if has_error is True:
            continue

        # procesing each magnitude in here, now that the tests that are not mag specific are done
        # It will append data to the fb library csv as it goes along.
        sites_gdf, lid_library_df, lid_discharges_df = __get_fb_discharge_and_library_data_per_lid(lid,
                                                                           sites_gdf,
                                                                           lid_threshold_data,
                                                                           segments)

        if len(lid_library_df) > 0:
            huc_library_df = pd.concat([huc_library_df, lid_library_df], ignore_index=True)

        if len(lid_discharges_df) > 0:
            huc_discharges_df = pd.concat([huc_discharges_df, lid_discharges_df], ignore_index=True)
        
    # Save the both the library and the discharge files. They will be picked up later.
    if len(huc_library_df) > 0:
        huc_library_df.to_csv(huc_initial_library_file_path, index=False)
        logging.info(f"Saving initial library file to {huc_initial_library_file_path}")

    if len(huc_discharges_df) > 0:
        huc_discharges_df.to_csv(discharge_file_path, index=False)
        logging.info(f"Saving discharge file to {discharge_file_path}")

    return sites_gdf


# This is here are we are still talking about raw threshold data at this point
def __get_fb_discharge_and_library_data_per_lid(lid, sites_gdf, lid_threshold_data, segments):

    # TODO: For now, this one is for FB only, and there is a seperate but very similar one for SB.
    # Most of the tests for FB and SB are the same, just the messages are different. 

    '''
    Process:
        To get here, we already validated the sites for restricted sites and it does have a least some flow threshold data.
        
        Processes a lid for FB and builds up the library (attribute) data for each magnitude_type for this lid.
        Later, it will all be rolled up to one HUC level attlibrary file.

        It also creates full df schema that can be directly used to create library files later. Note:
        The columns returned here include some additional columns then previous versions
        in order to be re-used for library data rows.


        As always, the sites_gdf is constantly being updated for mapping and status messages. When a mag is 
        missing, the sites_gdf be updated with the normal "missing action, minor.. .message"

        This function also creates the discharge df, previously called flow files,
        which previously was done via a folder structure of
        {huc}/{lid}/{mag}/file name:  ie)  flows/01020002/grnm1/action/grnm1_huc_01020002_flows_action.csv. This will
        flatten all discharges for all sites / mags into one csv at the HUC level.
        The flows folder is no longer needed. The discharge df for this lid returned will
        be rolled up to a HUC level discharge file.

    Returns:
        - sites_gdf with updates as applicable
        - An library dataset for this lid with basic starting columns for all valid mags. If no recs returned, then
          the lid has no valid mags to process. It will return between 0 and 5 FB library rows in it.
        - A discharge dataset for this lid.
    '''

    # ======================
    # Create the basic five library recs when applicable, based on their stage and flow values
    # We will do SB intervals later.

    logging.info(f"Building the initial library and discharge data all appliable magnitudes for {lid}")

    lid_library_df = pd.DataFrame()
    lid_discharges_df = pd.DataFrame()

    # Find the single lid record from the sites_gdf. Note: you can not update this rec, only the
    # parent site_gdf for the site. Use this for read-only.
    lid_sites_gdf = sites_gdf.loc[sites_gdf["nws_lid"] == lid].copy()
    
    # FB does use some stage values, but only to add to the library tables. It does not use the value in calcs.
    stages = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'stages'].to_dict(orient='records')[0]
    flows = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'flows'].to_dict(orient='records')[0]

    # These lists may change as processing continues and some get rejected.
    invalid_flows = []

    # -----------------------------------
    # get discharge values (previous data in /flows/ folders now rolled up at the huc level)
    for magnitude_type in csf.MAGNITUDES_TYPES:

        try:
            # -------------
            logging.info(f"Building initial library rec and discharge data for {lid} - {magnitude_type}")

            # Guaranted to be a float with either some value or -1 for invalid and is already rounded to 2 points
            flow_value = flows[magnitude_type]

            if flow_value == -1 or flow_value == 0:
                logging.warning(f"{lid} : {magnitude_type} has an invalid or n/a flow value of {flow_value}")
                invalid_flows.append(magnitude_type)
                continue

            # -------------
            # Discharge data
            flow_info_df = flow_data(segments, flow_value)

            if len(flow_info_df) > 0:
                flow_info_df["lid"] = lid
                flow_info_df["magnitude"] = magnitude_type
                lid_discharges_df = pd.concat([lid_discharges_df, flow_info_df], ignore_index=True)
            else:
                logging.warning(f"{lid} : {magnitude_type} failed to get segment flow data")
                invalid_flows.append(magnitude_type)
                continue

            # -------------
            # library recs

            # FB does include the stage value in the final library columns but does not use the data for any logic

            # We will create an initial library rec for this lid and mag based on valid
            # lid and mag type.
            # Ultimately, only valid recs, will use the same library recs as the starting
            # point for all relavent library records. Some of these will be updated, rejected (removed)
            # at laster stages based on logic down the road.
            lid_mag_library_rec_df  = __create_lid_mag_library_rec(
                                                    "fb",
                                                    lid,
                                                    lid_sites_gdf,
                                                    magnitude_type,
                                                    stages[magnitude_type],
                                                    flow_value,
                                                    lid_threshold_data)
            
            # should always have a rec instead something catestrophic failed.
            lid_library_df = pd.concat([lid_library_df, lid_mag_library_rec_df], ignore_index=True)

        except Exception as ex:
            msg = f'Error with flow/threshold processing of {flow_value} for {magnitude_type} stage'
            logging.error(f"{lid} :{magnitude_type}: {msg}")        
            logging.error(traceback.format_exc())
            raise ex


    # -------------
    if len(invalid_flows) == 5:
        msg = 'No valid flow values are available'
        logging.warning(f"{lid}: {msg}")        
        sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
        return sites_gdf, pd.DataFrame, []

    elif len(invalid_flows) > 0:
        warning_mags = ', '.join(invalid_flows)
        warning_message = f"Missing or invalid flow data for {warning_mags}" 

        # Might have been a warning from something else by this point.
        prev_warning = sites_gdf.loc[sites_gdf["nws_lid"] == lid, 'warnings'].item()
        if prev_warning != "":
            warning_message = f"{prev_warning}; {warning_message}"

        logging.warning(f"{lid}: {warning_message}")
        sites_gdf.loc[sites_gdf["nws_lid"] == lid, 'warnings'] = warning_message

    # It is ok if lid_library_df and/or lid_discharges_df goes back empty.
    return sites_gdf, lid_library_df, lid_discharges_df


# This is here are we are still talking about raw threshold data at this point
def __create_lid_mag_library_rec(catfim_type, lid, lid_sites_gdf, magnitude_type, stage_value, flow_value, lid_threshold_data):

    # TODO: do we need any column validation in here? It's twin in both the SB and FB code does not appear to
    # validate any.

    '''
    Notes:
        It can handle both FB and SB.

    Process:
    
    This creates just one new library df, with all known column values in place.
    The dataset return will likely be re-used to form all columns for the initial library dataset
    when a lid has an applicable mag.

    Some columns such a FB.stage are populated but not used later at any time.

    Returns:
    A df with one single library row rec based on just one mag, which can be concat later.
    It assumes all validation has been already done. However, we can add some validation in here if we like.
    '''

    # TODO:
    # HUMMMM
    # All library files have three stage columns (stage, stage_uni, s_src) and also
    # have three "q" (flow) columns  (q, q_uni, q_src)

    # All 6 columns are applicable to the mag type submitted of course
    # ie.. (library rec.source column when we are processing the mag type of action
    # becomes the "action" value in the threshold stage row or threshold flow row.
    # Remember, this creates a row for only one mag at a time.

    # The three "q" columns are the flow value (applicable to the mag)
# Rob: check hv to see what they do with these columns. Drop them? rename them? leave them
# in the meta?  And why are they called "q". No one would now that that means.

    # for FB library, obviously we only have library record if it has a "q" (flow value applicable)
    # to the mag. The "q" column is a float, and will always have a value and it won't be -1 as that
    # was validated earlier.

    # However.. for SB, that "q" column is a string and may or may not exist.

    # The two units columns (stage_uni and q_uni) as well as the two source columns
    # (s_src and q_src) are all string. And depending on the combinations, may or 
    # may not have values.


    # HUMM.... can we have a missing flow based record in lid_threshold_data when processing
    # a stage based catfim type? hummm


    # What is the easiest way to handle this. For now, lets leave the "q" as a string in all cases.

    # what if a flows or stage rec is missing

    # TODO: Test: find a huc that might be missing stage or flow based data or override the code
    # to debug. Also test if the units and src cols are empty strings.

    # units and sources can be empty strings
    q_uni = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "flows", 'units'].item()
    q_src = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "flows", 'source'].item()
    stage_uni = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "stages", 'units'].item()
    s_src = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "stages", 'source'].item()

    # columns applicable to both FB and SB
    line_df = pd.DataFrame(
        {
        'nws_lid': lid,
        'name': lid_sites_gdf.iloc[0]["name"],
        'WFO': lid_sites_gdf.iloc[0]["nws_data_wfo"],
        'rfc': lid_sites_gdf.iloc[0]["nws_data_rfc"],
        'huc': lid_sites_gdf.iloc[0]["HUC8"],
        'state': lid_sites_gdf.iloc[0]["nws_data_state"],
        'county': lid_sites_gdf.iloc[0]["nws_data_county"],
        'magnitude': magnitude_type,
        'stage': float(stage_value),  # are mag type specific stage values
        'stage_uni': stage_uni,
        's_src': s_src,
        'q': str(flow_value),    # are mag type specific flow values
        'q_uni': q_uni,
        'q_src': q_src,
        'wrds_time': lid_sites_gdf.iloc[0]["wrds_timestamp"],
        'nrldb_time': lid_sites_gdf.iloc[0]["nrldb_timestamp"],
        'nwis_time': lid_sites_gdf.iloc[0]["nwis_timestamp"],
        'lat': lid_sites_gdf.iloc[0]["nws_preferred_latitude"],
        'lon': lid_sites_gdf.iloc[0]["nws_preferred_longitude"],
        }
    )

    # TODO: These columns are missing from the original pickle file back from donwload_process_wrds.py
    # Emily is looking into it

    # add the extra SB cols
    if catfim_type == 'sb':
        print("test")
        # line_df["dtm_adj_ft"] = float(lid_threshold_data.iloc[0]['datum_adj_ft'])
        # line_df["dadj_w_ft"] = float(lid_threshold_data.iloc[0]['datum_adj_wse_ft'])
        # line_df["dadj_w_m"] = float(lid_threshold_data.iloc[0]['datum_adj_wse_m'])
        # line_df["lid_alt_ft"] = float(lid_threshold_data.iloc[0]['lid_alt_ft'])
        # line_df["lid_alt_m"] = float(lid_threshold_data.iloc[0]['lid_alt_m'])


        # The six will like be something like this...
        # line_df["dtm_adj_ft"] = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "stage", 'datum_adj_ft'].item()


        # TODO: Do we really need this? Rob: Check HV load and meta data
        # line_df["rfs_stage"] =  pending.. but in the SB library data, it always matches the stage column value

    return line_df


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
        logging.warning(f'{lid}: {msg}')        
        sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]        
        in_error = True

    return in_error, sites_gdf, segments


# Can not be called from command line.
