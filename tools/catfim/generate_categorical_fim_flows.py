#!/usr/bin/env python3

import argparse
import copy
import csv
import glob
import os
import pickle
import random
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from tools_shared_functions import (
    aggregate_wbd_hucs,
    filter_nwm_segments_by_stream_order,
    flow_data,
    get_metadata,
    get_nwm_segs,
    get_thresholds,
)

import utils.fim_logger as fl
from data.wrds.download_process_wrds import load_site_thresholds
from utils.shared_variables import VIZ_PROJECTION


'''

Aug 2024
This script was upgraded significantly with lots of misc TODO's embedded.
Lots of inline documenation needs updating as well

Oct 2025
Doc strings and improved documentation was added.

'''

# will become global once initiallized
FLOG = fl.FIM_logger()
MP_LOG = fl.FIM_logger()

gpd.options.io_engine = "pyogrio"


def get_env_paths(env_file):
    '''
    Loads environment variables from a .env file.
    Expects the .env file to contain API_BASE_URL and WBD_LAYER variables.

    Parameters
    ----------
        env_file (str): Path to the .env file.
    Returns
    -------
        tuple: (API_BASE_URL (str), WBD_LAYER (str))

    '''
    if os.path.exists(env_file) == False:
        raise Exception(f"The environment file of {env_file} does not seem to exist")

    load_dotenv(env_file)
    # import variables from .env file
    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    return API_BASE_URL, WBD_LAYER


# This one is for lid magnitudes only and is part of an MP pool
def generate_flows_for_huc(
    huc,
    huc_dictionary,
    threshold_url,
    all_meta_lists,
    output_flows_dir,
    attributes_dir,
    huc_messages_dir,
    nwm_flows_region_df,
    parent_log_output_file,
    child_log_file_prefix,
    df_restricted_sites,
    output_catfim_dir,
    threshold_file,
):
    '''
    Only runs for flow-based CatFIM.

    Generates categorical flow files and attribute CSVs for a given HUC
    using metadata, thresholds, and NWM stream segments.

    For each NWS site (lid) in the specified HUC:
        - Checks for restricted sites and skips them if necessary.
        - Loads threshold stage and flow data from WRDS or local files.
        - Validates the presence of required stage and flow data for flood categories.
        - Filters NWM stream segments by stream order and region.
        - Writes flow CSV files for each valid flood category.
        - Compiles site attributes and writes an attribute CSV.
        - Logs messages and warnings for missing or invalid data.
        - Writes a summary message file for the HUC.

    Parameters
    ----------
        huc (str): Hydrologic Unit Code to process.
        huc_dictionary (dict): Dictionary mapping HUCs to lists of NWS site identifiers (lids).
        threshold_url (str): URL for retrieving threshold data from WRDS.
        all_meta_lists (list of dict): List of metadata dictionaries for all NWS sites.
        output_flows_dir (str): Directory to write output flow CSV files.
        attributes_dir (str): Directory to write output attribute CSV files.
        huc_messages_dir (str): Directory to write HUC-specific message files.
        nwm_flows_region_df (pandas.DataFrame): DataFrame containing NWM stream segment data for the region.
        parent_log_output_file (str): Path to the parent log file for multiprocessing logging.
        child_log_file_prefix (str): Prefix for child log files created by this function.
        df_restricted_sites (pandas.DataFrame): DataFrame listing restricted NWS sites and reasons.
        output_catfim_dir (str): Directory for CATFIM output files.
        threshold_file (str): Path to local threshold file (if not using WRDS).

    Returns
    -------
    None

    Side Effects
    ------------
    - Writes flow CSV files for each site and flood category.
    - Writes attribute CSV files for each site.
    - Writes HUC-specific message text files.
    - Logs progress, warnings, and errors to the specified log files.

    Exceptions
    ----------
    Logs and handles exceptions, writing error details to the log file.
    '''

    try:
        # Note: child_log_file_prefix is "MP_process_gen_flows", meaning all logs created by this function start
        #  with the phrase "MP_process_gen_flows". This will roll up to the master catfim log.
        # This is setting up logging for this function to go up to the parent
        MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)

        start_time = datetime.now(timezone.utc)
        dt_string = start_time.strftime("%m/%d/%Y %H:%M:%S")

        # A bit of start staggering to help not overload the MP (20 sec)
        time_delay = random.randrange(0, 20)
        # MP_LOG.lprint(f" ... {huc} start time is {dt_string} and delay is {time_delay}")
        MP_LOG.lprint("")
        MP_LOG.lprint(f" ... {huc} flow generation start time is {dt_string}")

        time.sleep(time_delay)

        # Process each huc unit, first define message variable and categories.
        all_messages = []
        categories = ['action', 'minor', 'moderate', 'major', 'record']

        nws_lids = huc_dictionary[huc]

        if len(nws_lids) == 0:
            MP_LOG.lprint(f"huc {huc} has no applicable nws_lids")
            return

        # less columns then stage cols
        df_cols = {
            "nws_lid": pd.Series(dtype='str'),
            "name": pd.Series(dtype='str'),
            "WFO": pd.Series(dtype='str'),
            "rfc": pd.Series(dtype='str'),
            "huc": pd.Series(dtype='str'),
            "state": pd.Series(dtype='str'),
            "county": pd.Series(dtype='str'),
            "magnitude": pd.Series(dtype='str'),
            "q": pd.Series(dtype='str'),
            "q_uni": pd.Series(dtype='str'),
            "q_src": pd.Series(dtype='str'),
            "stage": pd.Series(dtype='float'),
            "stage_uni": pd.Series(dtype='str'),
            "s_src": pd.Series(dtype='str'),
            "wrds_time": pd.Series(dtype='str'),
            "nrldb_time": pd.Series(dtype='str'),
            "nwis_time": pd.Series(dtype='str'),
            "lat": pd.Series(dtype='float'),
            "lon": pd.Series(dtype='float'),
        }

        # Loop through each lid in list to create flow file
        for lid in nws_lids:
            MP_LOG.lprint("-----------------------------------")
            huc_lid_id = f"{huc} : {lid}"
            MP_LOG.lprint(f"processing {huc_lid_id}")

            # Convert lid to lower case
            lid = lid.lower()

            # Check whether LID is in the restricted sites list
            found_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]

            # Assume only one rec for now, fix later
            if len(found_restrict_lid) > 0:
                reason = found_restrict_lid.iloc[0, found_restrict_lid.columns.get_loc("restricted_reason")]
                msg = ': Restricted Site - ' + reason
                all_messages.append(lid + msg)
                MP_LOG.warning(huc_lid_id + msg)
                continue

            # TODO:  Jun 17, 2024 - This gets recalled for every huc but only uses the nws_list.
            # Move this somewhere outside the huc list so it doesn't need to be called over and over again

            # Careful, for "all_message.append" the syntax into it must be f'{lid}: (whever messages)
            # this is gets parsed and logic used against it.

            stages, flows, status_msg = load_site_thresholds(threshold_file, lid)

            # MP_LOG.lprint(status_msg) # TEMP DEBUG

            # Update status if flows are not found
            if flows is None or len(flows) == 0:  # Changed to flows Sept' 25
                if "WRDS response sucessful." in status_msg:
                    msg = ':WRDS response sucessful but no flow values available'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue
                else:
                    msg = ':Error getting flows values from WRDS API'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

            # Check if stages are supplied, if not write message and exit.
            if all(stages.get(category, None) is None for category in categories):
                message = f'{lid}:Missing all stage data'
                all_messages.append(message)
                MP_LOG.warning(f"{huc} - {message}")
                continue

            # Check if calculated flows are supplied, if not write message and exit.
            if all(flows.get(category, None) is None for category in categories):
                message = f'{lid}:Missing all calculated flows for all stages'
                all_messages.append(message)
                MP_LOG.warning(f"{huc} - {message}")
                continue

            # Sept 2024: Can flows be missing a category, yes, but we jsut filter them later

            # Find lid metadata from master list of metadata dictionaries (line 66).
            metadata = next(
                (item for item in all_meta_lists if item['identifiers']['nws_lid'] == lid.upper()), False
            )

            # Sept 2024: Should we skip these functions that are seen in stage based? Yes
            #    Flow doesn't need all of the elevation stuff
            #     acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id)
            #     lid_usgs_elev, dem_eval_messages = __adj_dem_evalation_val(
            #     lid_altitude = metadata['usgs_data']['altitude']
            #     Filter out sites that don't have "good" data (coords)
            #     datum_adj_ft, datum_messages = __adjust_datum_ft

            # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            unfiltered_segments = list(set(get_nwm_segs(metadata)))
            desired_order = metadata['nwm_feature_data']['stream_order']

            # Filter segments to be of like stream order.
            segments = filter_nwm_segments_by_stream_order(
                unfiltered_segments, desired_order, nwm_flows_region_df
            )
            # Previous input was nwm_flows_df, but now it is region specific df (9/25/25)

            # If there are no segments, write message and exit out
            if not segments or len(segments) == 0:
                message = f'{lid}:Missing nwm stream segments'
                all_messages.append(message)
                MP_LOG.warning(f"{huc} - {message}")
                continue

            missing_wrds_data_msg = ""
            invalid_flows = []
            # If we got here, we have at least one value stage/threshold
            # For each flood category
            for category in categories:
                # In stage we use the threshold data here from WRDS, but here it is flows
                flow = flows[category]

                MP_LOG.trace(f"{huc} : {lid} : {category} : flow value is {flow}")

                if flow is not None and flow != 0 and flow != "":

                    # If there is a valid flow value, write a flow file.
                    # if flow:
                    # round flow to nearest hundredth
                    flow = round(flow, 2)

                    # value of flow shows up in the {lid}_attributes.csv file as the "q" column

                    # Create the guts of the flow file.
                    flow_info = flow_data(segments, flow)

                    # Define destination path and create folders
                    csv_output_folder = os.path.join(output_flows_dir, huc, lid, category)
                    os.makedirs(csv_output_folder, exist_ok=True)
                    output_file = os.path.join(csv_output_folder, f'{lid}_huc_{huc}_flows_{category}.csv')

                    # Write flow file to file
                    flow_info.to_csv(output_file, index=False)

                else:
                    if missing_wrds_data_msg == "":
                        missing_wrds_data_msg = f":---Missing flow data for {category}"
                    else:
                        missing_wrds_data_msg += f"; {category}"

                    invalid_flows.append(category)

            if len(invalid_flows) == 5:
                msg = ':No valid flow values are available'
                all_messages.append(lid + msg)
                MP_LOG.warning(huc_lid_id + msg)
                continue

            if missing_wrds_data_msg != "":
                all_messages.append(lid + missing_wrds_data_msg)
                MP_LOG.warning(huc_lid_id + missing_wrds_data_msg)

            # Get various attributes of the site.
            lat = float(metadata['nws_preferred']['latitude'])
            lon = float(metadata['nws_preferred']['longitude'])
            wfo = metadata['nws_data']['wfo']
            rfc = metadata['nws_data']['rfc']
            state = metadata['nws_data']['state']
            county = metadata['nws_data']['county']
            name = metadata['nws_data']['name']
            flow_source = flows['source']
            stage_source = stages['source']
            wrds_timestamp = stages['wrds_timestamp']
            nrldb_timestamp = metadata['nrldb_timestamp']
            nwis_timestamp = metadata['nwis_timestamp']

            # Create a csv with same information as shapefile but with each threshold as new record.
            # if we got here, mapped is fine.
            csv_df = pd.DataFrame(df_cols)  # for first appending
            for threshold in categories:
                try:
                    line_df = pd.DataFrame(
                        {
                            'nws_lid': [lid],
                            'name': name,
                            'WFO': wfo,
                            'rfc': rfc,
                            'huc': [huc],
                            'state': state,
                            'county': county,
                            'magnitude': threshold,
                            'q': flows[threshold],
                            'q_uni': flows['units'],
                            'q_src': flow_source,
                            'stage': stages[threshold],
                            'stage_uni': stages['units'],
                            's_src': stage_source,
                            'wrds_time': wrds_timestamp,
                            'nrldb_time': nrldb_timestamp,
                            'nwis_time': nwis_timestamp,
                            'lat': [lat],
                            'lon': [lon],
                        }
                    )
                    csv_df = pd.concat([csv_df, line_df], ignore_index=True)

                except Exception:
                    # is this the text we want users to see
                    msg = f':Error with flow/threshold processing {threshold}'
                    all_messages.append(huc_lid_id + msg)
                    MP_LOG.error(huc_lid_id + msg)
                    MP_LOG.error(traceback.format_exc())
                    continue
                # sys.exit(1)

            # might be that none of the lids for this HUC passed
            # MP_LOG.trace(f"len(csv_df) is {len(csv_df)}")
            if len(csv_df) > 0:
                # Round flow and stage columns to 2 decimal places.
                csv_df = csv_df.round({'q': 2, 'stage': 2})

                # Export DataFrame to csv containing attributes
                file_name = os.path.join(attributes_dir, f'{lid}_attributes.csv')
                csv_df.to_csv(file_name, index=False)

                if missing_wrds_data_msg == "":
                    all_messages.append(lid + ':Good')
            else:
                msg = ':Missing all calculated flows'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)

            MP_LOG.success(f'{huc_lid_id}: Complete')

        # Write all_messages by HUC to be scraped later.
        if len(all_messages) > 0:

            # Check for duplicate sites in the messages
            lids = [msg.split(':')[0] for msg in all_messages]
            duplicate_lids = set([x for x in lids if lids.count(x) > 1])

            # If there are duplicate sites and one of the lines has '---', drop that line
            filtered_messages = [
                msg for msg in all_messages if not (msg.split(':')[0] in duplicate_lids and ':---' in msg)
            ]

            # TODO: Aug 2024: This is now identical to the way flow handles messages
            # but the system should probably be changed to somethign more elegant but good enough
            # for now. At least is is MP safe.

            # Write filtered_messages to huc-specific file.
            # MP_LOG.lprint(f'Writing message file for {huc}')
            huc_messages_txt_file = os.path.join(huc_messages_dir, str(huc) + '_messages.txt')
            with open(huc_messages_txt_file, 'w') as f:
                for item in filtered_messages:
                    item = item.strip()
                    # f.write("%s\n" % item)
                    f.write(f"{item}\n")
            # MP_LOG.lprint(f'--- generate_flow_for_huc done for {huc}')

        # end_time = datetime.now(timezone.utc)
        # dt_string = end_time.strftime("%m/%d/%Y %H:%M:%S")
        # time_duration = end_time - start_time
        # MP_LOG.lprint(f" ... {huc} end time is {dt_string} :  Duration: {str(time_duration).split('.')[0]}")
        # print("")

    except Exception as ex:
        MP_LOG.error(f"An error occured while generating flows for huc {huc}")
        MP_LOG.error(f"Details: {ex}")
        MP_LOG.error(traceback.format_exc())

    print("")
    return


# This is called from within this script and is not MP, so it can use FLOG directly
def generate_flows(
    output_catfim_dir,
    nwm_us_search,
    nwm_ds_search,
    env_file,
    job_number_huc,
    is_stage_based,
    lst_hucs,
    nwm_meta_file,
    log_output_file,
    df_restricted_sites,
    threshold_file,
):
    '''
    Runs for both stage- and flow-based CatFIM (but with different outputs/endpoints).

    Generates static flow files for all NWS LIDs and saves them to the specified workspace directory.
    The function supports both stage-based and flow-based inundation mapping workflows.
    For each HUC, the function:
        - Loads NWM flow metadata and region-specific flowline data.
        - Aggregates sites by HUC using spatial joins.
        - Generates flow files for each threshold (action/minor/moderate/major) using WRDS API data.
        - Handles multiprocessing for HUC-level flow generation.
        - Merges logs and attributes from child processes.
        - Produces summary CSV and GeoPackage files for mapped sites and their statuses.

    Parameters
    ----------
        output_catfim_dir (str): Root directory for the CATFIM run output (e.g., 'fim_4_3_3_4_stage_based').
        nwm_us_search (int or str): Upstream search distance (in miles) for traversing the NWM network.
        nwm_ds_search (int or str): Downstream search distance (in miles) for traversing the NWM network.
        env_file (str): Path to the environment file containing API and layer configuration.
        job_number_huc (int): Number of parallel jobs to run for HUC-level processing.
        is_stage_based (bool): If True, runs in stage-based mode and returns early with relevant objects.
        lst_hucs (list of str): List of HUC codes to process.
        nwm_meta_file (str): Path to NWM metadata file (may be empty).
        log_output_file (str): Path to the log output file for logging process information.
        df_restricted_sites (pandas.DataFrame): DataFrame of restricted sites to exclude from processing.
        threshold_file (str): Path to file containing threshold definitions for mapping.

    Returns
    -------
    If is_stage_based is True, returns a tuple:
        (huc_dictionary, out_gdf, metadata_url, threshold_url, all_meta_lists, flows_df_dict)
    Otherwise, returns None (results are written to disk).


    Side Effects
    ------------
    - Writes flow files, attribute CSVs, and GeoPackages to output directories.
      Saves to the workspace directory with the following format:
        <huc>/<lid>/<threshold>/flow file (ahps_{lid code}_huc_{huc 8 code}_flows_{threshold}.csv)
    - Logs process information and errors.
    - Merges and finalizes mapping results for visualization and downstream use.

    Notes
    -----
    - Handles special regions (Guam, American Samoa, Alaska) with region-specific flowline data.
    - Uses multiprocessing for efficient HUC-level flow generation.
    - Produces summary files for mapped and unmapped sites, including status messages.
    '''

    FLOG.setup(log_output_file)  # reusing the parent logs

    FLOG.lprint("Gettting flows")
    # FLOG.trace("args coming into generate flows")
    # FLOG.trace(locals()) # see all args coming in to the function

    attributes_dir = os.path.join(output_catfim_dir, 'attributes')
    os.makedirs(attributes_dir, exist_ok=True)

    mapping_dir = os.path.join(output_catfim_dir, "mapping")  # create var but don't make folder yet

    all_start = datetime.now(timezone.utc)
    API_BASE_URL, WBD_LAYER = get_env_paths(env_file)
    nwm_us_search = int(nwm_us_search)
    nwm_ds_search = int(nwm_ds_search)
    metadata_url = f'{API_BASE_URL}/metadata'
    threshold_url = f'{API_BASE_URL}/nws_threshold'
    ###################################################################

    # Create HUC message directory to store messages that will be read and joined after multiprocessing
    huc_messages_dir = os.path.join(mapping_dir, 'huc_messages')
    if not os.path.exists(huc_messages_dir):
        os.mkdir(huc_messages_dir)

    FLOG.lprint("Loading nwm flow metadata")
    start_dt = datetime.now(timezone.utc)

    # Open NWM flows geopackages
    # TODO: Pull from bash_variables.env once we switch from using catfim.env to bash_variables.env
    nwm_flows_gpkg = r'/data/inputs/nwm_hydrofabric/nwm_flows.gpkg'
    nwm_flows_alaska_gpkg = r'/data/inputs/nwm_hydrofabric/nwm_flows_alaska_nwmV3_ID.gpkg'
    input_nhd_flows_Guam = r'/data/inputs/nhdplus/Guam_6637/NHDFlowline_Guam_6637.gpkg'
    input_nhd_flows_AmericanSamoa = (
        r'/data/inputs/nhdplus/AmericanSamoa_32702/NHDFlowline_AmericanSamoa_32702.gpkg'
    )

    nwm_flows_df = gpd.read_file(nwm_flows_gpkg)
    nwm_flows_alaska_df = gpd.read_file(nwm_flows_alaska_gpkg)
    nhd_flows_guam_df = gpd.read_file(input_nhd_flows_Guam)
    nhd_flows_americansamoa_df = gpd.read_file(input_nhd_flows_AmericanSamoa)

    # Add the dfs to a dictionary for easy access later
    flows_df_dict = {
        'nwm_flows_df': nwm_flows_df,
        'nwm_flows_alaska_df': nwm_flows_alaska_df,
        'nhd_flows_guam_df': nhd_flows_guam_df,
        'nhd_flows_americansamoa_df': nhd_flows_americansamoa_df,
    }

    # nwm_meta_file might be an empty string
    # maybe ensure all projections are changed to one standard output of 3857 (see shared_variables) as the come out

    # TODO: Aug 2024:
    # Filter the meta list to just HUCs in the fim run output or huc if sent in as a param
    # all_meta_lists = __load_nwm_metadata( # TODO: Update in Guam branch
    #     output_catfim_dir, metadata_url, nwm_us_search, nwm_ds_search, nwm_meta_file
    # )

    # Open metadata file
    with open(nwm_meta_file, "rb") as p_handle:
        all_meta_lists = pickle.load(p_handle)

    end_dt = datetime.now(timezone.utc)
    time_duration = end_dt - start_dt
    FLOG.lprint(f"Retrieving metadata - Duration: {str(time_duration).split('.')[0]}")

    # FLOG.lprint("+++++++++++++++++++")
    # FLOG.lprint(f"all_meta_lists is {all_meta_lists}")
    # FLOG.lprint("+++++++++++++++++++")

    print("")

    # Assign HUCs to all sites using a spatial join of the FIM 4 HUC layer.
    # Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
    # of all sites used later in script.
    FLOG.lprint("Start aggregate_wbd_hucs")
    start_dt = datetime.now(timezone.utc)

    huc_dictionary, out_gdf = aggregate_wbd_hucs(all_meta_lists, WBD_LAYER, True, lst_hucs)

    # Drop list fields if invalid
    out_gdf = out_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')
    out_gdf = out_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')

    if 'metadata_sources' in out_gdf.columns:  # TODO: Is this column needed/used? Changed to accomodate Guam
        out_gdf = out_gdf.astype({'metadata_sources': str})

    FLOG.lprint("+++++++++++++")
    FLOG.lprint("Start Flow Generation")

    end_dt = datetime.now(timezone.utc)
    time_duration = end_dt - start_dt
    FLOG.lprint(f"End aggregate_wbd_hucs - Duration: {str(time_duration).split('.')[0]}")

    # It this is stage-based, it returns all of these objects here, but if it continues
    # (aka. Flow based), then it returns only nws_lid_layer (created later in this function)
    if is_stage_based:  # If it's stage-based, the function stops running here
        return (huc_dictionary, out_gdf, metadata_url, threshold_url, all_meta_lists, flows_df_dict)

    # only flow based needs the "flow" dir
    output_flows_dir = os.path.join(output_catfim_dir, "flows")
    if not os.path.exists(output_flows_dir):
        os.mkdir(output_flows_dir)

    start_dt = datetime.now(timezone.utc)

    # pulls out the parent log file and replaces it with the child prefix
    # catfim if coming from generate_categorical_fim.py
    child_log_file_prefix = FLOG.MP_calc_prefix_name(log_output_file, "MP_process_gen_flows")
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in huc_dictionary:

            # Get the correct nwm_flows_region_df based on the HUC
            if huc[:4] == '2201':  # Guam
                nwm_flows_region_df = flows_df_dict['nhd_flows_guam_df']
            elif huc[:4] == '2203':  # American Samoa
                nwm_flows_region_df = flows_df_dict['nhd_flows_americansamoa_df']
            elif huc[:2] == '19':  # Alaska
                nwm_flows_region_df = flows_df_dict['nwm_flows_alaska_df']
            else:  # CONUS + Hawaii + Puerto Rico
                nwm_flows_region_df = flows_df_dict['nwm_flows_df']

            # Deep copy that speed up Multi-Proc a little as all_meta_lists
            # is a huge object. Need to figure out how to filter that down somehow
            # later. Can not just filter by huc per loop, tried it and there are other factors
            copy_all_meta_lists = copy.copy(all_meta_lists)
            executor.submit(
                generate_flows_for_huc,
                huc,
                huc_dictionary,
                threshold_url,
                copy_all_meta_lists,
                output_flows_dir,
                attributes_dir,
                huc_messages_dir,
                nwm_flows_region_df,
                log_output_file,
                child_log_file_prefix,
                df_restricted_sites,
                output_catfim_dir,
                threshold_file,
            )
    # end ProcessPoolExecutor

    # rolls up logs from child MP processes into this parent_log_output_file
    FLOG.merge_log_files(log_output_file, child_log_file_prefix, True)

    end_dt = datetime.now(timezone.utc)
    time_duration = end_dt - start_dt
    FLOG.lprint(f"End flow generation - Duration: {str(time_duration).split('.')[0]}")
    print()

    FLOG.lprint('Start merging and finalizing flows generation data')
    # Recursively find all *_attributes csv files and append
    # attrib_csv_files = [x for x in os.listdir(attributes_dir) if x.endswith('_attributes.csv')]
    attrib_csv_files = glob.glob(f"{attributes_dir}/*_attributes.csv")

    if len(attrib_csv_files) == 0:
        FLOG.critical(f"No new flow files exist in the {attributes_dir} folder (errors in creating them?)")
        sys.exit(1)

    all_csv_df = pd.DataFrame()
    for csv_file in attrib_csv_files:
        full_csv_path = os.path.join(attributes_dir, csv_file)
        # Huc has to be read in as string to preserve leading zeros.
        temp_df = pd.read_csv(full_csv_path, dtype={'huc': str})
        all_csv_df = pd.concat([all_csv_df, temp_df], ignore_index=True)
    # Write to file
    all_csv_df.to_csv(os.path.join(attributes_dir, 'nws_lid_attributes.csv'), index=False)

    # This section populates a shapefile of all potential sites and details
    # whether it was mapped or not (mapped field) and if not, why (status field).
    # Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.

    viz_out_gdf = out_gdf.to_crs(VIZ_PROJECTION)
    viz_out_gdf.rename(
        columns={
            'identifiers_nwm_feature_id': 'nwm_seg',
            'identifiers_nws_lid': 'nws_lid',
            'identifiers_usgs_site_code': 'usgs_gage',
        },
        inplace=True,
    )
    viz_out_gdf['nws_lid'] = viz_out_gdf['nws_lid'].str.lower()

    # Using list of csv_files, populate DataFrame of all nws_lids that had
    # a flow file produced and denote with "mapped" column.
    nws_lids = []
    for csv_file in attrib_csv_files:
        nws_lids.append(csv_file.split('_attributes')[0])
    lids_df = pd.DataFrame(nws_lids, columns=['nws_lid'])
    # lids_df['mapped'] = 'yes'

    # Identify what lids were mapped by merging with lids_df. Populate
    # 'mapped' column with 'No' if sites did not map.
    viz_out_gdf = viz_out_gdf.merge(lids_df, how='left', on='nws_lid')
    viz_out_gdf.reset_index(inplace=True, drop=True)
    if 'mapped' not in viz_out_gdf.columns:
        viz_out_gdf['mapped'] = 'no'
    else:
        viz_out_gdf['mapped'] = viz_out_gdf['mapped'].fillna('no')

    # Read all messages for all HUCs
    # this is basically identical to a stage based set. Seach for huc_message_list and see my notes
    huc_message_list = []
    huc_messages_dir_list = os.listdir(huc_messages_dir)
    for huc_message_file in huc_messages_dir_list:
        full_path_file = os.path.join(huc_messages_dir, huc_message_file)
        with open(full_path_file, 'r') as f:
            if full_path_file.endswith('.txt'):
                lines = f.readlines()
                for line in lines:
                    line = line.strip()
                    huc_message_list.append(line)

    # Write messages to DataFrame, split into columns, aggregate messages.
    if len(huc_message_list) > 0:

        messages_df = pd.DataFrame(huc_message_list, columns=['message'])
        messages_df = (
            messages_df['message']
            .str.split(':', n=1, expand=True)
            .rename(columns={0: 'nws_lid', 1: 'status'})
        )

        # We want one viz_out_gdf record per ahps and if there are more than one, contact the messages

        # status_df = messages_df.groupby(['nws_lid'])['status'].apply(', '.join).reset_index()
        # df1 = df.groupby(['ID1','ID2'])['Status'].agg(lambda x: ','.join(x.dropna())).reset_index()
        status_df = messages_df.groupby(['nws_lid'])['status'].agg(lambda x: ',\n'.join(x)).reset_index()

        # some messages status values start with a space as the first character. Remove it
        # status_df["status"] = status_df["status"].apply(lambda x: x.strip())

        # Join messages to populate status field to candidate sites. Assign
        # status for null fields.
        viz_out_gdf = viz_out_gdf.merge(status_df, how='left', on='nws_lid')

        viz_out_gdf['status'] = viz_out_gdf['status'].fillna('Good')
        # viz_out_gdf['status'] = viz_out_gdf['status'].apply(lambda x: x[3:] if x.startswith("---") else x)

        # There could be duplicate message for one ahps (ie. missing nwm segments), so drop dups
        messages_df.drop_duplicates(subset=["nws_lid", "status"], keep="first", inplace=True)

    # Filter out columns and write out to file
    # viz_out_gdf = viz_out_gdf.filter(
    #     ['nws_lid', 'usgs_gage', 'nwm_seg', 'HUC8', 'mapped', 'status', 'geometry']
    # )

    # rename the column from nws_lid to ahps_lid
    viz_out_gdf.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)

    # stage based doesn't get here
    # crs is 3857 - web mercator at this point

    # The csv will be updated later if something fails during inundation
    nws_lid_csv_file_path = os.path.join(mapping_dir, 'flow_based_catfim_sites.csv')
    viz_out_gdf.to_csv(nws_lid_csv_file_path)

    catfim_sites_gpkg_file_path = os.path.join(mapping_dir, 'flow_based_catfim_sites.gpkg')
    viz_out_gdf.to_file(catfim_sites_gpkg_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine='fiona')

    # time operation
    all_end = datetime.now(timezone.utc)
    all_time_duration = all_end - all_start
    FLOG.lprint(f"End Wrapping up flows generation Duration: {str(all_time_duration).split('.')[0]}")
    print()


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Create forecast files for all nws_lid sites')
    parser.add_argument(
        '-w', '--output_catfim_dir', help='Workspace where all data will be stored.', required=True
    )

    parser.add_argument(
        '-log',
        '--log_output_file',
        help='REQUIRED: Path to where the output log file will be.'
        r'ie) /data/catfim/rob_test/logs/catfim_2024_07_07-22_26_18.log',
        required=True,
        type=str,
    )
    parser.add_argument(
        '-e',
        '--env_file',
        help='OPTIONAL: Docker mount path to the catfim environment file.'
        ' Defaults to: data/config/fim_enviro_values.env',
        default="/data/config/fim_enviro_values.env",
        required=False,
    )
    parser.add_argument(
        '-hucs',
        '--lst_hucs',
        help='list of hucs that you want to process. ie) -hucs 12090301 01100006 12040101',
        required=True,
        type=str,
        nargs='+',
    )

    parser.add_argument(
        '-u',
        '--nwm_us_search',
        help='Walk upstream on NWM network this many miles',
        required=False,
        default=5,
    )

    parser.add_argument(
        '-d',
        '--nwm_ds_search',
        help='Walk downstream on NWM network this many miles',
        required=False,
        default=5,
    )

    parser.add_argument(
        '-jh',
        '--job_number_huc',
        help='OPTIONAL: Number of processes to use for HUC scale operations.'
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count of the'
        ' machine. CatFIM sites generally only have 2-3 branches overlapping a site, so this number can be '
        'kept low (2-4). Defaults to 1.',
        required=False,
        default=1,
        type=int,
    )

    parser.add_argument(
        '-a',
        '--is_stage_based',
        help='Is this a stage based or flow based run? Add the -a to mean is_stage_based is True ',
        required=False,
        default=False,
        action='store_true',
    )

    parser.add_argument(
        '-n',
        '--nwm_meta_file',
        help='OPTIONAL: Path to the pre-made pickle file that already holds the nwm metadata',
        required=False,
        type=str,
        default="",
    )

    parser.add_argument(
        '-tf',
        '--threshold-file',
        help='OPTIONAL: Path to the pre-made pickle file that already holds the thresholds',
        required=False,
        type=str,
        default="",
    )

    args = vars(parser.parse_args())

    # Run get_env_paths and static_flow_lids
    generate_flows(**args)
