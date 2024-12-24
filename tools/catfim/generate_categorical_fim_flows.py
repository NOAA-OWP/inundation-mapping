#!/usr/bin/env python3

import argparse
import copy
import glob
import os
import pickle
import random
import shutil
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
from utils.shared_variables import VIZ_PROJECTION


# TODO: Aug 2024: This script was upgraded significantly with lots of misc TODO's embedded.
# Lots of inline documenation needs updating as well


# will become global once initiallized
FLOG = fl.FIM_logger()
MP_LOG = fl.FIM_logger()

gpd.options.io_engine = "pyogrio"


def get_env_paths(env_file):

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
    nwm_flows_df,
    parent_log_output_file,
    child_log_file_prefix,
):

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

            # TODO:  Jun 17, 2024 - This gets recalled for every huc but only uses the nws_list.
            # Move this somewhere outside the huc list so it doesn't need to be called over and over again

            # Careful, for "all_message.append" the syntax into it must be f'{lid}: (whever messages)
            # this is gets parsed and logic used against it.

            # for Stage based, is uses stage values from threshold data supplied by WRDS
            # but here (for flow), it uses the values from the flows data from WRDS
            stages, flows = get_thresholds(
                threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
            )

            # Yes.. stages, we will handle missing flows later even though we don't use the stage here
            if stages is None or len(stages) == 0:
                msg = ':Error getting stages values from WRDS API'
                all_messages.append(lid + msg)
                MP_LOG.warning(huc_lid_id + msg)
                continue

            # MP_LOG.lprint(f"Thresholds for {huc_lid_id} are : {thresholds}")

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
            #    Flow doesn't need all of the evalation stuff
            #     acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id)
            #     lid_usgs_elev, dem_eval_messages = __adj_dem_evalation_val(
            #     lid_altitude = metadata['usgs_data']['altitude']
            #     Filter out sites that don't have "good" data (coords)
            #     datum_adj_ft, datum_messages = __adjust_datum_ft

            # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            unfiltered_segments = list(set(get_nwm_segs(metadata)))
            desired_order = metadata['nwm_feature_data']['stream_order']

            # Filter segments to be of like stream order.
            segments = filter_nwm_segments_by_stream_order(unfiltered_segments, desired_order, nwm_flows_df)

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
# lid_to_run is temp disabled
def generate_flows(
    output_catfim_dir,
    nwm_us_search,
    nwm_ds_search,
    lid_to_run,
    env_file,
    job_number_huc,
    is_stage_based,
    lst_hucs,
    nwm_metafile,
    log_output_file,
):

    # TODO; Most docstrings like this are now very outdated and need updating
    '''
    This will create static flow files for all nws_lids and save to the
    workspace directory with the following format:
    huc code
        nws_lid_code
            threshold (action/minor/moderate/major if they exist/are defined by WRDS)
                flow file (ahps_{lid code}_huc_{huc 8 code}_flows_{threshold}.csv)

    This will use the WRDS API to get the nwm segments as well as the flow
    values for each threshold at each nws_lid and then create the necessary
    flow file to use for inundation mapping.
    Parameters
    ----------
    output_catfim_dir : STR
        root catfim dir for the particular run. ie) fim_4_3_3_4_stage_based
    nwm_us_search : STR
        Upstream distance (in miles) for walking up NWM network.
    nwm_ds_search : STR
        Downstream distance (in miles) for walking down NWM network.
    wbd_path : STR
        Location of HUC geospatial data (geopackage).

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
    nwm_flows_gpkg = r'/data/inputs/nwm_hydrofabric/nwm_flows.gpkg'
    nwm_flows_df = gpd.read_file(nwm_flows_gpkg)

    nwm_flows_alaska_gpkg = r'/data/inputs/nwm_hydrofabric/nwm_flows_alaska_nwmV3_ID.gpkg'
    nwm_flows_alaska_df = gpd.read_file(nwm_flows_alaska_gpkg)

    # nwm_metafile might be an empty string
    # maybe ensure all projections are changed to one standard output of 3857 (see shared_variables) as the come out

    # TODO: Aug 2024:
    # Filter the meta list to just HUCs in the fim run output or huc if sent in as a param
    all_meta_lists = __load_nwm_metadata(
        output_catfim_dir, metadata_url, nwm_us_search, nwm_ds_search, lid_to_run, nwm_metafile
    )

    end_dt = datetime.now(timezone.utc)
    time_duration = end_dt - start_dt
    FLOG.lprint(f"Retrieving metadata - Duration: {str(time_duration).split('.')[0]}")

    print("")

    # Assign HUCs to all sites using a spatial join of the FIM 4 HUC layer.
    # Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
    # of all sites used later in script.

    FLOG.lprint("Start aggregate_wbd_hucs")
    start_dt = datetime.now(timezone.utc)

    huc_dictionary, out_gdf = aggregate_wbd_hucs(all_meta_lists, WBD_LAYER, True, lst_hucs)

    # FLOG.lprint(f"WBD LAYER USED: {WBD_LAYER}")  # TEMP DEBUG
    # Drop list fields if invalid
    out_gdf = out_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')
    out_gdf = out_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')
    out_gdf = out_gdf.astype({'metadata_sources': str})

    end_dt = datetime.now(timezone.utc)
    time_duration = end_dt - start_dt
    FLOG.lprint(f"End aggregate_wbd_hucs - Duration: {str(time_duration).split('.')[0]}")

    FLOG.lprint("")
    FLOG.lprint("Start Flow Generation")

    # It this is stage-based, it returns all of these objects here, but if it continues
    # (aka. Flow based), then it returns only nws_lid_layer (created later in this function)
    if is_stage_based:  # If it's stage-based, the function stops running here
        return (
            huc_dictionary,
            out_gdf,
            metadata_url,
            threshold_url,
            all_meta_lists,
            nwm_flows_df,
            nwm_flows_alaska_df,
        )

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

            # nwm_flows_region_df = nwm_flows_df  # To exclude Alaska
            nwm_flows_region_df = (
                nwm_flows_alaska_df if huc[:2] == '19' else nwm_flows_df
            )  # To include Alaska

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


# local script calls __load_nwm_metadata so FLOG is already setup
def __load_nwm_metadata(
    output_catfim_dir, metadata_url, nwm_us_search, nwm_ds_search, lid_to_run, nwm_metafile
):

    FLOG.trace(metadata_url)

    output_meta_list = []
    # Check to see if meta file already exists
    # This feature means we can copy the pickle file to another enviro (AWS?) as it won't need to call
    # WRDS unless we need a smaller or modified version. This one likely has all nws_lid data.

    if os.path.isfile(nwm_metafile) == True:
        FLOG.lprint(f"Meta file already downloaded and exists at {nwm_metafile}")

        with open(nwm_metafile, "rb") as p_handle:
            output_meta_list = pickle.load(p_handle)

    else:
        meta_file = os.path.join(output_catfim_dir, "nwm_metafile.pkl")

        FLOG.lprint(f"Meta file will be downloaded and saved at {meta_file}")

        if lid_to_run != "all": # TODO: Deprecate LID options (in favor of HUC list functionlity)
            # Single lid for now (deprecated)
            output_meta_list, ___ = get_metadata(
                metadata_url,
                select_by='nws_lid',
                selector=[lid_to_run],
                must_include='nws_data.rfc_forecast_point',
                upstream_trace_distance=nwm_us_search,
                downstream_trace_distance=nwm_ds_search,
            )

        else:
            # Dec 2024: Running two API calls: one to get all forecast points, and another 
            # to get all points (non-forecast and forecast) for the OCONUS regions. Then, 
            # duplicate LIDs are removed.

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

            # print(f"len(all_meta_lists) is {len(all_meta_lists)}")
            
            # Filter the metadata list
            output_meta_list = []
            unique_lids, duplicate_lids = [], [] # TODO: remove?
            duplicate_meta_list = []
            nonelid_metadata_list = [] # TODO: remove

            for i, site in enumerate(unfiltered_meta_list):
                nws_lid = site['identifiers']['nws_lid']

                if nws_lid == None:
                    # No LID available
                    nonelid_metadata_list.append(site) # TODO: replace with Continue

                elif nws_lid in unique_lids:
                    # Duplicate LID
                    duplicate_lids.append(nws_lid)
                    duplicate_meta_list.append(site) # TODO: remove extra lists

                else: 
                    # Unique/unseen LID that's not None
                    unique_lids.append(nws_lid)
                    output_meta_list.append(site)

            FLOG.lprint(f'{len(duplicate_lids)} duplicate points removed.')
            FLOG.lprint(f'Filtered metadatada downloaded for {len(output_meta_list)} points.')

        # ----------

        with open(meta_file, "wb") as p_handle:
            pickle.dump(output_meta_list, p_handle, protocol=pickle.HIGHEST_PROTOCOL)

    return output_meta_list


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
        help='Docker mount path to the catfim environment file. ie) data/config/catfim.env',
        required=True,
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
        '--nwm_metafile',
        help='OPTIONAL: Path to the pre-made pickle file that already holds the nwm metadata',
        required=False,
        type=str,
        default="",
    )

    args = vars(parser.parse_args())

    # Run get_env_paths and static_flow_lids
    generate_flows(**args)
