#!/usr/bin/env python3
import argparse
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from tools_shared_functions import (
    aggregate_wbd_hucs,
    filter_nwm_segments_by_stream_order,
    flow_data,
    get_datum,
    get_metadata,
    get_nwm_segs,
    get_thresholds,
    mainstem_nwm_segs,
    ngvd_to_navd_ft,
)


sys.path.append('/foss_fim/src')
from utils.shared_variables import VIZ_PROJECTION


def get_env_paths():
    load_dotenv()
    # import variables from .env file
    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    return API_BASE_URL, WBD_LAYER


def process_generate_flows(
    huc,
    huc_dictionary,
    threshold_url,
    all_lists,
    workspace,
    attributes_dir,
    huc_messages_dir,
    nwm_flows_df,
):
    # Process each huc unit, first define message variable and flood categories.
    all_messages = []
    flood_categories = ['action', 'minor', 'moderate', 'major', 'record']

    print(f'Iterating through {huc}')
    # Get list of nws_lids
    nws_lids = huc_dictionary[huc]
    # Loop through each lid in list to create flow file
    for lid in nws_lids:
        # Convert lid to lower case
        lid = lid.lower()
        # Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
        print("getting thresholds")
        stages, flows = get_thresholds(
            threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
        )
        if stages == None or flows == None:
            print("Likely WRDS error")
            continue
        # Check if stages are supplied, if not write message and exit.
        if all(stages.get(category, None) == None for category in flood_categories):
            message = f'{lid}:missing threshold stages'
            all_messages.append(message)
            continue
        # Check if calculated flows are supplied, if not write message and exit.
        if all(flows.get(category, None) == None for category in flood_categories):
            message = f'{lid}:missing calculated flows'
            all_messages.append(message)
            continue
        # find lid metadata from master list of metadata dictionaries (line 66).
        metadata = next(
            (item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False
        )

        # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
        unfiltered_segments = list(set(get_nwm_segs(metadata)))

        desired_order = metadata['nwm_feature_data']['stream_order']
        # Filter segments to be of like stream order.
        print("filtering segments")
        start = time.time()
        segments = filter_nwm_segments_by_stream_order(
            unfiltered_segments, desired_order, nwm_flows_df
        )
        end = time.time()
        elapsed_time = round(((end - start) / 60), 6)
        print(f'Finished filtering segments in {elapsed_time} minutes')
        # if no segments, write message and exit out
        if not segments:
            message = f'{lid}:missing nwm segments'
            all_messages.append(message)
            continue
        # For each flood category
        for category in flood_categories:
            # G et the flow
            flow = flows[category]
            # If there is a valid flow value, write a flow file.
            if flow:
                # round flow to nearest hundredth
                flow = round(flow, 2)
                # Create the guts of the flow file.
                flow_info = flow_data(segments, flow)
                # Define destination path and create folders
                output_file = (
                    workspace
                    / huc
                    / lid
                    / category
                    / (f'ahps_{lid}_huc_{huc}_flows_{category}.csv')
                )
                output_file.parent.mkdir(parents=True, exist_ok=True)
                # Write flow file to file
                flow_info.to_csv(output_file, index=False)
            else:
                message = f'{lid}:{category} is missing calculated flow'
                all_messages.append(message)
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
        csv_df = pd.DataFrame()
        for threshold in flood_categories:
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
            csv_df = pd.concat([csv_df, line_df])
        # Round flow and stage columns to 2 decimal places.
        csv_df = csv_df.round({'q': 2, 'stage': 2})

        # If a site folder exists (ie a flow file was written) save files containing site attributes.
        output_dir = workspace / huc / lid
        if output_dir.exists():
            # Export DataFrame to csv containing attributes
            csv_df.to_csv(os.path.join(attributes_dir, f'{lid}_attributes.csv'), index=False)
            message = f'{lid}:flows available'
            all_messages.append(message)
        else:
            message = f'{lid}:missing all calculated flows'
            all_messages.append(message)

    # Write all_messages to huc-specific file.
    print("Writing message file for huc")
    huc_messages_txt_file = os.path.join(huc_messages_dir, str(huc) + '_messages.txt')
    with open(huc_messages_txt_file, 'w') as f:
        for item in all_messages:
            f.write("%s\n" % item)


def generate_catfim_flows(
    workspace,
    nwm_us_search,
    nwm_ds_search,
    stage_based,
    fim_dir,
    lid_to_run,
    attributes_dir="",
    job_number_huc=1,
):
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
    workspace : STR
        Location where output flow files will exist.
    nwm_us_search : STR
        Upstream distance (in miles) for walking up NWM network.
    nwm_ds_search : STR
        Downstream distance (in miles) for walking down NWM network.
    wbd_path : STR
        Location of HUC geospatial data (geopackage).

    Returns
    -------
    None.
    '''

    all_start = datetime.now()
    API_BASE_URL, WBD_LAYER = get_env_paths()
    # Define workspace and wbd_path as a pathlib Path. Convert search distances to integer.
    workspace = Path(workspace)
    nwm_us_search = int(nwm_us_search)
    nwm_ds_search = int(nwm_ds_search)
    metadata_url = f'{API_BASE_URL}/metadata'
    threshold_url = f'{API_BASE_URL}/nws_threshold'
    ###################################################################

    # Create workspace
    workspace.mkdir(parents=True, exist_ok=True)

    # Create HUC message directory to store messages that will be read and joined after multiprocessing
    huc_messages_dir = os.path.join(workspace, 'huc_messages')
    if not os.path.exists(huc_messages_dir):
        os.mkdir(huc_messages_dir)

    # Open NWM flows geopackage
    nwm_flows_gpkg = r'/data/inputs/nwm_hydrofabric/nwm_flows.gpkg'
    nwm_flows_df = gpd.read_file(nwm_flows_gpkg)

    print(f'Retrieving metadata for site(s): {lid_to_run}...')
    start_dt = datetime.now()

    # Get metadata for 'CONUS'
    print(metadata_url)
    if lid_to_run != 'all':
        all_lists, conus_dataframe = get_metadata(
            metadata_url,
            select_by='nws_lid',
            selector=[lid_to_run],
            must_include='nws_data.rfc_forecast_point',
            upstream_trace_distance=nwm_us_search,
            downstream_trace_distance=nwm_ds_search,
        )
    else:
        # Get CONUS metadata
        conus_list, conus_dataframe = get_metadata(
            metadata_url,
            select_by='nws_lid',
            selector=['all'],
            must_include='nws_data.rfc_forecast_point',
            upstream_trace_distance=nwm_us_search,
            downstream_trace_distance=nwm_ds_search,
        )
        # Get metadata for Islands
        islands_list, islands_dataframe = get_metadata(
            metadata_url,
            select_by='state',
            selector=['HI', 'PR'],
            must_include=None,
            upstream_trace_distance=nwm_us_search,
            downstream_trace_distance=nwm_ds_search,
        )
        # Append the dataframes and lists
        all_lists = conus_list + islands_list
    print(len(all_lists))

    end_dt = datetime.now()
    time_duration = end_dt - start_dt
    print(f"Retrieving metadata Duration: {str(time_duration).split('.')[0]}")
    print()

    print('Determining HUC using WBD layer...')
    start_dt = datetime.now()

    # Assign HUCs to all sites using a spatial join of the FIM 3 HUC layer.
    # Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
    # of all sites used later in script.
    huc_dictionary, out_gdf = aggregate_wbd_hucs(
        metadata_list=all_lists, wbd_huc8_path=WBD_LAYER, retain_attributes=True
    )
    # Drop list fields if invalid
    out_gdf = out_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')
    out_gdf = out_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')
    out_gdf = out_gdf.astype({'metadata_sources': str})

    end_dt = datetime.now()
    time_duration = end_dt - start_dt
    print(f"Determining HUC using WBD layer Duration: {str(time_duration).split('.')[0]}")
    print()

    if stage_based:
        return huc_dictionary, out_gdf, metadata_url, threshold_url, all_lists, nwm_flows_df

    print("Generating flows for hucs using " + str(job_number_huc) + " jobs...")
    start_dt = datetime.now()

    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in huc_dictionary:
            executor.submit(
                process_generate_flows,
                huc,
                huc_dictionary,
                threshold_url,
                all_lists,
                workspace,
                attributes_dir,
                huc_messages_dir,
                nwm_flows_df,
            )

    end_dt = datetime.now()
    time_duration = end_dt - start_dt
    print(f"Generating flows for hucs Duration: {str(time_duration).split('.')[0]}")
    print()

    print('Wrapping up flows generation...')
    # Recursively find all *_attributes csv files and append
    csv_files = os.listdir(attributes_dir)
    all_csv_df = pd.DataFrame()
    for csv in csv_files:
        full_csv_path = os.path.join(attributes_dir, csv)
        # Huc has to be read in as string to preserve leading zeros.
        temp_df = pd.read_csv(full_csv_path, dtype={'huc': str})
        all_csv_df = pd.concat([all_csv_df, temp_df], ignore_index=True)
    # Write to file
    all_csv_df.to_csv(os.path.join(workspace, 'nws_lid_attributes.csv'), index=False)

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
    for csv_file in csv_files:
        nws_lids.append(csv_file.split('_attributes')[0])
    lids_df = pd.DataFrame(nws_lids, columns=['nws_lid'])
    lids_df['mapped'] = 'yes'

    # Identify what lids were mapped by merging with lids_df. Populate
    # 'mapped' column with 'No' if sites did not map.
    viz_out_gdf = viz_out_gdf.merge(lids_df, how='left', on='nws_lid')
    viz_out_gdf['mapped'] = viz_out_gdf['mapped'].fillna('no')

    # Read all messages for all HUCs TODO
    huc_message_list = []
    huc_messages_dir_list = os.listdir(huc_messages_dir)
    for huc_message_file in huc_messages_dir_list:
        full_path_file = os.path.join(huc_messages_dir, huc_message_file)
        with open(full_path_file, 'r') as f:
            if full_path_file.endswith('.txt'):
                lines = f.readlines()
                for line in lines:
                    huc_message_list.append(line)

    # Write messages to DataFrame, split into columns, aggregate messages.
    messages_df = pd.DataFrame(huc_message_list, columns=['message'])
    messages_df = (
        messages_df['message']
        .str.split(':', n=1, expand=True)
        .rename(columns={0: 'nws_lid', 1: 'status'})
    )
    status_df = messages_df.groupby(['nws_lid'])['status'].apply(', '.join).reset_index()

    # Join messages to populate status field to candidate sites. Assign
    # status for null fields.
    viz_out_gdf = viz_out_gdf.merge(status_df, how='left', on='nws_lid')
    viz_out_gdf['status'] = viz_out_gdf['status'].fillna('all calculated flows available')

    # Filter out columns and write out to file
    #    viz_out_gdf = viz_out_gdf.filter(['nws_lid','usgs_gage','nwm_seg','HUC8','mapped','status','geometry'])
    nws_lid_layer = os.path.join(workspace, 'nws_lid_sites.gpkg').replace('flows', 'mapping')

    viz_out_gdf.to_file(nws_lid_layer, driver='GPKG')

    # time operation
    all_end = datetime.now()
    all_time_duration = all_end - all_start
    print(f"Duration: {str(all_time_duration).split('.')[0]}")
    print()

    return nws_lid_layer


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Create forecast files for all nws_lid sites')
    parser.add_argument(
        '-w', '--workspace', help='Workspace where all data will be stored.', required=True
    )
    parser.add_argument(
        '-u', '--nwm_us_search', help='Walk upstream on NWM network this many miles', required=True
    )
    parser.add_argument(
        '-d',
        '--nwm_ds_search',
        help='Walk downstream on NWM network this many miles',
        required=True,
    )
    parser.add_argument(
        '-a',
        '--stage_based',
        help='Run stage-based CatFIM instead of flow-based? NOTE: flow-based CatFIM is the default.',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-f',
        '--fim-dir',
        help='Path to FIM outputs directory. Only use this option if you are running in alt-catfim mode.',
        required=False,
        default="",
    )
    args = vars(parser.parse_args())

    # Run get_env_paths and static_flow_lids
    generate_catfim_flows(**args)
