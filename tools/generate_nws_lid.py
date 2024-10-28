#!/usr/bin/env python3

import argparse
import os
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from tools_shared_functions import aggregate_wbd_hucs, get_metadata

from utils.shared_variables import PREP_PROJECTION


gpd.options.io_engine = "pyogrio"


load_dotenv()
# import variables from .env file
API_BASE_URL = os.getenv("API_BASE_URL")
EVALUATED_SITES_CSV = os.getenv("EVALUATED_SITES_CSV")
WBD_LAYER = os.getenv("WBD_LAYER")


def generate_nws_lid(workspace):
    '''
    Generate the nws_lid layer containing all nws_lid points

    Parameters
    ----------
    workspace : STR
        Directory where outputs will be saved.

    Returns
    -------
    None.

    '''

    # Get all nws_lid points
    print('Retrieving metadata ..')

    metadata_url = f'{API_BASE_URL}/metadata/'
    # Trace downstream from all rfc_forecast_point.
    select_by = 'nws_lid'
    selector = ['all']
    must_include = 'nws_data.rfc_forecast_point'
    downstream_trace_distance = 'all'
    fcst_list, fcst_dataframe = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    # Get list of all evaluated sites not in fcst_list
    fcst_list_sites = [record.get('identifiers').get('nws_lid').lower() for record in fcst_list]
    evaluated_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].str.lower().to_list()
    evaluated_sites = list(set(evaluated_sites) - set(fcst_list_sites))

    # Trace downstream from all evaluated sites not in fcst_list
    select_by = 'nws_lid'
    selector = evaluated_sites
    must_include = None
    downstream_trace_distance = 'all'
    eval_list, eval_dataframe = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    # Append all lists
    all_lists = fcst_list + eval_list

    # Compile NWM segments from all_lists

    # Get dictionary of downstream segment (key) and target segments (values)
    # Get dictionary of target segment (key) and site code (value)
    downstream = defaultdict(list)
    target = defaultdict(list)
    # For each lid metadata dictionary in list
    for lid in all_lists:
        site = lid.get('identifiers').get('nws_lid')
        # Get the nwm feature id associated with the location
        location_nwm_seg = lid.get('identifiers').get('nwm_feature_id')
        # get all downstream segments
        downstream_nwm_segs = lid.get('downstream_nwm_features')
        # If valid location_nwm_segs construct two dictionaries.
        if location_nwm_seg:
            # Dictionary with target segment and site
            target[int(location_nwm_seg)].append(site)
            # Dictionary of key (2nd to last element) and value (target segment)
            # 2nd to last element used because last element is always 0 (ocean) and the 2nd to last allows
            # for us to get the river 'tree' (Mississippi, Colorado, etc)
            value = location_nwm_seg
            if not downstream_nwm_segs:
                # Special case, no downstream nwm segments are returned (PR/VI/HI).
                key = location_nwm_seg
            elif len(downstream_nwm_segs) == 1:
                # Special case, the nws_lid is within 1 segment of the ocean (0)
                key = location_nwm_seg
            elif len(downstream_nwm_segs) > 1:
                # Otherwise, 2nd to last element used to identify proper river system.
                key = downstream_nwm_segs[-2]
            # Dictionary with key of 2nd to last downstream segment and value of site nwm segment
            downstream[int(key)].append(int(value))

    # Get Spatial data and populate headwater/duplicate attributes
    print('Attributing nws_lid layer..')

    # Geodataframe from all_lists, reproject, and reset index.
    _, nws_lid_gdf = aggregate_wbd_hucs(all_lists, WBD_LAYER, retain_attributes=False)
    nws_lid_gdf.columns = [name.replace('identifiers_', '') for name in nws_lid_gdf.columns]
    nws_lid_gdf.to_crs(PREP_PROJECTION, inplace=True)
    nws_lid_gdf.reset_index(drop=True)
    Path(workspace).mkdir(parents=True, exist_ok=True)
    nws_lid_gdf.dropna(subset=['nwm_feature_id'], inplace=True)
    nws_lid_gdf.to_file(Path(workspace) / 'nws_lid.gpkg', driver='GPKG', engine='fiona')


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Create spatial data of nws_lid points attributed with mainstems and colocated.'
    )
    parser.add_argument('-w', '--workspace', help='Workspace where all data will be stored.', required=True)
    args = vars(parser.parse_args())

    # Run get_env_paths and static_flow_lids
    generate_nws_lid(**args)
