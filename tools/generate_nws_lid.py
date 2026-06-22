#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone

from tools_shared_functions import aggregate_wbd_hucs, get_metadata
from src.utils.shared_functions import FIM_Helpers as fh

from utils.shared_variables import PREP_PROJECTION


gpd.options.io_engine = "pyogrio"


def __validate_inputs(env_file, workspace, keep_blank_nwm):
    '''
    Get info from env file and validate input filepaths.
    '''

    load_dotenv(env_file)
    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    EVALUATED_SITES_CSV = os.getenv("EVALUATED_SITES_CSV")

    metadata_url = f'{API_BASE_URL}/metadata/'

    filepath_list = [WBD_LAYER, EVALUATED_SITES_CSV]
    for path in filepath_list:
        if not os.path.exists(path):
            raise Exception(f'Unable to find file at {path}, check input env file ({env_file})')

    # Set up output file and confirm that it exists
    Path(workspace).mkdir(parents=True, exist_ok=True)

    if not os.path.exists(workspace):
        raise Exception(f'Workspace filepath does not exist. Check workspace input filepath: {workspace}')

    out_filepath = os.path.join(workspace, 'nws_lid.gpkg')

    # Make sure this val is a true boolean
    keep_blank_nwm = bool(keep_blank_nwm)

    return WBD_LAYER, EVALUATED_SITES_CSV, metadata_url, out_filepath, keep_blank_nwm


def generate_nws_lid(workspace, env_file, keep_blank_nwm):
    '''
    Generate the nws_lid layer containing all nws_lid points

    Parameters
    ----------
    workspace : STR
        Directory where outputs will be saved.
    env_file : STR
        Direvtory where env file is saved.
    keep_blank_nwm : BOOL
        If True, all rows will be saved (regardless of whether they have NWM Feature ID values).

    Returns
    -------
    None.

    '''
    WBD_LAYER, EVALUATED_SITES_CSV, metadata_url, out_filepath, keep_blank_nwm = __validate_inputs(env_file, workspace, keep_blank_nwm)

    # Record start time
    overall_start_dt = datetime.now(timezone.utc)
    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")

    print("=========================================================================")
    print(f"Generating NWS LID GeoPackage - {display_dt_string} (UTC)")
    print("")

    if os.path.isfile(out_filepath):
        print(f"Warning: File already exists and will be overwritten: {out_filepath}")
        print("")

    # ---
    # Get the metdata (with downstream trace) for all RFC forecast points
    print("Getting metadata for all RFC forecast points...")

    select_by = 'nws_lid'
    selector = ['all']
    must_include = 'nws_data.rfc_forecast_point'
    downstream_trace_distance = 'all'
    fcst_list, __, err_msg = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    if err_msg != "":
        print(err_msg)

    # Get list of all evaluated sites not in fcst_list
    fcst_list_sites = [record.get('identifiers').get('nws_lid').lower() for record in fcst_list]
    evaluated_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].str.lower().to_list()
    evaluated_sites = list(set(evaluated_sites) - set(fcst_list_sites))

    print(f"Downloaded metadata for {len(fcst_list)} forecast sites")
    print("")

    # ---
    # Get metadata (with downstream trace) for all evaluated sites not in forecast sites list
    print(f"Getting metadata for {len(evaluated_sites)} additional non-forecast sites...")

    select_by = 'nws_lid'
    selector = evaluated_sites
    must_include = None
    downstream_trace_distance = 'all'
    eval_list, __, err_msg = get_metadata(
        metadata_url=metadata_url,
        select_by=select_by,
        selector=selector,
        must_include=must_include,
        upstream_trace_distance=None,
        downstream_trace_distance=downstream_trace_distance,
    )

    if err_msg != "":
        print(err_msg)

    # Append all lists
    all_lists = fcst_list + eval_list

    print(f"Downloaded metadata for {len(eval_list)} additional eval sites")
    print(f"Metadata retrieved for {len(all_lists)} total sites")
    print("")

    # ---
    # Get spatial data and populate headwater/duplicate attributes
    print("Aggregating WBD HUCs...")
    _, nws_lid_gdf = aggregate_wbd_hucs(all_lists, WBD_LAYER, retain_attributes=False)

    # Create GDF Geodataframe from all_lists and reproject GDF
    nws_lid_gdf.columns = [name.replace('identifiers_', '') for name in nws_lid_gdf.columns]
    nws_lid_gdf.to_crs(PREP_PROJECTION, inplace=True)

    # Remove rows without NWM Feature IDs if the -k flag was not used
    if not keep_blank_nwm:
        print("Removing rows with blank NWM feature ID")
        nws_lid_gdf.dropna(subset=['nwm_feature_id'], inplace=True)
    else:
        print("Will not remove rows with blank NWM feature ID")

    print("Finished creating NWS LID geodataframe")
    print("")

    # ---
    # Reset index and save output files
    nws_lid_gdf.reset_index(drop=True)
    nws_lid_gdf.to_file(out_filepath, driver='GPKG', engine='fiona')
    print(f"Saved GPKG to {out_filepath}")

    # Save a CSV for QC purposes
    out_csv_filepath = out_filepath.replace(".gpkg", ".csv")
    nws_lid_gdf.to_csv(out_csv_filepath, index=False)
    print(f"Saved CSV to {out_csv_filepath}")

    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    dur_msg = fh.print_date_time_duration(overall_start_dt, datetime.now(timezone.utc), False)

    print("")
    print(f"Program complete - {display_dt_string} (UTC)")
    print(f"{dur_msg}")
    print("=========================================================================")

    return


if __name__ == '__main__':
    '''
    python /foss_fim/tools/generate_nws_lid.py -w '/dev_fim_share/foss_fim/temp/emily/' -k
    '''
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Create spatial data of nws_lid points attributed with mainstems and colocated.'
    )
    parser.add_argument('-w', '--workspace', help='Workspace where all data will be stored.', required=True)

    parser.add_argument(
        '-e',
        '--env-file',
        help='OPTIONAL: Docker mount path to the environment file.'
        'default = /data/config/fim_enviro_values.env',
        required=False,
        default='/data/config/fim_enviro_values.env',
    )

    parser.add_argument(
        '-k',
        '--keep-blank-nwm',
        help="OPTIONAL: If this flag is set, the sites with blank nwm_feature_id columns will not be removed."
        "default = False",
        required=False,
        default=False,
        action='store_true',
    )

    args = vars(parser.parse_args())

    # Run generate_nws_lid
    generate_nws_lid(**args)
