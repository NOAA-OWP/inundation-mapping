#!/usr/bin/env python3

import argparse
import logging
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from tools_shared_functions import aggregate_wbd_hucs, get_metadata

from src.utils.io import write_geodataframe
from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import setup_file_logger
from utils.shared_variables import PREP_PROJECTION


gpd.options.io_engine = "pyogrio"


def __validate_inputs(env_file, workspace, keep_blank_nwm):
    '''
    Get info from env file and validate input filepaths.

    Arguments
    ---------
    env_file : STR
        Path to the env file containing the input filepaths.
    workspace : STR
        Directory where outputs will be saved.
    keep_blank_nwm : BOOL
        If True, all rows will be saved (regardless of whether they have NWM Feature ID values).

    Returns
    -------
    WBD_LAYER : STR
        Path to the WBD layer.
    EVALUATED_SITES_CSV : STR
        Path to the CSV containing the list of evaluated sites.
    metadata_url : STR
        URL to the metadata API.
    out_filepath : STR
        Path to the output GeoPackage file.
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

    today = datetime.now().strftime('%Y%m%d')
    out_filepath = os.path.join(workspace, f'nws_lid_{today}.gpkg')

    if os.path.exists(out_filepath):
        raise Exception(f'Output file already exists, delete existing file and re-run: {out_filepath}')

    # Make sure this val is a true boolean
    keep_blank_nwm = bool(keep_blank_nwm)

    return WBD_LAYER, EVALUATED_SITES_CSV, metadata_url, out_filepath, keep_blank_nwm


def generate_nws_lid(workspace, env_file, keep_blank_nwm):
    '''
    Generate the nws_lid layer containing all nws_lid points

    Arguments
    ---------
    workspace : STR
        Directory where outputs will be saved.
    env_file : STR
        Directory where env file is saved.
    keep_blank_nwm : BOOL
        If True, all rows will be saved (regardless of whether they have NWM Feature ID values).

    Returns
    -------
    None

    '''
    WBD_LAYER, EVALUATED_SITES_CSV, metadata_url, out_filepath, keep_blank_nwm = __validate_inputs(
        env_file, workspace, keep_blank_nwm
    )

    # Record start time
    overall_start_dt = datetime.now(timezone.utc)
    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")

    log_file_path = setup_file_logger(workspace, "generate_nws_lid")

    try:
        print("")
        print("=========================================================================")
        logging.info(f"Generating NWS LID GeoPackage - {display_dt_string} (UTC)")
        logging.info("")

        if os.path.isfile(out_filepath):
            logging.info(f"Warning: File already exists and will be overwritten: {out_filepath}")
            logging.info("")

        # ---
        # Get the metdata (with downstream trace) for all riverpoints
        logging.info("Getting metadata for all riverpoints...")
        select_by = 'nws_lid'
        selector = ['all']
        must_include = 'nws_data.riverpoint'

        # Use the riverpoint pull to get a site list (without upstream/downstream trace, for now)
        riverpoint_all_meta_list, riverpoint_all_meta_df, err_msg = get_metadata(
            metadata_url,
            select_by=select_by,
            selector=selector,
            must_include=must_include,
            upstream_trace_distance=None,
            downstream_trace_distance=None,
        )
        full_lid_list = riverpoint_all_meta_df['identifiers_nws_lid'].to_list()
        del riverpoint_all_meta_list, riverpoint_all_meta_df

        # Break the lid list into sublists (to prevent overloading the API)
        n = 200  # chunk size
        chunks = [full_lid_list[i : i + n] for i in range(0, len(full_lid_list), n)]
        logging.info(f'Pulled {len(full_lid_list)} riverpoint sites.')
        logging.info(f'Downloading metadata in {len(chunks)} chunks of {n} sites each.')

        # Download metadata for each chunk (with downstream trace) and append to a list
        logging.info("Downloading metadata for all riverpoints (with downstream trace)...")
        logging.info("")
        riverpoint_meta_list = []

        for index, chunk in enumerate(chunks, start=1):
            if index % 50 == 0:
                logging.info(f"Processed {index}/{len(chunks)} chunks...")

            riverpoint_chunk_meta_list, riverpoint_chunk_meta_df, err_msg = get_metadata(
                metadata_url,
                select_by=select_by,
                selector=chunk,
                must_include=None,  # because the LID list is already filtered to riverpoint
                upstream_trace_distance=None,
                downstream_trace_distance='all',
            )

            if err_msg != "":
                logging.error(f"Error retrieving metadata for chunk {index}: {err_msg}")
                continue

            # Make sure that the upstream and downstream trace are working)
            if "downstream_nwm_features" not in riverpoint_chunk_meta_df.columns:
                logging.warning('Downstream NWM features NOT available')

            riverpoint_meta_list.extend(riverpoint_chunk_meta_list)
            logging.info(f'Full meta list is now {len(riverpoint_meta_list)} objects long')
            # End of chunk for loop

        logging.info(f"Downloaded metadata for {len(riverpoint_meta_list)} forecast sites")
        logging.info("")

        # Get list of all evaluated sites not in riverpoint_meta_list
        riverpoint_meta_list_sites = [
            record.get('identifiers').get('nws_lid').lower() for record in riverpoint_meta_list
        ]
        evaluated_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].str.lower().to_list()
        evaluated_sites = list(set(evaluated_sites) - set(riverpoint_meta_list_sites))

        # ---
        if len(evaluated_sites) == 0:
            all_lists = riverpoint_meta_list
            logging.info("No additional non-forecast sites to process")
        else:
            # Get metadata (with downstream trace) for all evaluated sites not in forecast sites list
            logging.info(f"Getting metadata for {len(evaluated_sites)} additional non-forecast sites...")

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
                logging.error(err_msg)

            # Append all lists
            all_lists = riverpoint_meta_list + eval_list
            logging.info(f"Downloaded metadata for {len(eval_list)} additional eval sites")

        logging.info(f"Metadata retrieved for {len(all_lists)} total sites")
        logging.info("")

        # ---
        # Get spatial data and populate headwater/duplicate attributes
        logging.info("Aggregating WBD HUCs...")
        _, nws_lid_gdf = aggregate_wbd_hucs(all_lists, WBD_LAYER, retain_attributes=False)

        # Create GDF Geodataframe from all_lists and reproject GDF
        nws_lid_gdf.columns = [name.replace('identifiers_', '') for name in nws_lid_gdf.columns]
        nws_lid_gdf.to_crs(PREP_PROJECTION, inplace=True)

        # Remove rows without NWM Feature IDs if the -k flag was not used
        if not keep_blank_nwm:
            logging.info("Removing rows with blank NWM feature ID")
            nws_lid_gdf.dropna(subset=['nwm_feature_id'], inplace=True)
        else:
            logging.info("Will not remove rows with blank NWM feature ID")

        logging.info("Finished creating NWS LID geodataframe")
        logging.info("")

        # ---
        # Reset index and save output files
        nws_lid_gdf.reset_index(drop=True)
        write_geodataframe(nws_lid_gdf, out_filepath)
        logging.info(f"Saved GPKG to {out_filepath}")

        # Save a CSV for QC purposes
        out_csv_filepath = out_filepath.replace(".gpkg", ".csv")
        nws_lid_gdf.to_csv(out_csv_filepath, index=False)
        logging.info(f"Saved CSV to {out_csv_filepath}")

        display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
        dur_msg = fh.print_date_time_duration(overall_start_dt, datetime.now(timezone.utc), False)

        logging.info("")
        logging.info(f"Saved logs to {log_file_path}")
        logging.info(f"Program complete - {display_dt_string} (UTC)")
        logging.info(f"{dur_msg}")
        print("=========================================================================")
        print("")

    except Exception as e:
        trace_error = traceback.format_exc()
        logging.error(f"An error occurred: {e}")
        logging.error(trace_error)

    return


if __name__ == '__main__':
    '''
    Creates a geospatial dataset of NWS LID points attributed with mainstems and colocated.

    Arguments
    ---------
    -w, --workspace : STR
        Directory where outputs will be saved.
    -e, --env-file : STR
        OPTIONAL: Docker mount path to the environment file.
        default = /data/config/fim_enviro_values.env
    -k, --keep-blank-nwm : BOOL
        OPTIONAL: If this flag is set, the sites with blank nwm_feature_id columns will not be removed.
        default = False

    Outputs
    -------
    A geopackage and CSV file containing the nws_lid points with attributes.

    Example Usage
    -------------

    python /foss_fim/data/wrds/generate_nws_lid.py -w '/dev_fim_share/foss_fim/temp/' -k

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
