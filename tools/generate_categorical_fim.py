#!/usr/bin/env python3

import argparse
import csv
import glob

# import logging
import os
import random
import shutil
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait

# from logging.handlers import QueueHandler, QueueListener
from datetime import datetime, timezone
from itertools import repeat
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from dotenv import load_dotenv
from generate_categorical_fim_flows import generate_flows
from generate_categorical_fim_mapping import (
    manage_catfim_mapping,
    post_process_cat_fim_for_viz,
    produce_stage_based_catfim_tifs,
)
from tools_shared_functions import (
    filter_nwm_segments_by_stream_order,
    get_datum,
    get_nwm_segs,
    get_thresholds,
    ngvd_to_navd_ft,
)
from tools_shared_variables import (
    acceptable_alt_acc_thresh,
    acceptable_alt_meth_code_list,
    acceptable_coord_acc_code_list,
    acceptable_coord_method_code_list,
    acceptable_site_type_list,
)

import utils.fim_logger as fl
from utils.shared_variables import VIZ_PROJECTION


# global RLOG
FLOG = fl.FIM_logger()  # the non mp version
MP_LOG = fl.FIM_logger()  # the Multi Proc version

gpd.options.io_engine = "pyogrio"


"""
Jun 17, 2024
This system is continuing to mature over time. It has a number of optimizations that can still
be applied in areas such as logic, performance and error handling.

In the interium there is still a consider amount of debug lines and tools embedded in that can
be commented on/off as required.


NOTE: For now.. all logs roll up to the parent log file. ie) catfim_2024_07_09-22-20-12.log
This creates a VERY large final log file, but the warnings and errors file should be manageable.
Later: Let's split this to seperate log files per huc. Easy to do that for Stage Based it has
"iterate_through_stage_based" function. Flow based? we have to think that one out a bit

"""


def process_generate_categorical_fim(
    fim_run_dir,
    env_file,
    job_number_huc,
    job_number_inundate,
    is_stage_based,
    output_folder,
    overwrite,
    search,
    job_number_intervals,
    past_major_interval_cap,
    nwm_metafile,
):

    # ================================
    # Validation and setup

    # Append option configuration (flow_based or stage_based) to output folder name.
    if is_stage_based:
        catfim_method = "stage_based"
    else:
        catfim_method = "flow_based"

    # Define output directories
    output_catfim_dir = output_folder + "_" + catfim_method

    output_flows_dir = os.path.join(output_catfim_dir, 'flows')
    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')

    # The override is not for the parent folder as we want to keep logs around with or without override
    if os.path.exists(output_catfim_dir) is False:
        os.mkdir(output_catfim_dir)

    # Create output directories (check against maping only as a proxy for all three)
    if os.path.exists(output_mapping_dir) is True:
        if overwrite is False:
            raise Exception(
                f"The output mapping folder of {output_catfim_dir} already exists."
                " If you want to overwrite it, please add the -o flag. Note: When overwritten, "
                " the three folders of mapping, flows and attributes wil be deleted and rebuilt"
            )

        gpkg_dir = os.path.join(output_mapping_dir, 'gpkg')
        shutil.rmtree(gpkg_dir, ignore_errors=True)

        shutil.rmtree(output_mapping_dir, ignore_errors=True)
        shutil.rmtree(output_flows_dir, ignore_errors=True)
        shutil.rmtree(attributes_dir, ignore_errors=True)

        # Keeps the logs folder

    if nwm_metafile != "":
        if os.path.exists(nwm_metafile) is False:
            raise Exception("The nwm_metadata (-me) file can not be found. Please remove or fix pathing.")
        file_ext = os.path.splitext(nwm_metafile)
        if file_ext.count == 0:
            raise Exception("The nwm_metadata (-me) file appears to be invalid. It is missing an extension.")
        if file_ext[1].lower() != ".pkl":
            raise Exception("The nwm_metadata (-me) file appears to be invalid. The extention is not pkl.")

    # Define default arguments. Modify these if necessary
    fim_version = os.path.split(fim_run_dir)[1]

    # Check job numbers and raise error if necessary
    total_cpus_requested = job_number_huc * job_number_inundate * job_number_intervals
    total_cpus_available = os.cpu_count() - 2
    if total_cpus_requested > total_cpus_available:
        raise ValueError(
            f"The HUC job number (jh) [{job_number_huc}]"
            f" multiplied by the inundate job number (jn) [{job_number_inundate}]"
            f" multiplied by the job number intervals (ji) [{job_number_intervals}]"
            " exceeds your machine\'s available CPU count minus one."
            " Please lower one or more of those values accordingly."
        )

    # we are getting too many folders and files. We want just huc folders.
    # output_flow_dir_list = os.listdir(fim_run_dir)
    # looking for folders only starting with 0, 1, or 2

    # for now, we are dropping all Alaska HUCS

    valid_ahps_hucs = [
        x
        for x in os.listdir(fim_run_dir)
        if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2'] and x[:2] != "19"
    ]
    # print(valid_ahps_hucs)

    valid_ahps_hucs.sort()

    num_hucs = len(valid_ahps_hucs)
    if num_hucs == 0:
        raise ValueError(
            f'Output directory {fim_run_dir} is empty. Verify that you have the correct input folder.'
        )

    # End of Validation and setup
    # ================================

    log_dir = os.path.join(output_catfim_dir, "logs")
    log_output_file = FLOG.calc_log_name_and_path(log_dir, "catfim")
    FLOG.setup(log_output_file)

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    os.makedirs(output_flows_dir, exist_ok=True)
    os.makedirs(output_mapping_dir, exist_ok=True)
    os.makedirs(attributes_dir, exist_ok=True)

    FLOG.lprint("================================")
    FLOG.lprint(f"Start generate categorical fim for {catfim_method} - (UTC): {dt_string}")
    FLOG.lprint("")

    FLOG.lprint(f"Processing {num_hucs} huc(s) with Alaska temporarily removed")

    load_dotenv(env_file)
    API_BASE_URL = os.getenv('API_BASE_URL')
    if API_BASE_URL is None:
        raise ValueError(
            'API base url not found. '
            'Ensure inundation_mapping/tools/ has an .env file with the following info: '
            'API_BASE_URL, EVALUATED_SITES_CSV, WBD_LAYER, NWM_FLOWS_MS, '
            'USGS_METADATA_URL, USGS_DOWNLOAD_URL'
        )

    # TODO: Add check for if lid_to_run and lst_hucs parameters conflict

    # Check that fim_inputs.csv exists and raise error if necessary
    fim_inputs_csv_path = os.path.join(fim_run_dir, 'fim_inputs.csv')
    if not os.path.exists(fim_inputs_csv_path):
        raise ValueError(f'{fim_inputs_csv_path} not found. Verify that you have the correct input files.')

    # print()

    # FLOG.lprint("Filtering out HUCs that do not have related ahps site in them.")
    # valid_ahps_hucs = __filter_hucs_to_ahps(lst_hucs)

    # num_valid_hucs = len(valid_ahps_hucs)
    # if num_valid_hucs == 0:
    #     raise Exception("None of the HUCs supplied have ahps sites in them. Check your fim output folder")
    # else:
    #     FLOG.lprint(f"Processing {num_valid_hucs} huc(s) with AHPS sites")

    # Define upstream and downstream search in miles
    nwm_us_search, nwm_ds_search = search, search

    # STAGE-BASED
    if is_stage_based:
        # Generate Stage-Based CatFIM mapping3
        nws_sites_layer = generate_stage_based_categorical_fim(
            output_catfim_dir,
            fim_run_dir,
            nwm_us_search,
            nwm_ds_search,
            env_file,
            job_number_inundate,
            job_number_huc,
            valid_ahps_hucs,
            job_number_intervals,
            past_major_interval_cap,
            nwm_metafile,
        )

        # job_number_tif = job_number_inundate * job_number_intervals
        post_process_cat_fim_for_viz(
            output_catfim_dir, job_number_huc, job_number_inundate, fim_version, log_output_file
        )

        # Updating mapping status
        FLOG.lprint('Updating mapping status...')
        update_mapping_status(output_mapping_dir, nws_sites_layer)

    # FLOW-BASED
    else:
        FLOG.lprint('Creating flow files using the ' + catfim_method + ' technique...')
        start = time.time()
        nws_sites_layer = generate_flows(
            output_catfim_dir,
            nwm_us_search,
            nwm_ds_search,
            env_file,
            job_number_huc,
            is_stage_based,
            valid_ahps_hucs,
            nwm_metafile,
            log_output_file,
        )
        end = time.time()
        elapsed_time = (end - start) / 60
        FLOG.lprint(f"Finished creating flow files in {str(elapsed_time).split('.')[0]} minutes")

        # Generate CatFIM mapping
        manage_catfim_mapping(
            fim_run_dir,
            output_flows_dir,
            output_catfim_dir,
            job_number_huc,
            job_number_inundate,
            False,
            log_output_file,
        )

        # Updating mapping status
        FLOG.lprint('Updating mapping status')
        update_mapping_status(output_mapping_dir, nws_sites_layer)

    # Create CSV versions of the final geopackages.
    FLOG.lprint('Creating CSVs. This may take several minutes.')
    create_csvs(output_mapping_dir, is_stage_based)

    FLOG.lprint("================================")
    FLOG.lprint("End generate categorical fim")

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    FLOG.lprint(f"Ended (UTC): {dt_string}")

    # calculate duration
    time_duration = overall_end_time - overall_start_time
    FLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")

    return


def create_csvs(output_mapping_dir, is_stage_based):
    '''
    Produces CSV versions of desired geopackage in the output_mapping_dir.

    Parameters
    ----------
    output_mapping_dir : STR
        Path to the output directory of all inundation maps.
    reformatted_catfim_method : STR
        Text to append to CSV to communicate the type of CatFIM.

    Returns
    -------
    None.

    '''

    if is_stage_based is True:
        catfim_method = "stage_based"
    else:
        catfim_method = "flow_based"

    # Convert any geopackage in the root level of output_mapping_dir to CSV and rename.
    gpkg_list = glob.glob(os.path.join(output_mapping_dir, '*.gpkg'))

    # catfim_library.gpkg is saved as (flow_based or stage_based)_catfim.csv
    # nws_lid_sites.gpkg is saved as (flow_based or stage_based)_catfim_sites.csv

    for gpkg in gpkg_list:
        FLOG.lprint(f"Creating CSV for {gpkg}")
        gdf = gpd.read_file(gpkg, engine='fiona')
        parent_directory = os.path.split(gpkg)[0]
        if 'catfim_library' in gpkg:
            file_name = f"{catfim_method}_catfim.csv"
        if 'nws_lid_sites' in gpkg:
            file_name = f"{catfim_method}_catfim_sites.csv"

        csv_output_path = os.path.join(parent_directory, file_name)
        gdf.to_csv(csv_output_path)
    return


def update_mapping_status(output_mapping_dir, nws_sites_layer):
    '''
    Updates the status for nws_lids from the flows subdirectory. Status
    is updated for sites where the inundation.py routine was not able to
    produce inundation for the supplied flow files. It is assumed that if
    an error occured in inundation.py that all flow files for a given site
    experienced the error as they all would have the same nwm segments.

    Parameters
    ----------
    output_mapping_dir : STR
        Path to the output directory of all inundation maps.
    nws_sites_layer : STR


    Returns
    -------
    None.

    '''
    # Find all LIDs with empty mapping output folders
    subdirs = [str(i) for i in Path(output_mapping_dir).rglob('**/*') if i.is_dir()]

    print("")

    empty_nws_lids = [Path(directory).name for directory in subdirs if not list(Path(directory).iterdir())]
    if len(empty_nws_lids) > 0:
        FLOG.warning(f"Empty_nws_lids are.. {empty_nws_lids}")

    # Write list of empty nws_lids to DataFrame, these are sites that failed in inundation.py
    mapping_df = pd.DataFrame({'nws_lid': empty_nws_lids})

    mapping_df['did_it_map'] = 'no'
    mapping_df['map_status'] = ' and all categories failed to map'

    # Import geopackage output from flows creation
    flows_df = gpd.read_file(nws_sites_layer, engine='fiona')

    if len(flows_df) == 0:
        FLOG.critical(f"flows_df is empty. Path is {nws_sites_layer}. Program aborted.")
        sys.exit(1)

    try:
        # Join failed sites to flows df
        flows_df = flows_df.merge(mapping_df, how='left', on='nws_lid')

        # Switch mapped column to no for failed sites and update status
        flows_df.loc[flows_df['did_it_map'] == 'no', 'mapped'] = 'no'
        flows_df.loc[flows_df['did_it_map'] == 'no', 'status'] = flows_df['status'] + flows_df['map_status']

        #    # Perform pass for HUCs where mapping was skipped due to missing data  #TODO check with Brian
        #    if stage_based:
        #        missing_mapping_hucs =
        #    else:
        #        flows_hucs = [i.stem for i in Path(output_flows_dir).iterdir() if i.is_dir()]
        #        mapping_hucs = [i.stem for i in Path(output_mapping_dir).iterdir() if i.is_dir()]
        #        missing_mapping_hucs = list(set(flows_hucs) - set(mapping_hucs))
        #
        #    # Update status for nws_lid in missing hucs and change mapped attribute to 'no'
        #    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'status'] =
        #           flows_df['status'] + ' and all categories failed to map because missing HUC information'
        #    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'mapped'] = 'no'

        # Clean up GeoDataFrame and rename columns for consistency
        flows_df = flows_df.drop(columns=['did_it_map', 'map_status'])
        flows_df = flows_df.rename(columns={'nws_lid': 'ahps_lid'})

        # Write out to file
        flows_df.to_file(nws_sites_layer)
    except Exception as e:
        FLOG.critical(f"{output_mapping_dir} : No LIDs, \n Exception: \n {repr(e)} \n")
        FLOG.critical(traceback.format_exc())
    return


def mark_complete(site_directory):
    marker_file = Path(site_directory) / 'complete.txt'
    marker_file.touch()
    return


# This is always called as part of Multi-processing so uses MP_LOG variable and
# creates it's own logging object.
# This does flow files and mapping in the same function by HUC
def iterate_through_huc_stage_based(
    output_catfim_dir,
    huc,
    fim_dir,
    huc_dictionary,
    threshold_url,
    magnitudes,
    all_lists,
    past_major_interval_cap,
    job_number_inundate,
    number_of_interval_jobs,
    nwm_flows_df,
    parent_log_output_file,
    child_log_file_prefix,
    progress_stmt,
):
    """_summary_
    This and its children will create stage based tifs and catfim data based on a huc
    """

    try:
        # This is setting up logging for this function to go up to the parent
        # child_log_file_prefix is likely MP_iter_hucs
        MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)
        MP_LOG.lprint("\n**********************")
        MP_LOG.lprint(f'Processing {huc} ...')
        MP_LOG.lprint(f'... {progress_stmt} ...')
        MP_LOG.lprint("")

        missing_huc_files = []
        all_messages = []
        stage_based_att_dict = {}

        mapping_dir = os.path.join(output_catfim_dir, "mapping")
        attributes_dir = os.path.join(output_catfim_dir, 'attributes')
        output_flows_dir = os.path.join(output_catfim_dir, "flows")
        huc_messages_dir = os.path.join(output_flows_dir, 'huc_messages')

        # Make output directory for the particular huc in the mapping folder
        mapping_huc_directory = os.path.join(mapping_dir, huc)
        if not os.path.exists(mapping_huc_directory):
            os.mkdir(mapping_huc_directory)

        # Define paths to necessary HAND and HAND-related files.
        usgs_elev_table = os.path.join(fim_dir, huc, 'usgs_elev_table.csv')
        branch_dir = os.path.join(fim_dir, huc, 'branches')

        # Loop through each lid in nws_lids list
        nws_lids = huc_dictionary[huc]
        for lid in nws_lids:
            MP_LOG.lprint("-----------------------------------")
            huc_lid_id = f"{huc} : {lid}"
            MP_LOG.lprint(huc_lid_id)

            lid = lid.lower()  # Convert lid to lower case
            # -- If necessary files exist, continue -- #
            # Yes, each lid gets a record no matter what, so we need some of these messages duplicated
            # per lid record
            if not os.path.exists(usgs_elev_table):
                msg = ": usgs_elev_table missing, likely unacceptable gage datum error -- more details to come in future release"
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
                continue
            if not os.path.exists(branch_dir):
                msg = ": branch directory missing"
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
                continue
            usgs_elev_df = pd.read_csv(usgs_elev_table)

            # Make mapping lid_directory.
            mapping_lid_directory = os.path.join(mapping_huc_directory, lid)
            if not os.path.exists(mapping_lid_directory):
                os.mkdir(mapping_lid_directory)
            else:
                complete_marker = os.path.join(mapping_lid_directory, 'complete.txt')
                if os.path.exists(complete_marker):
                    msg = ": already completed in previous run."
                    all_messages.append(lid + msg)
                    MP_LOG.error(huc_lid_id + msg)
                    continue
            # Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
            stages, flows = get_thresholds(
                threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
            )

            if stages is None:
                msg = ': error getting thresholds from WRDS API'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
                continue

            # Check if stages are supplied, if not write message and exit.
            if all(stages.get(category, None) is None for category in magnitudes):
                msg = ': missing threshold stages'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
                continue

            acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id)
            if acceptable_usgs_elev_df is None:
                # This should only happen in a catastrophic code error.
                # Exceptions inside the function, normally return usgs_elev_df or a variant of it
                raise Exception("acceptable_usgs_elev_df failed to be created")

            # Get the dem_adj_elevation value from usgs_elev_table.csv.
            # Prioritize the value that is not from branch 0.
            lid_usgs_elev, dem_eval_messages = __adj_dem_evalation_val(
                acceptable_usgs_elev_df, lid, huc_lid_id
            )
            all_messages += dem_eval_messages
            if lid_usgs_elev is None:
                continue

            # Initialize nested dict for lid attributes
            stage_based_att_dict.update({lid: {}})

            # Find lid metadata from master list of metadata dictionaries.
            metadata = next(
                (item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False
            )
            lid_altitude = metadata['usgs_data']['altitude']

            # Filter out sites that don't have "good" data
            try:
                if not metadata['usgs_data']['coord_accuracy_code'] in acceptable_coord_acc_code_list:
                    MP_LOG.warning(
                        f"\t{huc_lid_id}: {metadata['usgs_data']['coord_accuracy_code']} "
                        "Not in acceptable coord acc codes"
                    )
                    continue
                if not metadata['usgs_data']['coord_method_code'] in acceptable_coord_method_code_list:
                    MP_LOG.warning(f"\t{huc_lid_id}: Not in acceptable coord method codes")
                    continue
                if not metadata['usgs_data']['alt_method_code'] in acceptable_alt_meth_code_list:
                    MP_LOG.warning(f"\t{huc_lid_id}: Not in acceptable alt method codes")
                    continue
                if not metadata['usgs_data']['site_type'] in acceptable_site_type_list:
                    MP_LOG.warning(f"\t{huc_lid_id}: Not in acceptable site type codes")
                    continue
                if not float(metadata['usgs_data']['alt_accuracy_code']) <= acceptable_alt_acc_thresh:
                    MP_LOG.warning(f"\t{huc_lid_id}: Not in acceptable threshold range")
                    continue
            except Exception:
                MP_LOG.error(f"{huc_lid_id}:  filtering out 'bad' data in the usgs_data")
                MP_LOG.error(traceback.format_exc())
                continue

            datum_adj_ft, datum_messages = __adjust_datum_ft(flows, metadata, lid, huc_lid_id)

            all_messages = all_messages + datum_messages
            if datum_adj_ft is None:
                continue

            ### -- Concluded Datum Offset --- ###
            # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            unfiltered_segments = list(set(get_nwm_segs(metadata)))

            # Filter segments to be of like stream order.
            desired_order = metadata['nwm_feature_data']['stream_order']
            segments = filter_nwm_segments_by_stream_order(unfiltered_segments, desired_order, nwm_flows_df)
            action_stage = stages['action']
            minor_stage = stages['minor']
            moderate_stage = stages['moderate']
            major_stage = stages['major']
            stage_list = [
                i for i in [action_stage, minor_stage, moderate_stage, major_stage] if i is not None
            ]
            # Create a list of stages, incrementing by 1 ft.
            if stage_list == []:
                msg = ': WARNING: no stage values available'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
                continue

            interval_list = np.arange(
                min(stage_list), max(stage_list) + past_major_interval_cap, 1.0
            )  # Go an extra 10 ft beyond the max stage, arbitrary

            # Check for large discrepancies between the elevation values from WRDS and HAND.
            #   Otherwise this causes bad mapping.
            elevation_diff = lid_usgs_elev - (lid_altitude * 0.3048)
            if abs(elevation_diff) > 10:
                msg = ': large discrepancy in elevation estimates from gage and HAND'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
                continue

            # For each flood category / magnitude
            MP_LOG.lprint(f"{huc_lid_id}: About to process flood categories")

            # This function sometimes is called within a MP but sometimes not.
            # So, we might have an MP inside an MP
            # and we will need a new prefix for it.

            # Becuase we already are in an MP, lets merge up what we have at this point
            # Before creating child MP files
            MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix)
            child_log_file_prefix = MP_LOG.MP_calc_prefix_name(
                parent_log_output_file, "MP_produce_catfim_tifs"
            )
            for category in magnitudes:
                MP_LOG.lprint(f"{huc_lid_id}: Magnitude is {category}")
                # Pull stage value and confirm it's valid, then process
                stage = stages[category]

                if stage is not None and datum_adj_ft is not None and lid_altitude is not None:
                    # Call function to execute mapping of the TIFs.
                    (messages, hand_stage, datum_adj_wse, datum_adj_wse_m) = produce_stage_based_catfim_tifs(
                        stage,
                        datum_adj_ft,
                        branch_dir,
                        lid_usgs_elev,
                        lid_altitude,
                        fim_dir,
                        segments,
                        lid,
                        huc,
                        mapping_lid_directory,
                        category,
                        job_number_inundate,
                        parent_log_output_file,
                        child_log_file_prefix,
                    )
                    all_messages += messages

                    # Extra metadata for alternative CatFIM technique.
                    # TODO Revisit because branches complicate things
                    stage_based_att_dict[lid].update(
                        {
                            category: {
                                'datum_adj_wse_ft': datum_adj_wse,
                                'datum_adj_wse_m': datum_adj_wse_m,
                                'hand_stage': hand_stage,
                                'datum_adj_ft': datum_adj_ft,
                                'lid_alt_ft': lid_altitude,
                                'lid_alt_m': lid_altitude * 0.3048,
                            }
                        }
                    )

                # If missing HUC file data, write message
                if huc in missing_huc_files:
                    msg = ': missing some HUC data'
                    all_messages.append(lid + msg)
                    MP_LOG.error(huc_lid_id + msg)

            # So, we might have an MP inside an MP
            # let's merge what we have at this point, before we go into another MP
            MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix, True)

            # and we will need a new prefix for it.
            tif_child_log_file_prefix = MP_LOG.MP_calc_prefix_name(
                parent_log_output_file, "MP_prod_sb_tifs", huc
            )
            with ProcessPoolExecutor(max_workers=number_of_interval_jobs) as executor:
                try:
                    for interval_stage in interval_list:
                        # Determine category the stage value belongs with.
                        if action_stage <= interval_stage < minor_stage:
                            category = 'action_' + str(interval_stage).replace('.', 'p') + 'ft'
                        if minor_stage <= interval_stage < moderate_stage:
                            category = 'minor_' + str(interval_stage).replace('.', 'p') + 'ft'
                        if moderate_stage <= interval_stage < major_stage:
                            category = 'moderate_' + str(interval_stage).replace('.', 'p') + 'ft'
                        if interval_stage >= major_stage:
                            category = 'major_' + str(interval_stage).replace('.', 'p') + 'ft'
                        executor.submit(
                            produce_stage_based_catfim_tifs,
                            interval_stage,
                            datum_adj_ft,
                            branch_dir,
                            lid_usgs_elev,
                            lid_altitude,
                            fim_dir,
                            segments,
                            lid,
                            huc,
                            mapping_lid_directory,
                            category,
                            job_number_inundate,
                            parent_log_output_file,
                            tif_child_log_file_prefix,
                        )
                except TypeError:  # sometimes the thresholds are Nonetypes
                    MP_LOG.error("ERROR: type error in ProcessPool somewhere")
                    MP_LOG.error(traceback.format_exc())
                    pass
                except Exception:
                    MP_LOG.critical("ERROR: ProcessPool has an error")
                    MP_LOG.critical(traceback.format_exc())
                    # merge MP Logs (Yes)
                    MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix)
                    sys.exit(1)

            # merge MP Logs (merging MP into an MP (proc_pool in a proc_pool))
            MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix, True)

            # Create a csv with same information as geopackage but with each threshold as new record.
            # Probably a less verbose way.
            csv_df = pd.DataFrame()
            for threshold in magnitudes:
                try:
                    line_df = pd.DataFrame(
                        {
                            'nws_lid': [lid],
                            'name': metadata['nws_data']['name'],
                            'WFO': metadata['nws_data']['wfo'],
                            'rfc': metadata['nws_data']['rfc'],
                            'huc': [huc],
                            'state': metadata['nws_data']['state'],
                            'county': metadata['nws_data']['county'],
                            'magnitude': threshold,
                            'q': flows[threshold],
                            'q_uni': flows['units'],
                            'q_src': flows['source'],
                            'stage': stages[threshold],
                            'stage_uni': stages['units'],
                            's_src': stages['source'],
                            'wrds_time': stages['wrds_timestamp'],
                            'nrldb_time': metadata['nrldb_timestamp'],
                            'nwis_time': metadata['nwis_timestamp'],
                            'lat': [float(metadata['nws_preferred']['latitude'])],
                            'lon': [float(metadata['nws_preferred']['longitude'])],
                            'dtm_adj_ft': stage_based_att_dict[lid][threshold]['datum_adj_ft'],
                            'dadj_w_ft': stage_based_att_dict[lid][threshold]['datum_adj_wse_ft'],
                            'dadj_w_m': stage_based_att_dict[lid][threshold]['datum_adj_wse_m'],
                            'lid_alt_ft': stage_based_att_dict[lid][threshold]['lid_alt_ft'],
                            'lid_alt_m': stage_based_att_dict[lid][threshold]['lid_alt_m'],
                        }
                    )
                    csv_df = pd.concat([csv_df, line_df])

                except Exception:
                    MP_LOG.error("ERROR: threshold has an error")
                    MP_LOG.error(traceback.format_exc())
                    return
                    # sys.exit(1)

            # Round flow and stage columns to 2 decimal places.
            csv_df = csv_df.round({'q': 2, 'stage': 2})
            # If a site folder exists (ie a flow file was written) save files containing site attributes.
            if os.path.exists(mapping_lid_directory):
                # Export DataFrame to csv containing attributes
                csv_df.to_csv(os.path.join(attributes_dir, f'{lid}_attributes.csv'), index=False)
            else:
                msg = ': missing all calculated flows'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)

            # If it made it to this point (i.e. no continues), there were no major preventers of mapping
            all_messages.append(lid + ': OK')
            MP_LOG.success(f'{huc_lid_id}: procesing the huc via iterate_through... ??')
            mark_complete(mapping_lid_directory)

        # Write all_messages by HUC to be scraped later.
        if len(all_messages) > 0:
            huc_messages_csv = os.path.join(huc_messages_dir, huc + '_messages.csv')
            with open(huc_messages_csv, 'w') as output_csv:
                writer = csv.writer(output_csv)
                for msg in all_messages:
                    writer.writerows(msg)

    except Exception:
        MP_LOG.error(f"{huc} : {lid} Error iterating through huc stage based")
        MP_LOG.error(traceback.format_exc())

    return


def __adjust_datum_ft(flows, metadata, lid, huc_lid_id):

    # Jul 2024: For now, we will duplicate messages via all_messsages and via the logging system.

    all_messages = []

    datum_adj_ft = None
    ### --- Do Datum Offset --- ###
    # determine source of interpolated threshold flows, this will be the rating curve that will be used.
    rating_curve_source = flows.get('source')
    if rating_curve_source is None:
        msg = f'{huc_lid_id}: No source for rating curve'
        all_messages.append(msg)
        MP_LOG.warning(msg)
        return None, all_messages

    # Get the datum and adjust to NAVD if necessary.
    nws_datum_info, usgs_datum_info = get_datum(metadata)
    if rating_curve_source == 'USGS Rating Depot':
        datum_data = usgs_datum_info
    elif rating_curve_source == 'NRLDB':
        datum_data = nws_datum_info

    # If datum not supplied, skip to new site
    datum = datum_data.get('datum', None)
    if datum is None:
        msg = f'{huc_lid_id}: datum info unavailable'
        all_messages.append(msg)
        MP_LOG.warning(msg)
        return None, all_messages

    # ___________________________________________________________________________________________________#
    # SPECIAL CASE: Workaround for "bmbp1" where the only valid datum is from NRLDB (USGS datum is null).
    # Modifying rating curve source will influence the rating curve and
    #   datum retrieved for benchmark determinations.
    if lid == 'bmbp1':
        rating_curve_source = 'NRLDB'
    # ___________________________________________________________________________________________________#

    # SPECIAL CASE: Custom workaround these sites have faulty crs from WRDS. CRS needed for NGVD29
    #   conversion to NAVD88
    # USGS info indicates NAD83 for site: bgwn7, fatw3, mnvn4, nhpp1, pinn4, rgln4, rssk1, sign4, smfn7,
    #   stkn4, wlln7
    # Assumed to be NAD83 (no info from USGS or NWS data): dlrt2, eagi1, eppt2, jffw3, ldot2, rgdt2
    if lid in [
        'bgwn7',
        'dlrt2',
        'eagi1',
        'eppt2',
        'fatw3',
        'jffw3',
        'ldot2',
        'mnvn4',
        'nhpp1',
        'pinn4',
        'rgdt2',
        'rgln4',
        'rssk1',
        'sign4',
        'smfn7',
        'stkn4',
        'wlln7',
    ]:
        datum_data.update(crs='NAD83')
    # ___________________________________________________________________________________________________#

    # SPECIAL CASE: Workaround for bmbp1; CRS supplied by NRLDB is mis-assigned (NAD29) and
    #   is actually NAD27.
    # This was verified by converting USGS coordinates (in NAD83) for bmbp1 to NAD27 and
    #   it matches NRLDB coordinates.
    if lid == 'bmbp1':
        datum_data.update(crs='NAD27')
    # ___________________________________________________________________________________________________#

    # SPECIAL CASE: Custom workaround these sites have poorly defined vcs from WRDS. VCS needed to ensure
    #   datum reported in NAVD88.
    # If NGVD29 it is converted to NAVD88.
    # bgwn7, eagi1 vertical datum unknown, assume navd88
    # fatw3 USGS data indicates vcs is NAVD88 (USGS and NWS info agree on datum value).
    # wlln7 USGS data indicates vcs is NGVD29 (USGS and NWS info agree on datum value).
    if lid in ['bgwn7', 'eagi1', 'fatw3']:
        datum_data.update(vcs='NAVD88')
    elif lid == 'wlln7':
        datum_data.update(vcs='NGVD29')
    # ___________________________________________________________________________________________________#

    # Adjust datum to NAVD88 if needed
    # Default datum_adj_ft to 0.0
    datum_adj_ft = 0.0
    crs = datum_data.get('crs')
    if datum_data.get('vcs') in ['NGVD29', 'NGVD 1929', 'NGVD,1929', 'NGVD OF 1929', 'NGVD']:
        # Get the datum adjustment to convert NGVD to NAVD. Sites not in contiguous US are previously
        #   removed otherwise the region needs changed.
        try:
            datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data, region='contiguous')
        except Exception as ex:
            MP_LOG.error(f"ERROR: {huc_lid_id}: ngvd_to_navd_ft")
            MP_LOG.error(traceback.format_exc())
            ex = str(ex)
            if crs is None:
                msg = f'{huc_lid_id}: NOAA VDatum adjustment error, CRS is missing'
                all_messages.append(msg)
                MP_LOG.error(msg)
            if 'HTTPSConnectionPool' in ex:
                time.sleep(10)  # Maybe the API needs a break, so wait 10 seconds
                try:
                    datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data, region='contiguous')
                except Exception:
                    msg = f'{huc_lid_id}: NOAA VDatum adjustment error, possible API issue'
                    all_messages.append(msg)
                    MP_LOG.error(msg)
            if 'Invalid projection' in ex:
                msg = f'{huc_lid_id}: :NOAA VDatum adjustment error, invalid projection: crs={crs}'
                all_messages.append(msg)
                MP_LOG.error(msg)
            return None, all_messages

    return datum_adj_ft, all_messages


def __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id):
    acceptable_usgs_elev_df = None
    try:
        # Drop columns that offend acceptance criteria
        usgs_elev_df['acceptable_codes'] = (
            usgs_elev_df['usgs_data_coord_accuracy_code'].isin(acceptable_coord_acc_code_list)
            & usgs_elev_df['usgs_data_coord_method_code'].isin(acceptable_coord_method_code_list)
            & usgs_elev_df['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list)
            & usgs_elev_df['usgs_data_site_type'].isin(acceptable_site_type_list)
        )

        usgs_elev_df = usgs_elev_df.astype({'usgs_data_alt_accuracy_code': float})
        usgs_elev_df['acceptable_alt_error'] = np.where(
            usgs_elev_df['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh, True, False
        )

        acceptable_usgs_elev_df = usgs_elev_df[
            (usgs_elev_df['acceptable_codes'] == True) & (usgs_elev_df['acceptable_alt_error'] == True)
        ]
    except Exception:
        # Not sure any of the sites actually have those USGS-related
        # columns in this particular file, so just assume it's fine to use

        # print("(Various columns related to USGS probably not in this csv)")
        # print(f"Exception: \n {repr(e)} \n")
        MP_LOG.error(f"{huc_lid_id}: An error has occurred while working with the usgs_elev table")
        MP_LOG.error(traceback.format_exc())
        acceptable_usgs_elev_df = usgs_elev_df

    return acceptable_usgs_elev_df


def __adj_dem_evalation_val(acceptable_usgs_elev_df, lid, huc_lid_id):

    lid_usgs_elev = None
    all_messages = []
    try:
        matching_rows = acceptable_usgs_elev_df.loc[
            acceptable_usgs_elev_df['nws_lid'] == lid.upper(), 'dem_adj_elevation'
        ]

        if len(matching_rows) == 2:  # It means there are two level paths, use the one that is not 0
            lid_usgs_elev = acceptable_usgs_elev_df.loc[
                (acceptable_usgs_elev_df['nws_lid'] == lid.upper())
                & (acceptable_usgs_elev_df['levpa_id'] != 0),
                'dem_adj_elevation',
            ].values[0]
        else:
            lid_usgs_elev = acceptable_usgs_elev_df.loc[
                acceptable_usgs_elev_df['nws_lid'] == lid.upper(), 'dem_adj_elevation'
            ].values[0]

    except IndexError:  # Occurs when LID is missing from table (yes. warning)
        MP_LOG.warning(f"{huc_lid_id}:  adjusting dem_adj_elevation")
        MP_LOG.warning(traceback.format_exc())
        msg = ': likely unacceptable gage datum error or accuracy code(s); please see acceptance criteria'
        all_messages.append(lid + msg)
        MP_LOG.warning(huc_lid_id + msg)

    return lid_usgs_elev, all_messages


# This creates a HUC iterator with each HUC creating its flow files and tifs
def generate_stage_based_categorical_fim(
    output_catfim_dir,
    fim_run_dir,
    nwm_us_search,
    nwm_ds_search,
    env_file,
    job_number_inundate,
    job_number_huc,
    lst_hucs,
    number_of_interval_jobs,
    past_major_interval_cap,
    nwm_metafile,
):
    magnitudes = ['action', 'minor', 'moderate', 'major', 'record']

    output_flows_dir = os.path.join(output_catfim_dir, 'flows')
    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')

    # Create HUC message directory to store messages that will be read and joined after multiprocessing
    huc_messages_dir = os.path.join(output_flows_dir, 'huc_messages')
    os.makedirs(huc_messages_dir, exist_ok=True)

    FLOG.lprint("Starting generate_flows (Stage Based)")

    # TODO: Add back in when we add AK back in
    # (huc_dictionary, out_gdf, ___, threshold_url, all_lists, nwm_flows_df, nwm_flows_alaska_df) = (
    # If it is stage based, generate flows returns all of these objects.
    # If flow based, generate flows returns only
    (huc_dictionary, out_gdf, ___, threshold_url, all_lists, nwm_flows_df) = generate_flows(
        output_catfim_dir,
        nwm_us_search,
        nwm_ds_search,
        env_file,
        job_number_huc,
        True,
        lst_hucs,
        nwm_metafile,
        str(FLOG.LOG_FILE_PATH),
    )
    FLOG.lprint("End generate_flows (Stage Based)")

    child_log_file_prefix = FLOG.MP_calc_prefix_name(FLOG.LOG_FILE_PATH, "MP_iter_hucs")

    FLOG.lprint("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    FLOG.lprint("Start processing HUCs for Stage-Based CatFIM")
    num_hucs = len(lst_hucs)
    huc_index = 0
    FLOG.lprint(f"Number of hucs to process is {num_hucs}")

    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in huc_dictionary:
            if huc in lst_hucs:
                # FLOG.lprint(f'Generating stage based catfim for : {huc}')

                # put back in when we put alaska back in.
                # flows_df = nwm_flows_alaska_df if huc[:2] == '19' else nwm_flows_df

                progress_stmt = f"index {huc_index + 1} of {num_hucs}"
                flows_df = nwm_flows_df
                executor.submit(
                    iterate_through_huc_stage_based,
                    output_catfim_dir,
                    huc,
                    fim_run_dir,
                    huc_dictionary,
                    threshold_url,
                    magnitudes,
                    all_lists,
                    past_major_interval_cap,
                    job_number_inundate,
                    number_of_interval_jobs,
                    flows_df,
                    str(FLOG.LOG_FILE_PATH),
                    child_log_file_prefix,
                    progress_stmt,
                )
                huc_index += 1
    # Need to merge MP logs here, merged into the "master log file"
    FLOG.merge_log_files(FLOG.LOG_FILE_PATH, child_log_file_prefix, True)

    FLOG.lprint('\nWrapping up processing HUCs for Stage-Based CatFIM...')
    FLOG.lprint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>\n")
    # csv_files = os.listdir(attributes_dir)

    csv_files = [x for x in os.listdir(attributes_dir) if x.endswith('.csv')]

    all_csv_df = pd.DataFrame()
    refined_csv_files_list = []
    for csv_file in csv_files:
        full_csv_path = os.path.join(attributes_dir, csv_file)
        # HUC has to be read in as string to preserve leading zeros.
        try:
            temp_df = pd.read_csv(full_csv_path, dtype={'huc': str})
            all_csv_df = pd.concat([all_csv_df, temp_df], ignore_index=True)
            refined_csv_files_list.append(csv_file)
        except Exception:  # Happens if a file is empty (i.e. no mapping)
            FLOG.error("ERROR: loading csv {full_csv_path}")
            FLOG.error(traceback.format_exc())
            pass

    # Write to file
    all_csv_df.to_csv(os.path.join(output_flows_dir, 'nws_lid_attributes.csv'), index=False)

    # This section populates a geopackage of all potential sites and details
    # whether it was mapped or not (mapped field) and if not, why (status field).

    # Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.
    viz_out_gdf = out_gdf.to_crs(VIZ_PROJECTION)  # TODO: Accomodate AK projection?
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

    # Create list from all messages in messages dir.
    # messages_dir = os.path.join(output_flows_dir, 'messages')
    all_messages = []
    all_message_csvs = os.listdir(huc_messages_dir)
    for message_csv in all_message_csvs:
        full_message_csv_path = os.path.join(huc_messages_dir, message_csv)
        with open(full_message_csv_path, newline='') as message_file:
            reader = csv.reader(message_file)
            for row in reader:
                # all_messages.append(row.strip())
                all_messages.append(row)

    # Filter out columns and write out to file
    nws_sites_layer = os.path.join(output_mapping_dir, 'nws_lid_sites.gpkg')

    # Only write to sites geopackage if it didn't exist yet
    # (and this line shouldn't have been reached if we had an interrupted
    # run previously and are picking back up with a restart)
    if not os.path.exists(nws_sites_layer):
        FLOG.lprint("nws_sites_layer does not exist")

    else:
        # Write messages to DataFrame, split into columns, aggregate messages.
        if len(all_messages) > 0:

            FLOG.lprint(f"nws_sites_layer ({nws_sites_layer}) : adding messages")
            messages_df = pd.DataFrame(all_messages, columns=['message'])

            messages_df = (
                messages_df['message']
                .str.split(':', n=1, expand=True)
                .rename(columns={0: 'nws_lid', 1: 'status'})
            )
            status_df = messages_df.groupby(['nws_lid'])['status'].apply(', '.join).reset_index()

            # Join messages to populate status field to candidate sites. Assign
            # status for null fields.
            viz_out_gdf = viz_out_gdf.merge(status_df, how='left', on='nws_lid')

            #    viz_out_gdf['status'] = viz_out_gdf['status'].fillna('OK')

            # Add acceptance criteria to viz_out_gdf before writing
            viz_out_gdf['acceptable_coord_acc_code_list'] = str(acceptable_coord_acc_code_list)
            viz_out_gdf['acceptable_coord_method_code_list'] = str(acceptable_coord_method_code_list)
            viz_out_gdf['acceptable_alt_acc_thresh'] = float(acceptable_alt_acc_thresh)
            viz_out_gdf['acceptable_alt_meth_code_list'] = str(acceptable_alt_meth_code_list)
            viz_out_gdf['acceptable_site_type_list'] = str(acceptable_site_type_list)

            viz_out_gdf.to_file(nws_sites_layer, driver='GPKG')
        else:
            FLOG.lprint(f"nws_sites_layer ({nws_sites_layer}) : has no messages")

    return nws_sites_layer


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM')
    parser.add_argument(
        '-f',
        '--fim_run_dir',
        help='Path to directory containing HAND outputs, e.g. /data/previous_fim/fim_4_0_9_2',
        required=True,
    )
    parser.add_argument(
        '-e',
        '--env_file',
        help='Docker mount path to the catfim environment file. ie) data/config/catfim.env',
        required=True,
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
        '-jn',
        '--job_number_inundate',
        help='OPTIONAL: Number of processes to use for inundating'
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count'
        ' of the machine. Defaults to 1.',
        required=False,
        default=1,
        type=int,
    )

    parser.add_argument(
        '-ji',
        '--job_number_intervals',
        help='OPTIONAL: Number of processes to use for inundating multiple intervals in stage-based'
        ' inundation and interval job numbers should multiply to no more than one less than the CPU count '
        'of the machine. Defaults to 1.',
        required=False,
        default=1,
        type=int,
    )

    parser.add_argument(
        '-a',
        '--is_stage_based',
        help='Run stage-based CatFIM instead of flow-based? Add this -a param to make it stage based,'
        ' leave it off for flow based',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-t',
        '--output_folder',
        help='OPTIONAL: Target location, Where the output folder will be. Defaults to /data/catfim/',
        required=False,
        default='/data/catfim/',
    )
    parser.add_argument(
        '-o', '--overwrite', help='OPTIONAL: Overwrite files', required=False, action="store_true"
    )
    parser.add_argument(
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.',
        required=False,
        default='5',
    )

    # lid_to_run temp disabled
    # parser.add_argument(
    #     '-l',
    #     '--lid_to_run',
    #     help='OPTIONAL: NWS LID, lowercase, to produce CatFIM for. Currently only accepts one. Defaults to all sites',
    #     required=False,
    #     default='all',
    # )

    # lst_hucs temp disabled. All hucs in fim outputs in a directory will used
    # parser.add_argument(
    #     '-lh',
    #     '--lst_hucs',
    #     help='OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs',
    #     required=False,
    #     default='all',
    # )

    parser.add_argument(
        '-mc',
        '--past_major_interval_cap',
        help='OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs?'
        ' of the machine. Defaults to 5.',
        required=False,
        default=5.0,
        type=float,
    )

    # NOTE: This params is for quick debugging only and should not be used in a production mode
    parser.add_argument(
        '-me',
        '--nwm_metafile',
        help='OPTIONAL: If you have a pre-existing nwm metadata pickle file, you can path to it here.'
        ' e.g.: /data/catfim/nwm_metafile.pkl',
        required=False,
        default="",
    )

    args = vars(parser.parse_args())

    try:

        # call main program
        process_generate_categorical_fim(**args)

    except Exception:
        FLOG.critical(traceback.format_exc())
