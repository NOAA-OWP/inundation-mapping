#!/usr/bin/env python3

import argparse
import copy
import csv
import glob
import os
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


# TODO: Aug 2024: This script was upgraded significantly with lots of misc TODO's embedded.
# Lots of inline documenation needs updating as well


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
    lid_to_run,
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
    if output_folder.endswith("/"):
        output_folder = output_folder[:-1]
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

    # TODO: Aug 2024: Job values are not well used. There are some times where not
    # all three job values are not being used. This needs to be cleaned up.
    # Check job numbers and raise error if necessary
    # Considering how we are using each CPU very well at all, we could experiment
    # with either overclocking or chagnign to threading. Of course, if we change
    # to threading we ahve to be super careful about file and thread collisions (locking)

    # commented out for now for some small overclocking tests (carefully of course)
    # total_cpus_requested = job_number_huc * job_number_inundate * job_number_intervals
    # total_cpus_available = os.cpu_count() - 2
    # if total_cpus_requested > total_cpus_available:
    #     raise ValueError(
    #         f"The HUC job number (jh) [{job_number_huc}]"
    #         f" multiplied by the inundate job number (jn) [{job_number_inundate}]"
    #         f" multiplied by the job number intervals (ji) [{job_number_intervals}]"
    #         " exceeds your machine\'s available CPU count minus one."
    #         " Please lower one or more of those values accordingly."
    #     )

    # we are getting too many folders and files. We want just huc folders.
    # output_flow_dir_list = os.listdir(fim_run_dir)
    # looking for folders only starting with 0, 1, or 2
    # Code variation for dropping all Alaska HUCS:

    valid_ahps_hucs = [
        x
        for x in os.listdir(fim_run_dir)
        if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2'] and x[:2] != "19"
    ]

    # Temp debug to drop it to one HUC or more only, not the full output dir
    valid_ahps_hucs = ["10200203"] # has dropped records
    # valid_ahps_hucs = ["05060001"]
    # valid_ahps_hucs = ["10260008"]

    # Code variation for KEEPING Alaska HUCS:
    # valid_ahps_hucs = [
    #     x
    #     for x in os.listdir(fim_run_dir)
    #     if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2']
    # ]

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

    # os.makedirs(output_flows_dir, exist_ok=True) # Stage doesn't use it
    os.makedirs(output_mapping_dir, exist_ok=True)
    os.makedirs(attributes_dir, exist_ok=True)

    FLOG.lprint("================================")
    FLOG.lprint(f"Start generate categorical fim for {catfim_method} - (UTC): {dt_string}")
    FLOG.lprint("")

    FLOG.lprint(
        f"Processing {num_hucs} huc(s) with Alaska temporarily removed"
    )  # Code variation for DROPPING Alaska HUCs
    # FLOG.lprint(f"Processing {num_hucs} huc(s)") # Code variation for KEEPING Alaska HUCs

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
    catfim_sites_gpkg_file_path = ""

    """
    Sept 2024: There is a very large amount of duplication to stage versus flow based but
    we can continue to optimize it over time
    """

    # STAGE-BASED
    if is_stage_based:
        # Generate Stage-Based CatFIM mapping
        # does flows and inundation  (mapping)
        catfim_sites_gpkg_file_path = generate_stage_based_categorical_fim(
            output_catfim_dir,
            fim_run_dir,
            nwm_us_search,
            nwm_ds_search,
            lid_to_run,
            env_file,
            job_number_inundate,
            job_number_huc,
            valid_ahps_hucs,
            job_number_intervals,
            past_major_interval_cap,
            nwm_metafile,
        )

        # creates the gkpgs (tif's created above)
        # TODO: Aug 2024, so we need to clean it up
        # This step does not need a job_number_inundate as it can't really use use it.
        # It processes primarily hucs and ahps in multiproc
        # for now, we will manuall multiple the huc * 5 (max number of ahps types)
        ahps_jobs = job_number_huc * 5
        post_process_cat_fim_for_viz(
            catfim_method, output_catfim_dir, ahps_jobs, fim_version, log_output_file
        )

    # FLOW-BASED
    else:
        FLOG.lprint("")
        FLOG.lprint('Start creating flow files using the ' + catfim_method + ' technique...')
        FLOG.lprint("")
        start = time.time()

        # generate flows is only using one of the incoming job number params
        # so let's multiply -jh (huc) and -jn (inundate)
        job_flows = job_number_huc * job_number_inundate
        catfim_sites_gpkg_file_path = generate_flows(
            output_catfim_dir,
            nwm_us_search,
            nwm_ds_search,
            lid_to_run,
            env_file,
            job_flows,
            is_stage_based,
            valid_ahps_hucs,
            nwm_metafile,
            log_output_file,
        )
        end = time.time()
        elapsed_time = (end - start) / 60
        FLOG.lprint("")
        FLOG.lprint(f"Finished creating flow files in {str(elapsed_time).split('.')[0]} minutes \n")

        # Generate CatFIM mapping (not used by stage)
        manage_catfim_mapping(
            fim_run_dir,
            output_flows_dir,
            output_catfim_dir,
            catfim_method,
            job_number_huc,
            job_number_inundate,
            log_output_file,
        )

    # end if else

    # Updating mapping status
    FLOG.lprint("")
    FLOG.lprint('Updating mapping status...')
    update_flow_mapping_status(output_mapping_dir, catfim_sites_gpkg_file_path)

    # Create CSV versions of the final geopackages.
    # FLOG.lprint('Creating CSVs. This may take several minutes.')
    # create_csvs(output_mapping_dir, is_stage_based)

    FLOG.lprint("================================")
    FLOG.lprint("End generate categorical fim")

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    FLOG.lprint(f"Ended (UTC): {dt_string}")

    # calculate duration
    time_duration = overall_end_time - overall_start_time
    FLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")

    return


def update_flow_mapping_status(output_mapping_dir, catfim_sites_gpkg_file_path):
  
    # Find all LIDs with empty mapping output folders
    subdirs = [str(i) for i in Path(output_mapping_dir).rglob('**/*') if i.is_dir()]

    print("")

    empty_nws_lids = [Path(directory).name for directory in subdirs if not list(Path(directory).iterdir())]
    # if len(empty_nws_lids) > 0:
    #     FLOG.warning(f"Empty_nws_lids are.. {empty_nws_lids}")

    # Write list of empty nws_lids to DataFrame, these are sites that failed in inundation.py
    missing_lids_df = pd.DataFrame({'ahps_lid': empty_nws_lids})

    missing_lids_df['did_it_map'] = 'no'

    # Import geopackage output from flows creation
    if not os.path.exists(catfim_sites_gpkg_file_path):
        FLOG.critical(f"Primary library gpkg of {catfim_sites_gpkg_file_path} does not exist."
                      " Check logs for possible errors. Program aborted.")
        sys.exit(1)
    
    flows_gdf = gpd.read_file(catfim_sites_gpkg_file_path, engine='fiona')

    if len(flows_gdf) == 0:
        FLOG.critical(f"flows_gdf is empty. Path is {catfim_sites_gpkg_file_path}. Program aborted.")
        sys.exit(1)

    try:
        # Join failed sites to flows df
        flows_gdf = flows_gdf.merge(missing_lids_df, how='left', on='ahps_lid')

        # Switch mapped column to no for failed sites and update status
        flows_gdf.loc[flows_gdf['did_it_map'] == 'no', 'mapped'] = 'no'
        
        # in theory this should not happen as if it failed a status message should exist
        flows_gdf.loc[(flows_gdf['mapped'] == 'no') & 
                      (flows_gdf['status'] == ''), 'status'] = 'ahps record in error'

        flows_gdf.loc[flows_gdf['status'] == 'OK', 'mapped'] = 'yes'

        # but if there is a status value starting with ---, it means it has some, but
        # not all missing stages/thresholds and there for should be mapped.
        flows_gdf.loc[flows_gdf['status'].str.startswith('---') == True, 'mapped'] = 'yes'
        
        flows_gdf = flows_gdf.drop(columns=['did_it_map'])

        # Write out to file
        # TODO: Aug 29, 204: Not 100% sure why, but the gpkg errors out... likely missing a projection
        #   Sep 25/24, we need to set which is the gdf geometry column
        
        flows_gdf.to_file(catfim_sites_gpkg_file_path, index=False, driver='GPKG', engine="fiona")

        # csv flow file name
        nws_lid_csv_file_path = catfim_sites_gpkg_file_path.replace(".gkpg", ".csv")

        # and we write a csv version at this time as well.
        # and this csv is good
        flows_gdf.to_csv(nws_lid_csv_file_path)

    except Exception as e:
        FLOG.critical(f"{output_mapping_dir} : No LIDs, \n Exception: \n {repr(e)} \n")
        FLOG.critical(traceback.format_exc())
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
    all_lists,
    past_major_interval_cap,
    job_number_inundate,
    job_number_intervals,
    nwm_flows_region_df,
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

        all_messages = []
        stage_based_att_dict = {}

        mapping_dir = os.path.join(output_catfim_dir, "mapping")
        attributes_dir = os.path.join(output_catfim_dir, 'attributes')
        # output_flows_dir = os.path.join(output_catfim_dir, "flows")
        huc_messages_dir = os.path.join(mapping_dir, 'huc_messages')

        # Make output directory for the particular huc in the mapping folder
        mapping_huc_directory = os.path.join(mapping_dir, huc)
        if not os.path.exists(mapping_huc_directory):
            os.mkdir(mapping_huc_directory)

        # Define paths to necessary HAND and HAND-related files.
        usgs_elev_table = os.path.join(fim_dir, huc, 'usgs_elev_table.csv')
        branch_dir = os.path.join(fim_dir, huc, 'branches')

        # Loop through each lid in nws_lids list
        nws_lids = huc_dictionary[huc]

        MP_LOG.lprint(f"Lids to process for {huc} are {nws_lids}")

        skip_lid_process = False
        # -- If necessary files exist, continue -- #
        # Yes, each lid gets a record no matter what, so we need some of these messages duplicated
        # per lid record
        if not os.path.exists(usgs_elev_table):
            msg = ":Internal Error: Missing key data from HUC record (usgs_elev_table missing)"
            all_messages.append(huc_lid_id + msg)
            MP_LOG.warning(huc_lid_id + msg)
            skip_lid_process = True

        if not os.path.exists(branch_dir):
            msg = ":branch directory missing"
            all_messages.append(huc_lid_id + msg)
            MP_LOG.warning(huc_lid_id + msg)
            skip_lid_process = True            

        categories = ['action', 'minor', 'moderate', 'major', 'record']

        if skip_lid_process ==  False: # else skip to message processing
            usgs_elev_df = pd.read_csv(usgs_elev_table)

            df_cols = {"nws_lid": pd.Series(dtype='str'),
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
                    "dtm_adj_ft": pd.Series(dtype='str'),
                    "dadj_w_ft": pd.Series(dtype='float'),
                    "dadj_w_m": pd.Series(dtype='float'),
                    "lid_alt_ft": pd.Series(dtype='float'), 
                    "lid_alt_m": pd.Series(dtype='float'),
                    "mapped": pd.Series(dtype='str'),
                    "status": pd.Series(dtype='str'),
                    }

            for lid in nws_lids:
                MP_LOG.lprint("-----------------------------------")
                huc_lid_id = f"{huc} : {lid}"
                MP_LOG.lprint(f"processing {huc_lid_id}")

                lid = lid.lower()  # Convert lid to lower case

                # Make mapping lid_directory.
                mapping_lid_directory = os.path.join(mapping_huc_directory, lid)
                if not os.path.exists(mapping_lid_directory):
                    os.mkdir(mapping_lid_directory)

                # Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
                thresholds, flows = get_thresholds(
                    threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
                )
                
                # MP_LOG.lprint(f"thresholds are {thresholds}")
                #MP_LOG.lprint(f"flows are {flows}")
                
                if thresholds is None or len(thresholds) == 0:
                    msg = ':error getting thresholds from WRDS API'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Check if stages are supplied, if not write message and exit.
                if all(thresholds.get(category, None) is None for category in categories):
                    msg = ':missing all threshold stage data'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Read stage values and calculate thresholds

                # TODO: Aug 2024, Is it really ok that record is missing? hummm
                # Earlier code lower was doing comparisons to see if the interval
                # value was been each of these 4 but sometimes one or more was None
                # Sep 24, 2024: We will add record to the list even though not there in 4.4.0.0 (sort of)

                # Yes.. this is goofy but we can fix it later
                action_stage = -1
                minor_stage = -1
                moderate_stage = -1
                major_stage = -1
                record_stage = -1

                 # An un-order list of just the stage value, stage not needed
                 # It is used to calculate intervals
                valid_stage_value_list = []
                valid_stages = []
                invalid_stages = []

                for stage in categories: # yes.. same as a stage list
                    if stage in thresholds:
                        stage_val = thresholds[stage]
                        if stage_val is None or stage_val == "":
                            stage_val = -1
                        else: 
                            valid_stage_value_list.append(stage_val)
                    else:
                        stage_val = -1  # temp value to help it fall out as being invalid

                    is_valid_stage = (stage_val != -1)

                    if is_valid_stage == True:
                        valid_stages.append(stage)
                    else:
                        invalid_stages.append(stage)

                    # Yes.. this is goofy but we can fix it later
                    if stage == "action" and is_valid_stage:
                        action_stage = stage_val
                    elif stage == "minor" and is_valid_stage:
                        minor_stage = stage_val
                    elif stage == "moderate" and is_valid_stage:
                        moderate_stage = stage_val
                    elif stage == "major" and is_valid_stage:
                        major_stage = stage_val
                    elif stage == "record" and is_valid_stage:
                        record_stage = stage_val
                        
                    # TODO: Sept 2024: What if WRDS gave us stage values that was inconsistantly ordered?
                    # ie) action higher than major.
                    # Look into it later
                
                MP_LOG.trace(f"stage values in order are {action_stage}, {minor_stage}, {moderate_stage}, {major_stage}, {record_stage} ")

                if len(invalid_stages) == 5:
                    msg = ':no valid threshold values are available'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                missing_stages_msg = ""
                # Yes.. a bit weird, we are going to put three dashs in front of the message
                # to help show it is valid even with a missing stage msg
                for ind, stage in enumerate(invalid_stages):
                    if ind == 0:
                        missing_stages_msg = f":---Missing stage data for {stage}"
                    else:
                        missing_stages_msg += f"; {stage}"

                if missing_stages_msg != "":
                    all_messages.append(lid + missing_stages_msg)
                    MP_LOG.warning(huc_lid_id + missing_stages_msg)

                interval_list = np.arange(
                    min(valid_stage_value_list), max(valid_stage_value_list) + past_major_interval_cap, 1.0
                )  # Go an extra 5 ft beyond the max stage, arbitrary

                MP_LOG.trace(f"interval list is {interval_list}")

                # Look for acceptable elevs
                acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id)
                if acceptable_usgs_elev_df is None or len(acceptable_usgs_elev_df) == 0:
                    msg = ":unable to find gage data"
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Get the dem_adj_elevation value from usgs_elev_table.csv.
                # Prioritize the value that is not from branch 0.
                lid_usgs_elev, dem_eval_messages = __adj_dem_evalation_val(
                    acceptable_usgs_elev_df, lid, huc_lid_id
                )
                all_messages = all_messages + dem_eval_messages
                if len(dem_eval_messages) > 0:
                    continue

                # Initialize nested dict for lid attributes
                stage_based_att_dict.update({lid: {}})

                # Find lid metadata from master list of metadata dictionaries.
                metadata = next(
                    (item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False
                )
                lid_altitude = metadata['usgs_data']['altitude']
                if lid_altitude is None or lid_altitude == 0:
                    msg = ':ahps altitude value is invalid'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue
                
                # Filter out sites that don't have "good" data
                try:
                    ## Removed this part to relax coordinate accuracy requirements
                    # if not metadata['usgs_data']['coord_accuracy_code'] in acceptable_coord_acc_code_list:
                    #     MP_LOG.warning(
                    #         f"\t{huc_lid_id}: {metadata['usgs_data']['coord_accuracy_code']} "
                    #         "Not in acceptable coord acc codes"
                    #     )
                    #     continue
                    # if not metadata['usgs_data']['coord_method_code'] in acceptable_coord_method_code_list:
                    #     MP_LOG.warning(f"\t{huc_lid_id}: Not in acceptable coord method codes")
                    #     continue
                    if not metadata['usgs_data']['alt_method_code'] in acceptable_alt_meth_code_list:
                        MP_LOG.warning(f"{huc_lid_id}: Not in acceptable alt method codes")
                        continue
                    if not metadata['usgs_data']['site_type'] in acceptable_site_type_list:
                        MP_LOG.warning(f"{huc_lid_id}: Not in acceptable site type codes")
                        continue
                    if not float(metadata['usgs_data']['alt_accuracy_code']) <= acceptable_alt_acc_thresh:
                        MP_LOG.warning(f"{huc_lid_id}: Not in acceptable threshold range")
                        continue
                except Exception:
                    MP_LOG.error(f"{huc_lid_id}: filtering out 'bad' data in the usgs data")
                    MP_LOG.error(traceback.format_exc())
                    continue

                datum_adj_ft, datum_messages = __adjust_datum_ft(flows, metadata, lid, huc_lid_id)
                all_messages = all_messages + datum_messages
                if datum_adj_ft is None:
                    continue
                
                # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
                unfiltered_segments = list(set(get_nwm_segs(metadata)))

                # Filter segments to be of like stream order.
                desired_order = metadata['nwm_feature_data']['stream_order']
                segments = filter_nwm_segments_by_stream_order(
                    unfiltered_segments, desired_order, nwm_flows_region_df
                )

                # Check for large discrepancies between the elevation values from WRDS and HAND.
                #   Otherwise this causes bad mapping.
                elevation_diff = lid_usgs_elev - (lid_altitude * 0.3048)
                if abs(elevation_diff) > 10:
                    msg = ':large discrepancy in elevation estimates from gage and HAND'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # This function sometimes is called within a MP but sometimes not.
                # So, we might have an MP inside an MP
                # and we will need a new prefix for it.

                # Becuase we already are in an MP, lets merge up what we have at this point
                # Before creating child MP files
                MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix)

                # For each flood category / magnitude
                MP_LOG.lprint(f"{huc_lid_id}: About to process flood categories")
                child_log_file_prefix = MP_LOG.MP_calc_prefix_name(
                    parent_log_output_file, "MP_produce_catfim_tifs"
                )

                # print(f"valid_stages are {valid_stages}")
                
                # At this point we have at least one valid stage/category
                # cyle through on the stages that are valid
                for category in valid_stages:  # a category is the same thing as a stage at this point.
                    # MP_LOG.lprint(f"{huc_lid_id}: Magnitude is {category}")
                    # Pull stage value and confirm it's valid, then process
                    stage = thresholds[category]

                    # datum_adj_ft should not be None at this point
                    # Call function to execute mapping of the TIFs.

                    # These are the up to 5 magnitudes being inundated at their stage value
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
                        })

                # So, we might have an MP inside an MP
                # let's merge what we have at this point, before we go into another MP
                MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix, True)

                MP_LOG.lprint(f"all interval stages are {interval_list}")

                # In order not to create duplicate features with the same stage value, 
                # we keep track of the stage values that have been inundated and skip them
                # if a dup is found. Start with a copy of the list that was created above
                # which will always have at least one record
                inudated_stages = copy.copy(valid_stage_value_list)

                # Now we will do another set of inundations, but this one is based on
                # not the stage flow but flow based on each interval
                tif_child_log_file_prefix = MP_LOG.MP_calc_prefix_name(parent_log_output_file, "MP_prod_sb_tifs")
                with ProcessPoolExecutor(max_workers=job_number_intervals) as executor:
                    try:
                        # There will always be at least one
                        # we need to skip the stages where their value is -1 as they 
                        # did not have a stage value from nwps and need to be discluded.
                        for interval_stage in interval_list:
                            
                            # That value has already been inundated, likely but the original stage record
                            if interval_stage in inudated_stages:
                                continue
                            else:
                                inudated_stages.append(interval_stage)
                            
                            # Determine category the stage value belongs with.
                            if action_stage != -1 and action_stage <= interval_stage < minor_stage:
                                category = 'action_' + str(interval_stage).replace('.', 'p') + 'ft'
                            elif minor_stage != -1 and minor_stage <= interval_stage < moderate_stage:
                                category = 'minor_' + str(interval_stage).replace('.', 'p') + 'ft'
                            elif moderate_stage != -1 and moderate_stage <= interval_stage < major_stage:
                                category = 'moderate_' + str(interval_stage).replace('.', 'p') + 'ft'
                            elif major_stage != -1 and interval_stage <= interval_stage < record_stage:
                                category = 'major_' + str(interval_stage).replace('.', 'p') + 'ft'
                            elif record_stage != -1:  # interval_stage >= record_stage
                                category = 'record_' + str(interval_stage).replace('.', 'p') + 'ft'
                            else:
                                continue

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
                        MP_LOG.error("ERROR: type error in ProcessPool, likely in the interval tests")
                        MP_LOG.error(traceback.format_exc())
                        continue

                    except Exception:
                        MP_LOG.critical("ERROR: ProcessPool has an error")
                        MP_LOG.critical(traceback.format_exc())
                        # merge MP Logs (Yes)
                        MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix, True)
                        sys.exit(1)

                # merge MP Logs (merging MP into an MP (proc_pool in a proc_pool))
                MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix, True)

                # Create a csv with same information as geopackage but with each threshold as new record.
                # Probably a less verbose way.
                csv_df = pd.DataFrame(df_cols)  # for first appending
                #for threshold in magnitudes:
                # TODO: Sept 2024: Should this be categories or valid_stage_list. Likely categories
                # as we want all five stages.
                # stage = category = threshold (renaming will be looked at later)
                # Missing_stages_msg might be empty but we still want the record to continue
                # even if there are one or more missing_stages

                for threshold in valid_stages: 
                #  for threshold in categories:
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
                                'stage': thresholds[threshold],
                                'stage_uni': thresholds['units'],
                                's_src': thresholds['source'],
                                'wrds_time': thresholds['wrds_timestamp'],
                                'nrldb_time': metadata['nrldb_timestamp'],
                                'nwis_time': metadata['nwis_timestamp'],
                                'lat': [float(metadata['nws_preferred']['latitude'])],
                                'lon': [float(metadata['nws_preferred']['longitude'])],
                                'dtm_adj_ft': stage_based_att_dict[lid][threshold]['datum_adj_ft'],
                                'dadj_w_ft': stage_based_att_dict[lid][threshold]['datum_adj_wse_ft'],
                                'dadj_w_m': stage_based_att_dict[lid][threshold]['datum_adj_wse_m'],
                                'lid_alt_ft': stage_based_att_dict[lid][threshold]['lid_alt_ft'],
                                'lid_alt_m': stage_based_att_dict[lid][threshold]['lid_alt_m'],
                                'mapped': 'yes',
                                'status': '',
                            }  # yes.. status is empty at this time, we update it later
                        )
                        csv_df = pd.concat([csv_df, line_df], ignore_index=True)

                    except Exception:
                        # is this the text we want users to see
                        msg = f':Error with threshold {threshold}'
                        all_messages.append(lid + msg)
                        MP_LOG.error(huc_lid_id + msg)                        
                        MP_LOG.error(traceback.format_exc())
                        continue
                        # sys.exit(1)

                # might be that none of the lids for this HUC passed
                # If a site folder exists (ie a flow file was written) save files containing site attributes.
                # if os.path.exists(mapping_lid_directory):
                if len(csv_df) > 0:
                    # Round flow and stage columns to 2 decimal places.
                    csv_df = csv_df.round({'q': 2, 'stage': 2})

                    # Export DataFrame to csv containing attributes
                    attributes_filepath = os.path.join(attributes_dir, f'{lid}_attributes.csv')
                    csv_df.to_csv(attributes_filepath, index=False)
                    
                    # If it made it to this point (i.e. no continues), there were no major preventers of mapping
                    if (missing_stages_msg == ""):
                        all_messages.append(lid + ':OK')
                else:
                    msg = ':missing all calculated flows'
                    all_messages.append(lid + msg)
                    MP_LOG.error(huc_lid_id + msg)

                MP_LOG.success(f'{huc_lid_id}: Complete')
                # mark_complete(mapping_lid_directory)
            # end of for loop
        # end of if

        # Write all_messages by HUC to be scraped later.
        if len(all_messages) > 0:

            # TODO: Aug 2024: This is now identical to the way flow handles messages
            # but the system should probably be changed to somethign more elegant but good enough
            # for now. At least is is MP safe.
            huc_messages_txt_file = os.path.join(huc_messages_dir, str(huc) + '_messages.txt')
            with open(huc_messages_txt_file, 'w') as f:
                for item in all_messages:
                    item = item.strip()
                    # f.write("%s\n" % item)
                    f.write(f"{item}\n")

    except Exception:
        MP_LOG.error(f"{huc} : {lid} Error iterating through huc stage based")
        MP_LOG.error(traceback.format_exc())

    return


def __adjust_datum_ft(flows, metadata, lid, huc_lid_id):

    # TODO: Aug 2024: This whole parts needs revisiting. Lots of lid data has changed and this
    # is all likely very old.

    # Jul 2024: For now, we will duplicate messages via all_messsages and via the logging system.
    all_messages = []

    datum_adj_ft = None
    ### --- Do Datum Offset --- ###
    # determine source of interpolated threshold flows, this will be the rating curve that will be used.
    rating_curve_source = flows.get('source')
    if rating_curve_source is None:
        msg = f'{huc_lid_id}:No source for rating curve'
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
        msg = f'{huc_lid_id}:datum info unavailable'
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
                msg = f'{huc_lid_id}:NOAA VDatum adjustment error, CRS is missing'
                all_messages.append(msg)
                MP_LOG.error(msg)
            if 'HTTPSConnectionPool' in ex:
                time.sleep(10)  # Maybe the API needs a break, so wait 10 seconds
                try:
                    datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data, region='contiguous')
                except Exception:
                    msg = f'{huc_lid_id}:NOAA VDatum adjustment error, possible API issue'
                    all_messages.append(msg)
                    MP_LOG.error(msg)
            if 'Invalid projection' in ex:
                msg = f'{huc_lid_id}:NOAA VDatum adjustment error, invalid projection: crs={crs}'
                all_messages.append(msg)
                MP_LOG.error(msg)
            return None, all_messages

    return datum_adj_ft, all_messages


def __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id):
    acceptable_usgs_elev_df = None
    try:
        # Drop columns that offend acceptance criteria
        usgs_elev_df['acceptable_codes'] = (
            # usgs_elev_df['usgs_data_coord_accuracy_code'].isin(acceptable_coord_acc_code_list)
            # & usgs_elev_df['usgs_data_coord_method_code'].isin(acceptable_coord_method_code_list)
            usgs_elev_df['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list)
            & usgs_elev_df['usgs_data_site_type'].isin(acceptable_site_type_list)
        )

        usgs_elev_df = usgs_elev_df.astype({'usgs_data_alt_accuracy_code': float})
        usgs_elev_df['acceptable_alt_error'] = np.where(
            usgs_elev_df['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh, True, False
        )

        acceptable_usgs_elev_df = usgs_elev_df[
            (usgs_elev_df['acceptable_codes'] == True) & (usgs_elev_df['acceptable_alt_error'] == True)
        ]

        # # TEMP DEBUG Record row difference and write it to a CSV or something
        # label = 'Old code' ## TEMP DEBUG
        # num_potential_rows = usgs_elev_df.shape[0]
        # num_acceptable_rows = acceptable_usgs_elev_df.shape[0]
        # out_message = f'{label}: kept {num_acceptable_rows} rows out of {num_potential_rows} available rows.'

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

    # MP_LOG.trace(locals())

    lid_usgs_elev = 0
    all_messages = []
    try:
        matching_rows = acceptable_usgs_elev_df.loc[
            acceptable_usgs_elev_df['nws_lid'] == lid.upper(), 'dem_adj_elevation'
        ]

        if len(matching_rows) == 0:
            msg = ':gage not in HAND usgs gage records'
            all_messages.append(lid + msg)
            MP_LOG.warning(huc_lid_id + msg)            
            return lid_usgs_elev, all_messages

        # It means there are two level paths, use the one that is not 0
        # There will never be more than two
        if len(matching_rows) == 2:
            lid_usgs_elev = acceptable_usgs_elev_df.loc[
                (acceptable_usgs_elev_df['nws_lid'] == lid.upper())
                & (acceptable_usgs_elev_df['levpa_id'] != 0),
                'dem_adj_elevation',
            ].values[0]
        else:
            lid_usgs_elev = acceptable_usgs_elev_df.loc[
                acceptable_usgs_elev_df['nws_lid'] == lid.upper(), 'dem_adj_elevation'
            ].values[0]

        if lid_usgs_elev == 0:
            msg = ':dem adjusted elevation is 0 or not set'
            all_messages.append(lid + msg)
            MP_LOG.warning(huc_lid_id + msg)            
            return lid_usgs_elev, all_messages

    except IndexError:  # Occurs when LID is missing from table (yes. warning)
        msg = ':error when extracting dem adjusted elevation value'
        all_messages.append(lid + msg)
        MP_LOG.warning(f"{huc_lid_id}: adjusting dem_adj_elevation")        
        MP_LOG.warning(huc_lid_id + msg)
        MP_LOG.warning(traceback.format_exc())

    MP_LOG.trace(f"{huc_lid_id} : lid_usgs_elev is {lid_usgs_elev}")

    return lid_usgs_elev, all_messages


# This creates a HUC iterator with each HUC creating its flow files and tifs
def generate_stage_based_categorical_fim(
    output_catfim_dir,
    fim_run_dir,
    nwm_us_search,
    nwm_ds_search,
    lid_to_run,
    env_file,
    job_number_inundate,
    job_number_huc,
    lst_hucs,
    job_number_intervals,
    past_major_interval_cap,
    nwm_metafile,
):

    '''
    Sep 2024,
    I believe this can be radically simplied, but just startign with a dataframe for each ahps and populate what we
    can as we go. By the end of this, it will know it's mapped status and reasons why. It can save one per huc and
    merged later.  This would drop the whole huc_messages system and the need to updates status later. It would
    also make it much easier to read. If we write a bit carefully with functions where reasonable, flow based
    can likely use most of them too.
    '''

    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')

    # Create HUC message directory to store messages that will be read and joined after multiprocessing
    huc_messages_dir = os.path.join(output_mapping_dir, 'huc_messages')
    os.makedirs(huc_messages_dir, exist_ok=True)

    FLOG.lprint("Starting generate_flows (Stage Based)")

    # If it is stage based, generate flows returns all of these objects.
    # If flow based, generate flows returns only
    # (huc_dictionary, out_gdf, ___, threshold_url, all_lists, nwm_flows_df, nwm_flows_alaska_df) = generate_flows( # With Alaska

    # Generate flows is only using one of the incoming job number params
    # so let's multiply -jh (huc) and -jn (inundate)
    job_flows = job_number_huc * job_number_inundate
    if job_flows > 90:
        job_flows == 90
    (huc_dictionary, out_gdf, ___, threshold_url, all_lists, all_nwm_flows_df) = generate_flows(  # No Alaska
        output_catfim_dir,
        nwm_us_search,
        nwm_ds_search,
        lid_to_run,
        env_file,
        job_flows,
        True,
        lst_hucs,
        nwm_metafile,
        str(FLOG.LOG_FILE_PATH),
    )

    child_log_file_prefix = FLOG.MP_calc_prefix_name(FLOG.LOG_FILE_PATH, "MP_iter_hucs")

    FLOG.lprint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    FLOG.lprint("Start processing HUCs for Stage-Based CatFIM")
    num_hucs = len(lst_hucs)
    huc_index = 0
    FLOG.lprint(f"Number of hucs to process is {num_hucs}")
   
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in huc_dictionary:
            if huc in lst_hucs:
                # FLOG.lprint(f'Generating stage based catfim for : {huc}')

                # Code variation for DROPPING Alaska HUCs
                nwm_flows_region_df = all_nwm_flows_df

                # # Code variation for keeping alaska HUCs
                # nwm_flows_region_df = nwm_flows_alaska_df if str(huc[:2]) == '19' else nwm_flows_df

                progress_stmt = f"index {huc_index + 1} of {num_hucs}"
                executor.submit(
                    iterate_through_huc_stage_based,
                    output_catfim_dir,
                    huc,
                    fim_run_dir,
                    huc_dictionary,
                    threshold_url,
                    all_lists,
                    past_major_interval_cap,
                    job_number_inundate,
                    job_number_intervals,
                    nwm_flows_region_df,
                    str(FLOG.LOG_FILE_PATH),
                    child_log_file_prefix,
                    progress_stmt,
                )
                huc_index += 1
    # Need to merge MP logs here, merged into the "master log file"

    FLOG.merge_log_files(FLOG.LOG_FILE_PATH, child_log_file_prefix, True)

    FLOG.lprint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    FLOG.lprint('Wrapping up processing HUCs for Stage-Based CatFIM...')

    attrib_csv_files = [x for x in os.listdir(attributes_dir) if x.endswith('_attributes.csv')]
    
    # print(f"attrib_csv_files are {attrib_csv_files}")

    all_csv_df = pd.DataFrame()
    refined_csv_files_list = []
    for csv_file in attrib_csv_files:

        full_csv_path = os.path.join(attributes_dir, csv_file)
        # HUC has to be read in as string to preserve leading zeros.
        try:
            temp_df = pd.read_csv(full_csv_path, dtype={'huc': str})
            if len(temp_df) > 0:
                all_csv_df = pd.concat([all_csv_df, temp_df], ignore_index=True)
                refined_csv_files_list.append(csv_file)
        except Exception:  # Happens if a file is empty (i.e. no mapping)
            FLOG.error(f"ERROR: loading csv {full_csv_path}")
            FLOG.error(traceback.format_exc())
            pass

    # Write to file
    if len(all_csv_df) == 0:
        raise Exception("no csv files found")
    
    all_csv_df.to_csv(os.path.join(attributes_dir, 'nws_lid_attributes.csv'), index=False)

    # This section populates a geopackage of all potential sites and details
    # whether it was mapped or not (mapped field) and if not, why (status field).

    # Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.

    # TODO: Accomodate AK projection?   Yes.. and Alaska and CONUS should all end up as the same projection output
    # epsg:5070, we really want 3857 out for all outputs
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
    lids_df['mapped'] = 'yes'

    # Identify what lids were mapped by merging with lids_df. Populate
    # 'mapped' column with 'No' if sites did not map.
    viz_out_gdf = viz_out_gdf.merge(lids_df, how='left', on='nws_lid')
    viz_out_gdf['mapped'] = viz_out_gdf['mapped'].fillna('no')

    # Read all messages for all HUCs
    # This is basically identical to a chunk in flow based. At a min, consolidate
    # or better yet, find a more elegant, yet still MP safe, system than .txt files
    # but it works.. so maybe someday.
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

    # Filter out columns and write out to file
    # flow based doesn't make it here only stage
    nws_lid_gpkg_file_path = os.path.join(output_mapping_dir, 'stage_based_catfim_sites.gpkg')

    # Write messages to DataFrame, split into columns, aggregate messages.
    if len(huc_message_list) > 0:

        FLOG.lprint(f"nws_sites_layer ({nws_lid_gpkg_file_path}) : adding messages")
        messages_df = pd.DataFrame(huc_message_list, columns=['message'])

        messages_df = (
            messages_df['message']
            .str.split(':', n=1, expand=True)
            .rename(columns={0: 'nws_lid', 1: 'status'})
        )

        # We want one viz_out_gdf record per ahps and if there are more than one, contact the messages
        # status_df = messages_df.groupby(['nws_lid'])['status'].apply(', '.join).reset_index()
        status_df = messages_df.groupby(['nws_lid'])['status'].agg(lambda x: ',\n'.join(x)).reset_index()

        # Join messages to populate status field to candidate sites. Assign
        # status for null fields.
        viz_out_gdf = viz_out_gdf.merge(status_df, how='left', on='nws_lid')

        # viz_out_gdf.reset_index(inplace=True)

        #  (msg in flows)
        viz_out_gdf['status'] = viz_out_gdf['status'].fillna('All calculated threshold values present')

        # Add acceptance criteria to viz_out_gdf before writing
        viz_out_gdf['acceptable_coord_acc_code_list'] = str(acceptable_coord_acc_code_list)
        viz_out_gdf['acceptable_coord_method_code_list'] = str(acceptable_coord_method_code_list)
        viz_out_gdf['acceptable_alt_acc_thresh'] = float(acceptable_alt_acc_thresh)
        viz_out_gdf['acceptable_alt_meth_code_list'] = str(acceptable_alt_meth_code_list)
        viz_out_gdf['acceptable_site_type_list'] = str(acceptable_site_type_list)

        # Rename the stage_based_catfim db column from nws_lid to ahps_lid to be
        # consistant with all other CatFIM outputs
        viz_out_gdf.rename(columns = {"nws_lid": "ahps_lid"}, inplace = True)

        viz_out_gdf.to_file(nws_lid_gpkg_file_path, driver='GPKG', index=True, engine='fiona')

        csv_file_path = nws_lid_gpkg_file_path.replace(".gpkg", ".csv")
        viz_out_gdf.to_csv(csv_file_path)
    else:
        FLOG.lprint(f"nws_sites_layer ({nws_lid_gpkg_file_path}) : has no messages")

    return nws_lid_gpkg_file_path


if __name__ == '__main__':
    
    '''
    Sample
    python /foss_fim/tools/generate_categorical_fim.py -f /outputs/Rob_catfim_test_1 -jh 1 -jn 10 -ji 8 
    -e /data/config/catfim.env -t /data/catfim/rob_test/docker_test_1
    -me '/data/catfim/rob_test/nwm_metafile.pkl' -sb
    '''
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM')
    parser.add_argument(
        '-f',
        '--fim_run_dir',
        help='Path to directory containing HAND outputs, e.g. /data/previous_fim/fim_4_5_2_11',
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
        '-sb',
        '--is_stage_based',
        help='Run stage-based CatFIM instead of flow-based? Add this -sb param to make it stage based,'
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

    parser.add_argument(
        '-l',
        '--lid_to_run',
        help='OPTIONAL: NWS LID, lowercase, to produce CatFIM for. Currently only accepts one. Defaults to all sites',
        required=False,
        default='all',
    )

    # lst_hucs temp disabled. All hucs in fim outputs in a directory will used
    # NOTE: The HUCs you put in this, MUST be a HUC that is valid in your -f/ --fim_run_dir (HAND output folder)
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