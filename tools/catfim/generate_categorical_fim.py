#!/usr/bin/env python3

import argparse
import copy
import glob
import math
import os
import shutil
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone

import geopandas as gpd
import numpy as np
import pandas as pd
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


# from itertools import repeat
# from pathlib import Path


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
    # lid_to_run,
    lst_hucs,
    job_number_intervals,
    past_major_interval_cap,
    step_num,
    nwm_metafile,
):

    # ================================
    # Step System
    # This system allows us to to skip steps.
    # Steps that are skipped are assumed to have the valid files that are needed
    # When a number is submitted, ie) 2, it means skip steps 1 and start at 2
    '''
    Step number usage:
        0 = cover all (it is changed to 999 so all steps are covered)
    flow:
        1 = start at generate_flows
        2 = start at manage_catfim_mapping
        3 = start at update mapping status
    stage:
        1 = start at generate_flows and tifs
        2 = start at creation of gpkgs
        3 = start at update mapping status
    '''

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

    # ================================
    set_start_files_folders(
        step_num, output_catfim_dir, output_mapping_dir, output_flows_dir, attributes_dir, overwrite
    )

    # ================================
    if nwm_metafile != "":
        if os.path.exists(nwm_metafile) is False:
            raise Exception("The nwm_metadata (-me) file can not be found. Please remove or fix pathing.")
        file_ext = os.path.splitext(nwm_metafile)
        if file_ext.count == 0:
            raise Exception("The nwm_metadata (-me) file appears to be invalid. It is missing an extension.")
        if file_ext[1].lower() != ".pkl":
            raise Exception("The nwm_metadata (-me) file appears to be invalid. The extention is not pkl.")

    # ================================
    # Define default arguments. Modify these if necessary
    fim_version = os.path.split(fim_run_dir)[1]

    # ================================
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

    # ================================
    # Get HUCs from FIM run directory
    valid_ahps_hucs = [
        x
        for x in os.listdir(fim_run_dir)
        if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2']
    ]

    # If a HUC list is specified, only keep the specified HUCs
    lst_hucs = lst_hucs.split()
    if 'all' not in lst_hucs:
        valid_ahps_hucs = [x for x in valid_ahps_hucs if x in lst_hucs]
        dropped_huc_lst = list((set(lst_hucs).difference(valid_ahps_hucs)))

    valid_ahps_hucs.sort()

    num_hucs = len(valid_ahps_hucs)
    if num_hucs == 0:
        raise ValueError(
            f'The number of valid hucs compared to the output directory of {fim_run_dir} is zero.'
            ' Verify that you have the correct input folder and if you used the -lh flag that it'
            ' is a valid matching HUC.'
        )
    # End of Validation and setup
    # ================================

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    FLOG.lprint("================================")
    FLOG.lprint(f"Start generate categorical fim for {catfim_method} - (UTC): {dt_string}")
    FLOG.lprint("")

    FLOG.lprint(f"Processing {num_hucs} huc(s)")

    # If HUCs are given as an input
    if 'all' not in lst_hucs:
        print(f'HUCs to use (from input list): {valid_ahps_hucs}')

        if len(dropped_huc_lst) > 0:
            FLOG.warning('Listed HUCs not available in FIM run directory:')
            FLOG.warning(dropped_huc_lst)

    load_dotenv(env_file)
    API_BASE_URL = os.getenv('API_BASE_URL')
    if API_BASE_URL is None:
        raise ValueError(
            'API base url not found. '
            'Ensure inundation_mapping/tools/ has an .env file with the following info: '
            'API_BASE_URL, EVALUATED_SITES_CSV, WBD_LAYER, NWM_FLOWS_MS, '
            'USGS_METADATA_URL, USGS_DOWNLOAD_URL'
        )

    # TODO: lid_to_run functionality... remove? for now, just hard code lid_to_run as "all"
    lid_to_run = "all"

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
    catfim_sites_file_path = ""

    # TODO: Sept 2024: There is a very large amount of duplication to stage versus flow based but
    # we can continue to optimize it over time

    # STAGE-BASED
    if is_stage_based:
        # Generate Stage-Based CatFIM mapping
        # does flows and inundation  (mapping)

        catfim_sites_file_path = os.path.join(output_mapping_dir, 'stage_based_catfim_sites.gpkg')

        if step_num <= 1:

            df_restricted_sites = load_restricted_sites()

            generate_stage_based_categorical_fim(
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
                df_restricted_sites,
            )
        else:
            FLOG.lprint("generate_stage_based_categorical_fim step skipped")

        FLOG.lprint("")
        if step_num <= 2:
            # creates the gpkgs (tif's created above)
            # TODO: Aug 2024, so we need to clean it up
            # This step does not need a job_number_inundate as it can't really use use it.
            # It processes primarily hucs and ahps in multiproc
            # for now, we will manuall multiple the huc * 5 (max number of ahps types)

            ahps_jobs = job_number_huc * 5
            post_process_cat_fim_for_viz(
                catfim_method, output_catfim_dir, ahps_jobs, fim_version, FLOG.LOG_FILE_PATH
            )
        else:
            FLOG.lprint("post_process_cat_fim_for_viz step skipped")

    # FLOW-BASED
    else:
        FLOG.lprint("")
        FLOG.lprint('Start creating flow files using the ' + catfim_method + ' technique...')
        FLOG.lprint("")
        start = time.time()

        catfim_sites_file_path = os.path.join(output_mapping_dir, 'flow_based_catfim_sites.gpkg')
        # generate flows is only using one of the incoming job number params
        # so let's multiply -jh (huc) and -jn (inundate)
        job_flows = job_number_huc * job_number_inundate

        if step_num <= 1:
            generate_flows(
                output_catfim_dir,
                nwm_us_search,
                nwm_ds_search,
                lid_to_run,
                env_file,
                job_flows,
                is_stage_based,
                valid_ahps_hucs,
                nwm_metafile,
                FLOG.LOG_FILE_PATH,
            )
            end = time.time()
            elapsed_time = (end - start) / 60
            FLOG.lprint(f"Finished creating flow files in {str(elapsed_time).split('.')[0]} minutes \n")
        else:
            FLOG.lprint("Generate Flow step skipped")

        FLOG.lprint("")
        if step_num <= 2:
            # Generate CatFIM mapping (not used by stage)
            manage_catfim_mapping(
                fim_run_dir,
                output_flows_dir,
                output_catfim_dir,
                catfim_method,
                job_number_huc,
                job_number_inundate,
                FLOG.LOG_FILE_PATH,
            )
        else:
            FLOG.lprint("manage_catfim_mapping step skipped")
    # end if else

    FLOG.lprint("")
    if (
        step_num <= 3
    ):  # can later be changed to is_flow_based and step_num > 3, so stage can have it's own numbers
        # Updating mapping status
        FLOG.lprint('Updating mapping status...')
        update_flow_mapping_status(output_mapping_dir, catfim_sites_file_path)
        FLOG.lprint('Updating mapping status complete')
    else:
        FLOG.lprint("Updating mapping status step skipped")

    FLOG.lprint("================================")
    FLOG.lprint("End generate categorical fim")

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    FLOG.lprint(f"Ended (UTC): {dt_string}")

    # calculate duration
    time_duration = overall_end_time - overall_start_time
    FLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")

    return


def update_flow_mapping_status(output_mapping_dir, catfim_sites_file_path):
    '''
    Overview:
        - Gets a list of valid ahps that have at least one gkpg file. If we have at least one, then the site mapped something
        - We use that list compared to the original sites gpkg (or csv) file name to update the rows for the mapped column
          By this point, most shoudl have had status messages until something failed in inundation or creating the gpkg.
        - We also use a convention where if a status messsage starts with ---, then remove the ---. It is reserved for showing
          that some magnitudes exists and some failed.
    '''

    # Import geopackage output from flows creation
    if not os.path.exists(catfim_sites_file_path):
        FLOG.critical(
            f"Primary library gpkg of {catfim_sites_file_path} does not exist."
            " Check logs for possible errors. Program aborted."
        )
        sys.exit(1)

    sites_gdf = gpd.read_file(catfim_sites_file_path, engine='fiona')

    if len(sites_gdf) == 0:
        FLOG.critical(f"flows_gdf is empty. Path is {catfim_sites_file_path}. Program aborted.")
        sys.exit(1)

    # Yes.. embedded def
    def get_list_ahps_with_library_gpkgs():

        # as it is a set it will dig out unique files
        ahps_ids_with_gpkgs = []
        # gpkg_file_names =
        file_pattern = os.path.join(output_mapping_dir, "gpkg") + '/*_dissolved.gpkg'
        # print(file_pattern)
        for file_path in glob.glob(file_pattern):
            file_name = os.path.basename(file_path)
            file_name_segs = file_name.split("_")
            if len(file_name_segs) <= 1:
                continue
            ahps_id = file_name_segs[1]
            if len(ahps_id) == 5:  # yes, we assume the ahps in the second arg
                if ahps_id not in ahps_ids_with_gpkgs:
                    ahps_ids_with_gpkgs.append(ahps_id)

        return ahps_ids_with_gpkgs

    try:

        valid_ahps_ids = get_list_ahps_with_library_gpkgs()

        if len(valid_ahps_ids) == 0:
            FLOG.critical(f"No valid ahps gpkg files found in {output_mapping_dir}/gpkg")
            sys.exit(1)

        # we could have used lambda but the if/else logic got messy and unstable
        for ind, row in sites_gdf.iterrows():
            ahps_id = row['ahps_lid']
            status_val = row['status']
            if status_val == 'OK' or status_val.startswith("---") is True:

                # Note. It is possible for a status to start with --- but fail
                # later. So we wil temp change it to yes, and it might be changed
                # back to false.
                sites_gdf.at[ind, 'mapped'] = 'yes'

                if status_val.startswith("---"):
                    sites_gdf.at[ind, 'status'] = status_val[3:]
            else:
                sites_gdf.at[ind, 'mapped'] = 'no'

            # overrides the mapped flag (can't use the row object)
            if ahps_id not in valid_ahps_ids and sites_gdf.at[ind, 'mapped'] == "yes":
                sites_gdf.at[ind, 'mapped'] = 'no'

                # override any previous status message
                if status_val == "":
                    sites_gdf.at[ind, 'status'] = (
                        "An internal error has occurred while creating features for this site."
                    )
                FLOG.warning(f"mapped status was changed to no for {ahps_id}. Check error logs for it")

        # sites_gdf.reset_index(inplace=True, drop=True)

        sites_gdf.to_file(catfim_sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona")

        # csv flow file name
        nws_lid_csv_file_path = catfim_sites_file_path.replace(".gpkg", ".csv")

        # and we write a csv version at this time as well.
        # and this csv is good
        sites_gdf.to_csv(nws_lid_csv_file_path)

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
    df_restricted_sites,
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
            # all_messages.append(huc + msg)
            MP_LOG.warning(huc + msg)
            skip_lid_process = True

        if not os.path.exists(branch_dir):
            msg = ":branch directory missing"
            # all_messages.append(huc + msg)
            MP_LOG.warning(huc + msg)
            skip_lid_process = True

        categories = ['action', 'minor', 'moderate', 'major', 'record']

        if skip_lid_process is False:  # else skip to message processing
            usgs_elev_df = pd.read_csv(usgs_elev_table)

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
                "dtm_adj_ft": pd.Series(dtype='str'),
                "dadj_w_ft": pd.Series(dtype='float'),
                "dadj_w_m": pd.Series(dtype='float'),
                "lid_alt_ft": pd.Series(dtype='float'),
                "lid_alt_m": pd.Series(dtype='float'),
                "mapped": pd.Series(dtype='str'),
                "status": pd.Series(dtype='str'),
            }

            for lid in nws_lids:

                # if lid.upper() != 'GRNN1': Debug
                #     continue

                # TODO: Oct 2024, yes. this is goofy but temporary
                # Some lids will add a status message but are allowed to continue.
                # When we want to keep a lid processing but have a message, we add :three dashes
                #  ":---"" in front of the message which will be stripped off the front.
                # However, most other that pick up a status message are likely stopped
                # being processed. Later the status message will go through some tests
                # analyzing the status message as one factor to decide if the record
                # is or should be mapped.
                status_msg_allowing_continue = ""
                
                lid = lid.lower()  # Convert lid to lower case

                MP_LOG.lprint("-----------------------------------")
                huc_lid_id = f"{huc} : {lid}"
                MP_LOG.lprint(f"processing {huc_lid_id}")

                found_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]

                # print(found_restrict_lid)

                # Assume only one rec for now, fix later
                if len(found_restrict_lid) > 0:
                    reason = found_restrict_lid.iloc[
                        0, found_restrict_lid.columns.get_loc("restricted_reason")
                    ]
                    msg = ':' + reason
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue


                # Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
                thresholds, flows = get_thresholds(
                    threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
                )

                # MP_LOG.lprint(f"thresholds are {thresholds}")
                # MP_LOG.lprint(f"flows are {flows}")

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
                # valid_stage_value_list = []
                magnitude_stage_values_list = []
                valid_stages = []
                invalid_stages = []

                for stage in categories:  # yes.. same as a stage list
                    if stage in thresholds:
                        stage_val = thresholds[stage]
                        if stage_val is None or stage_val == "":
                            stage_val = -1
                    else:
                        stage_val = -1  # temp value to help it fall out as being invalid

                    is_valid_stage = stage_val != -1

                    if is_valid_stage is True:
                        valid_stages.append(stage)
                        magnitude_stage_values_list.append(stage_val)
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

                MP_LOG.trace(
                    f"stage values (pre-processed) in order are {action_stage}, {minor_stage}, {moderate_stage},"
                    f" {major_stage}, {record_stage} "
                )

                if len(invalid_stages) == 5:
                    msg = ':no valid threshold values are available'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Yes.. a bit weird, we are going to put three dashs in front of the message
                # to help show it is valid even with a missing stage msg.
                # any other record with a status value that is not "OK"
                # or does not start with a --- is assumed to be possibly bad (not mapped)
                missing_stages_msg = ""
                for ind, stage in enumerate(invalid_stages):
                    if ind == 0:
                        missing_stages_msg = f":---Missing stage data for {stage}"
                    else:
                        missing_stages_msg += f"; {stage}"

                # might be concat "" to "" but that is ok
                status_msg_allowing_continue += missing_stages_msg

                if status_msg_allowing_continue != "":
                    all_messages.append(lid + status_msg_allowing_continue)
                    MP_LOG.warning(huc_lid_id + status_msg_allowing_continue)
                # Won't be using the status_msg_allowing_continue value past here (for now)

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

                # Make mapping lid_directory.
                mapping_lid_directory = os.path.join(mapping_huc_directory, lid)
                if not os.path.exists(mapping_lid_directory):
                    os.mkdir(mapping_lid_directory)

                # At this point we have at least one valid stage/category
                # cyle through on the stages that are valid
                for category in valid_stages:  # a category is the same thing as a stage at this point.
                    # MP_LOG.lprint(f"{huc_lid_id}: Magnitude is {category}")
                    # Pull stage value and confirm it's valid, then process
                    stage = thresholds[category]
                    MP_LOG.trace(f"About to create tifs for {huc_lid_id} : {category} : {stage}")

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
                        }
                    )

                # So, we might have an MP inside an MP
                # let's merge what we have at this point, before we go into another MP
                MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix, True)

                # If we only have a record stage value and none of the other 4, then there are no
                # intervals as we never add 5 intervals to record.
                skip_add_intervals = False
                if action_stage == -1 and minor_stage == -1 and moderate_stage == -1 and major_stage == -1:
                    skip_add_intervals = True
                    MP_LOG.lprint(f"{huc_lid_id}: Skipping intervals as it only has just a record stage")

                if skip_add_intervals is False:
                    # we do not want to include the record stage value for intervals

                    MP_LOG.trace(f"magnitude_stage_values_list is {magnitude_stage_values_list}")

                    # Max can go past the record stage value if it exists (for now)
                    max_interval_val = max(magnitude_stage_values_list) + past_major_interval_cap

                    # round up
                    max_interval_val = math.ceil(max_interval_val)
                    min_interval_val = math.ceil(min(magnitude_stage_values_list))

                    # these are now whole numbers
                    interval_list = np.arange(min_interval_val, max_interval_val, 1.0)
                    # Go an extra 5 ft beyond the max stage, arbitrary (but not past the record stage)

                    MP_LOG.trace(f"{huc_lid_id}:Potential interval list is {interval_list}")

                    # In order not to create duplicate features with the same stage value,
                    # we keep track of the stage values that have been inundated and skip them
                    # if a dup is found. Start with a copy of the list that was created above
                    # which will always have at least one record
                    claimed_interval_stages = copy.copy(magnitude_stage_values_list)

                    # Now we will do another set of inundations, but this one is based on
                    # not the stage flow but flow based on each interval
                    tif_child_log_file_prefix = MP_LOG.MP_calc_prefix_name(
                        parent_log_output_file, "MP_prod_sb_tifs"
                    )

                    MP_LOG.trace(
                        f"stage values (pre-interval processing) in order are {action_stage}, {minor_stage}, {moderate_stage},"
                        f" {major_stage}, {record_stage}"
                    )

                    # Now we add the interval tifs but no interval tifs for the "record" stage if there is one.
                    with ProcessPoolExecutor(max_workers=job_number_intervals) as executor:
                        try:
                            # In theory, there will always be at least one interval
                            # we need to skip the stages where their value is -1 as they
                            # did not have a stage value from nwps and need to be discluded.

                            for interval_stage in interval_list:

                                # That value has already been inundated, likely but the original stage record
                                if interval_stage in claimed_interval_stages:
                                    continue
                                else:
                                    claimed_interval_stages.append(interval_stage)

                                # MP_LOG.trace(f"interval_stage value is {lid} - {interval_stage}")

                                # Determine category the stage value belongs with.
                                # we are filling wholes at 1 ft intervals between the 4 magnitudes
                                # ie) action at 5.2 ft, action interval at 6 ft nad 7 ft
                                # moderate at 7.4, moderate interval at 8, 9, 10 ft
                                # major at 10.7, then always 5 intervals past major
                                #   if major exists, if not... them 5 past moderate, etc.
                                # In theory, we can have no intervals if we only have just a record value
                                if (
                                    action_stage != -1
                                    and action_stage <= interval_stage
                                    and interval_stage < minor_stage
                                ):
                                    adj_category_name = 'action_' + str(interval_stage) + 'ft'
                                elif (
                                    minor_stage != -1
                                    and minor_stage <= interval_stage
                                    and interval_stage < moderate_stage
                                ):
                                    adj_category_name = 'minor_' + str(interval_stage) + 'ft'
                                elif (
                                    moderate_stage != -1
                                    and moderate_stage <= interval_stage
                                    and interval_stage < major_stage
                                ):
                                    adj_category_name = 'moderate_' + str(interval_stage) + 'ft'
                                elif major_stage != -1 and interval_stage >= major_stage:
                                    adj_category_name = 'major_' + str(interval_stage) + 'ft'

                                # We don't add intervals to "record", but major intervals (up to 5 of them).
                                # If no major.. then up to 5 intervals past moderate and so forth down
                                # Note: Interval can exceed the record value.
                                # all 1 ft intervals are fill in between each stage to max plus 5 more.
                                else:
                                    continue

                                MP_LOG.trace(f"adj_category_name is {adj_category_name}")

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
                                    adj_category_name,
                                    job_number_inundate,
                                    parent_log_output_file,
                                    tif_child_log_file_prefix,
                                )
                        except TypeError:  # sometimes the thresholds are Nonetypes
                            MP_LOG.error(
                                f"{huc_lid_id}: ERROR: type error in ProcessPool,"
                                " likely in the interval code"
                            )
                            MP_LOG.error(traceback.format_exc())
                            continue

                        except Exception:
                            MP_LOG.critical(f"{huc_lid_id}: ERROR: ProcessPool has an error")
                            MP_LOG.critical(traceback.format_exc())
                            # merge MP Logs (Yes)
                            MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix, True)
                            sys.exit(1)

                    # merge MP Logs (merging MP into an MP (proc_pool in a proc_pool))
                    MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix, True)

                # end of skip_add_intervals is False

                # Create a csv with same information as geopackage but with each threshold as new record.
                # Probably a less verbose way.
                csv_df = pd.DataFrame(df_cols)  # for first appending

                # for threshold in magnitudes:
                # TODO: Sept 2024: Should this be categories or valid_stage_list. Likely categories
                # as we want all five stages.
                # stage = category = threshold (renaming will be looked at later)
                # Missing_stages_msg might be empty but we still want the record to continue
                # even if there are one or more missing_stages

                # for threshold in categories:
                for threshold in valid_stages:
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
                                'status': 'OK',
                            }
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
                    if missing_stages_msg == "":
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


def load_restricted_sites():
    """
    At this point, only stage based uses this. But a arg of "catfim_type (stage or flow) or something
    can be added later.

    Returns: a dataframe for the restricted lid and the reason why:
        "nws_lid", "restricted_reason"
    """

    file_name = "stage_based_ahps_restricted_sites.csv"
    current_script_folder = os.path.dirname(__file__)
    file_path = os.path.join(current_script_folder, file_name)

    df_restricted_sites = pd.read_csv(file_path, dtype=str)

    df_restricted_sites['nws_lid'].fillna("", inplace=True)
    df_restricted_sites['restricted_reason'].fillna("", inplace=True)

    # Need to drop the comment lines before doing any more processing
    df_restricted_sites.drop(
        df_restricted_sites[df_restricted_sites.nws_lid.str.startswith("#")].index, inplace=True
    )

    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.upper()

    # There are enough conditions and a low number of rows that it is easier to
    # test / change them via a for loop
    indexs_for_recs_to_be_removed_from_list = []
    for ind, row in df_restricted_sites.iterrows():
        nws_lid = row['nws_lid']
        restricted_reason = row['restricted_reason']

        if len(nws_lid) != 5:  # could be just a blank row in the
            FLOG.warning(
                f"From the ahps_restricted_sites, an invalid nws_lid value of '{nws_lid}'"
                " and has dropped from processing"
            )
            indexs_for_recs_to_be_removed_from_list.append(ind)
            continue

        if restricted_reason == "":
            restricted_reason = "From the ahps_restricted_sites,"
            " the site will not be mapped, but a reason has not be provided."
            df_restricted_sites.at[ind, 'restricted_reason'] = restricted_reason
            FLOG.warning(f"{restricted_reason}. Lid is '{nws_lid}'")
        continue
    # end for

    # Invalid records (not dropping, just completely invalid recs from the csv)
    # Could be just blank rows from the csv
    if len(indexs_for_recs_to_be_removed_from_list) > 0:
        df_restricted_sites = df_restricted_sites.drop(indexs_for_recs_to_be_removed_from_list).reset_index()

    # print(df_restricted_sites.head(10))

    return df_restricted_sites


def __adjust_datum_ft(flows, metadata, lid, huc_lid_id):

    # TODO: Aug 2024: This whole parts needs revisiting. Lots of lid data has changed and this
    # is all likely very old.

    # Jul 2024: For now, we will duplicate messages via all_messsages and via the logging system.
    all_messages = []

    datum_adj_ft = None
    ### --- Do Datum Offset --- ###
    # determine source of interpolated threshold flows, this will be the rating curve that will be used.
    rating_curve_source = flows.get('source')

    MP_LOG.trace(f"{huc_lid_id} : rating_curve_source is {rating_curve_source}")

    if rating_curve_source is None:
        msg = ':No source for rating curve'
        all_messages.append(lid + msg)
        MP_LOG.warning(huc_lid_id + msg)
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
        msg = ':datum info unavailable'
        all_messages.append(lid + msg)
        MP_LOG.warning(huc_lid_id + msg)
        return None, all_messages

    # ___________________________________________________________________________________________________#
    # NOTE: !!!!
    # When appending to a all_message and we may not automatcially want the record dropped
    # then add "---" in front of the message. Whenever the code finds a message that does not
    # start with a ---, it assumes if it is a fail and drops it. We will make a better system later.

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
                msg = ':NOAA VDatum adjustment error, CRS is missing'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
            if 'HTTPSConnectionPool' in ex:
                time.sleep(10)  # Maybe the API needs a break, so wait 10 seconds
                try:
                    datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data, region='contiguous')
                except Exception:
                    msg = ':NOAA VDatum adjustment error, possible API issue'
                    all_messages.append(lid + msg)
                    MP_LOG.error(huc_lid_id + msg)
            if 'Invalid projection' in ex:
                msg = f':NOAA VDatum adjustment error, invalid projection: crs={crs}'
                all_messages.append(lid + msg)
                MP_LOG.error(huc_lid_id + msg)
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
    df_restricted_sites,
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

    # Generate flows is only using one of the incoming job number params
    # so let's multiply -jh (huc) and -jn (inundate)
    job_flows = job_number_huc * job_number_inundate
    if job_flows > 90:
        job_flows == 90

    # If stage based, generate flows, mostly returns values sent in with a few changes
    # stage based doesn't really need generated flow data
    # But for flow based, it really does use it to generate flows.
    #
    (huc_dictionary, out_gdf, ___, threshold_url, all_lists, nwm_flows_df, nwm_flows_alaska_df) = (
        generate_flows(
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
    )

    child_log_file_prefix = FLOG.MP_calc_prefix_name(FLOG.LOG_FILE_PATH, "MP_iter_hucs")

    FLOG.lprint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    FLOG.lprint("Start processing HUCs for Stage-Based CatFIM")
    num_hucs = len(lst_hucs)
    huc_index = 0
    FLOG.lprint(f"Number of hucs to process is {num_hucs}")

    print(f"job_number_huc is {job_number_huc}")

    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        try:
            for huc in huc_dictionary:
                if huc in lst_hucs:
                    # FLOG.lprint(f'Generating stage based catfim for : {huc}')

                    # # Code variation for DROPPING Alaska HUCs
                    # nwm_flows_region_df = all_nwm_flows_df

                    # Code variation for keeping alaska HUCs
                    nwm_flows_region_df = nwm_flows_alaska_df if str(huc[:2]) == '19' else nwm_flows_df

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
                        df_restricted_sites,
                        str(FLOG.LOG_FILE_PATH),
                        child_log_file_prefix,
                        progress_stmt,
                    )
                    huc_index += 1

        except Exception:
            FLOG.critical("ERROR: ProcessPool has an error")
            FLOG.critical(traceback.format_exc())
            sys.exit(1)

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
    lids_df['mapped'] = 'no'  # will be adjsuted later

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
        viz_out_gdf['status'] = viz_out_gdf['status'].fillna('OK')

        # Add acceptance criteria to viz_out_gdf before writing
        viz_out_gdf['acceptable_coord_acc_code_list'] = str(acceptable_coord_acc_code_list)
        viz_out_gdf['acceptable_coord_method_code_list'] = str(acceptable_coord_method_code_list)
        viz_out_gdf['acceptable_alt_acc_thresh'] = float(acceptable_alt_acc_thresh)
        viz_out_gdf['acceptable_alt_meth_code_list'] = str(acceptable_alt_meth_code_list)
        viz_out_gdf['acceptable_site_type_list'] = str(acceptable_site_type_list)

        # Rename the stage_based_catfim db column from nws_lid to ahps_lid to be
        # consistant with all other CatFIM outputs
        viz_out_gdf.rename(columns={"nws_lid": "ahps_lid"}, inplace=True)

        viz_out_gdf.to_file(nws_lid_gpkg_file_path, driver='GPKG', index=True, engine='fiona')

        csv_file_path = nws_lid_gpkg_file_path.replace(".gpkg", ".csv")
        viz_out_gdf.to_csv(csv_file_path)
    else:
        FLOG.lprint(f"nws_sites_layer ({nws_lid_gpkg_file_path}) : has no messages")


def set_start_files_folders(
    step_num, output_catfim_dir, output_mapping_dir, output_flows_dir, attributes_dir, overwrite
):

    # ================================
    # Folder cleaning based on step system
    if step_num == 0:
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
            shutil.rmtree(output_flows_dir, ignore_errors=True)
            shutil.rmtree(attributes_dir, ignore_errors=True)
            shutil.rmtree(output_mapping_dir, ignore_errors=True)

    os.makedirs(output_flows_dir, exist_ok=True)
    os.makedirs(output_mapping_dir, exist_ok=True)
    os.makedirs(attributes_dir, exist_ok=True)

    # Always keeps the logs folder
    log_dir = os.path.join(output_catfim_dir, "logs")
    log_output_file = FLOG.calc_log_name_and_path(log_dir, "catfim")
    FLOG.setup(log_output_file)


if __name__ == '__main__':

    '''
    Sample
    python /foss_fim/tools/generate_categorical_fim.py -f /outputs/Rob_catfim_test_1 -jh 1 -jn 10 -ji 8
    -e /data/config/catfim.env -t /data/catfim/rob_test/docker_test_1
    -me '/data/catfim/rob_test/nwm_metafile.pkl' -sb -step 2
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
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.',
        required=False,
        default='5',
    )

    ## Deprecated, use lst_hucs instead
    # TODO: lid_to_run functionality... remove? for now, just hard code lid_to_run as "all"
    # parser.add_argument(
    #     '-l',
    #     '--lid_to_run',
    #     help='OPTIONAL: NWS LID, lowercase, to produce CatFIM for. Currently only accepts one. Defaults to all sites',
    #     required=False,
    #     default='all',
    # )

    # NOTE: The HUCs you put in this, MUST be a HUC that is valid in your -f/ --fim_run_dir (HAND output folder)
    parser.add_argument(
        '-lh',
        '--lst_hucs',
        help='OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs',
        required=False,
        default='all',
    )

    parser.add_argument(
        '-mc',
        '--past_major_interval_cap',
        help='OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs?'
        ' of the machine. Defaults to 5.',
        required=False,
        default=5.0,
        type=float,
    )

    parser.add_argument(
        '-step',
        '--step-num',
        help='OPTIONAL: By adding a number here, you may be able to skip levels of processing. The number'
        ' you submit means it will start at that step. e.g. step of 2 means start at step 2 which for flow'
        ' based is the creating of tifs and gpkgs. Note: This assumes'
        ' those previous steps have already been processed and the files are present.'
        ' Defaults to 0 which means all steps processed.',
        required=False,
        default=0,
        type=int,
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

    parser.add_argument(
        '-o', '--overwrite', help='OPTIONAL: Overwrite files', required=False, action="store_true"
    )

    args = vars(parser.parse_args())

    try:

        # call main program
        process_generate_categorical_fim(**args)

    except Exception:
        FLOG.critical(traceback.format_exc())