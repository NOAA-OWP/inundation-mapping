#!/usr/bin/env python3

import argparse
import glob
import math
import os
import pickle
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
    produce_stage_based_lid_tifs,
)
from tools_shared_functions import (
    aggregate_wbd_hucs,
    filter_nwm_segments_by_stream_order,
    get_datum,
    get_nwm_segs,
    ngvd_to_navd_ft,
    correct_datum_typos,
)
from tools_shared_variables import (
    acceptable_alt_acc_thresh,
    acceptable_alt_meth_code_list,
    acceptable_coord_acc_code_list,
    acceptable_coord_method_code_list,
    acceptable_site_type_list,
)

import utils.fim_logger as fl
from data.wrds.download_process_wrds import (
    check_metadata_CRS_availability,
    download_all_thresholds,
    load_nwm_metadata,
    load_site_thresholds,
)
from utils.shared_variables import VIZ_PROJECTION


# global RLOG
FLOG = fl.FIM_logger()  # the non mp version
MP_LOG = fl.FIM_logger()  # the Multi Proc version

gpd.options.io_engine = "pyogrio"


"""
Jun 2024
This system is continuing to mature over time. It has a number of optimizations that can still
be applied in areas such as logic, performance and error handling.

In the interium there is still a consider amount of debug lines and tools embedded in that can
be commented on/off as required.

Aug 2024
This script was upgraded significantly with lots of misc TODO's embedded.
Lots of inline documenation needs updating as well.

Oct 2025
Doc strings and improved documentation was added.


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
    job_number_intervals,
    is_stage_based,
    output_folder,
    search,
    lst_hucs,
    past_major_interval_cap,
    step_num,
    nwm_meta_file,
    get_new_meta_data,
    threshold_file,
    get_new_threshold_data,
    catfim_version,
    model_version,
    overwrite,
):
    '''
    Orchestrates the generation of CatFIM products for a set of Hydrologic Unit Codes (HUCs),
    supporting both stage-based and flow-based methodologies. Handles validation, setup, filtering, and multi-step processing
    including flow generation, mapping, post-processing, and status updates.

    Parameters
    ----------
    fim_run_dir : str
        Path to the FIM run directory containing HUC folders and input files.
    env_file : str
        Path to the .env file containing API and environment configuration.
    job_number_huc : int
        Number of parallel jobs to use for HUC-level processing.
    job_number_inundate : int
        Number of parallel jobs to use for inundation-level processing.
    is_stage_based : bool
        If True, runs stage-based CatFIM workflow; otherwise, runs flow-based workflow.
    output_folder : str
        Base output folder for CatFIM results.
    overwrite : bool
        If True, allows overwriting existing output files and folders.
    search : int or float
        Upstream and downstream search distance in miles for site selection.
    lst_hucs : str
        Space-separated list of HUCs to process, or 'all' to process all available HUCs.
    catfim_version : str
        CatFIM version string (e.g., '1.0').
    model_version : str
        HAND model version string (e.g., '2.1.5.2').
    job_number_intervals : int
        Number of parallel jobs for interval-based processing.
    past_major_interval_cap : int
        Cap for major interval processing (used in stage-based workflow).
    step_num : int
        Step number to start processing from (optional, allows skipping earlier steps).
    nwm_meta_file : str
        Path to the NWM metadata pickle file (optional, defaults to "" if not included).
    threshold_file : str
        Path to the threshold pickle file for manual input thresholds (optional, defaults to "" if not included).

    Raises
    ------
    Exception
        If required files or directories are missing or invalid.
    ValueError
        If input parameters are inconsistent or result in zero valid HUCs.

    Returns
    -------
    None
        Results are written to output directories and files; function does not return a value.

    Workflow Steps
    -------------
    1. Validation and setup of input directories, files, and parameters.
    2. Filtering and selection of valid HUCs based on input and threshold files.
    3. Stage-based or flow-based CatFIM processing, including flow generation, mapping, and post-processing.
    4. Compilation of threshold data and cleanup of intermediate files.
    5. Updating mapping status for processed sites.
    6. Logging of progress, warnings, and summary information.

    Notes
    -----
    - Supports skipping steps via the `step_num` parameter.
    - Handles both manual and automated threshold input via `threshold_file`.
    - Uses environment variables for API access and configuration.
    - Designed for parallel processing and scalable workflows.

    Step System
    -----------
    This system allows us to to skip steps.
    Steps that are skipped are assumed to have the valid files that are needed
    When a number is submitted, ie) 2, it means skip steps 1 and start at 2

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

    local_vals = locals()

    output_flows_dir = os.path.join(output_catfim_dir, 'flows')
    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')

    # ================================
    set_start_files_folders(
        step_num, output_catfim_dir, output_mapping_dir, output_flows_dir, attributes_dir, overwrite
    )

    FLOG.trace("locals...")
    FLOG.trace(local_vals)

    # For API usage
    load_dotenv(env_file)
    API_BASE_URL = os.getenv('API_BASE_URL')
    if API_BASE_URL is None:
        raise ValueError(
            'API base url not found. '
            'Ensure inundation_mapping/tools/ has an .env file with the following info: '
            'API_BASE_URL, WBD_LAYER, NWM_FLOWS_MS, '
            'USGS_METADATA_URL, USGS_DOWNLOAD_URL'
        )

    # ================================

    # Set metadata/threshold filepaths if they aren't provided
    if nwm_meta_file == "":
        nwm_meta_file = os.path.join(output_catfim_dir, 'nwm_metadata.pkl')  # TODO: Update default paths
    if threshold_file == "":
        threshold_file = os.path.join(output_catfim_dir, 'thresholds.pkl')  # TODO: Update default paths

    # Error if the files are not found and we are not getting new data
    if get_new_meta_data == False and os.path.exists(nwm_meta_file) == False:
        raise Exception(
            f"The nwm_metadata file can not be found at {nwm_meta_file}. Please fix pathing or use the get metadata flag."
        )

    if get_new_threshold_data == False and threshold_file != "" and os.path.exists(threshold_file) == False:
        raise Exception(
            f"The threshold input file can not be found at {threshold_file}. Please fix pathing or use the get threshold flag."
        )

    #         if os.path.exists(nwm_meta_file) == False:
    #             raise Exception("The nwm_metadata (-me) file can not be found. Please remove or fix pathing.")
    #         file_ext = os.path.splitext(nwm_meta_file)
    #         if file_ext.count == 0:
    #             raise Exception("The nwm_metadata (-me) file appears to be invalid. It is missing an extension.")
    #         if file_ext[1].lower() != ".pkl":
    #             raise Exception("The nwm_metadata (-me) file appears to be invalid. The extention is not pkl.")

    # else:
    #     nwm_meta_file = os.path.join(output_catfim_dir, 'nwm_metadata.pkl')

    # if threshold_file != "":
    #     if os.path.exists(threshold_file) == False:
    #         raise Exception("The threshold input file can not be found. Please remove or fix pathing.")
    #     file_ext = os.path.splitext(threshold_file)
    #     if file_ext.count == 0:
    #         raise Exception("The threshold input file appears to be invalid. It is missing an extension.")
    #     if file_ext[1].lower() != ".pkl":
    #         raise Exception("The threshold input file appears to be invalid. The extention is not pkl.")

    # ================================
    # Define default arguments. Modify these if necessary

    if model_version != "":
        model_version = "HAND " + model_version
        model_version = model_version.replace(".", "_")
    if catfim_version != "":
        catfim_version = "CatFIM " + catfim_version
        catfim_version = catfim_version.replace(".", "_")

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

    # Set default data source to WRDS
    data_source = 'WRDS'

    # ================================

    ## ===== START SECTION OF CODE COPIED FROM download_process_wrds.py =====

    # Load NWM metadata (either by downloading it or pulling it from WRDS)
    # Note: This is the function that we will put into CatFIM code
    output_meta_list, messages = load_nwm_metadata(nwm_meta_file, API_BASE_URL, search, get_new_meta_data)

    for message in messages:
        FLOG.lprint(message)

    # Get the HUC dictionary
    wbd_file = '/data/inputs/wbd/WBD_National.gpkg'  # TODO: Replace with os.getenv("input_wbd_layer")?
    huc_lid_dict, nwm_sites_all_gdf = aggregate_wbd_hucs(output_meta_list, wbd_file, retain_attributes=True)

    # Filter huc_lid_dict to only include HUCs in huc_lst
    if 'all' not in lst_hucs:
        # huc_lid_dict = {lid: huc for lid, huc in huc_lid_dict.items() if huc in lst_hucs}

        keep = set(lst_hucs)
        for huc in list(huc_lid_dict):
            if huc not in keep:
                del huc_lid_dict[huc]

    FLOG.lprint(f"Number of sites to download thresholds for: {len(huc_lid_dict)}")  # TEMP DEBUG

    if len(huc_lid_dict) == 0:
        raise Exception("The metadata pickle file does not have any applicable HUCs")

    if not huc_lid_dict:
        sys.exit('Error occurred in metadata download.')

    # Get a dictionary of which sources have valid CRS's for each site
    lid_source_dict = check_metadata_CRS_availability(output_meta_list)

    # Load thresholds if specified
    if get_new_threshold_data == True:
        threshold_url = f'{API_BASE_URL}/nws_threshold'

        # Download thresholds
        messages = download_all_thresholds(threshold_file, threshold_url, huc_lid_dict, lid_source_dict)

        for message in messages:
            FLOG.lprint(message)

    ## ===== END SECTION OF CODE COPIED FROM download_process_wrds.py =====

    # Get the source (important for differentiating processing for manual input vs wrds)
    with open(threshold_file, "rb") as p_handle:
        thresh_list = pickle.load(p_handle)
        source_list = list(thresh_list['source'])

        # If manual input is in source list, set data source to manual input
        # Assumes that if one is manual input, then all are manual input
        if 'Manual_Input' in source_list:
            print("Manual input found in threshold source list.")
            data_source = 'Manual_Input'

        # Otherwise, compile unique sources into a comma-separated string
        else:
            data_source = set(thresh_list['source'])

            # TODO: Nov 2025: Fix this. The data source line below with the join has a bug.
            # When the source comes in with a slash at the front, we get:
            #     TypeError: sequence item 0: expected str instance, NoneType found

            # When the source comes in without a slash at the front, we get:
            #     TypeError: sequence item 0: expected str instance, float found
            # data_source = ', '.join(data_source)

            # temp workaround
            data_source = 'WRDS'

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

    # # Print number of available hucs in threshold_file
    # if threshold_file != "":
    #     FLOG.lprint(f'Threshold file has data for {len(threshold_hucs)} HUC(s)')

    # FLOG.lprint(f'Data source: {data_source}')  # TEMP DEBUG

    # Check that fim_inputs.csv exists and raise error if necessary
    fim_inputs_csv_path = os.path.join(fim_run_dir, 'fim_inputs.csv')
    if not os.path.exists(fim_inputs_csv_path):
        raise ValueError(f"{fim_inputs_csv_path} not found. Verify that you have the correct input files.")

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

    # STAGE-BASED
    if is_stage_based:
        # Generate Stage-Based CatFIM mapping
        # does flows and inundation (mapping)

        catfim_sites_file_path = os.path.join(output_mapping_dir, 'stage_based_catfim_sites.gpkg')

        if step_num <= 1:

            df_restricted_sites = load_restricted_sites(is_stage_based)

            generate_stage_based_categorical_fim(
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
                nwm_meta_file,
                df_restricted_sites,
                threshold_file,
                data_source,
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
                catfim_method, output_catfim_dir, ahps_jobs, catfim_version, model_version, FLOG.LOG_FILE_PATH
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

            df_restricted_sites = load_restricted_sites(is_stage_based)

            generate_flows(
                output_catfim_dir,
                nwm_us_search,
                nwm_ds_search,
                env_file,
                job_flows,
                is_stage_based,
                valid_ahps_hucs,
                nwm_meta_file,
                FLOG.LOG_FILE_PATH,
                df_restricted_sites,
                threshold_file,
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
                catfim_version,
                model_version,
                job_number_huc,
                job_number_inundate,
                FLOG.LOG_FILE_PATH,
            )
        else:
            FLOG.lprint("manage_catfim_mapping step skipped")
    # end if else

    FLOG.lprint("")

    # This is done for SB and FB
    if (
        step_num <= 3
    ):  # can later be changed to is_flow_based and step_num > 3, so stage can have it's own numbers
        # Updating mapping status
        FLOG.lprint('Updating mapping status...')
        update_sites_mapping_status(output_mapping_dir, catfim_sites_file_path, catfim_version, model_version)
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


def get_list_ahps_with_library_gpkgs(output_mapping_dir):
    '''
    Used in both stage- and flow-based CatFIM.

    Scans the specified output mapping directory for GeoPackage (.gpkg) files within the 'gpkg' subdirectory,
    extracts unique AHPS IDs from the filenames, and returns a list of these IDs.

    The function assumes that the AHPS ID is the second segment in the filename when split by underscores.
    Only files with at least two underscore-separated segments in their names are considered.

    Args:
        output_mapping_dir (str): Path to the directory containing the 'gpkg' subdirectory with .gpkg files.

    Returns:
        ahps_ids_with_gpkgs (list): A list of unique AHPS IDs (as strings) extracted from the .gpkg filenames.

    Used to check whether AHPS LID is 5 characters, but no longer does (as of Aug '25)
    because LID lengths above 5 characters are probably invalid but we are not checking that here.
    '''

    ahps_ids_with_gpkgs = []
    file_pattern = os.path.join(output_mapping_dir, "gpkg") + '/*.gpkg'

    for file_path in glob.glob(file_pattern):
        file_name = os.path.basename(file_path)
        file_name_segs = file_name.split("_")
        if len(file_name_segs) <= 1:
            continue
        ahps_id = file_name_segs[1]

        if ahps_id not in ahps_ids_with_gpkgs:
            ahps_ids_with_gpkgs.append(ahps_id)

    return ahps_ids_with_gpkgs


def update_sites_mapping_status(output_mapping_dir, catfim_sites_file_path, catfim_version, model_version):
    '''
    Used in both stage- and flow-based CatFIM.

    Updates the mapping status and status messages for CatFIM sites based on the presence of valid inundation GeoPackage files.

    This function reads a GeoPackage or CSV file containing CatFIM site information, checks which sites have valid inundation
    mapping outputs, and updates the 'mapped' and 'status' columns accordingly. Gets a list of valid ahps that have at least
    one gkpg file. If we have at least one, then the site mapped something.

    It also adds 'model_version' and 'product_version' columns, and saves the updated data back to the original file and as a CSV.

    By this point, most should have had status messages until something failed in inundation or creating the gpkg.

    Args:
        output_mapping_dir (str): Directory containing the output mapping files, including inundation GeoPackages.
        catfim_sites_file_path (str): Path to the CatFIM sites GeoPackage or CSV file to be updated.
        catfim_version (str): The product version string to be recorded in the output.
        model_version (str): The model version string to be recorded in the output.

    Raises:
        SystemExit: If the input sites file does not exist, is empty, or no valid inundation files are found.

    Side Effects:
        - Updates the 'mapped' and 'status' columns in the input sites file.
        - Adds 'model_version' and 'product_version' columns.
        - Saves the updated file in both GeoPackage and CSV formats.
        - Logs critical errors and warnings using FLOG.

    Notes:
        - Sites without valid inundation files are marked as 'mapped' = 'no' and their status is updated.
        - Sites with valid inundation files are marked as 'mapped' = 'yes' and their status is set to 'Good' if empty.
        - If a status message starts with "---", it is removed to indicate a warning rather than an error.
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

    try:
        valid_ahps_ids = get_list_ahps_with_library_gpkgs(output_mapping_dir)
        if len(valid_ahps_ids) == 0:
            FLOG.critical(f"No valid ahps gpkg files found in {output_mapping_dir}/gpkg")
            sys.exit(1)

        # we could have used lambda but the if/else logic got messy and unstable
        for ind, row in sites_gdf.iterrows():
            ahps_id = row['ahps_lid']
            status_val = row['status']
            # If the ahps_id is not in the valid list, then mapped should be "no" and status updated
            if ahps_id not in valid_ahps_ids:
                sites_gdf.at[ind, 'mapped'] = 'no'
                FLOG.lprint(f"{ahps_id} : Mapped status was changed to no because no inundation GPKGs found.")
                if status_val is None or status_val == "" or status_val == "Good":
                    sites_gdf.at[ind, 'status'] = 'Site resulted with no valid inundated files'
                else:
                    if status_val.startswith("---") == True:
                        status_val = status_val[3:]  # remove the "---" from the status
                    sites_gdf.at[ind, 'status'] = status_val
                continue
                # It is safe to assume a status message for invalid ones already exist

            sites_gdf.at[ind, 'mapped'] = 'yes'
            # Mapped should be "yes", and "Good",
            if status_val is None or status_val == "":
                sites_gdf.at[ind, 'status'] = 'Good'
            elif status_val.startswith("---") == True:  # warning not an error
                sites_gdf.at[ind, 'mapped'] = 'yes'
                # remove the "---" from the start, as it is a warning, and not an error
                status_val = status_val[3:]
                sites_gdf.at[ind, 'status'] = status_val

        # sites_gdf.reset_index(inplace=True, drop=True)

        sites_gdf["model_version"] = model_version
        sites_gdf["product_version"] = catfim_version

        # We are re-saving the sites files
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
    threshold_file,
    data_source,
):
    '''
    Processes a single HUC to generate stage-based CatFIM.

    The function iterates through all NWS LIDs (locations) within the HUC, performing data validation,
    threshold extraction, elevation adjustment, and mapping for each flood category and interval.
    It handles logging, error reporting, and output file generation for each site.

    Does flow files and mapping in the same function by HUC.

    Parameters
    ----------
    output_catfim_dir : str
        Directory where CatFIM outputs will be saved.
    huc : str
        Hydrologic Unit Code to process.
    fim_dir : str
        Directory containing FIM input data for the HUC.
    huc_dictionary : dict
        Dictionary mapping HUCs to lists of NWS LIDs.
    threshold_url : str
        URL for WRDS API to fetch flood stage thresholds.
    all_lists : list
        List of metadata dictionaries for all sites.
    past_major_interval_cap : int or float
        Maximum interval value for stages past 'major' category.
    job_number_inundate : int
        Number of parallel jobs for inundation mapping.
    job_number_intervals : int
        Number of parallel jobs for interval mapping.
    nwm_flows_region_df : pandas.DataFrame
        DataFrame containing NWM flow data for the region.
    df_restricted_sites : pandas.DataFrame
        DataFrame listing restricted NWS LIDs and reasons.
    parent_log_output_file : str
        Path to the parent log file for multiprocessing logging.
    child_log_file_prefix : str
        Prefix for child log files in multiprocessing.
    progress_stmt : str
        Statement describing current progress for logging.
    threshold_file : str
        Path to local threshold file (if not using WRDS API).
    data_source : str
        Source of input data ('Manual_Input' or other).

    Returns
    -------
    None

    Side Effects
    ------------
    - Creates output directories and files for mapping and attributes.
    - Writes log messages and error reports to log files.
    - Generates stage-based and interval-based inundation TIFFs.
    - Exports site attribute CSV files for each processed LID.
    - Writes status and error messages to a HUC-specific messages file.

    Exceptions
    ----------
    - Handles and logs exceptions during processing, continuing to next site or exiting on critical errors.

    Notes
    -----
    - This function is designed to be multiprocessing-safe and may be called within a multiprocessing context.
    - Extensive logging is performed for debugging and status tracking.
    - Sites with missing or invalid data are skipped, and reasons are logged.
    '''

    try:
        # This is setting up logging for this function to go up to the parent
        # child_log_file_prefix is likely MP_iter_hucs
        MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)
        MP_LOG.lprint("**********************")
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
        huc_nws_lids = huc_dictionary[huc]

        nws_lids = []
        # sometimes we are getting duplicates but no idea how/why
        [nws_lids.append(val) for val in huc_nws_lids if val not in nws_lids]

        MP_LOG.lprint(f"Lids to process for {huc} are {nws_lids}")

        skip_lid_process = False
        # -- If necessary files exist, continue -- #
        # Yes, each lid gets a record no matter what, so we need some of these messages duplicated
        # per lid record

        if data_source != 'Manual_Input' and not os.path.exists(usgs_elev_table):
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

        if skip_lid_process == False:  # else skip to message processing
            if data_source != 'Manual_Input':  # Manual input data does not need usgs_elev_table
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
            }

            for lid in nws_lids:

                # # Debugging mode:
                # if lid.upper() not in ['PACI1']:
                #    continue

                # TODO: Oct 2024, yes. this is goofy but temporary
                # Some lids will add a status message but are allowed to continue.
                # When we want to keep a lid processing but have a message, we add :three dashes
                #  ":---"" in front of the message which will be stripped off the front.
                # However, most other that pick up a status message are likely stopped
                # being processed. Later the status message will go through some tests
                # analyzing the status message as one factor to decide if the record
                # is or should be mapped.

                lid = lid.lower()  # Convert lid to lower case

                MP_LOG.lprint("-----------------------------------")
                huc_lid_id = f"{huc} : {lid}"
                MP_LOG.lprint(f"processing {huc_lid_id}")

                found_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]

                # Assume only one rec for now, fix later
                if len(found_restrict_lid) > 0:
                    reason = found_restrict_lid.iloc[
                        0, found_restrict_lid.columns.get_loc("restricted_reason")
                    ]
                    msg = ':Restricted Site - ' + reason
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                if len(lid) != 5:
                    msg = ":This lid value is invalid"
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Get thresholds from threshold file
                thresholds, flows, status_msg = load_site_thresholds(threshold_file, lid)

                MP_LOG.trace(status_msg)

                # Update status if stage thresholds are not found
                if thresholds is None or len(thresholds) == 0:
                    if "WRDS response sucessful." in status_msg:
                        msg = ':WRDS response sucessful but no stage values available'
                        all_messages.append(lid + msg)
                        MP_LOG.warning(huc_lid_id + msg)
                        continue
                    else:
                        msg = ':Error getting stage thresholds from WRDS API'
                        all_messages.append(lid + msg)
                        MP_LOG.warning(huc_lid_id + msg)
                        continue

                # Check if stages are supplied, if not write message and exit.
                # This message will occur if some thresholds are supplied, but not for the
                # categories we use (such as  “low” or “bankfull”)
                if all(thresholds.get(category, None) is None for category in categories):
                    msg = ':No thresholds for required categories found on WRDS API'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Read stage values and calculate thresholds
                # The error and warning message is already formatted correctly if applicable
                # Hold the warning_msg to the end
                stage_values_df, valid_stage_names, stage_warning_msg, err_msg = __calc_stage_values(
                    categories, thresholds
                )

                if err_msg != "":
                    # The error message is already formatted correctly
                    all_messages.append(lid + err_msg)
                    MP_LOG.warning(huc_lid_id + err_msg)
                    continue

                # Find lid metadata from master list of metadata dictionaries.
                metadata = next(
                    (item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False
                )
                lid_altitude = metadata['usgs_data']['altitude']
                if lid_altitude is None or lid_altitude == 0:
                    msg = ':AHPS site altitude value is invalid'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # If not manual input, check elevation data and get datum adjustment
                if data_source != 'Manual_Input':

                    # Look for acceptable elevations
                    acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df, huc_lid_id)

                    if acceptable_usgs_elev_df is None or len(acceptable_usgs_elev_df) == 0:
                        msg = ":Unable to find gage data"  # TODO: USGS Gage Method: Update this error message to be more descriptive
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

                    # Filter out sites that don't have "good" data
                    # TODO: USGS Gage Method: It doens't seem like the below error messages are performing as expected....
                    try:
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
                        MP_LOG.error(f"{huc_lid_id}: Filtering out 'bad' data in the usgs data")
                        MP_LOG.error(traceback.format_exc())
                        continue

                    # Adjust datum of HAND grid based on elevation data from usgs_elev_table.csv.
                    datum_adj_ft, datum_messages = __adjust_datum_ft(flows, metadata, lid, huc_lid_id)
                    all_messages = all_messages + datum_messages
                    if datum_adj_ft is None:
                        MP_LOG.warning(f"{huc_lid_id}: datum_adj_ft is None")
                        continue

                else:  # if source is manual input, we skip the above elevation filtering
                    MP_LOG.lprint(
                        f"{huc_lid_id}: Skipping elevation checks and datum adjustment for Manual Input source"
                    )

                    lid_altitude = float(lid_altitude)  # LID altitude is expected to be in meters
                    lid_usgs_elev = (
                        lid_altitude * 0.3048
                    )  # lid_altitude is now in meters to match non-manual input units
                    # TODO: Automate conversion?

                    datum_adj_ft = 0  # no datum adjustment for manual input

                # Initialize nested dict for lid attributes
                stage_based_att_dict.update({lid: {}})

                # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
                unfiltered_segments = list(set(get_nwm_segs(metadata)))

                # Filter segments to be of like stream order.
                desired_order = metadata['nwm_feature_data']['stream_order']
                segments = filter_nwm_segments_by_stream_order(
                    unfiltered_segments, desired_order, nwm_flows_region_df
                )

                # If no segments, write message and exit out
                if not segments or len(segments) == 0:
                    msg = ':missing nwm segments'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Check for large discrepancies between the elevation values from WRDS and HAND.
                #   Otherwise this causes bad mapping.
                # Manual_Input will have no elev disparity because it's from the the same value.
                elevation_diff = lid_usgs_elev - (lid_altitude * 0.3048)
                diff_rounded = round(elevation_diff, 2)

                # Log elevation difference information - not an error, just for reference (maybe remove later)
                if elevation_diff > 0:
                    MP_LOG.lprint(f"{huc_lid_id}: USGS elev is higher than HAND elev by {diff_rounded} ft")
                elif elevation_diff < 0:
                    MP_LOG.lprint(
                        f"{huc_lid_id}: USGS elev is lower than HAND elev by {abs(diff_rounded)} ft"
                    )

                if abs(elevation_diff) > 10:
                    msg = ':Large discrepancy in elevation estimates from gage and HAND'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue
                elif abs(elevation_diff) > 5:
                    msg = (
                        f':Moderate discrepancy ({diff_rounded} ft) in elevation estimates from gage and HAND'
                    )
                    MP_LOG.warning(huc_lid_id + msg)
                    # all_messages.append(lid + msg) # just print as a warning for now (not appending to message)
                    # We are not continuing, just a warning

                # This function sometimes is called within a MP but sometimes not.
                # So, we might have an MP inside an MP
                # and we will need a new prefix for it.

                # For each flood category / magnitude
                MP_LOG.lprint(f"{huc_lid_id}: About to process flood categories")

                # Make mapping lid_directory.
                mapping_lid_directory = os.path.join(mapping_huc_directory, lid)
                if not os.path.exists(mapping_lid_directory):
                    os.mkdir(mapping_lid_directory)

                # Check whether stage value is actually a WSE value, and fix if needed:
                # Get lowest stage value
                lowest_stage_val = stage_values_df['stage_value'].min()

                maximum_stage_threshold = 250  # TODO: Move to a variables file?

                # Make an "rfc_stage" column for better documentation which shows the original
                # uncorrect WRDS value before we adjsuted it for inundation
                stage_values_df['rfc_stage'] = stage_values_df['stage_value']

                # Stage value is larger than the elevation value AND greater than the
                # maximum stage threshold, subtract the elev from the "stage" value
                # to get the actual stage

                if (lowest_stage_val > lid_altitude) and (lowest_stage_val > maximum_stage_threshold):
                    stage_values_df['stage_value'] = stage_values_df['stage_value'] - lid_altitude
                    MP_LOG.lprint(
                        f"{huc_lid_id}: Lowest stage val > elev and higher than max stage thresh. Subtracted elev from stage vals to fix."
                    )

                # +++++++++++++++++++++++++++++
                # This section is for inundating stages and intervals come later

                # At this point we have at least one valid stage/category
                # cyle through on the stages that are valid
                # This are not interval values

                negative_hand_stage = False  # initialize value

                for idx, stage_row in stage_values_df.iterrows():
                    # Pull stage value and confirm it's valid, then process

                    category = stage_row['stage_name']
                    stage_value = stage_row['stage_value']

                    if stage_value == -1:  # messages already included in the stage_warning_msg above
                        continue

                    if (
                        negative_hand_stage == True
                    ):  # if we already had a negative hand stage, skip remaining stages
                        continue

                    MP_LOG.trace(f"About to create tifs for {huc_lid_id} : {category} : {stage_value}")

                    # datum_adj_ft should not be None at this point
                    # Call function to execute mapping of the TIFs.

                    # Calcluate a portion of the file name which includes the category,
                    # a formatted stage value and a possible "i" to show it is an interval file
                    category_key = __calculate_category_key(category, stage_value, False)

                    # These are the up to 5 magnitudes being inundated at their stage value
                    (messages, hand_stage, datum_adj_wse, datum_adj_wse_m) = produce_stage_based_lid_tifs(
                        stage_value,
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
                        category_key,
                        job_number_inundate,
                        MP_LOG.LOG_FILE_PATH,
                        child_log_file_prefix,
                    )

                    # If we get a message back, then something went wrong with the site and we need to
                    # remove it as a valid site
                    all_messages += messages

                    # Mark site as invalid if any stage results in a negative hand stage value
                    if hand_stage < 0:
                        negative_hand_stage = True

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

                    # Let's see any tifs made it, if not.. change this to an invalid stage value
                    stage_file_name = os.path.join(
                        mapping_lid_directory, lid + '_' + category_key + '_extent.tif'
                    )
                    if os.path.exists(stage_file_name) == False:
                        # something failed and we didn't get a rolled up extent file, so we need to reject the stage
                        stage_values_df.at[idx, 'stage_value'] = -1

                # If any stage resulted in a negative hand stage value, mark site as invalid.
                # because this indicates that there is an elevation disparity that will
                # likely result in bad mapping.
                if negative_hand_stage == True:
                    msg = ': Discrepancy in elevation estimates from gage and HAND caused negative HAND stage value'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # So, we might have an MP inside an MP
                # let's merge what we have at this point, before we go into another MP
                # TODO: Oct 2025: We should re-enable this, but need to test it first.
                # MP_LOG.merge_log_files(MP_LOG.LOG_FILE_PATH, child_log_file_prefix_tifs, True)

                # we do intervals only on non-record and valid stages
                non_rec_stage_values_df_unsorted = stage_values_df[
                    (stage_values_df["stage_value"] != -1) & (stage_values_df["stage_name"] != 'record')
                ]

                non_rec_stage_values_df = non_rec_stage_values_df_unsorted.sort_values(
                    by='stage_value'
                ).reset_index()

                # +++++++++++++++++++++++++++++
                # Creating interval tifs (if applicable)

                # We already inundated and created files for the specific stages just not the intervals
                # Make list of interval recs to be created
                interval_list = []  # might stay empty

                num_non_rec_stages = len(non_rec_stage_values_df)
                if num_non_rec_stages > 0:

                    interval_list = __calc_stage_intervals(
                        non_rec_stage_values_df, past_major_interval_cap, huc_lid_id
                    )

                    tif_child_log_file_prefix = MP_LOG.MP_calc_prefix_name(
                        parent_log_output_file, "MP_sb_interval_tifs"
                    )

                    # Now we add the interval tifs but no interval tifs for the "record" stage if there is one.
                    with ProcessPoolExecutor(max_workers=job_number_intervals) as executor:
                        try:

                            for interval_rec in interval_list:  # list of lists

                                category = interval_rec[0]  # stage name
                                interval_stage_value = interval_rec[1]

                                # Calcluate a portion of the file name which includes the category,
                                # a formatted stage value and a possible "i" to show it is an interval file
                                category_key = __calculate_category_key(category, interval_stage_value, True)

                                executor.submit(
                                    produce_stage_based_lid_tifs,
                                    interval_stage_value,
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
                                    category_key,
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

                else:
                    MP_LOG.lprint(
                        f"{huc_lid_id}: Skipping intervals as there are not any 'non-record' stages"
                    )

                # end of skip_add_intervals == False

                # For each valid stage, and if all goes well, they should have files
                # that end with "_extent.tif". If there is anything between _extent and .tif
                # it is a branch file adn our test is it at least one rollup exists
                inundate_lid_files = glob.glob(f"{mapping_lid_directory}/*_extent.tif")
                if len(inundate_lid_files) == 0:
                    msg = ':All stages failed to inundate'
                    all_messages.append(lid + msg)
                    MP_LOG.warning(huc_lid_id + msg)
                    continue

                # Create a csv with same information as geopackage but with each threshold as new record.
                # Probably a less verbose way.
                csv_df = pd.DataFrame(df_cols)  # for first appending

                # for threshold in categories:  (threshold and category are somewhat interchangeable)
                # some may have failed inundation, which we will rectify later
                for threshold in valid_stage_names:
                    try:
                        # we don't know if the magnitude/stage can be mapped yes it hasn't been inundated
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
                                'rfs_stage': stage_values_df.loc[stage_values_df['stage_name'] == threshold][
                                    'rfc_stage'
                                ],
                                'stage': stage_values_df.loc[stage_values_df['stage_name'] == threshold][
                                    'stage_value'
                                ],
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
                                'mapped': '',
                                'status': '',
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
                    # well.. mostly. If it fails, we can change back to

                    if stage_warning_msg == "":  # does not mean the lid is good.
                        all_messages.append(lid + ':Good')
                    else:  # we will leave the ":---" on it for now if it is does have a warning message
                        all_messages.append(lid + stage_warning_msg)
                        MP_LOG.warning(huc_lid_id + stage_warning_msg)
                else:
                    msg = ':Missing all calculated flows'
                    all_messages.append(lid + msg)
                    MP_LOG.error(huc_lid_id + msg)

                # MP_LOG.success(f'{huc_lid_id}: Complete')
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


def __calc_stage_values(categories, thresholds):
    '''
    Used in stage-based CatFIM.

    Calculates stage values for flood categories based on provided thresholds.

    Args:
        categories (list): List of stage names (e.g., "action", "minor", "moderate", "major", "record").
        thresholds (dict): Dictionary mapping stage names to their threshold values (anywhere from 0 to 5 stages).

    Returns:
        stage_values_df (pandas.DataFrame): DataFrame with rows for each stage and
            their corresponding values (defaulted to -1 if missing or invalid).
        valid_stage_names (list): List of stage names with valid threshold values.
        warning_msg (str): Warning message if some stages are missing valid values.
        err_msg (str): Error message if all stages are missing or invalid.

    Notes:
        - Stages with missing or invalid threshold values are assigned -1.
        - If all five stages are invalid, returns None for the DataFrame and an error message.
        - Warning messages are formatted with "---" to indicate missing stage data.

    '''

    # Set default values
    err_msg = ""
    warning_msg = ""
    default_stage_data = [['action', -1], ['minor', -1], ['moderate', -1], ['major', -1], ['record', -1]]
    valid_stage_names = []

    # Setting up a default df (not counting record)
    stage_values_df = pd.DataFrame(default_stage_data, columns=['stage_name', 'stage_value'])

    for stage in categories:

        if stage in thresholds:
            stage_val = thresholds[stage]
            if stage_val is not None and stage_val != "" and stage_val > 0:
                stage_values_df.loc[stage_values_df.stage_name == stage, 'stage_value'] = stage_val
                valid_stage_names.append(stage)

    invalid_stages_df = stage_values_df[stage_values_df["stage_value"] <= 0]

    if len(invalid_stages_df) == 5:
        err_msg = ':All threshold values are unavailable or invalid'  # already formatted
        return None, [], "", err_msg

    # Yes.. a bit weird, we are going to put three dashs in front of the message
    # to help show it is valid even with a missing stage msg.
    # any other record with a status value that is not "Good"
    # or does not start with a --- is assumed to be possibly bad (not mapped)
    warning_msg = ""

    for ind, stage_row in invalid_stages_df.iterrows():
        if warning_msg == "":
            warning_msg = f":---Missing stage data for {stage_row['stage_name']}"
        else:
            warning_msg += f"; {stage_row['stage_name']}"

    return stage_values_df, valid_stage_names, warning_msg, err_msg


def __calc_stage_intervals(non_rec_stage_values_df, past_major_interval_cap, huc_lid_id):
    '''
    Used in stage-based CatFIM.

    Calculate stage intervals for inundation mapping based on non-recurrent stage values.
    This function generates a list of intervals between stage values, rounding up to the next whole number
    where necessary, and ensures that intervals are unique and in order. For each stage, it determines the
    range of integer depths to be used for inundation calculations, up to the next stage or a specified cap
    for the last stage.

    Args:
        non_rec_stage_values_df (pd.DataFrame): DataFrame containing stage names and their corresponding stage values.
            Must have columns "stage_name" and "stage_value".
        past_major_interval_cap (int): The number of intervals to add beyond the last stage value.
        huc_lid_id (str): Identifier used for logging and tracing.

    Returns:
        list: A list of lists, where each sublist contains a stage name and an integer interval value,
              e.g., [["action", 21], ["action", 22], ...]. This represents the stage names and depths
              to be used for inundation mapping.
    '''
    interval_recs, stage_values_claimed = [], []

    MP_LOG.trace(
        f"{huc_lid_id}: Calculating intervals for non_rec_stage_values_df is {non_rec_stage_values_df}"
    )

    num_stage_value_recs = len(non_rec_stage_values_df)
    MP_LOG.trace(f"{huc_lid_id}: num_stage_value_recs is {num_stage_value_recs}")

    # recs will be in order
    # we do this one stage at a time, so we keep track of the stage name associated with the interval
    for idx in non_rec_stage_values_df.index:

        row = non_rec_stage_values_df.loc[idx]
        cur_stage_name = row["stage_name"]
        cur_stage_val = row["stage_value"]

        # MP_LOG.trace(f"{huc_lid_id}: interval calcs - non_rec_stage_value is idx: {idx}; {row}")

        # calc the intervals between the current and the next stage
        # for the current, we need to round up, but the current and the next
        # to stay at full integers. We do this as it is possible for stages to be decimals
        # ie) action is 2.4, and mod is 4.6, we want intervals at 3 and 4.
        # The highest value of the interval_list is not included

        if float(cur_stage_val) % 1 == 0:  # then we have a whole number
            # we only put it on the stage_value_claimed if is whole becuase
            # the intervals are whole numbers and are looking for dups

            cur_stage_val = int(cur_stage_val)
            stage_values_claimed.append(cur_stage_val)
            min_interval_val = int(cur_stage_val) + 1

        else:
            # round up to next whole number
            min_interval_val = math.ceil(cur_stage_val) + 1

        if idx < len(non_rec_stage_values_df) - 1:  # not the last record
            # get the next stage value
            next_stage_val = non_rec_stage_values_df.iloc[idx + 1]["stage_value"]
            max_interval_val = int(next_stage_val)
            # MP_LOG.trace(f"{huc_lid_id}: Next stage value is {max_interval_val}")
        else:
            # last rec. Just add 5 more (or the value from the input args)
            max_interval_val = int(min_interval_val) + past_major_interval_cap
            # MP_LOG.trace(f"{huc_lid_id}: Last rec and max_in is {max_interval_val}")

            # + 1 as the last interval is not included
        # MP_LOG.lprint(f"{huc_lid_id}: {cur_stage_name} is {cur_stage_val} and"
        #               f"  min_interval_val is {min_interval_val} ; max interval value is {max_interval_val}")
        interval_list = np.arange(min_interval_val, max_interval_val)

        # sometimes dups seem to slip through but not sure why, this fixes it
        for int_val in interval_list:
            if int_val not in stage_values_claimed:
                interval_recs.append([cur_stage_name, int_val])
                # MP_LOG.trace(f"{huc_lid_id}: Added interval value of {int_val}")
                stage_values_claimed.append(int_val)

    # MP_LOG.lprint(f"{huc_lid_id} interval recs are {interval_recs}")

    return interval_recs


def load_restricted_sites(is_stage_based):
    '''
    Used in both stage- and flow-based CatFIM.

    The 'catfim_type' arg is used to determine whether the site should be filtered out
    for stage-based CatFIM, flow-based CatFIM, or both.

    Args:
        catfim_type (str): Can have three different values: 'stage', 'flow', or 'both'.

    Returns:
        df_restricted_sites (pandas.DataFrame): A dataframe for the restricted lid and the reason why.
            Columns: 'nws_lid', 'restricted_reason', 'catfim_type'
    '''

    file_name = "ahps_restricted_sites.csv"
    current_script_folder = os.path.dirname(__file__)
    file_path = os.path.join(current_script_folder, file_name)

    df_restricted_sites = pd.read_csv(file_path, dtype=str)

    df_restricted_sites['nws_lid'].fillna("", inplace=True)
    df_restricted_sites['restricted_reason'].fillna("", inplace=True)
    df_restricted_sites['catfim_type'].fillna("", inplace=True)

    # remove extra empty spaces on either side of all cellls
    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.strip()
    df_restricted_sites['restricted_reason'] = df_restricted_sites['restricted_reason'].str.strip()
    df_restricted_sites['catfim_type'] = df_restricted_sites['catfim_type'].str.strip()

    # Need to drop the comment lines before doing any more processing
    df_restricted_sites.drop(
        df_restricted_sites[df_restricted_sites.nws_lid.str.startswith("#")].index, inplace=True
    )

    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.upper()

    # Clean up dataframe
    for ind, row in df_restricted_sites.iterrows():
        nws_lid = row['nws_lid']
        restricted_reason = row['restricted_reason']

        # if len(nws_lid) != 5:  # Invalid row, could be just a blank row in the file
        # (7/17/25) Removed this logic becuase it was preventing sites with more or
        # less than 5 character LIDs from being filtered out.
        #     FLOG.warning(
        #         f"From the ahps_restricted_sites list, an invalid nws_lid value of '{nws_lid}'"
        #         " and has dropped from processing"
        #     )
        #     indexs_for_recs_to_be_removed_from_list.append(ind)
        #     continue

        if restricted_reason == "":
            restricted_reason = "From the ahps_restricted_sites,"
            " the site will not be mapped, but a reason has not be provided."
            df_restricted_sites.at[ind, 'restricted_reason'] = restricted_reason
            FLOG.warning(f"{restricted_reason}. Lid is '{nws_lid}'")
        continue

    # Invalid records in CSV (not dropping, just completely invalid recs from the csv)
    # Could be just blank rows from the csv
    # (7/17/25) Removed this logic becuase it was preventing sites with more or
    # less than 5 character LIDs from being filtered out.
    # if len(indexs_for_recs_to_be_removed_from_list) > 0:
    #     df_restricted_sites = df_restricted_sites.drop(indexs_for_recs_to_be_removed_from_list).reset_index()

    # Filter df_restricted_sites by CatFIM type
    if is_stage_based == True:  # Keep rows where 'catfim_type' is either 'stage' or 'both'
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['stage', 'both'])]

    else:  # Keep rows where 'catfim_type' is either 'flow' or 'both'
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['flow', 'both'])]

    # Remove catfim_type column
    df_restricted_sites.drop('catfim_type', axis=1, inplace=True)

    return df_restricted_sites


def __adjust_datum_ft(flows, metadata, lid, huc_lid_id):
    '''
    Used in stage-based CatFIM.

    Determines the vertical datum adjustment (in feet) to convert the datum of the
    rating curve to NAVD88.

    Uses the rating curve source and metadata to get the correct vertical datum and CRS.

    It applies custom workarounds for known sites with special datum or CRS requirements,
    and attempts to compute the adjustment using the NOAA VDatum service when necessary.

    Args:
        flows (dict): Dictionary containing flow information, including the source of the rating curve.
        metadata (dict): Dictionary containing site metadata, including datum and CRS information.
        lid (str): Location identifier for the site.
        huc_lid_id (str): Combined HUC and location identifier for logging and messaging.
    Returns:
        tuple:
            - datum_adj_ft (float or None): The vertical datum adjustment in feet to convert to NAVD88,
              or None if adjustment could not be determined.
            - all_messages (list of str): List of messages and warnings generated during processing.
    Notes:
        - Special handling is included for sites with known datum or CRS issues.
        - If the datum is already NAVD88 or equivalent, the adjustment is 0.0.
        - If the datum is NGVD29 or similar, an adjustment is attempted using the NOAA VDatum service.
        - If errors occur during adjustment, appropriate messages are logged and returned.

    TODO: Aug 2024: This whole parts needs revisiting. Lots of lid data has changed and this
    is all likely very old.
    '''

    # Jul 2024: For now, we will duplicate messages via all_messsages and via the logging system.
    all_messages = []

    datum_adj_ft = None
    ### --- Do Datum Offset --- ###
    # determine source of interpolated threshold flows, this will be the rating curve that will be used.
    rating_curve_source = flows.get('source')

    # MP_LOG.trace(f"{huc_lid_id} : rating_curve_source is {rating_curve_source}")

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
        msg = ':Datum info unavailable'
        all_messages.append(lid + msg)
        MP_LOG.warning(huc_lid_id + msg)
        return None, all_messages

    # ___________________________________________________________________________________________________#
    # Check for typos in the horizontal datum data

    crs = datum_data.get('crs')
    vcs = datum_data.get('vcs')

    crs_corrected, vcs_corrected, uncorrected_crs_error, uncorrected_vcs_error, datum_corr_msgs = correct_datum_typos(crs, vcs)

    # Update the datum data with the corrected CRS and VCS if needed
    if crs_corrected is not None:
        datum_data.update(crs=crs_corrected)
    if vcs_corrected is not None:
        datum_data.update(vcs=vcs_corrected)

    # Log output messages from datum typo correction
    for msg in datum_corr_msgs:
        all_messages.append(lid + msg)
        MP_LOG.warning(huc_lid_id + msg)

    if uncorrected_crs_error:
        msg = ':CRS value is unrecognized and could not be corrected'
        all_messages.append(lid + msg)
        MP_LOG.warning(huc_lid_id + msg)
    if uncorrected_vcs_error:
        msg = ':VCS value is unrecognized and could not be corrected'
        all_messages.append(lid + msg)
        MP_LOG.warning(huc_lid_id + msg)

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
        # Get the datum adjustment to convert NGVD to NAVD.
        try:
            datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data)
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
                    datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data)
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
    '''
    Used in stage-based CatFIM.

    Creates an updated USGS elevation table with a descriptive USGS exclusion status column.

    The function checks each row of the input DataFrame for:
        - Acceptable USGS data altitude method code
        - Acceptable USGS site type
        - Acceptable USGS altitude accuracy threshold

    For each criterion not met, a corresponding message is appended to the 'usgs_exclusion_status' column.
    If all criteria are met, the status is set to 'acceptable'.
    In case of missing columns or errors, the original DataFrame is returned and errors are logged.

    Args:
        usgs_elev_df (pd.DataFrame): DataFrame containing USGS elevation data with required columns.
        huc_lid_id (str): Identifier for the HUC/LID, used for logging.

    Returns:
        pd.DataFrame: DataFrame with an added 'usgs_exclusion_status' column indicating acceptability.
    '''
    acceptable_usgs_elev_df = None
    try:
        acceptable_msg = ''
        unacceptable_alt_meth_msg = 'Unacceptable USGS data altitude method: '
        unacceptable_site_type_msg = 'Unacceptable USGS site type: '
        unacceptable_alt_acc_msg = 'Unacceptable USGS altitude accuracy threshold: '

        # Create columns for whether the USGS data meets each criterion
        msg1 = np.where(
            usgs_elev_df['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list),
            acceptable_msg,
            unacceptable_alt_meth_msg + usgs_elev_df['usgs_data_alt_method_code'].astype(str) + ', ',
        )
        msg2 = np.where(
            usgs_elev_df['usgs_data_site_type'].isin(acceptable_site_type_list),
            acceptable_msg,
            unacceptable_site_type_msg + usgs_elev_df['usgs_data_site_type'].astype(str) + ', ',
        )
        msg3 = np.where(
            usgs_elev_df['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh,
            acceptable_msg,
            unacceptable_alt_acc_msg + usgs_elev_df['usgs_data_alt_accuracy_code'].astype(str) + ', ',
        )

        status_df = pd.DataFrame({'msg1': msg1, 'msg2': msg2, 'msg3': msg3})

        # Create detailed USGS exclusion status
        usgs_elev_df['usgs_exclusion_status'] = status_df['msg1'] + status_df['msg2'] + status_df['msg3']

        # If it doesn't have anything for the exclusion criteria, set the usgs_exclusion_status to acceptable
        # CatFIM will only be processed for sites with a usgs_exclusion_status of 'acceptable'
        usgs_elev_df['usgs_exclusion_status'] = usgs_elev_df['usgs_exclusion_status'].replace(
            '', 'acceptable'
        )

        # Copy df to de-fragment and rename
        acceptable_usgs_elev_df = usgs_elev_df.copy()

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
    '''
    Used in stage-based CatFIM.

    Retrieves the DEM-adjusted elevation value for a given USGS gage site (LID) from the provided DataFrame,
    and checks for exclusion criteria or data issues.

    Args:
        acceptable_usgs_elev_df (pd.DataFrame): DataFrame containing USGS gage information, including
            'nws_lid', 'levpa_id', 'dem_adj_elevation', and 'usgs_exclusion_status' columns.
        lid (str): The NWS LID to look up.
        huc_lid_id (str): Combined HUC and LID identifier for logging purposes.

    Returns:
        tuple:
            - lid_usgs_elev (float): The DEM-adjusted elevation value for the specified LID, or 0 if not found or excluded.
            - all_messages (list of str): List of warning or error messages encountered during the lookup process.

    Notes:
        - If the LID is not found, excluded, or has an elevation of 0, appropriate messages are logged and returned.
        - If multiple entries exist for the LID, the one with a non-zero 'levpa_id' is used.
        - Exclusion status other than 'acceptable' will result in an early return with a message.
    '''

    # MP_LOG.trace(locals())

    lid_usgs_elev = 0
    all_messages = []
    try:
        # Check for USGS elevation data that matches the LID
        matching_rows = acceptable_usgs_elev_df.loc[acceptable_usgs_elev_df['nws_lid'] == lid.upper()]

        # Check if the site is not in the usgs table in our data
        if len(matching_rows) == 0:
            # msg = ':Gage not in HAND usgs gage records' # prev error message (deprecated May 2025)
            msg = ':Gage not in HAND usgs gage records, likely due to exclusion criteria'
            all_messages.append(lid + msg)
            MP_LOG.warning(huc_lid_id + msg)
            return lid_usgs_elev, all_messages

        # It means there are two level paths, use the one that is not 0 (there will never be more than two)
        if len(matching_rows) == 2:
            # Get the site that does not have a levpa_id of zero and matches the LID
            lid_info = acceptable_usgs_elev_df.loc[
                (acceptable_usgs_elev_df['nws_lid'] == lid.upper())
                & (acceptable_usgs_elev_df['levpa_id'] != 0)
            ]

        else:
            # Get the site that matches the LID
            lid_info = acceptable_usgs_elev_df.loc[acceptable_usgs_elev_df['nws_lid'] == lid.upper()]

        # Get elevation and exclusion status
        lid_usgs_elev = lid_info['dem_adj_elevation'].values[0]
        usgs_exclusion_status = lid_info['usgs_exclusion_status'].values[0]

        # If there is an exclusion status other than 'acceptable,' return the status
        # Uses [:-2] to exclude the last comma and space in the string
        if usgs_exclusion_status != 'acceptable':
            msg = ':Gage excluded due to the following criteria -- ' + usgs_exclusion_status[:-2]
            all_messages.append(lid + msg)
            MP_LOG.warning(huc_lid_id + msg)
            return lid_usgs_elev, all_messages

        # Check whether DEM adjusted elevation is 0 or not set
        if lid_usgs_elev == 0:
            msg = ':DEM adjusted elevation is 0 or not set'
            all_messages.append(lid + msg)
            MP_LOG.warning(huc_lid_id + msg)
            return lid_usgs_elev, all_messages

    except IndexError:  # Occurs when LID is missing from table (yes. warning)
        msg = ':Error when extracting dem adjusted elevation value'
        all_messages.append(lid + msg)
        MP_LOG.warning(f"{huc_lid_id}: adjusting dem_adj_elevation")
        MP_LOG.warning(huc_lid_id + msg)
        MP_LOG.warning(traceback.format_exc())

    MP_LOG.trace(f"{huc_lid_id} : lid_usgs_elev is {lid_usgs_elev}")

    return lid_usgs_elev, all_messages


def __calculate_category_key(category, stage_value, is_interval_stage):
    '''
    Used in stage-based CatFIM.

    Calcuates the category key which becomes part of a file name
    Changes to an int if whole number only. ie.. we don't want 22.00 but 22.0, but keep 22.15
    category_key comes things like this: action, action_24.0ft, or action_24.6ft

    Args:
        category (str): The flood category (e.g., 'action', 'minor', etc.).
        stage_value (float): The stage value for the category.
        is_interval_stage (bool): Whether this is an interval stage (True) or a main stage (False).

    Returns:
        category_key (str): Category and stage value string for file naming.

    TODO: yes... this needs a better answer.
    '''
    category_key = category + "_"  # ie) action_

    if float(stage_value) % 1 == 0:  # then we have a whole number
        # then we will turn it into a int and manually add ".0" on it
        category_key += str(int(stage_value)) + ".0"
    else:
        category_key += "{:.2f}".format(stage_value)

    category_key += "ft"

    # The "i" in the end means it is an interval
    # Now we are action_24.0ft or action_24.6ft or action_24.65ft or action_24.0fti
    if is_interval_stage == True:
        category_key += "i"

    return category_key


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
    job_number_intervals,
    past_major_interval_cap,
    nwm_meta_file,
    df_restricted_sites,
    threshold_file,
    data_source,
):
    '''
    Generates stage-based CatFIM for a list of HUCs.

    This function orchestrates the workflow for producing stage-based CatFIM outputs, including:
    - Generating necessary flow data and site attributes.
    - Parallel processing of HUCs to create inundation mapping and attribute files.
    - Aggregating and merging results from parallel tasks.
    - Compiling a comprehensive GeoPackage and CSV of all candidate sites, indicating mapping status and reasons for unmapped sites.
    - Logging and error handling throughout the process.

    Parameters
    ----------
        output_catfim_dir (str): Directory where CatFIM outputs will be written.
        fim_run_dir (str): Directory containing FIM run data.
        nwm_us_search (str): Path or identifier for upstream NWM search data.
        nwm_ds_search (str): Path or identifier for downstream NWM search data.
        env_file (str): Path to the environment file for configuration.
        job_number_inundate (int): Number of parallel jobs for inundation processing.
        job_number_huc (int): Number of parallel jobs for HUC processing.
        lst_hucs (list of str): List of HUCs to process.
        job_number_intervals (int): Number of parallel jobs for interval processing.
        past_major_interval_cap (int): Cap for past major intervals.
        nwm_meta_file (str): Path to the NWM metafile.
        df_restricted_sites (pd.DataFrame): DataFrame of restricted sites to exclude from processing.
        threshold_file (str): Path to the threshold file for mapping.
        data_source (str): Identifier for the data source being used.

    Outputs
    ----------
        - Attribute CSVs for each mapped site.
        - A merged attribute CSV (`nws_lid_attributes.csv`) in the attributes directory.
        - A GeoPackage (`stage_based_catfim_sites.gpkg`) and CSV summarizing all candidate sites and their mapping status.
        - Log files documenting the process and any issues encountered.

    Note: The function assumes the existence of several external utilities and global variables (e.g., FLOG, VIZ_PROJECTION, acceptable_* lists).

    Raises
    ----------
        Exception: If no attribute CSV files are found or if other critical errors occur during processing.
    '''

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
    # Generate flows is only using one of the incoming job number params
    # so let's multiply -jh (huc) and -jn (inundate)
    job_flows = job_number_huc * job_number_inundate
    if job_flows > 90:
        job_flows == 90

    # If stage based, generate flows, mostly returns values sent in with a few changes
    # stage based doesn't really need generated flow data
    # But for flow based, it really does use it to generate flows.
    #
    (huc_dictionary, sites_gdf, ___, threshold_url, all_lists, flows_df_dict) = generate_flows(
        output_catfim_dir,
        nwm_us_search,
        nwm_ds_search,
        env_file,
        job_flows,
        True,
        lst_hucs,
        nwm_meta_file,
        str(FLOG.LOG_FILE_PATH),
        df_restricted_sites,
        threshold_file,
    )

    # FLOG.trace("Huc distionary is ...")
    # FLOG.trace(huc_dictionary)

    child_log_file_prefix = FLOG.MP_calc_prefix_name(FLOG.LOG_FILE_PATH, "MP_iter_hucs")

    FLOG.lprint(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    FLOG.lprint("Start processing HUCs for Stage-Based CatFIM")
    num_hucs = len(lst_hucs)
    huc_index = 0
    FLOG.lprint(f"Number of hucs to process is {num_hucs}")

    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        try:
            for huc in huc_dictionary:
                if huc in lst_hucs:
                    # FLOG.lprint(f'Generating stage based catfim for : {huc}')

                    if huc[:4] == '2201':  # Guam
                        nwm_flows_region_df = flows_df_dict['nhd_flows_guam_df']
                    elif huc[:4] == '2203':  # American Samoa
                        nwm_flows_region_df = flows_df_dict['nhd_flows_americansamoa_df']
                    elif huc[:2] == '19':  # Alaska
                        nwm_flows_region_df = flows_df_dict['nwm_flows_alaska_df']
                    else:  # CONUS + Hawaii + Puerto Rico
                        nwm_flows_region_df = flows_df_dict['nwm_flows_df']

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
                        threshold_file,
                        data_source,
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
        raise Exception(f"no csv files found - missing attribute CSVs in {attributes_dir}")
    # TODO: This error currently occurs if no sites are mapped (usually in a test).
    # Make a test that catches this earlier and provides a more legible error/warning message.

    all_csv_df.to_csv(os.path.join(attributes_dir, 'nws_lid_attributes.csv'), index=False)

    # This section populates a geopackage of all potential sites and details
    # whether it was mapped or not (mapped field) and if not, why (status field).

    # Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.

    # epsg:5070, we really want 3857 out for all outputs
    sites_gdf = sites_gdf.to_crs(VIZ_PROJECTION)
    sites_gdf.rename(
        columns={
            'identifiers_nwm_feature_id': 'nwm_seg',
            'identifiers_nws_lid': 'nws_lid',
            'identifiers_usgs_site_code': 'usgs_gage',
        },
        inplace=True,
    )
    sites_gdf['nws_lid'] = sites_gdf['nws_lid'].str.lower()

    # Using list of csv_files, populate DataFrame of all nws_lids that had
    # a flow file produced and denote with "mapped" column.
    nws_lids = []
    for csv_file in attrib_csv_files:
        nws_lids.append(csv_file.split('_attributes')[0])
    lids_df = pd.DataFrame(nws_lids, columns=['nws_lid'])

    # Identify what lids were mapped by merging with lids_df. Populate
    # 'mapped' column with 'No' if sites did not map.
    sites_gdf = sites_gdf.merge(lids_df, how='left', on='nws_lid')

    # Added here, but may be changed later if files don't inundate
    sites_gdf['mapped'] = "no"

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

        # see if there are any duplicates. Their should not be in theory
        # We pull them out so we can print dups, before removing them from the parent
        duplicate_lids = messages_df[messages_df.duplicated(['nws_lid'])]
        if len(duplicate_lids) > 0:
            FLOG.warning("Duplicate ahps ids found...")
            FLOG.warning(duplicate_lids)
            # let's just pick the last one (gulp)
            messages_df = messages_df.drop_duplicates(subset=['nws_lid'], keep='last').reset_index(drop=True)

        # Join messages to populate status field to candidate sites. Assign
        # status for null fields.
        sites_gdf = sites_gdf.merge(messages_df, how='left', on='nws_lid')

        # TODO: This is ugly. It is possible that there are no inundation files for any given lid
        # if that is true, we need to update this sites csv. We will figure that out in final
        # library mapping and update the sites csv at the same time for those scenarios

        #  (msg in flows)
        # sites_gdf['status'] = sites_gdf['status'].fillna('Good')  # hummmm
        # Status could still be starting with --- at this point and leave it for now.

        # Add acceptance criteria to viz_out_gdf before writing
        sites_gdf['acceptable_coord_acc_code_list'] = str(acceptable_coord_acc_code_list)
        sites_gdf['acceptable_coord_method_code_list'] = str(acceptable_coord_method_code_list)
        sites_gdf['acceptable_alt_acc_thresh'] = float(acceptable_alt_acc_thresh)
        sites_gdf['acceptable_alt_meth_code_list'] = str(acceptable_alt_meth_code_list)
        sites_gdf['acceptable_site_type_list'] = str(acceptable_site_type_list)

        # Rename the stage_based_catfim db column from nws_lid to ahps_lid to be
        # consistant with all other CatFIM outputs
        sites_gdf.rename(columns={"nws_lid": "ahps_lid"}, inplace=True)

        # Index = False as it already has a field called Index from the merge
        sites_gdf.to_file(nws_lid_gpkg_file_path, driver='GPKG', index=False, engine='fiona')

        csv_file_path = nws_lid_gpkg_file_path.replace(".gpkg", ".csv")
        sites_gdf.to_csv(csv_file_path, index=False)
    else:
        FLOG.warning(f"nws_sites_layer ({nws_lid_gpkg_file_path}) : has no messages and should have some")


def set_start_files_folders(
    step_num, output_catfim_dir, output_mapping_dir, output_flows_dir, attributes_dir, overwrite
):
    '''
    Used in both stage- and flow-based CatFIM.

    Sets up and manages the initial folder structure and log files.

    Depending on the step number and overwrite flag, this function will:
    - Create the main output directory if it does not exist.
    - Check for the existence of the output mapping directory as a proxy for all output folders.
    - If the mapping directory exists and overwrite is False, raises an Exception to prevent accidental data loss.
    - If overwrite is True, deletes and recreates the mapping, flows, and attributes directories.
    - Always creates the flows, mapping, and attributes directories if they do not exist.
    - Ensures a logs directory exists and sets up logging for the process.

    Args:
        step_num (int): The current step number in the workflow. Only performs folder cleaning if step_num == 0.
        output_catfim_dir (str): Path to the main output directory for the Categorical FIM process.
        output_mapping_dir (str): Path to the output directory for mapping results.
        output_flows_dir (str): Path to the output directory for flow results.
        attributes_dir (str): Path to the output directory for attribute results.
        overwrite (bool): If True, existing output directories will be deleted and recreated.
    '''

    # ================================
    # Folder cleaning based on step system
    if step_num == 0:
        # The override is not for the parent folder as we want to keep logs around with or without override
        if os.path.exists(output_catfim_dir) == False:
            os.mkdir(output_catfim_dir)

        # Create output directories (check against maping only as a proxy for all three)
        if os.path.exists(output_mapping_dir) == True:
            if overwrite == False:
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
    -t /data/catfim/rob_test/docker_test_1
    -me '/data/catfim/rob_test/nwm_metafile.pkl' -sb -cv "2.2" -hv "4.5.11.1" -step 2
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM')
    parser.add_argument(
        '-f',
        '--fim_run_dir',
        help='REQUIRED: Path to directory containing HAND outputs, e.g. /data/previous_fim/fim_4_5_2_11',
        required=True,
    )
    parser.add_argument(
        '-e',
        '--env_file',
        help='OPTIONAL: Docker mount path to the catfim environment file.'
        ' Defaults to: /data/config/fim_enviro_values.env',
        default="/data/config/fim_enviro_values.env",
        required=False,
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

    parser.add_argument(
        '-mf',
        '--nwm-meta-file',
        help='OPTIONAL: If you have a pre-existing nwm metadata pickle file, you can path to it here.'
        ' e.g.: /data/catfim/nwm_metafile.pkl',
        required=False,
        default="",
    )

    parser.add_argument(
        '-gmf',
        '--get-new-meta-data',
        help="OPTIONAL: If this argument is added, and this script is on a OWP server, then ignore"
        " and pre-existing meta file and go load new data directly from WRDS. Note: Calling WRDS"
        " directly means you can add filters, searching, site specific, etc. This allows for easier debugging."
        " However, the default behavior is to use the previously created nwm_metadata file and filter out the data"
        " CatFIM needs for processing.",
        required=False,
        default=False,
        action='store_true',
    )

    # get from bash_varibles.env or similar if not provided
    parser.add_argument(
        '-tf',
        '--threshold-file',
        help='OPTIONAL: If you have a pre-existing threshold file, you can path to it here. '
        'Providing this manual input will prevent the WRDS API from being queried for thresholds.'
        ' e.g.: /data/catfim/threshold_file.pkl',
        required=False,
        default="",
    )

    parser.add_argument(
        '-gtf',
        '--get-new-threshold-data',
        help="OPTIONAL: If this argument is added, and this script is on a OWP server, then ignore"
        " and pre-existing threshold data file and go load new data directly from WRDS. Note: Calling WRDS"
        " directly means you can add filters, searching, site specific, etc. This allows for easier debugging."
        " However, the default behavior is to use the previously created nwm_threshold file and filter out the data"
        " CatFIM needs for processing.",
        required=False,
        default=False,
        action='store_true',
    )

    parser.add_argument(
        '-cv',
        '--catfim-version',
        help='OPTIONAL: The version of the code that was used to run the product. This value is included'
        ' in the output gpkgs and csvs in a field named product_version. If you put in a value here,'
        ' we will add the phrase CatFIM to the front of it.'
        ' ie) 2.0 becomes CatFIM, 2.2 becomes CatFIM, etc. Defaults to blank',
        required=False,
        default="",
    )

    parser.add_argument(
        '-hv',
        '--model-version',
        help='OPTIONAL: The version of the HAND data outputs that was used to run the product.'
        ' This value is included in the output gpkgs and csvs in a field named model_version.'
        ' If you put in a value here, we will change dots to underscores only.'
        ' This should be a HAND version number only and not include the word HAND_'
        ' ie) 4.5.11.1 becomes 4_5_11_1, etc. Defaults to blank',
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
