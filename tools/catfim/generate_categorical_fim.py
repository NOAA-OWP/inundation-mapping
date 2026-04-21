#!/usr/bin/env python3

import argparse
import logging
import os
import re
import shutil
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

import data.wrds.download_process_wrds as dpw
import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf
from src.utils.shared_variables import VIZ_PROJECTION
from tools.catfim.catfim_post_processing import catfim_post_processing
from tools.catfim.catfim_process_huc import process_huc
from tools.tools_shared_functions import aggregate_wbd_hucs


"""
Oct/Nov 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing # TODO: Clean up

Tenative notes:
    - This script will fundamentally play the role stricly as pre-processing for processing HUC and their
      related sites.

    - Some of the functions in here may move or be split to smaller functions.

    - Data acquistion such as meta, threshold or flows, should be moved ot generate_categorical_fim_flows.py

    - Anything related to inundation, tifs, gpkgs, etc, shoudl be moved to generate_categorical_mapping.py

    - Anything relating to final post-processing such as merging of sites / library data, or last minute editing
      of site data will be moved into catfim_post_processing.py

    - This will continue to know if it is processing SB or FB.

    - Primary tasks for this script become:
        - processing incomings and creating system wide variables as needed. They will be saved into
          a runtime_args.env file that catfim_process_huc.py and catfim_post_processing.py can pick up and use.

        - This will setup the overall folder structure including the parent catfim output paths such
          as hand_4_x_x_x_flow_based.

        - Make calls to generate_categorical_fim_flows.py to create/acquire meta, threshold, flow data
          that could be used for all HUCs and sites no matter what hucs are being processed at this time.

        - Have a way to figure out if we can use a previously created pickle/parquet files for HUC processing.
          We need a way to also tell the system to reload meta/threshold/flow data when applicable. We might
          just reuse our current system or a similar one. Question: I assume we will have seperate files
          for meta versus threshold, so how do we tell the system to use one but reload the other or
          various combinations. Maybe we already have that in the code. :)

        - Setup an iterator using Multi-proc to process each HUC (catfim_process_huc.py), but keeping
          arguments to a minimum focusing primarily on letting each huc pick up the runtime_args.txt file to
          do its processing.

        - This can still take a list or file of HUCs, same as it current does and will need to
          validate as well, just as we currently do.

"""


def process_generate_categorical_fim(
    fim_run_dir,
    env_file,
    number_jobs,
    catfim_type,
    output_folder,
    search,
    lst_hucs,
    past_major_interval_cap,
    nwm_meta_file,
    get_new_meta_data,
    threshold_file,
    get_new_threshold_data,
    skip_processing,
    overwrite,
):
    '''

    Orchestrates the generation of CatFIM products for a set of Hydrologic Unit Codes (HUCs),
    supporting both stage-based and flow-based methodologies. Handles validation, setup, filtering, and multi-step processing
    including flow generation, mapping, post-processing, and status updates.

    # Note: lst_hucs argument is used but passed via locals() so VSCode thinks it is not in use.


    Arguments
    ---------
    fim_run_dir - str
        Filpath to the FIM output directory to run CatFIM on.
    env_file - str
        Docker mount filepath to the catfim environment file. Defaults to: /data/config/fim_enviro_values.env
    number_jobs - int
        Number HUCs to process simultaneously. Defaults to 20.
    catfim_type - str
        Indication of whether the CatFIM run is flow-based ('fb') or stage-based ('sb').
    output_folder - str
        Filepath to where the CatFIM output folder will be (ie /data/catfim/hand_4_8_7_2).
        Note: Output folder name will have flow_based or stage_based appended.
    search - int
        Upstream and downstream search in miles. How far up and downstream of a site do
        we want to inundate? Defaults to 5.
    lst_hucs - str
        Space-delimited list of HUCs to produce CatFIM for. Defaults to 'all' for all HUCs.
    past_major_interval_cap - int
        Stage-Based only. How many feet past major do you want to go for the interval FIMs?
        of the machine. Defaults to 5.
    nwm_meta_file - str
        Filepath to pre-existing metadata pickle file.
    get_new_meta_data - bool
        Whether or not to download new metadata from WRDS. Defaults to False (no download).
        Only works on OWP servers, not AWS.
    threshold_file - str
        Filepath to pre-existing thresholds pickle file.
    get_new_threshold_data - bool
        Whether or not to download new thresholds from WRDS. Defaults to False (no download).
        Only works on OWP servers, not Amazon AWS.
    skip_processing - bool
        If True, this function will set up all of the initial 'pre-processing' steps, but will
        not continue with the processing of the hucs or post processing.
    overwrite - bool
        If True, previous files (aside from logs) will be overwritten.

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

    Workflow Steps   (TODO: needs updating)
    -------------
    1. Validation and setup of input directories, files, and parameters.
    2. Filtering and selection of valid HUCs based on input and threshold files.
    3. Stage-based or flow-based CatFIM processing, including flow generation, mapping, and post-processing.
    4. Compilation of threshold data and cleanup of intermediate files.
    5. Updating mapping status for processed sites.
    6. Logging of progress, warnings, and summary information.

    Notes
    -----
    - Handles both manual and automated threshold input via `threshold_file`.
    - Uses environment variables for API access and configuration.
    - Designed for parallel processing and scalable workflows.
    - "search" : Define upstream and downstream search in miles


    '''

    is_logging_loaded = False

    try:
        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")
        print("================================")

        load_dotenv('/foss_fim/src/bash_variables.env')

        # ================================
        # Validation and setup

        # Validate catfim type outside of validation function so we can use catfim_type_name later
        if catfim_type not in ["fb", "sb"]:
            raise Exception(
                "Argument for -ct (catfim type) must be either fb (for flow based)"
                " or sb (for stage based)."
            )

        # Get CatFIM type
        if catfim_type == "sb":
            catfim_type_name = "stage_based"
        else:  # fb
            catfim_type_name = "flow_based"

        # Clean up output folder name
        if output_folder.endswith("/"):
            output_folder = output_folder[:-1]
        output_folder = output_folder + "_" + catfim_type_name

        # Validate inputs and get back validated huc list and paths for meta and threshold files
        local_vals = (
            locals()  # lst_hucs argument is used but passed via locals() so VSCode thinks it is not in use.
        )
        valid_fim_hucs, dropped_huc_lst, nwm_meta_file, threshold_file = __validate_inputs(local_vals)
        valid_fim_hucs.sort()

        # Note: this will handle a huc list arg of "all". If valid_fim_hucs is empty, it will thrown an exception
        # valid_fim_hucs are hucs that have valid huc folders in the fim output dir.
        # It has not yet been compared to metadata and sites.

        # Make output folder
        permissions_code = 0o776
        os.makedirs(output_folder, mode=permissions_code, exist_ok=True)

        # Clear previous temp folders if they exist and are not empty (if overwrite is True)
        # or throw an error if they are not empty (if overwrite is False)
        temp_folder = os.path.join(output_folder, "temp")

        if os.path.exists(temp_folder) and os.path.getsize(temp_folder) > 0:
            if overwrite:
                shutil.rmtree(temp_folder, ignore_errors=True)
                if os.path.exists(temp_folder):
                    try:
                        os.remove(temp_folder)
                    except OSError as e:
                        logging.critical(f"Error: Unable to remove {temp_folder} - {e}")
            else:
                raise Exception(
                    "Temp folder is not empty. Please clear the temp folder, use a new CatFIM output filename, or use the overwrite option to continue."
                )

        # Set up logging
        log_file_path = sf.setup_file_logger(temp_folder, "gen_catfim")
        is_logging_loaded = True

        # We will populate this later on, just need to define it here for the try except scope
        task_args_list = []

        logging.info(f"Starting CatFIM Processing - {catfim_type_name} ;  (UTC): {dt_string}")
        print("")
        print(
            f"... Logs will be saved to {log_file_path} initially and later copied over to {output_folder}/logs"
        )

        # Make sites output filepath (needed even if we choose skip_processing)
        nwm_sites_file = os.path.join(output_folder, "nwm_sites.parquet")

        # Create the runtime args file to store CatFIM inputs
        __create_runtime_args_file(
            output_folder,
            env_file,
            search,
            catfim_type,
            nwm_meta_file,
            nwm_sites_file,
            get_new_meta_data,
            threshold_file,
            get_new_threshold_data,
            fim_run_dir,
            past_major_interval_cap,
        )

        # Throw a warning if any listed HUCs are in our FIM outputs
        if len(dropped_huc_lst) > 0:
            logging.warning('Listed HUCs not available in FIM run directory:')
            logging.warning(dropped_huc_lst)

        # Load the API url if we are going to to call WRDS APIs for meta or threshold data
        api_base_url = ""
        if get_new_meta_data is True or get_new_threshold_data is True:
            api_base_url = csf.load_fim_global_env_values(env_file)

        # ================================
        # Load NWM metadata

        section_start_dt = datetime.now(timezone.utc)

        logging.info("Loading metadata and HUC dictionary")

        # Jan 2026 note:
        # I don't think I need the meta_list in this particular script, just the huc dictionary,
        # but load_nwm_metadata loads the meta data list to calc the huc_dictionary.
        # Other scripts, do use the meta_data values.

        # Load NWM metadata from pickle or WRDS
        # Note: We do not pass in a huc list as it can miss some sites. See notes at load_nwm_metadata
        metadata_json_list, return_msgs = dpw.load_nwm_metadata(
            nwm_meta_file, api_base_url, search, get_new_meta_data
        )

        # Parse any messages returned from load_nwm_metadata
        if len(return_msgs) > 0:
            # return_msgs is a list and might have some warnings, some messages and/or errors
            # TODO: This seems a bit bumpy but good enough for now. No idea on a better answer short of
            # custom exceptions.

            for msg in return_msgs:
                if "warning" in msg.lower():
                    logging.warning(msg)
                elif "error" in msg.lower():
                    raise Exception(msg)
                else:
                    logging.info(msg)

        end_dt = datetime.now(timezone.utc)
        time_duration = end_dt - section_start_dt
        logging.info(f"Completed loading metadata - Duration: {str(time_duration).split('.')[0]}")
        logging.info("")

        # ================================
        # Create HUC dictionary and NWM sites GeoDataFrame

        wbd_file = os.getenv("input_wbd_layer")

        huc_dictionary, nwm_sites_all_gdf = aggregate_wbd_hucs(
            metadata_json_list, wbd_file, retain_attributes=True
        )
        if len(huc_dictionary) == 0:
            raise Exception("The metadata pickle file does not have any applicable HUCs")

        # Dictionary of {col_name: col_type}
        colname_dict = {
            'metadata_sources': 'str',
            'downstream_nwm_features': 'str',
            'upstream_nwm_features': 'str',
            'nwm_feature_data_downstream_feature_id': 'str',
            'nws_data_county_code': 'str',
            'nwm_feature_data_nhd_waterbody_comid': 'str',
            'nws_data_latitude': 'float',
            'nws_data_longitude': 'float',
            'nws_data_zero_datum': 'float',
            'nwm_feature_data_stream_order': 'str',
        }

        # Specify column data types to avoid issues when saving to gpkg
        for col_name, col_type in colname_dict.items():
            if col_name not in nwm_sites_all_gdf.columns:
                # If column is missing, create it
                nwm_sites_all_gdf[col_name] = None
                logging.info(
                    f"Column '{col_name}' was missing from nwm_sites_all_gdf and has been created with default empty values."
                )

            # Set/Update the column type
            nwm_sites_all_gdf[col_name] = nwm_sites_all_gdf[col_name].astype(col_type)

        # NOTE: These fields throw errors when saving from gdf to gpkg, throwing Skipping field because of invalid value
        # but strangely not in all records. Likely bad records or nulls in key which get filtered out later.

        # Save the nwm_sites_all_gdf for catfim_process_huc.py to pick up.
        # It has all sites and its huc number, and the geometry for all points.
        # Each huc will make its own filtered copy, update status, etc and save at each huc level
        # for post processing rollup.

        nwm_sites_all_gdf = nwm_sites_all_gdf.to_crs(VIZ_PROJECTION)

        # Save a parquet version for quick loading in each HUC and 1/10th of the size
        nwm_sites_all_gdf.to_parquet(nwm_sites_file)

        # Save a GPKG version for debugging (not shared with the HUCs)
        nwm_sites_all_gdf.to_file(
            nwm_sites_file.replace('.parquet', '.gpkg'), driver='GPKG', engine='fiona', index=False
        )

        # Save the HUC list for this CatFIM run (AWS will need this list to know what HUCs to process and iterate)
        catfim_huc_list_file = os.path.join(output_folder, "catfim_huc_list.txt")
        with open(catfim_huc_list_file, "w") as f:
            for item in valid_fim_hucs:
                f.write(f"{item}\n")

        # Each huc has their own independent self-encapsulated folder under the "hucs" folder.
        # ie) /data/catfim/my_test_flow_based/hucs/12090301
        huc_dir = os.path.join(output_folder, 'hucs')
        os.makedirs(huc_dir, exist_ok=True)
        os.chmod(huc_dir, 0o777)  # 777 (rwxrwxrwx)

        # ================================
        # Download thresholds (if specified)

        if get_new_threshold_data == True:
            section_start_dt = datetime.now(timezone.utc)

            logging.info("Downloading threshold data from WRDS...")

            # Get a dictionary of which sources have valid CRS's for each site
            lid_source_dict = dpw.check_metadata_CRS_availability(metadata_json_list)

            output_lid_source_table_filename = 'lid_source_table.csv'
            output_lid_source_table_filepath = os.path.join(output_folder, output_lid_source_table_filename)

            lid_source_df = pd.DataFrame(lid_source_dict)
            lid_source_df.to_csv(output_lid_source_table_filepath, index=False)

            logging.info(f'Site source table will be saved to {output_lid_source_table_filepath}')

            # Note: Currently we default to downloading all thresholds.
            # If we wanted to speed things up in the future, we could filter the
            # HUC dictionary to the HUC list and then it would only download
            # thresholds for those HUCs. We would still kind of be stuck downloading
            # the full set of metadata, though, because the HUC column in WRDS is
            # too unreliable to filter with. If we do end up downloading only some
            # thresholds, we will want to implement dpw.label_data_file() here
            # too so the threshold data has an indicator that it is only a subset.

            output_thresholds_filename = 'thresholds.pkl'
            # TODO: Add output filenames to a shared variables or env file?
            thresholds_filepath = os.path.join(output_folder, output_thresholds_filename)

            logging.info(f'Threshold data will be saved to {thresholds_filepath}')

            threshold_url = f'{api_base_url}/nws_threshold'
            dpw.download_all_thresholds(thresholds_filepath, threshold_url, huc_dictionary, lid_source_dict)

            end_dt = datetime.now(timezone.utc)
            time_duration = end_dt - section_start_dt
            logging.info(f"Completed downloading thresholds - Duration: {str(time_duration).split('.')[0]}")
            print("")

        # End of pre-processing

        # ================================
        # Finish up if skip_processing is True

        if skip_processing:

            logging.info("Skipping processing as per the addition of the -sp (skip processing flag).")
            logging.info("CatFIM HUC processing and post-processing will be done independently.")

            logging.info("")
            duration_msg = sf.calculate_duration_msg(overall_start_time)
            logging.info(f"End of process_generate_categorical_fim() - {duration_msg}")
            logging.info("")

            return

        logging.info(
            f"Processing {len(valid_fim_hucs)} valid CatFIM HUCs. Note: Not all HUCs may have ahps sites."
        )

        # ================================
        # Clean old files if overwrite is True, check for leftover files if overwrite is False

        # Note:
        # Previously we had clean_up_previous_outputs() running in process_huc(),
        # where it removed the mapping and temp directory for a given HUC.
        # Moved it here because if there's an issue with removing a file,
        # it would be better to do here instead of when we are halfway through a big run.

        if overwrite:
            logging.info('Overwrite is True. Removing HUC-level mapping and temp folders, CSVs, and GPKGs.')
        else:
            logging.info(
                'Overwrite is False. Checking that HUC-level mapping and temp folders are empty and that no CSVs and GPKGs are saved.'
            )

        # Iterate through HUCs, validate inputs, and clear old outputs if needed
        for huc in valid_fim_hucs:

            # Validate inputs and get the HUC path (ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301)
            huc_path, output_folder = csf.get_huc_output_folder(huc, output_folder)

            # Create the huc folder if it does not exist
            os.makedirs(huc_path, exist_ok=True, mode=0o777)

            # Create filepath variables
            filepath_tuple = csf.make_huc_mapping_filepaths(huc, catfim_type, huc_path)

            if overwrite:
                # Delete HUC mapping and temp folders and the output CSVs and GPKGs
                clean_up_previous_huc_outputs(*filepath_tuple)

                # Check that the HUC mapping and temp folders are empty (if they exist)
                # and that there are no leftover CSVs and GPKGs
                error_msgs = check_for_previous_huc_outputs(*filepath_tuple)

                if len(error_msgs) > 0:
                    for msg in error_msgs:
                        logging.info(msg)

                    err_msg = (
                        f'{huc} - Unable to remove {len(error_msgs)} files'
                        ' This could be because of permissions or because the files are open somewhere.'
                        ' Please manually remove the mapping and temp folders and re-start CatFIM.'
                    )
                    logging.critical(err_msg)
                    raise Exception(err_msg)

            else:  # overwrite is False
                # Check that the HUC mapping and temp folders are empty (if they exist) and there are no leftover CSVs and GPKGs
                error_msgs = check_for_previous_huc_outputs(*filepath_tuple)

                if len(error_msgs) > 0:
                    for msg in error_msgs:
                        logging.info(msg)

                    err_msg = (
                        f'{huc} - {len(error_msgs)} files from a previous run were found.'
                        ' Please manually remove files, use the overwrite option, or choose a different output filename.'
                    )
                    logging.critical(err_msg)
                    raise Exception(err_msg)

        # ================================
        # Iterate over HUCs and process each one in parallel

        # Jan 2026 Notes on MP processing of HUCs: # TODO: Clean up, address if needed (TDQM question?)
        #
        # do we want a TQDM? depends on what we want to output to screen.
        # play with it a little. We recently figured out how to do both.
        # depending on what we choose to do, look at my new s3_shared_functions
        # even though it uses MT, but can be easily adjsuted to MP
        #
        # With each process_huc handing it's own logging
        # and may/may not be handing it's screen output...
        #     we may not want to use run_with_mp. TBD
        #
        #     We need to be able to have catfim_process_huc.py run completely independently in case it is
        #     running in AWS (hence.. its own log and log folder).
        #
        # But maybe we do let it all right to a common one and use run_with_mp. If we do:
        #     can the mp call a seprate py file? (like through a process_by_huc function here or somethign)
        #     right now, run_with_mp assumes one logger for all mp's so this does not work for AWS
        #     unless we come up with something else.  Maybe a None logger that catfim_process_huc can detect?
        #     or let a function in that script setup its own logger if comign through AWS?
        #
        # For now, due to debugging, just use our own process pool

        for huc in valid_fim_hucs:
            task_args_list.append({"huc": huc, "output_folder": output_folder})

        logging.info(f"Starting multi-process CatFIM HUC processing with {number_jobs} jobs.")

        failed_HUCs_list = []
        with ProcessPoolExecutor(max_workers=number_jobs) as executor:

            # Some mp functions might throw an exception, which means it may not get to as_completed
            # We still need to catch that and if so, shut down the script.
            futures_dict = [executor.submit(process_huc, **arg) for arg in task_args_list]

            for future in as_completed(futures_dict):
                # if future is not None:  # we don't have anything to return at this time.

                # Return whether the HUC-level processing was sucessful.
                if not future.exception():
                    huc, is_success = future.result()
                    if is_success is False:
                        failed_HUCs_list.append(huc)
                        logging.error(f"HUC {huc} FAILED")
                    else:
                        logging.info(f"HUC {huc} FINISHED")
                else:
                    raise future.exception()

        logging.info("Completed CatFIM HUC multiprocessing!")

        # Print number of failed HUCs
        if len(failed_HUCs_list) > 0:
            logging.error(f"{len(failed_HUCs_list)} HUCs failed. See logs for info.")

        # End of HUC multiprocessing

        # ================================
        # Run CatFIM post processing
        catfim_post_processing(output_folder)

        logging.info("")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(f"End of process_generate_categorical_fim() - {duration_msg}")
        logging.info("")

    except Exception as ex:
        trace_error = traceback.format_exc()
        err_msg = (
            f"A critical error has occurred in process_generate_categorical_fim(). Detail: {trace_error}"
        )

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # re-raise the exception, mostly for AWS
        raise ex

    finally:
        if is_logging_loaded:
            print("")
            print("Rolling up logs for process_generate_categorical_fim, HUCs, and multiproc...")
            print("")

            huc_list_rollup = [a['huc'] for a in task_args_list]
            __roll_up_final_logs(log_file_path, huc_list_rollup, output_folder)

            print("")
            print("================================")

        else:
            print("")
            print("Logging not loaded, skipping logs roll-up.")
            print("")
            print("================================")


def make_logs_path_list(main_log_path):
    '''
    Get filepaths for the generate categorical fim output logs.

    Arguments
    ---------
    main_log_path - str
        Filepath to the main log path in CatFIM outputs.

    Returns
    -------
    main_log_path - str
        Filepath to the main log path in the CatFIM outputs.
    warnings_log_path - str
        Filepath to the main warnings path in the CatFIM outputs (based on the given main log).
    errors_log_path - str
        Filepath to the main errors path in the CatFIM outputs (based on the given main log).
    '''
    warnings_log_path = main_log_path.replace(".log", "-warnings.log")
    errors_log_path = main_log_path.replace(".log", "-errors.log")

    return [main_log_path, warnings_log_path, errors_log_path]


def clean_up_previous_huc_outputs(
    output_mapping_dir,
    output_temp_dir,
    output_log_dir,
    sites_pre_mapping_file_path,
    sites_post_mapping_file_path,
    library_pre_mapping_file_path,
    library_post_mapping_file_path,
):
    '''
    Clean up specific output folders leftover from previous CatFIM runs.
    Removes the mapping folder, compiled sites and library gpkgs and CSVs.
    Was named __set_start_files_folders, renamed to __clean_up_previous_outputs for clarity.

    Arguments
    ---------
    output_mapping_dir - STR
        Filepath to the HUC-specific CatFIM mapping directory.
    output_temp_dir - STR
        Filepath to the HUC-specific CatFIM temp directory.
    log_file_dir - STR
        Filepath to the HUC-specific CatFIM log directory.
    sites_pre_mapping_file_path - STR
        Filepath to the pre-mapping sites file.
    sites_post_mapping_file_path - STR
        Filepath to the post-mapping sites file.
    library_pre_mapping_file_path - STR
        Filepath to the pre-mapping library file.
    library_post_mapping_file_path - STR
        Filepath to filepath to the post-mapping library file.

    Notes
    -----
    Files we could remove (but aren't as of now):
     - discharge_file_path
     - segments_file_path
     - huc_thresholds_file_path
     - library_pre_mapping_file_path
    '''

    # Removes these dirs if they already exist (will have gpkg's and tif's for this HUC in it)
    shutil.rmtree(output_mapping_dir, ignore_errors=True)
    shutil.rmtree(output_temp_dir, ignore_errors=True)
    # We are not removing the logs folder!!!

    # Remove HUC mapping files
    filepaths = [
        sites_pre_mapping_file_path,
        sites_post_mapping_file_path,
        library_pre_mapping_file_path,
        library_post_mapping_file_path,
    ]
    for path in filepaths:
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError as e:
                logging.critical(f"Error: Unable to remove {path} - {e}")

    return


def check_for_previous_huc_outputs(
    output_mapping_dir,
    output_temp_dir,
    output_log_dir,
    sites_pre_mapping_file_path,
    sites_post_mapping_file_path,
    library_pre_mapping_file_path,
    library_post_mapping_file_path,
):
    '''
    Checks whether certain HUC-level folders and files exists.
    This is used in the overwrite system to throw an error if
    there are files that didn't get properly deleted or in
    non-overwrite mode if there are leftover files that shouldn't
    be overwritten.


    Arguments
    ---------
    output_mapping_dir - STR
        Filepath to the HUC-specific CatFIM mapping directory.
    output_temp_dir - STR
        Filepath to the HUC-specific CatFIM temp directory.
    log_file_dir - STR
        Filepath to the HUC-specific CatFIM log directory.
    sites_pre_mapping_file_path - STR
        Filepath to the pre-mapping sites file.
    sites_post_mapping_file_path - STR
        Filepath to the post-mapping sites file.
    library_pre_mapping_file_path - STR
        Filepath to the pre-mapping library file.
    library_post_mapping_file_path - STR
        Filepath to filepath to the post-mapping library file.

    Returns
    -------
    error_msgs - list
        A list containing an error message for every file that
        was found. Returns an empty list if no files or folders
        were found.

    '''

    error_msgs = []

    # Check whether the HUC mapping dir and temp dirs are empty (error if not)
    # At this point, these two files should be empty whether or not override is selected
    if os.path.exists(output_mapping_dir) and os.path.getsize(output_mapping_dir) > 0:
        msg = f'ERROR: Mapping dir is not empty: {output_mapping_dir}'
        error_msgs.append(msg)

    if os.path.exists(output_temp_dir) and os.path.getsize(output_temp_dir) > 0:
        msg = f'ERROR: Temp dir is not empty: {output_temp_dir}'
        error_msgs.append(msg)

    # Check for these output files (should be removed if overwrite = true, or not there if overwrite = false)
    filepaths = [
        sites_pre_mapping_file_path,
        sites_post_mapping_file_path,
        library_pre_mapping_file_path,
        library_post_mapping_file_path,
    ]
    for path in filepaths:
        if os.path.exists(path):
            msg = f'ERROR: {path} already exists.'
            error_msgs.append(msg)

    return error_msgs


def __roll_up_final_logs(gen_log_file_path, huc_list_rollup, output_folder):
    '''
    Append HUC and post processing logs to final output logs.

    Copies HUC-level logs from {output_folder}/huc/{huc}/temp to {output_folder}/huc/{huc}/logs.

    Arguments
    ---------
    gen_log_file_path - str
        Filepath to the log for generate_categorical_fim.py.
    huc_list_rollup - list
        List of HUCs for which to roll up the logs.
    output_folder - str
        Filepath to the CatFIM output folder.

    '''
    try:
        msgs = []

        # Make the final log name and filepath list
        final_log_file_name = os.path.basename(gen_log_file_path).replace('gen_catfim_', 'ALL_LOGS_')

        log_folder = os.path.join(output_folder, "logs")

        # The main log folder doesn't get used until here, so if this isn't a CatFIM re-run then it
        # probably needs to be made. We store the logs in temp until this section because
        # it helps us make sure we are copying over and compiling the logs from the correct run.
        if not os.path.exists(log_folder):
            os.makedirs(log_folder, exist_ok=True)
            os.chmod(log_folder, 0o777)  # 777 (rwxrwxrwx)

        final_log_file_path = os.path.join(log_folder, final_log_file_name)

        msgs.append("")
        msgs.append(f"Compiling all logs into {final_log_file_path}")
        msgs.append("")
        msgs.append("--- Processing logs from generate_categorical_fim.py ---")

        # Make the filepath lists for the generate categorical fim output logs and the final logs
        gen_logs_path_list = make_logs_path_list(gen_log_file_path)
        final_logs_path_list = make_logs_path_list(final_log_file_path)

        # Check that the generate categorical fim output log files exist.
        for gen_log_path, final_log_path in zip(gen_logs_path_list, final_logs_path_list):
            if not os.path.exists(gen_log_path):
                with open(final_log_path, 'a'):
                    pass
                msgs.append(
                    f'WARNING: {os.path.basename(gen_log_path)} not found, made a blank file for {os.path.basename(final_log_path)}'
                )
                continue

            # If they do exist, copy the generate categorical fim output logs into the final
            # logs folder with "ALL_LOGS_" in the name. We will append the HUC and post-processing
            # logs to these "ALL_LOGS_" files.
            shutil.copyfile(gen_log_path, final_log_path)
            msgs.append(f'Copied {os.path.basename(gen_log_path)} to {os.path.basename(final_log_path)}')

        # ----------
        # Add HUC logs to the output logs

        msgs.append("")
        msgs.append("--- Processing logs from catfim_process_huc.py ---")

        if len(huc_list_rollup) == 0:
            msgs.append("No HUCs to roll up logs for.")

        # Iterate through HUCs and append the logs into the output log file (and copy the huc log file to from huc/temp to huc/logs)
        for huc in huc_list_rollup:

            # Confirm that HUC folder exists
            huc_folder = os.path.join(output_folder, "hucs", huc)
            if not os.path.exists(huc_folder):
                msgs.append(
                    f"{huc} - HUC folder not found at {huc_folder}, skipping adding HUC logs to final logs."
                )
                # Means that the HUC was in the args list but we never got far enough to
                # make a folder for it.
                continue

            # Get the HUC temp and log filepaths
            huc_temp_folder = os.path.join(huc_folder, "temp")
            huc_log_folder = os.path.join(huc_folder, "logs")

            # Exit this process if the HUC temp folder doesn't exist, because that means we didn't get very far
            # into processing for this HUC.
            if not os.path.exists(huc_temp_folder):
                msgs.append(
                    f"{huc} - HUC temp folder not found at {huc_temp_folder}, skipping adding HUC logs to final logs."
                )
                # Means that the HUC was in the args list but we never got far enough to
                # make a temp folder for it.
                continue

            # Get the most recent log file in the folder
            huc_log_file_name, num_log_files_avail = get_most_recent_log_file(huc_temp_folder, "process_huc")

            # Exit this process if the HUC log file doesn't exist, because that means we didn't get very far
            # into processing for this HUC and we don't have logs to add for this HUC.
            if huc_log_file_name is None:
                msgs.append(f"{huc} - No logs found for HUC, skipping adding HUC logs to final logs.")
                continue

            if num_log_files_avail > 1:
                msgs.append(
                    f"{huc} - {num_log_files_avail} logs available. Using most recent log: {huc_log_file_name}"
                )

            huc_log_file_path = os.path.join(huc_temp_folder, huc_log_file_name)

            if not os.path.exists(huc_log_file_path):
                msgs.append(
                    f"{huc} - HUC log not found at {huc_log_file_path}, skipping adding HUC logs (if they exist) to final logs."
                )
                continue

            huc_log_paths_list = make_logs_path_list(huc_log_file_path)

            # Iterate through log types
            for huc_log_path, final_log_path in zip(huc_log_paths_list, final_logs_path_list):

                if not os.path.exists(huc_log_path):
                    msgs.append(f'{huc} - WARNING: File not found: {os.path.basename(huc_log_path)}')
                    continue

                msgs.append(
                    f'{huc} - Appending {os.path.basename(huc_log_path)} to {os.path.basename(final_log_path)}'
                )  # TODO: Verbose? TEMP DEBUG

                # The HUC log folder doesn't get used until here, so if this isn't a CatFIM re-run then it
                # probably needs to be made. We store the HUC logs in temp until this section because
                # it helps us make sure we are copying over and compiling the logs from the correct run.
                if not os.path.exists(huc_log_folder):
                    os.makedirs(huc_log_folder, exist_ok=True)
                    os.chmod(huc_log_folder, 0o777)  # 777 (rwxrwxrwx)

                # Copy HUC log from from {huc}/temp to {huc}/logs
                huc_log_path_new = os.path.join(huc_log_folder, os.path.basename(huc_log_path))
                shutil.copyfile(huc_log_path, huc_log_path_new)

                # Append HUC .log file to gen .log file
                log_concat_success = sf.concat_files(huc_log_path, final_log_path, remove_old_src_file=False)

                # Print warning if needed
                if not log_concat_success:
                    msgs.append(
                        f'{huc} - WARNING: Unable to concat to final log: {os.path.basename(huc_log_path)}'
                    )
        # End HUC loop

        # ----------
        # Add post-processing logs to the main log
        msgs.append("")
        msgs.append("--- Processing logs from catfim_post_processing.py ---")

        # Get the most recent log file in the folder
        # log_folder = os.path.join(output_folder, "logs")
        temp_folder = os.path.join(output_folder, "temp")
        post_p_log_file_name, num_log_files_avail = get_most_recent_log_file(
            temp_folder, "catfim_post_processing"
        )

        if post_p_log_file_name is None:
            msgs.append(
                f'Postprocessing log file not found in {temp_folder}. Skipping appending post-processing logs to final logs.'
            )

        else:
            post_p_log_file_path = os.path.join(temp_folder, post_p_log_file_name)
            post_p_log_paths_list = make_logs_path_list(post_p_log_file_path)

            # Iterate through log types
            for post_p_log_path, final_log_path in zip(post_p_log_paths_list, final_logs_path_list):
                if not os.path.exists(post_p_log_path):
                    msgs.append(f'WARNING: File not found: {post_p_log_path}')
                    continue

                # Append HUC .log file to gen .log file
                log_concat_success = sf.concat_files(
                    post_p_log_path, final_log_path, remove_old_src_file=False
                )

                if log_concat_success is False:
                    msgs.append(f'Unable to concat to final log: {post_p_log_path}')

                else:
                    msgs.append(
                        f'Appended {os.path.basename(post_p_log_path)} to {os.path.basename(final_log_path)}'
                    )

        msgs.append("")
        msgs.append('Completed rolling up logs!')

    except Exception:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred while compiling logs. Detail: {trace_error}"
        msgs.append(err_msg)

    finally:
        for msg in msgs:
            print(msg)


def get_most_recent_log_file(folder, log_file_name_prefix):
    '''
    Gets the most recent log file in a folder if there are multiple.

    Shouldn't be necessary because the folders should usually just have the
    current log run, but an additional contingency to make sure we are
    getting the most recent logs for debugging purposes.

    Arguments
    ---------
    folder - str
        Filepath to the folder to get the most recent log from.
    log_file_name_prefix - str
        Prefix label of which logs to look through.

    Returns
    -------
    huc_log_file_path - str
        Filepath to the most recent log filepath.
    num_log_files_avail - int
        Number of log files that match the prefix in the folder.
    '''

    # Get files that match the prefix
    log_files_with_prefix = [f for f in os.listdir(folder) if log_file_name_prefix in f]

    # Get a list of available .log files (must not have warnings or errors)
    log_files = sorted(
        [
            f
            for f in log_files_with_prefix
            if f.endswith(".log") and "-warnings.log" not in f and "-errors.log" not in f
        ]
    )

    num_log_files_avail = len(log_files)

    if num_log_files_avail > 1:

        # Get most recent log file
        datetimes = {}
        for file_name in log_files:
            match = re.search(r'(\d{8}-\d{4})', file_name)
            if match:
                datetimes[match.group(1)] = file_name

        most_recent_datetime = sorted(datetimes.keys())[-1]
        huc_log_file_path = datetimes[most_recent_datetime]

    elif num_log_files_avail == 0:
        huc_log_file_path = None

    else:  # only one log file, which is what we expect
        huc_log_file_path = log_files[0]

    return huc_log_file_path, num_log_files_avail


def __validate_inputs(received_locals_dict):
    '''
    Validate CatFIM inputs.

    Arguments
    ---------
    received_locals_dict - dict
        Dictionary of the names and values of all local variables.

    Returns
    -------
    valid_fim_hucs - list
        If a huc_lst is provided, number of HUCs that are in the FIM outputs AND the huc_lst.
        If a huc_lst is not provided, number of HUCs that are in the FIM outputs.
    dropped_huc_lst - list
        If a huc_lst is provided, list of HUCs that are either not in the FIM outputs or not
        in the huc_lst (but available in the other).
        If a huc_lst is not provided, list will be blank.
    nwm_meta_file - str
        Filepath to the metadata pickle file.
    threshold_file - str
        Filepath to the threshold pickle file.

    '''

    # Check for main directories
    for name, value in received_locals_dict.items():
        match name:
            case "fim_run_dir":
                if not value or not os.path.exists(value):
                    raise Exception(
                        "Argument for -f (hand output pathing) is either None, empty"
                        " or the folder does not exist. Please check the argument."
                    )
            case "env_file":
                if not os.path.isfile(value):
                    raise FileNotFoundError(
                        f"The enviro file of {value} does not exist."
                        " If you have included the -e argument with a path, double check the path"
                        " or you did not include, something is wrong with bash_variables or its value."
                    )
            case "output_folder":
                if not value:
                    raise Exception("Argument for -t (output folder) can not be None or empty.")
                # We create the folder later after we append _flow_based or _stage_based
                # do we want to validate the parent pathing?

    # -----------------
    # Check if incoming HUC (or HUC list) is valid and if we have FIM data for it.
    fim_run_dir = received_locals_dict["fim_run_dir"]
    fim_hucs = [
        x
        for x in os.listdir(fim_run_dir)
        if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2']
    ]

    # -----------------
    # If a HUC list is specified, only keep the specified HUCs which have FIM data
    lst_hucs = received_locals_dict["lst_hucs"]
    if not lst_hucs:
        raise Exception("-lh list of HUC values much be the word 'all' or an actual list of HUCs")

    lst_hucs = lst_hucs.split()
    dropped_huc_lst = []
    if 'all' not in lst_hucs:
        valid_fim_hucs = [x for x in fim_hucs if x in lst_hucs]
        dropped_huc_lst = list((set(lst_hucs).difference(valid_fim_hucs)))
    else:
        valid_fim_hucs = [x for x in fim_hucs]

    num_hucs = len(valid_fim_hucs)
    if num_hucs == 0:
        raise ValueError(
            f'The number of valid hucs compared to the output directory of {fim_run_dir} is zero.'
            ' Verify that you have the correct input folder and if you used the -lh flag that it'
            ' is a valid matching HUC.'
        )

    valid_fim_hucs.sort()

    # -----------------
    # Check metadata inputs - Sort out flags and paths for getting the metadata

    # Rules:
    #    - If they used the get flag, then we assign the nwm_meta_file path so it knows where to save
    #        the file when it makes it.
    #    - If they did not use the gmf and did add a mf path, it needs to exist.
    #    - If they did not use the gmf flag and did not use the mf args, we default to bash_variables
    #        which also needs to be validated for pathing.

    get_meta_file = received_locals_dict["get_new_meta_data"]
    nwm_meta_file = received_locals_dict["nwm_meta_file"]

    if get_meta_file is True:
        # Here is the meta file that needs to be saved
        nwm_meta_file = os.path.join(received_locals_dict["output_folder"], 'nwm_metadata.pkl')

    else:  # Then we are using either a provided path or the default from bash_variables
        default_meta_file = os.getenv("nwm_meta_file")
        if nwm_meta_file == "":
            # check the path even though it came from bash_variables.
            if os.path.isfile(default_meta_file) is False:
                raise FileNotFoundError(
                    "You did not use the -mf, overridden meta file path, which means the system"
                    f" uses the default from bash_variable of {default_meta_file}."
                    " Unfortunately, that file does not exist. Check pathing, override the flag"
                    " or check the inputs directory."
                )
            nwm_meta_file = default_meta_file

        else:  # Use the path they supplied via the -mf flag
            if os.path.isfile(nwm_meta_file) is False:
                raise FileNotFoundError(
                    f"You provide a path to the meta file of {nwm_meta_file},"
                    " but that file does not exist."
                    " Check your pathing or leave the -mf argument off to use the bash_variables"
                    f" default value of {default_meta_file}."
                )

    # -----------------
    # Check threshold inputs - Sort out flags and paths for getting the threshold

    # Yes.. this script does not actually use the threshold data, but let's validate that it exists to
    #    help the catfim_process_huc.py so they have the correct path and a loaded file.

    # Rules: Same as metadata above.

    get_threshold_file = received_locals_dict["get_new_threshold_data"]
    threshold_file = received_locals_dict["threshold_file"]

    if get_threshold_file is True:
        # Here is the meta file that needs to be saved
        threshold_file = os.path.join(received_locals_dict["output_folder"], 'thresholds.pkl')

    else:  # Then we are using either a provided path or the default from bash_variables
        default_threshold_file = os.getenv("nwm_threshold_file")
        if threshold_file == "":
            # check the path even though it came from bash_variables.
            if os.path.isfile(default_threshold_file) is False:
                raise FileNotFoundError(
                    "You did not use the -tf, overridden threshold file path, which means the"
                    f"  system uses the default from bash_variable of {default_threshold_file}."
                    " Unfortunately, that file does not exist. Check pathing, override the flag"
                    " or check the inputs directory."
                )
            threshold_file = default_threshold_file

        else:  # Use the path they supplied via the -mf flag
            if os.path.isfile(threshold_file) is False:
                raise FileNotFoundError(
                    f"You provide a path to the threshold file of {threshold_file},"
                    " but that file does not exist."
                    " Check your pathing or leave the -tf argument off to use the bash_variables"
                    f" default value of {default_threshold_file}."
                )

    return valid_fim_hucs, dropped_huc_lst, nwm_meta_file, threshold_file


def __create_runtime_args_file(
    output_folder,
    env_file,
    search,
    catfim_type,
    nwm_meta_file,
    nwm_sites_file,
    get_new_meta_data,
    threshold_file,
    get_new_threshold_data,
    fim_run_dir,
    past_major_interval_cap,
):
    '''
    Create a runtime args environment file (saved as output_folder/runtime_args.env).
    This simplifies what we have to read into each function.

    Arguments
    ---------
    output_folder - str
        Filepath to the CatFIM output folder.
    env_file - str
        Docker mount filepath to the catfim environment file. Defaults to: /data/config/fim_enviro_values.env
    search - int
        Upstream and downstream search in miles. How far up and downstream of a site do
        we want to inundate? Defaults to 5.
    catfim_type - str
        Indication of whether the CatFIM run is flow-based ('fb') or stage-based ('sb').
    nwm_meta_file - str
        Filepath to the metadata pickle file.
    nwm_sites_file - str
        Filepath to the geometry file of all NWS_LID sites (from aggregate_wbd_hucs function).
    get_new_meta_data - bool
        Whether or not to download new metadata from WRDS. Defaults to False (no download).
        Only works on OWP servers, not AWS.
    threshold_file - str
        Filepath to the threshold pickle file.
    get_new_threshold_data - bool
        Whether or not to download new thresholds from WRDS. Defaults to False (no download).
        Only works on OWP servers, not Amazon AWS.
    fim_run_dir - str
        Filpath to the FIM output directory to run CatFIM on.
    past_major_interval_cap - int
        Stage-Based only. How many feet past major do you want to go for the interval FIMs?
        of the machine. Defaults to 5.

    '''

    args_file_name = "runtime_args.env"
    args_file_path = os.path.join(output_folder, args_file_name)

    if os.path.isfile(args_file_path):
        os.remove(args_file_path)

    # Open the file using standard IO, then write lines to it.
    # All of these will be validated before we get here
    with open(args_file_path, "w") as file:
        file.write(f"CATFIM_TYPE={catfim_type}\n")
        file.write(f"ENV_FILE=\"{env_file}\"\n")
        file.write(f"SEARCH={search}\n")
        file.write(f"NWM_METAFILE_PATH=\"{nwm_meta_file}\"\n")
        file.write(f"NWM_SITES_PATH=\"{nwm_sites_file}\"\n")
        file.write(f"GET_NEW_META_DATA={get_new_meta_data}\n")
        file.write(f"THRESHOLD_FILE_PATH=\"{threshold_file}\"\n")
        file.write(f"GET_NEW_THRESHOLD_DATA={get_new_threshold_data}\n")
        file.write(f"FIM_RUN_DIR=\"{fim_run_dir}\"\n")
        file.write(f"PAST_MAJOR_INTERVAL_CAP={past_major_interval_cap}\n")


if __name__ == '__main__':
    '''
    Runs the Categorical Flood Inundation Mapping (CatFIM) tool.

    Arguments
    ----------
    fim-run-dir (-f) - str
        REQUIRED: Path to directory containing HAND outputs, e.g. /data/previous_fim/hand_4_8_7_2
          or /data/outputs/test_hand_subset

    catfim_type (-ct) - str
        REQUIRED: add the value of 'fb' for Flow-Based processing or 'sb' for Stage-Based

    output_folder (-t) - str
        REQUIRED: Target location, Where the output folder will be.
          ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1.
          Note: the output folder names will have the phrase flow_based or stage_based appended

    env_file (-e) - str
        OPTIONAL: Docker mount path to the catfim environment file.
          Defaults to: /data/config/fim_enviro_values.env

    number_jobs (-j) - str
        OPTIONAL: Number HUCs to process simultaneously. Defaults to 20.

    search (-s) - int
        OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.

    lst_hucs (-lh) - str
        OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs',

    past_major_interval_cap (-mc) - int
        OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs?
          of the machine. Defaults to 5. TODO: I actually have this hard-coded elsewhere, need to reconcile!!

    nwm_meta_file (-mf)  - str
        OPTIONAL: If you have a pre-existing nwm metadata pickle file, you can path to it here.
          e.g.: /data/catfim/nwm_metafile.pkl. Default value comes from bash_variables.

    get_new_meta_data (-gmf) - flag
        OPTIONAL: Whether or not to download new metadata from WRDS.
          Only works on OWP servers, not Amazon AWS.
          and pre-existing meta file and go load new data directly from WRDS. Note: Calling WRDS
          directly means you can add filters, searching, site specific, etc. This allows for easier debugging.
          However, the default behavior is for CatFIM to use bash_variables to find and load the latest meta file.

    threshold_file (-tf) - str
        OPTIONAL: If you have a pre-existing threshold file, you can path to it here.
          Providing this manual input will prevent the WRDS API from being queried for thresholds.
          e.g.: /data/catfim/threshold_file.pkl. Default value comes from bash_variables.

    get_new_threshold_data (-gtf) - flag
        OPTIONAL: Whether or not to download new metadata from WRDS.
          Only works on OWP servers, not Amazon AWS.
          Note: Calling WRDS
          directly means you can add filters, searching, site specific, etc. This allows for easier debugging.
          However, the default behavior is for CatFIM to use bash_variables to find and load the
          latest threshold file.

    skip_processing (-sp) - flag
        OPTIONAL: If this flag is set, it will set up all of the initial 'pre-processing' steps, but will
          not continue with the processing of the hucs or post processing. This allows this tool to be used as
          either a full fun, or just do post processing and let other tools like AWS do process hucs and post processing.

    overwrite (-o) - flag
        OPTIONAL: Overwrite files.

    Example
    -------

    python /foss_fim/tools/generate_categorical_fim.py -f /data/previous_fim/fim_4_5_2_11
    -ct fb -t /data/catfim/hand_4_8_7_2 -j 20

    System defaults uses bash_variables for the default metadata and threshold files.

    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM')

    parser.add_argument(
        '-f',
        '--fim-run-dir',
        help='REQUIRED: Path to directory containing HAND outputs, e.g. /data/previous_fim/hand_4_8_7_2'
        ' or /data/outputs/test_hand_subset',
        required=True,
    )

    parser.add_argument(
        '-ct',
        '--catfim-type',
        help="REQUIRED: add the value of 'fb' for Flow-Based processing or 'sb' for Stage-Based",
        required=True,
    )

    parser.add_argument(
        '-t',
        '--output-folder',
        help='REQUIRED: Target location, Where the output folder will be.'
        ' ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1.'
        ' Note: the output folder names will have the phrase flow_based or stage_based appended',
        required=True,
    )

    # PR already incoming that changes this to /data/config/fim_enviro_values.env
    parser.add_argument(
        '-e',
        '--env-file',
        help='OPTIONAL: Docker mount path to the catfim environment file.'
        ' Defaults to: /data/config/fim_enviro_values.env',
        default="/data/config/fim_enviro_values.env",
        required=False,
    )

    parser.add_argument(
        '-j',
        '--number-jobs',
        help='OPTIONAL: Number HUCs to process simultaneously. Defaults to 20.',
        required=False,
        default=20,
        type=int,
    )

    parser.add_argument(
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.',
        required=False,
        default='5',
        type=int,
    )

    # NOTE: The HUCs you put in this, MUST be a HUC that is valid in your -f/ --fim_run_dir (HAND output folder)
    parser.add_argument(
        '-lh',
        '--lst-hucs',
        help='OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs.',
        required=False,
        default='all',
    )

    parser.add_argument(
        '-mc',
        '--past-major-interval-cap',
        help='OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs?'
        ' of the machine. Defaults to 5',  # TODO: I actually have this hard-coded elsewhere, need to reconcile!!
        required=False,
        default=5,
        type=int,
    )

    parser.add_argument(
        '-mf',
        '--nwm-meta-file',
        help='OPTIONAL: If you have a pre-existing nwm metadata pickle file, you can path to it here.'
        ' e.g.: /data/catfim/nwm_metafile.pkl. Default value comes from bash_variables.',
        required=False,
        default="",
    )

    parser.add_argument(
        '-gmf',
        '--get-new-meta-data',
        help="OPTIONAL: Whether or not to download new metadata from WRDS."
        " Only works on OWP servers, not Amazon AWS."
        " and pre-existing meta file and go load new data directly from WRDS. Note: Calling WRDS"
        " directly means you can add filters, searching, site specific, etc. This allows for easier debugging."
        " However, the default behavior is for CatFIM to use bash_variables to find and load the latest meta file.",
        required=False,
        default=False,
        action='store_true',
    )

    parser.add_argument(
        '-tf',
        '--threshold-file',
        help='OPTIONAL: If you have a pre-existing threshold file, you can path to it here.'
        ' Providing this manual input will prevent the WRDS API from being queried for thresholds.'
        ' e.g.: /data/catfim/threshold_file.pkl. Default value comes from bash_variables.',
        required=False,
        default="",
    )

    parser.add_argument(
        '-gtf',
        '--get-new-threshold-data',
        help="OPTIONAL: Whether or not to download new metadata from WRDS."
        " Only works on OWP servers, not Amazon AWS."
        " Note: Calling WRDS"
        " directly means you can add filters, searching, site specific, etc. This allows for easier debugging."
        " However, the default behavior is for CatFIM to use bash_variables to find and load the"
        " latest threshold file.",
        required=False,
        default=False,
        action='store_true',
    )

    parser.add_argument(
        '-sp',
        '--skip-processing',
        help="OPTIONAL: If this flag is set, it will set up all of the initial 'pre-processing' steps, but will"
        " not continue with the processing of the hucs or post processing. This allows this tool to be used as"
        " either a full fun, or just do post processing and let other tools like AWS do process hucs and post processing.",
        required=False,
        default=False,
        action='store_true',
    )

    parser.add_argument(
        '-o', '--overwrite', help='OPTIONAL: Overwrite files', required=False, action="store_true"
    )

    args = vars(parser.parse_args())

    # Call main program
    process_generate_categorical_fim(**args)
