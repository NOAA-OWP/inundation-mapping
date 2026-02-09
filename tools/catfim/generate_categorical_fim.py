#!/usr/bin/env python3

import argparse
import glob
import json
import logging
import os
import pickle
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone

from dotenv import load_dotenv

import data.wrds.download_process_wrds as dpw
import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf
from src.utils.shared_variables import VIZ_PROJECTION
from tools.catfim.catfim_post_processing import catfim_post_processing
from tools.catfim.catfim_process_huc import process_huc
from tools.tools_shared_functions import aggregate_wbd_hucs


"""
Oct/Nov 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing

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

    # TODO: Docstrings need more updating

    Note: lst_hucs argument is used but passed via locals() so VSCode thinks it is not in use.

    

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
    output_folder : str
        Base output folder for CatFIM results.
    search : int or float
        Upstream and downstream search distance in miles for site selection.
    lst_hucs : str
        Space-separated list of HUCs to process, or 'all' to process all available HUCs.
    past_major_interval_cap : int
        Cap for major interval processing (used in stage-based workflow).
    nwm_metafile : str
        Path to the NWM metadata pickle file (optional, defaults to "" if not included).
    threshold_file : str
        Path to the threshold pickle file for manual input thresholds (optional, defaults to "" if not included).
    overwrite : bool
        If True, allows overwriting existing output files and folders.


    get_new_meta_data,
    get_new_threshold_data

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
        # Note: likely can merge this with the catfim_method above as nothing else should use catfim_method only catfim_type
        if catfim_type == "sb":
            catfim_type_name = "stage_based"
        else:  # fb
            catfim_type_name = "flow_based"

        # Clean up output folder name
        if output_folder.endswith("/"):
            output_folder = output_folder[:-1]
        output_folder = output_folder + "_" + catfim_type_name


        # Validate inputs and get back validated huc list and paths for meta and threshold files
        local_vals = locals()  # lst_hucs argument is used but passed via locals() so VSCode thinks it is not in use.
        valid_fim_hucs, dropped_huc_lst, nwm_meta_file, threshold_file = __validate_inputs(local_vals)
        # Note: this will handle a huc list arg of "all". If valid_fim_hucs is empty, it will thrown an exception
        # valid_fim_hucs are hucs that have valid huc folders in the fim output dir
        # It has not yet been compared to metadata and sites

        # Make output folder
        os.makedirs(output_folder, exist_ok=True)

        # Set up logging
        log_folder = os.path.join(output_folder, "logs")
        log_file_path = sf.setup_file_logger(log_folder, "gen_catfim")
        is_logging_loaded = True

        logging.info(f"Start catfim processing for {catfim_type_name} ;  (UTC): {dt_string}")
        print("")
        print(f"... Logs will be saved to {log_file_path}")

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

        # TODO: This has duplicate headers, durations, etc from load_nwm_metadata
        # but those ones are prints. We want them logged.

        logging.info("Loading metadata and HUC dictionary")

        # Jan 2026 note: 
        # I don't think I need the meta_list in this particular script, just the huc dictionary,
        # but load_nwm_metadata loads the meta data list to calc the huc_dictionary.
        # Other scripts, do use the meta_data values.

        # Load NWM metadata from pickle or WRDS
        # Note: We do not pass in a huc list as it can miss some sites. See notes at load_nwm_metadata
        metadata_json_list, return_msgs = dpw.load_nwm_metadata(
            nwm_meta_file, api_base_url, search, get_new_meta_data, list()
        )

        # debugging # TODO: Clean up
        # meta_json_to_text = os.path.join(output_folder, "metadata_json_list_text.json")
        # with open(meta_json_to_text, 'w') as f:
        #     json.dump(metadata_json_list, f, indent=4)

        # Parse any messages returned from load_nwm_metadata
        if len(return_msgs) > 0:
            # return_msgs is a list and might have some warnings, some messages and/or errors
            # TODO: This seems a bit bumpy but good enough for now. No idea on a better answer short of
            # custom exceptions.

            # also.. we get duplicate info to the script as download_process_wrds.py has both prints
            # and returns as a message.  Hummmm. See notes in download_process_wrds.py

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
        print("")


        # ================================
        # Create HUC dictionary and NWM sites GeoDataFrame

        wbd_file = os.getenv("input_wbd_layer")  # '/data/inputs/wbd/WBD_National.gpkg'

        huc_dictionary, nwm_sites_all_gdf = aggregate_wbd_hucs(
            metadata_json_list, wbd_file, retain_attributes=True
        )
        if len(huc_dictionary) == 0:
            raise Exception("The metadata pickle file does not have any appliable HUCs")

        # Specify column data types to avoid issues when saving to gpkg
        nwm_sites_all_gdf = nwm_sites_all_gdf.astype(
            {
                'metadata_sources': str,
                'downstream_nwm_features': str,
                'upstream_nwm_features': str,
                'nwm_feature_data_downstream_feature_id': str,
                'nws_data_county_code': str,
                'nwm_feature_data_nhd_waterbody_comid': str,
                'nws_data_latitude': float,
                'nws_data_longitude': float,
                'nws_data_zero_datum': float,
                'nwm_feature_data_stream_order': str,
            }
        )
        # NOTE: These fields throw errors when saving from gdf to gpkg, throwing Skipping field because of invalid value
        # but strangely not in all records.
        # likely bad records or nulls in key which get filtered out later.
        # nwm_sites_all_gdf = nwm_sites_all_gdf.drop(['downstream_nwm_features', 'upstream_nwm_features'], axis=1, errors='ignore')

        # Save the nwm_sites_all_gdf for catfim_process_huc.py to pick up.
        # It has all sites and its huc number.
        # Each huc will make its own filtered copy, update status, etc and save at each huc level
        # for post processing rollup.
        # and has the geometry for all poitns

        nwm_sites_all_gdf = nwm_sites_all_gdf.to_crs(VIZ_PROJECTION)

        # Save a parquet version for quick loading in each HUC and 1/10th of the size
        nwm_sites_all_gdf.to_parquet(nwm_sites_file) 

        # Save a GPKG version for debugging (not shared with the HUCs)
        nwm_sites_all_gdf.to_file(nwm_sites_file.replace('.parquet', '.gpkg'), driver='GPKG', engine='fiona')

        # Change the HUC list to a simple string huc list # TODO: Clean up?
        # meta_huc_list = list(set(huc_dictionary.values()))
        # meta_huc_list = huc_dictionary.keys().sort()
        # NOTE: All HUCs in this list are validated as having hand data, plus are not on the restricted list.
        # Now I do not needs the aphs sites anymore, just unique huc list

        valid_fim_hucs.sort()

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

        # TODO: Should we get a threshold for all hucs like we do for meta? then the hucs can copy / filter
        # like meta? probably.. -> decide, eventual update to WRDS download workflow (downloading all would take more time though...) 

        if get_new_threshold_data == True:
            section_start_dt = datetime.now(timezone.utc)

            logging.info("Downloading threshold data from WRDS")

            threshold_url = f'{api_base_url}/nws_threshold'

            # label = '' # TODO: decide on whether to keep date label
            # label_with_date = dpw.label_data_file(label, lst_hucs)
            # output_thresholds_filename = f'thresholds{label_with_date}.pkl'

            output_thresholds_filename = f'thresholds.pkl'
            thresholds_filepath = os.path.join(output_folder, output_thresholds_filename)

            logging.info(f'Threshold data will be saved to {thresholds_filepath}')

            dpw.download_all_thresholds(thresholds_filepath, threshold_url, huc_dictionary)

            # Currently we download all thresholds, but we could filter the huc_dictionary to 
            # only those hucs we are processing if we wanted to speed things up. TODO: Decide

            end_dt = datetime.now(timezone.utc)
            time_duration = end_dt - section_start_dt
            logging.info(f"Completed downloading thresholds - Duration: {str(time_duration).split('.')[0]}")
            print("")



        # End of pre-processing ?


        # ================================
        # Finish up if skip_processing is True

        if skip_processing:

            logging.info("Skipping processing as per the addition of the -sp (skip processing flag).")
            logging.info("CatFIM HUC processing and post processing will be done independently.")

            # Skip duration as it would have been super short
            logging.info("End generate categorical fim processing")
            duration_msg = sf.calculate_duration_msg(overall_start_time)
            logging.info(duration_msg)

            return

        logging.info(
            f"Processing {len(valid_fim_hucs)} valid CatFIM HUCs. Note: not all HUCs may have ahps sites."
        )

        # ================================
        # Clean old files if overwrite is True # TODO: need to implement


        # TODO: Cleaning old files: remove all content in huc folders, EXCEPT their log files. Discuss - is this referring to cleaing out files from previous runs?
        # With us later scanning for files and file extensions, we may not want to be pulling in old bad HUCs.
        # or... do we. maybe we had some good HUCs that were left behind. Do we just let it pull them in?
        # hummmm
        # OR.. do we just look for all of the final files from HUCs such as it's final sites and library file and
        # remove them?  hummmm
        # Maybe we can just remove all of their "mapping" folders
        # What about override? do we even need it? or maybe we just check for override of the final post
        # processing files before we start?
        # What if we leave it all alone other than killing the final rolled up outputs. That way
        # someone could re-run a huc, then re-run post processing and it will pick it up from all folders
        # hummmm



        # # Emily.. for your testing let it stop here for testing gen_catfim # TODO: Remove later
        # # also add a system to abort after saving a huclist so it does not continue to iterate
        # # or run post processing. In EC2's, we do not want an early (pre-processing) abort as we do want
        # # it to iterate and hit post  processing.
        # # BUT.... in AWS, Step Functions will take care of the huc ietartion and post processing.
        # print("stop here in gen catfim for now")
        # sys.exit(0)



        # ================================
        # Iterate over HUCs and process each one in parallel

        # Jan 2026 Notes on MP processing of HUCs:
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

        task_args_list = []
        for huc in valid_fim_hucs:
            task_args_list.append({"huc": huc, "output_folder": output_folder})

        logging.info(f"Starting multi-process CatFIM HUC processing with {number_jobs} jobs.")

        # Jan 2026 Notes: We do not need anythign back at this point, only to know catch a fail but never shut down the thread
        # So we dont' even need a futures. what about CTRL-C? 
        failed_HUCs_list = []
        with ProcessPoolExecutor(max_workers=number_jobs) as executor:

            # Some mp functions might throw an exception, which means it may not get to as_completed
            # We still need to catch that and if so, shut down the script.
            futures_dict = [executor.submit(process_huc, **arg) for arg in task_args_list]

            # Need Try, except but need some combinations of exceptions, controlled errors and CTRL-C (aborts)
            # or do we?

            for future in as_completed(futures_dict):
                # if future is not None:  # we don't have anything to return at this time.
                
                if not future.exception():
                    huc, is_success = future.result()
                    if is_success is False:
                        failed_HUCs_list.append(huc)
                        logging.error(f"huc {huc} failed")
                    else:
                        logging.print(f"huc {huc} success")
                else:
                    raise future.exception()
                
            # TODO: At a min.. use as_completed to catch
            # catestrophic errors where we want to shut down the MP
            # (inc CTRL-C which may be more than one)

        logging.info("Completed multi-process CatFIM HUC processing.")
        
        if len(failed_HUCs_list) > 0:
            logging.error("show a list or someting of what failed.")  #failed_HUCs_list

        print("stop here in gen catfim - right before post processing - for now")
        sys.exit(0)

        # End of mp huc processing

        # ================================
        # Run CatFIM post processing
        catfim_post_processing(output_folder)

        logging.info("End generate categorical fim processing")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)

    except Exception as ex:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred performing post processing. Detail: {trace_error}" # TODO: should we change post processing to regular processing here?

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # re-raise the exception, mostly for AWS
        raise ex
    
    finally:
        # TODO: combine this file's logs, compiled HUC logs, and post-processing logs into log file here
        # - all errors file
        # - all warnings file

        print("Rolling up logs for process_generate_categorical_fim, HUCs, and multiproc")



def __validate_inputs(received_locals_dict):
    """
    Validate CatFIM inputs. 

    """

    # Let's check simple validation stuff.
    for name, value in received_locals_dict.items():
        # print(f"{name}: {value}")  # temp debug
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

            # case _: we dont' care about any others for validation (other than the ones below)

    # -----------------
    # Check if incoming HUC (or HUC list) is valid and if we have fim data for it.
    fim_run_dir = received_locals_dict["fim_run_dir"]
    fim_hucs = [
        x
        for x in os.listdir(fim_run_dir)
        if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2']
    ]

    # -----------------
    # If a HUC list is specified, only keep the specified HUCs which have fim data
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
    """
    Create a runtime args environment file (saved as output_folder/runtime_args.env).
    This simplifies what we have to read into each function.
    """

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
    Sample mins args:
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
        ' Note: the output folder names will have the phase flow_based or stage_based appended',
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
        help='OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs',
        required=False,
        default='all',
    )

    parser.add_argument(
        '-mc',
        '--past-major-interval-cap',
        help='OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs?'
        ' of the machine. Defaults to 5',
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
        help="OPTIONAL: If this argument is added, and this script is on a OWP server, then ignore"
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
        help='OPTIONAL: If you have a pre-existing threshold file, you can path to it here. '
        'Providing this manual input will prevent the WRDS API from being queried for thresholds.'
        ' e.g.: /data/catfim/threshold_file.pkl. Default value comes from bash_variables.',
        required=False,
        default="",
    )

    parser.add_argument(
        '-gtf',
        '--get-new-threshold-data',
        help="OPTIONAL: If this argument is added, and this script is on a OWP server, then ignore"
        " and pre-existing threshold data file and go load new data directly from WRDS. Note: Calling WRDS"
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
        help="OPTIONAL: If this flag is set, it will setup all of the initial 'pre-processing' steps, but will"
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

    # call main program
    process_generate_categorical_fim(**args)
