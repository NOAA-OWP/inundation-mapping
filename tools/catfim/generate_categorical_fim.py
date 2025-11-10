#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import traceback
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone

import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf
from tools.catfim.catfim_process_huc import process_huc
from tools.catfim.catfim_post_processing import catfim_post_processing

"""
Oct/Nov 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing

Tenative notes:
    - This script will fundamentally play the role stricly as pre-processing for processing HUC and their
      related sites.
      
    - Some of the functions in here may move or be split to smaller functions.
    
    - Data acquision such as meta, threshold or flows, should be moved ot generate_categorical_fim_flows.py
    
    - Anyting related to inundation, tifs, gpkgs, etc, shoudl be moved to generate_categorical_mapping.py
    
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


"""
Jun 17, 2024
This system is continuing to mature over time. It has a number of optimizations that can still
be applied in areas such as logic, performance and error handling.

In the interium there is still a consider amount of debug lines and tools embedded in that can
be commented on/off as required.


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
):
# Notes: lst_hucs argument is used but passed via locals() so VSCode thinks it is not in use.

    '''
        
    # TODO: Needs more updating
    
    
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
    - Handles both manual and automated threshold input via `threshold_file`.
    - Uses environment variables for API access and configuration.
    - Designed for parallel processing and scalable workflows.

    '''

    is_logging_loaded = False   

    try:
        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")
        print("================================")

        # ================================
        # Validation and setup
        # Note: lst_hucs argument is used but passed via locals() so VSCode thinks it is not in use.
        local_vals = locals()
        # this will handle a huc list arg of "all"
        valid_fim_hucs, dropped_huc_lst = __validate_inputs(local_vals)
        # We probably should validate some of those bash_variables we are using? (some paths?)

        if catfim_type == "sb":
            catfim_type_name = "stage_based"
        else:  # fb
            catfim_type_name = "flow_based"

        # likely can merge this with the catfim_method above as nothign else shoudl use catfim_method only catfim_type
        if output_folder.endswith("/"):
            output_folder = output_folder[:-1]
        output_folder = output_folder + "_" + catfim_type_name

        os.makedirs(output_folder, exist_ok=True)

        print(f"Start catfim processing for {catfim_type_name} ;  (UTC): {dt_string}")
        print("")

        log_folder = os.path.join(output_folder, "logs")
        log_file_path = sf.setup_file_logger(log_folder, "gen_catfim")
        print(f"  Logs will be save to {log_file_path}")
        is_logging_loaded = True

        # Needed even if we are skip_processes
        __create_runtime_args_file(output_folder,
                                   env_file,
                                   search,
                                   catfim_type,                                   
                                   nwm_meta_file,
                                   get_new_meta_data,
                                   threshold_file,
                                   get_new_threshold_data,
                                   fim_run_dir,
                                   past_major_interval_cap)

        if len(dropped_huc_lst) > 0:
            logging.warning('Listed HUCs not available in FIM run directory:')
            logging.warning(dropped_huc_lst)

        print("Let's stop here for a test")
        sys.exit(0)

        # valid_fim_hucs has already been validate to have at least one by this point ???
        # For now, we get the meta gdf, but don't need it here.
        
        # "search" : Define upstream and downstream search in miles
        huc_dictionary, _ = csf.get_meta_and_huc_data(output_folder,
                                                      search,
                                                      nwm_meta_file,
                                                      get_new_meta_data,
                                                      valid_fim_hucs)
        
        if len(huc_dictionary) == 0:
            raise Exception("The submitted huc list did not find any HUCs with nwm site meta data")

        # Change it to a simple string huc list.
        # All HUCs in this list are validated as having hand data, plus are not on the restricted list.
        huc_list = list[set(huc_dictionary.keys())]  # ie, 12090301, 05030201. using 'set' fixes uniqueness.
        huc_list.sort()
       
        # AWS will need this list to know what HUCs to process and iterate
        catfim_huc_list_file = os.path.join(output_catfim_dir, "catfim_huc_list.txt")
        with open(catfim_huc_list_file, "w") as f:
            for item in huc_list:
                f.write(f"{item}\n")
        
        # End of Validation and setup
        # ================================

        # Each huc has their own independent self-encapsulated folder under the "hucs" folder.
        # ie) /data/catfim/my_test_flow_based/hucs/12090301
        huc_dir = os.path.join(output_catfim_dir, 'hucs')
        os.makedirs(huc_dir, exist_ok=True)

        if skip_processing:
            
            logging.info("Skipping processing as per the addition of the -sp (skip processing flag).")
            logging.info("CatFIM HUC processing and post processing will be done independently.")
            
            # Skip duration as it would have been super short
            logging.info("End generate categorical fim processing")
            sf.print_andor_log_duration(overall_start_time, True, True, logging.getLogger())
            return

        num_hucs_to_process = len(huc_dir)
        logging.info(f"Processing {num_hucs_to_process} CatFIM HUCs. Note: not all may have ahps sites.")

        task_args_list = []    
        for huc in huc_list:
            task_args_list.append(
                {
                    "huc": huc,
                    "output_folder": output_folder,
                }
            )
        sorted_tasks_args_list = sorted(task_args_list, key=lambda x: ['huc'])
            

        # === Run jobs in parallel ===
        # Setup some sort of processpool
        # do we want a TQDM? depends on what we want to output to screen.
        # play with it a little. We recently figured out how to do both.
        # depending on what we choose to do, look at my new s3_shared_functions
        # even though it uses MT, but can be easily adjsuted to MP

        # With each process_huc handing it's own logging and may/may not be handing it's screen output
        # we may not want to use run_with_mp. TBD

        with ProcessPoolExecutor(max_workers=number_jobs) as executor:

            # Some mp functions might throw an exception, which means it may not get to as_completed
            # We still need to catch that and if so, shut down the script.
            futures_dict = [executor.submit(process_huc, **arg) for arg in sorted_tasks_args_list]

            # Need Try, except but need some combinations of exceptions, controlled errors and CTRL-C (aborts)
            
            # for future in as_completed(futures_dict):
            #    if future is not None:  # we don't have anything to return at this time.
                    # if not future.exception():
                    #     failed_huc = future.result()
                    #     if failed_huc != "":
                    #         failed_HUCs_list.append(failed_huc)
                    # else:
                    #     raise future.exception()
            # TODO: At a min.. use as_completed to catch
            # catestrophic errors where we want to shut down the MP
            # (inc CTRL-C which may be more than one)

        # End of mp huc processing

        catfim_post_processing(output_folder)

        logging.info("End generate categorical fim processing")
        sf.print_andor_log_duration(overall_start_time, True, True, logging.getLogger())

    except Exception as ex:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred performing post processing. Detail: {trace_error}"
        
        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)
            
        # re-raise the exception, mostly for AWS
        raise ex


def __validate_inputs(received_locals_dict):

    # validate some of incoming inputs
    # derived values can be return if applicable or even updated values. hummm.. might be able to update them live via ** (pointers)
    # can set global variables if any
        
    print("---------------")
    print("debugging __validate_inputs")
          
    for name, value in received_locals_dict.items():
        # print(f"{name}: {value}")  # temp debug
        match name:
            case "fim_run_dir":
                if not value or not os.path.exists(value):
                    raise Exception("Argument for -f (hand output pathing) is either None, empty"
                                    " or the folder does not exist. Please check the argument")
            case "env_file":
                if not value:
                    raise Exception("This tool relys on being able to load an enviro file either by"
                                    " default or by an explicit argument.")
                if not os.path.exists(value):
                    raise Exception("This tool relys on being able to load an enviro file either by"
                                    f" default or by an explicit argument. Path value is {value}")
            case "catfim_type":
                if value not in ["fb", "sb"]:
                    raise Exception("Argument for -ct (catfim type) must be either fb (for flow based)"
                                    " or sb (for stage based)")
            case "output_folder":
                if not value:
                    raise Exception("Argument for -t (output folder) can not be None or empty")
                # We create the folder later after we append _flow_based or _stage_based
                # do we want to validate the parent pathing?
                
            # case _: we dont' care about any others for validation

    # check if incoming HUC (or HUC list) is valid and we have fim data for it.
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
    
    print("---------------")    
    
    return valid_fim_hucs, dropped_huc_lst
   

def __create_runtime_args_file(output_folder,
                               env_file,
                               search,
                               catfim_type,
                               nwm_meta_file,
                               get_new_meta_data,
                               threshold_file,
                               get_new_threshold_data,
                               fim_run_dir,
                               past_major_interval_cap):
        
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
        file.write(f"GET_NEW_META_DATA={get_new_meta_data}\n")
        file.write(f"THRESHOLD_FILE_PATH=\"{threshold_file}\"\n")
        file.write(f"GET_NEW_THRESHOLD_DATA={get_new_threshold_data}\n")
        file.write(f"FIM_RUN_DIR=\"{fim_run_dir}\"\n")
        file.write(f"PAST_MAJOR_INTERVAL_CAP={past_major_interval_cap}\n")


if __name__ == '__main__':

    '''
    Sample mins args:
    python /foss_fim/tools/generate_categorical_fim.py -f /data/previous_fim/fim_4_5_2_11
    -ct fb -t /data/catfim/hand_4_8_7_2

    Note... you likely want to always use the 'j' (number of jobs flag) which defaults to 1.
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
        help='OPTIONAL: Number of processes to use for HUC scale operations.'
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count of the'
        ' machine. CatFIM sites generally only have 2-3 branches overlapping a site, so this number can be '
        'kept low (2-4). Defaults to 1.',
        required=False,
        default=1,
        type=int,
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
    
    # Do we want to keep this as an arg? does it ever get used? or do we just hardcoded it in as a form of a global var.
    parser.add_argument(
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.',
        required=False,
        default='5',
        type=int,
    )

    # NOTE: The HUCs you put in this, MUST be a HUC that is valid in your -f/ --fim_run_dir (HAND output folder)
    # Keep this as is
    parser.add_argument(
        '-lh',
        '--lst-hucs',
        help='OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs',
        required=False,
        default='all',
    )

    # Keep this
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
         action='store_true'
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
         action='store_true'
    )
    
    parser.add_argument(
        '-sp',
        '--skip-processing',
        help="OPTIONAL: If this flag is set, it will setup all of the initial 'pre-processing' steps, but will"
        " not continue with the processing of the hucs or post processing. This allows this tool to be used as"
        " either a full fun, or just do post processing and let other tools like AWS do process hucs and post processing.",
         required=False,
         default=False,
         action='store_true'
    )        

    args = vars(parser.parse_args())

    # call main program
    process_generate_categorical_fim(**args)
