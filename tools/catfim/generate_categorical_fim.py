#!/usr/bin/env python3

import argparse
import glob
import pickle
import math
import os
# import shutil
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone

# import geopandas as gpd
# import numpy as np
# import pandas as pd
from dotenv import load_dotenv

# import data.wrds.download_process_wrds 
import src.utils.shared_functions as sf
from tools.catfim.catfim_process_huc import process_huc

# from utils.shared_variables import VIZ_PROJECTION

# gpd.options.io_engine = "pyogrio"

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

    # ================================
    # Validation and setup


    local_vals = locals()
    __validate_inputs(local_vals)  # We probably should validate some of those bash_variables we are using?

    # may not even need the catfim_method as the catfim_process_huc and catfim_post_processing will now how
    # to the file names based on the type.
    # Append option configuration (flow_based or stage_based) to output folder name.
    catfim_type = catfim_type.lower()
   
    if catfim_type == "sb":
        catfim_method = "stage_based"
    else:  # fb
        catfim_method = "flow_based"
        
    # likely can merge this with the catfim_method above as nothign else shoudl use catfim_method only catfim_type
    if output_folder.endswith("/"):
        output_folder = output_folder[:-1]
    output_catfim_dir = output_folder + "_" + catfim_method
    
    os.makedirs(output_catfim_dir, exist_ok=True)
    
    log_folder = os.path.join(output_folder, "logs")
    sf.setup_file_logger(log_folder, "get_rating_curves")
    

    # If HUC list is given as an input
    if 'all' not in lst_hucs:
        print(f'HUCs to use (from input list): {valid_ahps_hucs}')

        if len(dropped_huc_lst) > 0:
            FLOG.warning('Listed HUCs not available in FIM run directory:')
            FLOG.warning(dropped_huc_lst)


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

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    FLOG.lprint("================================")
    FLOG.lprint(f"Start generate categorical fim for {catfim_method} - (UTC): {dt_string}")
    FLOG.lprint("")

    
    # do we need to load it to help sort out what HUCs are still valid for processing?
    if get_new_meta_data:
        # call new incoming data/wrds/ get wrds data tools
        # with whatever args we want
        print("Loading new meta data")
        
        # and we need the nwm_meta_file name / path
        # nwm_meta_file = obtain_wrds_data(...)  # but needs meta only in case we want to manually load threshold?
    else:
        # get it from bash_variables and ensure it exists
        print("placeholder")
    
    # if not os.path.exists(threshold_file):
    #   raise


    # do we need to load it to help sort out what HUCs are still valid for processing?
    if get_new_threshold_data:
        # call new incoming data/wrds/ get wrds data tools
        # with whatever args we want
        print("Loading new threshold meta data")
        
        # and we need the nwm_meta_file name / path
        # threshold_file = obtain_wrds_data(...)  # but needs meta only in case we want to manually load threshold?
    else:
        # get it from bash_variables and ensure it exists
        print("placeholder")

    # if not os.path.exists(threshold_file):
    #   raise
        

    # if threshold_file != "":
    #     if os.path.exists(threshold_file) == False:
    #         raise Exception("The threshold input file can not be found. Please remove or fix pathing.")
    #     file_ext = os.path.splitext(threshold_file)
    #     if file_ext.count == 0:
    #         raise Exception("The threshold input file appears to be invalid. It is missing an extension.")
    #     if file_ext[1].lower() != ".pkl":
    #         raise Exception("The threshold input file appears to be invalid. The extention is not pkl.")

    #     # Read pickle file and get a list of unique HUCs
    #     with open(threshold_file, 'rb') as f:
    #         loaded_data = pickle.load(f)

    #     hucs = loaded_data['huc'].unique().tolist()
    #     threshold_hucs= [str(num).zfill(8) for num in hucs]

    #     # Get the source (since it might be Manual_Input)
    #     data_source = loaded_data['source'].tolist()[0]
   
    #     # If a HUC list is specified, check that the HUCs in the list are also in the threshold file
    #     if 'all' not in lst_hucs:
    #         missing_hucs = [huc for huc in valid_ahps_hucs if huc not in threshold_hucs]
    #         if missing_hucs:
    #             raise Exception(
    #                 f"The following HUCs from the input list are not present in the threshold file ({threshold_file}): "
    #                 f"{', '.join(missing_hucs)}"
    #             )
    #     else:
    #         # If 'all' is specified, filter valid_ahps_hucs to only those present in the threshold file and warn about dropped HUCs
    #         filtered_hucs = [huc for huc in valid_ahps_hucs if huc in threshold_hucs]
    #         dropped_huc_lst = list(set(valid_ahps_hucs) - set(filtered_hucs))
    #         if dropped_huc_lst:
    #             FLOG.warning(
    #                 f"The following HUCs are present in the FIM run directory but not in the threshold file ({threshold_file}) and will be skipped: "
    #                 f"{', '.join(dropped_huc_lst)}"
    #             )
    #         valid_ahps_hucs = filtered_hucs
    #         num_hucs = len(valid_ahps_hucs)
    #         if num_hucs == 0:
    #             raise ValueError(
    #                 f'After filtering, the number of valid HUCs compared to the output directory of {fim_run_dir} is zero.'
    #                 ' Verify that you have the correct input folder and threshold file.'
    #             )

    # End of Validation and setup
    # ================================

    # Needed even if we are skip_processes
    __create_runtime_args_file(catfim_type,
                               env_file,
                               search,
                               nwm_meta_file,
                               threshold_file,
                               fim_run_dir,
                               past_major_interval_cap)

    if skip_processing:
        FLOG.lprint("Skipping processing as per the addition of the -sp (skip processing flag).")
        FLOG.lprint("CatFIM HUC processing and post processing will be done independently.")
        
        # Skip duration as it would have been super short
        __print_footer("End generate categorical fim", overall_start_time, False)
        return
        

    # ================================
    # Iterator for catfim_process_huc.py here
    # See various examples of possible MP systems we use.
    
    num_hucs_to_process = len(lst_hucs)
    if  num_hucs_to_process == 0:
        
        # most of these won't be needed as each HUC
        # print/log error message
        # Tell user it is being aborted

        # Skip duration as it would have been super short
        __print_footer("End generate categorical fim", overall_start_time, False)
        sys.exit(1)
            
    # will have their own of each of these possibly
    # I think all we need here is a folder named something like "hucs"
    huc_dir = os.path.join(output_catfim_dir, 'hucs')
    os.makedirs(huc_dir, exist_ok=True)  # Does this create the recurvive tree folder structure?
    # Each huc will make its own folder when it gets there.
    
    print(f"Processing {num_hucs_to_process} CatFIM HUCs")
    
    task_args_list = []    
    for huc in lst_hucs:
        task_args_list.append(
            {
                "huc": huc,
                "output_folder": output_catfim_dir,
            }
        )

    sorted_tasks_args_list = sorted(task_args_list, key=lambda x: ['huc'])
        
    FLOG.lprint(f"Processing {num_hucs} huc(s)")

    # Add MP here
        
    # setup MP
    #     process_huc()  # needs to be adjusted to ask args

    # End of mp huc processing
    
    # Important: must not have any valuse returned from each catfim_process_huc.py
    
    # do we want to iterate each HUC folder looking for the existance of its final libary file
    # and count it?  If any one HUC did not get to a final gpkg, we know it aborted or failed somehow
    # and each HUC logs / prints would have told the user why
    # Then we can show the user "x" hucs successfully processed.

    __print_footer("End generate categorical fim", overall_start_time, True)

    return


def __validate_inputs(received_locals_dict):

    # validate some of incoming inputs
    # derived values can be return if applicable or even updated values. hummm.. might be able to update them live via ** (pointers)
    # can set global variables if any
   
    for name, value in received_locals_dict.items():
        print(f"{name}: {value}")  # temp debug
        match name:
            case "fim_run_dir":
                if not value:
                    raise Exception("Argument for ")
                # if not os.path.exists(value):
                #     raise Exception
                print("placeholder")                
            case "env_file":
                # does exist, even if it was defaulted
                print("placeholder")
            case "catfim_type":
                # is name.lower == "fb" or "sb"
                print("placeholder")
            case "output_folder":
                # make sure it is not empty. make if it does not exist
                print("placeholder")
            case "lst_hucs":
                # ensure it is an array or "all" as the first element of the list?
                print("placeholder")                
            # case _: we dont' care about any others for validation
    
    # return if applicable?
   

# do we still want this? ya.. probably so we know if we still have any valid HUCs to process
# each HUC will also need to do it as well.
# Maybe a catfim_shared_functions.py file?
def __load_restricted_sites(is_stage_based):
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


def __create_runtime_args_file(output_catfim_dir,
                               env_file,
                               search,
                               catfim_type,
                               nwm_meta_file,
                               threshold_file,
                               fim_run_dir,
                               past_major_interval_cap):
    
    
    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_catfim_dir, args_file_name)
    
    # Open the file using standard IO, then write lines to it.
    # All of these will be validated before we get here
    
    # don't need output_catfim_dir as it is part of each files command args
    # ie output_catfim_dir = /data/config/hand_4_8_7_2_flow_based/  or /data/catfim/rob_test/my_test1_flow_based
    "CATFIM_TYPE"
    "ENV_FILE"
    "SEARCH"
    "NWM_METAFILE_PATH"
    "THRESHOLD_FILE_PATH"
    "FIM_RUN_DIR"
    "PAST_MAJOR_INTERVAL_CAP"


# I don't think we can make this a shared function as how would we log it?
def __print_footer(title, start_time, include_duration=True):
    
    FLOG.lprint("================================")
    FLOG.lprint(title)

    end_time = datetime.now(timezone.utc)
    dt_string = end_time.strftime("%m/%d/%Y %H:%M:%S")
    FLOG.lprint(f"Ended (UTC): {dt_string}")

    if include_duration:
        # calculate duration
        time_duration = end_time - start_time
        FLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")


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
        help='REQUIRED: Path to directory containing HAND outputs, e.g. /data/previous_fim/fim_4_5_2_11'
        ' or /data/outputs/test_hand_subset',
        required=True,
    )
    
    # PR already incoming that changes this to /data/config/fim_enviro_values.env
    parser.add_argument(
        '-e',
        '--env-file',
        help='Optional: Docker mount path to the catfim environment file. ie) data/config/catfim.env',
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
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )
    
    # Do we want to keep this as an arg? does it ever get used? or do we just hardcoded it in as a form of a global var.
    parser.add_argument(
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.',
        required=False,
        default='5',
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
        ' of the machine. Defaults to 5.0',
        required=False,
        default=5.0,
        type=float,
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
