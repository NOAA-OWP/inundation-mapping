#!/usr/bin/env python3
 
import os
import argparse
import logging
import shutil
import traceback

from datetime import datetime, timezone
from dotenv import load_dotenv

import pandas as pd

import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf

# Global variable  (some shortcuts from env files)
CATFIM_TYPE=""
ENV_PATH=""
SEARCH=""
FIM_RUN_DIR=""
PAST_MAJOR_INTERVAL_CAP=""
HUC_PATH=""
OUTPUT_FOLDER=""
NWM_METAFILE_PATH=""
GET_NEW_META_DATA=""  # will be string value of 'True' or 'False'
THRESHOLD_FILE_PATH=""
GET_NEW_THRESHOLD_DATA=""  # will be string value of 'True' or 'False'

"""_summary_

    A sample model HUC folder can be found at /....(data)/catfim/rob_tests/new_arc_test1_flow_based
      - it has a single sites files which combines all of the attribute files for each site into its own huc file
        updateing it as it is being processed. 
      - may / may not have one HUC level master or split level threashold / discharge data ??
      
      - It also can be using the mapped / status colums as it goes
      
      - When the huc is finished be processed, it's output files sit ready for post processing to merge with the rest of the huc files.


    Overall processing steps (tenatively)
    
    Will call generate_categorical_fim_flows and generate_categorical_fim when applicable.

    4: Start a folder structure if not already in place

    1: Start up its own non-shared log system

    1.b: load the runtime_arg.env
        
    2: validate the huc is valid and applicable to catfim ??
    
    3: Get list of applicable, valid sites for this HUCs?  from where? master sites metadata or site file?
       Watching for excluded sites from restricted sites csv.
    
    5: Create its own sites csv. Populate what we know if anything and continue updating throughout
       processing steps including mapping flags and status data.
       
    6: Load its own metadata, threshold data and flow data, if applicable using shared various files.
    
    7: Various meta and threshold processing? including validation of data ?
    
    8: Figure out stages and if SB also figure out stages.
    
    9: Data adjustments or rejections ? (might be higher or even need more here)
    
    10: If FB, Load branch and HAND data? (rems and hydrotables), liekly all done via inundation scripts
    
    11: Create inundation tifs if applicable and roll them up if branch tifs?
        FB: Call inundation.py ?
        SB: Do our own inundation like we currently do?
    
    12: make extent polys
    
    13: Finalize any data
    
    14: Make final library files for this HUC
    
"""
    
def process_huc(huc, output_folder):
  
    is_logging_loaded = False
    
    # load our standard bash_variables.env
    # Is there any bash_variables needed? 
    load_dotenv('/foss_fim/src/bash_variables.env')

    __validate_inputs(huc, output_folder)  # also validates some bash_variables if it needs any.

    try:

        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

        print("================================")

        # ---------------------
        # load the runtime_args.env, error if it does not exist. It should give us all values we need
        # See generate_categorical_fim.py -> save_env_args(output_path)
        # We will also do some validation in it as well.
        __load_runtime_args(output_folder)
        
        # Validate that we have that as a HUC in the fim_dir. 
        # Helping sort out if even a valid HUC was submitted
        
        catfim_type_name = ""
        if CATFIM_TYPE == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        print(f"Start generate {catfim_type_name} catfim fim for HUC: {huc} ;  (UTC): {dt_string}")
        print("")

        output_mapping_dir = os.path.join(HUC_PATH, "mapping")
        discharge_file_path, sites_file_path, library_file_path = __set_start_files_folders(output_mapping_dir)

        # ---------------------
        # Setup logging. It should make its own huc log folder inside the parent "logs" folder
        log_file_dir = os.path.join(HUC_PATH, "logs")
        log_file_path = sf.setup_file_logger(log_file_dir, f"{huc}_logs")
        print(f"  Logs will be saved to {log_file_path}")

        # ---------------------
        # Load meta data here then we can also double check if there are any valid ones to 
        # for the output_catfim_dir to get_meta_and_huc_data, we don't want to have it save
        # a pickle file if applicable and that function uses that variable only for saving 
        # applicable pickle files.
        
        # Why not save it?  pre-processing would have saved a pickle file for all HUCs

        # huc_dictionary will have one or more items, one per applicable site
        # ie:  12090301: stat1, 12090301: nybc1
                                                 
        huc_dictionary, meta_gdf = csf.get_meta_and_huc_data("",
                                                             SEARCH,
                                                             SEARCH,
                                                             NWM_METAFILE_PATH,
                                                             GET_NEW_META_DATA,
                                                             [huc],
                                                             ENV_PATH)
        if len(huc_dictionary) == 0:
            msg = f"HUC number of {huc} is invalid or does not have any nwm sites associated to it"
            logging.critical(msg)
            raise Exception(msg)

        # make a simple list of just the site_ids
        huc_nws_lids = list[set(huc_dictionary.values())]
        
        # ---------------------
        # Start building up the new sites file. We can adjust the status as we go.

        # ---------------------
        # Get list of applicable sites, valid sites for this HUCs from master sites metadata
        #   Watching for excluded sites from restricted sites csv.
        df_restricted_sites = __load_restricted_sites()

        # Check whether the LIDs is in the restricted sites list
        nwm_lids = []
        for lid in huc_nws_lids:
            is_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]
            if not is_restrict_lid:
                nwm_lids.append(lid)

        if len(nwm_lids) == 0:
            msg = f"All sites associated to HUC {huc} are retricted. No more processing will continue"
            logging.critical(msg)
            raise Exception(msg)

        # ---------------------
        # recheck if the HUC is valid and has valid apps sites. Log and abort it no sites
        # left to process or HUC is invalid. 
        
        # Check if huc exists in the FIM_RUN_DIR and has branches. (jsut in case it was a HUC that failed
        # in the HUC run. We also might have an invalid HUC passed in here if this file was called directly
        # from command line.
        # We will need to repeat most of the validating from generate_categorical_fim.py.
        # why? if this started up via command line or part of the generate_categorical_fim.py MP.
        
        # ---------------------
        # validate HUC and if it is applicable to CatFIM?
        # - does it has flow data in FIM_RUN_DIR?
        #    - does it have threshold data in the THRESHOLD_FILE_PATH?

       
        # Update the new sites file for this HUC for the status here.
        
        # How do we figure out if there are any sites left to process?
        
        # ---------------------       
        # threshold data and flow data, if applicable using shared various files.
        
        # ---------------------    
        # Various meta and threshold processing? including validation of data ?

        # ---------------------
        # Create its own sites csv. Populate what we know if anything and continue updating throughout
        # processing steps including mapping flags and status data.
        
        # ---------------------
        # ? When / how do the points get added to sites?
        
        # ---------------------    
        # Figure out categories. (ie.. action, moderate, etc) - SB to also figure out intervals?
        
        # ---------------------    
        # Data adjustments or rejections ? (might be higher or even need more here)
        
        # ---------------------    
        # If FB, Load branch and HAND data? (rems and hydrotables), liekly all done via inundation scripts
        
        # ---------------------    
        # Create inundation tifs if applicable and roll them up if branch tifs?
        #    FB: Call inundation.py ?
        #     SB: Do our own inundation like we currently do?
        
        # ---------------------    
        # Make extent polys
        
        # ---------------------    
        # Finalize any data
        
        # ---------------------    
        # Make final library files for this HUC
        
        
        logging.info(f"End processing for huc {huc}")
        sf.print_andor_log_duration(overall_start_time, True, True, logging.getLogger())
        
        # nothing to return as of now
        # but generate_categorical_fim.py can if it has value to return.
        # if you use "return" and it is AWS, it will not error out.
        # yes.. we want a "return", but may/may not have a value. 
        # if we add one, keep it a simple data type (str, int, float)
        return
        
        
    except Exception:
        trace_error = traceback.format_exc()
        
        err_msg = f"A critical error has occurred while processing {huc}. Detail: {trace_error}"
        
        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)
        
        # do we re-throw the error? gcf, aws, or cmd line? hummm


def __load_restricted_sites():
    """
    Previously, only stage based used this. It is now being used by stage-based and flow-based (1/24/25)

    The 'catfim_type' column can have three different values: 'stage', 'flow', and 'both'. This determines
    whether the site should be filtered out for stage-based CatFIM, flow-based CatFIM, or both of them.

    Returns: a dataframe for the restricted lid and the reason why:
        'nws_lid', 'restricted_reason', 'catfim_type'
    """

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


            # FLOG.warning(f"{restricted_reason}. Lid is '{nws_lid}'")            
            # Humm.. how do we log this? screen is ok, but log isn't (MP versus non MP)
            # can we try just using the "logging" instance? Let's try it and see what happens
            logging.warning(f"{restricted_reason}. Lid is '{nws_lid}'")     
                        
        continue
    # end loop

    # Filter df_restricted_sites by CatFIM type
    if CATFIM_TYPE == 'sb':  # Keep rows where 'catfim_type' is either 'stage' or 'both'
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['stage', 'both'])]

    else:
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['flow', 'both'])]

    # Remove catfim_type column
    df_restricted_sites.drop('catfim_type', axis=1, inplace=True)

    return df_restricted_sites


def __validate_inputs(huc, output_folder):

    global HUC_PATH, OUTPUT_FOLDER
    
    # TODO: valdiate huc value (8 numeric maybe and starts with 0, 1, or 2)
    
    
    if not output_folder:  # exists or empty
        raise ValueError("output_folder argument can not be None or empty.")
    if output_folder.endswith("/"):  # strip it off the end
        output_folder = output_folder[:-1]

    # does it already have the subfolder of "hucs"? strip it for now temporarily
    if output_folder.endswith("hucs"):
        output_folder = output_folder[:-4]

    if not os.path.exists(output_folder):  # the hucs subfolder may/may not exist but the root output folder must
        raise ValueError(f"output_folder of {output_folder} does not exist."
        " Please check pathing including case.")

    OUTPUT_FOLDER = output_folder
    # ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301
    HUC_PATH = os.path.join(OUTPUT_FOLDER, "hucs", huc)
    os.makedirs(HUC_PATH, exist_ok=True)
    
    # No need to validate any of the runtime_args as they were validated when it was created.
    
    # return any newly created values based on inputs if any. I don't see any at this time.


def __load_runtime_args(output_folder):
    
    # these are just shortcuts from os.getenv
    global CATFIM_TYPE, ENV_PATH, SEARCH, FIM_RUN_DIR, PAST_MAJOR_INTERVAL_CAP
    global NWM_METAFILE_PATH, GET_NEW_META_DATA, THRESHOLD_FILE_PATH, GET_NEW_THRESHOLD_DATA
    
    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)
    
    if not os.path.exists(args_file):
        raise ValueError(f"Unable to find the runtime_args.env at {output_folder}")
    
    # use load_env, and pull out just the variables it needs.
    load_dotenv(args_file)
    
    CATFIM_TYPE = os.getenv('CATFIM_TYPE')
    SEARCH = os.getenv('SEARCH')
    NWM_METAFILE_PATH = os.getenv('NWM_METAFILE_PATH')
    THRESHOLD_FILE_PATH = os.getenv('THRESHOLD_FILE_PATH')
    FIM_RUN_DIR = os.getenv('FIM_RUN_DIR')  # ie: /data/previous_fim/hand_4_8_7_2 or other
    PAST_MAJOR_INTERVAL_CAP = os.getenv('PAST_MAJOR_INTERVAL_CAP')
    GET_NEW_META_DATA = os.getenv('GET_NEW_META_DATA')  # string value of 'True' or 'False'
    GET_NEW_THRESHOLD_DATA = os.getenv('GET_NEW_THRESHOLD_DATA')  # string value of 'True' or 'False'
        
    
    # return
    
        
def __set_start_files_folders(output_mapping_dir):

    # Note: all key other variables have already been validated

    # ================================
    # CLEANUP
    # Remove all files / folders except anything in the log folder, we keep that one only.
    # remove discharge_file_name, sites_file_name, libary_file_name if they already exist
    discharge_file_path = os.path.join(HUC_PATH, "discharge_values.csv")
    if os.path.isfile(discharge_file_path):
        os.remove(discharge_file_path)

    sites_file_path = os.path.join(HUC_PATH, "sites.gpkg")
    if os.path.isfile(sites_file_path):
        os.remove(sites_file_path)

    library_file_path = os.path.join(HUC_PATH, "library.gpkg")
    if os.path.isfile(library_file_path):
        os.remove(library_file_path)

    if os.path.exists(output_mapping_dir):  # already exists, remove it
        shutil.rmtree(output_mapping_dir, ignore_errors=True)
    os.mkdir(output_mapping_dir)

    # Always keeps the logs folder
    
    return discharge_file_path, sites_file_path, library_file_path


if __name__ == '__main__':

    '''
    Sample
    python /foss_fim/tools/catfim/catfim_process_huc.py -u 12090301 -t /data/catfim/hand_4_8_7_2
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM for a HUC')
    args = vars(parser.parse_args())

    # Most args will be in the runtime_arg.env created in the generate_categorical_fim.py
    # This script will already know where to look for the runtime_args.env file

    # We need only the huc number and the output path for args
        
    parser.add_argument("-u", "--huc", help="REQUIRED: HUC8 Number", required=True)    

    parser.add_argument(
        '-t',
        '--output-folder',
        help='REQUIRED: Target location, Where the output folder will be.'
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )

    args = vars(parser.parse_args())

    process_huc(**args)
