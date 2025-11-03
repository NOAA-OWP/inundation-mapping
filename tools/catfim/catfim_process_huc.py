#!/usr/bin/env python3
 
import os
import argparse
import logging

from datetime import datetime, timezone
from dotenv import load_dotenv


# Global variable  (some shortcuts from env files)
CATFIM_TYPE=""
NWM_METAFILE_PATH=""
THRESHOLD_FILE_PATH=""
FIM_RUN_DIR=""
PAST_MAJOR_INTERVAL_CAP=""
HUC_PATH=""


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

    """_summary_
    
        Arguments:
        - huc: string (so we don't have problems with zero padding)
        - output_folder
            ie) /data/catfim/my_test_flow_based/hucs
    """
   

    # load our standard bash_variables.env
    # Is there any bash_variables needed? 
    load_dotenv('/foss_fim/src/bash_variables.env')

    __validate_inputs(huc, output_folder)  # also validates some bash_variables if it needs any.

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    FLOG.lprint("================================")
    FLOG.lprint(f"Start generate categorical fim for {catfim_method} - (UTC): {dt_string}")
    FLOG.lprint("")


    # ---------------------
    # Setup logging. It should make its own huc log folder inside the parent "logs" folder
    # log even if any other validation occurs after __validate_inputs.


    # ---------------------
    # recheck if the HUC is valid and has valid apps sites. Log and abort it no sites
    # left to process or HUC is invalid. 
    
    # Check if huc exists in the FIM_RUN_DIR and has branches. (jsut in case it was a HUC that failed
    # in the HUC run. We also might have an invalid HUC passed in here if this file was called directly
    # from command line.
    # We will need to repeat most of the validating from generate_categorical_fim.py.
    # why? if this started up via command line or part of the generate_categorical_fim.py MP.



    # ---------------------
    # Start a folder structure if not already in place ?? Do we want all of these?
    
    output_mapping_dir = os.path.join(HUC_PATH, 'mapping')
    
    # get rid of all files, etc previous log files.
    discharge_file = os.path.join(HUC_PATH, "discharge_values.csv")
    sites_file = os.path.join(HUC_PATH, "sites.csv")
    libary_file = os.path.join(HUC_PATH, "sites.csv")
    
   
    

    try:
        # ---------------------
        # load the runtime_args.env, error if it does not exist. It should give us all values we need
        # See generate_categorical_fim.py -> save_env_args(output_path)
        load_runtime_args(output_folder)
       
       
       
        # if CATFIM_TYPE == 'sb':
            # print("Processing stage based data")        
        # else
            # print("Processing flow based data")

        
        # ---------------------
        # validate HUC and if it is applicable to CatFIM?
        # - does it has flow data in FIM_RUN_DIR?
        #    - does it have threshold data in the THRESHOLD_FILE_PATH?

        # ---------------------
        # Get list of applicable sites, valid sites for this HUCs from master sites metadata
        #   Watching for excluded sites from restricted sites csv.
        
        # ---------------------       
        # Load its own metadata, threshold data and flow data, if applicable using shared various files.
        
        # ---------------------    
        # Various meta and threshold processing? including validation of data ?

        # ---------------------
        # Create its own sites csv. Populate what we know if anything and continue updating throughout
        # processing steps including mapping flags and status data.
        
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
        
        # Do not return anything
        
    except Exception as ex:
        print("Placeholder")
        
        # what if logging is not set up yet? It would be great to log everything.
        # but hard to do if huc is not validated, and logger created. HUMMMM.


def __validate_inputs(huc, output_folder):

    global HUC_PATH
    # valdiate huc value (8 numeric maybe and starts with 0, 1, or 2

    # validate the outfolder path exists first


    # ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301
    HUC_PATH = os.path.join(output_folder, "hucs", huc)
    # make path if not already there
    
    # No need to validate any of the runtime_args as they were validated when it was created.
    
    # return any newly created values based on inputs if any. I don't see any at this time

    
    # return


def load_runtime_args(output_folder):
    
    # these are just shortcuts from os.getenv
    global CATFIM_TYPE, NWM_METAFILE_PATH, THRESHOLD_FILE_PATH, FIM_RUN_DIR, PAST_MAJOR_INTERVAL_CAP
    
    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)
    
    # use load_env, and pull out just the variables it needs.
    load_dotenv(args_file)
    
    CATFIM_TYPE = os.getenv('CATFIM_TYPE')
    NWM_METAFILE_PATH = os.getenv('NWM_METAFILE_PATH')
    THRESHOLD_FILE_PATH = os.getenv('THRESHOLD_FILE_PATH')
    FIM_RUN_DIR = os.getenv('FIM_RUN_DIR')
    PAST_MAJOR_INTERVAL_CAP = os.getenv('PAST_MAJOR_INTERVAL_CAP')
    
    # others? flow subset data?
    

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
