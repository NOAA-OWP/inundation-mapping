#!/usr/bin/env python3
 
import os
import argparse

from dotenv import load_dotenv


# Global variable  (some shortcuts from env files)
CATFIM_TYPE=""
NWM_METAFILE_PATH=""
THRESHOLD_FILE_PATH=""
FIM_RUN_DIR=""
PAST_MAJOR_INTERVAL_CAP=""

"""_summary_
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

    # ------------
    huc_path = validate_inputs(huc, output_folder)  # ie: /data/catfim/(somefolder)/hucs/12090301

    # ---------------------
    # Start a folder structure if not already in place ?? Do we want all of these?
    output_flows_dir = os.path.join(huc_path, 'flows')  # include threshold data?
    output_mapping_dir = os.path.join(huc_path, 'mapping')
    attributes_dir = os.path.join(huc_path, 'attributes')  # do we want this anymore? Isn't it duplicate of what we create
      # for our huc level sies csv file we create pretty much right away?
    output_thresholds_dir = os.path.join(huc_path, 'thresholds')  # hummmm...
    
    # ---------------------
    # Setup logging. It should make its own huc log folder inside the parent "logs" folder
    # can't really setup logging until we have the huc validated

    try:
        # ---------------------
        # load the runtime_args.env, error if it does not exist. It should give us all values we need
        # See generate_categorical_fim.py -> save_env_args(output_path)
        load_runtime_args(output_folder)
        
        # ---------------------
        # validate HUC and if it is applicable to CatFIM?

        # ---------------------
        # Get list of applicable, valid sites for this HUCs?  from where? master sites metadata or site file?
        #   Watching for excluded sites from restricted sites csv.

        # ---------------------
        # Create its own sites csv. Populate what we know if anything and continue updating throughout
        # processing steps including mapping flags and status data.
        
        # ---------------------       
        # Load its own metadata, threshold data and flow data, if applicable using shared various files.
        
        # ---------------------    
        # Various meta and threshold processing? including validation of data ?
        
        # ---------------------    
        # Figure out stages and if SB also figure out stages.
        
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


def validate_inputs(huc, output_folder):

    # valdiate huc value (8 numeric maybe)

    # validate the outfolder path exists first

    # ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301
    huc_path = os.path.join(output_folder, "hucs", huc)
    
    
    # No need to validate any of the runtime_args as they were validated when it was created.
    
    # return any newly created values based on inputs
    return huc_path


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
