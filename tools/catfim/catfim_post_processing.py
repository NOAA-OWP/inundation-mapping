import os
import argparse

from dotenv import load_dotenv

# Global variable  (some shortcuts from env files)
CATFIM_TYPE=""
MODEL_VERSION=""

"""_summary_
    Overall processing steps (tenatively)
    
    Should not need to call any other of the catfim py files. Some of those master rollup 
    functions should be moved here. Should not need to update the master sites or library files,
    only append them.
    
    1: Start up its own non-shared log system
    
    1.b: load the runtime_arg.env    
    
    2: Validate HUCs data (has some sies and library data remaining)
    
    3: roll up all HUC level sites.csv/gpkg's and library files csv/gpkg into one big final set of files like we currently have.
    
    3.b Update the rolled up files for model_version (hand version) fields? TBD
    
    4: Roll up HUC logs?  not sure about that one.
    
    5: Roll up HUC error/warning logs?  seperate logs for warnign versus error?
    
"""

def catfim_post_processing(output_folder):
    
    # validate output_folder path
    if os.path.exist(output_folder):
        raise Exception("CatFIM output path does not exist. Post Processing aborted.")


    load_runtime_args(output_folder)  # have it here?

   
    # ---------------------
    # Validate that we have some huc sites / library data
    huc_path = os.path.join(output_folder, "hucs")
    if os.path.exist(huc_path):
        raise Exception("CatFIM output huc folder does not exist. Post Processing aborted.")

    # Gets a list of huc numbers by finding folder names from /data/catfim/hand_4_8_7_2_stage_based/huc)
    # get list of hucs included
    
    # what if none?


    # ---------------------
    # roll up all HUC level sites.csv/gpkg's and library files csv/gpkg.
    
    # ---------------------
    # Update the rolled up files for model_version (hand version) fields?
    
    # ---------------------
    # Rollup logs
    # Roll up HUC logs?  not sure about that one.
    # Roll up Error logs
    # Roll up Warning Logs
    

def load_runtime_args(output_folder):
    
    global CATFIM_TYPE, MODEL_VERSION
    
    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)
    
    # use load_env, and pull out just the variables it needs.
    load_dotenv(args_file)
    
    # catfim type and model_version might be the only one needed.
    CATFIM_TYPE = os.getenv('CATFIM_TYPE')
    MODEL_VERSION = os.getenv('MODEL_VERSION')
    

if __name__ == '__main__':

    '''
    Sample
    python /foss_fim/tools/catfim/catfim_post_processing.py -t /data/catfim/hand_4_8_7_2
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Post Processing for CatFIM')

    parser.add_argument(
        '-t',
        '--output-folder',
        help='REQUIRED: Target location, Where the output folder will be.'
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )

    args = vars(parser.parse_args())

    # call main program
    catfim_post_processing(**args)
