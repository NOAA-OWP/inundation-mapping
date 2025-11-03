import os
import argparse
import logging

from dotenv import load_dotenv

# Global variable  (some shortcuts from env files)
CATFIM_TYPE=""
MODEL_VERSION=""

"""_summary_
    Overall processing steps (tenatively)
    
    Should not need to call any other of the catfim py files. Some of those master rollup 
    functions should be moved here. Should not need to update the master sites or library files,
    only append them.
    
    1: Start up its own non-shared log system.  It can have its own log folder, and yes, each HUC
       has it's own log folder/files.
    
    1.b: load the runtime_arg.env if it needs anything from it.
    
    2: Validate HUCs data (has some sites and library data remaining)
    
    3: roll up all HUC level sites.csv/gpkg's and library files csv/gpkg into one big final set of files like we currently have.
    
    3.b Update the rolled up files for model_version (hand version) fields? TBD
    
    4: Roll up HUC logs?  Nah.. don't need it. A list of HUCs that we processed maybe?
    
    5: Roll up HUC error/warning logs?  seperate logs for warnign versus error?
       -- humnmm... if a HUC was re-run, it would have more than one possible error and/or warning file
       -- How do we roll up just the latest from each dir? and the rollup  here also needs date/time as it might 
       have more then one set of files.
    
"""

def catfim_post_processing(output_folder):
    
    # validate output_folder path
    if os.path.exist(output_folder):
        raise Exception("CatFIM output path does not exist. Post Processing aborted.")

    load_runtime_args(output_folder)  # have it here?

    # ---------------------
    # what if someone ran this only and skipped running generate_categorical_fim.py or any hucs?
    # what if there are no valid huc left over?
   
    # Validate that we have some huc sites / library data
    huc_path = os.path.join(output_folder, "hucs")
    if os.path.exist(huc_path):
        raise Exception("CatFIM output huc folder does not exist. Post Processing aborted.")

    # Gets a list of huc numbers by finding folder names from /data/catfim/hand_4_8_7_2_stage_based/huc)
    # get list of hucs included
    
    # what if none?


    # ---------------------
    # roll up all HUC level sites.csv/gpkg's and library files csv/gpkg.
    # any final validation needed here?
   
    # ---------------------
    # Rollup logs
    # Roll up Error logs (humm. how to use only each HUCs latest one as it might have more than one if the HUC was run again)
    # Roll up Warning Logs (same problem)
    

def load_runtime_args(output_folder):
    
    global CATFIM_TYPE, MODEL_VERSION
    
    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)
    
    # use load_env, and pull out just the variables it needs.
    load_dotenv(args_file)
    
    # catfim type and model_version might be the only one needed.
    CATFIM_TYPE = os.getenv('CATFIM_TYPE')
    

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
