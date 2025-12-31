import argparse
import logging
import os
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv

import src.utils.shared_functions as sf


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

    is_logging_loaded = False

    # validate output_folder path
    if not os.path.exists(output_folder):
        raise Exception("CatFIM output path does not exist. Post Processing aborted.")

    try:

        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")
        print("================================")

        # ---------------------
        # load the runtime_args.env, error if it does not exist. It should give us all values we need
        # See generate_categorical_fim.py -> save_env_args(output_path)
        catfim_type = __load_runtime_args(output_folder)

        catfim_type_name = ""
        if catfim_type == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        print(f"Start post processing for {catfim_type_name} ;  (UTC): {dt_string}")
        print("")

        sites_file_path, library_file_path = __set_start_files_folders(output_folder, catfim_type_name)

        # ---------------------
        # Gets its own logs at this root level. The folder may be shared with pre-processing.
        log_file_dir = os.path.join(output_folder, "logs")
        log_file_path = sf.setup_file_logger(log_file_dir, "catfim_post_processing")
        print(f"  Logs will be save to {log_file_path}")

        # ---------------------
        # what if someone ran this only and skipped running generate_categorical_fim.py or any hucs?
        # what if there are no valid huc left over?

        # Validate that we have some huc sites / library data
        huc_path = os.path.join(output_folder, "hucs")
        if os.path.exists(huc_path):
            raise Exception("CatFIM output huc folder does not exist. Post Processing aborted.")

        # Gets a list of huc numbers by finding folder names from /data/catfim/hand_4_8_7_2_stage_based/huc)
        # get list of hucs included

        # what if none?

        # roll up all HUC level sites.gpkg's and library files gpkg.
        # should always be at least one huc, but may not more depending on debugging

        # We are looking across all huc dirs for files with the convention of:
        # {huc}_sites.gpkg or {huc}_library.gpkg. By looking for files with only that
        # pattern, we can leave some debugging or intermedate files.

        # do we want to iterate each HUC folder looking for the existance of its final libary file
        # and count it?  If any one HUC did not get to a final gpkg, we know it aborted or failed somehow
        # and each HUC logs / prints would have told the user why  ???
        # Then we can show the user "x" hucs successfully processed.

        # Just because we have a HUC, does not mean we have a library files
        # And I guess it is possible we don't have a sites file either. ie) bad huc or huc with no sites

        # any final validation needed here? Maybe not other. Give warning but not
        # error that the file sites and library exists (again.. debugging)

        # ---------------------
        # make csv versions of the two gpkg files

        # ---------------------
        # Rollup logs
        # Rollup huc Logs? Likely not.. just rollup error and warning logs.
        #   (humm. how to use only each HUCs latest one as it might have more than one if the HUC was run again)
        #   or maybe all? not sure what is smart here.
        #   search for files in each huc level for file names with _errors or _warnings

        logging.info("End CatFIM post processing")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)

    except Exception:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred performing post processing. Detail: {trace_error}"

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # do we re-throw the error? gcf, aws, or cmd line? hummm


def __load_runtime_args(output_folder):

    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)

    # use load_env, and pull out just the variables it needs.
    load_dotenv(args_file)
    return os.getenv('CATFIM_TYPE')


def __set_start_files_folders(output_folder, catfim_type_name):

    # Note: all key other variables have already been validated

    # ================================
    # CLEANUP
    # Remove all files / folders except anything in the log folder, we keep that one only.
    sites_file_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_sites.gpkg")
    if os.path.isfile(sites_file_path):
        os.remove(sites_file_path)

    library_file_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_library.gpkg")
    if os.path.isfile(library_file_path):
        os.remove(library_file_path)

    # Always keeps the logs folder

    return sites_file_path, library_file_path


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
