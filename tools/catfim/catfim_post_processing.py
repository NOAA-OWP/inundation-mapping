import argparse
import logging
import os
import traceback
from datetime import datetime, timezone
import geopandas as gpd
import pandas as pd
import shutil

from dotenv import load_dotenv

import src.utils.shared_functions as sf


"""_summary_
    Overall processing steps (tenatively)

    Should not need to call any other of the catfim py files. Some of those compiled rollup
    functions should be moved here. Should not need to update the compiled sites or library files,
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


    this is taking over the functionality of the post_process_cat_fim_for_viz function... anything else?



"""


def catfim_post_processing(output_folder):

    is_logging_loaded = False # TODO: Does this ever become "True"?

    # Validate output_folder path
    if not os.path.exists(output_folder):
        raise Exception("CatFIM output path does not exist. Post-processing aborted.")

    try:

        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

        print("================================")

        # ---------------------
        # Load the runtime_args.env, error if it does not exist.
        # See generate_categorical_fim.py -> save_env_args(output_path)
        catfim_type = __load_runtime_args(output_folder)

        catfim_type_name = ""
        if catfim_type == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        print(f"Starting post-processing for {catfim_type_name} at {dt_string} (UTC)")
        print("")

        # Create filepath names and delete any pre-existing output files
        sites_gpkg_path, sites_csv_path, library_gpkg_path, library_csv_path, deleted_file_count = __set_start_files_folders(output_folder, catfim_type_name)

        if deleted_file_count > 0:
            print(f"Removed {deleted_file_count} pre-existing output file(s).")

        # ---------------------
        # Create a post-processing logger (Log folder may be shared with pre-processing)
        log_file_dir = os.path.join(output_folder, "logs")
        log_file_path = sf.setup_file_logger(log_file_dir, "catfim_post_processing")
        print(f"Logs will be saved to {log_file_path}")

        # ---------------------
        # Validate that we have some huc sites / library data
        huc_path = os.path.join(output_folder, "hucs")
        if not os.path.exists(huc_path):
            raise Exception("CatFIM output huc folder does not exist. Post-processing aborted.")

        # Gets a list of huc numbers by finding folder names from /data/catfim/hand_4_8_7_2_stage_based/huc)
        huc_list = [
            x
            for x in os.listdir(huc_path)
            if os.path.isdir(os.path.join(huc_path, x)) and x[0] in ['0', '1', '2', '9']
        ]

        if len(huc_list) == 0:
            raise Exception("No HUCs found in CatFIM output huc folder. Post-processing aborted.")

        logging.info("")
        logging.info(f"Found {len(huc_list)} HUC folder(s) to process.")
        logging.info("Beginning iteration through HUC folders...")
        # logging.info(huc_list) # TEMP DEBUG
        logging.info("")

        # ---------------------
        # Iterate through each HUC folder and compile all sites.gpkg and library.gpkg files into one big file each
        hucs_without_sites, hucs_without_library = [], []
        compiled_sites_gdf_list, compiled_library_gdf_list = [], []

        for huc in huc_list:
            huc_folder = os.path.join(huc_path, huc)
            missing_data = False # reset for each HUC

            # Sites
            huc_sites_file = os.path.join(huc_folder, f"{huc}_sites.gpkg")
            try:
                with open(huc_sites_file, 'r') as f:
                    huc_sites_gdf = gpd.read_file(huc_sites_file, engine='fiona')

            except FileNotFoundError:
                hucs_without_sites.append(huc)
                missing_data = True
                # logging.warning(f"{huc} - WARNING: The file '{huc_sites_file}' does not exist.") # TEMP DEBUG

            # Library
            huc_library_file = os.path.join(huc_folder, f"{huc}_library.gpkg")
            try:
                with open(huc_library_file, 'r') as f:
                    huc_library_gdf = gpd.read_file(huc_library_file, engine='fiona')

            except FileNotFoundError:
                hucs_without_library.append(huc)
                missing_data = True
                # logging.warning(f"{huc} - WARNING: The file '{huc_library_file}' does not exist.") # TEMP DEBUG

            # If both files were found, append to compiled lists
            if missing_data:
                logging.info(f"{huc} - Skipped appending due to missing data.")
            else:
                logging.info(f"{huc} - Sites and library files found, appending data to output lists.")
                compiled_sites_gdf_list.append(huc_sites_gdf)
                compiled_library_gdf_list.append(huc_library_gdf)

        # ---------------------
        # Summarize HUC processing

        logging.info("")
        logging.info("Done iterating through HUC folders.")
        logging.info(f"Compiled data from {len(compiled_sites_gdf_list)} out of {len(huc_list)} HUC folders.")

        # Print HUCs that had neither sites nor library file
        hucs_without_library_and_sites = list(set(hucs_without_sites) & set(hucs_without_library))
        if len(hucs_without_library_and_sites) > 0:
            logging.warning(f"WARNING: {len(hucs_without_library_and_sites)} HUC(s) skipped due to missing sites AND library files:")
            logging.warning(hucs_without_library_and_sites)

        # Print HUCs that had library but no sites (unlikely might indicate a bug)
        hucs_missing_only_sites = list(set(hucs_without_sites).difference(set(hucs_without_library)))
        if len(hucs_missing_only_sites) > 0:
            logging.warning(f"WARNING: {len(hucs_missing_only_sites)} HUC(s) skipped due to missing sites file:")
            logging.warning(hucs_missing_only_sites)

        # Print HUCs that had sites but no library (unlikely, might indicate a bug)
        hucs_missing_only_library = list(set(hucs_without_library).difference(set(hucs_without_sites)))
        if len(hucs_missing_only_library) > 0:
            logging.warning(f"WARNING: {len(hucs_missing_only_library)} HUC(s) skipped due to missing library file:")
            logging.warning(hucs_missing_only_library)

        # ---------------------
        # Save compiled sites and library files

        logging.info("")
        logging.info("Saving compiled sites and library files...")

        # Concatenate all GeoDataFrames into one GDF each
        compiled_sites_gdf = gpd.pd.concat(compiled_sites_gdf_list, ignore_index=True)
        compiled_library_gdf = gpd.pd.concat(compiled_library_gdf_list, ignore_index=True)

        # Save the compiled GeoDataFrames to GeoPackage files
        compiled_sites_gdf.to_file(sites_gpkg_path, driver='GPKG', engine='fiona')
        logging.info(f"Saved sites GeoPackage to {sites_gpkg_path}")

        compiled_library_gdf.to_file(library_gpkg_path, driver='GPKG', engine='fiona')
        logging.info(f"Saved library GeoPackage to {library_gpkg_path}")

        # Drop geometry column and save the csv versions
        compiled_sites_df = compiled_sites_gdf.drop(columns=['geometry'])
        compiled_sites_df.to_csv(sites_csv_path, index = False)
        logging.info(f"Saved sites CSV to {sites_csv_path}")

        compiled_library_df = compiled_library_gdf.drop(columns=['geometry'])
        compiled_library_df.to_csv(library_csv_path)
        logging.info(f"Saved library CSV to {library_csv_path}")

        # ---------------------
        # Rollup logs? TODO
        # Rollup huc Logs? Likely not.. just rollup error and warning logs.
        #   (humm. how to use only each HUCs latest one as it might have more than one if the HUC was run again)
        #   or maybe all? not sure what is smart here.
        #   search for files in each huc level for file names with _errors or _warnings

        logging.info("")
        logging.info("End CatFIM post-processing")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)
        logging.info("")
        print("================================")
        print("")



    except Exception:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred performing post-processing. Detail: {trace_error}"

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # do we re-throw the error? gcf, aws, or cmd line? hummm TODO


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
    # Remove pre-existing output files / folders except anything in the log folder, we keep that one only.
    # # TODO: Any other folders to remove? I don't think so
    
    deleted_file_count = 0

    sites_gpkg_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_sites.gpkg")
    if os.path.exists(sites_gpkg_path):
        os.remove(sites_gpkg_path)
        deleted_file_count += 1

    sites_csv_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_sites.csv")
    if os.path.exists(sites_csv_path):
        os.remove(sites_csv_path)
        deleted_file_count += 1

    library_gpkg_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_library.gpkg")
    if os.path.exists(library_gpkg_path):
        os.remove(library_gpkg_path)
        deleted_file_count += 1

    library_csv_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_library.csv")
    if os.path.exists(library_csv_path):
        os.remove(library_csv_path)
        deleted_file_count += 1

    # Always keeps the logs folder

    return sites_gpkg_path, sites_csv_path, library_gpkg_path, library_csv_path, deleted_file_count


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
        help='REQUIRED: Target location, location of the CatFIM output folder to be post-processed.'
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )

    args = vars(parser.parse_args())

    # call main program
    catfim_post_processing(**args)
