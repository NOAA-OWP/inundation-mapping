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

    is_logging_loaded = False

    # TEMPORARY DEBUGGING FUNCTIONALITY: Copy the contents of the input folder to a temp folder and work there.
    temp_output_folder = os.path.join(output_folder, "temp_post_process")
    os.mkdirs(temp_output_folder, exist_ok=True)
    shutil.copytree(output_folder, temp_output_folder, dirs_exist_ok=True)
    output_folder = temp_output_folder
    print('Using temporary output folder for post processing:', output_folder) ## TEMP DEBUG
    # REMOVE ABOVE BEFORE FLIGHT


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

        print(f"Start post-processing for {catfim_type_name} ;  (UTC): {dt_string}")
        print("")

        # Create filepath names and delete any pre-existing output files
        sites_gpkg_path, sites_csv_path, library_gpkg_path, library_csv_path = __set_start_files_folders(output_folder, catfim_type_name)

        # ---------------------
        # Create a post-processing logger (Log folder may be shared with pre-processing)
        log_file_dir = os.path.join(output_folder, "logs")
        log_file_path = sf.setup_file_logger(log_file_dir, "catfim_post_processing")
        print(f"  Logs will be saved to {log_file_path}")

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

        logging.info(f"  Found {len(huc_list)} HUCs to process.")

        # Iterate through each HUC folder and compile all sites.gpkg and library.gpkg files into one big file each
        hucs_without_sites, hucs_without_library, hucs_with_sites, hucs_with_library = [], [], [], []
        compiled_sites_gdf_list, compiled_library_gdf_list = [], []

        for huc in huc_list:
            huc_folder = os.path.join(huc_path, huc)

            # Sites file
            huc_sites_file = os.path.join(huc_folder, f"{huc}_sites.gpkg")
            if os.path.isfile(huc_sites_file):
                # Append to compiled sites file
                huc_sites_gdf = gpd.read_file(huc_sites_file, engine='fiona')

                if os.path.isfile(sites_gpkg_path):
                    compiled_sites_gdf = gpd.read_file(sites_gpkg_path, engine='fiona')
                    compiled_sites_gdf_list.append(huc_sites_gdf)
                # else:
                    # print warning?

                logging.info(f"    Appended sites from HUC {huc} to compiled sites GDF list.")
                hucs_with_sites.append(huc)

            else:
                hucs_without_sites.append(huc)
                logging.warning(f"    WARNING: No sites file found for HUC {huc}.")

            # Library file
            huc_library_file = os.path.join(huc_folder, f"{huc}_library.gpkg")
            if os.path.isfile(huc_library_file):
                # Append to compiled library file
                huc_library_gdf = gpd.read_file(huc_library_file, engine='fiona')

                if os.path.isfile(library_gpkg_path):
                    compiled_library_gdf = gpd.read_file(library_gpkg_path, engine='fiona')
                    compiled_library_gdf_list.append(huc_library_gdf)
                # else:
                    # print warning?

                logging.info(f"    Appended library from HUC {huc} to compiled library GDF list.")
                hucs_with_library.append(huc)

            else:
                hucs_without_library.append(huc)
                logging.warning(f"    WARNING: No library file found for HUC {huc}.")

        # Concatenate all GeoDataFrames into one GDF each
        compiled_sites_gdf = gpd.pd.concat(compiled_sites_gdf_list, ignore_index=True)
        compiled_library_gdf = gpd.pd.concat(compiled_library_gdf_list, ignore_index=True)

        # Save the compiled GeoDataFrames to GeoPackage files
        compiled_sites_gdf.to_file(sites_gpkg_path, driver='GPKG', engine='fiona')
        compiled_library_gdf.to_file(library_gpkg_path, driver='GPKG', engine='fiona')

        # Create a csv version of the sites and library files
        if os.path.isfile(sites_gpkg_path): # TODO: Decide if this check is needed
            compiled_sites_df = compiled_sites_gdf.drop(columns=['geometry'])
            compiled_sites_df.to_csv(sites_csv_path, index = False)
            logging.info(f"  Created CSV version of sites file at {sites_csv_path}.")

        if os.path.isfile(library_gpkg_path): # TODO: Decide if this check is needed
            compiled_library_df = compiled_library_gdf.drop(columns=['geometry'])
            compiled_library_df.to_csv(library_csv_path)
            logging.info(f"  Created CSV version of library file at {library_csv_path}.")

        # ---------------------
        # Print summary of HUC processing
        m = f"  HUC folders processed: {len(huc_list)}"
        print(m)
        logging.info(m)
        logging.info(f"    HUCs with sites files: {len(hucs_with_sites)}")
        logging.info(f"    HUCs with library files: {len(hucs_with_library)}")

        # Print HUCs that had neither sites nor library file
        hucs_with_neither = set(huc_list).difference(set(hucs_with_sites).union(set(hucs_with_library)))
        if len(hucs_with_neither) > 0:
            logging.warning(f"  WARNING: {len(hucs_with_neither)} HUCs had neither sites nor library files:")
            logging.warning(f"    {hucs_with_neither}")

        # Print HUCs that had a sites file but no library file (unlikely scenario)
        hucs_with_sites_but_no_library = set(hucs_with_sites).difference(set(hucs_with_library))
        if len(hucs_with_sites_but_no_library) > 0:
            logging.warning(f"  WARNING: {len(hucs_with_sites_but_no_library)} HUCs had a sites file but no library file:")
            logging.warning(f"    {hucs_with_sites_but_no_library})")

        # Print HUCs that had a library file but no sites file (unlikely scenario)
        hucs_with_library_but_no_sites = set(hucs_with_library).difference(set(hucs_with_sites))
        if len(hucs_with_library_but_no_sites) > 0:
            logging.warning(f"  WARNING: {len(hucs_with_library_but_no_sites)} HUCs had a library file but no sites file:")
            logging.warning(f"    {hucs_with_library_but_no_sites})")

        # ---------------------
        # Rollup logs? TODO
        # Rollup huc Logs? Likely not.. just rollup error and warning logs.
        #   (humm. how to use only each HUCs latest one as it might have more than one if the HUC was run again)
        #   or maybe all? not sure what is smart here.
        #   search for files in each huc level for file names with _errors or _warnings

        logging.info("End CatFIM post-processing")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)

    except Exception:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred performing post-processing. Detail: {trace_error}"

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
    # Remove pre-existing output files / folders except anything in the log folder, we keep that one only. # TODO: Any other folders to remove?
    sites_gpkg_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_sites.gpkg")
    if os.path.isfile(sites_gpkg_path):
        os.remove(sites_gpkg_path)

    sites_csv_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_sites.csv")
    if os.path.isfile(sites_csv_path):
        os.remove(sites_csv_path)

    library_gpkg_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_library.gpkg")
    if os.path.isfile(library_gpkg_path):
        os.remove(library_gpkg_path)

    library_csv_path = os.path.join(output_folder, f"{catfim_type_name}_catfim_library.csv")
    if os.path.isfile(library_csv_path):
        os.remove(library_csv_path)

    # Always keeps the logs folder

    return sites_gpkg_path, sites_csv_path, library_gpkg_path, library_csv_path


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
