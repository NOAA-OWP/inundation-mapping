import argparse
import logging
import os
import traceback
from datetime import datetime, timezone

import geopandas as gpd
from dotenv import load_dotenv

import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf


"""_summary_

    CatFIM Post-Processing Module

    This module handles the final aggregation and compilation of Categorical Flood Inundation Mapping (CatFIM) 
    output data from individual HUC folders into a consolidated dataset.

    Key Responsibilities:
    - Validates CatFIM output folder structure and runtime configuration
    - Iterates through HUC-level directories to collect sites and library geospatial data
    - Consolidates HUC-level GeoPackage and CSV files into unified output files
    - Renames legacy 'nws_lid' columns to 'ahps_lid' for consistency
    - Generates comprehensive logging of the post-processing workflow
    - Handles both stage-based and flow-based CatFIM processing types
    - Manages cleanup of pre-existing output files before regeneration
    - Tracks and reports on HUCs with missing or incomplete data

    Main Functions:
    - catfim_post_processing(): Primary orchestration function for the entire post-processing pipeline
    - __load_runtime_args(): Loads runtime configuration from environment file
    - __set_start_files_folders(): Manages output file paths and cleanup of pre-existing files

    Output Files Generated:
    - Compiled sites GeoPackage and CSV
    - Compiled library GeoPackage and CSV
    - Post-processing logs with warnings for problematic HUCs

    Notes:
    - Taking over the functionality previously handled by generate_categorical_fim.py's post_process_cat_fim_for_viz function
    - Assumes the presence of a standardized CatFIM output directory structure

"""

def catfim_post_processing(output_folder):

    is_logging_loaded = False

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

        # ---------------------
        # Create a post-processing logger (temp folder may be shared with pre-processing)
        temp_folder = os.path.join(output_folder, "temp")
        log_file_path = sf.setup_file_logger(temp_folder, "catfim_post_processing")
        is_logging_loaded = True

        logging.info(f"Begin CatFIM post-processing for {catfim_type_name} at {dt_string} (UTC)")
        logging.info("")
        print(f"Logs will be saved to {log_file_path} initially and later copied over to {output_folder}/logs")

        # Create filepath names and delete any pre-existing output files
        sites_gpkg_path, sites_csv_path, library_gpkg_path, library_csv_path, deleted_file_count = __set_start_files_folders(
            output_folder, catfim_type_name
        )

        if deleted_file_count > 0:
            logging.info(f"Removed {deleted_file_count} pre-existing output file(s).")

        # ---------------------
        # Validate that we have some huc sites / library data
        huc_parent_folder_path = os.path.join(output_folder, "hucs")
        if not os.path.exists(huc_parent_folder_path):
            raise Exception("CatFIM output huc folder does not exist. Post-processing aborted.")

        # Gets a list of huc numbers by finding folder names from /data/catfim/hand_4_8_7_2_stage_based/hucs
        # Note for the future: Never use the catfim_huc_list.txt as it might be invalid after initial processing
        huc_list = [
            x
            for x in os.listdir(huc_parent_folder_path)
            if os.path.isdir(os.path.join(huc_parent_folder_path, x)) and x[0] in ['0', '1', '2', '9']
        ]

        if len(huc_list) == 0:
            raise Exception("No HUCs found in CatFIM output huc folder. Post-processing aborted.")

        # ---------------------
        # Iterate through each HUC folder and compile all sites.gpkg and library.gpkg files into one big file each

        logging.info("")
        logging.info(f"Found {len(huc_list)} HUC folder(s) to process.")
        logging.info("Beginning iteration through HUC folders...")

        hucs_without_sites, hucs_without_library = [], []
        compiled_sites_gdf_list, compiled_library_gdf_list = [], []

        for huc in huc_list:
            huc_path = os.path.join(huc_parent_folder_path, huc)

            # Create filepath variables
            __, __, __, __, sites_post_mapping_file_path, __, library_post_mapping_file_path = csf.make_huc_mapping_filepaths(huc, catfim_type, huc_path)
            # TODO: There's probably a better way to do this without returning all the __'s,

            # Sites
            try:
                with open(sites_post_mapping_file_path, 'r') as f:
                    huc_sites_gdf = gpd.read_file(sites_post_mapping_file_path, engine='fiona')

                # Rename output columns if needed (should already be renamed, but just in case)
                huc_sites_gdf = csf.rename_output_columns(huc_sites_gdf)

                # Drop unnecessary columns if needed (should already be done, but just in case)
                huc_sites_gdf = csf.drop_output_columns(huc_sites_gdf, catfim_type)

                compiled_sites_gdf_list.append(huc_sites_gdf)

            except FileNotFoundError:
                hucs_without_sites.append(huc)

            # Library
            try:
                with open(library_post_mapping_file_path, 'r') as f:
                    huc_library_gdf = gpd.read_file(library_post_mapping_file_path, engine='fiona')

                # Rename output columns if needed (should already be renamed, but just in case)
                huc_library_gdf = csf.rename_output_columns(huc_library_gdf)

                # Drop unnecessary columns if needed (should already be done, but just in case)
                huc_library_gdf = csf.drop_output_columns(huc_library_gdf, catfim_type)

                compiled_library_gdf_list.append(huc_library_gdf)

            except FileNotFoundError:
                hucs_without_library.append(huc)
        # End huc loop

        # If there's no data to compile, then we should just error out here
        if len(compiled_sites_gdf_list) == 0 and len(compiled_library_gdf_list) == 0:
            logging.error("ERROR: No HUC-level sites or library files to concatenate. Aborting post-processing.")
            return

        # ---------------------
        # If files were found, summarize HUC processing

        logging.info("Done iterating through HUC folders.")
        logging.info(f"Sucessfully compiled data from {len(compiled_sites_gdf_list)}/{len(huc_list)} HUC folders.")

        # Print HUCs that had neither sites nor library file
        hucs_without_library_and_sites = list(set(hucs_without_sites) & set(hucs_without_library))
        if len(hucs_without_library_and_sites) > 0:
            logging.warning(f"WARNING: {len(hucs_without_library_and_sites)} HUC(s) skipped due to missing sites AND library results:")
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

        # Concatenate sites GeoDataFrames into one GDF and update LID column name
        compiled_sites_gdf = gpd.pd.concat(compiled_sites_gdf_list, ignore_index=True)
        compiled_sites_gdf.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)

        # Concatenate library GeoDataFrames into one GDF and update LID column name
        compiled_library_gdf = gpd.pd.concat(compiled_library_gdf_list, ignore_index=True)
        compiled_library_gdf.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)

        # Save the compiled GeoDataFrames to GeoPackage files
        compiled_sites_gdf.to_file(sites_gpkg_path, driver='GPKG', engine='fiona', index=False)
        logging.info(f"Saved sites GeoPackage to {sites_gpkg_path}")

        compiled_library_gdf.to_file(library_gpkg_path, driver='GPKG', engine='fiona', index=False)
        logging.info(f"Saved library GeoPackage to {library_gpkg_path}")

        # Drop geometry column and save the csv versions
        compiled_sites_df = compiled_sites_gdf.drop(columns=['geometry'])
        compiled_sites_df.to_csv(sites_csv_path, index = False)
        logging.info(f"Saved sites CSV to {sites_csv_path}")

        compiled_library_df = compiled_library_gdf.drop(columns=['geometry'])
        compiled_library_df.to_csv(library_csv_path, index=False)
        logging.info(f"Saved library CSV to {library_csv_path}")

        # ---------------------
        # Completed post-processing

        logging.info("")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(f"End CatFIM post-processing - {duration_msg}")
        logging.info("")
        print("================================")
        print("")

    except Exception as ex:
        trace_error = traceback.format_exc()
        err_msg = f"A critical error has occurred performing post-processing. Detail: {trace_error}"

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # re-raise the exception, mostly for AWS
        raise ex


def __load_runtime_args(output_folder):
    '''
    Loads the necessary variables from the runtime args file. 
    '''

    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)

    # Use load_env to just pull outthe variables it needs
    load_dotenv(args_file)

    return os.getenv('CATFIM_TYPE')


def __set_start_files_folders(output_folder, catfim_type_name):
    '''
    Removes pre-existing output files / folders except anything in the log folder.
    We should always keep the logs folder

    Arguments
    ----------
    output_folder - STR
        Filepath to CatFIM run output folder
    catfim_type_name - STR
        Type of CatFIM we are running ('flow_based' or 'stage_based')
    
    Returns
    -------
    sites_gpkg_path - STR
        Filepath for final sites GPKG.
    sites_csv_path - STR
        Filepath for final sites CSV.
    library_gpkg_path - STR
        Filepath for final library GPKG.
    library_csv_path - STR
        Filepath for final library CSV.
    deleted_file_count - INT?
        Number of files that the function deletes.
    '''
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

    return sites_gpkg_path, sites_csv_path, library_gpkg_path, library_csv_path, deleted_file_count


if __name__ == '__main__':

    '''
    Sample
    python /foss_fim/tools/catfim/catfim_post_processing.py -t /data/catfim/hand_4_8_7_2
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Post-Processing for CatFIM')

    parser.add_argument(
        '-t',
        '--output-folder',
        help='REQUIRED: Target location, location of the CatFIM output folder to be post-processed.'
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )

    args = vars(parser.parse_args())

    # Call main program
    catfim_post_processing(**args)
