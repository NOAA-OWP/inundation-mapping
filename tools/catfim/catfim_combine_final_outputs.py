#!/usr/bin/env python3

import argparse
import logging
import os
import traceback
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd

import src.utils.shared_functions as sf
import tools.catfim.catfim_post_processing as cpp
import tools.catfim.catfim_shared_functions as csf
import tools.catfim.generate_categorical_fim as gcf


'''
This tool is being used to combine the outputs of two CatFIM runs (primary and secondary) into a single set of outputs.

The primary and secondary directories must have the same values for CATFIM_TYPE, FIM_RUN_DIR, PAST_MAJOR_INTERVAL_CAP,
and SEARCH (all found in the runtime_args.env file of the directories).

The outputs are merged into new files in the primary folder with a label added to the filename.

'''


def merge_gpkgs(gpkg_path_list, output_dir, label):

    for path in gpkg_path_list:
        if not os.path.exists(path):
            logging.warning(f"Warning: File not found -> {path}")
            # Remove from list if file doesn't exist
            gpkg_path_list.remove(path)
            continue
            
    # Read and concatenate all files
    gdfs = []
    hucs_added = set()  # Keep track of HUCs that have already been added to the merged_gdf
    for f in gpkg_path_list:
        # Read gpkg
        gdf = gpd.read_file(f)

        # Filter out HUCs that have already been added
        huc_list = [huc for huc in huc_list if huc not in hucs_added]
        gdf = gdf[gdf['huc8'].isin(huc_list)]

        gdfs.append(gdf)

        # Update hucs_added with the HUCs from the current gdf
        hucs_added.update(gdf['huc8'].unique())

        logging.info(f"Added {len(gdf)} rows from {f} to the merged GeoDataFrame. Total unique HUCs added so far: {len(hucs_added)}")

    merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

    # Get filename without extension for the layer name
    filename = os.path.splitext(os.path.basename(gpkg_path_list[0]))[0]
    output_gpkg_path = os.path.join(output_dir, f"{filename}_{label}.gpkg")  # TODO: is this the correct use of my label?

    # Save merged file to output dir
    merged_gdf.to_file(output_gpkg_path, driver="GPKG", layer=filename)
    logging.info(f"Successfully merged GeoPackages into {output_gpkg_path} in layer {filename}")

    return


def merge_csvs(csv_path_list, output_dir, label):

    for path in csv_path_list:
        if not os.path.exists(path):
            logging.warning(f"Warning: File not found -> {path}")
            # Remove from list if file doesn't exist
            csv_path_list.remove(path)
            continue

    dfs = [pd.read_csv(f) for f in csv_path_list]
    merged_df = pd.concat(dfs, ignore_index=True)

    filename = os.path.splitext(os.path.basename(csv_path_list[0]))[0]
    output_csv_path = os.path.join(output_dir, f"{filename}_{label}.csv")

    merged_df.to_csv(output_csv_path, index=False)
    logging.info(f"Successfully merged CSVs into {output_csv_path}")

    return


def merge_geoparquets(parquet_path_list, output_dir, label):
    for path in parquet_path_list:
        if not os.path.exists(path):
            logging.warning(f"Warning: File not found -> {path}")
            # Remove from list if file doesn't exist
            parquet_path_list.remove(path)
            continue

    dfs = [pd.read_parquet(f) for f in parquet_path_list]
    merged_df = pd.concat(dfs, ignore_index=True)

    filename = os.path.splitext(os.path.basename(parquet_path_list[0]))[0]
    output_parquet_path = os.path.join(output_dir, f"{filename}_{label}.parquet")

    merged_df.to_parquet(output_parquet_path, index=False)
    logging.info(f"Successfully merged GeoParquets into {output_parquet_path}")

    return


def validate_dirs_and_get_pathlists(input_dirs):
    '''

    '''
    logging.info("Validating input directories and getting output filepaths...")

    sites_gpkg_path_list = []
    sites_csv_path_list = []
    sites_parquet_path_list = []
    library_gpkg_path_list = []
    library_csv_path_list = []
    library_parquet_path_list = []

    catfim_type_first = None
    fim_run_dir_first = None
    past_major_interval_cap_first = None
    search_first = None

    for dir in input_dirs:

        # Validate input path
        if not os.path.exists(dir):
            msg = f"Directory does not exist: {dir}"
            logging.error(msg)
            raise FileNotFoundError(msg)

        # Get catfim_type_name from the runtime_args.env file in the primary_dir
        csf.load_runtime_args(dir)
        catfim_type = os.getenv('CATFIM_TYPE')
        fim_run_dir = os.getenv('FIM_RUN_DIR')
        past_major_interval_cap = os.getenv('PAST_MAJOR_INTERVAL_CAP')
        search = os.getenv('SEARCH')

        # Confirm that the values match the first dir (or set the values if it's the first dir)
        if catfim_type_first is None:
            catfim_type_first = catfim_type
        else:
            if catfim_type != catfim_type_first:
                msg = f"CATFIM_TYPE in {dir} is {catfim_type}, which differs from the value in the first dir ({catfim_type_first})"
                logging.error(msg)
                raise ValueError(msg)

        if fim_run_dir_first is None:
            fim_run_dir_first = fim_run_dir
        else:
            if fim_run_dir != fim_run_dir_first:
                msg = f"FIM_RUN_DIR in {dir} is {fim_run_dir}, which differs from the value in the first dir ({fim_run_dir_first})"
                logging.error(msg)
                raise ValueError(msg)

        if past_major_interval_cap_first is None:
            past_major_interval_cap_first = past_major_interval_cap
        else:
            if past_major_interval_cap != past_major_interval_cap_first:
                msg = f"PAST_MAJOR_INTERVAL_CAP in {dir} is {past_major_interval_cap}, which differs from the value in the first dir ({past_major_interval_cap_first})"
                logging.error(msg)
                raise ValueError(msg)

        if search_first is None:
            search_first = search
        else:
            if search != search_first:
                msg = f"SEARCH in {dir} is {search}, which differs from the value in the first dir ({search_first})"
                logging.error(msg)
                raise ValueError(msg)

        if catfim_type == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        # Get output filepaths for the directories
        (
            sites_gpkg_path,
            sites_csv_path,
            sites_parquet_path,
            library_gpkg_path,
            library_csv_path,
            library_parquet_path,
        ) = cpp.get_output_filepaths(dir, catfim_type_name)

        sites_gpkg_path_list.append(sites_gpkg_path)
        library_gpkg_path_list.append(library_gpkg_path)
        sites_csv_path_list.append(sites_csv_path)
        library_csv_path_list.append(library_csv_path)
        sites_parquet_path_list.append(sites_parquet_path)
        library_parquet_path_list.append(library_parquet_path)
    # End loop

    return (
        sites_gpkg_path_list,
        sites_csv_path_list,
        sites_parquet_path_list,
        library_gpkg_path_list,
        library_csv_path_list,
        library_parquet_path_list,
    )


def rollup_logs(input_dirs, output_dir):
    '''

    '''
    final_log_path = os.path.join(output_dir, "ALL_LOGS_combined.log")

    for dir in input_dirs:

        log_folder_path = os.path.join(dir, "logs")

        # Get the most recent log file in the folder
        dir_log_file_name, num_log_files_avail = gcf.get_most_recent_log_file(log_folder_path, "ALL_LOGS_")

        # Exit this process if the log file doesn't exist, because that means we didn't get very far
        # into processing for this dir and we don't have logs to add for this HUC.
        if dir_log_file_name is None:
            logging.warning(f"{dir} - No logs found, skipping adding logs to final logs.")
            continue

        if num_log_files_avail > 1:
            logging.info(
                f"{dir} - {num_log_files_avail} logs available. Using most recent log: {dir_log_file_name}"
            )


        # Copy the dir log file to the final log path if it doesn't exist yet
        if not os.path.exists(final_log_path):
            sf.copy_file(dir_log_file_name, final_log_path)
            logging.info(f"Copied {dir_log_file_name} to {final_log_path}")
        else:
            # logging.info(f"Final log file already exists: {final_log_path}")

            # Append HUC .log file to gen .log file
            log_concat_success = sf.rollup_log_files(
                dir_log_file_name, final_log_path, remove_old_src_file=False
            )

            # Print warning if needed
            if not log_concat_success:
                logging.info(
                    f'{dir} - WARNING: Unable to concat to final log: {os.path.basename(dir_log_file_name)}'
                )
    # End log rollup loop

    return



def combine_final_outputs(output_dir, input_dirs, label):
    '''
    
    '''

    is_logging_loaded = False
    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    # Check if output_dir exists, make it if needed (and error out if it fails)
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            os.chmod(output_dir, 0o777)  # 777 (rwxrwxrwx)
            print(f"Created output directory: {output_dir}")
        except Exception as e:
            print(f"Failed to create output directory: {output_dir}. Error: {e}")
            raise

    # Turn input_dirs into a list and make sure there's at least two directories to combine
    input_dirs = input_dirs.split()

    if len(input_dirs) < 2:
        msg = "At least two input directories are required to combine CatFIM outputs."
        raise ValueError(msg)

    log_file_path = sf.setup_file_logger(output_dir, "catfim_combine_final_outputs")
    is_logging_loaded = True

    logging.info(f"Begin combining CatFIM final outputs at {dt_string} (UTC)")
    logging.info("")
    print(f"Logs will be saved to {log_file_path}")
    logging.info(f"Input directories: {input_dirs}")
    logging.info(f"Output directory: {output_dir}")

    try:

        # ------
        # Iterate through input folders. For each input folder, validate that the args match the first args, and then get a list of the filepaths
        (
            sites_gpkg_path_list,
            sites_csv_path_list,
            sites_parquet_path_list,
            library_gpkg_path_list,
            library_csv_path_list,
            library_parquet_path_list,
        ) = validate_dirs_and_get_pathlists(input_dirs)

        # ------
        # Loop through pathlists and compile the outputs

        # Merge GPKGs
        merge_gpkgs(sites_gpkg_path_list, output_dir, label)
        merge_gpkgs(library_gpkg_path_list, output_dir, label)

        # Merge CSVs
        merge_csvs(sites_csv_path_list, output_dir, label)
        merge_csvs(library_csv_path_list, output_dir, label)

        # Merge GeoParquets
        merge_geoparquets(sites_parquet_path_list, output_dir, label)
        merge_geoparquets(library_parquet_path_list, output_dir, label)

        # ------
        # Roll up all the logs from the input directories into a single log file in the output directory
        rollup_logs(input_dirs, output_dir)

        # ------
        # TODO: Could add a section where we roll up all the runtime_args.env files from the input directories into a single runtime_args.env file in the output directory
        # -> Probably not needed for now

        # TODO: Could add a section where we copy all of the folders in the huc directories into the output huc directory
        # -> Probably not needed for now

        logging.info(
            'Successfully combined CatFIM outputs into new files in the output directory.'
        )

    except Exception as ex:
        trace_error = traceback.format_exc()
        err_msg = f"Critical error has occurred:. Error: {ex} Detail: {trace_error}"

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

    return


if __name__ == '__main__':
    '''
    Joins the CatFIM outputs from a secondary folder to the outputs in a primary folder. The outputs are merged into new files in the primary folder with a label added to the filename.

    Arguments
    ----------
    output-dir (-od) - str
        REQUIRED: Path to directory where combined CatFIM outputs will be saved
    input-dirs (-id) - str
        REQUIRED: Space-delimited list of paths to directories containing CatFIM outputs (to be joined)
    label (-l) - str
        OPTIONAL: Label for the output files (to differentiate them from the original primary outputs)

    Example
    -------

    python /foss_fim/tools/catfim/catfim_combine_final_outputs.py -od /data/catfim/emily_test/4_9_20_1_stage_based -id "/data/catfim/emily_test/guam_4_9_20_1_stage_based/ /data/catfim/emily_test/4_9_20_1_stage_based" -l 'w_Guam'


    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Join CatFIM outputs from a list of input directories')

    parser.add_argument(
        '-od',
        '--output-dir',
        help='REQUIRED: Path to directory where combined CatFIM outputs will be saved',
        required=True,
    )

    parser.add_argument(
        '-id',
        '--input-dirs',
        help='REQUIRED: Space-delimited list of paths to directories containing CatFIM outputs (to be joined to the primary)',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--label',
        help='OPTIONAL: Label for the output files (to differentiate them from the original primary outputs)',
        required=False,
        default='combined'
    )

    args = vars(parser.parse_args())

    # Call main program
    combine_final_outputs(**args)
