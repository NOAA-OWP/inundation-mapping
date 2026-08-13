#!/usr/bin/env python3

import argparse
import logging
import os
import shutil
import traceback
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd

import src.utils.shared_functions as sf
import tools.catfim.catfim_post_processing as cpp
import tools.catfim.catfim_shared_functions as csf
import tools.catfim.generate_categorical_fim as gcf


# Force GDAL to use standard locking and synchronous write modes
# helps with gpkg.to_file writes
os.environ["GDAL_GEO_TRUNCATE_JOURNAL"] = "YES"
os.environ["OGR_SQLITE_SYNCHRONOUS"] = "OFF"  # Speeds up network writes

'''
This tool is being used to combine the outputs of multiple CatFIM runs into a single set of outputs.

The CatFIM directories must have the same values for CATFIM_TYPE, FIM_RUN_DIR, PAST_MAJOR_INTERVAL_CAP,
and SEARCH (all found in the runtime_args.env file of the directories).

The outputs are merged into new files in the output directory with a label added to the filename.

'''


def merge_library_gpkgs(gpkg_path_list):
    '''
    Merge library gpkgs and create an output table showing which library polygons are from which
    CatFIM run source.

    Arguments
    ---------
    gpkg_path_list - list of str
        List of paths to the GeoPackage files to be merged

    Returns
    -------
    gdfs - list of GeoDataFrames
        List of GeoDataFrames read from the GeoPackage files, with duplicate HUCs removed
    library_source_df
    '''

    logging.info("")
    logging.info("Begin merging library GPKGs...")

    # Get rid of any paths that don't exist and log a warning
    for path in gpkg_path_list:
        if not os.path.exists(path):
            logging.warning(f"Warning: File not found -> {path}")
            gpkg_path_list.remove(path)  # Remove from list if file doesn't exist
            continue

    # Read and concatenate all files
    gdfs = []
    data_source_list = []
    hucs_added = set()  # Keep track of HUCs that have already been added to the merged_gdf

    for f in gpkg_path_list:
        logging.info("")
        logging.info(f'CatFIM run: {os.path.basename(os.path.dirname(f))}')
        logging.info(f"Library GPKG: {f}")

        # Read library gpkg
        gdf = gpd.read_file(f)

        # TODO: Deal with interval_stage_x and interval_stage_y?

        # Define desired data types for specific columns
        dtype_mapping = {
            'huc8': 'str',
            'status': 'str',
            'wfo': 'str',
            'rfc': 'str',
            'state': 'str',
            'county': 'str',
            'name': 'str',
            'magnitude': 'str',
            'stage': 'float64',
            'stage_uni': 'str',
            'stage_src': 'str',
            'rfc_stage': 'float64',
            'datum_adj_ft': 'float64',
            'datum_adj_wse_ft': 'float64',
            'gage_zero_elev_ft': 'float64',
            'is_interval': 'bool',
            'interval_stage': 'float64',
            'hand_dem_elev_ft': 'float64',
            'hand_stage': 'float64',
        }

        for colname, coltype in dtype_mapping.items():
            if colname in gdf.columns:
                gdf[colname] = gdf[colname].astype(coltype)

        # Update the huc8 column to ensure 8-digit strings with leading zeros
        gdf["huc8"] = gdf["huc8"].astype(str).str.zfill(8)

        # Get a list of HUCs in the library
        huc_list = gdf['huc8'].unique()
        logging.info(f"Found {len(huc_list)} unique HUCs")

        # Get the list of HUCs that are in the HUC list and not already in hucs_added
        huc_list_filtered = [huc for huc in huc_list if huc not in hucs_added]

        # Log the duplicate HUCs that are being skipped
        duplicate_hucs = set(gdf['huc8'].unique()) - set(huc_list_filtered)
        if duplicate_hucs:
            logging.warning(
                f"Skipping {len(duplicate_hucs)} duplicate HUCs (already merged from previous GPKGs):"
            )
            logging.warning(duplicate_hucs)

        # Filter out HUCs that have libraries that have already been added
        gdf = gdf[gdf['huc8'].isin(huc_list_filtered)]
        gdfs.append(gdf)

        # Update hucs_added with the HUCs from the current gdf
        hucs_added.update(huc_list_filtered)
        logging.info(f"Added {len(gdf)} rows to the merged GeoDataFrame.")
        logging.info(f"Total unique HUCs added so far: {len(hucs_added)}")

        # Update the data source list to show which HUCs from this library source are being added
        data_source_f = {
            'catfim_run_folder': os.path.basename(os.path.dirname(f)),
            'library_gpkg': f,
            'library_huc_list': huc_list_filtered,
        }
        data_source_list.append(data_source_f)

    library_source_df = pd.DataFrame(data_source_list)

    logging.info("")
    logging.info(f"Compiled library GDFs for {len(hucs_added)} total unique HUCs")
    logging.info("")

    return gdfs, library_source_df


def merge_sites_gpkgs(library_source_df):
    '''
    Gets a df of which HUCs to get from which CatFIM run source, generates a gpkg list,
    and iterates through the library GPKGs to merge the correct HUCs into the output
    library gdf list.

    Arguments
    ---------
    library_source_df - DataFrame
        Dataframe with the columns huc, library_gpkg, and catfim_run_folder. Shows which sources the library
        data has come from (so we can make sure we get the sites data from the same source).

    Returns
    -------
    gdfs - list of GeoDataFrames
        List of GeoDataFrames read from the GeoPackage files, with duplicate HUCs removed
    gpkg_path_list - list of str
        List of paths to the GeoPackage files to be merged
    '''
    logging.info("")
    logging.info("Begin merging sites GPKGs...")

    # Get a list of unique catfim_run_folder values from the library_source_df
    library_gpkg_list = library_source_df['library_gpkg'].unique().tolist()

    # Get a list of sites gpkgs from the library gpkg list
    sites_gpkg_list = [item.replace("library", "sites") for item in library_gpkg_list]

    # Get rid of any paths that don't exist and log a warning
    for path in sites_gpkg_list:
        if not os.path.exists(path):
            logging.warning(f"Warning: File not found -> {path}")
            sites_gpkg_list.remove(path)  # Remove from list if file doesn't exist
            continue

    gdfs = []
    hucs_added = []  # Keeps track of HUCs that have already been added to the merged_gdf
    col_types_dict = {}

    # Read and concatenate all files
    for f in sites_gpkg_list:
        logging.info("")
        logging.info(f"Reading sites GPKG: {f}")

        # Read gpkg
        gdf = gpd.read_file(f)

        if len(col_types_dict) == 0:
            col_types_dict = gdf.dtypes.to_dict()
        else:
            for col in col_types_dict.keys():
                if col not in gdf.columns:
                    logging.info(f'Added missing col: {col}')
                    gdf[col] = None
            gdf = gdf.astype(col_types_dict)

        # Fix coltypes as needed
        gdf['nwm_seg'] = gdf['nwm_seg'].astype(str)

        huc_list = gdf['huc8'].unique()
        logging.info(f"Found {len(huc_list)} unique HUCs")

        # Get the library HUC list for this CatFIM run
        catfim_run_folder_f = os.path.basename(os.path.dirname(f))
        logging.info(f'CatFIM run: {catfim_run_folder_f}')

        [library_huc_list_f] = library_source_df[
            library_source_df['catfim_run_folder'] == catfim_run_folder_f
        ]['library_huc_list'].tolist()
        logging.info(
            f'{len(library_huc_list_f)} HUCs used this source for their library, adding their sites gpkgs'
        )

        # Get the sites values for the HUCs that we have the library from this source
        gdf = gdf[gdf['huc8'].isin(library_huc_list_f)]
        gdfs.append(gdf)

        # Update hucs_added with the HUCs from the current gdf
        hucs_added.extend(library_huc_list_f)

        logging.info(f"Added {len(gdf)} rows to the merged GeoDataFrame.")
        logging.info(f"Total unique HUCs added so far: {len(hucs_added)}")

    logging.info("")
    logging.info(f"Compiled sites GDFs for {len(hucs_added)} total unique HUCs")
    logging.info("")

    return gdfs, sites_gpkg_list


def validate_dirs_and_get_pathlists(input_dirs):
    '''
    Validate input filepaths and get path lists for sites and library GPKGs.

    Arguments
    ---------
    input_dirs - list of str
        List of paths to directories containing CatFIM outputs (to be joined)

    Returns
    -------
    library_gpkg_path_list - list of str
        List of paths to the library GeoPackage files in the input directories

    '''
    logging.info("")
    logging.info("Validating input directories and getting output filepaths...")

    library_gpkg_path_list = []
    catfim_type_first, fim_run_dir_first, past_major_interval_cap_first, search_first = None, None, None, None

    # Get the args from the first dir
    first_dir = input_dirs[0]

    # Get catfim_type_name from the runtime_args.env file in the primary_dir
    csf.load_runtime_args(first_dir)
    catfim_type_first = os.getenv('CATFIM_TYPE')
    fim_run_dir_first = os.getenv('FIM_RUN_DIR')
    past_major_interval_cap_first = os.getenv('PAST_MAJOR_INTERVAL_CAP')
    search_first = os.getenv('SEARCH')

    # Print args from first dir
    logging.info("")
    logging.info(f"Getting runtime args from first directory: {first_dir}")
    logging.info(f"CATFIM_TYPE: {catfim_type_first}")
    logging.info(f"FIM_RUN_DIR: {fim_run_dir_first}")
    logging.info(f"PAST_MAJOR_INTERVAL_CAP: {past_major_interval_cap_first}")
    logging.info(f"SEARCH: {search_first}")
    logging.info("")

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

        # Confirm that the values match the first dir
        if catfim_type_first != catfim_type:
            msg = f"CATFIM_TYPE in {dir} is {catfim_type}, which differs from value in first dir ({catfim_type_first})"
            logging.error(msg)
            raise ValueError(msg)

        if fim_run_dir_first != fim_run_dir:
            msg = f"FIM_RUN_DIR in {dir} is {fim_run_dir}, which differs from value in first dir ({fim_run_dir_first})"
            logging.error(msg)
            raise ValueError(msg)

        if past_major_interval_cap_first != past_major_interval_cap:
            msg = f"PAST_MAJOR_INTERVAL_CAP in {dir} is {past_major_interval_cap}, which differs from value in first dir ({past_major_interval_cap_first})"
            logging.error(msg)
            raise ValueError(msg)

        if search_first != search:
            msg = f"SEARCH in {dir} is {search}, which differs from value in first dir ({search_first})"
            logging.error(msg)
            raise ValueError(msg)

        if catfim_type == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        # Get output filepaths for the directories
        (__,__,__,library_gpkg_path,__,__,) = cpp.get_output_filepaths(dir, catfim_type_name)

        # TODO: Currently this is just working with gpkgs but we could switch to parquets if that helps with memory/processing
        library_gpkg_path_list.append(library_gpkg_path)

    logging.info(f"Compiled {len(library_gpkg_path_list)} library gpkg filepaths")

    return library_gpkg_path_list


def rollup_logs(input_dirs, output_dir):
    '''
    Combine final output logs from the input dirs.

    Arguments
    ---------
    output_dir - str
        Path to the output directory where the compiled files will be saved
    input_dirs - str
        Space-delimited list of paths to directories containing CatFIM outputs (to be joined)

    '''
    logging.info("Compiling final logs...")
    final_log_path = os.path.join(
        output_dir, f"ALL_LOGS_combined_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )

    for dir in input_dirs:
        logging.info(f"Processing logs for {os.path.basename(dir)}")
        log_folder_path = os.path.join(dir, "logs")

        # Get the most recent log file in the folder
        dir_log_file_name, num_log_files_avail = gcf.get_most_recent_log_file(log_folder_path, "ALL_LOGS_")

        if dir_log_file_name is None:
            logging.warning(f"{dir} - No logs found, skipping adding logs to final logs.")
            continue

        if num_log_files_avail > 1:
            logging.info(f"{num_log_files_avail} logs available. Using most recent log: {dir_log_file_name}")

        dir_log_file_path = os.path.join(log_folder_path, dir_log_file_name)

        # Make the warning and errors filenames too
        gen_logs_path_list = gcf.make_logs_path_list(dir_log_file_path)
        final_logs_path_list = gcf.make_logs_path_list(final_log_path)

        for path, final_path in zip(gen_logs_path_list, final_logs_path_list):

            if not os.path.exists(final_path):
                # Copy the dir log file to the final log path if it doesn't exist yet
                shutil.copyfile(path, final_path)
                logging.info(f"  Copied {os.path.basename(path)} to {os.path.basename(final_path)}")
            else:
                # If final log exists, append HUC .log file to gen .log file
                log_concat_success = sf.rollup_log_files(path, final_path, remove_old_src_file=False)
                logging.info(f"  Copying {os.path.basename(path)} into {os.path.basename(final_path)}")

                if not log_concat_success:
                    logging.warning(
                        f'Unable to concat to final log for {dir} (Log: {os.path.basename(path)})'
                    )
    logging.info("Finished rolling up final logs")

    return


def save_compiled_outputs(gdfs, gpkg_path_list, output_dir, label, file_type):
    '''
    Save the compiled GDFs as output CSVs, GPKGs, and parquets.

    Arguments
    ---------
    gdfs - list of GeoDataFrames
        List of GeoDataFrames to be saved
    gpkg_path_list - list of str
        List of paths to the original GeoPackage files (used to derive the output filename)
    output_dir - str
        Path to the output directory where the compiled files will be saved
    label - str
        Label to be added to the output filenames
    file_type - str
        Type of file being saved (e.g., 'sites' or 'library') for logging and file naming
    '''

    # Get filename without extension for the layer name (i.e. flow_based_catfim_sites)
    filename = os.path.splitext(os.path.basename(gpkg_path_list[0]))[0]

    # Make filenames (i.e. compiled_flow_based_catfim_library.csv)
    merged_gpkg_path = os.path.join(output_dir, f"{label}_{filename}.gpkg")
    merged_parquet_path = os.path.join(output_dir, f"{label}_{filename}.parquet")
    merged_csv_path = os.path.join(output_dir, f"{label}_{filename}.csv")

    for path in [merged_gpkg_path, merged_parquet_path, merged_csv_path]:
        if os.path.exists(path):
            logging.warning(f'File already exists at {path}, file will be overwritten.')
            # TODO: Do we want to toggle an override option? or make sure we're not
            # overwriting original outputs? no.. I guess that's what the label protects
            # against...

    # Concatenate sites GeoDataFrames into one GDF and update LID column name
    logging.info('Begin concatenating gdf list into single gdf...')
    compiled_gdf = gpd.pd.concat(gdfs, ignore_index=True)
    compiled_gdf.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)  # TODO: Is this still necessary?
    logging.info('Finished creating single gdf')

    # Save the compiled GeoDataFrames to GeoPackage files
    compiled_gdf.to_file(
        merged_gpkg_path,
        driver='GPKG',
        # engine='fiona',  # might be more correct but kept crashing the tool...
        index=False,
    )
    logging.info(f"Saved {file_type} GeoPackage to {merged_gpkg_path}")

    # Save the GeoDataFrames to GeoParquet files
    compiled_gdf.to_parquet(merged_parquet_path, index=False)
    logging.info(f"Saved {file_type} GeoParquet to {merged_parquet_path}")

    # Drop geometry column and save the csv versions
    compiled_df = compiled_gdf.drop(columns=['geometry'])
    compiled_df.to_csv(merged_csv_path, index=False)
    logging.info(f"Saved {file_type} CSV to {merged_csv_path}")
    logging.info("")

    return


def combine_final_outputs(output_dir, input_dirs, label):
    '''
    Main function.

    Arguments
    ---------
    output_dir - str
        Path to the output directory where the compiled files will be saved
    input_dirs - str
        Space-delimited list of paths to directories containing CatFIM outputs (to be joined)
    label - str
        Label to be added to the output filenames
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

    log_file_path = sf.setup_file_logger(output_dir, "combine_final_outputs")
    is_logging_loaded = True

    print('\n==================================================================\n')
    logging.info(f"Begin combining CatFIM final outputs at {dt_string} (UTC)")
    logging.info("")
    print(f"Logs will be saved to {log_file_path}")
    logging.info("Input directories:")
    for dir in input_dirs:
        logging.info(f" {dir}")
    logging.info("Output directory:")
    logging.info(f" {output_dir}")

    try:

        # ------
        # Iterate through input folders.
        # For each input folder, validate that the args match the first args, and then get a list of the filepaths
        library_gpkg_path_list = validate_dirs_and_get_pathlists(input_dirs)

        # ------
        # Compile and save library GDFs

        gdfs_library, library_source_df = merge_library_gpkgs(library_gpkg_path_list)

        if len(gdfs_library) > 0:
            logging.info('Begin saving compiled library data...')
            save_compiled_outputs(gdfs_library, library_gpkg_path_list, output_dir, label, 'library')

            source_path = os.path.join(output_dir, 'combine_final_outputs_data_sources.csv')
            library_source_df.to_csv(source_path, index=False)

            del gdfs_library  # to save on storage
        else:
            logging.warning("No library GeoDataFrames were found to combine.")

        # ------
        # Compile and save sites GDFs

        gdfs_sites, sites_gpkg_path_list = merge_sites_gpkgs(library_source_df)

        if len(gdfs_sites) > 0:
            logging.info('Begin saving compiled sites data...')
            save_compiled_outputs(gdfs_sites, sites_gpkg_path_list, output_dir, label, 'sites')

            del gdfs_sites
        else:
            logging.warning("No sites GeoDataFrames were found to combine.")

        # ------
        # Roll up all the logs from the input directories into a single log file in the output directory
        rollup_logs(input_dirs, output_dir)

        # ------
        # TODO: Could add a section where we roll up all the runtime_args.env files from the input directories into a single runtime_args.env file in the output directory
        # -> Probably not needed for now

        # TODO: Could add a section where we copy all of the folders in the huc directories into the output huc directory
        # -> Probably not needed for now

        logging.info("")
        logging.info('Successfully combined CatFIM outputs into new files in the output directory.')
        print('\n==================================================================\n')

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

    python /foss_fim/tools/catfim/catfim_combine_final_outputs.py -t /data/catfim/emily_test/4_9_20_1_stage_based -i "/data/catfim/emily_test/guam_4_9_20_1_stage_based/ /data/catfim/emily_test/4_9_20_1_stage_based" -l 'w_Guam'

    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Join CatFIM outputs from a list of input directories')

    parser.add_argument(
        '-t',
        '--output-dir',
        help='REQUIRED: Path to directory where combined CatFIM outputs will be saved',
        required=True,
    )

    parser.add_argument(
        '-i',
        '--input-dirs',
        help='REQUIRED: Space-delimited list of paths to directories containing CatFIM outputs (to be joined to the primary)',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--label',
        help='OPTIONAL: Label for the output files (to differentiate them from the original primary outputs)',
        required=False,
        default='combined',
    )

    args = vars(parser.parse_args())

    # Call main program
    combine_final_outputs(**args)
