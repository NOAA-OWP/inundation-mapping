#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import re
import subprocess
import pandas as pd
import traceback
from datetime import date, datetime, timezone

import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh
import tools.catfim.catfim_shared_functions as csf




def create_output_folder(output_folder_location):
    '''
    Creates the output folders needed for processing (if needed).

    Creates:
        {output_folder_location}/catfim_hecras_preprocessing/
        {output_folder_location}/catfim_hecras_preprocessing/temp/

    '''
    mode = 0o777  # allows read, write, and execute for all (rwxrwxrwx)

    # Make output folder
    output_folder = os.path.join(output_folder_location, 'catfim_hecras_preprocessing')
    os.makedirs(output_folder, exist_ok=True, mode=mode)

    if not os.path.exists(output_folder):
        raise Exception(f'Unable to create output folder at {output_folder_location}')

    # Make intermediates folder
    intermediates_folder = os.path.join(output_folder, 'temp')
    os.makedirs(intermediates_folder, exist_ok=True, mode=mode)

    if not os.path.exists(intermediates_folder):
        raise Exception(f'Unable to intermediates folder at {output_folder}')

    return output_folder, intermediates_folder


def create_flows_files(threshold_file, nwm_meta_file, intermediates_folder, magnitude_types):
    '''
    Creates flows CSV files for each magnitude type (action, minor, moderate, major, record) using the thresholds and metadata.
    Saves the CSVs to the intermediate files path and returns a dictionary with magnitude type as key and flows CSV filepath as value.
    '''
    logging.info('')
    logging.info('Begin creating flows files...')

    # Read in the thresholds data and metadata
    thresh_df = pd.read_pickle(threshold_file)
    meta_list = pd.read_pickle(nwm_meta_file)

    # Filter out rows where threshold_type is not "flows"
    thresh_df = thresh_df[thresh_df['threshold_type'] == 'flows']

    # Use pd.melt to pivot the df to long format
    long_thresh_df = pd.melt(
        thresh_df,
        id_vars=['nws_lid'],
        value_vars=['action', 'minor', 'moderate', 'major', 'record'],
        var_name='magnitude_type',
        value_name='magnitude_value',
    )

    # Remove rows where magnitude_value is -1.0 (THRESH_NODATA_VALUE) these are where it was NaN in the database
    long_thresh_df = long_thresh_df[
        long_thresh_df['magnitude_value'] != csf.THRESH_NODATA_VALUE
    ]

    # Make a df that has the nws_lid and the nwm_feature_id
    identifiers_row_list = []
    for item in meta_list:
        row_dict = {
            'nws_lid': item['identifiers']['nws_lid'],
            'nwm_feature_id': item['identifiers']['nwm_feature_id'],
        }
        identifiers_row_list.append(row_dict)

    # Make the list into a df
    identifiers_df = pd.DataFrame(identifiers_row_list)

    # Save this flows CSV to a folder
    identifiers_csv_filepath = os.path.join(intermediates_folder, 'identifiers.csv')
    identifiers_df.to_csv(identifiers_csv_filepath, index=False)

    logging.info(f'Created identifiers df with {len(identifiers_df)} rows, saved to {os.path.basename(identifiers_csv_filepath)}')

    flows_csv_dict = {}

    # For each magnitude, create a flows CSV with the following cols: nwm_feature_id,discharge
    for magnitude in magnitude_types:

        logging.info(f'{magnitude} - Processing flows...')

        # Filter long_thresh_df to just be the magnitude (Colnames: nws_lid, magnitude_type, magnitude_value)
        mag_thresh_df = long_thresh_df[long_thresh_df['magnitude_type'] == magnitude]

        # Add a nwm_feature_id column to the mag thresh df (using the identifiers_df)
        # Colnames: nws_lid, magnitude_type, magnitude_value, nwm_feature_id
        mag_thresh_df = pd.merge(mag_thresh_df, identifiers_df, on='nws_lid', how='left')

        # Make a discharge column (which is the magnitude_value column)
        mag_thresh_df['discharge'] = mag_thresh_df['magnitude_value']

        # Create a table with the following colnames: nwm_feature_id, discharge (formerly magnitude_value)
        mag_flows_df =  mag_thresh_df[['nwm_feature_id', 'discharge']].copy().dropna()

        # Save this flows CSV to a folder
        flows_csv_filename = f'flows_{magnitude}.csv'
        flows_csv_filepath = os.path.join(intermediates_folder, flows_csv_filename)

        mag_flows_df.to_csv(flows_csv_filepath, index=False)

        if not os.path.exists(flows_csv_filepath):
            logging.error(f'{magnitude} - Failed to save flows file to {os.path.basename(intermediates_folder)}/{flows_csv_filename}')

        else:
            # If save was sucessful, add flows CSV to filepath dictionary
            flows_csv_dict[magnitude] = flows_csv_filepath
            logging.info(f'{magnitude} - Saved flows file to {os.path.basename(intermediates_folder)}/{flows_csv_filename}')

    logging.info('Finished creating flow files!')

    return flows_csv_dict, identifiers_csv_filepath


def run_controls(magnitude, ripple_path, model_name, flows_filename, flows2fim_path, intermediates_folder):
    '''
    Runs flows2fim controls for a given model and magnitude, using the specified flows file.
    Saves the output CSV to the intermediate files path.

    '''
    logging.info('')
    logging.info(f'{model_name} : {magnitude} - Run flows2fim controls subprocess')

    # Create the input and output file paths
    model_path = os.path.join("ripple", ripple_path, "collections", model_name)
    db_path = os.path.join(model_path, "ripple.gpkg")
    flows_csv = os.path.join(intermediates_folder, flows_filename)
    starts_csv = os.path.join(model_path, "start_reaches.csv")
    controls_filename = f'controls_{model_name}_{magnitude}.csv'
    output_csv = os.path.join(intermediates_folder, controls_filename)

    # Validate input paths
    input_path_list = [model_path, db_path, flows_csv, starts_csv]
    for path in input_path_list:
        if not os.path.exists(path):
            msg = f'Input file {path} does not exist. Cannot run controls for model {model_name} and magnitude {magnitude}.'
            logging.critical(msg)
            raise Exception

    try:
        # Use subprocess to run flows2fim controls
        result = subprocess.run(
            [
                flows2fim_path,
                "controls",
                "-db", db_path,
                "-f", flows_csv,
                "-scsv", starts_csv,
                "-o", output_csv
            ], 
            capture_output=True,
            text=True
        )
    except subprocess.CalledProcessError as e:
        # Handles non-zero exit codes (e.g., command not found or invalid args)
        msg = f"Command failed with exit code {e.returncode}: {e.stderr}"
        logging.critical(msg)
        raise Exception

    except FileNotFoundError as e:
        # Handles cases where the executable itself cannot be found
        msg = f"Unable to find program. {e}"
        logging.critical(msg)
        raise Exception

    # Parse outputs for common errors and record reach_ids where they occur
    common_warning_list = ["Large difference in target vs found flow", "Flow not found for reach", "Large difference in target vs found control reach stage"]
    parse_subprocess_outputs(result, common_warning_list, model_name, magnitude)

    # Validate that output file was created
    if not os.path.exists(output_csv):
        logging.error(f'{model_name} : {magnitude} - [flow2fim controls] Controls output file {os.path.basename(output_csv)} not created')
        return None
    else:
        logging.info(f'{model_name} : {magnitude} - [flow2fim controls] Saved controls file as {os.path.basename(output_csv)}')

    return output_csv


def run_controls_for_all_models_and_magnitudes(magnitude_types, flows_csv_dict, collections_path, ripple_path, flows2fim_path, intermediates_folder, identifiers_csv_filepath, lst_models, output_folder):
    '''
    Run flows2fim controls for each model and magnitude
    '''
    logging.info('')
    logging.info("Begin running controls....")
    logging.info(f"Getting model collections from {collections_path}")

    # Get a list of all available model collections form ripple_path
    all_collections_lst = os.listdir(collections_path)

    # If lst_models is all, get a list of them
    if lst_models == 'all':
        collection_list = all_collections_lst

    else:
        # Values in lst_models that are NOT in all_collections_lst
        missing_collection_list = [item for item in lst_models if item not in all_collections_lst]

        if len(missing_collection_list) > 0:
            logging.warning(f'Unable to find the following model collection(s): {missing_collection_list}')

        # Values in the input list that ARE available
        collection_list = [item for item in lst_models if item in all_collections_lst]

    if len(collection_list) == 0:
        msg = 'No model collections found. Double check filepaths and input lists.'
        logging.critical(msg)
        raise Exception

    logging.info(f'Found {len(collection_list)} model collection(s) to process: {collection_list}')

    # Read identifiers_csv_filepath
    identifiers_df = pd.read_csv(identifiers_csv_filepath)

    controls_output_csv_list = []
    for model_name in collection_list:
        logging.info('')
        logging.info(f'{model_name} - Running controls')
        section_start_dt = datetime.now(timezone.utc)

        for magnitude in magnitude_types:
            flows_filename = flows_csv_dict[magnitude]

            controls_output_csv = run_controls(magnitude, ripple_path, model_name, flows_filename, flows2fim_path, intermediates_folder)

            if controls_output_csv is None:
                logging.warning(f'{model_name} : {magnitude} - No controls CSV created, error likely occurred')
                continue

            # Read the output CSV and add the necessary columns
            df = pd.read_csv(controls_output_csv)
            df['magnitude'] = magnitude
            df['model_collection'] = model_name
            df['collection_parent_folder'] = ripple_path

            # Join the identifiers_df to the controls output df to add the nws_lid column (joining on reach_id for df and nwm_feature_id for identifiers df)
            df = pd.merge(df, identifiers_df, left_on='reach_id', right_on='nwm_feature_id', how='left')

            df.to_csv(controls_output_csv, index=False)
            controls_output_csv_list.append(controls_output_csv)

            logging.info(f"{model_name} : {magnitude} - Updated controls CSV with additional metadata columns")

        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f'{model_name} - Finished running controls for {model_name} - {dur_msg}')

    logging.info("")
    logging.info(f"Finished running controls for {len(collection_list)} models.")

    # Compile the outputs of the controls in controls_output_csv_list
    combined_df = pd.concat([pd.read_csv(f) for f in controls_output_csv_list], ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    date_formatted = date.today().strftime("%Y%m%d")
    compiled_outputs_path = os.path.join(output_folder, f'combined_controls_output_{date_formatted}.csv')
    combined_df.to_csv(compiled_outputs_path, index=False)

    logging.info(f'Compiled controls output saved to {os.path.basename(compiled_outputs_path)}')

    return compiled_outputs_path


def parse_subprocess_outputs(result, common_warning_list, model_name, magnitude):

    stderr_list = result.stderr.splitlines()

    # Get the reach ID's for each occurrence of each common warning
    for common_warning in common_warning_list:
        reach_id_list = []
        for line in stderr_list:
            if common_warning in line:
                # Get the reach ID
                match = re.search(rf"{re.escape("reach_id=")}\s*(\d{{7}})", line)
                if match:
                    reach_id = match.group(1)
                    reach_id_list.append(reach_id)

        if len(reach_id_list) > 0:
            logging.info(f'{model_name} : {magnitude} - [flow2fim controls] {common_warning} (Returned for {len(reach_id_list)} Reach IDs)')
            # logging.info(f'{model_name} : {magnitude} - [flow2fim controls] Reach IDs: {reach_id_list}')  # Too many feature IDs to print (could toggle for debugging)

    # Get warnings that aren't in the common warnings list
    uncommon_warning_list = [s for s in stderr_list if not any(k in s for k in common_warning_list)]

    for uncommon_warning in uncommon_warning_list:
        logging.info(f'{model_name} : {magnitude} - [flow2fim controls] {uncommon_warning}')

    return


def create_site_model_table(compiled_outputs_path, output_folder):
    logging.info('')
    logging.info('Creating list of sites with HEC-RAS models available...')

    # Read compiled_outputs_path and filter out rows that have NaN in the nws_lid column 
    compiled_df = pd.read_csv(compiled_outputs_path)
    compiled_df = compiled_df[~compiled_df['nws_lid'].isna()]

    # Remove unneeded columns (flow, control_stage, magnitude) and then remove duplicate rows, keeping the first occurrence of each LID
    compiled_df = compiled_df.drop(columns=['flow', 'control_stage', 'magnitude'])
    compiled_df = compiled_df.drop_duplicates(subset=['nws_lid'], keep='first')

    site_list = compiled_df['nws_lid'].to_list()

    # Save the resulting DataFrame to a new CSV file with the date in the filename
    date_formatted = date.today().strftime("%Y%m%d")
    sites_with_hecras_models_path = os.path.join(output_folder, f'sites_with_hecras_models_{date_formatted}.csv')
    compiled_df.to_csv(sites_with_hecras_models_path, index=False)

    logging.info(f'Compiled HEC-RAS model info for {len(site_list)} AHPS sites')
    logging.info(f'Saved sites/model table to {os.path.basename(sites_with_hecras_models_path)}')

    return


# Main function
def catfim_hecras_preprocessing(
    threshold_file,
    nwm_meta_file,
    ripple_path,
    output_folder_location,
    lst_models,
):
    '''
    Main function for script.
    '''
    # Get input variables
    magnitude_types = csf.MAGNITUDES_TYPES
    flows2fim_path = "/projects/catfim_hecras_fb/flows2fim_030/flows2fim"  # csf.FLOWS2FIM_PATH TODO: finalize file location and Add to shared vars

    # TODO: Finalize file locationand update input path (maybe from an env file?)
    ripple_whitelist_path = '/home/rdp-user/projects/catfim_hecras_fb/ripple_feature_ids_whitelist_final_20260729_1420_no_path.csv'

    # Create output folders
    output_folder, intermediates_folder = create_output_folder(output_folder_location)

    # Validate input paths and variables
    collections_path = os.path.join("ripple", ripple_path, "collections")
    input_path_list = [threshold_file, nwm_meta_file, intermediates_folder, output_folder, collections_path, flows2fim_path, ripple_whitelist_path]
    for path in input_path_list:
        if not os.path.exists(path):
            raise Exception(f'Input file {path} does not exist. Cannot create flows files.')

    lst_models = lst_models.split()

    # Set up the main logger
    log_file_path = sf.setup_file_logger(output_folder, "catfim_hecras_preprocessing")

    # Record overall start time
    overall_start_dt = datetime.now(timezone.utc)
    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")

    try:

        print('======================================')
        logging.info(f'Starting CatFIM HEC-RAS preprocessing...  - {display_dt_string} (UTC)')
        logging.info("")
        logging.info(f"Logs will be saved to {log_file_path}")
        logging.info("")

        # Make flows file from the input WRDS data
        flows_csv_dict, identifiers_csv_filepath = create_flows_files(threshold_file, nwm_meta_file, intermediates_folder, magnitude_types)

        # Create the controls CSVs for the model/magnitude combinations
        compiled_outputs_path = run_controls_for_all_models_and_magnitudes(magnitude_types, flows_csv_dict, collections_path, ripple_path, flows2fim_path, intermediates_folder, identifiers_csv_filepath, lst_models, output_folder)

        # Create a table matching AHPS sites to available HEC-RAS models
        create_site_model_table(compiled_outputs_path, output_folder)

        # ----- 

    except Exception as ex:
        logging.critical(f"Exception occured: {ex}")
        logging.critical(traceback.format_exc())


    display_dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    dur_msg = fh.print_date_time_duration(overall_start_dt, datetime.now(timezone.utc), False)

    logging.info('')
    logging.info(f"Program complete! - {display_dt_string} (UTC)")
    logging.info(f"Logs saved to {log_file_path}")
    logging.info(f"{dur_msg}")
    print("=========================================================================")

    return


if __name__ == '__main__':
    '''
    # TODO: Add


    Example:
    
    python /projects/catfim_hecras_fb/catfim_hecras_preprocessing.py 
    -tf '/data/inputs/wrds/thresholds_20260413.pkl'
    -mf '/data/inputs/wrds/nwm_metadata_20260413.pkl'
    -r '/ripple/ripple_100_20251004'
    -of '/projects/catfim_hecras_fb/test_outputs/'
    -l 'ble_12100202_MiddleGuadalupe ble_12030106_EastForkTrinity'
    
    '''
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM HEC-RAS Pre-Processing')

    parser.add_argument(
        '-tf',
        '--threshold-file',
        help='REQUIRED: Filepath to the threshold pkl file.'
        ' e.g.: /data/inputs/wrds/thresholds_20260413.pkl',
        required=True,
    )

    parser.add_argument(
        '-mf',
        '--nwm-meta-file',
        help='REQUIRED: Filepath to nwm metadata pickle file.'
        ' e.g.: /data/inputs/wrds/nwm_metadata_20260413.pkl',
        required=True,
    )

    parser.add_argument(
        '-r',
        '--ripple-path',
        help='REQUIRED: Folder from which to get Ripple model inputs.'
        ' ie ripple_100_20251004',
        required=True,
    )

    parser.add_argument(
        '-of',
        '--output-folder-location',
        help='REQUIRED: Target location to create the catfim_hecras_preprocessing folder to store'
            ' final outputs. A temp folder will also be created in the catfim_hecras_preprocessing dir.'
            ' ie /projects/catfim_hecras_fb/test_outputs/final_outputs',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--lst-models',
        help='OPTIONAL: Space-delimited list of models to preprocess HEC-RAS for. Defaults to all'
             ' models in the given ripple folder. ie ble_12100202_MiddleGuadalupe',
        required=False,
        default='all',
    )

    args = vars(parser.parse_args())

    # Call main program
    catfim_hecras_preprocessing(**args)
