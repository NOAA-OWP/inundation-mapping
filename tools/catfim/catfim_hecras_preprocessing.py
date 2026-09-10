#!/usr/bin/env python3

import argparse
import logging
import os
import re
import sys
import subprocess
import traceback
import shutil
from datetime import date, datetime, timezone

import pandas as pd

import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf
import data.aws.aws_shared_functions as asf
import data.aws.s3_shared_functions as s3_sf
from src.utils.shared_functions import FIM_Helpers as fh

from dotenv import load_dotenv



def create_output_folder(output_folder_location):
    '''
    Creates the output folders needed for processing (if needed).

    Creates:
        {output_folder_location}/catfim_hecras_preprocessing/
        {output_folder_location}/catfim_hecras_preprocessing/temp/

    Arguments
    ----------
    output_folder_location : str
        The location where the output folder will be created.

    Returns
    -------
    output_folder : str
        The path to the output folder.
    intermediates_folder : str
        The path to the intermediates folder.
    '''
    mode = 0o777  # allows read, write, and execute for all (rwxrwxrwx)

    # Confirm that the parent dir of the output folder exists
    output_folder_parent_dir = os.path.dirname(output_folder_location)
    if not os.path.isdir(output_folder_parent_dir):
        raise Exception(f"Output folder parent dir does not exist, unable to create output folder at {output_folder_location}")

    # Make output folder
    date_formatted = date.today().strftime("%Y%m%d")
    output_folder = os.path.join(output_folder_location, f'catfim_hecras_preprocessing_{date_formatted}')
    os.makedirs(output_folder, exist_ok=True, mode=mode)

    if not os.path.exists(output_folder):
        raise Exception(f'Unable to create output folder at {output_folder_location}')

    # Make intermediates folder
    intermediates_folder = os.path.join(output_folder, 'temp')
    os.makedirs(intermediates_folder, exist_ok=True, mode=mode)

    if not os.path.exists(intermediates_folder):
        raise Exception(f'Unable to create intermediates folder at {intermediates_folder}')

    return output_folder, intermediates_folder


def create_flows_files(threshold_file, nwm_meta_file, intermediates_folder, magnitude_types):
    '''
    Creates flows CSV files for each magnitude type (action, minor, moderate, major, record)
    using the thresholds and metadata.

    Saves the CSVs to the intermediate files path and returns a dictionary with magnitude type
    as key and flows CSV filepath as value.

    Arguments
    ----------
    threshold_file : str
        Filepath to the thresholds pickle file.
    nwm_meta_file : str
        Filepath to the NWM metadata pickle file.
    intermediates_folder : str
        Path to the folder where intermediate files will be saved.
    magnitude_types : list
        List of magnitude types to create flows files for.

    Returns
    -------
    flows_csv_dict : dict
        Dictionary with magnitude type as key and flows CSV filepath as value.
    identifiers_csv_path : str
        Filepath to the identifiers CSV file created during processing.

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
    long_thresh_df = long_thresh_df[long_thresh_df['magnitude_value'] != csf.THRESH_NODATA_VALUE]

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
    identifiers_csv_path = os.path.join(intermediates_folder, 'identifiers.csv')
    identifiers_df.to_csv(identifiers_csv_path, index=False)

    logging.info(
        f'Created identifiers df with {len(identifiers_df)} rows, saved to {os.path.basename(identifiers_csv_path)}'
    )

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
        mag_flows_df = mag_thresh_df[['nwm_feature_id', 'discharge']].copy().dropna()

        # Save this flows CSV to a folder
        flows_csv_filename = f'flows_{magnitude}.csv'
        flows_csv_filepath = os.path.join(intermediates_folder, flows_csv_filename)

        mag_flows_df.to_csv(flows_csv_filepath, index=False)

        if not os.path.exists(flows_csv_filepath):
            logging.error(
                f'{magnitude} - Failed to save flows file to {os.path.basename(intermediates_folder)}/{flows_csv_filename}'
            )

        else:
            # If save was sucessful, add flows CSV to filepath dictionary
            flows_csv_dict[magnitude] = flows_csv_filepath
            logging.info(
                f'{magnitude} - Saved flows file to {os.path.basename(intermediates_folder)}/{flows_csv_filename}'
            )

    logging.info('Finished creating flow files!')

    return flows_csv_dict, identifiers_csv_path


def __create_runtime_args_file(
    output_folder,
    threshold_file,
    nwm_meta_file,
    ripple_filename,
    lst_models,
    BUCKET_NAME,
    flows2fim_path,
    ripple_model_status_path,
    aws_creds_file,
    hv_params_file,
):
    '''
    Create a runtime args environment file to document input parameters (saved as output_folder/runtime_args.env).

    Arguments
    ---------
    output_folder : str
        The folder where the runtime args file will be saved.
    threshold_file : str
        The path to the threshold file.
    nwm_meta_file : str
        The path to the NWM metadata file.
    ripple_filename : str
        The name of the ripple file.
    lst_models : list
        The list of models.
    BUCKET_NAME : str
        The name of the S3 bucket.
    flows2fim_path : str
        The path to the flows2fim file.
    ripple_model_status_path : str
        The path to the ripple model status file.
    aws_creds_file : str
        The path to the AWS credentials file.
    hv_params_file : str
        The path to the HV parameters file.
    '''
    args_file_name = "runtime_args.env"
    args_file_path = os.path.join(output_folder, args_file_name)

    if os.path.isfile(args_file_path):
        os.remove(args_file_path)

    # Open the file using standard IO, then write lines to it.
    # All of these will be validated before we get here
    with open(args_file_path, "w") as file:
        file.write(f"THRESHOLD_FILE_PATH=\"{threshold_file}\"\n")
        file.write(f"NWM_METAFILE_PATH=\"{nwm_meta_file}\"\n")
        file.write(f"RIPPLE_FILENAME=\"{ripple_filename}\"\n")
        file.write(f"LST_MODELS={lst_models}\n")
        file.write(f"BUCKET_NAME={BUCKET_NAME}\n")
        file.write(f"FLOWS2FIM_PATH=\"{flows2fim_path}\"\n")
        file.write(f"RIPPLE_MODEL_STATUS_PATH=\"{ripple_model_status_path}\"\n")
        file.write(f"AWS_CREDS_FILE=\"{aws_creds_file}\"\n")
        file.write(f"HV_PARAMS_FILE=\"{hv_params_file}\"\n")
    return


def __setup_aws(aws_creds_file):
    # adapted FROM deploy to hydrovis
    # TODO: Do we need to remake this function? or should we just bring it in from the other file?

    # We validate the bucket existance in here and assume the deploy env file is already loaded

    global S3_CLIENT

    if not aws_creds_file:
        raise ValueError("aws credentials file argument is None or empty")

    if not os.path.isfile(aws_creds_file):
        raise ValueError(
            f"aws credentials file of {aws_creds_file} can not be found. Check path and/or case."
        )

    logging.info(f"Loading AWS credentials file ({aws_creds_file})")
    load_dotenv(aws_creds_file)

    # setup the client and validate the bucket
    hv_aws_access_key = sf.get_env_value("HV_AWS_ACCESS_KEY_ID")
    hv_aws_secret_key = sf.get_env_value("HV_AWS_SECRET_ACCESS_KEY")
    hv_aws_region = sf.get_env_value("HV_AWS_REGION_NAME")

    is_success, return_msg, S3_CLIENT = asf.create_aws_client(
        aws_service_type_name='s3',
        aws_access_key_id=hv_aws_access_key,
        aws_secret_access_key=hv_aws_secret_key,
        aws_region=hv_aws_region,
    )

    if not is_success:  # if it was not already thrown from asf
        raise Exception(return_msg)

    # Validate bucket (assumes the bucket name is already loaded)
    is_success, return_msg = s3_sf.does_s3_bucket_exist(S3_CLIENT, BUCKET_NAME)
    if not is_success:
        logging.error(
            f"HV_S3_BUCKET_NAME value of {BUCKET_NAME}. Check the aws creds env file and case."
        )
        logging.error(return_msg)
        print("Program aborted")
        sys.exit(1)


def download_ripple_file_from_s3(ripple_filename, collection_id, collection_temp_folder, filename):
    '''
    Download the necessary ripple files from S3.
    
    '''

    s3_file_key = f'/fim/ripple/{ripple_filename}/collections/{collection_id}/{filename}'
    target_file_path = os.path.join(collection_temp_folder, filename)

    does_file_exist = s3_sf.download_s3_file(S3_CLIENT, BUCKET_NAME, s3_file_key, target_file_path, test_bucket_exists=True)
    logging.info(f'File exists at {BUCKET_NAME}/{s3_file_key}: {does_file_exist}')

    return target_file_path


def run_controls(magnitude, collection_id, flows_filename, flows2fim_path, intermediates_folder, db_path, starts_csv):
    '''
    Runs flows2fim controls for a given model and magnitude, using the specified flows file.
    Saves the output CSV to the intermediate files path.

    Arguments
    ----------
    magnitude : str
        The magnitude type (e.g., action, minor, moderate, major, record).
    collection_id : str
        The identifier for the specific model collection.
    flows_filename : str
        The filename of the flows CSV to use for this magnitude.
    flows2fim_path : str
        The path to the flows2fim executable.
    intermediates_folder : str
        The path to the folder where intermediate files will be saved.
    db_path : str
        Path to the ripple.gpkg for the given collection ID.
    starts_csv : str
        Path to the start_reaches.csv for the given collection ID.

    Returns
    -------
    output_csv : str
        The path to the output controls CSV file created by flows2fim.

    '''
    logging.info('')
    logging.info(f'{collection_id} : {magnitude} - Run flows2fim controls subprocess')

    # Create the input and output file paths
    # model_path = os.path.join("ripple", ripple_filename, "collections", collection_id)
    # db_path = os.path.join(model_path, "ripple.gpkg") # TODO: need to get these from AWS download
    flows_csv = os.path.join(intermediates_folder, flows_filename)
    # starts_csv = os.path.join(model_path, "start_reaches.csv") # TODO: need to get these from AWS download
    controls_filename = f'controls_{collection_id}_{magnitude}.csv'
    output_csv = os.path.join(intermediates_folder, controls_filename)

    # Validate input paths
    input_path_list = [db_path, flows_csv, starts_csv]
    for path in input_path_list:
        if not os.path.exists(path):
            msg = f'Input file {path} does not exist. Cannot run controls for model {collection_id} and magnitude {magnitude}.'
            logging.critical(msg)
            raise Exception(msg)

    try:
        # Use subprocess to run flows2fim controls
        result = subprocess.run(
            [
                flows2fim_path,
                "controls",
                "-db",
                db_path,
                "-f",
                flows_csv,
                "-scsv",
                starts_csv,
                "-o",
                output_csv,
            ],
            capture_output=True,
            text=True,
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
    common_warning_list = [
        "Large difference in target vs found flow",
        "Flow not found for reach",
        "Large difference in target vs found control reach stage",
    ]
    parse_subprocess_outputs(result, common_warning_list, collection_id, magnitude)

    # Validate that output file was created
    if not os.path.exists(output_csv):
        logging.error(
            f'{collection_id} : {magnitude} - [flow2fim controls] Controls output file {os.path.basename(output_csv)} not created'
        )
        return None
    else:
        logging.info(
            f'{collection_id} : {magnitude} - [flow2fim controls] Saved controls file as {os.path.basename(output_csv)}'
        )

    return output_csv


def run_controls_for_all_models_and_magnitudes(
    magnitude_types,
    flows_csv_dict,
    collections_path,
    ripple_filename,
    flows2fim_path,
    intermediates_folder,
    identifiers_csv_path,
    lst_models,
    output_folder,
):
    '''
    Run flows2fim controls for each model and magnitude.

    Arguments
    ----------
    magnitude_types : list
        List of magnitude types to process (e.g., action, minor, moderate, major, record).
    flows_csv_dict : dict
        Dictionary with magnitude type as key and flows CSV filepath as value.
    collections_path : str
        Path to the Ripple model collections on S3.
    ripple_filename : str
        The path to the Ripple model collections.
    flows2fim_path : str
        The path to the flows2fim executable.
    intermediates_folder : str
        The path to the folder where intermediate files will be saved.
    identifiers_csv_path : str
        Filepath to the identifiers CSV file created during processing.
    ripple_model_status_path : str
        Filepath to the CSV containing the status of Ripple model collections.
    lst_models : list
        List of model collections to process. If 'all', all available collections will be processed.
    output_folder : str
        The path to the output folder where final outputs will be saved.

    Returns
    -------
    compiled_outputs_path : str
        The path to the compiled controls output CSV file created by concatenating all individual controls CSVs

    '''
    logging.info('')
    logging.info("Begin running controls....")
    logging.info(f"Getting model collections from {collections_path}")

    # Get a list of all available model collections from ripple_filename
    collections_filepath_lst = s3_sf.get_folder_list(S3_CLIENT, BUCKET_NAME, collections_path)
    all_collections_lst = [os.path.basename(os.path.normpath(p)) for p in collections_filepath_lst]

    logging.info(f'Found {len(all_collections_lst)} available collections in S3')

    # If lst_models is all, get a list of them
    if 'all' in lst_models:
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

    # # Filter the collection list using ripple_model_status_path TODO: Confirm that we should filter by collection id and not another col?
    # # Read in CSV and get a list of valid collections (where is_valid == True)
    # ripple_model_status_table = pd.read_csv(ripple_model_status_path)
    # valid_ripple_collections_list = (
    #     ripple_model_status_table[ripple_model_status_table['is_valid'] == True]['collection_id']
    #     .unique()
    #     .tolist()
    # )

    # # Filter the collection list to only include valid ripple collections
    # collection_list = [item for item in collection_list if item in valid_ripple_collections_list]

    # # Return a list of collections that were removed due to not being valid # TODO: Clean up
    # invalid_collections_list = [item for item in lst_models if item not in valid_ripple_collections_list]
    # if len(invalid_collections_list) > 0:
    #     logging.warning(
    #         f'The following model collection(s) were removed from processing due to not being valid: {invalid_collections_list}'
    #     )

    # ## DEBUG MODE: Only run the first n models TEMP DEBUG
    # n = 100
    # logging.warning(f'DEBUG MODE!! Only processing first {n} vals from collections list')
    # collection_list = collection_list[:n]
    # ## DEBUG MODE

    logging.info(f'Found {len(collection_list)} model collection(s) to process: {collection_list}')

    # Read identifiers_csv_path
    identifiers_df = pd.read_csv(identifiers_csv_path)

    controls_output_csv_list = []
    for collection_id in collection_list:
        logging.info('')
        logging.info(f'{collection_id} - Running controls')
        section_start_dt = datetime.now(timezone.utc)

        # Make collection-specific temp folder
        collection_temp_folder = os.path.join(intermediates_folder, collection_id)

        if os.path.exists(collection_temp_folder):
            logging.info(f'Removing previously-made collection temp folder')
            shutil.rmtree(collection_temp_folder)

        os.mkdir(collection_temp_folder, mode=0o777)

        db_path = download_ripple_file_from_s3(ripple_filename, collection_id, collection_temp_folder, "ripple.gpkg")
        starts_csv = download_ripple_file_from_s3(ripple_filename, collection_id, collection_temp_folder, "start_reaches.csv")

        print(f"Downloaded files to {collection_temp_folder}:")  # TEMP DEBUG
        print(f" - {db_path}")  # TEMP DEBUG
        print(f" - {starts_csv}")  # TEMP DEBUG

        # Iterate through magnitudes and run controls
        for magnitude in magnitude_types:
            flows_filename = flows_csv_dict[magnitude]

            controls_output_csv = run_controls(
                magnitude, collection_id, flows_filename, flows2fim_path, intermediates_folder, db_path, starts_csv
            )

            if controls_output_csv is None:
                logging.warning(
                    f'{collection_id} : {magnitude} - No controls CSV created, error likely occurred'
                )
                continue

            # Read the output CSV and add the necessary columns
            df = pd.read_csv(controls_output_csv)
            df['magnitude'] = magnitude
            df['model_collection'] = collection_id
            df['collection_parent_folder'] = ripple_filename

            # Join the identifiers_df to the controls output df to add the nws_lid column (joining on reach_id for df and nwm_feature_id for identifiers df)
            df = pd.merge(df, identifiers_df, left_on='reach_id', right_on='nwm_feature_id', how='left')

            df.to_csv(controls_output_csv, index=False)
            controls_output_csv_list.append(controls_output_csv)

            logging.info(
                f"{collection_id} : {magnitude} - Updated controls CSV with additional metadata columns"
            )

        # Delete the temp folder containing the ripple.gpkg and the start_reachs.csv from S3
        logging.info(f'Removing collection temp folder: {collection_temp_folder}')
        shutil.rmtree(collection_temp_folder)

        dur_msg = fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False)
        logging.info(f'{collection_id} - Finished running controls for {collection_id} - {dur_msg}')

    logging.info("")
    logging.info(f"Finished running controls for {len(collection_list)} models.")

    # Compile the outputs of the controls in controls_output_csv_list
    combined_df = pd.concat([pd.read_csv(f) for f in controls_output_csv_list], ignore_index=True)

    # Delete temp folder
    logging.info('Deleting temp folder')
    shutil.rmtree(intermediates_folder)

    # Save the combined DataFrame to a new CSV file
    compiled_outputs_path = os.path.join(output_folder, f'combined_controls_output.csv')
    combined_df.to_csv(compiled_outputs_path, index=False)

    logging.info(f'Compiled controls output saved to {os.path.basename(compiled_outputs_path)}')

    return compiled_outputs_path


def parse_subprocess_outputs(result, common_warning_list, collection_id, magnitude):
    '''
    Parses the stderr output from the flows2fim controls subprocess to identify common warnings and log them.

    Arguments
    ----------
    result : subprocess.CompletedProcess
        The result object returned from the subprocess.run() call.
    common_warning_list : list
        List of common warning strings to look for in the stderr output.
    collection_id : str
        The identifier for the specific model collection.
    magnitude : str
        The magnitude type (e.g., action, minor, moderate, major, record).

    '''

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
            logging.info(
                f'{collection_id} : {magnitude} - [flow2fim controls] {common_warning} (Returned for {len(reach_id_list)} Reach IDs)'
            )
            # logging.info(f'{collection_id} : {magnitude} - [flow2fim controls] Reach IDs: {reach_id_list}')  # Too many feature IDs to print (could toggle for debugging)

    # Get warnings that aren't in the common warnings list
    uncommon_warning_list = [s for s in stderr_list if not any(k in s for k in common_warning_list)]

    for uncommon_warning in uncommon_warning_list:
        logging.info(f'{collection_id} : {magnitude} - [flow2fim controls] {uncommon_warning}')

    return


def create_site_model_table(compiled_outputs_path, output_folder, ripple_model_status_path):
    '''
    Creates a table of sites that have HEC-RAS models available.

    Arguments
    ----------
    compiled_outputs_path : str
        The path to the compiled outputs CSV file.
    output_folder : str
        The path to the output folder where the resulting table will be saved.
    ripple_model_status_path : str
        Path to the ripple model status CSV.

    '''
    logging.info('')
    logging.info('Creating list of sites with HEC-RAS models available...')

    # Read compiled_outputs_path and filter out rows that have NaN in the nws_lid column
    compiled_df = pd.read_csv(compiled_outputs_path)
    compiled_df = compiled_df[~compiled_df['nws_lid'].isna()]

    # Remove unneeded columns (flow, control_stage, magnitude) and then remove duplicate rows, keeping the first occurrence of each LID
    compiled_df = compiled_df.drop(columns=['flow', 'control_stage', 'magnitude'])

    # TODO: Is it correct to be removing the duplicates here? Does that mean we should filter out bad models somwhere else?
    compiled_df = compiled_df.drop_duplicates(subset=['nws_lid'], keep='first')

    # Filter the compiled df to only include feature IDs where is_valid = True in the whitelist
    ripple_model_status_table = pd.read_csv(ripple_model_status_path)
    valid_ripple_reach_id_list = (
        ripple_model_status_table[ripple_model_status_table['is_valid'] == True]['feature_id']
        .unique()
        .tolist()
    )

    # Filter out sites from compiled_df where the reach_id is not in the valid_ripple_reach_id_list
    compiled_df = compiled_df[compiled_df['reach_id'].isin(valid_ripple_reach_id_list)]

    # Print a list of sites that were removed due to not being valid
    invalid_sites_list = compiled_df[~compiled_df['reach_id'].isin(valid_ripple_reach_id_list)]['nws_lid'].unique().tolist()
    if len(invalid_sites_list) > 0:
        logging.warning(
            f'The {len(invalid_sites_list)} sites were removed from the final list due to not being valid: {invalid_sites_list}'
        )

    # Save the resulting DataFrame to a new CSV file with the date in the filename
    # date_formatted = date.today().strftime("%Y%m%d")
    sites_with_hecras_models_path = os.path.join(
        output_folder, f'sites_with_hecras_models.csv'
    )
    compiled_df.to_csv(sites_with_hecras_models_path, index=False)

    site_list = compiled_df['nws_lid'].to_list()
    logging.info(f'Compiled HEC-RAS model info for {len(site_list)} AHPS sites')
    logging.info(f'Saved sites/model table to {os.path.basename(sites_with_hecras_models_path)}')

    return


# Main function
def catfim_hecras_preprocessing(
    threshold_file, nwm_meta_file, ripple_filename, output_folder_location, lst_models
):
    '''
    Main function for script.

    Arguments
    ----------
    threshold_file : str
        Filepath to the thresholds pickle file.
    nwm_meta_file : str
        Filepath to the NWM metadata pickle file.
    ripple_filename : str
        The path to the Ripple model collections.
    output_folder_location : str
        The location where the output folder will be created.
    lst_models : str
        Space-delimited list of models to preprocess HEC-RAS for. Defaults to all models in the given ripple folder. If 'all', all available models will be processed.

    '''
    # Get input variables
    magnitude_types = csf.MAGNITUDES_TYPES
    flows2fim_path = "/projects/catfim_hecras_fb/flows2fim_030/flows2fim"  # csf.FLOWS2FIM_PATH TODO: finalize file location and Add to shared vars
    ripple_model_status_path = '/home/rdp-user/projects/catfim_hecras_fb/ripple_feature_ids_whitelist_final_20260729_1420_no_path.csv' # TODO: Finalize file location and update input path (maybe from an env file?) ... maybe eventually we will download this from S3 too

    aws_creds_file = '/data/config/aws_credentials.env' # TODO: should we get this from somewhere?
    hv_params_file = '/foss_fim/config/hv_deploy_params.env' # TODO: should we get this from somewhere?

    # S3 Setup: Make the S3 client, get the bucket name, and validate S3 input paths
    load_dotenv(hv_params_file)
    global BUCKET_NAME
    BUCKET_NAME = os.getenv("HV_S3_BUCKET_NAME")

    __setup_aws(aws_creds_file)

    collections_path = '/fim/ripple/' + ripple_filename + '/collections/'
    collections_path_success = s3_sf.does_s3_folder_exist(S3_CLIENT, BUCKET_NAME, collections_path)

    if not collections_path_success:
        raise Exception(f'S3 collections path {collections_path} does not exist.')

    # Create and validate local folders
    output_folder, intermediates_folder = create_output_folder(output_folder_location)

    input_path_list = [
        threshold_file,
        nwm_meta_file,
        intermediates_folder,
        output_folder,
        flows2fim_path,
        ripple_model_status_path,
    ]
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
        logging.info(f"Using S3 bucket: {BUCKET_NAME}") ### TEMP DEBUG
        logging.info(f"Using Ripple filename: {ripple_filename}")
        logging.info("")


        # Make flows file from the input WRDS data
        flows_csv_dict, identifiers_csv_path = create_flows_files(
            threshold_file, nwm_meta_file, intermediates_folder, magnitude_types
        )

        # Create the controls CSVs for the model/magnitude combinations
        compiled_outputs_path = run_controls_for_all_models_and_magnitudes(
            magnitude_types,
            flows_csv_dict,
            collections_path,
            ripple_filename,
            flows2fim_path,
            intermediates_folder,
            identifiers_csv_path,
            lst_models,
            output_folder,
        )

        # Create a table matching AHPS sites to available HEC-RAS models
        create_site_model_table(compiled_outputs_path, output_folder, ripple_model_status_path)

        # Create a runtime args file to document the input parameters and files used
        __create_runtime_args_file(
            output_folder,
            threshold_file,
            nwm_meta_file,
            ripple_filename,
            lst_models,
            BUCKET_NAME,
            flows2fim_path,
            ripple_model_status_path,
            aws_creds_file,
            hv_params_file,
        )

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
    Command line parser for running the CatFIM HEC-RAS preprocessing script.

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
        '--ripple-filename',  # TODO: or should we get this val from the CSV?
        help='REQUIRED: Folder from which to get Ripple model inputs, ie ripple_100_20251004',
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
