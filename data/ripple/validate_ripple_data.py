#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone

import numpy
import pandas as pd
import tqdm
from dotenv import load_dotenv

import data.aws.aws_shared_functions as asf
import data.aws.s3_shared_functions as s3_sf
import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh


# TODO: Mar 2026: For future releases:
#     - Add a column named "ripple_type", which currently woudl be the values of 'mip' or 'ble'. This helps
#       Hydrovis with precedence of a feature record that has an mip AND a ble record.
#     - Currently, it is possible for one feature to be in more than one model_collection folder, past
#       even mip or ble ripple_type. ie) (not a real example): 250404 showing up in mip_11050002 and mip_11050003.
#       Calcuate if one is valid and one is not? if both valid, adjsut records so HV has only one rec to pick
#     - Likely more validation can be performed (Heidi did some by hand). Maybe code look inside ripple.gpkg
#       or validate that the S3 paths have at least some tifs or maybe even the right tifs?


def validate_ripple_data(
    s3_ripple_root_path, ripple_version, terrain_whitelist_file_path, output_folder, aws_creds_file
):

    print("")
    print("****  Validate Ripple data  ****")
    print("")

    # TODO: validate inputs

    # strip starting slash
    s3_ripple_root_path.lstrip("/")

    ripple_version.strip("/")
    terrain_whitelist_file_path.lstrip("/")
    output_folder.strip("/")

    s3_bucket_name, s3_root_path = s3_sf.parse_bucket_and_folder_name(s3_ripple_root_path)

    s3_full_folder_path = f"{s3_ripple_root_path}/{ripple_version}"  # mostly just for logs

    # May throw exceptions or shut the program down.
    s3_client = __setup_aws(aws_creds_file, s3_bucket_name, s3_root_path)

    # ----------------------
    overall_start_dt = datetime.now(timezone.utc)

    # setup logs
    # make a download_logs folder inside the target
    log_path = os.path.join(output_folder, "logs")
    log_file_path = sf.setup_file_logger(log_path, "ripple_validation")
    logging.info(f"Start time: {overall_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")
    print(f"Logs saved to: {log_file_path}")    

    logging.info(f"Validating data in {s3_full_folder_path}.")
    time.sleep(5)  # gives the a min to read this.
    print("")

    try:
        # create the initial df
        df_features = __create_ripple_file_df(terrain_whitelist_file_path, ripple_version)

        # validate if the feature s3 columns exist
        df_features = __validate_s3_paths(df_features, s3_client, s3_bucket_name, s3_root_path)

        if df_features.empty:
            raise Exception("features dataframe has not records")

        file_dt_string = datetime.now(timezone.utc).strftime("%Y%m%d")
        validation_file_path = os.path.join(output_folder, f"ripple_feature_list_{file_dt_string}.csv")

        # save new csv
        logging.info(f"Saving feature file as {validation_file_path}")
        df_features.to_csv(validation_file_path, index=False)

    except Exception:
        print("********************************")
        logging.critical("**** Error ***")
        logging.critical(traceback.format_exc())
        logging.critical("**** Program Aborted ***")
    finally:
        logging.info("==========================================================")
        end_time = datetime.now(timezone.utc)
        logging.info("****  Completed running tool  ****")
        logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
        logging.info(fh.print_date_time_duration(overall_start_dt, end_time, False))
        print("")


# ============================
def __validate_s3_paths(df_features, s3_client, s3_bucket_name, s3_root_path):

    # s3_root_path: ie) /fim/ripple  (does not have the verion name on it)
    # make a deep copy to we can use it against tqdm

    df = df_features.copy(deep=True)

    num_recs = len(df)
    # for idx, row in df_features.iterrows():
    for idx, row in tqdm.tqdm(df.iterrows(), total=num_recs, desc="Validating S3 Paths"):
        # ie) 20260211_merged/collections/mip_1209301/library_extent/1234455
        library_path = row['library_path']
        if library_path == "":
            does_exist = False
        else:
            model_folder_path = f"{s3_root_path}/{library_path}"
            # print(model_folder_path)
            does_exist = s3_sf.does_s3_folder_exist(s3_client, s3_bucket_name, model_folder_path)
            # print(f"{model_folder_path} is {does_exist}")

        df.loc[idx, "is_library_path_valid"] = str(does_exist)

        # if row['is_valid'] == 'True' and row['is_bridge'] == 'False' and does_exist is False:
        #     df.loc[idx, "is_valid"] = str(False)
        # print (row)

    return df


# ============================
def __create_ripple_file_df(terrain_whitelist_file_path, ripple_version):

    # Using the incoming terrain file, make a simplified version for HV purposes.

    logging.info(f"Loading the terrain file: {terrain_whitelist_file_path}")
    columns_to_load = [
        'huc',
        'feature_id',
        'nwm_to_id',
        'order_',
        'collection_id',
        'model_id',
        'db_path',
        'is_blacklisted',
        'is_bridge',
        'is_valid',
    ]
    dtype_mapping = {
        'feature_id': numpy.int32,
        'collection_id': str,
        'model_id': str,
        'is_blacklisted': str,
        'huc': str,
    }
    df = pd.read_csv(terrain_whitelist_file_path, usecols=columns_to_load, dtype=dtype_mapping)
    # df = terrain_df[['feature_id', 'collection_id', 'is_blacklisted', 'huc']].copy().astype(str)
    df['huc'] = df['huc'].astype(int).astype(str).str.zfill(8)

    # new column flipping is_blacklist from True to False
    df["library_path"] = ""
    df["is_library_path_valid"] = 'False'

    logging.info(f"Num of terrain data recs is {len(df)}")
    print("")

    for idx, row in df.iterrows():
        # ie) 20260211_merged/collections/mip_1209301/library_extent/1234455
        # Yes.. without the root s3 path.
        library_path = (
            f"{ripple_version}/collections/{row['collection_id']}/library_extent/{row['feature_id']}"
        )
        # print(library_path)
        df.loc[idx, "library_path"] = library_path

    return df


# ============================
def __setup_aws(aws_creds_file, s3_bucket_name, s3_file_path):

    # We validate the bucket existance in here and assume the deploy env file is already loaded

    if not aws_creds_file:
        raise ValueError("aws credentials file argument is None or empty")

    if not os.path.isfile(aws_creds_file):
        raise ValueError(
            f"aws credentials file of {aws_creds_file} can not be found. Check path and/or case."
        )

    logging.info(f"loading aws credentials file ({aws_creds_file})")
    load_dotenv(aws_creds_file)

    # setup the client and validate the bucket
    aws_access_key = sf.get_env_value("HV_AWS_ACCESS_KEY_ID")
    aws_secret_key = sf.get_env_value("HV_AWS_SECRET_ACCESS_KEY")
    aws_region = sf.get_env_value("HV_AWS_REGION_NAME")

    is_success, return_msg, s3_client = asf.create_aws_client(
        aws_service_type_name='s3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        aws_region=aws_region,
    )

    if not is_success:  # if it was not already thrown from asf
        raise Exception(return_msg)

    # validate the bucket
    # may also throw an exception
    is_success, return_msg = s3_sf.does_s3_bucket_exist(s3_client, s3_bucket_name)
    if not is_success:
        logging.error(
            f"s3 bucket name of {s3_bucket_name} appears to not exist. It was extracted from the"
            " -sr (s3-ripple-root-path) input argument. Check the input arg or your aws credentials"
            " file data."
        )
        logging.error(return_msg)
        print("Program aborted")
        sys.exit(1)

    # Make sure the S3 source folder exists
    does_exists = s3_sf.does_s3_folder_exist(s3_client, s3_bucket_name, s3_file_path)
    if not does_exists:
        logging.error(
            f"S3 source folder of {s3_file_path} appears to not exist. It was calculated by"
            " extracted from the -sr (s3-ripple-root-path) input argument and adding the"
            " -rv (ripple version) value. Check the input args"
        )
        logging.error(return_msg)
        print("Program aborted")
        sys.exit(1)

    return s3_client


# ============================
if __name__ == '__main__':

    '''
    Sample

    # Note: It will assume that the actual model folders are under the version folder in a
    'collections' folder.  ie) s3:// (some bucket) /fim/ripple/2026011_merged/collections/ble_12090301/.
    It will further assume and validate that their is a "library_extent" folder under that
    which a subfolder for each valid feature id, and validate that it exists as well.
        ie) ripple/20260211_merged/collections/ble_12090301/library_extent/123446/

    python /foss_fim/data/ripple/validate_ripple_data.py
      -sr 's3://some-bucket/fim/ripple/' or 's3://hydrovis-ti-deployment-us-east-1/fim/ripple/'
      -rv '20260211_merged'
      -tf /data/ripple/ripple_20260211_merged/ripple_feature_list_sample.csv
      -n /data/ripple/ripple_20260211_merged/data_validation/
    '''

    parser = argparse.ArgumentParser(description='validate ripple data including s3 library feature pathing')

    parser.add_argument(
        '-sr',
        '--s3-ripple-root-path',
        help='REQUIRED: s3 pathing to the root ripple folder, but not the versioned subfolder.'
        ' Not s3://somebucket/fim/ripple/2026011_merged/collections, but just the root'
        ' before the versioned folder. ie) s3://somebucket/fim/ripple',
        required=True,
        type=str,
    )

    parser.add_argument(
        '-rv',
        '--ripple-version',
        help='REQUIRED: pathing from where the latest S3 root ripple path to the model folders.'
        ' Not: s3://( somebucket)/fim/ripple/2026011_merged/collections, but just the version'
        ' path to the model collections.. ie 20260211_merged. '
        ' Did you notice how this path is the second part of the s3-ripple-root-path above?',
        required=True,
        type=str,
    )

    parser.add_argument(
        '-tf',
        '--terrain-whitelist-file-path',
        help='REQUIRED: The path to the whitelist csv created by the terrain metrics code.'
        ' ie) /data/ripple/ripple_20260211_merged/ripple_20260211_merged/ripple_feature_list_sample.csv',
        required=True,
        type=str,
    )

    parser.add_argument(
        '-n',
        '--output-folder',
        help='REQUIRED: The path to the folder, not file name, where the new adjsuted ripple_feature_list.csv will be saved.'
        ' ie) /data/ripple/ripple_20260211_merged/ripple_20260211_merged/validation/',
        required=True,
        type=str,
    )

    parser.add_argument(
        '-ac',
        '--aws-creds-file',
        help='OPTIONAL: full pathed mapped docker path to the AWS Credentials file.\n'
        '  Defaults to /data/config/aws_credentials.env',
        default='/data/config/aws_credentials.env',
    )

    args = vars(parser.parse_args())
    validate_ripple_data(**args)
