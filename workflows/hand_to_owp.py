#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv

import data.aws.aws_shared_functions as asf
import data.aws.s3_shared_functions as s3_sf
import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh


'''
aws s3 cli equiv (note.. may not be up-to-date here)
   - Why do we do it via python/boto3? We can use multi-thread to make it faster
     and have more control of contents and rules.
'''

# ============================
# GLOBAL Vars including files / folders to be copied

S3_CLIENT = None  # boto3 client (works for both buckets)
FIM_S3_BUCKET_NAME = ""  # comes from the aws creds file

# Neither of these include their bucket names
SRC_S3_HAND_PATH = ""  # Comes partialy from the workflows env file and part is an arg
TRG_DATA_HAND_PATH = ""  # Comes partialy from the workflows env file and part is an arg


# ============================
def hand_to_owp(source_path, target_path, aws_creds_file, workflows_params_file, num_jobs):

    print("****  Downloading FIM S3 HAND to OWP Started   ****")
    print("Note: This tool has been known to take up to 5 hours to run based on multiple factors")
    print("")

    # --------------
    # Validation. We are validating all variables in case the call came in from another py file
    # We also load a number of key variables (load env)
    __validate_input(source_path, target_path, workflows_params_file, num_jobs)

    # May throw exceptions or shut the program down.
    __setup_aws(aws_creds_file)

    overall_start_dt = datetime.now(timezone.utc)

    # setup logs
    # make a download_logs folder inside the target
    log_path = os.path.join(TRG_DATA_HAND_PATH, "download_logs")
    sf.setup_file_logger(log_path, "hand_to_owp")
    logging.info(f"Start time: {overall_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")

    logging.info(f"Loading from s3://{FIM_S3_BUCKET_NAME}{SRC_S3_HAND_PATH} to {TRG_DATA_HAND_PATH}.")
    print("")

    try:
        __load_hand_dataset(num_jobs)

    except Exception:
        print("********************************")
        print("**** Error: Program Aborted.")
        logging.critical(traceback.format_exc())
    finally:
        logging.info("==========================================================")
        end_time = datetime.now(timezone.utc)
        print("****  Completed downloading all applicable files   ****")
        print(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
        logging.info(fh.print_date_time_duration(overall_start_dt, end_time, False))
        print("")


# ============================
def __load_hand_dataset(num_jobs):

    # We filter to keep only the files we specifically want
    # We will build up a list of files for download
    files_to_download = []
    section_start_dt = datetime.now(timezone.utc)

    # We get a list of applicable files
    # as each search_key needs to be used one at a time to figure out which are to be included
    # or maybe we use MT here?
    search_keys = []  # purely keys, but we use the for loop to show the user the search keys loaded
    file_pattern_key = "OWP_HAND_LOAD_PATTERN"
    for name, value in os.environ.items():
        if file_pattern_key in name:
            search_keys.append(value)
            logging.info(f"Getting file names for pattern {name} : {value}")

    logging.info("------------------------------------")

    print(
        "*** Building a list of files to upload: We get the names and paths"
        " of all files that are applicable before copying"
    )
    print("    Note: Some loads per pattern can be slow, especially branches."
          " It may even appear to be frozen for up to 20 mins. Keep an eye on network"
          " stats to ensure it is still working.")

    print(f"    Start time: {section_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")
    time.sleep(5)  # gives the a min to read this.
    print("")

    if len(search_keys) == 0:
        logging.Error(
            "No search patterns were found. Check the env and ensure that all variable names"
            " for loading hand to owp files start with OWP_HAND_LOAD_PATTERN"
        )

    file_paths = s3_sf.get_file_list(S3_CLIENT, FIM_S3_BUCKET_NAME, SRC_S3_HAND_PATH, search_keys)
    for file_path in file_paths:
        if not file_path.startswith("/"):
            file_path = "/" + file_path
        trg_file = file_path.replace(SRC_S3_HAND_PATH, TRG_DATA_HAND_PATH)
        item = {"src_file": file_path, "trg_file": trg_file}
        files_to_download.append(item)

    logging.info(f"Download list created: Total number of files to download is {len(files_to_download)}")
    logging.info(fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False))
    logging.info("------------------------------------")

    if len(files_to_download) == 0:
        logging.error(
            "No files were found using the env OWP_HAND_LOAD_PATTERN variables."
            " Check that variables exists starting with that pattern."
        )
    else:
        section_start_dt = datetime.now(timezone.utc)
        logging.info("*** Downloading files")
        print(f"  Start time: {section_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")
        sorted_files_to_download = sorted(files_to_download, key=lambda x: x["src_file"])
        s3_sf.download_files_by_file_list(S3_CLIENT, FIM_S3_BUCKET_NAME, sorted_files_to_download, num_jobs)
        # def download_large_filesets(s3_client, bucket_name, s3_src_path, trg_folder_path, list_of_search_key, num_workers=10):
        #     num_files_downloaded = s3_sf.download_large_filesets(S3_CLIENT, FIM_S3_BUCKET_NAME, SRC_S3_HAND_PATH, TRG_DATA_HAND_PATH,
        # search_keys, len(search_keys))
        print()
        logging.info(f"Number of files downloaded: {len(files_to_download)}")
        end_time = datetime.now(timezone.utc)
        logging.info("**** Completed downloading files ****")
        print(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
        logging.info(fh.print_date_time_duration(section_start_dt, datetime.now(timezone.utc), False))
        logging.info("------------------------------------")


# ============================
def __validate_input(source_path, target_path, workflows_params_file, num_jobs):

    global SRC_S3_HAND_PATH, TRG_DATA_HAND_PATH, FIM_S3_BUCKET_NAME

    if not source_path:
        raise ValueError("The -st source path value is None or empty")
    SRC_S3_HAND_PATH = sf.add_slashes_to_path(source_path)

    if not target_path:
        raise ValueError("The -st source path value is None or empty")
    TRG_DATA_HAND_PATH = sf.add_slashes_to_path(target_path)

    if not workflows_params_file:
        raise ValueError("The workflows params file argument value is None or empty")
    if not os.path.isfile(workflows_params_file):
        raise ValueError(f"params file of {workflows_params_file} can not be found. Check path and/or case.")

    logging.info(f"loading working params file ({workflows_params_file})")
    load_dotenv(workflows_params_file)

    if num_jobs > 20:
        # show a warning, then sleep for a bit allowwing them to abort if they like.
        msg = "Warning: The number of jobs you have submitted may be larger than your network connection speed.\n"
        "This may results in S3 issuing 'Connection Pool Is Full' warnings. If this happens, lower your"
        "job number restart.\n Note: for OWP Staff: for the larger servers, it seems ok at 10 for prod."
        print(msg)
        time.sleep(10)  # gives them time to abort if they want.

    # We load the bucket here and the src path above, but validate them in the __set_aws function
    FIM_S3_BUCKET_NAME = sf.get_value_from_env("FIM_S3_BUCKET_NAME", workflows_params_file)
    FIM_S3_BUCKET_NAME = FIM_S3_BUCKET_NAME.strip('/')


# ============================
def __setup_aws(aws_creds_file):

    # We validate the src s3 folder and bucket name here. It assumes it is already loaded
    # from the workflow file.
    global S3_CLIENT

    if not aws_creds_file:
        raise ValueError("aws credentials file argument value is None or empty")

    if not os.path.isfile(aws_creds_file):
        raise ValueError(
            f"aws credentials file of {aws_creds_file} can not be found. Check path and/or case."
        )

    logging.info(f"loading aws credentials file ({aws_creds_file})")
    load_dotenv(aws_creds_file)

    # setup the client and validate the bucket
    aws_access_key = sf.get_value_from_env("FIM_AWS_ACCESS_KEY_ID", aws_creds_file)
    aws_secret_key = sf.get_value_from_env("FIM_AWS_SECRET_ACCESS_KEY", aws_creds_file)
    aws_region = sf.get_value_from_env("FIM_AWS_REGION_NAME", aws_creds_file)

    is_success, return_msg, S3_CLIENT = asf.create_aws_client(
        aws_service_type_name='s3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        aws_region=aws_region,
    )

    if not is_success:  # if it was not already thrown from asf
        raise Exception(return_msg)

    # validate the bucket
    # may also throw an exception
    is_success, return_msg = s3_sf.does_s3_bucket_exist(S3_CLIENT, FIM_S3_BUCKET_NAME)
    if not is_success:
        logging.error(
            f"FIM_S3_BUCKET_NAME value of {FIM_S3_BUCKET_NAME}. Check the aws creds env file and case."
        )
        logging.error(return_msg)
        print("Program aborted")
        sys.exit(1)

    # Make sure the S3 source folder exists
    does_exists = s3_sf.does_s3_folder_exist(S3_CLIENT, FIM_S3_BUCKET_NAME, SRC_S3_HAND_PATH)
    if not does_exists:
        logging.error(f"S3 source folder of {SRC_S3_HAND_PATH}. Check the -sp, source path, arg and case.")
        logging.error(return_msg)
        print("Program aborted")
        sys.exit(1)


# ============================
if __name__ == '__main__':

    '''
    This script looks for two defaulted params env file.
       - '-ac/--aws-creds-file': contains only aws credentials info such as AWS Access Keys.
       - '-wp/--workflows-params-file': Contains an array of values which can things such as
         variables for copying files from FIM S3 to OWP servers.

    Sample Usages
        python ./foss_fim/workflows/hand_to_owp.py -sp '/foss_fim/previous_fim/hand_4_8_7_2' -tp '/data/previous_fim/hand_4_8_7_2'

        python ./foss_fim/workflows/hand_to_owp.py -sp '/foss_fim/previous_fim/hand_4_8_7_2' \
            -tp '/data/previous_fim/hand_4_8_7_2' \
            -wp '/data/config/workflow_test_params.env'

    Notes:
        - The files to download use a "pattern" system via the env file. It is similar to glob in its
          use of wildcards. The patterns can be seen in the workflow_params.env file
        - All files downloaded will overwrite existing ones.
    '''

    parser = argparse.ArgumentParser(
        description='Copies specific files/folders from FIM s3 bucket to OWP server folders.'
        'It includes only key files needed OWP servers that need to talk to WRDS\n'
        ' such as the CatFIM family and FIM Performance, points and polys, (eval_plots.py).'
    )

    parser.add_argument(
        '-sp',
        '--source-path',
        help='REQUIRED: S3 Path to where the log file will downloaded from.\n'
        '  ie) /foss_fim/previous_fim/hand_4_8_7_2',
        required=True,
        type=str,
    )

    parser.add_argument(
        '-tp',
        '--target-path',
        help='REQUIRED: local data path to where the log file will saved.\n'
        '  ie) /data/previous_fim/hand_4_8_7_2',
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

    parser.add_argument(
        '-wp',
        '--workflows-params-file',
        help='OPTIONAL: Path to workflows params(config) file.\n'
        '  Defaults to /data/config/workflow_params.env',
        default="/data/config/workflow_params.env",
    )

    parser.add_argument(
        '-j', "--num-jobs", help="OPTIONAL: Number of processes (defaults to 10)", type=int, default=10
    )

    args = parser.parse_args()

    hand_to_owp(**vars(args))
