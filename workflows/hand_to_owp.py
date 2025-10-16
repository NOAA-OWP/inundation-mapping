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

# ============================
# GLOBAL Vars including files / folders to be copied

S3_CLIENT = None  # boto3 client (works for both buckets)

# Neither of these include their bucket names
FIM_S3_BUCKET_NAME = ""  # Comes from the aws cred env file
FIM_S3_ROOT_PATH = ""  # Comes from the workflows env file
FIM_PREVIOUS_FIM_HAND_PATH = ""  # Comes from the workflows env file

HAND_VERSION = ""


# ============================
def hand_to_owp(hand_version, aws_creds_file, workflows_params_file, log_path, num_jobs):

    print("****  Doadling FIM S3 HAND to OWP Started   ****")

    # --------------
    # Validation. We are validating all variables in case the call came in from another py file
    # We also load a number of key variables (load env)
    deploy_types = __validate_input(hand_version, workflows_params_file, num_jobs)

    # May throw exceptions or shut the program down.
    __setup_aws(aws_creds_file)

    overall_start_dt = datetime.now(timezone.utc)

    # setup logs
    sf.setup_file_logger(log_path, "deloy_to_hydrovis")
    logging.info(f"Start time: {overall_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")
    logging.info(f"Deploy types to upload: {deploy_type}")

    logging.info(
        f"Loading to s3://{HV_S3_BUCKET_NAME}{HV_S3_ROOT_HANDSET_PATH}"
        f" and s3://{HV_S3_BUCKET_NAME}{HV_S3_ROOT_QA_DATASETS_PATH}."
        " Depending on while deploy types were selected."
    )
    print("")

    try:

    except Exception:
        print("********************************")
        print("**** Error: Program Aborted.")
        logging.critical(traceback.format_exc())
    finally:
        logging.info("==========================================================")
        end_time = datetime.now(timezone.utc)
        logging.info("****  Completed Deploy to HydroVIS  ****")
        print(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
        logging.info(fh.print_date_time_duration(overall_start_dt, end_time, False))
        print("")



# ============================
def __validate_input(hand_version, workflows_params_file, num_jobs):

    global FIM_S3_ROOT_PATH, FIM_PREVIOUS_FIM_HAND_PATH, HAND_VERSION

    if workflows_params_file is None or workflows_params_file == "":
        raise ValueError("workflows params file variable is None or empty")
    if not os.path.isfile(workflows_params_file):
        raise ValueError(f"params file of {workflows_params_file} can not be found. Check path and/or case.")

    logging.info(f"loading working params file ({workflows_params_file})")
    load_dotenv(workflows_params_file)

    if hand_version is None or hand_version == "":
        HAND_VERSION = hand_version

    FIM_S3_HAND_PATH = __get_env_variable_with_args("FIM_S3_HAND_PATH", "HAND_VERSION", workflows_params_file)
    FIM_PREVIOUS_FIM_HAND_PATH = __get_env_variable_with_args("FIM_PREVIOUS_FIM_HAND_PATH", "HAND_VERSION", workflows_params_file)


    # The bucket name / load is in the aws function
    HV_S3_ROOT_HANDSET_PATH = __get_env_variable_with_versions("HV_S3_ROOT_HANDSET_PATH", deploy_params_file)
    HV_S3_ROOT_HANDSET_PATH = sf.add_slashes_to_path(HV_S3_ROOT_HANDSET_PATH)

    HV_S3_ROOT_QA_DATASETS_PATH = __get_env_variable_with_versions(
        "HV_S3_ROOT_QA_DATASETS_PATH", deploy_params_file
    )
    HV_S3_ROOT_QA_DATASETS_PATH = sf.add_slashes_to_path(HV_S3_ROOT_QA_DATASETS_PATH)

    if num_jobs > 20:
        # show a warning, then sleep for a bit allowwing them to abort if they like.
        msg = "Warning: The number of jobs you have submitted may be larger than your network connection speed.\n"
        "This may results in S3 issuing 'Connection Pool Is Full' warnings. If this happens, lower your"
        "job number restart.\n Note: for OWP Staff: for the larger servers, it seems ok at 20."
        print(msg)
        time.sleep(10)  # gives them time to abort if they want.

    return deploy_types


# ============================
def __get_env_variable_with_args(env_var_name, replace_env_var_name, params_file):
    # This will load an enviro variable, raising an exception if it does not exist
    # if it does exist, see if it needs subsitution in the value for either the

    env_value = sf.get_value_from_env(env_var_name, params_file)
    # env_value = env_value.replace("{RELEASE_FIM_PUBLIC_VERSION}", RELEASE_FIM_PUBLIC_VERSION)
    # env_value = env_value.replace("{HAND_VERSION}", HAND_VERSION)
    return env_value

# ============================
def __setup_aws(aws_creds_file):

    global S3_CLIENT, FIM_S3_BUCKET_NAME

    if aws_creds_file is None or aws_creds_file == "":
        raise ValueError("aws credentials file argument is None or empty")

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

    # we load the bucket name from the aws file to help with git security a little.
    FIM_S3_BUCKET_NAME = sf.get_value_from_env("FIM_S3_BUCKET_NAME", aws_creds_file)
    FIM_S3_BUCKET_NAME = FIM_S3_BUCKET_NAME.strip('/')

    # validate the bucket
    # may also throw an exceptoin
    is_success, return_msg = s3_sf.does_s3_bucket_exist(S3_CLIENT, FIM_S3_BUCKET_NAME)
    if not is_success:
        logging.error(f"FIM_S3_BUCKET_NAME value of {FIM_S3_BUCKET_NAME}. Check the aws creds env file and case.")
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
        python ./foss_fim/workflows/hand_to_owp.py -hv 'hand_4_8_7_2'

        python ./foss_fim/workflows/hand_to_owp.py -hv 'hand_4_8_7_2'
            -lp '/data/workflows/deploy/
            -wp '/data/config/workflow_test_params.env'


    Notes:
        - This uses a combination of the -hv value and pathing included in the workflows_params.env.
          By default (overrideable in an env file), it downloads a filtered dataset
             from s3:{our bucket}/foss_fim/previous_fim/{hand version}
             to /data/previous_fim/{hand_version}
        - The files to download use a "pattern" system via the env file. It is similar to glob in its
          use of wildcards.
        - All files downloaded will overwrite existing ones.
    '''

    parser = argparse.ArgumentParser(
        description='Copies specific files/folders from FIM s3 bucket to OWP server folders.'
        'It includes only key files needed OWP servers that need to talk to WRDS\n'
        ' such as the CatFIM family and FIM Performance, points and polys, (eval_plots.py).'
    )

    parser.add_argument(
        '-hv',
        '--hand-version',
        help='REQUIRED: The version of hand you are copying. ie) hand_4_8_7_2. This will be the name in the \n'
        'S3... foss_fim/previous_fim folder and will also be the name of the local data/previous_fim folder.\n'
        'For debugging purposes, you can use values other than hand_x_x_x_x. Be careful of dots and cases',
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
        help='OPTIONAL: Path to workflows params(config) file.\n' '  Defaults to /data/config/workflow_params.env',
        default="/data/config/workflow_params.env",
    )

    parser.add_argument(
        '-lp',
        '--log-path',
        help='OPTIONAL: Path to where the log file will saved.\n'
        '  Defaults to /data/workflows/deploy/logs.\n'
        'The file name is auto-generated.',
        default='/data/workflows/deploy/logs',
    )

    parser.add_argument('-j', "--num-jobs", help="OPTIONAL: Number of processes (defaults to 10)",
                        type=int, default=10)

    args = parser.parse_args()

    hand_to_owp(**vars(args))

