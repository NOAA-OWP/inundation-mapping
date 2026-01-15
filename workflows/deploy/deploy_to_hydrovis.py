#!/usr/bin/env python3
import argparse
import glob
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
Any files uploaded will overwrite files when applicable.

Remember to check the values in the params env to ensure you have
the HV_S3_BUCKET_NAME and other HV_S3_ values. Remember, the default is
production folder for all HV services. But you can make a test env
to put files up to test locations and add the params arg.

This tools is designed for a source being a local/EFS drive (docker mapped drive)
and the target being HydroVIS buckets and pathing.
'''

'''
All deploy types can be done with aws s3 cli.  Why do we do it via python/boto3?
    We can use multi-thread to make it faster and have more control of contents and rules.
'''

# ============================
# GLOBAL Vars including files / folders to be copied

S3_CLIENT = None  # boto3 client (works for both buckets)
HV_S3_BUCKET_NAME = ""  # Comes from the aws cred env file

# Neither of these include their bucket names
HV_S3_ROOT_HANDSET_PATH = ""  # The path up to and including the hand folder but without the bucket name.
HV_S3_ROOT_QA_DATASETS_PATH = (
    ""  # the patch up to and including the qa_dataset path but without the bucket name.
)
HAND_VERSION = ""
RELEASE_FIM_PUBLIC_VERSION = ""
HAND_PREVIOUS_VERSION = ""


# ============================
def deploy_to_hydrovis(deploy_type, aws_creds_file, deploy_params_file, log_path, num_jobs):
    '''
    Notes:
        - num_jobs is used by HAND dataset uploads at this time. Maximum number is variables and depends on how fast
          the machines network speed is.

    Valid deploy_types
        - hand (HAND Bed outputs)
        - fpc  (FIM Performance Catchments)
        - fpp  (FIM Performance Points / Polys)
        - cffb (CatFIM Flow Based)
        - cffc (CatFIM Flow Based Compare files)
        - cfsb (CatFIM Stage Based)
        - cfsc (CatFIM Stage Based Compare files)
        - rcc  (Rating Curve Comparion Metrics (Sierra Tests))
        - urc  (USGS Rating Curve)
    '''

    all_valid_types = ['fpc', 'fpp', 'cffb', 'cffc', 'cfsb', 'cfsc', 'rcc', 'urc', 'hand']

    print("****  Deploy to HydroVIS Started   ****")
    # --------------
    # Validation. We are validating all variables in case the call came in from another py file
    # We also load a number of key variables (load env)
    deploy_types = __validate_input(deploy_type, all_valid_types, deploy_params_file, num_jobs)

    # May throw exceptions or shut the program down.
    __setup_aws(aws_creds_file)

    overall_start_dt = datetime.now(timezone.utc)

    # setup logs
    sf.setup_file_logger(log_path, "deloy_to_hydrovis")
    logging.info(f"Start time: {overall_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")
    logging.info(f"Deploy types to upload: {deploy_type}")

    logging.info(
        f"Loading to s3://{HV_S3_BUCKET_NAME}{HV_S3_ROOT_HANDSET_PATH}"
        f" and/or s3://{HV_S3_BUCKET_NAME}{HV_S3_ROOT_QA_DATASETS_PATH} as applicable."
    )
    print("")

    try:

        # We will send all to qa datasets and it can pick out what it needs.
        __load_qa_dataset(deploy_types, deploy_params_file)

        if 'hand' in deploy_type:
            __load_hand_dataset(deploy_params_file, num_jobs)

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
def __load_hand_dataset(deploy_params_file, num_jobs):

    # We filter to keep only the files we specifically want
    # We will build up a list of files for upload as AWS can only
    # upload one a time, but for uploading we will use mp and muliple s3 clients, not he one from this page level
    files_to_upload = []  # a list of dictionaries
    # {"src_file": file_path, "trg_file": trg_file}

    logging.info("------------------------------------")
    logging.info("**** Begin loading HAND dataset into HydroVIS S3 ****")
    section_start_dt = datetime.now(timezone.utc)
    print(f"Section start time: {section_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")

    # Probably from previous_fim
    hand_local_dataset_path = __get_env_variable_with_versions(
        'FIM_HAND_DATASET_LOCAL_PATH', deploy_params_file
    )
    if not os.path.exists(hand_local_dataset_path):
        raise ValueError(f"FIM_HAND_DATASET_LOCAL_PATH of {hand_local_dataset_path} does not exist")
    hand_local_dataset_path = sf.add_slashes_to_path(hand_local_dataset_path)

    # ----------------
    # as each search_key needs to be used one at a time to figure out which are to be included
    # or maybe we use MT here?
    file_patterns = []

    file_pattern_key = "HV_HAND_LOAD_PATTERN"
    for name, value in os.environ.items():
        if file_pattern_key in name:
            file_patterns.append({"env_var_name": name, "env_var_value": value})

    # Load each cmd one at a time from the enviro, then feed it to grep to get the files we
    # want. Remember.. AWS can only download/upload one file at a time (AWS Keys versus actual
    # directories.)
    print("*** Note: Some loads per pattern can be slow, especially branches.")
    print("*** We get the names and paths of all files that are applicable before copying")
    time.sleep(5)  # gives the a min to read this.
    print("")

    for file_pattern in file_patterns:
        pattern_name = file_pattern["env_var_name"]
        pattern = file_pattern["env_var_value"]

        logging.info(f"Getting file names for pattern {pattern_name} : {pattern}")

        # slashes already exist on the hand_local_dataset_path
        full_path_pattern = hand_local_dataset_path + pattern
        # some path combinations can end up with a double slash
        full_path_pattern = full_path_pattern.replace("//", "/")

        file_paths = glob.glob(full_path_pattern)

        for file_path in file_paths:

            # glob can end with double slashes sometimes so remove them
            if "//" in file_path:
                file_path = file_path.replace("//", "/")

            trg_file = file_path.replace(hand_local_dataset_path, HV_S3_ROOT_HANDSET_PATH)
            upload_item = {"src_file": file_path, "trg_file": trg_file}
            files_to_upload.append(upload_item)

        logging.info(f".. found {len(file_paths)} files")

        if len(file_paths) == 0:
            print("*********************")
            logging.error(
                f"**** ERROR: no files were found pattern {pattern_name} : {pattern}."
                " Check the data source folders and/or the patterns from the env file."
            )
            time.sleep(5)  # allows the user time to react if required

    logging.info(f"--- Total number of files to be loaded to HV S3 is {len(files_to_upload)}")

    sorted_files_to_upload = sorted(files_to_upload, key=lambda x: x["src_file"])
    logging.info("------------------------------------")

    # Let's it do its own upload, instead of the generic parent deploy_to_hydrovis pattern.
    # This set is pretty big so pass it to s3_shared_functions.upload_large_files,
    # which has a form of a multi-proc inside of it. (no logging though)
    logging.info("Downloading started")
    s3_sf.upload_by_file_list(S3_CLIENT, HV_S3_BUCKET_NAME, sorted_files_to_upload, num_jobs)

    print("")
    end_time = datetime.now(timezone.utc)
    logging.info("**** Completed loading HAND dataset into HydroVIS S3 ****")
    print(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(fh.print_date_time_duration(section_start_dt, end_time, False))
    logging.info("------------------------------------")


# ============================
def __load_qa_dataset(deploy_types, deploy_params_file):

    files_to_upload = []

    # catchments
    if 'fpc' in deploy_types:
        upload_item = __get_file_to_upload_item('HV_FIM_PERF_CATCHMENTS_FILE', deploy_params_file)
        files_to_upload.append(upload_item)

    # Points and Polys are loaded together and are very quick
    if 'fpp' in deploy_types:
        # Polys first
        upload_item = __get_file_to_upload_item('HV_FIM_PERF_POLYS_FILES', deploy_params_file)
        files_to_upload.append(upload_item)

        # Points first
        upload_item = __get_file_to_upload_item('HV_FIM_PERF_POINTS_FILES', deploy_params_file)
        files_to_upload.append(upload_item)

    # rating curve comparison (Sierra test)
    if 'rcc' in deploy_types:
        upload_item = __get_file_to_upload_item('HV_RCC_NWM_RECURR_FLOW_FILE', deploy_params_file)
        files_to_upload.append(upload_item)

    # Latest usgs rating curve
    if 'urc' in deploy_types:
        upload_item = __get_file_to_upload_item('HV_URC_RATING_CURVE_FILE', deploy_params_file)
        files_to_upload.append(upload_item)

    # CatFIM Flow (sites and library)
    if 'cffb' in deploy_types:
        upload_item = __get_file_to_upload_item('HV_CAT_FLOW_SITES', deploy_params_file)
        files_to_upload.append(upload_item)

        upload_item = __get_file_to_upload_item('HV_CAT_FLOW_LIBRARY', deploy_params_file)
        files_to_upload.append(upload_item)

    # CatFIM Flow Compare files
    if 'cffc' in deploy_types:
        upload_item = __get_file_to_upload_item('HV_CAT_COMPARE_FLOW_SITES', deploy_params_file)
        files_to_upload.append(upload_item)

        upload_item = __get_file_to_upload_item('HV_CAT_COMPARE_FLOW_GAINED_COVERAGE', deploy_params_file)
        files_to_upload.append(upload_item)

        upload_item = __get_file_to_upload_item('HV_CAT_COMPARE_FLOW_LOST_COVERAGE', deploy_params_file)
        files_to_upload.append(upload_item)

    # CatFIM Stage (sites and library)
    if 'cfsb' in deploy_types:
        upload_item = __get_file_to_upload_item('HV_CAT_STAGE_SITES', deploy_params_file)
        files_to_upload.append(upload_item)

        upload_item = __get_file_to_upload_item('HV_CAT_STAGE_LIBRARY', deploy_params_file)
        files_to_upload.append(upload_item)

    # CatFIM Stage Compare files
    if 'cfsc' in deploy_types:
        upload_item = __get_file_to_upload_item('HV_CAT_COMPARE_STAGE_SITES', deploy_params_file)
        files_to_upload.append(upload_item)

        upload_item = __get_file_to_upload_item('HV_CAT_COMPARE_STAGE_GAINED_COVERAGE', deploy_params_file)
        files_to_upload.append(upload_item)

        upload_item = __get_file_to_upload_item('HV_CAT_COMPARE_STAGE_LOST_COVERAGE', deploy_params_file)
        files_to_upload.append(upload_item)

    # -----------------------
    # Upload files as per arguments going to HV QA Dataset folders

    # this is qa datasets and there are not many files, so we will just load them one at a time.
    if len(files_to_upload) > 0:
        logging.info("------------------------------------")
        logging.info("**** Begin loading misc HydroVIS QA dataset files into S3 ****")
        section_start_dt = datetime.now(timezone.utc)
        print(f"Section start time: {section_start_dt.strftime('%m/%d/%Y %H:%M:%S')} UTC")

        # Load each file. Note: One AWS Client can only load one file at a time.
        for file in files_to_upload:
            logging.info(f"-- Uploading {file['src_file']}")

            # boto3 only allows one file at a time.
            # Upload file will tell us if the file does not exist.
            file_exists, ___ = s3_sf.upload_file(
                S3_CLIENT, HV_S3_BUCKET_NAME, file['src_file'], file['trg_file']
            )
            if not file_exists:
                print("")
                logging.info(f"**** Skipped uploading {file['src_file']}. File does not exist in source path.")
                print("")

        print("")
        end_time = datetime.now(timezone.utc)
        logging.info(
            f"**** Completed loading misc HydroVIS QA dataset files into S3: {len(files_to_upload)} uploaded."
        )
        print(f"Section end time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
        logging.info(fh.print_date_time_duration(section_start_dt, end_time, False))
        logging.info("------------------------------------")


# ============================
def __get_env_variable_with_versions(env_var_name, deploy_params_file):
    # This will load an enviro variable, raising an exception if it does not exist
    # if it does exist, see if it needs subsitution in the value for either the
    # HAND_VERSION or RELEASE_FIM_PUBLIC_VERSION as many paths have one or more of these in the value
    # it is ok if both or neither of those values exist in then env arg
    env_value = sf.get_value_from_env(env_var_name, deploy_params_file)
    env_value = env_value.replace("{RELEASE_FIM_PUBLIC_VERSION}", RELEASE_FIM_PUBLIC_VERSION)
    env_value = env_value.replace("{HAND_PREVIOUS_VERSION}", HAND_PREVIOUS_VERSION)
    env_value = env_value.replace("{HAND_VERSION}", HAND_VERSION)
    return env_value


# ============================
def __get_file_to_upload_item(env_var_name, deploy_params_file):

    file_path = __get_env_variable_with_versions(env_var_name, deploy_params_file)
    if not file_path.startswith("/"):
        file_path = "/" + file_path
    file_name = os.path.basename(file_path)
    trg_file = HV_S3_ROOT_QA_DATASETS_PATH + file_name
    upload_item = {"src_file": file_path, "trg_file": trg_file}
    return upload_item


# ============================
def __validate_input(deploy_type, all_valid_types, deploy_params_file, num_jobs):
    '''
    Will return updates to variables or new variables extrapolated.
    We are checking values more carefully for empty values as we can not assume this script
    was run from command line, but possible as part of other scripts.
    This also sets up a bunch of key variables and paths.
    '''

    global HV_S3_ROOT_HANDSET_PATH, HV_S3_ROOT_QA_DATASETS_PATH, HV_S3_BUCKET_NAME
    global RELEASE_FIM_PUBLIC_VERSION, HAND_VERSION, HAND_PREVIOUS_VERSION

    if not deploy_type:
        raise ValueError("The deploy type variable is None or empty")

    deploy_types = deploy_type.split()
    invalid_types = list(set(deploy_types) - set(all_valid_types))
    if len(invalid_types) > 0:
        raise ValueError(f"The following deployment types are invalid: {invalid_types}")

    if not deploy_params_file:
        raise ValueError("workflows params file variable is None or empty")
    if not os.path.isfile(deploy_params_file):
        raise ValueError(f"params file of {deploy_params_file} can not be found. Check path and/or case.")

    logging.info(f"loading working params file ({deploy_params_file})")
    load_dotenv(deploy_params_file)

    # shorthand for the os.getenv
    HAND_VERSION = sf.get_value_from_env("HAND_VERSION", deploy_params_file)
    RELEASE_FIM_PUBLIC_VERSION = sf.get_value_from_env("RELEASE_FIM_PUBLIC_VERSION", deploy_params_file)
    HAND_PREVIOUS_VERSION = sf.get_value_from_env("HAND_PREVIOUS_VERSION", deploy_params_file)

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

    # we load the bucket name from the aws file to help with git security a little.
    # We validate the bucket existance in __setup_aws
    HV_S3_BUCKET_NAME = sf.get_value_from_env("HV_S3_BUCKET_NAME", deploy_params_file)
    HV_S3_BUCKET_NAME = HV_S3_BUCKET_NAME.strip('/')

    return deploy_types


# ============================
def __setup_aws(aws_creds_file):

    # We validate the bucket existance in here and assume the deploy env file is alreayd loaded

    global S3_CLIENT

    if not aws_creds_file:
        raise ValueError("aws credentials file argument is None or empty")

    if not os.path.isfile(aws_creds_file):
        raise ValueError(
            f"aws credentials file of {aws_creds_file} can not be found. Check path and/or case."
        )

    logging.info(f"loading aws credentials file ({aws_creds_file})")
    load_dotenv(aws_creds_file)

    # setup the client and validate the bucket
    hv_aws_access_key = sf.get_value_from_env("HV_AWS_ACCESS_KEY_ID", aws_creds_file)
    hv_aws_secret_key = sf.get_value_from_env("HV_AWS_SECRET_ACCESS_KEY", aws_creds_file)
    hv_aws_region = sf.get_value_from_env("HV_AWS_REGION_NAME", aws_creds_file)

    is_success, return_msg, S3_CLIENT = asf.create_aws_client(
        aws_service_type_name='s3',
        aws_access_key_id=hv_aws_access_key,
        aws_secret_access_key=hv_aws_secret_key,
        aws_region=hv_aws_region,
    )

    if not is_success:  # if it was not already thrown from asf
        raise Exception(return_msg)

    # validate the bucket
    # may also throw an exception
    # assumes the bucket name is already loaded when the deploy env was loaded
    is_success, return_msg = s3_sf.does_s3_bucket_exist(S3_CLIENT, HV_S3_BUCKET_NAME)
    if not is_success:
        logging.error(
            f"HV_S3_BUCKET_NAME value of {HV_S3_BUCKET_NAME}. Check the aws creds env file and case."
        )
        logging.error(return_msg)
        print("Program aborted")
        sys.exit(1)


# ============================
if __name__ == '__main__':

    '''
    This script looks for two defaulted params env file.
       - '-ac/--aws-creds-file': contains only aws credentials info such as AWS Access Keys.
       - '-dp/--deploy-params-file': Contains an array of values which can things such as
         variables for deployments to HV.

    SRC pathing can be from local folders (EFS or dev_fim_share)

    Sample Usages
        python /foss_fim/workflows/deploy/deploy_to_hydrovis.py
            -dt 'fpc fpp'
            # Yes.. can be more than one dt

        python /foss_fim/workflows/deploy/deploy_to_hydrovis.py
            -dt hand
            -lp '/data/workflows/deploy/
            -dp '/data/config/deploy_params_tests.env'

    *** While this system does support mp, it is used only when loading HAND datasets. Also.. there is a max of how many
        connections we can make to AWS at the same time. That number is variable and depends on your machines network speed mostly.

    # For HV deployments...

    Notes about Types:
       The type value provided tells the script which files to pull from the src EFS
       and where to put them in the HV bucket. Those file / subfolder paths are hardcoded in here
       to make uploads standardized. You can submit only one type for uploading at a time.
       Some of those types will automatically upload one or multiple files / folders as
       applicable.
       Options are:
         - hand (HAND Bed outputs)
         - fpc  (FIM Performance Catchments)
         - fpp  (FIM Performance Points / Polys)
         - cffb (CatFIM Flow Based)
         - cffc (CatFIM Flow Based Compare files)
         - cfsb (CatFIM Stage Based)
         - cfsc (CatFIM Stage Based Compare files)
         - rcc  (Rating Curve Comparison Metrics (Sierra Tests))
         - urc  (USGS Rating Curve)

    This can handle multiple types of uploads to HV. The actual pathing is in the .env
    file and needs to be adjusted to HV target. That bucket folder needs to pre-exist.
    Generally the pattern as of Sept 2025 is:
       s3://{hv bucket name}/fim/{fim_version}/{hand_version}
    When the HAND (BED) dataset is uploaded, it filters out files / folders to keep only what hv.
       That is typically comes from /data/previous_fim
    needs.  The "/qa_dataset" is where most non HAND BED files live such as:
        - catfim files
        - fim performance files
        - usgs files
        - etc
    '''

    parser = argparse.ArgumentParser(
        description='Copies specific files/folders to HV s3.'
        'It includes only key files HV needs for services.'
    )

    # We will not use nargs but parse it ourselves.
    parser.add_argument(
        '-dt',
        '--deploy-type',
        help='REQUIRED: Type of deployment. For allowed values, see code notes',
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
        '-dp',
        '--deploy-params-file',
        help='OPTIONAL: Path to deploy params(config) file.\n'
        '  Defaults to /data/config/hv_deploy_params.env',
        default="/data/config/hv_deploy_params.env",
    )

    parser.add_argument(
        '-lp',
        '--log-path',
        help='OPTIONAL: Path to where the log file will saved.\n'
        '  Defaults to /data/workflows/deploy/logs.\n'
        'The file name is auto-generated.',
        default='/data/workflows/deploy/logs',
    )

    parser.add_argument(
        '-j', "--num-jobs", help="OPTIONAL: Number of processes (defaults to 10)", type=int, default=10
    )

    args = parser.parse_args()

    deploy_to_hydrovis(**vars(args))
