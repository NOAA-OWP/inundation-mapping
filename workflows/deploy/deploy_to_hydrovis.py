#!/usr/bin/env python3
import argparse
import glob
import logging
import os
import sys
import traceback

from datetime import datetime, timezone
from dotenv import load_dotenv

import src.utils.shared_functions as sf
import data.aws.aws_shared_functions as asf
import data.aws.s3_shared_functions as s3_sf
import workflows.workflow_shared_functions as wsf

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

# ============================
# GLOBAL Vars including files / folders to be copied

S3_CLIENT = None  # boto3 client (works for both buckets)

# Neither of these include their bucket names
TRG_HV_BUCKET_NAME = ""
HV_TRG_S3_HAND_PATH = ""  # The path up to and including the hand folder but without the bucket name.
HV_TRG_S3_QA_DATASET_PATH = ""  # the patch up to and including the qa_dataset path but without the bucket name.
FIM_SRC_ROOT_PATH = ""

# ============================
def deploy_to_hydrovis(deploy_type, aws_creds_file, workflow_params_file, log_path):

    print("****  Deploy to HydroVIS Started   ****")
    # --------------
    # Validation. We are validating all variables in case the call came in from another py file
    # We also load a number of key variables (load env)
    deploy_types = __validate_input(deploy_type, workflow_params_file)

    # May throw exceptions or shut the program down.
    __setup_aws(aws_creds_file)

    # setup logs
    overall_start_dt = datetime.now(timezone.utc)
    file_datetime_string = overall_start_dt.strftime("%Y%m%d-%H%M")    
    log_file_name = f"deploy_to_hydrovis-{file_datetime_string}.log"
    log_file_path = os.path.join(log_path, log_file_name)
    sf.setup_file_logger(log_file_path)
    logging.info(f"Start time: {overall_start_dt.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(f"Logs saved to: {log_file_path}")
    logging.info(f"Deploy types to upload: {deploy_type}")
    logging.info(f"Loading to s3://{TRG_HV_BUCKET_NAME}{HV_TRG_S3_HAND_PATH}"
                 f" and s3://{TRG_HV_BUCKET_NAME}{HV_TRG_S3_QA_DATASET_PATH}."
                 " Depending on while deploy types were selected.")
    logging.info("------------------------")

    try:

        files_to_upload = []

        # breaking this up to smaller parts for readability. Remember, we can have more than one deploy_type
        # if 'hand' in deploy_type:
        #     __load_hand_dataset()
        # TODO: It is so large, maybe let it do it's own loading with mp??
        # using aws s3 sync takes 1 to 2 hours, but we can get that faster here
        # with more managed mp

        if 'fpc' in deploy_types or 'fpp' in deploy_types:
            files_to_upload.extend(__load_fim_performance(deploy_types))

        # if 'cffb' in deploy_types or 'cffc' in deploy_types or 'cfsb' in deploy_types or 'cfsc' in deploy_types:
        #     files_to_upload.extend(__load_catfim_files(deploy_types)

        if 'rcc' in deploy_types or 'urc' in deploy_types:
            files_to_upload.extend(__load_misc_files(deploy_types))

        # Load each file. Note: One AWS Client can only load one file at a time.
        # TODO: Add MP so we can create mulitiple clients and load multiple at a time.
        for file in files_to_upload:
            logging.info(f"-- Uploading {file['src_file']}")

            # boto3 only allows one file at a time.
            # Upload file will tell us if the file does not exist.
            file_exists = s3_sf.upload_file(S3_CLIENT, TRG_HV_BUCKET_NAME,
                                            file['src_file'],
                                            file['trg_file'])
            if not file_exists:
                logging.info(f"-- Skipped uploading {file['src_file']}. File does not exist.")

    except Exception:
        logging.critical(traceback.format_exc())

    logging.info("==========================================================")
    end_time = datetime.now(timezone.utc)
    logging.info("****  Completed Deploy to HydroVIS  ****")
    logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(fh.print_date_time_duration(overall_start_dt, end_time, False))


# ============================
def __load_hand_dataset():

    # We filter to keep only the files we specifically want
    # We will build up a list of files for upload as AWS can only 
    # upload one a time.


    # TODO: Figure out what we have for roads.

    # osm_roads_fimpact.csv at huc level?

    files_to_upload = []
    file_patterns = [f"*/hydrotable.feather",
                     f"*/hydrotable.parquet",
                     f"*/wbd.gpkg",
                     f"*/usgs_elev_table.csv",
                     f"*/osm_bridge_centroids.gpkg",
                     f"*/osm_roads_fimpact.csv",
                     f"*/branches/*/gw_catchments_reaches_filtered_addedAttributes_*.tif",
                     f"*/branches/*/gw_catchments_reaches_filtered_addedAttributes_crosswalk_*.gpkg",
                     f"*/branches/*/src_full_crosswalked_*.csv",
                     f"*/branches/*/rem_zeroed_masked_*.tif",
                     f"crosswalk_table.csv",
                     f"fim_inputs.csv",
                     ]
    
    logging.info("--- Loading HAND dataset")

    for pattern in file_patterns:
        full_pattern = f"/{SRC_PATH}/{pattern}"
        files_to_upload.extend(glob.glob(full_pattern))
    print(f"Number of files found is {len(files_to_upload)}")


# ============================
def __load_fim_performance(deploy_types):

    files_to_upload = []

    # catchments
    if 'fpc' in deploy_types:
        src_file = wsf.get_value_from_env('FIM_PERF_CATCHMENTS_FILE', validate_path=False)

        # src_file = os.path.join(HV, file_name)
        file_name = src_file.split("/")[-1]
        trg_file = HV_TRG_S3_QA_DATASET_PATH + file_name

        upload_item = {
            "file_name": file_name,
            "src_file": src_file,
            "trg_file": trg_file
        }
        files_to_upload.append(upload_item)

    # Points and Polys are loaded together and are very quick
    if 'fpp' in deploy_types:
        # comes in a list of paths
        point_poly_file_arg = wsf.get_value_from_env('FIM_PERF_POINTS_POLYS_FILES', validate_path=False)
        point_poly_files = point_poly_file_arg.split(',')

        for file_path in point_poly_files:
            # It is ok if the file is not there, we will just show it.
            file_name = file_path.split("/")[-1]
            trg_file = HV_TRG_S3_QA_DATASET_PATH + file_name

            upload_item = {
                "file_name": file_name,
                "src_file": file_path,
                "trg_file": trg_file
            }
            files_to_upload.append(upload_item)

    return files_to_upload


# ============================
# def __load_catfim_files(deploy_types):


# ============================
def __load_misc_files(deploy_types):

    files_to_upload = []

    # rating curve comparison (Sierra test)
    if 'rcc' in deploy_types:
        src_file = wsf.get_value_from_env('RCC_NWM_RECURR_FLOW_FILE', validate_path=False)
        file_name = src_file.split("/")[-1]
        trg_file = HV_TRG_S3_QA_DATASET_PATH + file_name

        upload_item = {
            "file_name": file_name,
            "src_file": src_file,
            "trg_file": trg_file
        }
        files_to_upload.append(upload_item)

    # Latest usgs rating curve
    if 'urc' in deploy_types:
        src_file = wsf.get_value_from_env('URC_RATING_CURVE_FILE', validate_path=False)
        file_name = src_file.split("/")[-1]
        trg_file = HV_TRG_S3_QA_DATASET_PATH + file_name

        upload_item = {
            "file_name": file_name,
            "src_file": src_file,
            "trg_file": trg_file
        }
        files_to_upload.append(upload_item)

    return files_to_upload


# ============================
def __validate_input(deploy_type, workflow_params_file):

    '''
    Will return updates to variables or new variables extrapolated.
    We are checking values more carefully for empty values as we can not assume this script
    was run from command line, but possible as part of other scripts.

    - hand (HAND Bed outputs)
    - fpc  (FIM Performance Catchments)
    - fpp  (FIM Performance Points / Polys)
    - cffb (CatFIM Flow Based)
    - cffc (CatFIM Flow Based Compare files)
    - cfsb (CatFIM Stage Based)
    - cfsc (CatFIM Stage Based Compare files)
    - rcc  (Rating Curve Comparion Metrics (Sierra Tests))
    - urc  (USGS Rating Curve)    

    This also sets up a bunch of key variables and paths.
    '''

    global FIM_SRC_ROOT_PATH, TRG_HV_BUCKET_NAME, HV_TRG_S3_ROOT_HAND_PATH, HV_TRG_S3_QA_DATASET_PATH

    valid_types = ['hand', 'fpc', 'fpp', 'cffb', 'cffc', 'cfsb', 'cfsc', 'rcc', 'urc']

    if deploy_type is None or deploy_type == "":
        raise ValueError("The deploy type variable is None or empty")

    deploy_types = deploy_type.split()
    invalid_types = list(set(deploy_types) - set(valid_types))
    if len(invalid_types) > 0:
        raise ValueError(f"The following deployment types are invalid: {invalid_types}")

    if workflow_params_file is None or workflow_params_file == "":
        raise ValueError("workflows params file variable is None or empty")
    if not os.path.exists(workflow_params_file):
        raise ValueError(f"params file of {workflow_params_file} can not be found. Check path and/or case.")

    load_dotenv(workflow_params_file)

    # shorthand for the os.getenv
    FIM_SRC_ROOT_PATH = os.getenv('FIM_SRC_ROOT_PATH')
    if FIM_SRC_ROOT_PATH is None or FIM_SRC_ROOT_PATH == "":
        raise ValueError("workflows params env variable of FIM_SRC_ROOT_PATH does not exist or is empty")
    
    TRG_HV_BUCKET_NAME = os.getenv('HV_S3_BUCKET_NAME')
    if TRG_HV_BUCKET_NAME is None or TRG_HV_BUCKET_NAME == "":
        raise ValueError("workflows params env variable of TRG_HV_BUCKET_NAME does not exist or is empty")

    HV_TRG_S3_ROOT_HAND_PATH = os.getenv('HV_TRG_S3_ROOT_HAND_PATH')
    if HV_TRG_S3_ROOT_HAND_PATH is None or HV_TRG_S3_ROOT_HAND_PATH == "":
        raise ValueError("workflows params env variable of HV_TRG_S3_ROOT_HAND_PATH does not exist or is empty")

    HV_TRG_S3_QA_DATASET_PATH = os.getenv('HV_TRG_S3_ROOT_QA_DATASETS_PATH')
    if HV_TRG_S3_QA_DATASET_PATH is None or HV_TRG_S3_QA_DATASET_PATH == "":
        raise ValueError("workflows params env variable of HV_TRG_S3_QA_DATASET_PATH does not exist or is empty")

    # add slashs front and back for all paths
    if not FIM_SRC_ROOT_PATH.endswith("/"):
        FIM_SRC_ROOT_PATH += "/"
    if not FIM_SRC_ROOT_PATH.startswith("/"):
        FIM_SRC_ROOT_PATH = "/" + FIM_SRC_ROOT_PATH

    if not HV_TRG_S3_ROOT_HAND_PATH.endswith("/"):
        HV_TRG_S3_ROOT_HAND_PATH += "/"
    if not HV_TRG_S3_ROOT_HAND_PATH.startswith("/"):
        HV_TRG_S3_ROOT_HAND_PATH = "/" + HV_TRG_S3_ROOT_HAND_PATH

    if not HV_TRG_S3_QA_DATASET_PATH.endswith("/"):
        HV_TRG_S3_QA_DATASET_PATH += "/"
    if not HV_TRG_S3_QA_DATASET_PATH.startswith("/"):
        HV_TRG_S3_QA_DATASET_PATH = "/" + HV_TRG_S3_QA_DATASET_PATH

    return deploy_types


# ============================
def __setup_aws(aws_creds_file):

    global S3_CLIENT

    if aws_creds_file is None or aws_creds_file == "":
        raise ValueError("aws credentials file argument is None or empty")
    
    if not os.path.exists(aws_creds_file):
        raise ValueError(f"aws credentials file of {aws_creds_file} can not be found. Check path and/or case.")

    load_dotenv(aws_creds_file)

    # setup the client and validate the bucket
    hv_aws_access_key = os.getenv('HV_AWS_ACCESS_KEY_ID')
    if hv_aws_access_key is None or hv_aws_access_key == "":
        raise ValueError("aws creds env file variable of HV_AWS_ACCESS_KEY_ID does not exist or is empty")

    hv_aws_secret_key = os.getenv('HV_AWS_SECRET_ACCESS_KEY')
    if hv_aws_secret_key is None or hv_aws_secret_key == "":
        raise ValueError("aws creds env file variable of HV_AWS_SECRET_ACCESS_KEY does not exist or is empty")

    hv_aws_region = os.getenv('HV_AWS_REGION_NAME')
    if hv_aws_region is None or hv_aws_region == "":
        raise ValueError("aws creds env file variable of HV_AWS_REGION_NAME does not exist or is empty")

    is_success, return_msg, S3_CLIENT = asf.create_aws_client(
        aws_service_type_name = 's3',
        aws_access_key_id = hv_aws_access_key,
        aws_secret_access_key = hv_aws_secret_key,
        aws_region = hv_aws_region,
    )

    if not is_success:
        raise Exception(return_msg)

    # validate the bucket
    # may also throw an exceptoin
    is_success, return_msg = s3_sf.does_s3_bucket_exist(S3_CLIENT, TRG_HV_BUCKET_NAME)
    if not is_success:
        # logging.error("FIM bucket name of {FIM_S3_BUCKET_NAME}. Check the config file and case.")
        # logging.error(return_msg)
        print(return_msg)
        print("Program aborted")
        sys.exit(1)


# ============================
if __name__ == '__main__':

    '''
    This script looks for two defaulted params env file.
       - '-ac/--aws-creds-file': contains only aws credentials info such as AWS Access Keys.
       - '-wp/--workflow-params-file': Contains an array of values which can things such as
         variables for deployments, uploads, downloads, workflow automation values, etc

    SRC pathing can be from local folders (EFS or dev_fim_share)

    Sample Usage (min args)
        python /foss_fim/workflows/deploy/deploy_to_hydrovis.py
            -dt 'fpc fpp' 
            # Yes.. can be more than one dt

        python /foss_fim/workflows/deploy/deploy_to_hydrovis.py
            -dt hand
            -lp '/data/workflows/deploy/
            -wp '/data/config/workflow_params_tests.env'

    # For HV deployments...

    Notes about Types:
       The type value provided tells the script which files to pull from the src EFS
       and where to put them in the HV bucket. Those file / subfolder paths are hardcoded in here
       to make uploads standardized. You can submit only one type for uploading at a time.
       Some of those types will automatically upload one or multiple files / folders as 
       applicable.
       Options are:
         - hand (HAND Bed outputs)
         - fpc  (Fim Performance Catchments)
         - fpp  (FIM Performance Points / Polys)
         - cffb (CatFIM Flow Based)
         - cffc (CatFIM Flow Based Compare files)
         - cfsb (CatFIM Stage Based)
         - cfsc (CatFIM Stage Based Compare files)
         - rcc  (Rating Curve Comparion Metrics (Sierra Tests))
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

    parser = argparse.ArgumentParser(description='Copies specific files/folders to HV s3.'
                                     'It includes only key files HV needs for services.')

    # We will not use nargs but parse it ourselves.
    parser.add_argument('-dt', '--deploy-type',
                        help='REQUIRED: Type of deployment. For allowed values, see code notes',
                        required=True,
                        type=str,
                        )

    parser.add_argument('-ac', '--aws-creds-file',
                        help='OPTIONAL: full pathed mapped docker path to the AWS Credentials file.\n'
                        '  Defaults to /data/config/aws_credentials.env',
                        default='/data/config/aws_credentials.env'
                        )

    parser.add_argument('-wp', '--workflow-params-file',
                        help='OPTIONAL: Path to workflow params(config) file.\n'
                        '  Defaults to /data/config/workflow_params.env',
                        default="/data/config/workflow_params.env",
                        )

    parser.add_argument('-lp', '--log-path',
                        help='OPTIONAL: Path to where the log file will saved.\n'
                        '  Defaults to /data/workflows/deploy.\n'
                        'The file name is auto-generated.',
                        default='/data/workflows/deploy'
                        )

    args = parser.parse_args()

    deploy_to_hydrovis(**vars(args))

