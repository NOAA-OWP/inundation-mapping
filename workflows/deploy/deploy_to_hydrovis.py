#!/usr/bin/env python3

import argparse
import glob
import os
import sys

from datetime import datetime, timezone
from dotenv import load_dotenv # type: ignore

import data.aws.s3_shared_functions as s3_sf
import workflows.deploy.deploy_shared_functions as dsf
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
TRG_HV_S3_HAND_PATH = ""  # The path up to and including the hand folder but without the bucket name.
TRG_HV_S3_QA_DATASET_PATH = ""  # the patch up to and including the qa_dataset path but without the bucket name.
SRC_PATH = ""


# ============================
def deploy_to_hydrovis(src_path, deploy_types, params_file, aws_profile_name):

    # split the string on spaces
    deploy_types = deploy_types.split()

    # --------------
    # Validation. We are validating all variables in case the call came in from another py file
    __validate_input(src_path, deploy_types, params_file)

    # May throw exceptions or shut the program down.
    __setup_aws(aws_profile_name)

    # TODO: Setup logging
    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    # sf.setup_file_logger(TRG_ROOT_PATH, "get_sample_data")
    # logging.info(f"Start time: {overall_start_time.strftime('%m/%d/%Y %H:%M:%S')}")

    print("********")
#    logging.info(f"Copying files/folders from {SRC_FIM_FILE_PATHS} to {output_root_folder}")    

    # breaking this up to smaller parts for readability. Remember, we can have more than one deploy_type
    # if 'hand' in deploy_types:
    #     __load_hand_dataset()

    if 'fpc' in deploy_types or 'fpp' in deploy_types:
        __load_fim_performance(deploy_types)

    # if 'cffb' in deploy_types or 'cffc' in deploy_types or 'cfsb' in deploy_types or 'cfsc' in deploy_types:
    #     __load_catfim_files(deploy_types)

    # if 'rcc' in deploy_types or 'urc' in deploy_types:
    #     __load_misc_files(deploy_types)
    

    # logging.info("==========================================================")
    end_time = datetime.now(timezone.utc)
    print("All done for now... yahoo")
    # logging.info("-- Completed getting sample data")
    # logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    # logging.info(fh.print_date_time_duration(overall_start_time, end_time, False))

# ============================
# def __load_hand_dataset():


# ============================
def __load_fim_performance(deploy_types):

    files_to_upload = []

    # catchments
    # Takes appx 3.5 hours on a prod EC2, progress bar will be auto shown in s3_shared_functions
    if 'fpc' in deploy_types:
        file_name = "fim_performance_catchments.csv"
        src_file = os.path.join(SRC_PATH, file_name)
        trg_file = TRG_HV_S3_QA_DATASET_PATH + file_name

        upload_item = {
            "file_name": file_name,
            "src_file": src_file,
            "trg_file": trg_file
        }
        files_to_upload.append(upload_item)

    # Points is very quick
    if 'fpp' in deploy_types:
        file_name = "fim_performance_points.csv"
        src_file = os.path.join(SRC_PATH, file_name)
        trg_file = TRG_HV_S3_QA_DATASET_PATH + file_name

        upload_item = {
            "file_name": file_name,
            "src_file": src_file,
            "trg_file": trg_file
        }
        files_to_upload.append(upload_item)

        # ------------------------
        file_name = "fim_performance_polys.csv"
        src_file = os.path.join(SRC_PATH, file_name)
        trg_file = TRG_HV_S3_QA_DATASET_PATH + file_name

        upload_item = {
            "file_name": file_name,
            "src_file": src_file,
            "trg_file": trg_file
        }
        files_to_upload.append(upload_item)

    for file in files_to_upload:
        # logging.info(f"-- Uploading {file['src_file']}")
        print(f"-- Uploading {file['src_file']}")

        # boto3 only allows one file at a time.
        file_exists = s3_sf.upload_file(S3_CLIENT, TRG_HV_BUCKET_NAME,
                                        file['src_file'],
                                        file['trg_file'])
        # if not file_exists:
            # logging.info(f"-- Skipped uploading {src_file}. File does not exist.")
        print(f"did it work? {file_exists}")


# ============================
# def __load_catfim_files(deploy_types):


# ============================
# def __load_misc_files(deploy_types):


# ============================
def __validate_input(src_path, deploy_types, params_file):

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

    global SRC_PATH

    valid_types = ['hand', 'fpc', 'fpp', 'cffb', 'cffc', 'cfsb', 'cfsc', 'rcc', 'urc']

    if not isinstance(deploy_types, list) or len(deploy_types) == 0:
        raise ValueError("The value deploy types is either not a list or is empty")

    invalid_deploy_types = list(set(deploy_types) - set(valid_types))
    if len(invalid_deploy_types) > 0:
        raise ValueError(f"Some invalid deploy types have been included ({invalid_deploy_types})")

    if not src_path:
        raise ValueError("The source path version variable is None or empty")

    if not os.path.exists(src_path):
        raise ValueError(f"The source path of '{src_path}' does not exist or is unreachable")

    # add slashs front and back
    if not src_path.endswith("/"):
        src_path += "/"

    if not src_path.startswith("/"):
        src_path = "/" + src_path

    SRC_PATH = src_path

    if not params_file:
        raise ValueError("params file variable is None or empty")
    
    if not os.path.exists(params_file):
        raise ValueError(f"params file of {params_file} can not be found. Check path and/or case.")
        
    load_dotenv(params_file)


# ============================
def __setup_aws(aws_profile_name):

    global TRG_HV_BUCKET_NAME, TRG_HV_S3_HAND_PATH, TRG_HV_S3_QA_DATASET_PATH, S3_CLIENT
    # shorthand for the os.getenv
    TRG_HV_BUCKET_NAME = os.getenv('HV_S3_BUCKET_NAME')
    TRG_HV_S3_HAND_PATH = os.getenv('HV_S3_ROOT_HAND_PATH')
    TRG_HV_S3_QA_DATASET_PATH = os.getenv('HV_S3_ROOT_QA_DATASETS_PATH')

    # add slashs front and back for all paths
    if not TRG_HV_S3_HAND_PATH.endswith("/"):
        TRG_HV_S3_HAND_PATH += "/"

    if not TRG_HV_S3_HAND_PATH.startswith("/"):
        TRG_HV_S3_HAND_PATH = "/" + TRG_HV_S3_HAND_PATH

    if not TRG_HV_S3_QA_DATASET_PATH.endswith("/"):
        TRG_HV_S3_QA_DATASET_PATH += "/"

    if not TRG_HV_S3_QA_DATASET_PATH.startswith("/"):
        TRG_HV_S3_QA_DATASET_PATH = "/" + TRG_HV_S3_QA_DATASET_PATH

    # setup the client and validate the bucket
    is_success, return_msg, S3_CLIENT = s3_sf.create_s3_client(aws_profile_name)  # aws_profile_name might be empty and that is ok

    if not is_success:
        raise Exception(return_msg)

    # validate the bucket
    # may also throw an exceptoin
    is_success, return_msg = s3_sf.does_s3_bucket_exist(S3_CLIENT, TRG_HV_BUCKET_NAME)
    if not is_success:
        # logging.error("FIM bucket name of {FIM_S3_BUCKET_NAME}. Check the config file and case.")
        # logging.error(return_msg)
        print("Program aborted")
        sys.exit(1)


# ============================
if __name__ == '__main__':

    '''
    This script by default looks for a pre defined env file, but the -p file can be 
    used to use a custom/test env file.

    SRC pathing can be from local folders (EFS or dev_fim_share)

    For aws permissions, remember that permissions for the user inside the docker container
    may not be the same permissions when you are not using docker.

    The easiest way to manage aws credentials is to use the command line cmd of:
        aws configure --profile ti-temp (or some name)
        It will ask you for a keys
        Then add that as an arg to this script. ie) -ap "ti-temp"

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

    Sample Usage (min args)
        python /foss_fim/workflows/deploy/deploy_to_hydrovis.py
            -s "/data/previous_fim/hand_4_8_7_2"
            -types "hand fpc"  (in quotes and space delimited)

        examples fo src:
            - "/data/previous_fim/hand_4_8_7_2"
            - "/data/catfim/hand_4_8_7_2_flow_based/mapping"
            - "/data/catfim/hand_4_8_7_2_flow_based" (compare files ??)
            - "/data/fim_performance/20250821
            - etc

    Notes about Types:
       The type value(s) provided tells the script which files to pull from the src EFS
       and where to put them in the HV bucket. Those file / folder paths are hardcoded in here
       to make changes standardized. You can submit one or more types for uploading.
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

    '''

    parser = argparse.ArgumentParser(description='Copies specific files/folders to HV s3.'
                                     'It includes only key files HV needs for services.')

    parser.add_argument('-s', '--src-path',
                        help='REQUIRED: full pathed mapped docker path to the data source',
                        required=True,                        
                        )

    parser.add_argument('-types', '--deploy-types',
                        help='REQUIRED: Type of deployment. For allowed values, see code notes',
                        required=True,
                        type=str,
                        default=[],
                        )

    parser.add_argument('-p', '--params-file',
                        help='OPTIONAL: Path to params(config) file. Defaults to /data/config/deploy_params_dev.env',
                        default="/data/config/deploy_params_dev.env",
                        )
    
    parser.add_argument('-ap', '--aws-profile-name',
                        help='OPTIONAL: If you use an explicit aws credentials file, put that profile name here.',
                        default="",
                        )    

    args = parser.parse_args()

    deploy_to_hydrovis(**vars(args))

