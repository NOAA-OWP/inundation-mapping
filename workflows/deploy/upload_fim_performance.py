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


# WIP FILE


##########################
# IMPORTANT
# While this tool makes the most sense for helping get the right files to HV,
# it might seem like overkill for the FIM S3 uploads as it is primarily everything
# in the versioned src folder. However, this can be used in long chain workflows
# where multiple scripts to load the data, then upload it to the correct FIM places.
##########################

'''
This script can be used on one of two ways:
  - Deploy EFS to FIM S3
  - This tool has no need to copy any ESIP files as they are non applicable to ESIP

It can be used against EFS only to S3 buckets at this time.

It can only do either FIM S3, HV S3 or both at once. FIM and HV an easily be on different
scheduling.

This can cover fim performance catchments, points and polys if available. If some of the
files already exist, they will be overwritten.

S3 bucket names and basic pathing for both will be available via a config file in
/data/config/deploy_params.env along with any s3 credential info that may be required.
The pathing to the config file is an optional argument for testing if required.

Basic pathing EFS, FIM S3 and HV deployment bucket pathing are hardcoded. Some pathing
is more dynamic usually related to versioning. Those will be command line arguments here
as applicable. Sometimes partial pathing may be embedded here to build up the chain
for the full correct path.

For source EFS (referential to docker)  = {0}/hand_{1}/  (files and/or folders)
       {0} -> [optional arg - src fim performance root path (including /data if applicable) ]
           -> [hardcoded {/hand_} ]
       {1} -> [arg - HAND version]
    ie: Becomes: /data/fim_performance/hand_4_8_10_0/ (files and/or folders)

For target FIM S3 = {0}/{1}/hand_{2}/ (files and/or folders)
       {0} -> [config file arg - trg fims3 bucket and root path (including s3://) ]
       {1} -> [optional arg - fim_performance_root_path ]
           -> [hardcoded {/hand_} ]
       {2} -> [arg - HAND version]
           -> [hardcoded files and subfolders]
   ie: Becomes: s3://{our bucket}/foss_fim/fim_performance/hand_4_8_10_0/ (files and/or folders)

For target HV Deployment = {0}/{1}/hand_{2}/qa_datasets/  (files and/or folders)
       {0} -> [config file arg - bucket and root path (including s3://) ]
       {1} -> [arg - FIM version]
           -> [hardcoded {/hand_} ]
       {2} -> [arg - HAND version]
           -> [hardcoded files and subfolders]
   ie: Becomes: s3://{hv deployment bucket}/fim/v6_0/hand_4_8_7_2/qa_dataset/ (files and/or folders)

'''

# ============================
# GLOBAL Vars including files / folders to be copied

S3_CLIENT = None  # boto3 client (works for both buckets)

# Neither of these include their bucket names
TRG_FIM_BUCKET_NAME = ""
TRG_FIM_S3_PATH = ""  # including full pathing including. ie: /foss_fim_performance/hand_4_8_7_2/

SRC_FOLDER_PATH = ""  # likely something like /data/fim_performance/hand_4_8_7_2/


# ============================
def upload_fim_performance(deploy_type, hand_version, params_file, fim_version, src_fim_perf_root_path):

    # --------------
    # Validation. We are validating all variables in case the call came in from another py file
    # Only one variables is updated.
    hand_version = __validate_input(deploy_type, hand_version, params_file, fim_version, src_fim_perf_root_path)

    # May throw exceptions or shut the program down.
    __setup_aws(deploy_type, hand_version, fim_version)
    
    

    # TODO: Setup logging
    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    # sf.setup_file_logger(TRG_ROOT_PATH, "get_sample_data")
    # logging.info(f"Start time: {overall_start_time.strftime('%m/%d/%Y %H:%M:%S')}")

    print("********")
#    logging.info(f"Copying files/folders from {SRC_FIM_FILE_PATHS} to {output_root_folder}")    

    if deploy_type == 'fim' or deploy_type == 'all':

        full_s3_fim_path = f"s3://{TRG_FIM_BUCKET_NAME}{TRG_FIM_S3_PATH}"
        # logging.info(f"Copying files/folders from {SRC_FIM_FILE_PATHS} to {full_s3_fim_path}")    
        __upload_fim_data()

    if deploy_type == 'hv' or deploy_type == 'all':

        full_s3_hv_path = f"s3://{TRG_HV_BUCKET_NAME}{TRG_HV_S3_PATH}"
        # logging.info(f"Copying files/folders from {SRC_FIM_FILE_PATHS} to {full_s3_hv_path}")            
        __upload_hydrovis_data()
    
    # logging.info("==========================================================")
    end_time = datetime.now(timezone.utc)
    # logging.info("-- Completed getting sample data")
    # logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    # logging.info(fh.print_date_time_duration(overall_start_time, end_time, False))


# ============================
def __upload_fim_data():

    # We assume all EFS paths go up to the matching FIM S3 path. Can use wildcards.
    files_to_upload = [
        'fim_performance_catchments*.*',
        'fim_performance_points*.*',
        'fim_performance_polys*.*',
    ]

    folders_to_upload = [
        'logs',
        'ble',
        'ifc',
        'nws',
        'ras2fim',
        'usgs',
    ]

    all_files_to_upload = []

    # this would not work.. needs more pathing
    for file_name in files_to_upload:
        cur_dir_files = glob.glob(file_name)
        all_files_to_upload = all_files_to_upload + cur_dir_files

    # recursive
    for folder_name in folders_to_upload:
        start_path = SRC_FOLDER_PATH + folder_name
        for root, ___, files in os.walk(start_path):
            for file in files:
                all_files_to_upload.append(os.path.join(root, file))

    for file in all_files_to_upload:

        # src ie: /data/fim_performance/hand_4_8_7_2/ble/ble_comp_analyzed_data.csv
        # trg ie: /foss_fim/fim_performance/hand_4_8_7_2/ble/ble_comp_analyzed_data.csv

        trg_file = file.replace(SRC_FOLDER_PATH, TRG_FIM_S3_PATH)
        # boto3 only allows one file at a time.
        s3_sf.upload_file(S3_CLIENT, TRG_HV_BUCKET_NAME, TRG_FIM_S3_PATH, trg_file)
    

# ============================
def __upload_hydrovis_data():

    files_to_upload = [
        'fim_performance_catchments.csv',
        'fim_performance_points.csv',
        'fim_performance_polys.csv',
    ]

    for file_name in files_to_upload:
        # Assumes files are at the root of the src_fim_folder_path
        src_path = os.path.join(SRC_FOLDER_PATH, file_name)
        # ie: /data/fim_performance/hand_4_8_7_2/fim_performance_catchments.csv

        trg_path = f"{TRG_HV_S3_PATH}{file_name}"
        # ie: /fim/v6_0/hand_4_8_7_2/qa_dataset/fim_performance_catchments.csv

        if os.path.isfile(src_path):
            s3_sf.upload_file(S3_CLIENT, TRG_HV_BUCKET_NAME, src_path, trg_path)
        # else:
        #     logging.warning(f"Skipping: {src_path} does not exist")

# ============================
def __validate_input(deploy_type, hand_version, params_file, fim_version, src_fim_perf_root_path):

    '''
    Will return updates to variables or new variables extrapolated.
    We are checking values more carefully for empty values as we can not assume this script
    was run from command line, but possible as part of other scripts.

    This also sets up a bunch of key variables and paths
    '''

    if deploy_type not in ['fim', 'hv', 'all']:
        raise ValueError("deploy type variable must be values of fim, hv, or all (case sensitive)")
    
    if not hand_version:
        raise ValueError("hand version variable is None or empty")
    
    hand_version = hand_version.lower()
    if not 'hand_' in hand_version:
        hand_version = f"hand_{hand_version}"

    if not params_file:
        raise ValueError("params file variable is None or empty")
    
    if not os.path.exists(params_file):
        raise ValueError(f"params file of {params_file} can not be found. Check path and/or case.")
        
    load_dotenv(params_file)

    if not src_fim_perf_root_path:
        raise ValueError("source fim performance path variable is None or empty")
    
    if not os.path.exists(src_fim_perf_root_path):
        raise ValueError(f"source fim performance path  of {src_fim_perf_root_path} can not be found."
                         " Check path and/or case.")
    
    # Add starting and ending slashes if not already there
    if not src_fim_perf_root_path.startswith("/"):
        src_fim_perf_root_path = "/" + src_fim_perf_root_path

    if not src_fim_perf_root_path.endwith("/"):
        src_fim_perf_root_path += "/"
    
    globals()['SRC_FIM_FILE_PATHS'] = src_fim_perf_root_path

    if deploy_type in ['hv', 'all']:
        if not fim_version:
            raise ValueError("Fim version variable is None or empty when copying to HydroVIS")

    return hand_version


# ============================
def __setup_aws(deploy_type, hand_version, fim_version):

    # #############################
    # 
    # This can be largely copied/pasted into other deploy py files
    # but this does include a few fim_performance specific paths.
    # 
    # #############################

    # Set up boto3 client and paths.

    # Note: The hand_version and the fim_version are the two most common
    # s3 buckets and paths we use. You can optionally pass in one or both

    # This will pull from the config/deploy_params.env

    # It is possible that the user might not use explicit keys, but implicit keys
    # such as the default credentials file. So do not test for keys
    # All errors are thrown as Exceptions
    is_success, return_msg, s3_client = s3_sf.create_boto3_s3_client(
        os.getenv('AWS_ACCESS_KEY_ID'),
        os.getenv('AWS_SECRET_ACCESS_KEY'),
        os.getenv('AWS_REGION'),
    )
    if not is_success:
        raise Exception(return_msg)    

    globals()['S3_CLIENT'] = s3_client

    if deploy_type == 'fim' or deploy_type == 'all':

        # validate the bucket
        # ensure the FIM bucket exists
        is_success, return_msg = s3_sf.does_s3_bucket_exist(s3_client, os.getenv('FIM_S3_BUCKET_NAME'))
        if not is_success:
            # logging.error("FIM bucket name of {FIM_S3_BUCKET_NAME}. Check the config file and case.")
            # logging.error(return_msg)
            print("Program aborted")
            sys.exit(1)

        globals()['TRG_FIM_BUCKET_NAME'] = os.getenv('FIM_S3_ROOT_PATH')
        globals()['TRG_FIM_S3_PATH'] = f"{TRG_FIM_BUCKET_NAME}fim_performance/{hand_version}"
        # becomes something like "/foss_fim/fim_performance/hand_4_8_7_2"        

    elif deploy_type == 'hv' or deploy_type == 'all':

        # validate the bucket and root path
        # for HV, we do need to validate most of the path
        s3_prefix_folder_path = dsf.validate_hv_root_path(s3_client, hand_version, fim_version)

        # for HV deploys, the files go only in the qa_dataset folder.
        s3_prefix_folder_path += "qa_dataset/"
        globals()['TRG_HV_BUCKET_NAME'] = s3_prefix_folder_path
        globals()['TRG_HV_S3_PATH'] = s3_prefix_folder_path


# ============================
if __name__ == '__main__':

    '''
    Sample Usage (if going to HV):
    python /foss_fim/workflows/deploy/deploy_fim_perf_catchments.py -type hv -hver 4_8_7_2 \
        -fver v6_0 
    '''

    parser = argparse.ArgumentParser(description='Copies correct FIM Performance catchment'
                                     ' files/folders to FIM s3 and or HV s3')
    parser.add_argument('-type', '--deploy-type',
                        help='REQUIRED: Type of deployment. Values allowed are: fim, hv or all',
                        required=True,
                        )
    parser.add_argument('-hver', '--hand-version',
                        help='REQUIRED: HAND version with underscores, but not the word hand.\n'
                        'ie. 4_8_7_2',
                        required=True,
                        )
    parser.add_argument('-p', '--params-file',
                        help='OPTIONAL: Path to params(config) file. Defaults to /data/config/deploy_params.env',
                        default="",
                        )
    parser.add_argument('-fver', '--fim-version',
                        help='OPTIONAL: But only if the type is fim, otherwise it is required.\n'
                        'This is the public release version name. ie: v6_0 \n\n'
                        'This is part of a path for the HV deployment bucket. Ensure that path'
                        ' already exists in S3.',
                        default="",
                        )
    parser.add_argument('-r', '--src-fim-perf-root-path',
                        help='OPTIONAL: Root EFS path to fim performance directory, not the versioned'
                        ' folder name. Defaults to: /data/fim_performance/',
                        default="/data/fim_performance/"
                        )

    args = parser.parse_args()

    upload_fim_performance(**vars(args))

