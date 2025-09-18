#!/usr/bin/env python3

import os
import sys

import data.aws.s3_shared_functions as s3_sf


def validate_hv_root_path(s3_client, hand_version, fim_version):

    # NOTE: This does assume that the config file has already been loaded via load_dotenv
    # and now those values are global variables.

    # Config values used are:
    #    HV_S3_BUCKET_NAME
    #    HV_S3_ROOT_PATH

    '''
    Returns the full new hv deployment path with the latest release patterns.
    If the path does not exist, it will return an empty string.

    This can also throw exceptions from any of our aws shared function scripts.

    '''

    # Current full url pattern for v6_0 is:
    # s3://{bucket name}/fim/v6_0/hand_4_8_7_2
    s3_hv_prefix_folder_path = f"/fim/{fim_version}/hand_{hand_version}/"

    # validate the bucket and root path
    # ensure the FIM bucket exists if applicable
    is_success, return_msg = s3_sf.does_s3_bucket_exist(s3_client, os.getenv('HV_S3_BUCKET_NAME'))
    if not is_success:
        print(return_msg)
        print("Program aborted")
        sys.exit(1)

    # In this case, due to the nature of it being super important on pathing, we need to make sure
    # the folder aleady exists.
    does_hv_folder_exist = s3_sf.does_s3_folder_exist(s3_client,
                                                      os.getenv('HV_S3_BUCKET_NAME'),
                                                      s3_hv_prefix_folder_path)
    if not does_hv_folder_exist:
        msg = "Error: The current HV deployment full path of"
        f" s3://{os.getenv('HV_S3_BUCKET_NAME')}{s3_hv_prefix_folder_path}"
        " does not exist. Please talk to devops / hv team."
        print(return_msg)
        print("Program aborted")
        sys.exit(1)

    return s3_hv_prefix_folder_path

