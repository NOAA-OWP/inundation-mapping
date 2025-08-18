#!/usr/bin/env python3

import datetime as dt
import fnmatch
import logging
import os
import sys
import traceback
from concurrent import futures
from functools import partial

import boto3
import botocore.exceptions
import tqdm
from botocore.client import ClientError

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
#import shared_variables as sv


# Note: you may already know this but S3 does not have a concept of "folders", but
# uses a similar system using "prefixes".  When the word "folder" is used here, it 
# is translated to a pattern of "prefixes" that S3 can use.

# Jun 2025: This was created from a file in the ras2fim repo / also named s3_shared_functions.
# It has a wide number of additional tools such as:
#   - deleting S3 folders
#   - uploading individual files
#   - upload folders
#   - moving folder in S3
#   - downloading folders from a list
#   - downloading files from a list
#   - getting a list of files in an S3 folder
#   - getting a list of folder in an s3 folder
#   - getting a folder size
# While msot of it is ras2fim specific, it is easily adjusted to be more generic for FIM to use
# if/as needed down the road.

# -------------------------------------------------
def is_valid_s3_folder(s3_client, bucket_name, s3_prefix):
    """
    Process:
    Input:
        - s3_prefix: eg. inputs/fema  (from s3://some_bucket/inputs/fema)
    Returns two strings:
        (success) True / False,
        (err msg if any) -- (some message, but only in error)
                options coming back are:
                - "bad credentials"
                - "no such bucket"
                - "folder not found"
        But can throw an error when catastrophic
        Reason for sending back "key error strings", is so that the caller and print, log
        etc.
    """
    is_success = False
    return_msg = ""

    try:
        # If the bucket is incorrect, it will throw an exception that already makes sense
        # Don't need pagination as MaxKeys = 2 as prefix will likely won't trigger more than 1000 rec
        s3_objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix, MaxKeys=2, Delimiter="/")

        # print(s3_objs)
        if s3_objs["KeyCount"] > 0:
            return_msg = "folder not found"
        else:
            is_success = True

    except botocore.exceptions.NoCredentialsError:
        return_msg = "bad credentials"

    except s3_client.exceptions.NoSuchBucket:
        return_msg = "no such bucket"

    # other exceptions can be passed through
    return is_success, return_msg


# # -------------------------------------------------
# def is_valid_s3_file(s3_full_file_path):
#     """
#     Process:  This will throw exceptions for all errors
#     Input:
#         - s3_full_file_path: eg. s3://some_bucket/inputs/my_file.csv
#     Output:
#         True/False (exists)
#     """

#     file_exists = False

#     s3_full_file_path = s3_full_file_path.replace("\\", "/")

#     if s3_full_file_path.endswith("/"):
#         raise Exception("s3 file path is invalid as it ends with as forward slash")

#     s3_full_file_path = s3_full_file_path.replace("S3://", "s3://")

#     logging.info(f"Validating s3 file of {s3_full_file_path}")

#     bucket_name, s3_file_path = parse_bucket_and_folder_name(s3_full_file_path)

#     try:
#         if does_s3_bucket_exist(bucket_name) is False:
#             raise ValueError(f"s3 bucket of {bucket_name} does not appear to exist")

#         client = boto3.client("s3")

#         result = client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)

#         if 'Contents' in result:
#             file_exists = True

#     except botocore.exceptions.NoCredentialsError:
#         logging.critical("** Credentials not available. Try aws configure")
#     except Exception:
#         logging.critical("An error has occurred with talking with S3")
#         logging.critical(traceback.format_exc())

#     return file_exists


# -------------------------------------------------
def does_s3_bucket_exist(s3_client, bucket_name):

    """
    Process:
        - The calling function and decide if to stop, abort, etc
        - catastropic errors will be thrown, this catches bad credentials too

    Returns two string of:
      (success) True / False,
      (err msg if any) -- (some message, but only in error)
            options coming back are:
             - "bad credentials"
             - "no such bucket"
        Reason for sending back "key error strings", is so that the caller and print, log
        etc.
    """

    is_success = False
    return_msg = ""

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        # resp = client.head_bucket(Bucket=bucket_name)
        # print(resp)
        is_success = True  # no exception?  means it exist

    except botocore.exceptions.NoCredentialsError:
        return_msg = "bad credentials"

    except s3_client.exceptions.NoSuchBucket:
        return_msg = "no such bucket"

    # other exceptions can be passed through
    return is_success, return_msg


# -------------------------------------------------
def parse_bucket_and_folder_name(s3_full_folder_path):

    """
    Process:
    Input:
        - s3_full_folder_path: eg. s3://some_bucket/hand_data/inputs/fema
    Returns:
        bucket name, s3_folder_path  (ie, 'some_bucket', 'hand_data/inputs/fema')
    """

    if s3_full_folder_path.endswith("/"):
        s3_full_folder_path = s3_full_folder_path[:-1]
    s3_full_folder_path = s3_full_folder_path.replace("S3://", "s3://")

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_full_folder_path.replace("s3://", "")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]

    s3_folder_path = adj_s3_path.replace(bucket_name, "", 1)
    s3_folder_path = s3_folder_path.lstrip("/")
    s3_folder_path = s3_folder_path.rstrip("/")

    return bucket_name, s3_folder_path


# -------------------------------------------------
# def download_s3_file(s3_client, bucket_name, s3_file_key, target_file_path):
#     """
#     Process:
#         - Note: The boto3.client must be already instantated and passed in
#         - The s3_file_key is the bucket relative file path to the file name
#         - File may or may not exist in S3
#     Inputs:
#         - s3_client: my_client = boto3.client(profile, creds, whatever)
#         - bucket_name: ie) hand_data_bucket
#         - s3_file_key: bucket relative file path to the file name (ie. /inputs/fema/12090301.gpkg) 
#           (as in s3://hand_data_bucket/inputs/fema/12090301.gpkg)
#         - target_file_path: e.g . /data/inputs/fema/12090301.gpkg
#     Returns:
#         - True if file exists and was downloaded, False if not
#     """

#     does_file_exist = False
#     try:
#         # Does not return anythign but will throw and exception if it does not exist
#         s3_client.download_file(bucket_name, s3_file_key, target_file_path)
#         does_file_exist = True
#     except ClientError as e:
#         does_file_exist = False
    
#     return does_file_exist


# -------------------------------------------------
# def download_folders(list_folders, is_verbose=True):
#     """
#     Process:
#         - The s3 pathing values needs to be case-sensitive.
#         - This method is multi-threaded (not multi-proc) for performance.
#         - If the local_folders_already exist, it will not pre-clean the folders so it is
#           encouraged to pre-delete the child folders if required.
#         - all recursive files/folders will be included

#     Inputs:
#         - list_folders. List of dictionary objects
#             - schema is:
#                 - "s3_src_folder": e.g. s3://{somebucket}/inputs/test_cases/xyz
#                 - "target_local_folder": e.g. /data/inputs/test_cases/xyz
#                     all downloaded files and folders will be under this folder.
#         - is_verbose: Each folder being downloaded will be logged (console and/or file) automatically
#               If this flag is true, it will do the same for files under the folder.
#     Output
#         - A list of dictionary objects will have three keys.
#             - "s3_src_folder": the input 
#             - "download_success" as either
#                 the string value of 'True' or 'False'
#             - "error_details" - why did it fail
#           encouraged to pre-delete the child folders if required.

#         We leave it to the calling function to decide if it an error or not

#         Catastrophic errors wil be thrown as applicable.
#     """
#     rtn_threads = []
#     rtn_download_details = []

#     try:
#         max_num_threads = 20
#         num_list_folders = len(list_folders)
#         if num_list_folders == 0:
#             raise Exception("No folders were identified for downloaded")

#         # MT not used at this parent level, given to child download_single_folder
#         if num_list_folders < max_num_threads:
#             fn_partial_download_single_folder = partial(
#                 download_single_folder, num_of_workers=max_num_threads, is_verbose=True
#             )

#             num_completed = 0
#             for download_args in list_folders:
#                 download_args["s3_src_folder"] = download_args["s3_src_folder"].replace("\\", "/")

#                 item = {
#                     "bucket_name": download_args["bucket_name"],
#                     "folder_id": download_args["folder_id"],
#                     "s3_src_folder": download_args["s3_src_folder"],
#                     "target_local_folder": download_args["target_local_folder"],
#                 }
#                 rtn_threads.append(fn_partial_download_single_folder(**item))
#                 num_completed += 1
#                 RLOG.lprint(f"--- {num_completed} of {num_list_folders} folders completed")

#         else:  # we use MT here and NOT in the child download_single_folder
#             num_workers = num_list_folders
#             if num_workers > max_num_threads:
#                 num_workers = max_num_threads

#             # only 1 worker for each child single folder
#             fn_partial_download_single_folder = partial(
#                 download_single_folder, num_of_workers=1, is_verbose=False
#             )

#             with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
#                 futures_dict = []

#                 for download_args in list_folders:
#                     item = {
#                         "bucket_name": download_args["bucket_name"],
#                         "folder_id": download_args["folder_id"],
#                         "s3_src_folder": download_args["s3_src_folder"],
#                         "target_local_folder": download_args["target_local_folder"],
#                     }
#                     futures_dict.append(executor.submit(fn_partial_download_single_folder, **item))

#                 for future_result in futures.as_completed(futures_dict):
#                     if future_result is not None:
#                         future_exception = future_result.exception()
#                         if future_exception:
#                             RLOG.error(future_exception)
#                         else:
#                             result = future_result.result()
#                             rtn_threads.append(result)

#         for result in rtn_threads:
#             # err_msg might be empty
#             item = {
#                 "folder_id": result['folder_id'],
#                 "download_success": result['is_success'],
#                 "error_details": result['err_msg'],
#             }

#             rtn_download_details.append(item)

#         return rtn_download_details

#     except botocore.exceptions.NoCredentialsError:
#         print("-----------------")
#         RLOG.critical(
#             "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
#             "permissions options"
#         )
#         sys.exit(1)

#     except Exception as ex:
#         print("-----------------")
#         RLOG.critical("** Error downloading folders from S3:")
#         RLOG.critical(traceback.format_exc())
#         raise ex

# -------------------------------------------------
def download_s3_folder(s3_client, bucket_name, s3_src_path, target_local_folder):
    """
    Process:
        - Note: The boto3.client must be already instantated and passed in
        - Using the incoming s3 src folder, call get_records to get a list of child folders and files
        - Open a s3 client and iterate through the files to download
        - Is recursive
    Inputs:
        - s3_client: my_client = boto3.client(profile, creds, whatever)
        - s3_src_path: e.g. /inputs/fema (from s3://{some_bucket}/inputs/fema)
        - target_local_folder: e.g . /data/inputs/fema

    Returns:
        - True or False (did a least one file download successfully)
        
        The calling code and decide what to do with it.
        Note: Exceptions can still be thrown for catastropic errors (creds, other)
    """

    # This also validates that the bucket exists
    # bucket_name, s3_prefix_path = parse_bucket_and_folder_name(s3_client, s3_src_path)

    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name,
                            'Prefix': s3_src_path,
                            'Delimiter': '/'}
    page_iterator = paginator.paginate(**operation_parameters)

    files_exist = False
    for page in page_iterator:
        if 'Contents' in page:
            # Iterate over each object and download it
            for obj in page['Contents']:
                s3_key = obj['Key']
                if s3_key[-1] != "/": # if it was a folder (ending in a slash, we skip it)
                    rel_path = os.path.relpath(s3_key, s3_src_path)
                    local_file_path = os.path.join(target_local_folder, rel_path)

                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    s3_client.download_file(bucket_name, s3_key, local_file_path)
                    files_exist = True

    return files_exist
