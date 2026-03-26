#!/usr/bin/env python3

import fnmatch
import logging
import os
import random
import threading
import time
from concurrent import futures
from functools import partial

import botocore
import botocore.exceptions
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

import data.aws.aws_shared_functions as awssf


'''
Note: you may already know this but S3 does not have a concept of "folders", but
uses a similar system using "prefixes".  When the word "folder" is used here, it
is translated to a pattern of "prefixes" that S3 can use.

'''

# Note: Sep 2025: Why all of this architecture usign boto3 instead of simpler subprocess
# and cli?  We want more control over errors ane exceptions, plus this system can do more
# complex things that cli can do such as wildcard, more selective recursion, etc.
# This AWS system will also be upgraded soon handle much more than just s3, but things like
# step functions, task definitions, execution scripts, etc.

# Also examined other performance packges such as bulkboto3, aioboto3, s3fs/fsspec,
# and robinzhon code and found it was no faster. They ultimatly are all built as wrappers
# against boto3 and botocore, but I want more options.
# We max out he network performance no matter what based on the job arg


# -------------------------------------------------
def parse_bucket_and_folder_name(s3_full_folder_path):
    """
    Process:
    Input:
        - s3_full_folder_path: eg. s3://some_bucket/hand_data/inputs/fema
    Returns:
        bucket name, s3_folder_path  (ie, 'some_bucket', 'hand_data/inputs/fema')
    """

    s3_full_folder_path = s3_full_folder_path.replace("S3://", "s3://")

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_full_folder_path.replace("s3://", "")
    s3_full_folder_path = s3_full_folder_path.strip("/")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]

    s3_folder_path = adj_s3_path.replace(bucket_name, "", 1)

    return bucket_name, s3_folder_path


# -------------------------------------------------
def does_s3_bucket_exist(s3_client, bucket_name):
    """
    Process:
        - Helps validate the client as well
        - The calling function and decide if to stop, abort, etc
        - catastropic errors will be thrown, this catches bad credentials too

    Return:
      - (success) True / False (bucket exists)
      - msg: Can be any message but likely is an error msg, which the calling code
        can optional decide to use or not.
      Can also throw exceptions.
    """

    if s3_client is None:
        raise Exception("S3 Client not instantiated")

    # strip start and end slashs if exist
    bucket_name = bucket_name.strip("")
    is_success = False
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        # resp = client.head_bucket(Bucket=bucket_name)
        # print(resp)
        is_success = True  # no exception?  means it exist
        return_msg = ""
    except s3_client.exceptions.NoSuchBucket:
        return_msg = f"S3 Bucket '{bucket_name}' does not exist."
        is_success = False
    except botocore.exceptions.ClientError as ex:
        error_code = int(ex.response['Error']['Code'])
        if error_code == 404:
            return_msg = f"Bucket '{bucket_name}' does not exist."
            is_success = False
        elif error_code == 403:
            return_msg = f"Access denied to bucket '{bucket_name}'. Check permissions."
            is_success = False
        else:  # forward it to aws exceptions to see if it recognizes the exception
            return_msg, type_known = awssf.aws_exception_handler(ex)
            if type_known is False:
                raise Exception(return_msg)
            is_success = False

    except Exception as ex:
        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)

    return is_success, return_msg


# -------------------------------------------------
def does_s3_folder_exist(s3_client, bucket_name, s3_prefix_folder_path):
    """
    Process:
    Input:
        - s3_prefix_folder_path: eg. inputs/fema  (from s3://some_bucket/inputs/fema)
    Returns
        (success) True / False,
      Can also throw exceptions.
    """
    # re-validate the connection and credentials as well
    is_success, return_msg = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        # In this case, we want to raise a new Exception
        raise Exception(return_msg)

    # strip starting and ending slashes
    # s3_prefix_folder_path = s3_prefix_folder_path.strip("/")

    # must have a slash on the end only
    # strip leading slash if exists
    s3_prefix_folder_path = s3_prefix_folder_path.lstrip("/")
    if not s3_prefix_folder_path.endswith("/"):
        s3_prefix_folder_path += "/"

    # print(f"just before list_obj is {s3_prefix_folder_path}")

    # s3://hydrovis-ti-deployment-us-east-1/fim/ripple/20260211_merged/collections/mip_05130101/library_extent/10188596/
    #                                       fim/ripple/2026011_merged/collections/mip_05130101/library_extent/10188304/

    # If the bucket is incorrect, it will throw an exception that already makes sense
    # Don't need pagination as MaxKeys = 2 as prefix will likely won't trigger more than 1000 rec
    s3_objs = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=s3_prefix_folder_path, MaxKeys=2, Delimiter='/'
    )

    if s3_objs["KeyCount"] == 0:
        # print(f"{s3_prefix_folder_path} was not found")
        is_success = False
    else:
        # print(f"{s3_prefix_folder_path} was found")
        is_success = True

    # other exceptions can be passed through
    return is_success


# -------------------------------------------------
def does_s3_file_exist(s3_client, bucket_name, s3_file_path):
    """

    Note: This function might have limited use as if you actualy attempt to download
       it will tell you as a True/False if the file existed.

    Process:  This will throw exceptions for all errors
    Input:
        - s3_file_path: eg. /inputs/my_file.csv
    Output:
    Returns
        (success) True / False,
        But can throw an error when catastrophic
    """

    # TODO: add aws_exception handling

    file_exists = False

    s3_full_file_path = s3_file_path.replace("\\", "/")
    # strip starting slash
    s3_full_file_path.lstrip("/")

    bucket_name, s3_file_path = parse_bucket_and_folder_name(s3_full_file_path)

    # re-validate the connection and credentials as well
    is_success, return_msg = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        # In this case, we want to raise a new Exception
        raise Exception(return_msg)

    result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)

    if 'Contents' in result:
        file_exists = True

    return file_exists, 0


# -------------------------------------------------
def get_file_list_by_key(s3_client, bucket_name, s3_parent_src_folder_path, search_key=""):
    """
    Process:
        - uses a S3 paginator to recursively look for matches (non case-sensitive)
        - You can optionally use a search key to filter records
    Inputs:
        - bucket_name: e.g mys3bucket_name
        - s3_parent_src_folder_path: e.g. foss_fim/previous_fim/hand_4_8_7_2 (case-sensitive).
          All applicable files will be found under this folder.
        - search_key: OPTIONAL: phrase (str) to be searched: e.g */wbd.gpkg
          search key can have more than one astericks char as a wildcard, but only astericks work
          which means 0 to many chars.

    Output
        - A list of files found matching the pattern if applicable.
    """

    # Examples:
    # search_key = "*/hydrotable.csv"  - looks at the first level only (ie. huc level)
    # search_key = "*/branches/*/rem_zeroed_masked_*.tif"  - looks for all branch level rems
    # search_key = "fim_inputs.csv"  - file at the root src_s3_folder_path (ie. hand_4_8_7_2)
    # if you want all files under the s3_parent_src_folder_pathr, just submit ""

    # TODO: Oct 30, 2025: found a bug to fix later.
    # the pattern of */ is recursive.. hummmm..
    # ie "*/usgs_elev_table.csv" which was expected to find it at the huc level
    # also found the branch version as well.  hummm...

    try:
        # both src must have a slash on the end only
        # strip leading slash if exists
        s3_parent_src_folder_path = s3_parent_src_folder_path.lstrip("/")
        if not s3_parent_src_folder_path.endswith("/"):
            s3_parent_src_folder_path += "/"

        s3_items = []  # a list s3 keys (files paths without the bucket name)

        operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_parent_src_folder_path}
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(**operation_parameters)

        for page in pages:
            if 'Contents' in page:
                # Iterate over each object and download it
                for obj in page['Contents']:
                    s3_key = obj['Key']
                    key_adj = s3_key.replace(s3_parent_src_folder_path, "")
                    if search_key == "" or fnmatch.fnmatch(key_adj, search_key) is True:
                        s3_items.append(s3_key)
                    # no need for an eslse

        return s3_items

    except Exception as ex:
        # Check if our aws_exception_handler knows what it is.
        # If it finds it, it returns a nice user friendly message.
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)


# -------------------------------------------------
def get_file_list(s3_client, bucket_name, s3_parent_src_folder_path, list_of_search_key=[""], num_workers=10):
    """
    Process:
        - Using a list of search keys, it will use multi-threading to get a single list of files
        - For examples on how to use search keys, see the examples below.
    Inputs:
        - bucket_name: e.g mys3bucket_name
        - s3_parent_src_folder_path: e.g. foss_fim/previous_fim/hand_4_8_7_2 (case-sensitive). All applicable files will be
          found under this folder.
        - list_of_search_key: A simple list with search key strings: ie: ["*/wbd.gpkg", "*/hydrotable.csv"]
          search key can have more than one astericks char as a wildcard, but only astericks work
          which means 0 to many chars.

    Output
        - A list of files found matching all combined search patterns.
    """

    # Examples:
    # search_key = "*/hydrotable.csv"  - looks at the first level only (ie. huc level)
    # search_key = "*/branches/*/rem_zeroed_masked_*.tif"  - looks for all branch level rems
    # search_key = "fim_inputs.csv"  - file at the root src_s3_folder_path (ie. hand_4_8_7_2)

    # TODO: Oct 30, 2025: found a bug to fix later.
    # the pattern of */ is recursive.. hummmm..
    # ie "*/usgs_elev_table.csv" which was expected to find it at the huc level
    # also found the branch version as well.  hummm...

    # If you want all files under the s3_parent_src_folder_path, just submit "" as an as a single list item
    #    If you do this.. no other search keys will be attempted as they will simply be duplications

    if not list_of_search_key:
        raise ValueError("Error: list of search keys can not be null or empty")

    try:

        tasks_args_list = []
        full_list_files = []

        # "partials" allow you to set some of the values that are shared by all
        # Multi-threads (or procs) so we don't have so much duplication.
        # In this case, we had to use this for sharing the s3_client.
        # The other two remaing args to upload_file are unique and are handled
        # by the for loop and list of dictionaries.
        merged_partial_get_file_by_key = partial(
            get_file_list_by_key,
            s3_client=s3_client,
            bucket_name=bucket_name,
            s3_parent_src_folder_path=s3_parent_src_folder_path,
        )

        # Shared client appears to not be threadsafe as long as I keep the job down (under 20?)
        for search_key in list_of_search_key:
            if search_key == "":
                # ignore all other keys as they are relevant
                tasks_args_list = [{"search_key": search_key}]
                break
            args_item = {"search_key": search_key}
            tasks_args_list.append(args_item)

        # files = merged_partial_get_file_by_key(**tasks_args_list[0])

        # Dispatch work tasks with our s3_client
        # Need to use a thread and not an mp here (sharing usage of the s3 client)
        # boto3 clients are thread-safe, sessions and resources are not.

        # Create an event to signal tasks to stop
        stop_event = threading.Event()
        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures_dict = [executor.submit(merged_partial_get_file_by_key, **arg) for arg in tasks_args_list]

            # adding the min intervals speeds it up so it doesn't try to repaint every tqdm loop
            for future in tqdm(
                futures.as_completed(futures_dict), total=len(tasks_args_list), desc="Retrieving file paths"
            ):
                try:
                    if future.cancelled():
                        continue
                    if future is not None:
                        future_exception = future.exception()
                        # These are exceptions from withing the function being executed
                        if future_exception:
                            # reraise it
                            raise Exception(f"zooiks: future item has an exception {future_exception}")
                        else:
                            result = future.result()
                            full_list_files.extend(result)
                    # else:
                    #     print("looks good, future item is None though, how is that possible?")
                except Exception as fex:
                    print(f"Error returned by future. Details: {fex}")
                    logging.info(f"Error returned by future. Details: {fex}")

                    stop_event.set()  # Signal running tasks to stop
                    executor.shutdown(wait=False, cancel_futures=True)

        return full_list_files

    except Exception as ex:

        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)


# -------------------------------------------------
# NOTE:  not fully tested yet as of Oct 16, 2025 - this is a temp unfinished placeholder
def get_folder_list(s3_client, bucket_name, s3_src_folder_path):
    """
    Process:
        - Uses a S3 paginator to get a list of folders in the child dir below the
          s3_src_folder_path
          Only folder names with some file or folders under it will be found (an aws
          thing as it is not really folders but prefix keys).
          List will be not be a recursive search.
    Inputs:
        - bucket_name: e.g mys3bucket_name
        - s3_src_folder_path: e.g. test_cases/ble/12090301 (case-sensitive)
    Output
        - a list of the full path (if s3, without the bucket name)
            test_cases/ble/12090301/some_folder
    """

    # TODO; add aws exception handling

    # TODO: Add flag to allow for optional recursive and possibly wildcards
    # see wildcard feature in the ras2fim s3 code.

    s3_src_folder_path = s3_src_folder_path.replace("\\", "/")

    # strip leading slash if exists
    s3_src_folder_path = s3_src_folder_path.lstrip("/")

    # re-validate the connection and credentials as well
    is_success, return_msg = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        # In this case, we want to raise a new Exception
        raise Exception(return_msg)

    s3_items = []

    default_kwargs = {"Bucket": bucket_name, "Prefix": s3_src_folder_path, "Delimiter": "/"}

    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token

        # will limit to 1000 objects - hence tokens
        response = s3_client.list_objects_v2(**updated_kwargs)
        if response.get("KeyCount") == 0:
            next_token = response.get("NextContinuationToken")
            continue

        prefix_recs = response.get("CommonPrefixes")
        if prefix_recs is None:
            next_token = response.get("NextContinuationToken")
            continue

        # TODO: ensure this is not recusive
        for result in prefix_recs:
            prefix = result.get("Prefix")
            prefix_adj = prefix.replace(s3_src_folder_path, "")
            if prefix_adj.endswith("/"):
                prefix_adj = prefix_adj[:-1]
            if prefix_adj != "":  # empty.. likely the parent folder itself.

                item = f"{s3_src_folder_path}{prefix_adj}"
                s3_items.append(item)

        next_token = response.get("NextContinuationToken")

    return s3_items  # could be empty if none found


# -------------------------------------------------
def download_s3_file(s3_client, bucket_name, s3_file_key, target_file_path, test_bucket_exists=False):
    """
    Process:
        - Note: The boto3.client must be already instantated and passed in
        - The s3_file_key is the bucket relative file path to the file name
        - File may or may not exist in S3
    Inputs:
        - s3_client: my_client = boto3.client(profile, creds, whatever)
        - bucket_name: ie) hand_data_bucket
        - s3_file_key: bucket relative file path to the file name (ie. /inputs/fema/12090301.gpkg)
          (as in s3://hand_data_bucket/inputs/fema/12090301.gpkg)
        - target_file_path: e.g . /data/inputs/fema/12090301.gpkg

    Returns:
        - True if file exists and was downloaded, False if not
    """
    # Yes.. outside the try/catch

    # re-validate the connection and credentials as well
    if test_bucket_exists is True:
        is_success, return_msg = does_s3_bucket_exist(s3_client, bucket_name)
        if not is_success:
            # In this case, we want to raise a new Exception
            raise Exception(return_msg)

    does_file_exist = False
    # strip leading slash if exists
    s3_file_key = s3_file_key.lstrip("/")

    # the target must start with a slash
    if not target_file_path.startswith("/"):
        target_file_path = "/" + target_file_path

    # The target folder must pre-exist
    trg_dir = os.path.dirname(target_file_path)
    if not os.path.exists(trg_dir):
        os.makedirs(trg_dir, mode=0o776, exist_ok=True)

    # Configure multipart  settings. Speeds up large files
    # TODO: test to play with chunking sizes
    trans_config = TransferConfig(
        max_concurrency=30,  # 10 concurrent threads is default
        use_threads=True,  # Enable threading  (is by default)
    )
    # multipart_threshold=1024 * 25,  # 25MB - start multipart for files > 25MB
    # max_concurrency=30,  # 10 concurrent threads
    # multipart_chunksize=1024 * 25,  # 25MB per part
    # use_threads=True,  # Enable threading

    try:
        s3_client.download_file(bucket_name, s3_file_key, target_file_path, Config=trans_config)
        # s3_client.download_file(bucket_name, s3_file_key, target_file_path)
        does_file_exist = True
    except Exception as ex:

        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, type_known = awssf.aws_exception_handler(ex)
        if type_known is False:
            raise Exception(return_msg)
        elif "404" in return_msg:  # File likely does not exist
            does_file_exist = False
        else:
            raise Exception(return_msg)

    return does_file_exist


# -------------------------------------------------
# You generally want to use this only when you have a large number of files to upload.
# Size of each file is not relavent.
# This uses multi-threading and not multi-proc
def download_files_by_file_list(s3_client, bucket_name, file_list, num_workers=1):
    '''
    file_list must be a list of dictionaries: {"src_file": file_path, "trg_file": trg_file}
    Note: src_file is the S3 path without the "s3://" and "bucket name"
    ie: {
    "src_file":
       '/foss_fim/temp/RobH/Rob_deploy_test_10/03140303/branches/2411000027/hydroTable_2411000027.csv',
    "trg_file": /outputs/Rob_deploy_test_10' :
       '/outputs/Rob_deploy_test_10/03140303/branches/2411000027/hydroTable_2411000027.csv',
    }

    '''
    # This assumes the bucket exists and the session/client are still alive and valid
    try:
        tasks_args_list = []

        # "partials" allow you to set some of the values that are shared by all
        # Multi-threads (or procs) so we don't have so much duplication.
        # In this case, we had to use this for sharing the s3_client.
        # The other two remaing args to upload_file are unique and are handled
        # by the for loop and list of dictionaries.
        merged_partial_download_file = partial(
            download_s3_file, s3_client=s3_client, bucket_name=bucket_name, test_bucket_exists=False
        )
        # Shared client appears to not be threadsafe as long as I keep the job down (under 20?)
        for file_item in file_list:

            # In theory these should never happen
            if "//" in file_item['src_file']:
                file_item['src_file'] = file_item['src_file'].replace("//", "/")
            if file_item['src_file'] == "/":
                continue

            args_item = {"s3_file_key": file_item['src_file'], "target_file_path": file_item['trg_file']}
            tasks_args_list.append(args_item)

        # Dispatch work tasks with our s3_client
        # Need to use a thread and not an mp here (sharing usage of the s3 client)
        # boto3 clients are thread-safe, sessions and resources are not.

        # Create an event to signal tasks to stop
        stop_event = threading.Event()
        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures_dict = [executor.submit(merged_partial_download_file, **arg) for arg in tasks_args_list]

            # adding the min intervals speeds it up so it doesn't try to repaint every tqdm loop
            for future in tqdm(
                futures.as_completed(futures_dict),
                total=len(tasks_args_list),
                desc="downloading files",
                miniters=10,
            ):
                try:
                    if future.cancelled():
                        continue
                    if future is not None:
                        future_exception = future.exception()
                        # These are exceptions from withing the function being executed
                        if future_exception:
                            # reraise it
                            raise Exception(f"zooiks: future item has an exception {future_exception}")
                        # else:  # we don't care about the result at this time.
                        #     result = future.result()
                        #     print(result)
                except Exception as fex:
                    print(f"Error returned by future. Details: {fex}")
                    logging.info(f"Error returned by future. Details: {fex}")

                    stop_event.set()  # Signal running tasks to stop
                    executor.shutdown(wait=False, cancel_futures=True)

    except Exception as ex:
        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)


# -------------------------------------------------
# You generally want to use this only when you have a large number of files to upload.
# Size of each file is not relavent.
# This uses multi-threading and not multi-proc
def download_folders(
    s3_client, bucket_name, s3_src_path, trg_folder_path, list_of_search_key=[""], num_workers=1
):
    """
    Process:

        This can be for one folder or multiple folders via search keys.

        - Note: The boto3.client must be already instantiated and passed in
        - Using the incoming s3 src folder, call get_records to get a list of child folders and files
        - Open a s3 client and iterate through the files to download
        - Is recursive
        - You can optionaly use a list of search keys (see examples below). If you do use non empty
          search keys, performance will be a bit slower.
        - If you want everything in a folder, do not submit the list_of_search_keys and it will
          give you all files under the s3_src_path. This will generally download faster.
    Inputs:
        - s3_client: my_client = boto3.client(profile, creds, whatever)
        - s3_src_path: e.g. /inputs/fema (from s3://{some_bucket}/inputs/fema)
        - trg_folder_path: e.g . /data/inputs/fema
        - list_of_search_key: simple list of strings, see search key below
        - num_of_workers if you want to use MT (multi-threading)

    Returns:
        - Number of files downloaded after searching if applicable

        The calling code and decide what to do with it.
        Note: Exceptions can still be thrown for catastropic errors (creds, other)
    """

    # Examples for each item in the list of search_keys:
    #   */hydrotable.csv  - looks at the first level only (ie. huc level)
    #   */branches/*/rem_zeroed_masked_*.tif  - looks for all branch level rems
    #   fim_inputs.csv  - file at the root src_s3_folder_path (ie. hand_4_8_7_2)
    # if list_of_search_key is None or empty, This will just download an entire directory

    # Note: untested if you want all all files under a subfolder. Might need to use download_folder, TBD

    # if list_of_search_key is None:
    #     list_of_search_key = [""]

    if not isinstance(list_of_search_key, list):
        raise TypeError("Error: argument for list_of_search_keys must be a 'list' type")

    skip_multi = False
    if list_of_search_key[0] == "" and len(list_of_search_key) == 0:
        skip_multi = True

    # This assumes the bucket exists and the session/client are still alive and valid
    total_files_downloads = 0
    try:

        # both src must have a slash on the end only
        # strip leading slash if exists
        s3_src_path = s3_src_path.lstrip("/")

        if not s3_src_path.endswith("/"):
            s3_src_path += "/"

        # target must have a slash on the end only
        # strip leading slash if exists
        trg_folder_path = trg_folder_path.lstrip("/")
        if not trg_folder_path.endswith("/"):
            trg_folder_path += "/"

        tasks_args_list = []

        # "partials" allow you to set some of the values that are shared by all
        # Multi-threads (or procs) so we don't have so much duplication.
        # In this case, we had to use this for sharing the s3_client.
        # The other two remaing args to upload_file are unique and are handled
        # by the for loop and list of dictionaries.
        merged_partial_download_file = partial(
            download_files_by_search_key,
            s3_client=s3_client,
            bucket_name=bucket_name,
            s3_src_path=s3_src_path,
            trg_folder_path=trg_folder_path,
        )
        # Shared client appears to not be threadsafe as long as I keep the job down (under 20?)
        for i, search_key in enumerate(list_of_search_key):
            if search_key == "":
                # ignore all other keys as they are relevant
                tasks_args_list = [{"search_key": search_key}]
                break
            args_item = {"search_key": search_key, "id_number": i}
            tasks_args_list.append(args_item)

        if skip_multi:
            print("-----------------")
            print(
                "*** Note: We use one progress bar per pattern. There are some small bugs and sometimes"
                " you will see a progress bar for a key show up a second time. This is fine and"
                " it is not really downloading that key set twice."
            )
            print("-----------------")

        # Dispatch work tasks with our s3_client
        # Need to use a thread and not an mp here (sharing usage of the s3 client)
        # boto3 clients are thread-safe, sessions and resources are not.
        # Create an event to signal tasks to stop
        stop_event = threading.Event()
        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # with futures.ProcessPoolExecutor(max_workers=num_workers) as executor:

            futures_dict = [executor.submit(merged_partial_download_file, **arg) for arg in tasks_args_list]

            for future in futures.as_completed(futures_dict):
                try:
                    if future is not None:
                        if future.cancelled():
                            continue
                        if not future.exception():
                            num_files_downloaded = future.result()
                            total_files_downloads += num_files_downloaded
                        else:
                            raise future.exception()  # re-raise it
                except Exception as fex:
                    print(f"Error returned by future. Details: {fex}")
                    logging.info(f"Error returned by future. Details: {fex}")

                    stop_event.set()  # Signal running tasks to stop
                    executor.shutdown(wait=False, cancel_futures=True)

    except Exception as ex:
        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)

    return total_files_downloads


def download_files_by_search_key(
    s3_client, bucket_name, s3_src_path, trg_folder_path, search_key="", id_number=0
):

    # Examples of searching
    # search_key = "" means download everything in that s3_src_path_folder
    # search_key = "*/hydrotable.csv"  - looks at the first level only (ie. huc level)
    # search_key = "*/branches/*/rem_zeroed_masked_*.tif"  - looks for all branch level rems
    # search_key = "fim_inputs.csv"  - file at the root src_s3_folder_path (ie. hand_4_8_7_2)
    # if you want all files under the s3_parent_src_folder_path, just submit ""

    # both src must have a slash on the end only
    # strip leading slash if exists
    s3_src_path = s3_src_path.lstrip("/")

    if not s3_src_path.endswith("/"):
        s3_src_path += "/"

    # target must have a slash on the end only
    # strip leading slash if exists
    trg_folder_path = trg_folder_path.lstrip("/")
    if not trg_folder_path.endswith("/"):
        trg_folder_path += "/"

    # This assumes the bucket exists and the session/client are still alive and valid
    num_files_downloaded = 0

    # A bit of start staggering to help not overload the MP (milliseconds) (total is 1 seconds)
    time.sleep(random.uniform(1, 1000) / 1000)

    try:

        # operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_src_path, 'Delimiter': '/'}
        # if you add the delimiter of '/' it will get files at that level only and not recursive
        operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_src_path}

        # We want the number of pages, so we need to get run the paginator twice.
        # One to get the page count (Yes.. goofy, but sounds like there is no other way).
        # The second to process it. If we do it both in one, the pages get messed up and some
        # pages get skipped.
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(**operation_parameters)

        number_of_pages = 0  # for some reason, I can not easily get a count for of pages
        for page in pages:
            number_of_pages += 1

        # show this message for the first one only, even if it function did not come in as MT
        if id_number == 0:
            print("s3 files are searched 1000 at a time and downloading applicable files.", flush=True)

        pages = paginator.paginate(**operation_parameters)

        # for page in pages:
        cur_page_num = 1
        with tqdm(
            position=id_number,
            total=number_of_pages,
            miniters=10,
            desc=f"key: '{search_key}' : pg {cur_page_num} of {number_of_pages} [{id_number}]",
            bar_format="{l_bar}{bar:20}{r_bar} ",
        ) as pbar:
            for page in pages:
                # need a small tqdm sleep so updating the progress bar does not collide
                # time.sleep(0.05*thread_number)
                if 'Contents' in page:

                    # Iterate over each object and download it
                    for obj in page['Contents']:
                        s3_key = obj['Key']
                        key_adj = s3_key.replace(s3_src_path, "")
                        if search_key == "" or fnmatch.fnmatch(key_adj, search_key) is True:
                            rel_path = os.path.relpath(s3_key, s3_src_path)
                            local_file_path = "/" + os.path.join(trg_folder_path, rel_path)

                            os.makedirs(os.path.dirname(local_file_path), mode=0o776, exist_ok=True)
                            s3_client.download_file(bucket_name, s3_key, local_file_path)
                            num_files_downloaded += 1
                time.sleep(0.1)  # needed for via an inner loop allows time for print queue
                pbar.update(1)

        if num_files_downloaded == 0:
            print(
                "*** Warning: Downloading file using search key of 'search_key' found zero files to download."
                " Check the search key value and/or pathing"
            )

    except Exception as ex:
        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)

    return num_files_downloaded


# -------------------------------------------------
def upload_file(
    s3_client, bucket_name, src_file_path, trg_file_path, show_progress_bar=True, test_bucket_exists=True
):
    """
    Process:
        - Note: The boto3.client must be already instantated and passed in
        - The s3_src_path is the bucket relative file path to the file name
        - If file exists in S3, it will be overwritten
    Inputs:
        - s3_client: my_client = boto3.client(profile, creds, whatever). We use s3 clients as they are
          thread safe, not all AWS object types are.
        - bucket_name: ie) hand_data_bucket
        - src_file_path:
               ie. /data/inputs/fema/12090301.gpkg
        - trg_file_path: The S3 path to the file without "s3://" and the bucket name.
               ie. /foss_fim/inputs/fema/12090301.gpkg)
        - show_progress_bar: Any files over 1 GiB can automatically show a progress bar
          if this value is set to True

    Returns:
        - True if file exists and was uploaded, False if not
        - the original src_file_path
    """
    # Yes.. outside the try/catch
    # re-validate the connection and credentials as well

    if test_bucket_exists is True:
        is_success, return_msg = does_s3_bucket_exist(s3_client, bucket_name)
        if not is_success:
            # In this case, we want to raise a new Exception
            raise Exception(return_msg)

    # add a leading slash if it is not already there.
    if not src_file_path.startswith("/"):
        src_file_path = "/" + src_file_path

    if not os.path.isfile(src_file_path):
        return False, src_file_path

    # strip leading slash if exists, we will put it back if need too
    trg_file_path = trg_file_path.lstrip("/")

    # we need to build up the full s3 path.
    # s3_trg_file_path = f"s3://{bucket_name}/{trg_file_path}"

    # debug
    # print(f"Staring upload for  {src_file_path}")

    # Configure multipart  settings. Speeds up large files
    trans_config = TransferConfig(
        max_concurrency=30,  # 10 concurrent threads is default
        use_threads=True,  # Enable threading  (is by default)
    )
    # multipart_threshold=1024 * 25,  # 25MB - start multipart for files > 25MB
    # multipart_chunksize=1024 * 25,  # 25MB per part

    try:
        # Will show progress if a large file
        is_large_file = False
        file_size_gbytes = os.path.getsize(src_file_path) / 1024 / 1024 / 1024
        if file_size_gbytes > 1 and show_progress_bar:  # GiB
            file_name = os.path.basename(src_file_path)
            print(f"---- {file_name} is a large file (over 1 GiB), showing upload progress bar for it.")

            is_large_file = True

        with open(src_file_path, "rb"):
            if is_large_file and show_progress_bar:
                s3_client.upload_file(
                    src_file_path,
                    bucket_name,
                    trg_file_path,
                    Callback=awssf.ProgressPercentage(src_file_path),
                    Config=trans_config,
                )
                # Config=s3_config,
                print("", flush=True)  # reset the console lines as it does not have an line break
            else:
                s3_client.upload_file(src_file_path, bucket_name, trg_file_path, Config=trans_config)
                # s3_client.upload_file(src_file_path, bucket_name, trg_file_path)

    except Exception as ex:

        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)

    return True, src_file_path  # file uploaded succesfully


# -------------------------------------------------
# TODO: Build an upload folder function, similar to the donwload_folders
# def upload_folder(s3_client, bucket_name, src_folder, path, s3_trg_path, num_workers=10):


# -------------------------------------------------
# You generally want to use this only when you have a large number of files to upload.
# Size of each file is not relavent.
# This uses multi-threading and not multi-proc
def upload_by_file_list(s3_client, bucket_name, file_list, num_workers=10):
    '''
    file_list must be a list of dictionaries: {"src_file": file_path, "trg_file": trg_file}
    Note: trg_file is the S3 path without the "s3://" and "bucket name"
    ie: {
    "src_file":
       '/data/inputs/fema/12090301.gpkg',
    "trg_file": '/foss_fim/inputs/' :
       '/foss_fim/inputs/fema/12090301.gpkg',
    }
    '''

    # This assumes the bucket exists and the session/client are still alive and valid
    try:
        tasks_args_list = []

        # "partials" allow you to set some of the values that are shared by all
        # Multi-threads (or procs) so we don't have so much duplication.
        # In this case, we had to use this for sharing the s3_client.
        # The other two remaing args to upload_file are unique and are handled
        # by the for loop and list of dictionaries.
        merged_partial_upload_file = partial(
            upload_file,
            s3_client=s3_client,
            bucket_name=bucket_name,
            show_progress_bar=False,
            test_bucket_exists=False,
        )
        # Shared client appears to not be threadsafe as long as I keep the job down (under 10?)
        for i, file_item in enumerate(file_list):

            # In theory these should never happen
            if "//" in file_item['src_file']:
                file_item['src_file'] = file_item['src_file'].replace("//", "/")
            if file_item['src_file'] == "/":
                continue

            # debugging test
            # if i > 20:
            #     break
            args_item = {"src_file_path": file_item['src_file'], "trg_file_path": file_item['trg_file']}
            tasks_args_list.append(args_item)

        # Dispatch work tasks with our s3_client
        # Need to use a thread and not an mp here (sharing usage of the s3 client)
        # boto3 clients are thread-safe, sessions and resources are not.

        # Create an event to signal tasks to stop
        stop_event = threading.Event()
        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures_dict = [executor.submit(merged_partial_upload_file, **arg) for arg in tasks_args_list]

            # adding the min intervals speeds it up so it doesn't try to repaint every tqdm loop
            for future in tqdm(
                futures.as_completed(futures_dict),
                total=len(tasks_args_list),
                desc="uploading files",
                miniters=10,
            ):
                try:
                    if future.cancelled():
                        continue
                    if future is not None:
                        future_exception = future.exception()
                        # These are exceptions from withing the function being executed
                        if future_exception:
                            # reraise it
                            raise Exception(f"zooiks: future item has an exception {future_exception}")
                        # else:  # we don't care about the result at this time.
                        #     result = future.result()
                        #     print(result)
                    # else:
                    #     print("looks good, future item is None though, how is that possible?")
                except Exception as fex:
                    print(f"Error returned by future. Details: {fex}")
                    logging.info(f"Error returned by future. Details: {fex}")

                    stop_event.set()  # Signal running tasks to stop
                    executor.shutdown(wait=False, cancel_futures=True)

    except Exception as ex:
        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)


# -------------------------------------------------
# NOTE:  not fully tested yet as of Oct 16, 2025 - this is a temp unfinished placeholder
# TODO: Do we want multi-thread on this? would be gain any value by makign up a file
# delete list via pagination then MT the actual deletes?
def delete_s3_folder(s3_client, bucket_name, s3_folder_path):
    """
    Returns:
        - Number of files deleted
    """

    # re-validate the connection and credentials as well
    is_success, return_msg = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        # In this case, we want to raise a new Exception
        raise Exception(return_msg)

    # must have a slash on the end only
    # strip leading slash if exists
    s3_folder_path = s3_folder_path.lstrip("/")
    if not s3_folder_path.endswith("/"):
        s3_folder_path += "/"

    files_to_delete = []

    try:
        # use paginator as it can handle more than the 1000 file limit max from list_objects_v2
        # operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_src_path, 'Delimiter': '/'}
        # if you add the delimiter of '/' it will get files at that level only and not recursive
        operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_folder_path}
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(**operation_parameters)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    files_to_delete.append({'Key': obj['Key']})

        # Should we add mt to this?
        # If we do add TQDM, we really have to do it via pages and not files ???
        if files_to_delete:
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': files_to_delete})
            # maybe add tqdm ?
            # Callback=awssf.ProgressPercentage(src_file_path)

    except Exception as ex:

        # Check if our aws_exception_handler knows what it is.
        # if it finds it, it returns a nice user friendly message
        return_msg, _ = awssf.aws_exception_handler(ex)
        raise Exception(return_msg)

    return len(files_to_delete)


# -------------------------------------------------
# TODO: is this even possible via boto3?
# def move_s3_folder(bucket_name, s3_folder_path):
