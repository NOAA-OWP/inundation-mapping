#!/usr/bin/env python3

import os

import boto3
import botocore.exceptions
from botocore.client import ClientError


'''
Note: you may already know this but S3 does not have a concept of "folders", but
uses a similar system using "prefixes".  When the word "folder" is used here, it
is translated to a pattern of "prefixes" that S3 can use.

July 2025: This was created from a file in the ras2fim repo / also named s3_shared_functions.

In that repo, above what we have here, it also contains code for: (likely needs small mods)
  - upload files(s), folder(s)
  - delete files/folders
  - move files/folders  (note: ras2fim has it wrong that you have to manually copy then delete)
  - file(s) search with wildcard
  - get folder sizes

'''


# -------------------------------------------------
def create_boto3_s3_client(aws_access_key="", aws_secret_access_key="", aws_region="", aws_session_token=""):
    '''
    There are a number of ways a client can be created and it depends on various combinations.
    All of the arg above are optional as if they are using implicit aws cred such as an saved
    credentials files, or IAM permissions to the logged in user or server. They also might
    submit explicit credentials such as access key, secret access key, region. The session token
    can also be optional as it again is based on how the permissioxn are setup.

    There are number of ways to authenticate. Sometimes you can even submit explicit access
    key and secret key but it can fail if you have certain types (but not all) of implied
    authorization.

    Note: session tokens are temp access keys

    Return:
      (success) True / False (bucket exists)
      return_code (int): return 0 if successful, otherwise returns an error code
      Can also throw exceptions.

    '''
    s3_client = None

    # You can create a client with no credentials which means it is implicit, either by an
    # an AWS credentials file, "EXPORT" commands, or authentication of the logged in user or server

    try:
        if (
            aws_access_key == ""
            and aws_secret_access_key == ""
            and aws_region == ""
            and aws_session_token == ""
        ):
            s3_client = boto3.client("s3")

        # It is possible to add (or need to add) a region only.
        elif (
            aws_access_key == ""
            and aws_secret_access_key == ""
            and aws_region != ""
            and aws_session_token == ""
        ):
            s3_client = boto3.client("s3", aws_region=aws_region)

        elif (aws_access_key != "" and aws_secret_access_key == "") or (
            aws_access_key == "" and aws_secret_access_key != ""
        ):
            raise Exception(
                "Error: You submitted a value to either the AWS access key or AWS secret key"
                " but the other (access key or secret key) does not exist. Both must exist"
                " or neither exist, depending on which AWS authentication system you are using"
                " (explicit creds, implied via .crendentials file, implicit user or server"
                " authorization, session exports, etc)"
            )

        elif aws_access_key != "" and aws_secret_access_key != "" and aws_region == "":
            raise Exception(
                "Error: When submitting an AWS access key and secret key, you must also provide"
                " an AWS region value which has not be provided"
            )

        elif aws_access_key != "" and aws_secret_access_key != "" and aws_region != "":
            if aws_session_token != "":
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_region=aws_region,
                    aws_session_token=aws_session_token,
                )
            else:
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_region=aws_region,
                )

    except botocore.exceptions.NoCredentialsError:
        is_success = False
        return_code = 1000

    except botocore.exceptions.ClientError:
        is_success = False
        return_code = 1001

    except botocore.exceptions.NoAuthTokenError:
        is_success = False
        return_code = 1002

    except (
        botocore.exceptions.NoRegionError,
        botocore.exceptions.InvalidRegionError,
        botocore.exceptions.UnknownRegionError,
    ):
        is_success = False
        return_code = 1003
    except Exception as ex:
        msg = "Something went wrong with the values or combination of values "
        "submitted for AWS authenication or creating the s3 communcation."
        msg += f"; Details = {ex}"
        raise Exception(msg)

    # in case some combination slipped through for error checking
    if s3_client is None:
        raise Exception(
            "Something went wrong with the values or combination of values "
            "submitted for AWS authenication or creating the s3 communcation."
        )
    else:
        is_success = True
        return_code = 0

    return is_success, return_code, s3_client


# -------------------------------------------------
def get_descriptive_error_msg(error_code):

    msg = ""

    if error_code == 0:
        msg = "Success"

    # We are including the most common AWS errors mostly based around permissions.
    if error_code == 1000:  # bad credentials or NoCredentialsError
        msg = "ERROR: Bad AWS Credentials: There are a number of ways this can fail. You may be missing"
        " or have invalid aws access key, aws secret access key or aws region values (case-sensitive)."
        " It is also possible that you may have used implicit aws credentials using a default"
        " aws credentials file which may be out of sync."
        " Note: It is possible it has expired. Check your arg values, ensure it has"
        " quotes around the arg value or check with your aws bucket owner."

    elif error_code == 1001:  # error while validating AWS Creds
        msg = "ERROR: Undefined Error: An unknown internal error has occured while validate the aws credentials."
        " Please review your AWS credential information in case that it the issue (case-sensitive)."

    elif error_code == 1002:  # AWS NoAuthTokenError
        msg = "ERROR: AWS Auth Token Error: Part of the authorization process to talk to AWS often, but not"
        " always, uses AWS Authorization Tokens. Depending on how you are using AWS authenication, such as"
        " AWS Access Key / AWS Secret Key, aws credentials file, there is an error. Review your AWS"
        " credentials arguments, file, system, other (case-sensitive)."

    elif error_code == 1003:  # AWS NoRegionError, UnknownRegionError, InvalidRegionError
        msg = "ERROR: AWS No Region Value Defined: An aws region value, such as 'us-east-1', needs to be defined,"
        " is missing or is invalid (case-sensitive). Review your AWS credentials arguments, file, system, other."

    elif error_code == 1050:  # No such bucket
        msg = "ERROR: no such bucket: The aws bucket name does not exist."
        " Please check the spelling (case-sensitive) or with the aws bucket owner."

    elif error_code == 1051:  # Folder not found
        msg = "ERROR: Folder not found: The aws folder name (key) does not exist."
        " Please check the spelling (case-sensitive) or pathing."

    else:
        raise Exception("Error: Unknown error code submitted")

    return msg


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
      (success) True / False (bucket exists)
      return_code (int): return 0 if successful, otherwise returns an error code
      Can also throw exceptions.
    """

    if s3_client is None:
        raise Exception("S3 Client not initiated")

    # strip start and end slashs if exist
    bucket_name = bucket_name.strip("")

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        # resp = client.head_bucket(Bucket=bucket_name)
        # print(resp)
        is_success = True  # no exception?  means it exist
        return_code = 0

    except botocore.exceptions.NoCredentialsError:
        is_success = False
        return_code = 1000

    except botocore.exceptions.ClientError:
        is_success = False
        return_code = 1001

    except botocore.exceptions.NoAuthTokenError:
        is_success = False
        return_code = 1002

    except (
        botocore.exceptions.NoRegionError,
        botocore.exceptions.InvalidRegionError,
        botocore.exceptions.UnknownRegionError,
    ):
        is_success = False
        return_code = 1003

    except s3_client.exceptions.NoSuchBucket:
        is_success = False
        return_code = 1050

    # should throw anything else (communcation error, accessdenied, etc )

    # other exceptions can be passed through
    return is_success, return_code


# -------------------------------------------------
def does_s3_folder_exist(s3_client, bucket_name, s3_prefix_folder_path):
    """
    Process:
    Input:
        - s3_prefix_folder_path: eg. inputs/fema  (from s3://some_bucket/inputs/fema)
    Returns two strings:
        (success) True / False,
        (err code if any) -- (some message, but only in error)
                options coming back are:
                - "1000 (bad or no credentials"
                - "1050 (no such bucket)"
                - "1003 (region issues)"
        But can throw an error when catastrophic
        To get more user friendly info for the code, you can call get_error_msg_description
        etc.
    """
    # validate the connection and credentials as well
    is_success, return_code = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        raise Exception(f"Error: details: {get_descriptive_error_msg(return_code)}")

    # strip starting and ending slashes
    s3_prefix_folder_path = s3_prefix_folder_path.strip("/")

    # If the bucket is incorrect, it will throw an exception that already makes sense
    # Don't need pagination as MaxKeys = 2 as prefix will likely won't trigger more than 1000 rec
    s3_objs = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=s3_prefix_folder_path, MaxKeys=2, Delimiter="/"
    )

    # print(s3_objs)
    if s3_objs["KeyCount"] > 0:
        is_success = False
        return_code = 1051  # folder does not exist. Don't auto raise an exception
    else:
        return_code = 0
        is_success = True

    # other exceptions can be passed through
    return is_success, return_code


# -------------------------------------------------
def does_s3_file_exist(s3_client, bucket_name, s3_file_path):
    """

    Note: This function might have limited use as if you actualy attempt to download
       it will tell you as a True/False if the file existed.

    Process:  This will throw exceptions for all errors
    Input:
        - s3_file_path: eg. /inputs/my_file.csv
    Output:
    Returns two strings:
        (success) True / False,
        (err code if any) -- (some message, but only in error)
                options coming back are:
                - "1000 (bad or no credentials"
                - "1050 (no such bucket)"
                - "1003 (region issues)"
        But can throw an error when catastrophic
        To get more user friendly info for the code, you can call get_error_msg_description
        etc.
    """

    file_exists = False

    s3_full_file_path = s3_file_path.replace("\\", "/")
    # strip starting slash
    s3_full_file_path.lstrip("/")

    bucket_name, s3_file_path = parse_bucket_and_folder_name(s3_full_file_path)

    # validate the connection and credentials as well
    is_success, return_code = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        raise Exception(f"Error: details: {get_descriptive_error_msg(return_code)}")

    result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)

    if 'Contents' in result:
        file_exists = True

    return file_exists, 0


# -------------------------------------------------
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

    # TODO: Add flag to allow for optional recursive and possibly wildcards

    s3_src_folder_path = s3_src_folder_path.replace("\\", "/")

    # strip start / ending slashes if exist
    s3_src_folder_path = s3_src_folder_path.strip("/")

    # validate the connection and credentials as well
    is_success, return_code = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        raise Exception(f"Error: details: {get_descriptive_error_msg(return_code)}")

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

    return s3_items


# -------------------------------------------------
def download_s3_file(s3_client, bucket_name, s3_file_key, target_file_path):
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

    # validate the connection and credentials as well
    is_success, return_code = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        raise Exception(f"Error: details: {get_descriptive_error_msg(return_code)}")

    does_file_exist = False
    # strip leading slash if exists
    s3_file_key = s3_file_key.lstrip("/")
    try:
        # Does not return anythign but will throw and exception if it does not exist
        s3_client.download_file(bucket_name, s3_file_key, target_file_path)
        does_file_exist = True
    except ClientError:  # usually only thrown when it is not there
        does_file_exist = False

    return does_file_exist


# -------------------------------------------------
# TODO: Consider adding multi-threading. Might have to check how MT affects the s3_client.
# ie.. client collisions?
def download_s3_folder(s3_client, bucket_name, s3_src_path, trg_folder_path):
    """
    Process:
        - Note: The boto3.client must be already instantated and passed in
        - Using the incoming s3 src folder, call get_records to get a list of child folders and files
        - Open a s3 client and iterate through the files to download
        - Is recursive
    Inputs:
        - s3_client: my_client = boto3.client(profile, creds, whatever)
        - s3_src_path: e.g. /inputs/fema (from s3://{some_bucket}/inputs/fema)
        - trg_folder_path: e.g . /data/inputs/fema

    Returns:
        - True or False (did a least one file download successfully)

        The calling code and decide what to do with it.
        Note: Exceptions can still be thrown for catastropic errors (creds, other)
    """

    # This also validates that the bucket exists
    is_success, return_code = does_s3_bucket_exist(s3_client, bucket_name)
    if not is_success:
        raise Exception(f"Error: details: {get_descriptive_error_msg(return_code)}")

    s3_src_path = s3_src_path.strip("/")

    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': s3_src_path, 'Delimiter': '/'}
    page_iterator = paginator.paginate(**operation_parameters)

    min_one_file_downloaded = False
    for page in page_iterator:
        if 'Contents' in page:
            # Iterate over each object and download it
            for obj in page['Contents']:
                s3_key = obj['Key']
                if s3_key[-1] != "/":  # if it was a folder (ending in a slash, we skip it)
                    rel_path = os.path.relpath(s3_key, s3_src_path)
                    local_file_path = os.path.join(trg_folder_path, rel_path)

                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    s3_client.download_file(bucket_name, s3_key, local_file_path)
                    min_one_file_downloaded = True

    return min_one_file_downloaded
