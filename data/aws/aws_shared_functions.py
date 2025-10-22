#!/usr/bin/env python3

import os
import sys
import threading
import traceback

import boto3
import botocore.exceptions
from botocore.client import Config


# Note: Sep 2025: Why all of this architecture usign boto3 instead of simpler subprocess
# and cli?  We want more control over errors ane exceptions, plus this system can do more
# complex things that cli can do such as wildcard, more selective recursion, etc.
# This AWS system will also be upgraded soon handle much more than just s3, but things like
# step functions, task definitions, execution scripts, etc. ie: tools to make simple
# python scripts to launch UAT step function runs, then check the results and aws logs.


# -------------------------------------------------
def create_aws_session(
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
    aws_region: str = None,
    aws_config_profile_name: str = None,
):
    '''
    Inputs:
        - aws_service_type_names: What type of client are you creating. This code currently
          supports only the "s3" type, but more are expected soon.

        - aws_*:  See notes below about creating AWS Boto3 clients.

    Returns:
        - True / False (success)

        - error_msg: The calling code, may not want to automatically want to shut the programe
          down based on one of these exceptions. For most exceptions, then "success" will
          be False and an error message.  We will attempt to catch most types of common aws exceptions
          and submit it back in a user friendly error message that can be optionally displayed to the
          user. For some exceptions, we will just let the actual exception be raised back and not
          part of this return error_msg.

        - An AWS Boto3 session.
          One session can be used against multiple clients and they don't need to be the same type
          of client. ie)
                s3_client = session.client('s3')
                ec2_client = session.client('ec2')
                lambda_client = session.client('lambda')

        - Can also send back an exception. See notes above talking about the error_msg return value.

    AWS Boto3 Sessions:
    AWS Sessions can be created in a very wide array of ways to authenticate.
    It depends on how the aws administrator setup permissions and what account has permission
    to which services.
    users. They can include:
        - Explicit AWS access keys:
            - aws_access_key_id and aws_secret_access_key: Can not have one without the other.
        - Tokens:
            - If you use explicit aws access keys, sometimes, but not always you may need a temp token.
              If you don't need to provide it, an implicit token will be created. Tokens can be set by
              AWS admins to timeout or not, default is 12 hours timeouts.
        - AWS regions:
            - Sometimes an aws_region_name may need to explicitly defined but generally not.
              ie) us-east-1
        - AWS Profiles:
            - In some situations, you may choose to use an AWS config profile. This is a command
              line option creating named aws credential profiles. Details not provided here.

    Some types of authenication create an implicit session token and do not need to automatically
    be explicitly defined. Some AWS administrators can set where sessions never time out.

    '''

    '''
    TODO: Sept 2025
    Look into this for session timeout issues, if applicable, if sessions are being used:
    Refresh temporary credentials: For long-running processes, you may need to periodically
    refresh the temporary credentials before they expire. This involves calling assume_role
      or get_session_token again to obtain new credentials and updating the boto3.Session object with them.
    '''

    if aws_access_key_id is not None and aws_access_key_id != "":
        # then the aws_secret_access_key must also exist
        if aws_secret_access_key is None or aws_secret_access_key == "":
            raise ValueError(
                "The AWS Access Key ID has been submitted but the AWS Secret Access Key"
                " has not. Both are always used together if authenicating using this method."
            )

    error_msg = ""
    aws_session_obj = None

    try:
        session_args = {}

        if (
            aws_access_key_id is not None and aws_access_key_id != ""
        ):  # We validated above that either both or none mst exist
            session_args['aws_access_key_id'] = aws_access_key_id
            session_args['aws_secret_access_key'] = aws_secret_access_key
        elif (
            aws_config_profile_name is not None and aws_config_profile_name != ""
        ):  # Can not have a profile and explicit keys
            session_args['profile_name'] = aws_config_profile_name

        if aws_region is not None and aws_region != "":
            session_args['region_name'] = aws_region

        if aws_session_token is not None and aws_session_token != "":
            session_args['aws_session_token'] = aws_session_token

        if len(session_args) == 0:  # It is possible.
            aws_session_obj = boto3.Session()
        else:
            aws_session_obj = boto3.Session(**session_args)

    except Exception as ex:
        # the aws will handle many messages and what it can not, it will
        # re-raise, which we can further re-raise
        error_msg, type_known = aws_exception_handler(ex)
        return False, error_msg, None

    # True/False (success), no error_msg, s3_client object
    return True, "", aws_session_obj


# -------------------------------------------------
def create_aws_client(
    aws_service_type_name: str,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
    aws_region: str = None,
    aws_config_profile_name: str = None,
    bucket_name = None
):
    '''

    Using this tool will automatically create it's own session object. Note: Sessions objects
    can be used for multiple clients (not even all the same type), but a client can only be
    started based on one session. See more notes about sessions in the create_aws_session function.

    If you want to make your own session and use it to make multiple clients, use other functions.

    Boto3 with managing our own multi-thread is pretty much the fastest option also recognizing
    that are usually using filtered lists with mostly small files, high volume. We can only go
    as fast as our network pip (speed) can handle, but we want it maxed out.

    We also do not use s3 config 'use_accelerate_endpoint': True as the bucket has to have it enabled.

    Inputs:
        - aws_service_type_names: What type of client are you creating. This code currently
          supports only the "s3" type, but more are expected soon.

        - aws_*:  See notes below about creating AWS Boto3 clients and sessions. Some notes above
        in the create_aws_session.

    Returns:
        - True / False (success)

        - error_msg: The calling code, may not want to automatically want to shut the programe
          down based on one of these exceptions. For most exceptions, then "success" will
          be False and an error message.  We will attempt to catch most types of common aws exceptions
          and submit it back in a user friendly error message that can be optionally displayed to the
          user. For some exceptions, we will just let the actual exception be raised back and not
          part of this return error_msg.

        - An AWS Boto3 client.
          Note: One client type is good for only one service. ie) s3.
          If you need more than one client type at the same time, you will need more than one
          client and will call function twice.
          However, with this client being built on aws sessiosn, aws sessions can have multiple
          clients and can have different types of simultaneous types.

        - Can also send back an exception. See notes above talking about the error_msg return value.
    '''

    # Validation:
    if aws_service_type_name != 's3':
        raise ValueError("AWS Service types values are only s3 at this time. More coming soon.")

    error_msg = ""
    aws_client_obj = None

    try:
        # -------------------
        # setup session first. It might send back an friendly error message
        # but can also send back an exception.
        is_success, error_msg, aws_session_obj = create_aws_session(
            aws_access_key_id, aws_secret_access_key, aws_session_token, aws_region, aws_config_profile_name
        )

        if is_success is False:  # we assume an error message came back
            return False, error_msg, None

        # TODO: fix this... needs some tweaks
        # Setup Config to manage timeouts on initial connection
        # The read_timeout has been overridden to higher than default
        # in case it is pulling down a large file.
        # And a very high max_pool_connections for multi-thread and clients that are open a long
        # time and re-used. Likely will be more than the network speed can handle but lets leave it higher anyways.
        client_config = Config(
            connect_timeout=20,
            read_timeout=(60 * 5),
            max_pool_connections=30,
            retries={"mode": "standard", 'max_attempts': 5}
        )

        # -------------------
        # Now the client and aws config overrides
        if aws_service_type_name == "s3":
            aws_client_obj = aws_session_obj.client("s3", config=client_config)
        else:
            raise ValueError("AWS Service types values are only s3 at this time. More coming soon.")

        # validate if the client is fully valid as it is
        if aws_client_obj is None:
            raise Exception(
                "Error: Something when wrong creating the communication path to AWS."
                " Exact details are not always known. Please recheck your credentials,"
                " aws pathing, etc before trying again."
            )

    except Exception as ex:
        # the aws will handle many messages and what it can not, it will
        # re-raise, which we can further re-raise
        error_msg, type_known = aws_exception_handler(ex)
        if type_known is False:
            raise Exception(error_msg)

        return False, error_msg, None

    # True/False (success), no error_msg, s3_client object
    return True, "", aws_client_obj


# -------------------------------------------------
def aws_exception_handler(ex):
    """
    There are a number of places in this code that can throw exceptions of all types.
    This helps with message standarization.

    This part will handle the ones it can and return msg's so the client can decide
    how to managed it.

    """
    msg = ""
    type_known = True

    if (isinstance(ex, botocore.exceptions.NoCredentialsError)) or (
        isinstance(ex, botocore.exceptions.PartialCredentialsError)
    ):
        msg = (
            "ERROR: Bad AWS Credentials: There are a number of ways this can fail. You may be missing"
            " or have invalid aws access key, aws secret access key or aws region values (case-sensitive)."
            " It is also possible that you may have used implicit aws credentials using a default"
            " aws credentials file which may be out of sync.\n\n"
            " Note: It is possible it has expired. Check your arg values, ensure it has"
            " quotes around the arg value or check with your aws admin."
        )

    elif isinstance(ex, botocore.exceptions.ProfileNotFound):
        msg = (
            "ERROR: AWS named profile or default profile not found:\n"
            "AWS profiles can use the default one, or if you have provided an explicity named"
            " profile, you may have provided the wrong name (case-sensitive)"
            " It is also possible that you may have used implicit aws credentials using a default"
            " aws credentials file which may be out of sync.\n\n"
            "Check your aws profiles or profile name or check with your aws admin."
        )

    elif isinstance(ex, botocore.exceptions.NoAuthTokenError):
        msg = (
            "ERROR: AWS Auth Token Error: Part of the authorization process to talk to AWS often, but not"
            " always, uses AWS Authorization Tokens. Depending on how you are using AWS authenication, such as"
            " AWS Access Key / AWS Secret Key, aws credentials file, there is an error. Review your AWS"
            " credentials arguments, file, system, other (case-sensitive)."
        )

    elif (
        (isinstance(ex, botocore.exceptions.NoRegionError))
        or (isinstance(ex, botocore.exceptions.InvalidRegionError))
        or (isinstance(ex, botocore.exceptions.UnknownRegionError))
    ):
        msg = (
            "ERROR: AWS No Region Value, Invalid or Unknown Region Defined: An aws region value,"
            " such as 'us-east-1', needs to be defined, is missing or is invalid (case-sensitive)."
            " Review your AWS credentials arguments, file, system, other."
        )

    # This can be used to catch more specific errors usuall after the client has been created succssfully
    elif isinstance(ex, botocore.exceptions.ClientError):
        # Common possibilities are:
        #    AccessDeniedException
        #    ValidationException
        #    NoSuchEntityException
        #    InvalidParameterException
        #    lots of others

        # Access the error code and message
        error_code = ex.response['Error']['Code']
        error_message = ex.response['Error']['Message']

        if error_code == 'NoSuchBucket':
            msg = (
                "ERROR: no such bucket: The aws bucket name does not exist."
                " Please check the spelling (case-sensitive) or with the aws bucket owner."
            )

        elif error_code == 'AccessDeniedException':
            msg = (
                "ERROR: AccessDeniedException: Your authorization to talk to AWS is just fine,"
                " but permission to talk to the object (S3 bucket, Step function, etc) is not authorized.\n"
                " Talk to your AWS administrator about it."
            )

        elif error_code == 'SignatureDoesNotMatch':
            msg = (
                "ERROR: SignatureDoesNotMatch: This generally means the AWS Secret Access key is incorrect."
                " Please check the value or with the aws bucket owner. Note: some AWS SA keys can have legitimate"
                " non alpha-chars like forward slashs and sometimes copy / paste can mess it up."
            )

        elif error_code == "404":  # File/object does not exist
            msg = "ERROR: (404) File or object does not exist"

        else:
            # catch whatever is left over.
            msg = (
                "ERROR: Undefined AWS ClientError:"
                " An AWS client error has occurred which can be a wide range of possibilities. "
                " Please review your AWS credential information in case that it the issue (case-sensitive)."
                "\n\n Details: "
                f"\n    Operation: {ex.operation_name}"
                f"\n    Error Code: {error_code}"
                f"\n    Error Message: {error_message}"
            )

    elif isinstance(ex, botocore.exceptions.BotoCoreError):
        # This category encompasses exceptions related to client-side issues within Boto3
        #   or its underlying library, Botocore, rather than errors returned by AWS services.
        #   But can also include things like:
        #      ConnectTimeoutError
        #      ConnectionClosedError
        #      EndpointConnectionError

        msg = (
            "ERROR: BotoCoreError: There are a wide number of things that can occur for"
            "this type of error to occur. Generally is with something about communication / connection"
            " talking to AWS, but it can also point to a python package problem. It can also be that you"
            " supplied in invalid aws region value. Please try it again to see if the error continues.\n\n"
            f"   Details from AWS: {ex}"
        )

    else:
        # anything left over here we will rethrow, likely not an AWS error
        #         raise ex
        msg = traceback.format_exc()
        type_known = False

    return msg, type_known


# -------------------------------------------------
# Can show upload or download progress for large files.
class ProgressPercentage(object):
    def __init__(self, filename):
        # self._filename = filename
        size_in_mib = round((os.path.getsize(filename) / 1024 / 1024), 2)
        self._orig_size_formatted = f"{size_in_mib:.2f}"
        self._size = size_in_mib
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            mg_so_far = round((bytes_amount / 1024 / 1024), 2)
            self._seen_so_far += mg_so_far
            percentage = (self._seen_so_far / self._size) * 100
            seen_so_far = f"{self._seen_so_far:.2f}"
            sys.stdout.write("\r%s / %s MiB (%.2f%%)" % (seen_so_far, self._orig_size_formatted, percentage))
            sys.stdout.flush()
