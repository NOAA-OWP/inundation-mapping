#!/usr/bin/env python3

import math
import os
import sys
import threading

import botocore.exceptions

# -------------------------------------------------
def aws_exception_handler(ex):
    """
    There are a number of places in this code that can throw exceptions of all types.
    This helps with message standarization.

    This part will handle the ones it can and return msg's so the client can decide
    how to managed it.

    A few exceptions will re-throw new exceptions.

    """
    msg = ""

    if (
         (isinstance(ex, botocore.exceptions.NoCredentialsError))
         or (isinstance(ex, botocore.exceptions.PartialCredentialsError))
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
    ):  # 1003
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
        raise (ex)

    return msg

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
            sys.stdout.write(
                "\r%s / %s MiB (%.2f%%)" % (
                    seen_so_far, self._orig_size_formatted, percentage
                )
            )
            sys.stdout.flush()