#!/usr/bin/env python3

import os
import subprocess
import sys

from dotenv import load_dotenv

sys.path.append(os.getenv('srcDir'))
from utils.shared_functions import FIM_Helpers as fh


class AWS_Base(object):
    '''
    This class implements all common variables related when communicating to AWS
    '''

    def __init__(self, path_to_cred_env_file, *args, **kwargs):
        '''
        Overview
        ----------
        This will load the aws credentials enviroment file.
        For now, we will feed it via an env file. Eventuallly, it should be
        changed to ~/.aws/credentials (and maybe profiles)

        The aws_credentials_file will be loaded and an aws client
        will be created ready for use.

        Parameters
        ----------
        path_to_cred_env_file : str
            File path of the aws_credentials_file as an .env

        is_verbose : bool
            If True, then debugging output will be included

        '''

        if not os.path.exists(path_to_cred_env_file):
            raise FileNotFoundError("AWS credentials file not found")

        load_dotenv(path_to_cred_env_file)

        if kwargs:
            is_verbose = kwargs.pop("is_verbose", None)

        self.is_verbose = is_verbose

        # TODO: validate service name with AWS (which will help
        # validate the connection)

    def get_aws_cli_credentials(self):
        '''
        Overview
        ----------
        To run aws cli commands (subprocess), aws creds need to be set up
        in the command environment. This method will take care of that
        via bash "exports"

        Returns
        -----------
        A string that can be concatenated to the front of a subprocess cmd
        and includes the export creds.

        '''

        fh.vprint("getting aws credential string", self.is_verbose, True)

        cmd = "export AWS_ACCESS_KEY_ID=" + os.getenv('AWS_ACCESS_KEY')
        cmd += " && export AWS_SECRET_ACCESS_KEY=" + os.getenv('AWS_SECRET_ACCESS_KEY')
        cmd += " && export AWS_DEFAULT_REGION=" + os.getenv('AWS_REGION')

        return cmd

    def create_aws_cli_include_argument(self, whitelist_file_names):
        '''
        Overview
        ----------
        Creates a string valid for aws_cli include commands.

        When using an "include", this string will automatically add
        --exclude "*" , without it, the includes will not work.

        If the whitelist_file_names is empty, then an empty string will be returned.

        Parameters
        ----------
        whitelist_file_names : list
            a list of file names to be whitelisted. The file names should already be adjusted
             to the "*" pattern already and stripped as applicable.

        Returns
        ----------
        A string that can be added straight into a aws cli command.

        example: export AWS_ACCESS_KEY_ID=A{somekey}Q && export
        AWS_SECRET_ACCESS_KEY=KW(examplekey)80 &&
        export AWS_DEFAULT_REGION=us-west-1
        '''

        if (whitelist_file_names is None) or (len(whitelist_file_names) == 0):
            return ""  # empty string

        # For there to be "includes", for aws cli, you must have exclude "all"
        cli_whitelist = '--exclude "*"'

        for whitelist_file_name in whitelist_file_names:
            if not whitelist_file_name.startswith("*"):
                whitelist_file_name = "*" + whitelist_file_name

            whitelist_file_name = whitelist_file_name.replace("{}", "*")

            cli_whitelist += f' --include "{whitelist_file_name}"'

        fh.vprint(f"cli include string is {cli_whitelist}", self.is_verbose, True)

        return cli_whitelist
