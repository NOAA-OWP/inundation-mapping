#!/usr/bin/env python3

# standard library imports (https://docs.python.org/3/library/
# (imports first, then "froms", in alpha order)
from operator import truediv
import os
from pickle import FALSE
import sys
from datetime import datetime

# third party imports
# (imports first, then "froms", in alpha order)
import argparse
import subprocess

sys.path.append('/foss_fim/src')
from aws_base import *
from utils.shared_functions import FIM_Helpers as fh

'''
This file is for communicating to any AWS S3 buckets.
For now, it can only push to a bucket (such as hydroviz)

Note: For now, we will add a parameter for the aws credentials .env file.
This means the .env file does not need to automatically be with the source code.
Later, this will be changed to aws credentials profiles (or ~/aws/credentials (folders))
'''

class S3(AWS_Base):
    
    def put_to_bucket(self, src_folder_path,
                      aws_target_path, whitelist_file_path = None):
        
        '''
        Overview
        ----------
        Push a data folder to hydroviz visualization (primarily for now)
        
        If the aws_target_folder_path folder does not exist, it will be created. If it does exist, files will
        be just added or overwritten (no pre-clean)
       
        The source folder will copy all contents (files and subfolders)
        ie) aws_target_path = s3://some-address-us-east-1/test_uploads/220818
            src_folder_path = /outputs/fim_0_34_1_fr/(some files and folders)
            
            Becomes: some-aws-address/test_uploads/220818/(some files and folders)
            
        Note:
            - This is build on aws cli as boto3 can push up single files but is very slow.
              As of Aug 2022, they do not have a bulk boto3 recursive folder load

        Parameters
        ----------
       
        - src_folder_path : str
            folder path of files to be copied up
             
        - aws_target_path : str
            A s3 bucket and folder locations in AWS (can not be blank) 
             
        - whitelist_file_path : str
            A file with a set of line delimited file names that can be copied up to S3.
            Note: make sure the list file is unix encoded and not windows encoded.
            If None, then all files / folders will be pushed up
            Note: wildcard variables of * is available.
            
        '''
        whitelist_file_names = [] # if blank, it is assumed to load all

        # --- Validate incoming values
        # test the whitelist path and load it into a collection
        if (whitelist_file_path is not None) and (len(whitelist_file_path.strip()) > 1):
            
            # if this list has items, then we load those items only 
            whitelist_file_names = fh.load_list_file(whitelist_file_path.strip())
            
        if (self.is_verbose) and (len(whitelist_file_names) > 0):
            print("whitelist entries")
            print(whitelist_file_names)
    
        # test src_folder_path
        if (not os.path.exists(src_folder_path)):
            raise ValueError("Invalid local_folder_path (src directory)")
        src_folder_path = src_folder_path.rstrip('/') # trailing slashes
        
        # test aws target folder
        if (aws_target_path is None) or (len(aws_target_path.strip()) == 0):
            raise ValueError("aws target folder path not supplied")
        
        aws_target_path = aws_target_path.strip() # spaces
        aws_target_path = aws_target_path.strip('/') # leading/trailing slashes
        

        # --- Upload the files
        print("************************************")
        print("--- Start uploading files ")
        fh.print_current_date_time()
        start_dt = datetime.now()

        self.bulk_upload(src_folder_path, aws_target_path, whitelist_file_names)        

        print("--- End uploading files")        
        fh.print_current_date_time()
        end_dt = datetime.now()
        fh.print_date_time_duration(start_dt, end_dt)

        print("************************************")        


    def bulk_upload(self, src_folder_path, aws_target_path, whitelist_file_names = []):
        
        '''
        Overview
        ----------
        Files will be loaded keeping their folder structure and will be loaded
        via Bash AWS CLI. It is preferred to load via boto3 but as of Aug 2022, it does
        not have a bulk loader, only a single file loader and it is very slow.
        
        With CLI, we have a bit of trouble with outputs, but will track it as best we can
        
        Parameters
        ----------
        - src_folder_path : str
            fully pathed location of where the files are being copied from
            
        - aws_target_path : str
            The full url including subfolder path
            For example: s3://example-us-east-1/test_upload/test_2
        
        - whitelist_file_names : list (but can be empty)
            A list of file names to be included in the transfer. If the file name includes
            a {}, it will be adjusted for aws cli format automatically.

        Returns
        -----------------
        True (successful copy ) or False (unsuccessful)

        '''

        is_copy_successful = False

        cmd = self.get_aws_cli_credentials()
        
        # used cp (copy and replace. could have used 'sync' which looks for updates only)
        cmd += f' && aws s3 cp --recursive {src_folder_path}'
        
        if not aws_target_path.startswith("s3://"):
            aws_target_path = "s3://" + aws_target_path
        
        cmd += f' {aws_target_path}'
        
        if (whitelist_file_names is not None) and (len(whitelist_file_names) > 1):
            cmd += f' {self.create_aws_cli_include_argument(whitelist_file_names)} '

        fh.vprint(f"cmd is {cmd}", self.is_verbose, True)
        print("")
        
        process = subprocess.Popen(cmd, shell = True, bufsize = 1,
                                stdout = subprocess.PIPE, 
                                stderr = subprocess.STDOUT,
                                errors = 'replace'
                                ) 
        
        while True:
            
            realtime_output = process.stdout.readline()
            if realtime_output == '' and process.poll() is not None:
                break
            if realtime_output:
                # AWS spits out a tons of "completes"
                if (not realtime_output.startswith("Completed")):
                    print(realtime_output.strip(), flush=False)
                sys.stdout.flush()
                    
        is_copy_successful = True  # catching of correct error not quite working
            
        return is_copy_successful


if __name__ == '__main__':

    # Sample Usage
    #python3 /foss_fim/data/aws/s3.py -a put -c /data/config/aws_creds.env -s /data/previous_fim/fim_4_0_13_1 -t "s3://example-us-east-1/fim_4_0_13_1" 
    
    # You can leave the -w flag off to load all files/folders from a directory
    # but default is to -w /foss_fim/config/aws_s3_put_fim4_hydrovis_whitelist.lst
    
    # this works for hydroviz but can work with other s3 sites as well (ie esip)
            
    parser = argparse.ArgumentParser(description='Communication with aws s3 data services')
    parser.add_argument('-a','--action_type', 
                        help='value of get or put (defaults to put)',
                        default="put", required=False)
    parser.add_argument('-c','--aws_cred_env_file', 
                        help='path to aws credentials env file', required=True)
    parser.add_argument('-s','--local_folder_path',
                        help='folder path of all files to be saved to or from', required=True)
    parser.add_argument('-t','--aws_target_path',
                        help='s3 bucket address and folder', required=True)
    parser.add_argument('-w','--whitelist_file_path', 
                        help='A file with the last of file names to be copied up (line delimited)',
                        default='/foss_fim/config/aws_s3_put_fim4_hydrovis_whitelist.lst',
                        required=False)
    parser.add_argument('-v','--is_verbose', 
                        help='Adding this flag will give additional tracing output',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())
    
    if (args['action_type'] == "put"):
        
        s3 = S3(path_to_cred_env_file = args['aws_cred_env_file'],
                is_verbose = args['is_verbose'])
        s3.put_to_bucket(src_folder_path = args['local_folder_path'],
                         aws_target_path = args['aws_target_path'],
                         whitelist_file_path = args['whitelist_file_path'])
        
    elif (args["action_type"] == "get"):
        raise Exception("Error: get method not yet implemented or available")
    else:
        raise Exception("Error: action type value invalid. Current options are: 'put' (more coming soon) ")


