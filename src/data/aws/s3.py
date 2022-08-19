#!/usr/bin/env python3

# standard library imports (https://docs.python.org/3/library/
# (imports first, then "froms", in alpha order)
import os
from pickle import FALSE
import sys
from datetime import datetime

# third party imports
# (imports first, then "froms", in alpha order)
import argparse

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

    def __init__(self, path_to_cred_env_file, is_verbose = False):
        
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
        path_to_cred_env_file : str  (manditory)
            File path of the aws_credentials_file as an .env
           
        is_verbose : bool
            If True, then debugging output will be included
        
        '''
        
        # pass the pathing to the aws credentials env file
        # to the parent as all aws child classes will need it
        super().__init__(path_to_cred_env_file)

        self.is_verbose = is_verbose

    
    def put_to_bucket(self, bucket_address, src_folder_path,
                      aws_target_folder_path, whitelist_file_path = None):
        
        '''
        Overview
        ----------
        Push a data folder to hydroviz visualization (primarily for now)
        
        If the aws_target_folder_path folder does not exist, it will be created. If it does exist, files will
        be just added or overwritten (no pre-clean)
       
        The source folder will copy all contents (files and subfolders)
        ie) bucket_address = some-address-us-east-1
            aws_target_folder_path = test_uploads/220818
            src_folder_path = /outputs/fim_0_34_1_fr/(some files and folders)
            
            Becomes: some-aws-address/test_uploads/220818/(some files and folders)
            
        Note:
            - We did not do a subprocess and cli due to filtering
            - strip tests are done in case this did not come in via command line
                (ie.. came in via direct method calls)
        

        Parameters
        ----------
        - bucket_address : str
            bucket address not including folder names
            ie.  some-address-us-east-1
        
        - src_folder_path : str
            folder path of files to be copied up
             
        - aws_target_folder_path : str
            folder locations in AWS (can not be blank) 
             
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
            
        if (self.is_verbose):
            print("files_to_be_submitted")
            print(whitelist_file_names)
    
        # test src_folder_path
        if (not os.path.exists(src_folder_path)):
            raise ValueError("Invalid local_folder_path (src directory)")
        src_folder_path = src_folder_path.rstrip('/') # trailing slashes

        # test bucket_address
        if (bucket_address is None) or (len(bucket_address.strip()) == 0):
            raise ValueError("aws bucket address not supplied")
        
        bucket_address = bucket_address.strip()
        
        # test aws target folder
        if (aws_target_folder_path is None) or (len(aws_target_folder_path.strip()) == 0):
            raise ValueError("aws target folder path not supplied")
        
        aws_target_folder_path = aws_target_folder_path.strip() # spaces
        aws_target_folder_path = aws_target_folder_path.strip('/') # leading/trailing slashes
        
        # --- Get the aws client
        self.aws_client = self.get_aws_client('s3')

        #if (self.is_verbose):
        #    print('Check to see if bucket exists')
        #Attempted code for validating that the bucket exists but had trouble and removed it
       
        # loop through files to copy up (or use cp/rsync commands from a subprocess)
        # seems like it is a mix of direct file/folder copy versus sub command.
        # Went with boto3 instead of CLI.  Boto3 might be slower but better to manage issues
        # like connections or dups, and can handle the whitelist better (without some very
        # ugly workarounds in cli)

        # --- Upload the files
        print("************************************")
        print("--- Start uploading files ")
        fh.print_current_date_time()
        start_dt = datetime.now()
        ctr=0
        try:
            for root, dirs, files in os.walk(src_folder_path):
                for file_name in files:
                    if (len(whitelist_file_names) > 0):
                        if (fh.is_string_in_list(file_name, whitelist_file_names) == False):
                            continue # skip this file

                    full_path_file_name = os.path.join(root, file_name)
                    
                    print(full_path_file_name)
                                            
                    aws_file_name = full_path_file_name.replace(src_folder_path, aws_target_folder_path)
                    self.aws_client.upload_file(full_path_file_name, bucket_address, aws_file_name)
                    ctr+=1
                    
        except Exception:        
            print(f"Error occured after {ctr} files were uploaded")
            fh.print_current_date_time() # helps track how long it was before erroring out
            raise

        print()
        aws_target_location = f"{bucket_address}/{aws_target_folder_path}"
        print(f"{ctr} files have been uploaded to {aws_target_location}")

        fh.print_current_date_time()
        end_dt = datetime.now()
        fh.print_date_time_duration(start_dt, end_dt)
        print("--- End uploading files ")

        print("************************************")        


if __name__ == '__main__':

    # Sample Usage
    #python3 /foss_fim/src/data/aws/s3.py -a put -b hydrovis-(some address)-us-east-1 -c /data/{some location}/aws_creds.env -w /foss_fim/config/aws_s3_put_whitelist.lst -s /data/aws_test_upload_folder
    
    # this works for hydroviz but can work with other s3 sites as well (ie esip)
    
    # You can leave the -w flag off to load all files/folders from a directory
        
    parser = argparse.ArgumentParser(description='Communication with aws s3 data services')
    parser.add_argument('-a','--action_type', 
                        help='value of get or put (defaults to put)',
                        default="put", required=False)
    parser.add_argument('-b','--bucket_address', 
                        help='s3 bucket address and folder', required=True)
    parser.add_argument('-c','--aws_cred_env_file', 
                        help='path to aws credentials env file', required=True)
    parser.add_argument('-s','--local_folder_path',
                        help='folder path of all files to be saved to or from', required=True)
    parser.add_argument('-t','--aws_target_folder_path',
                        help='folder path inside the bucket', required=True)
    parser.add_argument('-w','--whitelist_file_path', 
                        help='A file with the last of file names to be copied up (line delimited)',
                        required=False)
    parser.add_argument('-v','--is_verbose', 
                        help='Adding this flag will give additional tracing output',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())
    
    if (args['action_type'] == "put"):
        
        s3 = S3(path_to_cred_env_file = args['aws_cred_env_file'],
                is_verbose = args['is_verbose'])
        s3.put_to_bucket(bucket_address = args['bucket_address'],
                         src_folder_path = args['local_folder_path'],
                         aws_target_folder_path = args['aws_target_folder_path'],
                         whitelist_file_path = args['whitelist_file_path'])
        
    elif (args["action_type"] == "get"):
        raise Exception("Error: get method not yet implemented or available")
    else:
        raise Exception("Error: action type value invalid. Current options are: 'put' (more coming soon) ")


