#!/usr/bin/env python3

# standard library imports (https://docs.python.org/3/library/
# (imports first, then "froms", in alpha order)
import os
from xmlrpc.client import boolean

# third party imports
# (imports first, then "froms", in alpha order)
import boto3
from botocore.exceptions import ClientError

from dotenv import load_dotenv

'''
This implements all common variables related when communicating to AWS
'''
class AWS_Base(object):
    
    def __init__(self, path_to_cred_env_file):

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

        '''
        
        if (not os.path.exists(path_to_cred_env_file)):
            raise FileNotFoundError("AWS credentials file not found")
            
        load_dotenv(path_to_cred_env_file)
        
        # TODO: validate service name with AWS (which will help
        # validate the connection)
    
    
    def get_aws_client(self, aws_service_type):
        
        '''
        Overview
        ----------
        This will create an aws (boto3) client, open it and add it
        as a property to your object, if it does not already exist
        
        Parameters
        ----------
        aws_service_type : str
            an aws service name such as aws, batch, sts, s3, account, or whatever.
            list of accepted aws service names at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/index.html
           
        Returns
        ----------
        a boto3 client
        '''

        if (self.is_verbose):
            print("loading client")
            print(aws_service_type)
            print(os.getenv('AWS_REGION'))
            print(os.getenv('AWS_ACCESS_KEY'))
            print(os.getenv('AWS_SECRET_ACCESS_KEY'))
        
        aws_client = boto3.client(
            aws_service_type,
            region_name = os.getenv('AWS_REGION'),
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'))
        
        if (self.is_verbose):
            print("client loaded")
            print(aws_client)        
        
        # if any issues, it will throw an exception. If no exception
        # it is assumed to be valid.
        # we can not validate a client against the S3 interface
        #self.__validate_aws_credentials__('sts')
            
        # TODO: validate the connection
        # ?? https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-configuring-buckets.html
   
        return aws_client
   
