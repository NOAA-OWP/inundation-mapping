#!/usr/bin/env python3
import os

# Assumes the env file has been loaded into the os.environ objects
def get_value_from_env(arg_key, validate_path=True):

    '''
    Params:
        - arg_key is the variables in the loaded environment object
        - validate_path: if False, do not validate that the file exists
             Note: not all uses of this tool will be for file paths
    Returns
        - The arg_key value. If do_except_in_error, this value will be empty.
    '''

    if arg_key is None or arg_key == "":
        raise Exception("arg key is missing or empty")

    arg_value = os.environ[arg_key]

    if arg_value is None or arg_value == "":
        raise ValueError(f"workflow env file : {arg_key} variable does not exist or empty")
    
    if validate_path and not os.path.exists(arg_value):
        raise ValueError(f"workflow env file : {arg_key} variable"
                            " file path can not be found. Check path and/or case.")
    return arg_value