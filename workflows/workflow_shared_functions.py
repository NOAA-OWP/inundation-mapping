#!/usr/bin/env python3
import os


# Assumes the env file has been loaded into the os.environ objects
def get_value_from_env(arg_key, env_file_path, validate_local_path=True):
    '''
    Params:
        - arg_key is the variables in the loaded environment object
        - validate_path: if False, do not validate that the file exists
             Note: not all uses of this tool will be for file paths
             ** Only work
    Returns
        - The arg_key value. If do_except_in_error, this value will be empty.
    '''

    env_file_name = ""

    if arg_key is None or arg_key == "":
        raise Exception("arg key is missing or empty")

    if env_file_path is None or env_file_path == "":
        env_file_name = "Env file"
    if "/" in env_file_path:
        env_file_name = os.path.basename(env_file_path)

    arg_value = os.environ[arg_key]

    if arg_value is None or arg_value == "":
        raise ValueError(f"{env_file_name} : {arg_key} variable does not exist or empty")

    if validate_local_path and not os.path.exists(arg_value):
        raise ValueError(
            f"{env_file_name} : {arg_key} variable file path can not be found. Check path and/or case."
        )
    return arg_value


# Adds a starting and ending slash if not already there
def add_slashes_to_path(file_path):
    if not file_path.endswith("/"):
        file_path += "/"
    if not file_path.startswith("/"):
        file_path = "/" + file_path
    return file_path
