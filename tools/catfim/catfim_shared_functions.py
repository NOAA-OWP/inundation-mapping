#!/usr/bin/env python3
 
import os
import logging
import pickle

import pandas as pd

from datetime import datetime, timezone

from dotenv import load_dotenv


def load_fim_global_env_values(env_file):
    '''
    Loads environment variables from a .env file.
    Expects the .env file to contain API_BASE_URL
    
    Parameters
    ----------
        env_file (str): Path to the .env file.

    '''
    if os.path.exists(env_file) == False:
        raise Exception(f"The environment file of {env_file} does not seem to exist")

    load_dotenv(env_file)
    # import variables from .env file
    api_base_url = os.getenv("API_BASE_URL")
    
    # At this point, we only have one value to return.
    return api_base_url


