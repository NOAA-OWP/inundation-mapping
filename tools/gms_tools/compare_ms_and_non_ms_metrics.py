#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
                                   csi, mcc, tpr, far
from tools.shared_variables import TEST_CASES_DIR, OUTPUTS_DIR
from glob import glob
from itertools import product


def Compare_ms_and_non_ms_areas():
    
    return(None)




if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    #parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=True)
    

    #args = vars(parser.parse_args())
    
    Compare_ms_and_non_ms_areas()
