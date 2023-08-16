#!/usr/bin/env python3

import argparse
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob
from itertools import product

import numpy as np
import pandas as pd
from tools_shared_functions import csi, far, mcc, tpr
from tqdm import tqdm

from tools.shared_variables import OUTPUTS_DIR, TEST_CASES_DIR


def Compare_ms_and_non_ms_areas():
    return None


if __name__ == "__main__":
    # Parse arguments.
    parser = argparse.ArgumentParser(description="Caches metrics from previous versions of HAND.")
    # parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=True)

    # args = vars(parser.parse_args())

    Compare_ms_and_non_ms_areas()
