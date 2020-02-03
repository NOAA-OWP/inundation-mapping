#!/usr/bin/env python3
# -*- coding: utf-8

from raster import Raster
import numpy as np
import json
import sys
from tqdm import tqdm
import pandas as pd

"""
USAGE:
./inundateREM.py rem catchments stages
"""

rem_fileName = sys.argv[1]
catchments_fileName = sys.argv[2]
stages_fileName = sys.argv[3]

rem = Raster(rem_fileName)
catchments = Raster(catchments_fileName)
stages = pd.read_file(stages_fileName)
