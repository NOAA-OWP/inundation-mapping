#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import argparse
import sys
from tqdm import tqdm
from os.path import isfile
from os import remove

input_catchments_fileName = sys.argv[1]
input_flows_fileName = sys.argv[2]
output_catchments_fileName = sys.argv[3]

input_catchments = gpd.read_file(input_catchments_fileName)
input_flows = gpd.read_file(input_flows_fileName)

output_catchments = input_catchments.merge(input_flows.drop(['geometry'],axis=1),on='HydroID')

output_catchments.to_file(output_catchments_fileName)
