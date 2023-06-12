#!/usr/bin/env python3

import os
import numpy as np
import pandas as pd
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile


# EBFE_urls_20230608.xlsx acquired from FEMA (fethomps@usgs.gov)

# NOTE: Need to convert .xlsx to .csv prior to running this script

save_folder = '/Users/matt/Downloads'

data = pd.read_csv('/Users/matt/Downloads/EBFE_urls_20230608.csv', header=None, names=['size', 'units', 'URL'])

# Subset Spatial Data URLs
spatial = data[data['URL'].str.contains('SpatialData')]

# Convert size to MiB
spatial['MiB'] = np.where(spatial['units']=='GiB', spatial['size'] * 1000, spatial['size'])

spatial = spatial.reset_index()

# Download and unzip each file
hucs = []
huc_names = []
for i, row in spatial.iterrows():
    # Extract HUC and HUC Name from URL
    huc, huc_name = os.path.basename(os.path.dirname(row['URL'])).split('_')
    hucs.append(huc)
    huc_names.append(huc_name)

    # Download and unzip file
    http_response = urlopen(row['URL'])
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=os.path.join(save_folder, os.path.basename(row['URL'])))

spatial['HUC'] = hucs
spatial['HUC_Name'] = huc_names

# TODO: Extract rasters from each GDB file (use arcpy)

# TODO: tools/create_flow_forecast_file.py

# TODO: tools/preprocess_benchmark.py