#!/usr/bin/env python3

import os
import sys
import re
sys.path.append('/foss_fim/data')
import argparse
import requests
from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS
from tools_shared_variables import INPUTS_DIR
import pandas as pd
import geopandas as gpd
import py7zr

out_dir = os.path.join(INPUTS_DIR, 'nhdplus_vectors')
epsg_code = re.search('\d+$', DEFAULT_FIM_PROJECTION_CRS).group()


def acquire_sinks():
    """
    Downloads NHDPlus sinks data from EPA.
    """

    out_dir = os.path.join(INPUTS_DIR, 'nhdplus_vectors')

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    # Download links from https://www.epa.gov/waterdata/nhdplus-national-data
    url_root = 'https://edap-ow-data-commons.s3.amazonaws.com/NHDPlusV21/Data/NationalData/'
    url_basename = 'NHDPlusV21_NationalData_Seamless_Geodatabase'
    filename = 'NHDPlusV21_National_Seamless_Flattened'
    layer = 'Sink'

    urls = [('HI_PR_VI_PI', '03'),
            ('Lower48', '07')]

    for i, (region, version) in enumerate(urls):
        url_filename = f'{url_basename}_{region}_{version}.7z'
        url = f'{url_root}{url_filename}'

        out_filename = os.path.join(out_dir, url_filename)
        out_filename_gdb = os.path.join(out_dir, 'NHDPlusNationalData', f'{filename}_{region}.gdb')

        # Download file
        if not os.path.exists(out_filename):
            print('Downloading ' + out_filename)
            with open(out_filename, 'wb') as out_file:
                content = requests.get(url, stream=True).content
                out_file.write(content)
        else:
            print(out_filename + ' already exists')

        # Extract files
        print('Extracting ' + out_filename)
        with py7zr.SevenZipFile(out_filename, mode='r') as z:
            z.extractall(path=out_dir)

        print(f'Reading and reprojecting to {DEFAULT_FIM_PROJECTION_CRS}')
        if i == 0:
            data = gpd.read_file(out_filename_gdb, layer=layer).to_crs(DEFAULT_FIM_PROJECTION_CRS)

        elif i > 0:
            data = gpd.GeoDataFrame(pd.concat([data, gpd.read_file(out_filename_gdb, layer=layer).to_crs(DEFAULT_FIM_PROJECTION_CRS)], ignore_index=True), crs=DEFAULT_FIM_PROJECTION_CRS)

    # Save Sinks layer
    data.to_file(os.path.join(os.path.dirname(out_filename_gdb), f'{filename}_{layer}.gpkg'), index=False, driver='GPKG', crs=DEFAULT_FIM_PROJECTION_CRS, layer=layer)


if __name__ == '__main__':

    acquire_sinks()