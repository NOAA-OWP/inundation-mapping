#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
from glob import glob
import argparse
from os.path import join
from tqdm import tqdm


def find_unique_hucs(inputsDir,hucLength):

    # get file list with glob
    huc4_vaa_list = glob(join(inputsDir,'NHDPlusFlowlineVAA_*.gpkg'))

    unique_hucs = np.array([])
    for vaa_file in tqdm(huc4_vaa_list):
        reachCodes = gpd.read_file(vaa_file)['ReachCode']
        reachCodes = reachCodes.astype(str)
        reachCodes = reachCodes.apply(lambda x : x[0:8])
        unique_hucs = np.append(unique_hucs,reachCodes.apply(lambda x: x[0:hucLength]).unique())
    
    unique_hucs = pd.Series(unique_hucs)
    unique_hucs.to_csv(join(inputsDir,'included_huc{}.lst'.format(hucLength)),header=False,index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get unique HUCs in results data dir')
    parser.add_argument('-i','--inputs-directory',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-l','--huc-length',help='Basins polygons to use within proiject path',required=True,type=int)

    args = vars(parser.parse_args())

    inputsDir = args['inputs_directory']
    hucLength = args['huc_length']

    find_unique_hucs(inputsDir,hucLength)
