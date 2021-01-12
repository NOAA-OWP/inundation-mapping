#!/usr/bin/env python3

import os
import argparse
import geopandas as gpd
import pandas as pd
from os.path import splitext
from utils.shared_variables import PREP_PROJECTION
from derive_headwaters import findHeadWaterPoints
from tqdm import tqdm

in_dir ='/data/inputs/nhdplus_vectors'
nhd_dir ='/data/inputs/nhdplus_vectors_aggregate'
nwm_dir = '/data/inputs/nwm_hydrofabric'


def aggregate_vector_inputs(overwrite_nwm_headwaters=False):
    """
    This functions aggregates vector data for FIM production.

    Args:
        overwrite_nwm_headwaters (boolean): Optional flag to overwrite NWM headwaters data
    """


    ## NWM Headwaters
    if not os.path.isfile(os.path.join(nwm_dir, 'nwm_headwaters.gpkg')) or overwrite_nwm_headwaters:
        print ('deriving NWM headwater points')
        nwm_streams = gpd.read_file(os.path.join(nwm_dir,'nwm_flows.gpkg'))
        nwm_headwaters = findHeadWaterPoints(nwm_streams)
        nwm_headwaters.to_file(os.path.join(nwm_dir,'nwm_headwaters.gpkg'),driver='GPKG',index=False)
    else:
        print ('Using existing NWM headwater points')

    ## NHDPlus HR
    print ('aggregating NHDPlus HR burnline layers')
    nhd_streams_wVAA_fileName_pre=os.path.join(nhd_dir,'NHDPlusBurnLineEvent_wVAA.gpkg')

    schema = {'geometry': 'MultiLineString','properties': {'NHDPlusID': 'str','ReachCode': 'str',
                                                    'FromNode': 'str','ToNode': 'str',
                                                    'StreamOrde': 'str','DnLevelPat': 'str',
                                                    'LevelPathI': 'str'}}

    for huc in tqdm(os.listdir(in_dir)):
        if not huc[0]=='#':
            burnline_filename = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')
            vaa_filename = os.path.join(in_dir,huc,'NHDPlusFlowLineVAA' + str(huc) + '.gpkg')
            flowline_filename = os.path.join(in_dir,huc,'NHDFlowline' + str(huc) + '.gpkg')
            if os.path.exists(os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')):
                burnline = gpd.read_file(burnline_filename)
                nhd_streams_vaa = gpd.read_file(vaa_filename)
                flowline = gpd.read_file(flowline_filename)
                burnline = burnline[['NHDPlusID','ReachCode','geometry']]
                flowline = flowline[['NHDPlusID','FCode']]
                nhd_streams_vaa = nhd_streams_vaa[['FromNode','ToNode','NHDPlusID','StreamOrde','DnLevelPat','LevelPathI']]
                nhd_streams_withVAA = burnline.merge(nhd_streams_vaa,on='NHDPlusID',how='inner')
                nhd_streams_fcode = nhd_streams_withVAA.merge(flowline,on='NHDPlusID',how='inner')
                nhd_streams = nhd_streams_fcode.to_crs(PREP_PROJECTION)
                if os.path.isfile(nhd_streams_wVAA_fileName_pre):
                    nhd_streams.to_file(nhd_streams_wVAA_fileName_pre,driver='GPKG',index=False, mode='a')
                else:
                    nhd_streams.to_file(nhd_streams_wVAA_fileName_pre,driver='GPKG',index=False)
            else:
                print ('missing data for huc ' + str(huc))
        else:
            print ('skipping huc ' + str(huc))


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Aggegates vector inputs for use in fim_run.sh.')
    parser.add_argument('-o', '--overwrite-nwm-headwaters', help='Optional flag to overwrite NWM Headwaters Data',required=False,action='store_true')

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    aggregate_vector_inputs(**args)