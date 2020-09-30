#!/usr/bin/env·python3

import os
import geopandas as gpd
import pandas as pd
from os.path import splitext
from utils.shared_variables import PREP_PROJECTION
from derive_headwaters import findHeadWaterPoints

in_dir ='data/inputs/nhdplus_vectors'
nhd_dir ='data/inputs/nhdplus_vectors_aggregate'
nwm_dir = 'data/inputs/nwm_hydrofabric'

## NWM Headwaters
print ('deriving NWM headwater points')
nwm_streams = gpd.read_file(os.path.join(nwm_dir,'nwm_flows.gpkg'))
nwm_headwaters = findHeadWaterPoints(nwm_streams)
nwm_headwaters.to_file(os.path.join(nwm_dir,'nwm_headwaters.gpkg'),driver='GPKG',index=False)

## NHDPlus HR
print ('aggregating NHDPlus HR burnline layers')
nhd_streams_wVAA_fileName_pre=os.path.join(nhd_dir,'NHDPlusBurnLineEvent_wVAA.gpkg')

nhd_streams_CONUS = pd.DataFrame({"NHDPlusID": pd.Series([], dtype='object'),
                                  "ReachCode": pd.Series([], dtype='object'),
                                  "geometry": pd.Series([], dtype='object')})

nhd_streams_vaa_CONUS = pd.DataFrame({"NHDPlusID": pd.Series([], dtype='object'),
                                      "StreamOrde": pd.Series([], dtype='object'),
                                      "FromNode": pd.Series([], dtype='object'),
                                      "ToNode": pd.Series([], dtype='object'),
                                      "DnLevelPat": pd.Series([], dtype='object'),
                                      "LevelPathI": pd.Series([], dtype='object')})

for huc in os.listdir(in_dir):
    if not huc[0]=='#':
        burnline_filename = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')
        vaa_filename = os.path.join(in_dir,huc,'NHDPlusFlowLineVAA' + str(huc) + '.gpkg')
        if os.path.exists(os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')):
            burnline = gpd.read_file(burnline_filename)
            nhd_streams_vaa = gpd.read_file(vaa_filename)
            nhd_streams_CONUS = nhd_streams_CONUS.append(burnline[['NHDPlusID','ReachCode','geometry']])
            nhd_streams_vaa_CONUS = nhd_streams_vaa_CONUS.append(nhd_streams_vaa[['FromNode','ToNode','NHDPlusID','StreamOrde','DnLevelPat','LevelPathI']])
        else:
            print ('missing data for huc ' + str(huc))
    else:
        print ('skipping huc ' + str(huc))

nhd_streams_CONUS_withVAA = nhd_streams_CONUS.merge(nhd_streams_vaa_CONUS,on='NHDPlusID',how='inner')
nhd_streams = gpd.GeoDataFrame(nhd_streams_CONUS_withVAA, geometry=nhd_streams_CONUS_withVAA.geometry, crs=PREP_PROJECTION)
nhd_streams.to_file(nhd_streams_wVAA_fileName_pre,driver='GPKG',index=False)
