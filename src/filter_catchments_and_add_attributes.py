#!/usr/bin/env python3

import geopandas as gpd
import numpy as np
import sys
from utils.shared_variables import FIM_ID

input_catchments_fileName = sys.argv[1]
input_flows_fileName = sys.argv[2]
output_catchments_fileName = sys.argv[3]
output_flows_fileName = sys.argv[4]
wbd_fileName = sys.argv[5]
hucCode = str(sys.argv[6])

input_catchments = gpd.read_file(input_catchments_fileName)
wbd = gpd.read_file(wbd_fileName)
input_flows = gpd.read_file(input_flows_fileName)

# filter segments within huc boundary
select_flows = tuple(map(str,map(int,wbd[wbd.HUC8.str.contains(hucCode)][FIM_ID])))

if input_flows.HydroID.dtype != 'str': input_flows.HydroID = input_flows.HydroID.astype(str)
output_flows = input_flows[input_flows.HydroID.str.startswith(select_flows)].copy()
if output_flows.HydroID.dtype != 'int': output_flows.HydroID = output_flows.HydroID.astype(int)

if len(output_flows) > 0:
    
    # merges input flows attributes and filters hydroids
    if input_catchments.HydroID.dtype != 'int': input_catchments.HydroID = input_catchments.HydroID.astype(int)
    output_catchments = input_catchments.merge(output_flows.drop(['geometry'],axis=1),on='HydroID')

    # filter out smaller duplicate features
    duplicateFeatures = np.where(np.bincount(output_catchments['HydroID'])>1)[0]

    for dp in duplicateFeatures:

        indices_of_duplicate = np.where(output_catchments['HydroID'] == dp)[0]
        areas = output_catchments.iloc[indices_of_duplicate,:].geometry.area
        indices_of_smaller_duplicates = indices_of_duplicate[np.where(areas != np.amax(areas))[0]]
        output_catchments = output_catchments.drop(output_catchments.index[indices_of_smaller_duplicates])

    # add geometry column
    output_catchments['areasqkm'] = output_catchments.geometry.area/(1000**2)

    output_catchments.to_file(output_catchments_fileName, driver="GPKG",index=False)
    output_flows.to_file(output_flows_fileName, driver="GPKG", index=False)
    
#TODO We need a main block
