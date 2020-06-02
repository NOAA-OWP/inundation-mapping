#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
import argparse
import sys

input_catchments_fileName = sys.argv[1]
input_flows_fileName = sys.argv[2]
# input_majorities_fileName = sys.argv[3]
output_catchments_fileName = sys.argv[3]
# output_flows_fileName = sys.argv[4]

input_catchments = gpd.read_file(input_catchments_fileName)
input_flows = gpd.read_file(input_flows_fileName)
# input_majorities = gpd.read_file(input_majorities_fileName)

# input_majorities = input_majorities.rename(columns={'_majority' : 'feature_id'})
# input_majorities = input_majorities[:][input_majorities['feature_id'].notna()]
# input_majorities['feature_id'] = input_majorities['feature_id'].astype(int)

# merges input flows attributes and filters hydroids
output_catchments = input_catchments.merge(input_flows.drop(['geometry'],axis=1),on='HydroID')

# filter out smaller duplicate features
duplicateFeatures = np.where(np.bincount(output_catchments['HydroID'])>1)[0]
# print(duplicateFeatures)

for dp in duplicateFeatures:
    # print(dp)
    indices_of_duplicate = np.where(output_catchments['HydroID'] == dp)[0]
    # print(indices_of_duplicate)
    areas = output_catchments.iloc[indices_of_duplicate,:].geometry.area
    # print(areas)
    indices_of_smaller_duplicates = indices_of_duplicate[np.where(areas != np.amax(areas))[0]]
    # print(indices_of_smaller_duplicates)
    output_catchments = output_catchments.drop(output_catchments.index[indices_of_smaller_duplicates])

# output_catchments = output_catchments[:][output_catchments['feature_id'].notna()]
# output_catchments = output_catchments.merge(input_majorities[['HydroID','feature_id']],on='HydroID')

# output_flows = input_flows.merge(input_majorities[['HydroID','feature_id']],on='HydroID')

# print(len(output_catchments))
# print(len(np.unique(output_catchments['HydroID'])))

# add geometry column
output_catchments['areasqkm'] = output_catchments.geometry.area/(1000**2)

output_catchments.to_file(output_catchments_fileName, driver="GPKG",index=False)
# output_flows.to_file(output_flows_fileName, driver="GPKG", index=False)
