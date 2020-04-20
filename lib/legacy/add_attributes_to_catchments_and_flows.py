#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import argparse
import sys

input_catchments_fileName = sys.argv[1]
input_flows_fileName = sys.argv[2]
input_majorities_fileName = sys.argv[3]
output_catchments_fileName = sys.argv[4]
output_flows_fileName = sys.argv[5]

input_catchments = gpd.read_file(input_catchments_fileName)
input_flows = gpd.read_file(input_flows_fileName)
input_majorities = gpd.read_file(input_majorities_fileName)

input_majorities = input_majorities.rename(columns={'_majority' : 'feature_id'})
input_majorities = input_majorities[:][input_majorities['feature_id'].notna()]
input_majorities['feature_id'] = input_majorities['feature_id'].astype(int)

# output_catchments = input_catchments.merge(input_flows.drop(['geometry'],axis=1),on='HydroID')
output_catchments = output_catchments.merge(input_majorities[['HydroID','feature_id']],on='HydroID')

output_flows = input_flows.merge(input_majorities[['HydroID','feature_id']],on='HydroID')


output_catchments.to_file(output_catchments_fileName, driver="GPKG",index=False)
output_flows.to_file(output_flows_fileName, driver="GPKG", index=False)
