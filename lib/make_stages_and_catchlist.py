#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
import argparse
import sys
import decimal

flows_fileName = sys.argv[1]
catchments_fileName = sys.argv[2]
stages_fileName = sys.argv[3]
catchlist_fileName = sys.argv[4]
stages_min = float(sys.argv[5])
stages_interval = float(sys.argv[6])
stages_max = float(sys.argv[7])

flows = gpd.read_file(flows_fileName)
catchments = gpd.read_file(catchments_fileName)

# filter out smaller duplicate features
if catchments.HydroID.dtype != 'int': catchments.HydroID = catchments.HydroID.astype(int)
duplicateFeatures = np.where(np.bincount(catchments['HydroID'])>1)[0]

for dp in duplicateFeatures:

    indices_of_duplicate = np.where(catchments['HydroID'] == dp)[0]

    areas = catchments.iloc[indices_of_duplicate,:].geometry.area

    indices_of_smaller_duplicates = indices_of_duplicate[np.where(areas != np.amax(areas))[0]]

    catchments = catchments.drop(catchments.index[indices_of_smaller_duplicates])

# add geometry column
catchments['areasqkm'] = catchments.geometry.area/(1000**2)

hydroIDs = flows['HydroID'].tolist()
len_of_hydroIDs = len(hydroIDs)
slopes = flows['S0'].tolist()
lengthkm = flows['LengthKm'].tolist()
areasqkm = catchments['areasqkm'].tolist()


stages_max = stages_max + stages_interval
stages = np.round(np.arange(stages_min,stages_max,stages_interval),4)

with open(stages_fileName,'w') as f:
    f.write("Stage\n")
    for stage in stages:
        f.write("{}\n".format(stage))

with open(catchlist_fileName,'w') as f:
    f.write("{}\n".format(len_of_hydroIDs))
    for h,s,l,a in zip(hydroIDs,slopes,lengthkm,areasqkm):
        f.write("{} {} {} {}\n".format(h,s,l,a))
