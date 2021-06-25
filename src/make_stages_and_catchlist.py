#!/usr/bin/env python3

import geopandas as gpd
import numpy as np
import sys

flows_fileName = sys.argv[1]
catchments_fileName = sys.argv[2]
stages_fileName = sys.argv[3]
catchlist_fileName = sys.argv[4]
stages_min = float(sys.argv[5])
stages_interval = float(sys.argv[6])
stages_max = float(sys.argv[7])

flows = gpd.read_file(flows_fileName)
catchments = gpd.read_file(catchments_fileName)


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

#TODO we need a main block