#!/usr/bin/env python3
# -*- coding: utf-8

from raster import Raster
import numpy as np
import json
import sys
from tqdm import tqdm


"""
USAGE:
./d8_rem_v2.py dem reachesBoolean featureCatchments rem
"""

dem = Raster(sys.argv[1])
reachesBoolean = Raster(sys.argv[2],dtype=np.bool)
featureCatchments = Raster(sys.argv[3])
rem_fileName = sys.argv[4]

rem = dem.copy()
rem.array = np.zeros((dem.nrows,dem.ncols),dtype=np.float32) + dem.ndv

uniqueCatchments = np.unique(featureCatchments.array[featureCatchments.array!=featureCatchments.ndv])

for ucat in tqdm(uniqueCatchments):

    indicesOfCatchment = featureCatchments.array == ucat
    thalwegElevationIndex = np.logical_and(indicesOfCatchment,reachesBoolean.array)

    demValues = dem.array[indicesOfCatchment]
    thalwegElevation = dem.array[thalwegElevationIndex]

    rem.array[indicesOfCatchment] = demValues - thalwegElevation


rem.writeRaster(rem_fileName)
