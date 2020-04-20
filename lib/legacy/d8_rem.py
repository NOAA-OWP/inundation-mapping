#!/usr/bin/env python3
# -*- coding: utf-8

from raster import Raster
import numpy as np
# import json
import sys
# from tqdm import tqdm
from multiprocessing import Pool #  Process pool
from multiprocessing import sharedctypes

"""
USAGE:
./d8_rem_v2.py dem reaches pixelCatchments rem
"""

dem = Raster(sys.argv[1])
reaches = Raster(sys.argv[2],dtype=np.bool)
# reaches = Raster(sys.argv[2])
pixelCatchments = Raster(sys.argv[3])
rem_fileName = sys.argv[4]
numOfProcesses = int(sys.argv[5])

rem = dem.copy()
rem.array = np.zeros((dem.nrows,dem.ncols),dtype=np.float32) + dem.ndv

uniqueCatchments = np.unique(pixelCatchments.array[pixelCatchments.array!=pixelCatchments.ndv])

result = np.ctypeslib.as_ctypes(rem.array)
shared_array = sharedctypes.RawArray(result._type_, result)


def getREM(ucat):

    tmp = np.ctypeslib.as_array(shared_array)

    indicesOfCatchment = pixelCatchments.array == ucat
    # thalwegElevationIndex = reaches.array == ucat
    thalwegElevationIndex = np.logical_and(indicesOfCatchment,reaches.array)

    demValues = dem.array[indicesOfCatchment]
    thalwegElevation = dem.array[thalwegElevationIndex]

    tmp[indicesOfCatchment] = demValues - thalwegElevation

p = Pool(processes=numOfProcesses)
res = p.map(getREM, uniqueCatchments)
result = np.ctypeslib.as_array(shared_array)
rem.array = result

rem.writeRaster(rem_fileName)
# print(np.array_equal(X, result))

# for ucat in tqdm(uniqueCatchments):
#
#     indicesOfCatchment = pixelCatchments.array == ucat
#     # thalwegElevationIndex = reaches.array == ucat
#     thalwegElevationIndex = np.logical_and(indicesOfCatchment,reaches.array)
#
#     demValues = dem.array[indicesOfCatchment]
#     thalwegElevation = dem.array[thalwegElevationIndex]
#
#     rem.array[indicesOfCatchment] = demValues - thalwegElevation
#
#
# rem.writeRaster(rem_fileName)



# indicesOfUniqueCatchments = np.where(pixelCatchments.array!=pixelCatchments.ndv)

# @njit
# def getDEMvalues(demArray,featureCatchmentsArray, uniqueCatchments):
#
#     demDict = {}
#     for uc in uniqueCatchments:
#         demValue = demArray[featureCatchmentsArray == uc]
#         demDict[uc] = demValue
#
#     return(demDict)
#
# print(np.searchsorted(dem.array.ravel(),uniqueCatchments.ravel()))
#
# # print(getDEMvalues(dem.array.ravel(),pixelCatchments.array.ravel(), uniqueCatchments))
#
# # print(uniqueCatchments)
# # print(indicesOfUniqueCatchments[0])
# # dictionaryUniqueCatchments = dict(zip(uniqueCatchments,indicesOfUniqueCatchments[0]))
# #print(dictionaryUniqueCatchments)
