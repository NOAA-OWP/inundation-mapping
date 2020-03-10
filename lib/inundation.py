#!/usr/bin/env python3

import numpy as np
from numba import njit, typeof, typed, types
import argparse
from raster import Raster
import pickle
import gc


def inundateREM(remFileName,catchmentsFileName,catchmentStageDictFileName):

    print("Loading files ..")
    catchments = Raster(catchmentsFileName)

    with open(catchmentStageDictFileName, 'rb') as handle:
        catchmentStagesDict = pickle.load(handle)

    # stageGrid

    catchmentsBoolean = catchments.array != catchments.ndv
    flat_catchments = catchments.array[catchmentsBoolean].ravel()
    flat_stage_grid = flat_catchments.copy()
    flat_stage_grid = flat_stage_grid.astype(np.float64)

    # convert to numba dictionary
    print("Numba Dictionary Conversion")
    d1_catchmentStagesDict = typed.Dict.empty(types.int32,types.float64)
    for k, v in catchmentStagesDict.items():
        d1_catchmentStagesDict[k] = v

    del catchmentStagesDict, catchments
    gc.collect()

    @njit
    def make_stage_grid(flat_stage_grid,catchmentStagesDict,flat_catchments):

        for i,cm in enumerate(flat_catchments):
            flat_stage_grid[i] = catchmentStagesDict[cm]

        return(flat_stage_grid)


    print('starting grid')
    flat_stage_grid = make_stage_grid(flat_stage_grid,d1_catchmentStagesDict,flat_catchments)

    print("Stage grid reshaping")
    rem = Raster(remFileName)

    stage_grid = rem.copy()
    stage_grid.array = stage_grid.array.astype(np.float32)
    stage_grid.ndv = -9999
    stage_grid.array[:] = stage_grid.ndv
    stage_grid.array[catchmentsBoolean] = flat_stage_grid

    del flat_stage_grid, flat_catchments
    gc.collect()

    print("Make depths grid")
    depths_grid = stage_grid.copy()

    stage_grid.writeRaster('tests/stages.tif')

    depths_grid.array[catchmentsBoolean] = stage_grid.array[catchmentsBoolean] - rem.array[catchmentsBoolean]

    del rem, stage_grid
    gc.collect()

    print("Make inundation grid")
    inundation_grid = depths_grid.copy()
    inundation_grid.array = inundation_grid.array.astype(np.int32)
    inundation_grid.ndv = 0
    inundation_grid.array[:] = inundation_grid.ndv
    inundatedLocations = np.logical_and(catchmentsBoolean,depths_grid.array > 0)
    nonInundatedLocations = np.logical_and(catchmentsBoolean,depths_grid.array <= 0)
    inundation_grid.array[nonInundatedLocations] = 1
    inundation_grid.array[inundatedLocations] = 2

    inundation_grid.writeRaster('tests/inundation.tif')
    # print("Polygonize inundation raster")
    # gdal_calc.py --overwrite -A tests/inundation.tif -B data/test2/outputs/gw_catchments_reaches_clipped.tif --calc="B*(A>1)" --NoDataValue=0 --outfile="tests/inundation_catchments.tif"
    # gdal_polygonize.py -8 -f GPKG tests/inundation_tests/catchments.tif inundation.gpkg inundation id
    # inundation_grid.array[nonInundatedLocations] = inundation_grid.ndv
    # inundation_grid.polygonize('tests/inundation.gpkg',vector_driver='GPKG',layer_name='inundation',verbose=True)

    del inundatedLocations, nonInundatedLocations, inundation_grid
    gc.collect()

    print("Zero out depths grid")
    negativeBoolean = np.logical_and(catchmentsBoolean,depths_grid.array < 0)
    depths_grid.array[negativeBoolean] = depths_grid.ndv
    depths_grid.writeRaster('tests/depths.tif')

    # flat_stage_grid = flat_stage_grid.reshape(catchments.nrows,catchments.ncols)

if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-r','--rem', help='DEM to use within project path', required=True)
    parser.add_argument('-c','--catchments',help='Basins polygons to use within project path',required=False,default=None)
    parser.add_argument('-d','--catchment-stages',help='Pixel based watersheds raster to use within project path',required=True)

    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    remFileName = args['rem']
    catchmentsFileName = args['catchments']
    catchmentStageDictFileName = args['catchment_stages']

    inundateREM(remFileName,catchmentsFileName,catchmentStageDictFileName)
