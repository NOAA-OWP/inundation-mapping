#!/usr/bin/env python3

from numba import njit, typeof, typed, types
import rasterio
import numpy as np
import argparse
import os
from osgeo import ogr, gdal


def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, cost_distance_raster, cost_distance_tolerance):
    """
        Calculates REM/HAND/Detrended DEM
        
        Parameters
        ----------
        dem_fileName : str
            File name of pit filled DEM raster.
        pixel_watersheds_fileName : str
            File name of stream pixel watersheds raster.
        rem_fileName : str
            File name of output relative elevation raster.

    """

    
    # ------------------------------------------- Get catchment_min_dict --------------------------------------------------- #
    # The following algorithm searches for the zonal minimum elevation in each pixel catchment
    # It updates the catchment_min_dict with this zonal minimum elevation value.
    @njit
    def make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments, cost_window, cost_tolerance):
  
        for i,cm in enumerate(flat_catchments):
            if cost_window[i] <= cost_tolerance:  # Only allow reference elevation to be within 50m of thalweg.
                # If the catchment really exists in the dictionary, compare elevation values.
                if (cm in catchment_min_dict):
                    if (flat_dem[i] < catchment_min_dict[cm]):
                        # If the flat_dem's elevation value is less than the catchment_min_dict min, update the catchment_min_dict min.
                        catchment_min_dict[cm] = flat_dem[i]                                                
                else:
                    catchment_min_dict[cm] = flat_dem[i]                
        return(catchment_min_dict)
    

    # Open the masked gw_catchments_pixels_masked and dem_thalwegCond_masked.
    gw_catchments_pixels_masked_object = rasterio.open(pixel_watersheds_fileName)
    dem_thalwegCond_masked_object = rasterio.open(dem_fileName)
    cost_distance_raster_object = rasterio.open(cost_distance_raster)
    
    # Specify raster object metadata.
    meta = dem_thalwegCond_masked_object.meta.copy()
    meta['tiled'], meta['compress'] = True, 'lzw'
        
    cost_tolerance = float(cost_distance_tolerance)
    
    # -- Create catchment_min_dict -- #
    catchment_min_dict = typed.Dict.empty(types.int32,types.float32)  # Initialize an empty dictionary to store the catchment minimums.
    # Update catchment_min_dict with pixel sheds minimum.
    for ji, window in dem_thalwegCond_masked_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
        dem_window = dem_thalwegCond_masked_object.read(1,window=window).ravel()  # Define dem_window.
        catchments_window = gw_catchments_pixels_masked_object.read(1,window=window).ravel()  # Define catchments_window.
        cost_window = cost_distance_raster_object.read(1, window=window).ravel()  # Define cost_window.
        
        # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
        catchment_min_dict = make_catchment_min_dict(dem_window, catchment_min_dict, catchments_window, cost_window, cost_tolerance)
    
    dem_thalwegCond_masked_object.close()
    gw_catchments_pixels_masked_object.close()
    cost_distance_raster_object.clost()
    # ------------------------------------------------------------------------------------------------------------------------ #
    
    
    # ------------------------------------------- Produce relative elevation model ------------------------------------------- #
    @njit
    def calculate_rem(flat_dem,catchmentMinDict,flat_catchments,ndv):

        rem_window = np.zeros(len(flat_dem),dtype=np.float32)
        for i,cm in enumerate(flat_catchments):
            if catchmentMinDict[cm] == ndv:
                rem_window[i] = ndv
            else:
                rem_window[i] = flat_dem[i] - catchmentMinDict[cm]

        return(rem_window)

    rem_rasterio_object = rasterio.open(rem_fileName,'w',**meta)  # Open rem_rasterio_object for writing to rem_fileName.
    pixel_catchments_rasterio_object = rasterio.open(pixel_watersheds_fileName)  # Open pixel_catchments_rasterio_object
    dem_rasterio_object = rasterio.open(dem_fileName)
        
    for ji, window in dem_rasterio_object.block_windows(1):
        dem_window = dem_rasterio_object.read(1,window=window)
        window_shape = dem_window.shape

        dem_window = dem_window.ravel()
        catchments_window = pixel_catchments_rasterio_object.read(1,window=window).ravel()
        
        rem_window = calculate_rem(dem_window, catchment_min_dict, catchments_window, meta['nodata'])
        rem_window = rem_window.reshape(window_shape).astype(np.float32)
        
        rem_rasterio_object.write(rem_window, window=window, indexes=1)
        
    dem_rasterio_object.close()
    pixel_catchments_rasterio_object.close()
    rem_rasterio_object.close()
    # ------------------------------------------------------------------------------------------------------------------------ #


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-d','--dem', help='DEM to use within project path', required=True)
    parser.add_argument('-w','--watersheds',help='Pixel based watersheds raster to use within project path',required=True)
    parser.add_argument('-d','--cost_distance_raster',help='Raster of cost distances for the allocation raster.',required=True)
    parser.add_argument('-t','--cost_distance_tolerance',help='Tolerance in meters to use when searching for zonal minimum.',required=True)
    parser.add_argument('-o','--rem',help='Output REM raster',required=True)
    
    
    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    dem_fileName = args['dem']
    pixel_watersheds_fileName = args['watersheds']
    rem_fileName = args['rem']
    cost_distance_raster = args['cost_distance_raster']
    cost_distance_tolerance = args['cost_distance_tolerance']

    rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, cost_distance_raster, cost_distance_tolerance)
