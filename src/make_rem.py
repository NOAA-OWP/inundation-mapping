#!/usr/bin/env python3

import argparse
import os

import numpy as np
import rasterio
import rioxarray
from numba import njit, typed, types, prange


def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, pixel_catchments_elevation_filename, thalweg_raster):
    """
    Calculates REM/HAND/Detrended DEM

    Parameters
    ----------
    dem_fileName : str
        File name of pit filled DEM raster.
    pixel_watersheds_fileName : str
        File name of stream pixel watersheds raster.
    pixel_catchments_elevation_filename : str
        File name of pixel catchments thalweg elevation raster.
    rem_fileName : str
        File name of output relative elevation raster.

    """

    # --------------------------------- Get catchment_min_dict --------------------------------------------- #
    # The following creates a dictionary of the catchment ids (key) and
    # their elevation along the thalweg (value).

    @njit
    def make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments, thalweg_window):
        for i, cm in enumerate(flat_catchments):
            if thalweg_window[i] == 1:  # Only allow reference elevation to be within thalweg.
                # If the catchment really exists in the dictionary, compare elevation values.
                if cm in catchment_min_dict:
                    if flat_dem[i] < catchment_min_dict[cm]:
                        # If the flat_dem's elevation value is less than the catchment_min_dict min,
                        # update the catchment_min_dict min.
                        catchment_min_dict[cm] = flat_dem[i]
                else:
                    catchment_min_dict[cm] = flat_dem[i]
        return catchment_min_dict

    # Open the masked gw_catchments_pixels_masked and dem_thalwegCond_masked.
    gw_catchments_pixels_masked_object = rasterio.open(pixel_watersheds_fileName)
    dem_thalwegCond_masked_object = rasterio.open(dem_fileName)
    thalweg_raster_object = rasterio.open(thalweg_raster)

    # Specify raster object metadata.
    meta = dem_thalwegCond_masked_object.meta.copy()
    meta['tiled'], meta['compress'] = True, 'lzw'

    # -- Create catchment_min_dict -- #
    catchment_min_dict = typed.Dict.empty(
        types.int32, types.float32
    )  # Initialize an empty dictionary to store the catchment minimums.
    # Update catchment_min_dict with pixel sheds minimum.
    for ji, window in dem_thalwegCond_masked_object.block_windows(
        1
    ):  # Iterate over windows, using dem_rasterio_object as template.
        dem_window = dem_thalwegCond_masked_object.read(1, window=window).ravel()  # Define dem_window.
        catchments_window = gw_catchments_pixels_masked_object.read(
            1, window=window
        ).ravel()  # Define catchments_window.
        thalweg_window = thalweg_raster_object.read(1, window=window).ravel()  # Define cost_window.

        # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
        catchment_min_dict = make_catchment_min_dict(
            dem_window, catchment_min_dict, catchments_window, thalweg_window
        )

    @njit(parallel=True)
    def remap_raster_numba(raster_arr, d_mapping, nodata_val=np.nan):
        # Flatten or shape iteration safely
        out = np.full(raster_arr.shape, nodata_val, dtype=np.float32)
        
        # prange parallelizes across your CPU threads automatically
        for i in prange(raster_arr.shape[0]):
            for j in range(raster_arr.shape[1]):
                for k in range(raster_arr.shape[2]):
                    val = raster_arr[i, j, k]
                    if val in d_mapping:
                        out[i, j, k] = d_mapping[val]
        return out

    # Execute the JIT function (passes your typed.Dict seamlessly)
    raster = rioxarray.open_rasterio(pixel_watersheds_fileName)

    new_nodata_val = -999999
    raster.rio.write_nodata(new_nodata_val, inplace=True)

    remapped_data = remap_raster_numba(raster.values, catchment_min_dict)

    # Save output
    raster.values = remapped_data
    raster.rio.to_raster(pixel_catchments_elevation_filename)

    dem_thalwegCond_masked_object.close()
    gw_catchments_pixels_masked_object.close()
    thalweg_raster_object.close()
    # ------------------------------------------------------------------------------------------------------ #

    # --------------------------------- Produce relative elevation model ----------------------------------- #
    @njit
    def calculate_rem(flat_dem, catchmentMinDict, flat_catchments, ndv):
        rem_window = np.zeros(len(flat_dem), dtype=np.float32)
        for i, cm in enumerate(flat_catchments):
            if cm in catchmentMinDict:
                if catchmentMinDict[cm] == ndv or flat_dem[i] == ndv:
                    rem_window[i] = ndv
                else:
                    rem_window[i] = flat_dem[i] - catchmentMinDict[cm]

        return rem_window

    rem_rasterio_object = rasterio.open(
        rem_fileName, 'w', **meta
    )  # Open rem_rasterio_object for writing to rem_fileName.
    pixel_catchments_rasterio_object = rasterio.open(
        pixel_watersheds_fileName
    )  # Open pixel_catchments_rasterio_object
    dem_rasterio_object = rasterio.open(dem_fileName)

    for ji, window in dem_rasterio_object.block_windows(1):
        dem_window = dem_rasterio_object.read(1, window=window)
        window_shape = dem_window.shape

        dem_window = dem_window.ravel()
        catchments_window = pixel_catchments_rasterio_object.read(1, window=window).ravel()

        rem_window = calculate_rem(dem_window, catchment_min_dict, catchments_window, meta['nodata'])
        rem_window = rem_window.reshape(window_shape).astype(np.float32)

        rem_rasterio_object.write(rem_window, window=window, indexes=1)

    dem_rasterio_object.close()
    pixel_catchments_rasterio_object.close()
    rem_rasterio_object.close()
    # ------------------------------------------------------------------------------------------------------ #


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-d', '--dem', help='DEM to use within project path', required=True)
    parser.add_argument(
        '-w', '--watersheds', help='Pixel based watersheds raster to use within project path', required=True
    )
    parser.add_argument(
        '-e', '--pixel-catchments-elevation-filename', help='NWM pixel catchments elevation filename', required=True, type=str
    )
    parser.add_argument(
        '-t',
        '--thalweg-raster',
        help='A binary raster representing the thalweg. 1 for thalweg, 0 for non-thalweg.',
        required=True,
    )
    parser.add_argument('-o', '--rem', help='Output REM raster', required=True)

    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    dem_fileName = args['dem']
    pixel_watersheds_fileName = args['watersheds']
    rem_fileName = args['rem']
    thalweg_raster = args['thalweg_raster']
    pixel_catchments_elevation_filename = args['pixel_catchments_elevation_filename']

    rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, pixel_catchments_elevation_filename, thalweg_raster)
