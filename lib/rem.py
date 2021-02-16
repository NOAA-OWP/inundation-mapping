#!/usr/bin/env python3

from numba import njit, typeof, typed, types
import rasterio
import numpy as np
import argparse
import os
import pandas as pd
from osgeo import ogr, gdal
import geopandas as gpd
from utils.shared_functions import getDriver


def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, thalweg_raster, hydroid_fileName, hand_ref_elev_fileName, dem_reaches_filename):
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
        hydroid_fileName : str
            File name of the hydroid raster (i.e. gw_catchments_reaches.tif)
        hand_ref_elev_fileName
            File name of the output csv containing list of hydroid values and HAND zero/reference elev
        dem_reaches_filename
            File name of the reaches layer to populate HAND elevation attribute values and overwrite as output

    """

    # ------------------------------------------- Get catchment_hydroid_dict --------------------------------------------------- #
    # The following creates a dictionary of the catchment ids (key) and their hydroid along the thalweg (value).
    # This is needed to produce a HAND zero reference elevation by hydroid dataframe (helpful for evaluating rating curves & bathy properties)
    @njit
    def make_catchment_hydroid_dict(flat_value_raster, catchment_hydroid_dict, flat_catchments, thalweg_window):

        for i,cm in enumerate(flat_catchments):
            if thalweg_window[i] == 1:  # Only allow reference hydroid to be within thalweg.
                catchment_hydroid_dict[cm] = flat_value_raster[i]
        return(catchment_hydroid_dict)

    # Open the masked gw_catchments_pixels_masked, hydroid_raster, and dem_thalwegCond_masked.
    gw_catchments_pixels_masked_object = rasterio.open(pixel_watersheds_fileName)
    hydroid_pixels_object = rasterio.open(hydroid_fileName)
    thalweg_raster_object = rasterio.open(thalweg_raster)

    # Specify raster object metadata.
    meta = hydroid_pixels_object.meta.copy()
    meta['tiled'], meta['compress'] = True, 'lzw'

    # -- Create catchment_hydroid_dict -- #
    catchment_hydroid_dict = typed.Dict.empty(types.int64,types.int64)  # Initialize an empty dictionary to store the catchment hydroid.
    # Update catchment_hydroid_dict with each pixel sheds hydroid.
    # Creating dictionary containing catchment ids (key) and corresponding hydroid within the thalweg...
    for ji, window in hydroid_pixels_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
        hydroid_window = hydroid_pixels_object.read(1,window=window).ravel()  # Define hydroid_window.
        catchments_window = gw_catchments_pixels_masked_object.read(1,window=window).ravel()  # Define catchments_window.
        thalweg_window = thalweg_raster_object.read(1, window=window).ravel()  # Define cost_window.

        # Call numba-optimized function to update catchment_hydroid_dict with pixel sheds overlapping hydroid.
        catchment_hydroid_dict = make_catchment_hydroid_dict(hydroid_window, catchment_hydroid_dict, catchments_window, thalweg_window)

    hydroid_pixels_object.close()
    gw_catchments_pixels_masked_object.close()
    thalweg_raster_object.close()
    # ------------------------------------------- Get catchment_min_dict --------------------------------------------------- #
    # The following creates a dictionary of the catchment ids (key) and their elevation along the thalweg (value).
    @njit
    def make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments, thalweg_window):

        for i,cm in enumerate(flat_catchments):
            if thalweg_window[i] == 1:  # Only allow reference elevation to be within thalweg.
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
    thalweg_raster_object = rasterio.open(thalweg_raster)

    # Specify raster object metadata.
    meta = dem_thalwegCond_masked_object.meta.copy()
    meta['tiled'], meta['compress'] = True, 'lzw'

    # -- Create catchment_min_dict -- #
    catchment_min_dict = typed.Dict.empty(types.int64,types.float32)  # Initialize an empty dictionary to store the catchment minimums.
    # Update catchment_min_dict with pixel sheds minimum.
    # Creating dictionary containing catchment ids (key) and corresponding elevation within the thalweg (value)...
    for ji, window in dem_thalwegCond_masked_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
        dem_window = dem_thalwegCond_masked_object.read(1,window=window).ravel()  # Define dem_window.
        catchments_window = gw_catchments_pixels_masked_object.read(1,window=window).ravel()  # Define catchments_window.
        thalweg_window = thalweg_raster_object.read(1, window=window).ravel()  # Define cost_window.

        # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
        catchment_min_dict = make_catchment_min_dict(dem_window, catchment_min_dict, catchments_window, thalweg_window)

    dem_thalwegCond_masked_object.close()
    gw_catchments_pixels_masked_object.close()
    thalweg_raster_object.close()

###############################################
    # Merge and export dictionary to to_csv
    catchment_min_dict_df = pd.DataFrame.from_dict(catchment_min_dict, orient='index') # convert dict to dataframe
    catchment_min_dict_df.columns = ['Min_Thal_Elev_meters']
    catchment_hydroid_dict_df = pd.DataFrame.from_dict(catchment_hydroid_dict, orient='index') # convert dict to dataframe
    catchment_hydroid_dict_df.columns = ['HydroID']
    merge_df = catchment_hydroid_dict_df.merge(catchment_min_dict_df, left_index=True, right_index=True)
    merge_df.index.name = 'pixelcatch_id'
    merge_df.to_csv(hand_ref_elev_fileName,index=True) # export dataframe to csv file

    # Merge the HAND reference elvation by HydroID dataframe with the demDerived_reaches layer (add new layer attribute)
    merge_df = merge_df.groupby(['HydroID']).median() # median value of all Min_Thal_Elev_meters for pixel catchments in each HydroID reach
    input_reaches = gpd.read_file(dem_reaches_filename)
    input_reaches = input_reaches.merge(merge_df, on='HydroID') # merge dataframes by HydroID variable
    input_reaches.to_file(dem_reaches_filename,driver=getDriver(dem_reaches_filename),index=False)
    # ------------------------------------------------------------------------------------------------------------------------ #


    # ------------------------------------------- Produce relative elevation model ------------------------------------------- #
    @njit
    def calculate_rem(flat_dem,catchmentMinDict,flat_catchments,ndv):
        rem_window = np.zeros(len(flat_dem),dtype=np.float32)
        for i,cm in enumerate(flat_catchments):
            if cm in catchmentMinDict:
                if catchmentMinDict[cm] == ndv:
                    rem_window[i] = ndv
                else:
                    rem_window[i] = flat_dem[i] - catchmentMinDict[cm]

        return(rem_window)

    rem_rasterio_object = rasterio.open(rem_fileName,'w',**meta)  # Open rem_rasterio_object for writing to rem_fileName.
    pixel_catchments_rasterio_object = rasterio.open(pixel_watersheds_fileName)  # Open pixel_catchments_rasterio_object
    dem_rasterio_object = rasterio.open(dem_fileName)

    # Producing relative elevation model raster
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
    parser.add_argument('-t','--thalweg-raster',help='A binary raster representing the thalweg. 1 for thalweg, 0 for non-thalweg.',required=True)
    parser.add_argument('-o','--rem',help='Output REM raster',required=True)
    parser.add_argument('-i','--hydroid', help='HydroID raster to use within project path', required=True)
    parser.add_argument('-r','--hand_ref_elev_table',help='Output table of HAND reference elev by catchment',required=True)
    parser.add_argument('-s','--dem_reaches_in_out',help='DEM derived reach layer to join HAND reference elevation attribute',required=True)


    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    dem_fileName = args['dem']
    pixel_watersheds_fileName = args['watersheds']
    rem_fileName = args['rem']
    thalweg_raster = args['thalweg_raster']
    hydroid_fileName = args['hydroid']
    hand_ref_elev_fileName = args['hand_ref_elev_table']
    dem_reaches_filename = args['dem_reaches_in_out']

    rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, thalweg_raster, hydroid_fileName, hand_ref_elev_fileName, dem_reaches_filename)
