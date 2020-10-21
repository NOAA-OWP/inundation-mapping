#!/usr/bin/env python3

from numba import njit, typeof, typed, types
import rasterio
import numpy as np
import argparse
import os
from osgeo import ogr, gdal


def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, thalweg_shapefile, thalweg_raster):
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
    
    # Get path to the outputs directory.
    outputs_dir = os.path.dirname(thalweg_shapefile)

    # -- Create mask of pixels 50m buffer around thalweg -- #
    as_thalweg = os.path.split(thalweg_shapefile)[1].replace('.shp', '')
    buffered_thalweg = os.path.join(outputs_dir, 'buffered_thalweg.shp')
    os.system('ogr2ogr -f "ESRI Shapefile" {buffered_thalweg} {thalweg_shapefile} -dialect sqlite -sql "select ST_buffer(geometry, 50.0) as geometry FROM {as_thalweg}"'.format(buffered_thalweg=buffered_thalweg, thalweg_shapefile=thalweg_shapefile, as_thalweg=as_thalweg))
    dem_thalwegCond = os.path.join(outputs_dir, 'dem_thalwegCond.tif')
    InputVector = buffered_thalweg
    buffered_thalweg_tif = os.path.join(outputs_dir, 'demDerived_reaches_buffer.tif')
    RefImage = dem_thalwegCond
    gdalformat, datatype, burnVal = 'GTiff', gdal.GDT_Byte, 1
    Image = gdal.Open(RefImage, gdal.GA_ReadOnly) # Get projection info from reference image
    Shapefile = ogr.Open(InputVector) # Open Shapefile
    Shapefile_layer = Shapefile.GetLayer()
    # Rasterise
    Output = gdal.GetDriverByName(gdalformat).Create(buffered_thalweg_tif, Image.RasterXSize, Image.RasterYSize, 1, datatype, options=['COMPRESS=DEFLATE'])
    Output.SetProjection(Image.GetProjectionRef())
    Output.SetGeoTransform(Image.GetGeoTransform()) 
    # Write data to band 1
    Band = Output.GetRasterBand(1)
    Band.SetNoDataValue(0)
    gdal.RasterizeLayer(Output, [1], Shapefile_layer, burn_values=[burnVal])
    Band, Output, Image, Shapefile = None, None, None, None  # Close datasets
    
    # Open mask layer.
    buffered_thalweg_object = rasterio.open(buffered_thalweg_tif)
    buffered_thalweg_array = buffered_thalweg_object.read(1)
    
    # Subset the dem_fileName to only the 50m buffered area.
    dem_thalwegCond_object = rasterio.open(dem_fileName)
    dem_thalwegCond_array = dem_thalwegCond_object.read(1)
    masked_dem_thalwegCond_array = np.where(buffered_thalweg_array==1, dem_thalwegCond_array, buffered_thalweg_object.nodata)
    
    # Write the subset masked_dem_thalwegCond_array to raster file.
    dem_thalwegCond_masked = os.path.join(outputs_dir, 'dem_thalwegCond_masked.tif')
    with rasterio.Env():
        profile = dem_thalwegCond_object.profile
        with rasterio.open(dem_thalwegCond_masked, 'w', **profile) as dst:
            dst.write(masked_dem_thalwegCond_array, 1)
    
    # Subset the pixel_watersheds_fileName to only the 50m buffered area.
    gw_catchments_pixels_object = rasterio.open(pixel_watersheds_fileName)
    gw_catchments_pixels_array = gw_catchments_pixels_object.read(1)
    masked_gw_catchments_pixels_array = np.where(buffered_thalweg_array==1, gw_catchments_pixels_array, gw_catchments_pixels_object.nodata)
    
    # Write the subset masked_gw_catchments_pixels_array to raster file.      
    gw_catchments_pixels_masked = os.path.join(outputs_dir, 'gw_catchments_pixels_masked.tif')
    with rasterio.Env():
        profile = gw_catchments_pixels_object.profile
        with rasterio.open(gw_catchments_pixels_masked, 'w', **profile) as dst2:
            dst2.write(masked_gw_catchments_pixels_array.astype(rasterio.int32), 1)
<<<<<<< HEAD
    
    
    # ------------------------------------------- Get catchment_min_dict --------------------------------------------------- #
    # The following algorithm searches for the zonal minimum elevation in each pixel catchment
    # It updates the catchment_min_dict with this zonal minimum elevation value.
    @njit
    def make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments):
  
        for i,cm in enumerate(flat_catchments):
            # If the catchment really exists in the dictionary, compare elevation values.
            if (cm in catchment_min_dict):
                if (flat_dem[i] < catchment_min_dict[cm]):
                    # If the flat_dem's elevation value is less than the catchment_min_dict min, update the catchment_min_dict min.
                    catchment_min_dict[cm] = flat_dem[i]                                                
            else:
                catchment_min_dict[cm] = flat_dem[i]                
        return(catchment_min_dict)
    
=======
    
    # ------------------------------------------- Get catchment_min_dict --------------------------------------------------- #

    # The following algorithm searches for the zonal minimum elevation in each pixel catchment
    # It updates the catchment_min_dict with this zonal minimum elevation value.
    @njit
    def make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments):
  
        for i,cm in enumerate(flat_catchments):
            # If the catchment really exists in the dictionary, compare elevation values.
            if (cm in catchment_min_dict):
                if (flat_dem[i] < catchment_min_dict[cm]):
                    # If the flat_dem's elevation value is less than the catchment_min_dict min, update the catchment_min_dict min.
                    catchment_min_dict[cm] = flat_dem[i]                                                
            else:
                catchment_min_dict[cm] = flat_dem[i]                
        return(catchment_min_dict)
    
>>>>>>> d6dc400e39f288dcc3324e0fdf1e2054a16933ff
    # Open the masked gw_catchments_pixels_masked and dem_thalwegCond_masked.
    gw_catchments_pixels_masked_object = rasterio.open(gw_catchments_pixels_masked)
    dem_thalwegCond_masked_object = rasterio.open(dem_thalwegCond_masked)
    
    # Specify raster object metadata.
    meta = dem_thalwegCond_masked_object.meta.copy()
    meta['tiled'], meta['compress'] = True, 'lzw'
        
    # -- Create catchment_min_dict -- #
    catchment_min_dict = typed.Dict.empty(types.int32,types.float32)  # Initialize an empty dictionary to store the catchment minimums.
    # Update catchment_min_dict with pixel sheds minimum.
    for ji, window in dem_thalwegCond_masked_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
         dem_window = dem_thalwegCond_masked_object.read(1,window=window).ravel()  # Define dem_window.
         catchments_window = gw_catchments_pixels_masked_object.read(1,window=window).ravel()  # Define catchments_window.

         # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
         catchment_min_dict = make_catchment_min_dict(dem_window, catchment_min_dict, catchments_window)
    
    dem_thalwegCond_masked_object.close()
    gw_catchments_pixels_masked_object.close()
    # ------------------------------------------------------------------------------------------------------------------------ #
    
    
    
    # ------------------------------------------- Assign zonal min to thalweg ------------------------------------------------ #
    @njit
    def minimize_thalweg_elevation(dem_window, catchment_min_dict, catchments_window, thalweg_window):
                
        # Copy elevation values into new array that will store the minimized elevation values.
        dem_window_to_return = np.empty_like (dem_window)
        dem_window_to_return[:] = dem_window
                       
        for i,cm in enumerate(catchments_window):
            
            thalweg_cell = thalweg_window[i]  # From flows_grid_boolean.tif (0s and 1s)
            if thalweg_cell == 1:  # Make sure thalweg cells are checked.
                
                zonal_min_elevation = catchment_min_dict[catchments_window[i]]  # The zonal minimum. Includes thalweg and beyond within zone.
                original_thalweg_elevation = dem_window[i]  # The original dem value at the thalweg.
                
                # Compute difference between zonal_min_elevation and original_thalweg_elevation.
                elevation_difference = original_thalweg_elevation - zonal_min_elevation
                
                # If the elevation_difference is less than 5 meters and if the original_thalweg_elevation is greater than the zonal
                # minimum, update the thalweg elevation value to m atch the zonal minimum elevation.
                if zonal_min_elevation < original_thalweg_elevation and elevation_difference < 5:
                    dem_window_to_return[i] = zonal_min_elevation

        return(dem_window_to_return)
<<<<<<< HEAD

        
    thalweg_object = rasterio.open(thalweg_raster)
    
    output_minimized_thalweg = os.path.join(outputs_dir, 'dem_minimized_thalweg.tif')
    minimized_thalweg_object = rasterio.open(output_minimized_thalweg, 'w', **meta)
    
    for ji, window in dem_thalwegCond_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
        dem_window = dem_thalwegCond_object.read(1,window=window)  # Define dem_window.
        window_shape = dem_window.shape

=======

        
    thalweg_object = rasterio.open(thalweg_raster)
    
    output_minimized_thalweg = os.path.join(outputs_dir, 'dem_minimized_thalweg.tif')
    minimized_thalweg_object = rasterio.open(output_minimized_thalweg, 'w', **meta)
    
    for ji, window in dem_thalwegCond_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
        dem_window = dem_thalwegCond_object.read(1,window=window)  # Define dem_window.
        window_shape = dem_window.shape

>>>>>>> d6dc400e39f288dcc3324e0fdf1e2054a16933ff
        dem_window = dem_window.ravel()
        
        catchments_window = gw_catchments_pixels_object.read(1,window=window).ravel()  # Define catchments_window.
        thalweg_window = thalweg_object.read(1,window=window).ravel()  # Define thalweg_window.
        
        # Call numba-optimized function to reassign thalweg cell values to catchment minimum value.
        minimized_dem_window = minimize_thalweg_elevation(dem_window, catchment_min_dict, catchments_window, thalweg_window)
        minimized_dem_window = minimized_dem_window.reshape(window_shape).astype(np.float32)


        minimized_thalweg_object.write(minimized_dem_window, window=window, indexes=1)    
    # ------------------------------------------------------------------------------------------------------------------------ #    
    
    
        
    # ------------------------------------------- Produce relative elevation model ------------------------------------------- #
    thalweg_object.close()
    minimized_thalweg_object.close()
    gw_catchments_pixels_object.close()
    
    @njit
    def calculate_rem(flat_dem,catchmentMinDict,flat_catchments,ndv):

        rem_window = np.zeros(len(flat_dem),dtype=np.float32)
        for i,cm in enumerate(flat_catchments):
            if catchmentMinDict[cm] == ndv:
                rem_window[i] = ndv
            else:
                rem_window[i] = flat_dem[i] - catchmentMinDict[cm]

<<<<<<< HEAD
        rem_window = np.where(rem_window < 0, 0, rem_window)

=======
>>>>>>> d6dc400e39f288dcc3324e0fdf1e2054a16933ff
        return(rem_window)


    rem_rasterio_object = rasterio.open(rem_fileName,'w',**meta)  # Open rem_rasterio_object for writing to rem_fileName.
    pixel_catchments_rasterio_object = rasterio.open(pixel_watersheds_fileName)  # Open pixel_catchments_rasterio_object
    dem_rasterio_object = rasterio.open(output_minimized_thalweg)
        
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
    parser.add_argument('-ts','--thalweg-shapefile',help='Thalweg shapefile to use within project path',required=True)
    parser.add_argument('-tr','--thalweg-raster',help='Thalweg raster to use within project path',required=True)
    parser.add_argument('-o','--rem',help='Output REM raster',required=True)
    
    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    dem_fileName = args['dem']
    pixel_watersheds_fileName = args['watersheds']
    rem_fileName = args['rem']
    thalweg_shapefile = args['thalweg_shapefile']
    thalweg_raster = args['thalweg_raster']

    rel_dem(dem_fileName, pixel_watersheds_fileName,rem_fileName, thalweg_shapefile, thalweg_raster)
