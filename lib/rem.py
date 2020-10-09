#!/usr/bin/env python3

from numba import njit, typeof, typed, types
import rasterio
import numpy as np
import argparse
import os

def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, thalweg):
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
    
    
    @njit
    def make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments):
  
        for i,cm in enumerate(flat_catchments):
            if (cm in catchment_min_dict):
                if (flat_dem[i] < catchment_min_dict[cm]):
                    catchment_min_dict[cm] = flat_dem[i]
            else:
                catchment_min_dict[cm] = flat_dem[i]

        return(catchment_min_dict)
    

    # create rem_fileName grid 
    
    @njit
    def calculate_rem(flat_dem,catchmentMinDict,flat_catchments,ndv):

        rem_window = np.zeros(len(flat_dem),dtype=np.float32)
        for i,cm in enumerate(flat_catchments):
            if catchmentMinDict[cm] == ndv:
                rem_window[i] = ndv
            else:
                rem_window[i] = flat_dem[i] - catchmentMinDict[cm]

        return(rem_window)

    # -- Main Function Block -- #
    
    print("Buffering...")
    print("Subsetting the DEM to 50m buffer on either side of thalweg...........")
    
    outputs_dir = os.path.dirname(thalweg)

    thalweg = os.path.join(outputs_dir, 'demDerived_reaches.shp')
    as_thalweg = os.path.split(thalweg)[1].replace('.shp', '')
    
    # -- Create mask of pixels 50m buffer around thalweg -- #
    buffered_thalweg = os.path.join(outputs_dir, 'buffered.shp')
    
    os.system('ogr2ogr -f "ESRI Shapefile" {buffered_thalweg} {thalweg} -dialect sqlite -sql "select ST_buffer(geometry, 50.0) as geometry FROM {as_thalweg}"'.format(buffered_thalweg=buffered_thalweg, thalweg=thalweg, as_thalweg=as_thalweg))
    dem_thalwegCond = os.path.join(outputs_dir, 'dem_thalwegCond.tif')
    
    from osgeo import ogr, gdal
    
    InputVector = buffered_thalweg
    buffered_thalweg_tif = os.path.join(outputs_dir, 'demDerived_reaches_buffer.tif')
    
    RefImage = dem_thalwegCond
    
    gdalformat = 'GTiff'
    datatype = gdal.GDT_Byte
    burnVal = 1 #value for the output image pixels
    # Get projection info from reference image
    Image = gdal.Open(RefImage, gdal.GA_ReadOnly)
    
    # Open Shapefile
    Shapefile = ogr.Open(InputVector)
    Shapefile_layer = Shapefile.GetLayer()
    
    # Rasterise
    print("Rasterising shapefile...")
    Output = gdal.GetDriverByName(gdalformat).Create(buffered_thalweg_tif, Image.RasterXSize, Image.RasterYSize, 1, datatype, options=['COMPRESS=DEFLATE'])
    Output.SetProjection(Image.GetProjectionRef())
    Output.SetGeoTransform(Image.GetGeoTransform()) 
    
    # Write data to band 1
    Band = Output.GetRasterBand(1)
    Band.SetNoDataValue(0)
    gdal.RasterizeLayer(Output, [1], Shapefile_layer, burn_values=[burnVal])
    
    # Close datasets
    Band = None
    Output = None
    Image = None
    Shapefile = None
    
    # Open mask layer.
    buffered_thalweg_object = rasterio.open(buffered_thalweg_tif)
    buffered_thalweg_array = buffered_thalweg_object.read(1)
    
    # Open dem_fileName.
    dem_thalwegCond_object = rasterio.open(dem_fileName)
    dem_thalwegCond_array = dem_thalwegCond_object.read(1)

    gw_catchments_pixels_object = rasterio.open(pixel_watersheds_fileName)
    gw_catchments_pixels_array = gw_catchments_pixels_object.read(1)
        
    print(buffered_thalweg_array.shape)
    print(dem_thalwegCond_array.shape)
    
    # Subset the dem_thalwegCond_array and gw_catchments_pixels_array to only the 50m buffered area.
    masked_dem_thalwegCond_array = np.where(buffered_thalweg_array==1, dem_thalwegCond_array, buffered_thalweg_object.nodata)
    masked_gw_catchments_pixels_array = np.where(buffered_thalweg_array==1, gw_catchments_pixels_array, gw_catchments_pixels_object.nodata)
    dem_thalwegCond_masked = os.path.join(outputs_dir, 'dem_thalwegCond_masked.tif')
    with rasterio.Env():
        profile = dem_thalwegCond_object.profile
        with rasterio.open(dem_thalwegCond_masked, 'w', **profile) as dst:
            dst.write(masked_dem_thalwegCond_array, 1)
    
    gw_catchments_pixels_masked = os.path.join(outputs_dir, 'gw_catchments_pixels_masked.tif')
    print(os.path.exists(gw_catchments_pixels_masked))
    with rasterio.Env():
        profile = gw_catchments_pixels_object.profile
        print(profile)
        with rasterio.open(gw_catchments_pixels_masked, 'w', **profile) as dst2:
            dst2.write(masked_gw_catchments_pixels_array.astype(rasterio.uint32), 1)
    
    gw_catchments_pixels_masked_object = rasterio.open(gw_catchments_pixels_masked)
    dem_thalwegCond_masked_object = rasterio.open(dem_thalwegCond_masked)
    
    # Specify raster object metadata.
    meta = dem_thalwegCond_masked_object.meta.copy()
    meta['tiled'] = True
    meta['compress'] = 'lzw'
    
    # Initialize an empty dictionary to store the catchment minimums.
    catchment_min_dict = typed.Dict.empty(types.int32,types.float32)
    
    # Update catchment_min_dict with pixel sheds minimum.
    for ji, window in dem_thalwegCond_masked_object.block_windows(1):  # Iterate over windows, using dem_rasterio_object as template.
         dem_window = dem_thalwegCond_masked_object.read(1,window=window).ravel()  # Initialize dem_window.
         catchments_window = gw_catchments_pixels_masked_object.read(1,window=window).ravel()  # Initialize catchments_window.

        # Call numba-optimized function to update catchment_min_dict with pixel sheds minimum.
         catchment_min_dict = make_catchment_min_dict(dem_window, catchment_min_dict, catchments_window)



    # Open rem_rasterio_object for writing to rem_fileName.
    rem_rasterio_object = rasterio.open(rem_fileName,'w',**meta)
    
    dem_rasterio_object = rasterio.open(dem_fileName)
    pixel_catchments_rasterio_object = rasterio.open(pixel_watersheds_fileName)
    
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


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-d','--dem', help='DEM to use within project path', required=True)
    parser.add_argument('-w','--watersheds',help='Pixel based watersheds raster to use within project path',required=True)
    parser.add_argument('-t','--thalweg',help='Thalweg raster to use within project path',required=True)
    parser.add_argument('-o','--rem',help='Output REM raster',required=True)
    
    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    dem_fileName = args['dem']
    pixel_watersheds_fileName = args['watersheds']
    rem_fileName = args['rem']
    thalweg = args['thalweg']

    rel_dem(dem_fileName, pixel_watersheds_fileName,rem_fileName, thalweg)
