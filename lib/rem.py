<<<<<<< HEAD
#!/usr/bin/env python3

from numba import njit, typeof, typed, types
import rasterio
import numpy as np
import argparse


def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName):
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

    dem_rasterio_object = rasterio.open(dem_fileName)
    pixel_catchments_rasterio_object = rasterio.open(pixel_watersheds_fileName)
    
    meta = dem_rasterio_object.meta.copy()
    meta['tiled'] = True
    meta['compress'] = 'lzw'
    
    @njit
    def make_catchment_min_dict(flat_dem,catchmentMinDict,flat_catchments):

        for i,cm in enumerate(flat_catchments):
            if (cm in catchmentMinDict):
                if (flat_dem[i] < catchmentMinDict[cm]):
                    catchmentMinDict[cm] = flat_dem[i]
            else:
                catchmentMinDict[cm] = flat_dem[i]

        return(catchmentMinDict)
    
    catchmentMinDict = typed.Dict.empty(types.int32,types.float32)
    
    # get pixel sheds minimum dictionary
    for ji, window in dem_rasterio_object.block_windows(1):
         dem_window = dem_rasterio_object.read(1,window=window).ravel()
         catchments_window = pixel_catchments_rasterio_object.read(1,window=window).ravel()

         catchmentMinDict = make_catchment_min_dict(dem_window,catchmentMinDict,catchments_window)
         
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

    rem_rasterio_object = rasterio.open(rem_fileName,'w',**meta)
    
    for ji, window in dem_rasterio_object.block_windows(1):
        dem_window = dem_rasterio_object.read(1,window=window)
        window_shape = dem_window.shape
        dem_window = dem_window.ravel()
        catchments_window = pixel_catchments_rasterio_object.read(1,window=window).ravel()
        
        rem_window = calculate_rem(dem_window,catchmentMinDict,catchments_window,meta['nodata'])
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
    parser.add_argument('-o','--rem',help='Output REM raster',required=True)
    
    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    dem_fileName = args['dem']
    pixel_watersheds_fileName = args['watersheds']
    rem_fileName = args['rem']

    rel_dem(dem_fileName, pixel_watersheds_fileName,rem_fileName)
=======
#!/usr/bin/env python3

from numba import njit, typeof, typed, types
import rasterio
import numpy as np
import argparse


def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName):
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

    dem_rasterio_object = rasterio.open(dem_fileName)
    pixel_catchments_rasterio_object = rasterio.open(pixel_watersheds_fileName)
    
    meta = dem_rasterio_object.meta.copy()
    meta['tiled'] = True
    meta['compress'] = 'lzw'
    
    @njit
    def make_catchment_min_dict(flat_dem,catchmentMinDict,flat_catchments):

        for i,cm in enumerate(flat_catchments):
            if (cm in catchmentMinDict):
                if (flat_dem[i] < catchmentMinDict[cm]):
                    catchmentMinDict[cm] = flat_dem[i]
            else:
                catchmentMinDict[cm] = flat_dem[i]

        return(catchmentMinDict)
    
    catchmentMinDict = typed.Dict.empty(types.int32,types.float32)
    
    # get pixel sheds minimum dictionary
    for ji, window in dem_rasterio_object.block_windows(1):
         dem_window = dem_rasterio_object.read(1,window=window).ravel()
         catchments_window = pixel_catchments_rasterio_object.read(1,window=window).ravel()

         catchmentMinDict = make_catchment_min_dict(dem_window,catchmentMinDict,catchments_window)
         
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

    rem_rasterio_object = rasterio.open(rem_fileName,'w',**meta)
    
    for ji, window in dem_rasterio_object.block_windows(1):
        dem_window = dem_rasterio_object.read(1,window=window)
        window_shape = dem_window.shape
        dem_window = dem_window.ravel()
        catchments_window = pixel_catchments_rasterio_object.read(1,window=window).ravel()
        
        rem_window = calculate_rem(dem_window,catchmentMinDict,catchments_window,meta['nodata'])
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
    parser.add_argument('-o','--rem',help='Output REM raster',required=True)
    
    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    dem_fileName = args['dem']
    pixel_watersheds_fileName = args['watersheds']
    rem_fileName = args['rem']

    rel_dem(dem_fileName, pixel_watersheds_fileName,rem_fileName)
>>>>>>> remotes/origin/foss-fim-batch-processing-eol_testing
