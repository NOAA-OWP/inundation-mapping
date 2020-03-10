#!/usr/bin/env python3

import gc
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio import features
import numpy as np
from numba import njit
import argparse


def rel_dem(res_flat_dem, stream_px_ws, rem, basins=None,ndv=-9999):
    """Creates a rasterized catchment mask, "ws_raster.tif", and a relative height DEM, "rel_dem.tif", based on the watersheds calculated for each stream pixel.

        Parameters
        ----------
        res_flat_dem : str
            File name of pit filled and flat resolved elevation DEM raster.
        basins : str
            File name of NHDPlusV2 catchment vector. (Default = None)
        stream_px_ws : str
            File name of stream pixel watersheds raster.
        rem : str
            File name of output relative elevation raster.

    """

    # Open up the flat resolved elevation DEM, get its shape ans transform
    # and flattened data

    res_flat = rasterio.open(res_flat_dem)
    # res_flat_arr = res_flat.read(1).astype(np.int)
    res_flat_arr = res_flat.read(1).astype(np.float32)
    res_flat_shape = res_flat_arr.shape
    res_flat_transform = res_flat.transform
    meta = res_flat.meta.copy()
    flattened_arr = res_flat_arr.flatten()

    del res_flat
    del res_flat_arr

    if basins is not None:
        # Load catchment vector and extract its geomtry and desired attribute value
        basin_df = gpd.read_file(basins)
        shapes = ((geom, value) for geom, value in zip(basin_df.geometry, basin_df['FEATUREID']))

        # del basin_df

        # Rasterize the vector geometries and save catchment mask raster
        basin_rast = features.rasterize(shapes=shapes, fill=0, out_shape=res_flat_shape,
                                        transform=res_flat_transform)


        meta.update(dtype=rasterio.uint32)
        meta.update(nodata=0)
        ws_raster = '/ws_raster.tif'
        with rasterio.open(ws_raster, 'w', **meta) as out:

            out.write_band(1, basin_rast)

        del basin_df
        del shapes

    # Open stream pixel watershed raster and get its flattened data
    gage = rasterio.open(stream_px_ws)
    gage_arr = gage.read(1)
    gage_flat = gage_arr.flatten()
    gage_flat = np.clip(gage_flat, 0, np.max(gage_flat))

    del gage
    del gage_arr
    gc.collect()

    @njit
    def get_values(values, dict_arr):
        """Optimized function for getting min elevation for each stream pixel watershed

        Parameters
        ----------
        values : array_like
            Float stream pixel watershed identifier array
        dict_arr : array_like
            Float array of lowest elevation in Flat Resolved DEM based on watershed identifier

        Returns
        -------
        array_like
            Float array of minimum elevation values for each stream pixel watershed.

        """

        for idx, val in enumerate(values):
            if values[idx] > 0:
                values[idx] = dict_arr[int(val)]

        return values

    # Create dataframe with flattened elevation values and stream pixel watershed
    # identifiers, group by identifier, get minumum elevation values for each.
    res_flat_df = pd.DataFrame({'values': pd.Series(flattened_arr)})

    del flattened_arr

    res_flat_df['keys'] = pd.Series(gage_flat)

    del gage_flat

    res_flat_max = np.max(res_flat_df['keys']) + 1
    gage_min_df = res_flat_df.groupby('keys').min().to_dict()['values']
    gage_flat = res_flat_df['keys'].values

    del res_flat_df
    gc.collect()

    # Initialize sparse array
    # gage_array = np.zeros(res_flat_max, dtype=np.int)
    gage_array = np.zeros(res_flat_max, dtype=np.float32)

    # Allocate minimum elevation for each identifier index
    for gage_val in gage_min_df:
        if gage_val > 0:
            gage_array[gage_val] = gage_min_df[gage_val]

    del gage_min_df

    # Get minimum elevation values for each stream pixel watershed identifier
    gage_values = get_values(gage_flat, gage_array)
    del gage_flat

    del gage_array
    gc.collect()

    # Reshape to 2d array
    min_data = gage_values.reshape(res_flat_shape)
    del gage_values

    meta.update(dtype=rasterio.float32)
    meta.update(nodata=ndv)

    # Reload stream pixel watershed for future nodata masking
    gage_rast = rasterio.open(stream_px_ws)
    gage_arr = gage_rast.read(1)

    rel_dem_path = rem

    # Reload resolve flat dem for subtraction
    res_flat = rasterio.open(res_flat_dem)
    # res_flat_arr = res_flat.read(1).astype(np.int)
    res_flat_arr = res_flat.read(1).astype(np.float32)

    # Subtract elevation DEM with min elevation data to get relative height DEM
    with rasterio.open(rel_dem_path, 'w', **meta) as out:

        # data = (res_flat_arr - min_data) / 100
        data = (res_flat_arr - min_data)
        del res_flat, res_flat_arr, min_data
        data[gage_arr == gage_rast.meta['nodata']] = ndv
        del gage_rast, gage_arr
        out.write_band(1, data.astype(np.float32))

if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-d','--dem', help='DEM to use within project path', required=True)
    parser.add_argument('-b','--basins',help='Basins polygons to use within project path',required=False,default=None)
    parser.add_argument('-w','--watersheds',help='Pixel based watersheds raster to use within project path',required=True)
    parser.add_argument('-o','--rem',help='Output REM raster',required=True)
    parser.add_argument('-n','--ndv',help='Output REM raster No Data Value',required=True)

    # extract to dictionary
    args = vars(parser.parse_args())

    # rename variable inputs
    res_flat_dem = args['dem']
    stream_px_ws = args['watersheds']
    rem = args['rem']
    basins = args['basins']
    ndv = args['ndv']

    rel_dem(res_flat_dem, stream_px_ws,rem, basins,ndv)
