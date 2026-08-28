#!/usr/bin/env python3
"""
make_rem.py
-----------
Replicates dev rel_dem logic bit-for-bit in RAM using GDAL Datasets.
"""

import numpy as np
from numba import njit, typed, types
from osgeo import gdal


@njit
def _make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments, thalweg_window):
    for i, cm in enumerate(flat_catchments):
        # Strict check matching dev: 1 represents binary thalweg pixel
        if thalweg_window[i] == 1:
            if cm in catchment_min_dict:
                if flat_dem[i] < catchment_min_dict[cm]:
                    catchment_min_dict[cm] = flat_dem[i]
            else:
                catchment_min_dict[cm] = flat_dem[i]
    return catchment_min_dict


@njit
def _calculate_rem(flat_dem, catchmentMinDict, flat_catchments, ndv):
    # Replicate dev's np.zeros initialization
    rem_window = np.zeros(len(flat_dem), dtype=np.float32)

    for i, cm in enumerate(flat_catchments):
        if cm in catchmentMinDict:
            if catchmentMinDict[cm] == ndv or flat_dem[i] == ndv:
                rem_window[i] = ndv
            else:
                rem_window[i] = flat_dem[i] - catchmentMinDict[cm]

    return rem_window


def create_rem_in_memory(
    dem_ds: gdal.Dataset,
    pixel_watersheds_ds: gdal.Dataset,
    thalweg_ds: gdal.Dataset,
    nodata_val: float = -999999.0,
) -> gdal.Dataset:
    """In-memory driver replicating dev rel_dem logic bit-for-bit."""
    dem_band = dem_ds.GetRasterBand(1)
    dem_arr = dem_band.ReadAsArray().astype(np.float32)

    dem_ndv = dem_band.GetNoDataValue()
    if dem_ndv is None:
        dem_ndv = nodata_val

    catch_arr = pixel_watersheds_ds.GetRasterBand(1).ReadAsArray().astype(np.int32)

    # Read thalweg array & ensure binary encoding (1 for stream, 0 for non-stream)
    thalweg_arr = thalweg_ds.GetRasterBand(1).ReadAsArray()
    thalweg_arr = np.where((thalweg_arr > 0) & (thalweg_arr != 32767), 1, 0).astype(np.int32)

    flat_dem = dem_arr.ravel()
    flat_catch = catch_arr.ravel()
    flat_thalweg = thalweg_arr.ravel()

    # 1. Build catchment minimum dictionary on binary thalweg pixels
    catchment_min_dict = typed.Dict.empty(types.int32, types.float32)
    catchment_min_dict = _make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catch, flat_thalweg)

    # 2. Compute REM array matching dev logic
    rem_flat = _calculate_rem(flat_dem, catchment_min_dict, flat_catch, float(dem_ndv))
    rem_arr = rem_flat.reshape(dem_arr.shape)

    # 3. Package GDAL Memory Dataset
    driver = gdal.GetDriverByName("MEM")
    cols, rows = dem_ds.RasterXSize, dem_ds.RasterYSize
    out_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjectionRef())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(float(dem_ndv))
    band.WriteArray(rem_arr)
    out_ds.FlushCache()

    return out_ds
