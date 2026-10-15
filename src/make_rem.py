#!/usr/bin/env python3

import argparse

import numpy as np
import rasterio
from numba import njit, typed, types
from osgeo import gdal


# ------------------------------------------------------------------------------------------------------ #
# NUMBA KERNELS WITH NODATA FIX (-999999.0 PRE-FILL)
# ------------------------------------------------------------------------------------------------------ #
@njit
def make_catchment_min_dict(flat_dem, catchment_min_dict, flat_catchments, thalweg_window):
    """Populates dictionary of minimum elevation along the thalweg for each catchment ID."""
    for i, cm in enumerate(flat_catchments):
        if thalweg_window[i] == 1:  # Only allow reference elevation to be within thalweg.
            if cm in catchment_min_dict:
                if flat_dem[i] < catchment_min_dict[cm]:
                    catchment_min_dict[cm] = flat_dem[i]
            else:
                catchment_min_dict[cm] = flat_dem[i]
    return catchment_min_dict


@njit
def calculate_rem(flat_dem, catchmentMinDict, flat_catchments, ndv):
    """
    Computes REM/HAND pixel values.
    CRITICAL FIX: Pre-fills window with `ndv` (-999999.0) instead of np.zeros
    to prevent stream cells (REM == 0.0) from being dropped as NoData!
    """
    rem_window = np.full(len(flat_dem), ndv, dtype=np.float32)

    for i, cm in enumerate(flat_catchments):
        if cm in catchmentMinDict:
            if catchmentMinDict[cm] == ndv or flat_dem[i] == ndv:
                rem_window[i] = ndv
            else:
                rem_window[i] = flat_dem[i] - catchmentMinDict[cm]
        else:
            rem_window[i] = ndv

    return rem_window


# ------------------------------------------------------------------------------------------------------ #
# IN-MEMORY REM ENGINE EXPECTED BY delineate_hydros_and_produce_HAND.py
# ------------------------------------------------------------------------------------------------------ #
def create_rem_in_memory(dem_ds, pixel_watersheds_ds, thalweg_ds, nodata_val=-999999.0):
    """
    Calculates REM/HAND in-memory accepting GDAL Dataset objects.
    Returns an in-memory gdal.Dataset.
    """
    # Extract 2D numpy arrays from GDAL datasets
    dem_band = dem_ds.GetRasterBand(1)
    shed_band = pixel_watersheds_ds.GetRasterBand(1)
    thal_band = thalweg_ds.GetRasterBand(1)

    dem_arr = dem_band.ReadAsArray().astype(np.float32)
    shed_arr = shed_band.ReadAsArray().astype(np.int32)
    thal_arr = thal_band.ReadAsArray().astype(np.int32)

    # Determine NoData value (ensure negative value to avoid 0.0 collision)
    dem_ndv = dem_band.GetNoDataValue()
    if dem_ndv is None or dem_ndv == 0.0 or np.isnan(dem_ndv):
        dem_ndv = float(nodata_val)
    else:
        dem_ndv = float(dem_ndv)

    ndv_f32 = np.float32(dem_ndv)

    # Step 1: Create catchment_min_dict along thalweg
    catchment_min_dict = typed.Dict.empty(types.int32, types.float32)
    catchment_min_dict = make_catchment_min_dict(
        dem_arr.ravel(), catchment_min_dict, shed_arr.ravel(), thal_arr.ravel()
    )

    # Step 2: Compute relative elevation array
    rem_flat = calculate_rem(dem_arr.ravel(), catchment_min_dict, shed_arr.ravel(), ndv_f32)
    rem_2d = rem_flat.reshape(dem_arr.shape).astype(np.float32)

    # Step 3: Build GDAL MEM dataset output
    cols, rows = dem_ds.RasterXSize, dem_ds.RasterYSize
    driver = gdal.GetDriverByName("MEM")
    out_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjectionRef())

    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(dem_ndv)
    out_band.WriteArray(rem_2d)
    out_ds.FlushCache()

    return out_ds


# ------------------------------------------------------------------------------------------------------ #
# FILE-BASED REM ENGINE
# ------------------------------------------------------------------------------------------------------ #
def rel_dem(dem_fileName, pixel_watersheds_fileName, rem_fileName, thalweg_raster):
    """Calculates REM and writes output GeoTIFF to disk."""
    ds_dem = gdal.Open(dem_fileName)
    ds_shed = gdal.Open(pixel_watersheds_fileName)
    ds_thal = gdal.Open(thalweg_raster)

    ds_rem = create_rem_in_memory(ds_dem, ds_shed, ds_thal, nodata_val=-999999.0)

    driver = gdal.GetDriverByName("GTiff")
    dst_ds = driver.CreateCopy(rem_fileName, ds_rem, options=["COMPRESS=LZW", "TILED=YES"])
    dst_ds.FlushCache()

    ds_dem = None
    ds_shed = None
    ds_thal = None
    ds_rem = None
    dst_ds = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Relative elevation from pixel based watersheds")
    parser.add_argument("-d", "--dem", help="DEM to use within project path", required=True)
    parser.add_argument("-w", "--watersheds", help="Pixel based watersheds raster", required=True)
    parser.add_argument("-t", "--thalweg-raster", help="Binary thalweg raster", required=True)
    parser.add_argument("-o", "--rem", help="Output REM raster", required=True)

    args = vars(parser.parse_args())
    rel_dem(args["dem"], args["watersheds"], args["rem"], args["thalweg_raster"])
