#!/usr/bin/env python3
"""
make_rem.py
-----------
Replicates dev FIM logic:
Subtracts pixel watershed thalweg elevations from dem_thalwegCond.
"""

import numpy as np
from osgeo import gdal
from scipy.ndimage import minimum


def create_rem_in_memory(
    dem_ds: gdal.Dataset,
    pixel_watersheds_ds: gdal.Dataset,
    stream_pixels_ds: gdal.Dataset = None,
    nodata_val: float = -999999.0,
) -> gdal.Dataset:
    """Computes raw REM = DEM - Thalweg_Elevations matching dev baseline."""
    # 1. Read Conditioned DEM
    dem_band = dem_ds.GetRasterBand(1)
    dem_arr = dem_band.ReadAsArray().astype(np.float32)
    dem_nodata = dem_band.GetNoDataValue()
    if dem_nodata is None:
        dem_nodata = -9999.0

    valid_dem_mask = (dem_arr != dem_nodata) & (dem_arr > -1000.0) & (~np.isnan(dem_arr))

    # 2. Read Pixel Watersheds
    pixel_catch_band = pixel_watersheds_ds.GetRasterBand(1)
    pixel_catch_arr = pixel_catch_band.ReadAsArray().astype(np.int32)

    # Clean DEM for zonal minimum extraction: set NoData to infinity
    dem_clean = np.where(valid_dem_mask, dem_arr, np.inf)

    # 3. Calculate zonal minimum elevation per pixel watershed ID
    max_id = pixel_catch_arr.max() if pixel_catch_arr.size > 0 else 0
    lookup = np.full(max_id + 1, np.nan, dtype=np.float32)

    valid_catch_mask = (pixel_catch_arr > 0) & valid_dem_mask

    if max_id > 0 and np.any(valid_catch_mask):
        labels = np.unique(pixel_catch_arr[valid_catch_mask])
        min_elevs = minimum(dem_clean, labels=pixel_catch_arr, index=labels)
        lookup[labels] = min_elevs

        catch_ids_bounded = np.where(pixel_catch_arr > 0, pixel_catch_arr, 0)
        thalweg_elev_grid = lookup[catch_ids_bounded]
    else:
        thalweg_elev_grid = np.full_like(dem_arr, np.nan, dtype=np.float32)

    # 4. Compute Raw REM (DEM minus Pixel Thalweg Elevation)
    rem_arr = np.full_like(dem_arr, fill_value=nodata_val, dtype=np.float32)

    has_thalweg = ~np.isnan(thalweg_elev_grid) & (thalweg_elev_grid < 1e8)
    valid_rem = valid_dem_mask & has_thalweg

    # Raw REM calculation strictly inside pixel watersheds
    rem_arr[valid_rem] = dem_arr[valid_rem] - thalweg_elev_grid[valid_rem]

    # Non-catchment cells remain as nodata_val (-999999.0)
    rem_arr[~valid_rem] = nodata_val

    # 5. Build GDAL Memory Dataset
    driver = gdal.GetDriverByName("MEM")
    cols, rows = dem_ds.RasterXSize, dem_ds.RasterYSize
    out_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjectionRef())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(float(nodata_val))
    band.WriteArray(rem_arr)
    out_ds.FlushCache()

    return out_ds
