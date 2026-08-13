#!/usr/bin/env python3
"""
make_rem.py
-----------
Calculates Relative Elevation Model (REM / HAND) using GDAL in-memory datasets.
Supports direct in-memory dataset calls as well as standalone CLI execution.
"""

import argparse

import numpy as np
from osgeo import gdal
from scipy.ndimage import minimum


gdal.UseExceptions()


def create_rem_in_memory(
    dem_ds: gdal.Dataset,
    pixel_watersheds_ds: gdal.Dataset,
    thalweg_ds: gdal.Dataset,
    nodata_val: float = -9999.0,
) -> gdal.Dataset:
    """
    Computes Relative Elevation Model (REM) in RAM using vectorized array lookups.
    Safely handles negative NoData values in catchment raster.
    """
    dem_band = dem_ds.GetRasterBand(1)
    dem_arr = dem_band.ReadAsArray().astype(np.float32)
    dem_nodata = dem_band.GetNoDataValue()

    cat_band = pixel_watersheds_ds.GetRasterBand(1)
    cat_arr = cat_band.ReadAsArray().astype(np.int32)
    cat_nodata = cat_band.GetNoDataValue()

    thalweg_arr = thalweg_ds.GetRasterBand(1).ReadAsArray()

    # Valid mask for DEM and Catchments (catchments must be > 0)
    valid_dem = (dem_arr != dem_nodata) if dem_nodata is not None else np.ones_like(dem_arr, dtype=bool)
    valid_cat = (cat_arr != cat_nodata) & (cat_arr > 0) if cat_nodata is not None else (cat_arr > 0)
    valid_mask = valid_dem & valid_cat

    # Mask thalweg cells to extract stream elevation per catchment
    stream_mask = (thalweg_arr > 0) & valid_mask

    # Extract unique positive catchment IDs
    cat_ids = np.unique(cat_arr[valid_cat])

    if len(cat_ids) == 0:
        rem_arr = np.full_like(dem_arr, nodata_val, dtype=np.float32)
    else:
        # Extract minimum stream elevation per catchment zone
        stream_cat_ids = np.unique(cat_arr[stream_mask])

        if len(stream_cat_ids) > 0:
            min_stream_elevs = minimum(dem_arr, labels=cat_arr, index=stream_cat_ids)
        else:
            min_stream_elevs = np.array([])

        # Build 1D direct lookup array
        max_cat_id = int(cat_arr[valid_cat].max()) if np.any(valid_cat) else 0
        lookup = np.full(max_cat_id + 1, np.nan, dtype=np.float32)

        if len(stream_cat_ids) > 0:
            lookup[stream_cat_ids] = min_stream_elevs

        # Fallback for catchments without stream pixels: use catchment minimum DEM
        missing_cats = np.setdiff1d(cat_ids, stream_cat_ids)
        if len(missing_cats) > 0:
            fallback_elevs = minimum(dem_arr, labels=cat_arr, index=missing_cats)
            lookup[missing_cats] = fallback_elevs

        # --- FIX: Clip negative/NoData values to 0 to prevent negative index out-of-bounds ---
        cat_safe = np.where(valid_cat, cat_arr, 0)

        # Vectorized Broadcast: Map Catchment ID to Stream Elevation across entire 2D Grid
        thalweg_elev_grid = lookup[cat_safe]

        # Calculate REM: DEM Elevation - Thalweg/Stream Elevation
        rem_arr = np.where(valid_mask & ~np.isnan(thalweg_elev_grid), dem_arr - thalweg_elev_grid, nodata_val)

        # Floor negative noise values to 0.0
        rem_arr = np.where((rem_arr != nodata_val) & (rem_arr < 0), 0.0, rem_arr)

    # Package GDAL MEM Dataset
    cols = dem_ds.RasterXSize
    rows = dem_ds.RasterYSize
    driver = gdal.GetDriverByName("MEM")

    rem_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    rem_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    rem_ds.SetProjection(dem_ds.GetProjectionRef())

    out_band = rem_ds.GetRasterBand(1)
    out_band.SetNoDataValue(nodata_val)
    out_band.WriteArray(rem_arr.astype(np.float32))

    return rem_ds


def rel_dem(dem_path: str, pixel_watersheds_path: str, out_rem_path: str, thalweg_path: str):
    """File-based CLI wrapper for backward compatibility."""
    dem_ds = gdal.Open(dem_path)
    pw_ds = gdal.Open(pixel_watersheds_path)
    th_ds = gdal.Open(thalweg_path)

    rem_ds = create_rem_in_memory(dem_ds, pw_ds, th_ds)

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(out_rem_path, rem_ds, options=["COMPRESS=LZW", "TILED=YES"])


def main():
    parser = argparse.ArgumentParser(description="Create Relative Elevation Model (REM / HAND)")
    parser.add_argument("-d", "--dem", required=True, help="DEM raster path")
    parser.add_argument("-w", "--watersheds", required=True, help="Pixel watersheds raster path")
    parser.add_argument("-o", "--output", required=True, help="Output REM raster path")
    parser.add_argument("-t", "--thalweg", required=True, help="Thalweg stream raster path")

    args = parser.parse_args()
    rel_dem(args.dem, args.watersheds, args.output, args.thalweg)


if __name__ == "__main__":
    main()
