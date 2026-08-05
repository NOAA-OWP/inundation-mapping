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


gdal.UseExceptions()


def create_rem_in_memory(
    dem_ds: gdal.Dataset,
    pixel_watersheds_ds: gdal.Dataset,
    thalweg_ds: gdal.Dataset,
    nodata_val: float = -9999.0,
) -> gdal.Dataset:
    """Calculates REM (HAND) directly in RAM from GDAL Datasets."""
    dem_arr = dem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    catch_arr = pixel_watersheds_ds.GetRasterBand(1).ReadAsArray().astype(np.int32)
    thalweg_arr = thalweg_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)

    thalweg_elevs = np.where(thalweg_arr > 0, dem_arr, np.nan)
    unique_ids = np.unique(catch_arr)
    unique_ids = unique_ids[unique_ids > 0]

    stream_elev_map = np.full_like(dem_arr, fill_value=np.nan, dtype=np.float32)

    for uid in unique_ids:
        mask = catch_arr == uid
        t_elevs = thalweg_elevs[mask]
        valid_t = t_elevs[~np.isnan(t_elevs)]
        if len(valid_t) > 0:
            stream_elev_map[mask] = np.min(valid_t)

    rem_arr = dem_arr - stream_elev_map
    rem_arr[np.isnan(stream_elev_map) | (dem_arr == nodata_val)] = nodata_val

    driver = gdal.GetDriverByName("MEM")
    out_ds = driver.Create("", dem_ds.RasterXSize, dem_ds.RasterYSize, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjection())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(float(nodata_val))
    band.WriteArray(rem_arr)
    out_ds.FlushCache()

    return out_ds


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
