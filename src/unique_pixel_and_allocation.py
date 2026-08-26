#!/usr/bin/env python3
"""
unique_pixel_and_allocation.py
------------------------------
In-Memory wrapper for WhiteboxTools euclidean_distance and euclidean_allocation.
Guarantees 100% exact parity with dev pipeline outputs.
"""

import argparse
import os
import tempfile

import numpy as np
import rasterio
import whitebox
from osgeo import gdal


# Initialize WhiteboxTools
wbt = whitebox.WhiteboxTools()
wbt_path = os.environ.get("WBT_PATH")
if wbt_path:
    wbt.set_whitebox_dir(wbt_path)
wbt.set_verbose_mode(False)


def unique_pixel_allocation_in_memory(
    stream_pixels_ds: gdal.Dataset,
) -> tuple[gdal.Dataset, gdal.Dataset, gdal.Dataset]:
    """Runs WhiteboxTools directly to produce 100% dev-identical IDs, Allocation, and Distance rasters."""
    band = stream_pixels_ds.GetRasterBand(1)
    streams = band.ReadAsArray()
    gt = stream_pixels_ds.GetGeoTransform()
    proj = stream_pixels_ds.GetProjection()
    rows, cols = stream_pixels_ds.RasterYSize, stream_pixels_ds.RasterXSize

    # 1. Dev ID scheme: unique 1D flattened index as float64 array
    unique_vals = np.arange(streams.size, dtype=np.float64).reshape((rows, cols))
    stream_pixel_values = np.where(streams == 1, unique_vals, 0.0)

    # Use temporary file paths for Whitebox processing
    with tempfile.TemporaryDirectory() as tmpdir:
        stream_pixels_path = os.path.join(tmpdir, "stream_pixels.tif")
        unique_ids_path = os.path.join(tmpdir, "unique_ids.tif")
        distance_grid = os.path.join(tmpdir, "dist.tif")
        allocation_grid = os.path.join(tmpdir, "allo.tif")

        # Write inputs expected by Whitebox
        driver = gdal.GetDriverByName("GTiff")

        # Write stream_pixels.tif
        ds_sp = driver.Create(stream_pixels_path, cols, rows, 1, gdal.GDT_Byte)
        ds_sp.SetGeoTransform(gt)
        ds_sp.SetProjection(proj)
        ds_sp.GetRasterBand(1).WriteArray((streams == 1).astype(np.uint8))
        ds_sp.FlushCache()
        ds_sp = None

        # Write unique_ids.tif (Float64 matching dev)
        ds_uid = driver.Create(unique_ids_path, cols, rows, 1, gdal.GDT_Float64)
        ds_uid.SetGeoTransform(gt)
        ds_uid.SetProjection(proj)
        ds_uid.GetRasterBand(1).WriteArray(stream_pixel_values)
        ds_uid.FlushCache()
        ds_uid = None

        # 2. Execute exact WhiteboxTools functions from dev
        wbt.euclidean_distance(stream_pixels_path, distance_grid)
        wbt.euclidean_allocation(unique_ids_path, allocation_grid)

        # 3. Replicate dev's allocation post-processing step exactly
        # dev line: allocation = np.where(allocation > 0, allocation, stream_pixel_values)
        with rasterio.open(allocation_grid) as allo_src:
            allocation = allo_src.read(1)

        allocation = np.where(allocation > 0, allocation, stream_pixel_values)

        with rasterio.open(distance_grid) as dist_src:
            distance = dist_src.read(1)

    # 4. Package into GDAL MEM Datasets for your in-memory pipeline
    mem_driver = gdal.GetDriverByName("MEM")

    ids_ds = mem_driver.Create("", cols, rows, 1, gdal.GDT_Float64)
    ids_ds.SetGeoTransform(gt)
    ids_ds.SetProjection(proj)
    ids_ds.GetRasterBand(1).WriteArray(stream_pixel_values)

    allo_ds = mem_driver.Create("", cols, rows, 1, gdal.GDT_Float64)
    allo_ds.SetGeoTransform(gt)
    allo_ds.SetProjection(proj)
    allo_ds.GetRasterBand(1).WriteArray(allocation)

    dist_ds = mem_driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    dist_ds.SetGeoTransform(gt)
    dist_ds.SetProjection(proj)
    dist_ds.GetRasterBand(1).WriteArray(distance.astype(np.float32))

    return ids_ds, allo_ds, dist_ds


def main():
    parser = argparse.ArgumentParser(
        description="Produce unique stream pixel values and allocation/proximity grids matching dev."
    )
    parser.add_argument("-s", "--stream", required=True, help="Input stream pixels raster path")
    parser.add_argument("-o", "--out", required=True, help="Output raster of unique IDs")

    args = parser.parse_args()

    s_ds = gdal.Open(args.stream)
    ids_ds, allo_ds, dist_ds = unique_pixel_allocation_in_memory(s_ds)

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(args.out, ids_ds, options=["COMPRESS=LZW", "TILED=YES"])

    out_base = str(args.out).replace(".tif", "")
    driver.CreateCopy(f"{out_base}_allo.tif", allo_ds, options=["COMPRESS=LZW", "TILED=YES"])
    driver.CreateCopy(f"{out_base}_dist.tif", dist_ds, options=["COMPRESS=LZW", "TILED=YES"])


if __name__ == "__main__":
    main()
