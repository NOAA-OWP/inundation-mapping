#!/usr/bin/env python3
"""
unique_pixel_and_allocation.py
------------------------------
Generates unique ID grid, allocation grid, and Euclidean distance grid for stream pixels.
Supports direct in-memory dataset calls as well as standalone CLI execution.
"""

import argparse

import numpy as np
from osgeo import gdal
from scipy.ndimage import distance_transform_edt


gdal.UseExceptions()


def unique_pixel_allocation_in_memory(
    stream_pixels_ds: gdal.Dataset,
) -> tuple[gdal.Dataset, gdal.Dataset, gdal.Dataset]:
    """Generates unique IDs, allocation, and Euclidean distance grids in RAM."""
    stream_arr = stream_pixels_ds.GetRasterBand(1).ReadAsArray()

    unique_ids = np.zeros_like(stream_arr, dtype=np.int32)
    stream_indices = np.where(stream_arr > 0)
    unique_ids[stream_indices] = np.arange(1, len(stream_indices[0]) + 1)

    dist_arr, indices = distance_transform_edt(unique_ids == 0, return_indices=True)
    allo_arr = unique_ids[indices[0], indices[1]]

    driver = gdal.GetDriverByName("MEM")
    cols, rows = stream_pixels_ds.RasterXSize, stream_pixels_ds.RasterYSize
    gt = stream_pixels_ds.GetGeoTransform()
    proj = stream_pixels_ds.GetProjection()

    ids_ds = driver.Create("", cols, rows, 1, gdal.GDT_Int32)
    ids_ds.SetGeoTransform(gt)
    ids_ds.SetProjection(proj)
    ids_ds.GetRasterBand(1).WriteArray(unique_ids)

    allo_ds = driver.Create("", cols, rows, 1, gdal.GDT_Int32)
    allo_ds.SetGeoTransform(gt)
    allo_ds.SetProjection(proj)
    allo_ds.GetRasterBand(1).WriteArray(allo_arr)

    dist_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    dist_ds.SetGeoTransform(gt)
    dist_ds.SetProjection(proj)
    dist_ds.GetRasterBand(1).WriteArray(dist_arr.astype(np.float32))

    return ids_ds, allo_ds, dist_ds


def main():
    parser = argparse.ArgumentParser(
        description="Generate unique IDs and allocation grids for stream pixels."
    )
    parser.add_argument("-s", "--stream-pixels", required=True, help="Input stream pixels raster path")
    parser.add_argument("-o", "--output-ids", required=True, help="Output stream pixel IDs raster path")

    args = parser.parse_args()

    s_ds = gdal.Open(args.stream_pixels)
    ids_ds, allo_ds, dist_ds = unique_pixel_allocation_in_memory(s_ds)

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(args.output_ids, ids_ds, options=["COMPRESS=LZW", "TILED=YES"])

    out_base = str(args.output_ids).replace(".tif", "")
    driver.CreateCopy(f"{out_base}_allo.tif", allo_ds, options=["COMPRESS=LZW", "TILED=YES"])
    driver.CreateCopy(f"{out_base}_dist.tif", dist_ds, options=["COMPRESS=LZW", "TILED=YES"])


if __name__ == "__main__":
    main()
