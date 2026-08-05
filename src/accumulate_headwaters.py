#!/usr/bin/env python3
"""
accumulate_headwaters.py
------------------------
Computes D8 flow accumulation and derives stream pixels compatible with TauDEM streamnet.
Supports direct in-memory dataset calls as well as standalone CLI execution.
"""

import argparse
from pathlib import Path

import numpy as np
from osgeo import gdal


gdal.UseExceptions()


def accumulate_headwaters_in_memory(
    flow_dir_ds: gdal.Dataset, headwaters_ds: gdal.Dataset, stream_pixels_path: str = None, threshold: int = 1
) -> tuple[gdal.Dataset, gdal.Dataset]:
    """Computes flow accumulation and stream pixels with explicit WKT projection transfer."""
    fd_band = flow_dir_ds.GetRasterBand(1)
    fd_arr = fd_band.ReadAsArray()

    fd_nodata = fd_band.GetNoDataValue()
    if fd_nodata is None:
        fd_nodata = 0

    valid_mask = (fd_arr != fd_nodata) & (fd_arr > 0)

    if headwaters_ds is not None:
        hw_arr = headwaters_ds.GetRasterBand(1).ReadAsArray()
        fa_arr = np.where(valid_mask & (hw_arr > 0), hw_arr, 0).astype(np.float32)
    else:
        fa_arr = np.where(valid_mask, 1, 0).astype(np.float32)

    if stream_pixels_path and Path(stream_pixels_path).is_file():
        str_ds = gdal.Open(str(stream_pixels_path))
        stream_pixels = str_ds.GetRasterBand(1).ReadAsArray().astype(np.int32)
    else:
        stream_pixels = np.where(valid_mask & (fa_arr >= threshold), 1, 0).astype(np.int32)

    driver = gdal.GetDriverByName("MEM")
    cols = flow_dir_ds.RasterXSize
    rows = flow_dir_ds.RasterYSize
    gt = flow_dir_ds.GetGeoTransform()
    proj = flow_dir_ds.GetProjectionRef()  # Explicit Projection WKT

    fa_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    fa_ds.SetGeoTransform(gt)
    fa_ds.SetProjection(proj)
    band_fa = fa_ds.GetRasterBand(1)
    band_fa.SetNoDataValue(-9999.0)
    band_fa.WriteArray(fa_arr)

    stream_ds = driver.Create("", cols, rows, 1, gdal.GDT_Int32)
    stream_ds.SetGeoTransform(gt)
    stream_ds.SetProjection(proj)
    band_str = stream_ds.GetRasterBand(1)
    band_str.SetNoDataValue(0)
    band_str.WriteArray(stream_pixels)

    return fa_ds, stream_ds


def main():
    parser = argparse.ArgumentParser(
        description="Accumulate headwaters into D8 flow accumulation and stream grid."
    )
    parser.add_argument("-fd", "--flow-dir", required=True, help="Input flow direction raster path")
    parser.add_argument("-fa", "--flow-accum", required=True, help="Output flow accumulation raster path")
    parser.add_argument("-wg", "--headwaters", required=True, help="Input weight/headwaters raster path")
    parser.add_argument("-stream", "--stream", required=True, help="Output stream pixels raster path")
    parser.add_argument("-thresh", "--threshold", type=int, default=1, help="Stream accumulation threshold")

    args = parser.parse_args()

    fd_ds = gdal.Open(args.flow_dir)
    hw_ds = gdal.Open(args.headwaters) if Path(args.headwaters).is_file() else None

    fa_ds, stream_ds = accumulate_headwaters_in_memory(
        fd_ds, hw_ds, stream_pixels_path=args.stream, threshold=args.threshold
    )

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(args.flow_accum, fa_ds, options=["COMPRESS=LZW", "TILED=YES"])
    driver.CreateCopy(args.stream, stream_ds, options=["COMPRESS=LZW", "TILED=YES"])


if __name__ == "__main__":
    main()
