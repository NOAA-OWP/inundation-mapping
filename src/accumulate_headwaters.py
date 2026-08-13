#!/usr/bin/env python3
"""
accumulate_headwaters.py
------------------------
Computes D8 flow accumulation and derives stream pixels in-memory using pyflwdir.accuflux.
Exact GDAL/MEM adaptation of original rasterio implementation.
"""

import argparse
import os

import numpy as np
import pyflwdir
from osgeo import gdal


gdal.UseExceptions()


def accumulate_headwaters_in_memory(
    flow_dir_ds: gdal.Dataset, headwaters_ds: gdal.Dataset, threshold: float = 1.0
) -> tuple[gdal.Dataset, gdal.Dataset]:
    """
    In-memory equivalent of original accumulate_flow using GDAL MEM datasets.
    """
    fd_band = flow_dir_ds.GetRasterBand(1)
    data = fd_band.ReadAsArray()
    nodata = fd_band.GetNoDataValue()
    if nodata is None:
        nodata = 0

    # Convert TauDEM flow directions to pyflwdir D8 convention
    temp = data.copy()
    temp[data == 1] = 1
    temp[data == 2] = 128
    temp[data == 3] = 64
    temp[data == 4] = 32
    temp[data == 5] = 16
    temp[data == 6] = 8
    temp[data == 7] = 4
    temp[data == 8] = 2
    temp[data == nodata] = 247

    temp = temp.astype(np.uint8)
    flw = pyflwdir.from_array(temp, ftype='d8')
    del temp

    # Read headwaters
    hw_band = headwaters_ds.GetRasterBand(1)
    headwaters = hw_band.ReadAsArray()
    hw_nodata = hw_band.GetNoDataValue()

    # Exact accuflux call
    flowaccum = flw.accuflux(headwaters, nodata=hw_nodata, direction='up')
    del flw

    stream = np.where(flowaccum > 0, threshold, 0).astype(np.int32)

    # Build GDAL MEM Datasets
    driver = gdal.GetDriverByName("MEM")
    cols = flow_dir_ds.RasterXSize
    rows = flow_dir_ds.RasterYSize
    gt = flow_dir_ds.GetGeoTransform()
    proj = flow_dir_ds.GetProjectionRef()

    fa_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    fa_ds.SetGeoTransform(gt)
    fa_ds.SetProjection(proj)
    band_fa = fa_ds.GetRasterBand(1)
    band_fa.SetNoDataValue(-9999.0)
    band_fa.WriteArray(flowaccum.astype(np.float32))

    stream_ds = driver.Create("", cols, rows, 1, gdal.GDT_Int32)
    stream_ds.SetGeoTransform(gt)
    stream_ds.SetProjection(proj)
    band_str = stream_ds.GetRasterBand(1)
    band_str.SetNoDataValue(0)
    band_str.WriteArray(stream)

    return fa_ds, stream_ds


def accumulate_flow(
    flow_direction_filename,
    headwaters_filename,
    flow_accumulation_filename,
    stream_pixel_filename,
    flow_accumulation_threshold,
):
    """CLI wrapper for disk execution."""
    assert os.path.isfile(flow_direction_filename), 'Flow direction raster does not exist.'
    assert os.path.isfile(headwaters_filename), 'Headwaters raster does not exist.'

    fd_ds = gdal.Open(flow_direction_filename)
    hw_ds = gdal.Open(headwaters_filename)

    fa_ds, stream_ds = accumulate_headwaters_in_memory(
        fd_ds, hw_ds, threshold=float(flow_accumulation_threshold)
    )

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(flow_accumulation_filename, fa_ds, options=["COMPRESS=LZW", "TILED=YES"])
    driver.CreateCopy(stream_pixel_filename, stream_ds, options=["COMPRESS=LZW", "TILED=YES"])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-fd', '--flow-direction-filename', help='Flow direction filename', required=True, type=str
    )
    parser.add_argument('-wg', '--headwaters-filename', help='Headwaters filename', required=True, type=str)
    parser.add_argument(
        '-fa', '--flow-accumulation-filename', help='Flow accumulation filename', required=True, type=str
    )
    parser.add_argument(
        '-stream', '--stream-pixel-filename', help='Stream pixel filename', required=True, type=str
    )
    parser.add_argument(
        '-thresh',
        '--flow-accumulation-threshold',
        help='Flow accumulation threshold',
        required=True,
        type=float,
    )

    args = parser.parse_args()

    accumulate_flow(**vars(args))
