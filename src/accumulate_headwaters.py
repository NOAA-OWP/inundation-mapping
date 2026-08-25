#!/usr/bin/env python3
"""
accumulate_headwaters.py
------------------------
Calculates D8 flow accumulation and stream masks using pyflwdir in RAM.
Translates TauDEM flow direction codes to standard D8 bitmasks before calling accuflux().
"""

import numpy as np
import pyflwdir
from osgeo import gdal


def accumulate_headwaters_in_memory(
    flow_dir_ds: gdal.Dataset, headwaters_ds: gdal.Dataset = None, threshold: float = 1.0
) -> tuple[gdal.Dataset, gdal.Dataset]:
    """Computes flow accumulation and stream binary rasters in RAM matching original pyflwdir logic."""
    fdir_band = flow_dir_ds.GetRasterBand(1)
    data = fdir_band.ReadAsArray()
    nodata = fdir_band.GetNoDataValue()
    if nodata is None:
        nodata = -9999

    # Translate TauDEM flow direction values (1..8) to pyflwdir D8 bitmasks
    temp = np.zeros_like(data, dtype=np.uint8)
    temp[data == 1] = 1
    temp[data == 2] = 128
    temp[data == 3] = 64
    temp[data == 4] = 32
    temp[data == 5] = 16
    temp[data == 6] = 8
    temp[data == 7] = 4
    temp[data == 8] = 2
    temp[data == nodata] = 247

    flw = pyflwdir.from_array(temp, ftype="d8")
    del temp

    # Extract headwaters grid or default to unit inputs
    if headwaters_ds is not None:
        hw_band = headwaters_ds.GetRasterBand(1)
        headwaters = hw_band.ReadAsArray()
        hw_nodata = hw_band.GetNoDataValue()
    else:
        headwaters = np.ones_like(data, dtype=np.int32)
        hw_nodata = nodata

    # Compute accumulation using accuflux upwards
    flowaccum = flw.accuflux(headwaters, nodata=hw_nodata, direction="up")
    del flw

    # Create stream raster matching original logic
    thresh_val = float(threshold)
    stream = np.where(flowaccum > 0, thresh_val, 0).astype(flowaccum.dtype)

    # Build GDAL Memory Datasets
    driver = gdal.GetDriverByName("MEM")
    geo_transform = flow_dir_ds.GetGeoTransform()
    proj = flow_dir_ds.GetProjectionRef()
    cols, rows = flow_dir_ds.RasterXSize, flow_dir_ds.RasterYSize

    # Flow Accumulation Dataset
    ds_accum = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    ds_accum.SetGeoTransform(geo_transform)
    ds_accum.SetProjection(proj)
    b_accum = ds_accum.GetRasterBand(1)
    if nodata is not None:
        b_accum.SetNoDataValue(float(nodata))
    b_accum.WriteArray(flowaccum.astype(np.float32))

    # Stream Pixels Dataset
    ds_streams = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    ds_streams.SetGeoTransform(geo_transform)
    ds_streams.SetProjection(proj)
    b_streams = ds_streams.GetRasterBand(1)
    if nodata is not None:
        b_streams.SetNoDataValue(float(nodata))
    b_streams.WriteArray(stream.astype(np.float32))

    del flowaccum, stream

    return ds_accum, ds_streams
