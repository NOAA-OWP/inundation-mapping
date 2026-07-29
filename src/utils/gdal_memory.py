#!/usr/bin/env python3
"""In-Memory GDAL and NumPy raster processing operations."""

from typing import Callable, List, Union

import numpy as np
from osgeo import gdal, ogr


gdal.UseExceptions()

GTIFF_COS = ["COMPRESS=LZW", "BIGTIFF=YES", "TILED=YES"]


def numpy_to_mem_dataset(
    array: np.ndarray, ref_ds: gdal.Dataset, nodata_val: Union[int, float] = None, dtype: int = None
) -> gdal.Dataset:
    """Converts a NumPy array into an in-memory GDAL Dataset using a reference Dataset."""
    driver = gdal.GetDriverByName("MEM")
    rows, cols = array.shape
    if dtype is None:
        dtype = gdal.GDT_Float32

    mem_ds = driver.Create("", cols, rows, 1, dtype)
    mem_ds.SetGeoTransform(ref_ds.GetGeoTransform())
    mem_ds.SetProjection(ref_ds.GetProjection())

    band = mem_ds.GetRasterBand(1)
    if nodata_val is not None:
        band.SetNoDataValue(nodata_val)
    band.WriteArray(array)
    return mem_ds


def raster_math_in_memory(
    input_datasets: dict[str, gdal.Dataset],
    calc_fn: Callable[..., np.ndarray],
    nodata_val: Union[int, float] = None,
    dtype: int = gdal.GDT_Float32,
) -> gdal.Dataset:
    """In-memory replacement for gdal_calc.py."""
    array_dict = {}
    ref_ds = None

    for key, ds in input_datasets.items():
        array_dict[key] = ds.GetRasterBand(1).ReadAsArray()
        if ref_ds is None:
            ref_ds = ds

    result_array = calc_fn(**array_dict)
    return numpy_to_mem_dataset(result_array, ref_ds, nodata_val=nodata_val, dtype=dtype)


def rasterize_layer_in_memory(
    src_vector: ogr.DataSource,
    ref_raster: gdal.Dataset,
    attribute: str = None,
    burn_value: float = None,
    nodata_val: float = 0.0,
    init_val: float = 0.0,
    dtype: int = gdal.GDT_Int32,
) -> gdal.Dataset:
    """In-memory replacement for gdal_rasterize."""
    cols = ref_raster.RasterXSize
    rows = ref_raster.RasterYSize
    geotransform = ref_raster.GetGeoTransform()
    projection = ref_raster.GetProjection()

    mem_driver = gdal.GetDriverByName("MEM")
    out_ds = mem_driver.Create("", cols, rows, 1, dtype)
    out_ds.SetGeoTransform(geotransform)
    out_ds.SetProjection(projection)

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(nodata_val)
    band.Fill(init_val)

    options = []
    if attribute:
        options.append(f"ATTRIBUTE={attribute}")

    burn_vals = [burn_value] if burn_value is not None else []
    gdal.RasterizeLayer(out_ds, [1], src_vector.GetLayer(), burn_values=burn_vals, options=options)
    return out_ds


def polygonize_in_memory(src_raster: gdal.Dataset, layer_name: str = "catchments") -> ogr.DataSource:
    """In-memory replacement for gdal_polygonize.py using OGR Memory driver."""
    src_band = src_raster.GetRasterBand(1)
    drv = ogr.GetDriverByName("Memory")
    out_ds = drv.CreateDataSource("mem_vector")

    srs = ogr.osr.SpatialReference()
    srs.ImportFromWkt(src_raster.GetProjection())

    dst_layer = out_ds.CreateLayer(layer_name, srs=srs, geom_type=ogr.wkbPolygon)
    dst_layer.CreateField(ogr.FieldDefn("HydroID", ogr.OFTInteger))

    gdal.Polygonize(src_band, None, dst_layer, 0, ["8CONNECTED=8"])
    return out_ds


def export_ds_to_disk(ds: gdal.Dataset, out_path: str, co_options: List[str] = GTIFF_COS) -> None:
    """Writes an in-memory dataset to physical disk."""
    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(out_path, ds, options=co_options)
