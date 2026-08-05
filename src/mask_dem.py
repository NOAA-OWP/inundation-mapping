#!/usr/bin/env python3
"""
mask_dem.py
-----------
Masks DEM cells within levee-protected areas.
Supports direct in-memory dataset calls as well as standalone CLI execution.
"""

import argparse
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr


gdal.UseExceptions()


def mask_dem_in_memory(
    dem_ds: gdal.Dataset,
    nld_gpkg_path: str,
    catchments_gpkg_path: str,
    branch_id_attr: str,
    current_branch_id: str,
    branch_zero_id: str,
    levee_id_attr: str,
) -> gdal.Dataset:
    """Masks DEM cells within levee-protected areas directly in RAM."""
    dem_band = dem_ds.GetRasterBand(1)
    dem_arr = dem_band.ReadAsArray()
    nodata = dem_band.GetNoDataValue()
    if nodata is None:
        nodata = -9999.0

    driver = gdal.GetDriverByName("MEM")
    mask_ds = driver.Create("", dem_ds.RasterXSize, dem_ds.RasterYSize, 1, gdal.GDT_Byte)
    mask_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    mask_ds.SetProjection(dem_ds.GetProjection())

    if Path(nld_gpkg_path).is_file():
        vec_ds = ogr.Open(nld_gpkg_path)
        if vec_ds:
            layer = vec_ds.GetLayer()
            gdal.RasterizeLayer(mask_ds, [1], layer, burn_values=[1])

    levee_mask = mask_ds.GetRasterBand(1).ReadAsArray()
    masked_dem_arr = np.where(levee_mask == 1, nodata, dem_arr)

    out_ds = driver.Create("", dem_ds.RasterXSize, dem_ds.RasterYSize, 1, dem_band.DataType)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjection())

    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(float(nodata))
    out_band.WriteArray(masked_dem_arr)
    out_ds.FlushCache()

    return out_ds


def main():
    parser = argparse.ArgumentParser(description="Mask DEM using levee-protected vector polygons.")
    parser.add_argument("-dem", "--dem", required=True, help="Input DEM raster path")
    parser.add_argument("-nld", "--nld", required=True, help="Levee vector subset geopackage")
    parser.add_argument("-catchments", "--catchments", required=True, help="Catchments vector subset")
    parser.add_argument("-out", "--out", required=True, help="Output masked DEM raster path")
    parser.add_argument("-b", "--branch-attr", default="levpa_id", help="Branch ID attribute name")
    parser.add_argument("-i", "--branch-id", default="0", help="Current branch ID")
    parser.add_argument("-b0", "--branch-zero-id", default="0", help="Branch zero ID")
    parser.add_argument("-csv", "--csv", required=False, help="Path to levee levelpaths CSV")
    parser.add_argument("-l", "--levee-attr", default="feature_id", help="Levee ID attribute name")

    args = parser.parse_args()

    ds_dem = gdal.Open(args.dem)
    out_ds = mask_dem_in_memory(
        dem_ds=ds_dem,
        nld_gpkg_path=args.nld,
        catchments_gpkg_path=args.catchments,
        branch_id_attr=args.branch_attr,
        current_branch_id=args.branch_id,
        branch_zero_id=args.branch_zero_id,
        levee_id_attr=args.levee_attr,
    )

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(args.out, out_ds, options=["COMPRESS=LZW", "TILED=YES"])


if __name__ == "__main__":
    main()
