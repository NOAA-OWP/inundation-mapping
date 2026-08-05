#!/usr/bin/env python3
"""
adjust_thalweg_lateral.py
-------------------------
Adjusts stream thalweg elevations based on lateral minimum zonal statistics.
Supports direct in-memory dataset calls as well as standalone CLI execution.
"""

import argparse

import numpy as np
from osgeo import gdal


gdal.UseExceptions()


def adjust_thalweg_lateral_in_memory(
    dem_ds: gdal.Dataset,
    stream_pixels_ds: gdal.Dataset,
    allo_ds: gdal.Dataset,
    dist_ds: gdal.Dataset,
    max_dist: float = 50.0,
    elev_threshold: float = 2.0,
) -> gdal.Dataset:
    """Adjusts stream thalweg elevations based on lateral minimum zonal stats in RAM."""
    dem_arr = dem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    stream_arr = stream_pixels_ds.GetRasterBand(1).ReadAsArray()
    allo_arr = allo_ds.GetRasterBand(1).ReadAsArray()
    dist_arr = dist_ds.GetRasterBand(1).ReadAsArray()

    adjusted_dem = np.copy(dem_arr)
    valid_mask = (dist_arr <= max_dist) & (allo_arr > 0)

    unique_allocations = np.unique(allo_arr[valid_mask])
    for alloc_id in unique_allocations:
        zone_mask = (allo_arr == alloc_id) & (dist_arr <= max_dist)
        min_elev = np.min(dem_arr[zone_mask])

        stream_mask = (allo_arr == alloc_id) & (stream_arr > 0)
        if np.any(stream_mask):
            current_elev = dem_arr[stream_mask]
            if np.abs(current_elev - min_elev) <= elev_threshold:
                adjusted_dem[stream_mask] = min_elev

    driver = gdal.GetDriverByName("MEM")
    out_ds = driver.Create("", dem_ds.RasterXSize, dem_ds.RasterYSize, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjection())

    nodata = dem_ds.GetRasterBand(1).GetNoDataValue()
    if nodata is None:
        nodata = -9999.0

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(float(nodata))
    band.WriteArray(adjusted_dem)
    out_ds.FlushCache()

    return out_ds


def main():
    parser = argparse.ArgumentParser(description="Adjust thalweg lateral minimum elevation.")
    parser.add_argument("-e", "--dem", required=True, help="Input DEM raster path")
    parser.add_argument("-s", "--stream-pixels", required=True, help="Input stream pixels raster path")
    parser.add_argument("-a", "--allocation", required=True, help="Input stream allocation raster path")
    parser.add_argument("-d", "--distance", required=True, help="Input distance raster path")
    parser.add_argument("-t", "--max-dist", type=float, default=50.0, help="Maximum search distance")
    parser.add_argument("-o", "--output", required=True, help="Output adjusted DEM raster path")
    parser.add_argument("-th", "--threshold", type=float, default=2.0, help="Elevation threshold tolerance")

    args = parser.parse_args()

    dem_ds = gdal.Open(args.dem)
    sp_ds = gdal.Open(args.stream_pixels)
    allo_ds = gdal.Open(args.allocation)
    dist_ds = gdal.Open(args.distance)

    out_ds = adjust_thalweg_lateral_in_memory(
        dem_ds=dem_ds,
        stream_pixels_ds=sp_ds,
        allo_ds=allo_ds,
        dist_ds=dist_ds,
        max_dist=args.max_dist,
        elev_threshold=args.threshold,
    )

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(args.output, out_ds, options=["COMPRESS=LZW", "TILED=YES"])


if __name__ == "__main__":
    main()
