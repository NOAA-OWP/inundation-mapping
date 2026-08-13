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
from scipy.ndimage import distance_transform_edt, minimum


gdal.UseExceptions()


def adjust_thalweg_lateral_in_memory(
    dem_ds: gdal.Dataset,
    stream_pixels_ds: gdal.Dataset,
    allocation_ds: gdal.Dataset,
    distance_ds: gdal.Dataset = None,
    distance_threshold: float = 50.0,
    elev_threshold: float = 0.0,
) -> gdal.Dataset:
    """Vectorized lateral thalweg elevation adjustment using SciPy zonal minimums."""
    dem_band = dem_ds.GetRasterBand(1)
    dem_arr = dem_band.ReadAsArray().astype(np.float32)
    dem_nodata = dem_band.GetNoDataValue()

    stream_arr = stream_pixels_ds.GetRasterBand(1).ReadAsArray()
    allo_arr = allocation_ds.GetRasterBand(1).ReadAsArray().astype(np.int32)

    # Distance calculation or lookup
    if distance_ds is not None:
        dist_arr = distance_ds.GetRasterBand(1).ReadAsArray()
    else:
        dist_arr = distance_transform_edt(stream_arr == 0)

    # Valid mask for DEM
    valid_dem_mask = (dem_arr != dem_nodata) if dem_nodata is not None else np.ones_like(dem_arr, dtype=bool)

    # Compute Zonal Minimum Elevation per Stream Allocation Zone
    labels = np.unique(allo_arr[allo_arr > 0])

    if len(labels) == 0:
        min_elevs = np.array([])
    else:
        min_elevs = minimum(dem_arr, labels=allo_arr, index=labels)

    max_label = allo_arr.max() if allo_arr.size > 0 else 0
    lookup = np.full(max_label + 1, np.nan, dtype=np.float32)
    lookup[labels] = min_elevs

    zonal_min_grid = lookup[allo_arr]

    # Vectorized Condition Check
    adjustment_mask = (dist_arr <= distance_threshold) & valid_dem_mask & ~np.isnan(zonal_min_grid)

    if elev_threshold > 0:
        adjustment_mask &= dem_arr - zonal_min_grid <= elev_threshold

    dem_adj_arr = dem_arr.copy()
    dem_adj_arr[adjustment_mask] = np.minimum(dem_arr[adjustment_mask], zonal_min_grid[adjustment_mask])

    # Package GDAL MEM Dataset
    cols = dem_ds.RasterXSize
    rows = dem_ds.RasterYSize
    driver = gdal.GetDriverByName("MEM")

    out_ds = driver.Create("", cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjectionRef())

    out_band = out_ds.GetRasterBand(1)
    if dem_nodata is not None:
        out_band.SetNoDataValue(dem_nodata)
    out_band.WriteArray(dem_adj_arr)

    return out_ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Adjust thalweg lateral elevations.")
    # Add CLI arguments here if executing standalone...


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
