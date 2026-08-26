#!/usr/bin/env python3
"""
adjust_thalweg_lateral.py
-------------------------
Adjusts stream thalweg elevations based on lateral minimum zonal statistics.
Matches dev branch logic.
"""

import argparse

import numpy as np
from numba import njit, typed, types
from osgeo import gdal


@njit
def _make_zone_min_dict_numba(elevation_window, zone_window, cost_window, cost_tolerance):
    zone_min_dict = typed.Dict.empty(types.int32, types.float32)
    n = elevation_window.size

    for i in range(n):
        if cost_window[i] <= cost_tolerance:
            elev_val = elevation_window[i]
            if elev_val > 0:  # Ignore bad/NoData elevation values
                zone_id = types.int32(zone_window[i])
                if zone_id in zone_min_dict:
                    if elev_val < zone_min_dict[zone_id]:
                        zone_min_dict[zone_id] = elev_val
                else:
                    zone_min_dict[zone_id] = elev_val

    return zone_min_dict


@njit
def _minimize_thalweg_elevation_numba(
    dem_array, zone_window, thalweg_window, zone_min_dict, lateral_elevation_threshold
):
    dem_to_return = np.copy(dem_array)
    n = dem_array.size

    for i in range(n):
        if thalweg_window[i] == 1:  # CRITICAL: ONLY adjust actual stream thalweg pixels!
            zone_id = types.int32(zone_window[i])
            if zone_id in zone_min_dict:
                zone_min_elevation = zone_min_dict[zone_id]
                dem_thalweg_elevation = dem_array[i]
                elevation_difference = dem_thalweg_elevation - zone_min_elevation

                if (zone_min_elevation < dem_thalweg_elevation) and (
                    elevation_difference <= lateral_elevation_threshold
                ):
                    dem_to_return[i] = zone_min_elevation

    return dem_to_return


def adjust_thalweg_lateral_in_memory(
    dem_ds: gdal.Dataset,
    stream_pixels_ds: gdal.Dataset,
    allocation_ds: gdal.Dataset,
    distance_ds: gdal.Dataset,
    distance_threshold: float = 50.0,
    elev_threshold: float = 2.0,
) -> gdal.Dataset:
    """In-memory thalweg lateral minimum adjustment matching dev logic."""
    dem_band = dem_ds.GetRasterBand(1)
    dem_arr = dem_band.ReadAsArray().astype(np.float32).ravel()
    dem_nodata = dem_band.GetNoDataValue()

    stream_arr = stream_pixels_ds.GetRasterBand(1).ReadAsArray().astype(np.int32).ravel()
    allo_arr = allocation_ds.GetRasterBand(1).ReadAsArray().astype(np.int32).ravel()
    dist_arr = distance_ds.GetRasterBand(1).ReadAsArray().astype(np.float32).ravel()

    # 1. Build zone minimum dictionary (matching dev)
    zone_min_dict = _make_zone_min_dict_numba(
        elevation_window=dem_arr,
        zone_window=allo_arr,
        cost_window=dist_arr,
        cost_tolerance=int(distance_threshold),
    )

    # 2. Minimize elevations ONLY on thalweg pixels (matching dev)
    adjusted_dem_flat = _minimize_thalweg_elevation_numba(
        dem_array=dem_arr,
        zone_window=allo_arr,
        thalweg_window=stream_arr,
        zone_min_dict=zone_min_dict,
        lateral_elevation_threshold=float(elev_threshold),
    )

    adjusted_dem_2d = adjusted_dem_flat.reshape((dem_ds.RasterYSize, dem_ds.RasterXSize))

    # Package GDAL MEM dataset
    driver = gdal.GetDriverByName("MEM")
    out_ds = driver.Create("", dem_ds.RasterXSize, dem_ds.RasterYSize, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_ds.GetGeoTransform())
    out_ds.SetProjection(dem_ds.GetProjectionRef())

    out_band = out_ds.GetRasterBand(1)
    if dem_nodata is not None:
        out_band.SetNoDataValue(dem_nodata)
    out_band.WriteArray(adjusted_dem_2d)

    return out_ds


if __name__ == "__main__":
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
        allocation_ds=allo_ds,
        distance_ds=dist_ds,
        distance_threshold=args.max_dist,
        elev_threshold=args.threshold,
    )

    driver = gdal.GetDriverByName("GTiff")
    driver.CreateCopy(args.output, out_ds, options=["COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES"])
