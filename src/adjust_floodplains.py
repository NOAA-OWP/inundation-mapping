#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import numpy as np
import rasterio as rio
import whitebox
from rasterstats import zonal_stats


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)
wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))


def adjust_floodplains(
    input_file, dem_file, distance_file, output_file, branch_polygons, branch_id, z_factor
):
    wbt.euclidean_distance(input_file, distance_file)

    with rio.open(distance_file) as src, rio.open(dem_file) as dem_src:
        profile = src.profile
        distance = src.read(1)
        dem = dem_src.read(1)
        dem_nodata = dem_src.nodata

    branch_polys = gpd.read_file(branch_polygons)
    branch_poly = branch_polys[branch_polys['levpa_id'] == branch_id]

    # Calculate the mean and standard deviation of the distance
    zs = zonal_stats(branch_poly, distance_file, stats=['mean', 'std'])
    distance_mean = zs[0]['mean']
    distance_std = zs[0]['std']
    distance_threshold = distance_mean + distance_std

    # Limit the distance to the mean + 1 std
    distance = np.where(distance <= distance_threshold, distance, np.nan)

    # Calculate the floodplain adjustment
    adjustment = z_factor - (distance / distance_threshold) * z_factor

    adjustment[np.isnan(adjustment)] = 0

    # Carry masks through the calculations
    adjustment[np.isnan(dem)] = np.nan

    # Adjust the DEM
    new_dem = dem - adjustment

    new_dem[new_dem < -5000] = dem_nodata

    profile.update(dtype=rio.float32, nodata=dem_nodata)

    with rio.open(output_file, 'w', **profile) as dst:
        dst.write(new_dem.astype(rio.float32), 1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Adjust floodplains')
    parser.add_argument('-i', '--input-file', help='Input file', type=str)
    parser.add_argument('-e', '--distance-file', help='Distance file', type=str)
    parser.add_argument('-d', '--dem-file', help='DEM file', type=str)
    parser.add_argument('-p', '--branch-polygons', help='Branch polygons', type=str)
    parser.add_argument('-b', '--branch-id', help='Branch ID', type=str)
    parser.add_argument('-o', '--output-file', help='Output file', type=str)
    parser.add_argument('-z', '--z-factor', help='Z factor', type=float)

    args = parser.parse_args()

    adjust_floodplains(**vars(args))
