#!/usr/bin/env python3

import argparse
import os

# import geopandas as gpd
import numpy as np
import rasterio as rio
import whitebox


# from rasterstats import zonal_stats


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)
wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))


def adjust_floodplains(input_file, dem_file, wbd_file, distance_file, output_file, z_factor):
    """
    Adjusts the floodplains in a DEM based on the distance to a given input file.

    Parameters
    ----------
    input_file : str
        The input raster file to calculate the distance from.
    dem_file : str
        The DEM file to adjust.
    wbd_file : str
        The watershed boundary dataset file.
    distance_file : str
        The output distance file.
    output_file : str
        The output adjusted DEM file.
    z_factor : float
        The z-factor to adjust the DEM.

    Returns
    -------
    None
    """

    wbt.euclidean_distance(input_file, distance_file)

    with rio.open(distance_file) as src, rio.open(dem_file) as dem_src:
        profile = src.profile
        distance = src.read(1)
        dem = dem_src.read(1)
        dem_nodata = dem_src.nodata

    # branch_polys = gpd.read_file(branch_polygons)
    # branch_poly = branch_polys[branch_polys['levpa_id'] == branch_id]

    # fema_flood_zones = gpd.read_file(fema_flood_zones_file)

    # # Clip the FEMA flood zones to the branch polygon
    # fema_flood_zones_clipped = gpd.clip(fema_flood_zones, branch_poly)

    # # Mask the distance raster with fema_flood_zones_clipped
    # distance_mask = np.zeros_like(distance)
    # for geom in fema_flood_zones_clipped.geometry:
    #     mask = rio.features.geometry_mask(
    #         [geom], out_shape=distance.shape, transform=src.transform, invert=True
    #     )
    #     distance_mask[mask] = 1
    # distance = np.where(distance_mask == 1, distance, np.nan)

    # # Save distance raster
    # with rio.open(distance_file, 'w', **profile) as dst:
    #     dst.write(distance.astype(rio.float32), 1)

    # # Calculate the mean and standard deviation of the distance
    # zs = zonal_stats(fema_flood_zones_clipped, distance_file, stats=['mean', 'std'])

    # distance_mean = zs[0]['mean']
    # distance_std = zs[0]['std']
    # distance_threshold = distance_mean + distance_std

    # with rio.open(distance_file) as src:
    #     distance = src.read(1)

    distance_threshold = 3000.0
    # z_factor = z_factor * distance_threshold / 1000.0

    # Limit the distance to the mean + 1 std
    distance = np.where(distance <= distance_threshold, distance, np.nan)

    # Save distance raster
    with rio.open(distance_file, 'w', **profile) as dst:
        dst.write(distance.astype(rio.float32), 1)

    # Calculate the floodplain adjustment
    adjustment = np.where(
        distance < distance_threshold,
        ((distance_threshold - distance) / distance_threshold) ** 2 * z_factor,
        0,
    )

    adjustment[np.isnan(adjustment)] = 0

    # Carry masks through the calculations
    adjustment[np.isnan(dem)] = np.nan

    # Adjust the DEM
    new_dem = dem - adjustment

    # Subtract z_factor from the DEM outside of wbd
    # wbd = gpd.read_file(wbd_file)
    # wbd = wbd.to_crs(dem_src.crs)
    # wbd_mask = np.zeros_like(dem)
    # for geom in wbd.geometry:
    #     mask = rio.features.geometry_mask(
    #         [geom], out_shape=dem.shape, transform=dem_src.transform, invert=False
    #     )
    #     wbd_mask[mask] = 1
    # new_dem = np.where(wbd_mask == 1, new_dem - z_factor, new_dem)

    new_dem[new_dem < -5000] = dem_nodata

    profile.update(dtype=rio.float32, nodata=dem_nodata)

    with rio.open(output_file, 'w', **profile) as dst:
        dst.write(new_dem.astype(rio.float32), 1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Adjust floodplains')
    parser.add_argument('-i', '--input-file', help='Input file', type=str)
    parser.add_argument('-e', '--distance-file', help='Distance file', type=str)
    parser.add_argument('-d', '--dem-file', help='DEM file', type=str)
    parser.add_argument('-w', '--wbd-file', help='WBD file', type=str)
    parser.add_argument('-o', '--output-file', help='Output file', type=str)
    parser.add_argument('-z', '--z-factor', help='Z factor', type=float)

    args = parser.parse_args()

    adjust_floodplains(**vars(args))
