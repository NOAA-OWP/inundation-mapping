#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import numpy as np
import rasterio as rio
import rasterio.features as features
import whitebox


# from rasterstats import zonal_stats


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)
wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))


def adjust_floodplains(
    input_file: str,
    dem_file: str,
    distance_file: str,
    output_file: str,
    distance_threshold: float,
    slope_exponent: float,
    z_factor: float,
    branch_polygons: str,
    branch_id: str,
    fema_flood_zones_file: str,
):
    """
    Adjusts the floodplains in a DEM based on the distance to a given input file.

    Parameters
    ----------
    input_file : str
        The input raster file to calculate the distance from.
    dem_file : str
        The DEM file to adjust.dataset file.
    distance_file : str
        The output distance file.
    output_file : str
        The output adjusted DEM file.
    distance_threshold : float
        The distance threshold to limit the adjustment.
    slope_exponent : float
        The slope exponent to adjust the DEM.
    z_factor : float
        The z-factor to adjust the DEM.
    branch_polygons : str
        The file containing the branch polygons.
    branch_id : int
        The ID of the branch to adjust.
    fema_flood_zones_file : str
        The file containing the FEMA flood zones.

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

    branch_polys = gpd.read_file(branch_polygons)
    branch_poly = branch_polys[branch_polys['levpa_id'] == branch_id]

    fema_flood_zones = gpd.read_file(fema_flood_zones_file)

    if os.path.exists(fema_flood_zones_file):
        fema_flood_zones = gpd.read_file(fema_flood_zones_file)

        # Clip the FEMA flood zones to the branch polygon
        fema_flood_zones_clipped = gpd.clip(fema_flood_zones, branch_poly)

        # Mask the distance raster with fema_flood_zones_clipped
        distance_mask = np.zeros_like(distance)
        for geom in fema_flood_zones_clipped.geometry:
            mask = features.geometry_mask(
                [geom], out_shape=distance.shape, transform=src.transform, invert=True
            )
            distance_mask[mask] = 1
        distance = np.where(distance_mask == 1, distance, np.nan)

    # Limit the distance to the mean + 1 std
    distance = np.where(distance <= distance_threshold, distance, np.nan)

    # Save distance raster
    with rio.open(distance_file, 'w', **profile) as dst:
        dst.write(distance.astype(rio.float32), 1)

    # Calculate the floodplain adjustment
    adjustment = np.where(
        distance < distance_threshold,
        ((distance_threshold - distance) / distance_threshold) ** slope_exponent * z_factor,
        0,
    )

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
    parser.add_argument('-o', '--output-file', help='Output file', type=str)
    parser.add_argument('-t', '--distance-threshold', help='Distance threshold', type=float)
    parser.add_argument('-s', '--slope-exponent', help='Slope exponent', type=float)
    parser.add_argument('-z', '--z-factor', help='Z factor', type=float)
    parser.add_argument('-p', '--branch-polygons', help='Branch polygons file', type=str)
    parser.add_argument('-b', '--branch-id', help='Branch ID', type=str)
    parser.add_argument('-f', '--fema-flood-zones-file', help='FEMA flood zones file', type=str)

    args = parser.parse_args()

    adjust_floodplains(**vars(args))
