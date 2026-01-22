#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import whitebox
from rasterio.mask import mask
from shapely.geometry import mapping


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)
wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))


def adjust_floodplains(
    input_file: str,
    dem_file: str,
    nwm_catchments: str,
    nwm_streams: str,
    nwm_levelpaths: str,
    distance_file: str,
    output_file: str,
    distance_threshold: float,
    slope_exponent: float,
    z_factor: float,
    branch_polygons: str,
    branch_id: str,
    fema_flood_zones_file: str,
    fema_flood_zones_layer: str,
):
    """
    Adjusts the floodplains in a DEM based on the distance to a given input file.

    Parameters
    ----------
    input_file : str
        The input raster file to calculate the distance from.
    dem_file : str
        The DEM file to adjust.dataset file.
    nwm_catchments : str
        The file containing the NWM catchments.
    nwm_streams : str
        The file containing the NWM streams.
    nwm_levelpaths : str
        The file containing the NWM levelpaths.
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
    fema_flood_zones_layer : str.

    Returns
    -------
    None
    """

    wbt.euclidean_distance(input_file, distance_file)

    catchments = gpd.read_file(nwm_catchments)
    streams = gpd.read_file(nwm_streams)
    levelpaths = gpd.read_file(nwm_levelpaths)
    branch_polys = gpd.read_file(branch_polygons)
    branch_poly = branch_polys[branch_polys['levpa_id'] == branch_id]

    # Filter levelpaths by branch
    levelpaths['ID'] = levelpaths['ID'].astype(int)
    levelpaths = levelpaths[levelpaths['levpa_id'] == branch_id]

    # Find streams that flow to the levelpaths
    ids = levelpaths['ID'].tolist()

    # Get all upstream streams
    def get_upstream_streams(hydro_ids, streams_df):
        upstream_streams = streams_df[streams_df['ID'].isin(hydro_ids)]
        for hydro_id in hydro_ids:
            direct_upstream = streams_df[streams_df['to'] == hydro_id]
            if not direct_upstream.empty:
                upstream_streams = pd.concat([upstream_streams, direct_upstream])
                upstream_streams = pd.concat(
                    [upstream_streams, get_upstream_streams(direct_upstream['ID'].tolist(), streams_df)]
                )
        return upstream_streams.drop_duplicates()

    upstream_streams = get_upstream_streams(ids, streams)

    # Filter catchments by upstream streams
    upstream_catchments = catchments[catchments['ID'].isin(upstream_streams['ID'].tolist())].dissolve()

    with rio.open(distance_file) as src, rio.open(dem_file) as dem_src:
        profile = src.profile
        distance = src.read(1)
        dem = dem_src.read(1)
        dem_nodata = dem_src.nodata

        distance_mask = np.zeros_like(distance)

        # Use NFHL flood hazard zones
        if os.path.exists(fema_flood_zones_file):
            nfhl_layers = gpd.list_layers(fema_flood_zones_file)['name'].tolist()
            # Initialize the variable as None so it always exists
            fema_flood_zones_availability_mask = None
            # Read combined layer if it exists
            if 'combined' in nfhl_layers:
                fema_flood_zones_availability_mask = gpd.read_file(fema_flood_zones_file, layer='combined')

            # Mask out areas inside the FEMA flood zone availability
            if 'availability' in nfhl_layers:
                fema_flood_zones_availability = gpd.read_file(fema_flood_zones_file, layer='availability')
                if fema_flood_zones_availability_mask is not None:
                    fema_flood_zones_availability_mask = (
                        pd.concat([fema_flood_zones_availability_mask, fema_flood_zones_availability])
                        .drop_duplicates(subset='geometry', keep=False)
                        .dissolve()
                    )
                else:
                    fema_flood_zones_availability_mask = fema_flood_zones_availability.copy()

            if fema_flood_zones_layer in nfhl_layers:
                # Read the FEMA flood zones layer
                fema_flood_zones = gpd.read_file(fema_flood_zones_file, layer=fema_flood_zones_layer)

                # Exclude fema_flood_zones from fema_flood_zones_availability_mask
                fema_flood_zones_availability_mask = gpd.overlay(
                    fema_flood_zones_availability_mask, fema_flood_zones, how='difference'
                )

            fema_flood_zones_availability_mask = gpd.clip(fema_flood_zones_availability_mask, branch_poly)

            if fema_flood_zones_availability_mask.empty:
                return  # No flood zones to process, exit the function

            # Extract the polygon geometry (as a list of GeoJSON-like dicts)
            # If you have multiple polygons in gdf, this will create a list of them
            geometries = [mapping(geom) for geom in fema_flood_zones_availability_mask.geometry]

            # Perform the masking
            distance_mask, out_transform = mask(src, geometries, crop=False, nodata=np.nan, invert=True)
            distance_mask = distance_mask[0]  # Extract the first band

        else:
            distance_mask = distance

        # Clip to the upstream catchment polygon
        geometries = [mapping(geom) for geom in upstream_catchments.geometry]
        distance_clipped, out_transform = mask(src, geometries, crop=False, nodata=np.nan)
        distance_clipped = distance_clipped[0]  # Extract the first band

    distance = np.where(
        ~np.isnan(distance_clipped) & (distance_mask <= distance_threshold), distance_mask, np.nan
    )

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
    parser.add_argument('-l', '--fema-flood-zones-layer', help='FEMA flood zones layer', type=str)
    parser.add_argument('-c', '--nwm-catchments', help='NWM catchments file', type=str)
    parser.add_argument('-n', '--nwm-streams', help='NWM streams file', type=str)
    parser.add_argument('-lp', '--nwm-levelpaths', help='NWM levelpaths file', type=str)

    args = parser.parse_args()

    adjust_floodplains(**vars(args))
