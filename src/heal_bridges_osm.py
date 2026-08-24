#!/usr/bin/env python3

import argparse
import glob
import os
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from rasterio import features
from rasterio.warp import Resampling, reproject
from rasterstats import zonal_stats

from utils.io import write_geodataframe
from utils.spatial import sjoin


threatened_percent = 0.75


def process_non_lidar_osm(osm_gdf, hand_grid_array, hand_grid_profile, non_lidar_buffer):
    non_lidar_osm_gdf = osm_gdf[osm_gdf["has_lidar_tif"] == "N"].copy()

    if non_lidar_osm_gdf.empty:
        return None, hand_grid_array

    non_lidar_osm_gdf["geometry"] = non_lidar_osm_gdf.geometry.buffer(
        non_lidar_buffer, resolution=non_lidar_buffer
    )

    # Get max hand values for each bridge
    stats = zonal_stats(
        non_lidar_osm_gdf["geometry"],
        hand_grid_array,
        affine=hand_grid_profile["transform"],
        stats="max",
        nodata=hand_grid_profile["nodata"],
        all_touched=True,
    )
    non_lidar_osm_gdf.loc[:, "threshold_hand"] = pd.to_numeric([x.get("max") for x in stats], errors="coerce")
    non_lidar_osm_gdf = non_lidar_osm_gdf.sort_values(by="threshold_hand", ascending=False)

    # Burn the bridges into the HAND grid
    shapes = (
        (geom, value) for geom, value in zip(non_lidar_osm_gdf.geometry, non_lidar_osm_gdf.threshold_hand)
    )
    features.rasterize(
        shapes=shapes, out=hand_grid_array, transform=hand_grid_profile["transform"], all_touched=False
    )

    return non_lidar_osm_gdf, hand_grid_array


def process_lidar_osm(osm_gdf, hand_grid_array, hand_grid_profile, lidar_buffer, bridge_elev_diff_raster):
    lidar_osm_gdf = osm_gdf[osm_gdf["has_lidar_tif"] == "Y"].copy()

    if lidar_osm_gdf.empty:
        return None, hand_grid_array

    with rasterio.open(bridge_elev_diff_raster) as diff_grid:
        diff_grid_array = diff_grid.read(1)
        diff_grid_transform = diff_grid.transform
        diff_grid_crs = diff_grid.crs

        # Ensure CRS and transform match
        if hand_grid_profile["crs"] != diff_grid_crs or hand_grid_profile["transform"] != diff_grid_transform:
            reprojected_diff_grid_array = np.empty_like(hand_grid_array, dtype=diff_grid_array.dtype)
            reproject(
                source=diff_grid_array,
                destination=reprojected_diff_grid_array,
                src_transform=diff_grid_transform,
                src_crs=diff_grid_crs,
                dst_transform=hand_grid_profile["transform"],
                dst_crs=hand_grid_profile["crs"],
                dst_shape=hand_grid_array.shape,
                resampling=Resampling.nearest,
            )
        else:
            reprojected_diff_grid_array = diff_grid_array

    nodata_value = hand_grid_profile.get("nodata", None)
    updated_hand_grid_array = np.where(
        (hand_grid_array == nodata_value) | (reprojected_diff_grid_array == nodata_value),
        nodata_value,
        hand_grid_array + reprojected_diff_grid_array,
    )

    lidar_osm_gdf = osm_gdf[osm_gdf["has_lidar_tif"] == "Y"].copy()
    lidar_osm_gdf["geometry"] = lidar_osm_gdf.geometry.buffer(lidar_buffer, resolution=lidar_buffer)
    stats = zonal_stats(
        lidar_osm_gdf["geometry"],
        updated_hand_grid_array,
        affine=hand_grid_profile["transform"],
        stats="median",
        nodata=hand_grid_profile["nodata"],
        all_touched=True,
    )
    lidar_osm_gdf.loc[:, "threshold_hand"] = pd.to_numeric([x.get("median") for x in stats], errors="coerce")

    return lidar_osm_gdf, updated_hand_grid_array


def heal_bridges_osm_in_memory(
    source_hand_raster,
    bridge_elev_diff_raster,
    bridge_vector_file,
    non_lidar_buffer,
    lidar_buffer,
    catchments_gdf,
    bridge_centroids_path,
):
    """Pure In-Memory execution function for integration into delineate_hydros_and_produce_HAND.py."""
    if not os.path.exists(source_hand_raster):
        print(f"-- no hand grid, {source_hand_raster}")
        return None, None

    if os.path.exists(bridge_vector_file):
        osm_gdf = gpd.read_file(bridge_vector_file)
        osm_gdf["centroid_geometry"] = osm_gdf.centroid
    else:
        print(f"-- no OSM file, {bridge_vector_file}")
        return None, None

    with rasterio.open(source_hand_raster, "r") as hand_grid:
        hand_grid_profile = hand_grid.profile.copy()
        hand_grid_array = hand_grid.read(1)

    non_lidar_osm_gdf, hand_grid_array = process_non_lidar_osm(
        osm_gdf, hand_grid_array, hand_grid_profile, non_lidar_buffer
    )
    lidar_osm_gdf, updated_hand_grid_array = process_lidar_osm(
        osm_gdf, hand_grid_array, hand_grid_profile, lidar_buffer, bridge_elev_diff_raster
    )

    valid_osm_gdfs = [gdf for gdf in [non_lidar_osm_gdf, lidar_osm_gdf] if gdf is not None]

    if valid_osm_gdfs:
        osm_gdf = pd.concat(valid_osm_gdfs, ignore_index=True)
    else:
        osm_gdf = gpd.GeoDataFrame()

    # Switch geometry over to the centroid points
    if not osm_gdf.empty and "centroid_geometry" in osm_gdf.columns:
        osm_gdf["geometry"] = osm_gdf["centroid_geometry"]
        osm_gdf = osm_gdf.drop(columns="centroid_geometry")

        osm_gdf = osm_gdf.loc[osm_gdf.threshold_hand > 0]

        # Spatial Join catchments in RAM
        osm_gdf = sjoin(osm_gdf, catchments_gdf[["HydroID", "feature_id", "order_", "geometry"]], how="inner")
        osm_gdf = osm_gdf.drop(columns="index_right", errors="ignore")

        # Calculate threatened stage
        osm_gdf["threshold_hand_75"] = osm_gdf.threshold_hand * threatened_percent

        # Extract branch_id from destination file path context
        m_branch = re.search(r"branches/(\d{10}|0)/", str(bridge_centroids_path))
        if m_branch:
            branch_id = m_branch.group(1)
        else:
            branch_id = str(Path(bridge_centroids_path).stem.split("_")[-1])

        osm_gdf["branch"] = str(branch_id)
        osm_gdf["mainstem"] = 0 if str(branch_id) == "0" else 1

        if not osm_gdf.empty and bridge_centroids_path:
            write_geodataframe(osm_gdf, bridge_centroids_path, index=False)

    return updated_hand_grid_array, hand_grid_profile


def process_bridges_in_huc(
    source_hand_raster,
    bridge_elev_diff_raster,
    bridge_vector_file,
    non_lidar_buffer,
    lidar_buffer,
    catchments,
    bridge_centroids,
):
    """File I/O CLI Wrapper supporting both Parquet and GPKG catchment inputs."""
    if not os.path.exists(catchments):
        print(f"-- no catchments file, {catchments}")
        return

    # Read catchments using Parquet or GPKG dynamically matching dev
    if str(catchments).endswith(".parquet"):
        catchments_gdf = gpd.read_parquet(catchments)
    else:
        catchments_gdf = gpd.read_file(catchments)

    updated_hand_array, hand_profile = heal_bridges_osm_in_memory(
        source_hand_raster=source_hand_raster,
        bridge_elev_diff_raster=bridge_elev_diff_raster,
        bridge_vector_file=bridge_vector_file,
        non_lidar_buffer=non_lidar_buffer,
        lidar_buffer=lidar_buffer,
        catchments_gdf=catchments_gdf,
        bridge_centroids_path=bridge_centroids,
    )

    if updated_hand_array is not None:
        # Write the new HAND grid to disk
        with rasterio.open(source_hand_raster, "w", **hand_profile) as new_hand_grid:
            new_hand_grid.write(updated_hand_array, 1)


def flow_lookup(stages, hydroid, hydroTable):
    single_hydroTable = hydroTable.loc[hydroTable.HydroID == hydroid]
    return_flows = np.interp(
        stages, single_hydroTable.loc[:, "stage"], single_hydroTable.loc[:, "discharge_cms"]
    )
    return return_flows


def flows_from_hydrotable(bridge_pnts, hydroTable):
    bridge_pnts[["threshold_discharge", "threshold_discharge75"]] = bridge_pnts.apply(
        lambda row: flow_lookup((row.threshold_hand, row.threshold_hand_75), row.HydroID, hydroTable),
        axis=1,
        result_type="expand",
    )
    # Convert stages and discharges to ft and cfs respectively
    bridge_pnts["threshold_hand_ft"] = bridge_pnts["threshold_hand"] * 3.28084
    bridge_pnts["threshold_hand_75_ft"] = bridge_pnts["threshold_hand_75"] * 3.28084
    bridge_pnts["threshold_discharge_cfs"] = bridge_pnts["threshold_discharge"] * 35.3147
    bridge_pnts["threshold_discharge_75_cfs"] = bridge_pnts["threshold_discharge75"] * 35.3147

    return bridge_pnts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heals HAND for osm bridges")

    parser.add_argument(
        "-g",
        "--source_hand_raster",
        help="REQUIRED: Path and file name of source output raster",
        required=True,
    )

    parser.add_argument(
        "-d",
        "--bridge_elev_diff_raster",
        help="REQUIRED: Path of bridge_elev_diff raster file",
        required=True,
    )

    parser.add_argument(
        "-s", "--bridge_vector_file", help="REQUIRED: A gpkg that contains the bridges vectors", required=True
    )

    parser.add_argument(
        "-b1",
        "--non_lidar_buffer",
        help="OPTIONAL: Buffer to apply to non-lidar OSM bridges. Default value is 10m (on each side)",
        required=False,
        default=10,
        type=float,
    )

    parser.add_argument(
        "-b2",
        "--lidar_buffer",
        help="OPTIONAL: Buffer to apply to lidar OSM bridges. Default value is 1.5m (on each side)",
        required=False,
        default=1.5,
        type=float,
    )

    parser.add_argument(
        "-p",
        "--catchments",
        help="REQUIRED: Path and file name of the catchments geopackage or parquet",
        required=True,
    )

    parser.add_argument(
        "-c",
        "--bridge_centroids",
        help="REQUIRED: Path and file name of the output bridge centroid geopackage",
        required=True,
    )

    args = vars(parser.parse_args())

    process_bridges_in_huc(**args)
