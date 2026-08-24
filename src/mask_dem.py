#!/usr/bin/env python3
"""
mask_dem.py
-----------
Masks DEM cells within levee-protected areas.
Supports direct in-memory dataset calls as well as standalone CLI execution.
"""

import argparse
import os
from pathlib import Path
from typing import Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from rasterio.mask import mask
from shapely.geometry import box


def clip_geoms_to_raster_bounds(geoms: list, bounds) -> list:
    """Clips a list of geometries to the raster bounding box to prevent out-of-bounds masking."""
    raster_box = box(*bounds)
    clipped = []
    for g in geoms:
        if g is not None and not g.is_empty:
            inter = g.intersection(raster_box)
            if not inter.is_empty:
                clipped.append(inter)
    return clipped


def mask_dem_in_memory(
    dem_filename: str,
    nld_filename: str,
    catchments_filename: str,
    levee_id_attribute: str = "feature_id",
    branch_id_attribute: str = "levpa_id",
    branch_id: Union[int, str] = "0",
    branch_zero_id: Union[int, str] = "0",
    levee_levelpaths: str = None,
) -> tuple[np.ndarray, dict]:
    """Masks DEM cells within levee-protected areas directly in RAM using Rasterio masking logic from dev.

    Returns:
    --------
    tuple[np.ndarray, dict]:
        - masked_dem_array: 2D numpy array of masked DEM elevations.
        - dem_profile: Rasterio dataset profile dictionary for writing.
    """
    assert os.path.exists(dem_filename), f"DEM file {dem_filename} does not exist"
    assert os.path.exists(nld_filename), f"NLD file {nld_filename} does not exist"

    dem_masked = None
    levee_catchments_masked = None

    with rio.open(dem_filename) as dem:
        dem_profile = dem.profile.copy()
        nodata = dem.nodata if dem.nodata is not None else -9999.0
        dem_crs = dem.crs
        dem_arr = dem.read(1)

        str_branch = str(branch_id)
        str_branch_zero = str(branch_zero_id)

        if str_branch == str_branch_zero:
            # Mask if branch zero
            leveed = gpd.read_file(nld_filename, engine="fiona")
            if leveed.crs != dem_crs:
                leveed = leveed.to_crs(dem_crs)
            geoms = [feature for feature in leveed.geometry]
            geoms = clip_geoms_to_raster_bounds(geoms, dem.bounds)

            if len(geoms) > 0:
                masked_data, _ = mask(dem, geoms, invert=True)
                dem_masked = masked_data[0]

        elif levee_levelpaths and os.path.exists(levee_levelpaths):
            # Mask levee-protected areas protected against level path
            if str(catchments_filename).endswith(".parquet"):
                catchments = gpd.read_parquet(catchments_filename)
            else:
                catchments = gpd.read_file(catchments_filename, engine="fiona")

            levee_levelpaths_df = pd.read_csv(levee_levelpaths)
            leveed = gpd.read_file(nld_filename, engine="fiona")

            if leveed.crs != dem_crs:
                leveed = leveed.to_crs(dem_crs)

            # Select levees associated with branch
            branch_levees = levee_levelpaths_df[
                levee_levelpaths_df[branch_id_attribute].astype(str) == str_branch
            ]
            levelpath_levees = list(branch_levees[levee_id_attribute])

            if len(levelpath_levees) > 0:
                geoms = [
                    feature
                    for i, feature in leveed[
                        leveed[levee_id_attribute].isin(levelpath_levees)
                    ].geometry.items()
                ]
                geoms = clip_geoms_to_raster_bounds(geoms, dem.bounds)

                if len(geoms) > 0:
                    masked_data, _ = mask(dem, geoms, invert=True)
                    dem_masked = masked_data[0]

            # Mask levee-protected areas not protected against level path
            if catchments.crs != dem_crs:
                catchments = catchments.to_crs(dem_crs)

            leveed_area_catchments = gpd.overlay(catchments, leveed, how="union")

            # Select levee catchments not associated with level path
            levee_catchments_to_mask = leveed_area_catchments.loc[
                ~leveed_area_catchments[levee_id_attribute].isna() & leveed_area_catchments["ID"].isna(), :
            ]

            geoms = [feature for feature in levee_catchments_to_mask.geometry]
            geoms = clip_geoms_to_raster_bounds(geoms, dem.bounds)

            if len(geoms) > 0:
                masked_data, _ = mask(dem, geoms, invert=True)
                levee_catchments_masked = masked_data[0]

        # Combine masked layers
        if dem_masked is None:
            out_masked = levee_catchments_masked if levee_catchments_masked is not None else dem_arr
        else:
            if levee_catchments_masked is None:
                out_masked = dem_masked
            else:
                out_masked = np.where(levee_catchments_masked == nodata, nodata, dem_masked)

    return out_masked, dem_profile


def mask_dem(
    dem_filename: str,
    nld_filename: str,
    catchments_filename: str,
    out_dem_filename: str,
    levee_id_attribute: str = "feature_id",
    branch_id_attribute: str = "levpa_id",
    branch_id: Union[int, str] = "0",
    branch_zero_id: Union[int, str] = "0",
    levee_levelpaths: str = None,
) -> None:
    """CLI wrapper writing masked DEM output file to disk."""
    out_masked, dem_profile = mask_dem_in_memory(
        dem_filename=dem_filename,
        nld_filename=nld_filename,
        catchments_filename=catchments_filename,
        levee_id_attribute=levee_id_attribute,
        branch_id_attribute=branch_id_attribute,
        branch_id=branch_id,
        branch_zero_id=branch_zero_id,
        levee_levelpaths=levee_levelpaths,
    )

    dem_profile.update(BIGTIFF="YES", compress="LZW", tiled=True)
    with rio.open(out_dem_filename, "w", **dem_profile) as dest:
        dest.write(out_masked, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mask levee-protected areas from DEM")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument(
        "-nld", "--nld-filename", help="NLD levee-protected areas filename", required=True, type=str
    )
    parser.add_argument(
        "-catchments", "--catchments-filename", help="NWM catchments filename", required=True, type=str
    )
    parser.add_argument(
        "-l",
        "--levee-id-attribute",
        help="Levee ID attribute name",
        required=False,
        default="feature_id",
        type=str,
    )
    parser.add_argument(
        "-out", "--out-dem-filename", help="DEM filename to be written", required=True, type=str
    )
    parser.add_argument(
        "-b",
        "--branch-id-attribute",
        help="Branch ID attribute name",
        required=False,
        default="levpa_id",
        type=str,
    )
    parser.add_argument("-i", "--branch-id", help="Branch ID", required=False, default="0")
    parser.add_argument("-b0", "--branch-zero-id", help="Branch zero ID", required=False, default="0")
    parser.add_argument(
        "-csv",
        "--levee-levelpaths",
        help="Levee - levelpath layer filename",
        type=str,
        required=False,
        default=None,
    )

    args = vars(parser.parse_args())

    mask_dem(**args)
