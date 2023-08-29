#!/usr/bin/env python3

import argparse
import os

import fiona
import geopandas as gpd
import pandas as pd
import rasterio as rio
from rasterio.mask import mask

from utils.shared_functions import mem_profile


@mem_profile
def mask_dem(
    dem_filename: str,
    nld_filename: str,
    levee_id_attribute: str,
    out_dem_filename: str,
    branch_id_attribute: str,
    branch_id: int,
    branch_zero_id: int,
    levee_levelpaths: str,
):
    """
    Masks levee-protected areas from DEM in branch 0 or if the level path is associated with a levee
    (determined in src/associate_levelpaths_with_levees.py).

    Parameters
    ----------
    dem_filename: str
        Path to DEM file.
    nld_filename: str
        Path to levee-protected areas file.
    levee_id_attribute: str
        Name of levee ID attribute.
    out_dem_filename: str
        Path to write masked DEM.
    branch_id_attribute: str
        Name of branch ID attribute.
    branch_id: int
        Branch ID number
    branch_zero_id: int
        Branch 0 ID number
    levee_levelpaths: str
        Path to levee-levelpath association file.
    """

    # Rasterize if branch zero
    if branch_id == branch_zero_id:
        with rio.open(dem_filename) as dem, fiona.open(nld_filename) as leveed:
            dem_profile = dem.profile.copy()

            geoms = [feature["geometry"] for feature in leveed]

            # Mask out levee-protected areas from DEM
            out_dem_masked, _ = mask(dem, geoms, invert=True)

            with rio.open(out_dem_filename, "w", **dem_profile, BIGTIFF='YES') as dest:
                dest.write(out_dem_masked[0, :, :], indexes=1)

    elif os.path.exists(levee_levelpaths):
        levee_levelpaths = pd.read_csv(levee_levelpaths)

        levee_levelpaths = levee_levelpaths[levee_levelpaths[branch_id_attribute] == branch_id]

        levelpath_levees = list(levee_levelpaths[levee_id_attribute])

        if len(levelpath_levees) > 0:
            with rio.open(dem_filename) as dem:  # , fiona.open(nld_filename) as leveed:
                leveed = gpd.read_file(nld_filename)
                dem_profile = dem.profile.copy()

                geoms = [
                    feature['geometry']
                    for i, feature in leveed.iterrows()
                    if feature[levee_id_attribute] in levelpath_levees
                ]

                # Mask out levee-protected areas from DEM
                out_dem_masked, _ = mask(dem, geoms, invert=True)

                with rio.open(out_dem_filename, "w", **dem_profile, BIGTIFF='YES') as dest:
                    dest.write(out_dem_masked[0, :, :], indexes=1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mask levee-protected areas from DEM')
    parser.add_argument('-dem', '--dem-filename', help='DEM filename', required=True, type=str)
    parser.add_argument(
        '-nld', '--nld-filename', help='NLD levee-protected areas filename', required=True, type=str
    )
    parser.add_argument('-l', '--levee-id-attribute', help='Levee ID attribute name', required=True, type=str)
    parser.add_argument('-out', '--out-dem-filename', help='out DEM filename', required=True, type=str)
    parser.add_argument(
        '-b', '--branch-id-attribute', help='Branch ID attribute name', required=True, type=str
    )
    parser.add_argument('-i', '--branch-id', help='Branch ID', type=int, required='True')
    parser.add_argument('-b0', '--branch-zero-id', help='Branch zero ID', type=int, required=False, default=0)
    parser.add_argument(
        '-csv', '--levee-levelpaths', help='Levee - levelpath layer filename', type=str, required=True
    )

    args = vars(parser.parse_args())

    mask_dem(**args)
