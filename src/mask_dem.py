#!/usr/bin/env python3

import argparse
import os

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from rasterio.mask import mask


def mask_dem(
    dem_filename: str,
    nld_filename: str,
    levee_id_attribute: str,
    catchments_filename: str,
    out_dem_filename: str,
    branch_id_attribute: str,
    branch_id: int,
    branch_zero_id: int,
    levee_levelpaths: str,
):
    """
    Masks levee-protected areas from DEM in branch 0 or if the level path is associated with a levee
    (determined in src/associate_levelpaths_with_levees.py). Also masks parts of levee-protected areas
    through which level paths flow that are not in the level path catchment.

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
        Path to levee-levelpath association file (generated by src/associate_levelpaths_with_levees.py)
    """

    assert os.path.exists(dem_filename), f"DEM file {dem_filename} does not exist"
    assert os.path.exists(nld_filename), f"NLD file {nld_filename} does not exist"
    assert os.path.exists(catchments_filename), f"Catchments file {catchments_filename} does not exist"

    dem_masked = None

    with rio.open(dem_filename) as dem:
        dem_profile = dem.profile.copy()
        nodata = dem.nodata

        if branch_id == branch_zero_id:
            # Mask if branch zero
            with fiona.open(nld_filename) as leveed:
                geoms = [feature["geometry"] for feature in leveed]

            if len(geoms) > 0:
                dem_masked, _ = mask(dem, geoms, invert=True)

        elif os.path.exists(levee_levelpaths):
            # Mask levee-protected areas protected against level path
            levee_levelpaths = pd.read_csv(levee_levelpaths)

            # Select levees associated with branch
            levee_levelpaths = levee_levelpaths[levee_levelpaths[branch_id_attribute] == branch_id]

            # Get levee IDs
            levelpath_levees = list(levee_levelpaths[levee_id_attribute])

            if len(levelpath_levees) > 0:
                leveed = gpd.read_file(nld_filename)

                # Get geometries of levee protected areas associated with levelpath
                geoms = [
                    feature['geometry']
                    for i, feature in leveed.iterrows()
                    if feature[levee_id_attribute] in levelpath_levees
                ]

                if len(geoms) > 0:
                    dem_masked, _ = mask(dem, geoms, invert=True)

            # Mask levee-protected areas not protected against level path
            catchments = gpd.read_file(catchments_filename)
            leveed = gpd.read_file(nld_filename)

            leveed_area_catchments = gpd.overlay(catchments, leveed, how="union")

            # Select levee catchments not associated with level path
            levee_catchments_to_mask = leveed_area_catchments.loc[
                ~leveed_area_catchments[levee_id_attribute].isna() & leveed_area_catchments['ID'].isna(), :
            ]

            geoms = [feature["geometry"] for i, feature in levee_catchments_to_mask.iterrows()]

            levee_catchments_masked = None
            if len(geoms) > 0:
                levee_catchments_masked, _ = mask(dem, geoms, invert=True)

            out_masked = None
            if dem_masked is None:
                if levee_catchments_masked is not None:
                    out_masked = levee_catchments_masked

            else:
                if levee_catchments_masked is None:
                    out_masked = dem_masked
                else:
                    out_masked = np.where(levee_catchments_masked == nodata, nodata, dem_masked)

            if out_masked is not None:
                with rio.open(out_dem_filename, "w", **dem_profile, BIGTIFF='YES') as dest:
                    dest.write(out_masked[0, :, :], indexes=1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mask levee-protected areas from DEM')
    parser.add_argument('-dem', '--dem-filename', help='DEM filename', required=True, type=str)
    parser.add_argument(
        '-nld', '--nld-filename', help='NLD levee-protected areas filename', required=True, type=str
    )
    parser.add_argument(
        '-catchments', '--catchments-filename', help='NWM catchments filename', required=True, type=str
    )
    parser.add_argument('-l', '--levee-id-attribute', help='Levee ID attribute name', required=True, type=str)
    parser.add_argument(
        '-out', '--out-dem-filename', help='DEM filename to be written', required=True, type=str
    )
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
