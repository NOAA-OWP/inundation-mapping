#!/usr/bin/env python3

import fiona
import rasterio as rio
from rasterio.mask import mask
# import numpy as np
import argparse
# from utils.shared_functions import mem_profile

# @mem_profile
def mask_dem(dem_filename, nld_filename, out_dem_filename):# , flowdir_filename)
    """
    Masks levee-protected areas from DEM
    """
    with rio.open(dem_filename) as dem, fiona.open(nld_filename) as leveed:#, \
        # rio.open(flowdir_filename) as fdir:
        dem_data = dem.read(1)
        dem_profile = dem.profile.copy()

        geoms = [feature["geometry"] for feature in leveed]

        # Mask out levee-protected areas from DEM
        out_dem_masked, out_transform = mask(dem, geoms, invert=True)


        with rio.open(out_dem_filename, "w", **dem_profile, BIGTIFF='YES') as dest:
            dest.write(out_dem_masked[0,:,:], indexes=1)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mask levee-protected areas from DEM')
    parser.add_argument('-dem','--dem-filename', help='DEM filename', required=True,type=str)
    # parser.add_argument('-fd', '--flowdir-filename', help='Flow Direction filename', required=True, type=str)
    parser.add_argument('-nld','--nld-filename', help='NLD filename', required=True,type=str)
    parser.add_argument('-out','--out-dem-filename', help='out DEM filename', required=True,type=str)

    args = vars(parser.parse_args())

    mask_dem(**args)
