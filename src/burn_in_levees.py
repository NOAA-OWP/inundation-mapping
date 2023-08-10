#!/usr/bin/env python3

import rasterio
import numpy as np
import argparse
from utils.shared_functions import mem_profile


@mem_profile
def burn_in_levees(dem_filename, nld_filename, out_dem_filename):
    # TODO Document this code
    dem = rasterio.open(dem_filename)
    nld = rasterio.open(nld_filename)

    dem_data = dem.read(1)
    nld_data = nld.read(1)

    no_data = nld.nodata

    nld_m = np.where(nld_data == int(no_data), -9999.0, (nld_data).astype(rasterio.float32))

    dem_profile = dem.profile.copy()

    dem_nld_burn = np.maximum(dem_data, nld_m)

    with rasterio.open(out_dem_filename, "w", **dem_profile, BIGTIFF='YES') as dest:
        dest.write(dem_nld_burn, indexes=1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Burn in NLD elevations')
    parser.add_argument('-dem', '--dem-filename', help='DEM filename', required=True, type=str)
    parser.add_argument('-nld', '--nld-filename', help='NLD filename', required=True, type=str)
    parser.add_argument(
        '-out', '--out-dem-filename', help='out DEM filename', required=True, type=str
    )

    args = vars(parser.parse_args())

    dem_filename = args['dem_filename']
    nld_filename = args['nld_filename']
    out_dem_filename = args['out_dem_filename']

    burn_in_levees(dem_filename, nld_filename, out_dem_filename)
