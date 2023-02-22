#!/usr/bin/env python3

import os
import geopandas as gpd
import fiona
import rasterio as rio
from rasterio.mask import mask
import argparse
from utils.shared_functions import mem_profile

@mem_profile
def mask_dem(dem_filename, nld_filename, out_dem_filename, stream_layer, branch_id_attribute, branch_id, order_attribute, branch_zero_id):
    """
    Masks levee-protected areas from DEM in branch 0 or if stream order is at least the minimum order (max - 1)
    """
    if os.path.exists(stream_layer):
        streams_df = gpd.read_file(stream_layer, ignore_geometry=True)

        # Rasterize if branch zero or if stream order is at least the minimum order (max - 1)
        if (branch_id == branch_zero_id) or (streams_df.loc[streams_df[branch_id_attribute].astype(int)==branch_id, order_attribute].max() >= streams_df[order_attribute].max() - 1):

            with rio.open(dem_filename) as dem, fiona.open(nld_filename) as leveed:
                dem_profile = dem.profile.copy()

                geoms = [feature["geometry"] for feature in leveed]

                # Mask out levee-protected areas from DEM
                out_dem_masked, _ = mask(dem, geoms, invert=True)

                with rio.open(out_dem_filename, "w", **dem_profile, BIGTIFF='YES') as dest:
                    dest.write(out_dem_masked[0,:,:], indexes=1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mask levee-protected areas from DEM')
    parser.add_argument('-dem','--dem-filename', help='DEM filename', required=True,type=str)
    parser.add_argument('-nld','--nld-filename', help='NLD filename', required=True,type=str)
    parser.add_argument('-out','--out-dem-filename', help='out DEM filename', required=True,type=str)
    parser.add_argument('-s', '--stream-layer', help='Stream layer filename', required=True)
    parser.add_argument('-b', '--branch-id-attribute', help='Branch ID attribute name', required=False, default='levpa_id')
    parser.add_argument('-i', '--branch-id', help='Branch ID', type=int, required='True')
    parser.add_argument('-a', '--order-attribute', help='Stream order attribute name', required=False, default='order_')
    parser.add_argument('-b0', '--branch-zero-id', help='Branch zero ID', type=int, required=False, default=0)

    args = vars(parser.parse_args())

    mask_dem(**args)
