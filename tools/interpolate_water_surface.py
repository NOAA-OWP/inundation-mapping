#!/usr/bin/env python3

import argparse

import numpy as np
import rasterio
import rasterio.mask
from rasterio.fill import fillnodata


def interpolate_wse(
    inundation_depth_raster,
    hydroconditioned_dem,
    output_depth_raster,
    output_interpolated_wse=None,
    max_distance=20,
    smooth_iterations=2,
):
    with rasterio.open(inundation_depth_raster) as depth:
        depth_rast = depth.read(1)
        profile = depth.profile
    with rasterio.open(hydroconditioned_dem) as huc_dem:
        dem = huc_dem.read()
        dem[np.where(dem == huc_dem.profile['nodata'])] = np.nan

    # Calculate water surface elevation grid
    wse_rast = depth_rast + dem
    wse_rast = np.where(depth_rast == profile['nodata'], profile['nodata'], wse_rast)
    wse_rast = np.where(depth_rast == 0.0, 0.0, wse_rast)

    # Run interpolation
    wse_interpolated = fillnodata(
        wse_rast,
        mask=wse_rast.astype(np.intc),
        max_search_distance=max_distance,
        smoothing_iterations=smooth_iterations,
    )

    # Write interpolated water surface elevation raster if specified
    if output_interpolated_wse:
        with rasterio.open(output_interpolated_wse, 'w', **profile) as dst:
            wse_interpolated[np.where(np.isnan(wse_interpolated))] = profile['nodata']
            dst.write(wse_interpolated)

    # Calculate depth from new interpolated WSE
    final_depth = wse_interpolated - dem
    final_depth[np.where(final_depth <= 0)] = profile['nodata']
    # Remove levees
    final_depth[np.where(dem == profile['nodata'])] = profile['nodata']
    # Write interpolated depth raster
    with rasterio.open(output_depth_raster, 'w', **profile) as dst:
        dst.write(final_depth)


if __name__ == '__main__':
    # TODO parse arguments
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-y', '--hydrofabric-dir', help='Bounding box file', required=True)
    parser.add_argument('-f', '--forecast-file', help='WBD file', required=True)
    parser.add_argument('-i', '--inundation-file', help='WBD file', required=False, default=None)

    args = vars(parser.parse_args())

    interpolate_wse(**args)
