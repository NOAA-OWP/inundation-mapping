#!/usr/bin/env python3

import argparse
import os

import numpy as np
import rasterio as rio
import whitebox


def compute_slopes(dem_file: str, slope_file: str, units: str = 'proportion'):
    '''
    Compute slopes from DEM

    Parameters
    ----------
    dem_file : STR
        Path to input DEM raster.
    slope_file : STR
        Path to output slopes raster.
    units : STR
        Units of output slopes raster: degrees, percent, proportion (default), or radians.
    '''

    # Set wbt envs
    wbt = whitebox.WhiteboxTools()
    wbt.set_verbose_mode(False)
    wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))

    # Compute slopes
    wbt.slope(dem_file, slope_file, zfactor=None, units='percent' if units == 'proportion' else units)

    # Convert percent to proportion
    if units == 'proportion':
        with rio.open(slope_file) as slope:
            slope_data = slope.read(1)
            slope_data = slope_data / 100.0
            slope_data[np.where(slope_data == slope.nodata / 100.0)] = slope.nodata
            slope_profile = slope.profile
        with rio.Env():
            with rio.open(slope_file, 'w', **slope_profile) as raster:
                raster.write(slope_data, 1)


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Compute slopes from DEM')
    parser.add_argument('-d', '--dem-file', help='Input DEM raster', required=True)
    parser.add_argument('-s', '--slope-file', help='Output slopes raster', required=True)
    parser.add_argument(
        '-u',
        '--units',
        help='Units of output slopes raster: degrees, percent, proportion (default), or radians)',
        required=False,
        default='proportion',
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # Run stream_pixel_zones
    compute_slopes(**args)
