#!/usr/bin/env python3
import argparse
import os
import re

import numpy as np
import pyflwdir
import rasterio
import whitebox


wbt_pattern = re.compile('(^\*|\%$)')


def fill_depressions_wbt(workspace, branch_zero_id):
    '''
    Wrapper around whitebox tools fill_depressions method:
    https://www.whiteboxgeo.com/manual/wbt_book/available_tools/hydrological_analysis.html#filldepressions
    '''

    # Set wbt envs
    wbt = whitebox.WhiteboxTools()
    wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))  # need to set path prior to running
    wbt.set_verbose_mode(True)

    if branch_zero_id:
        input_dem = os.path.join(workspace, f'dem_burned_{branch_zero_id}.tif')
        output_dem = os.path.join(workspace, f'dem_burned_filled_{branch_zero_id}.tif')
    else:
        input_dem = os.path.join(workspace, 'dem_burned.tif')
        output_dem = os.path.join(workspace, 'dem_burned_filled.tif')

    wbt.fill_depressions(
        input_dem, output_dem, fix_flats=False, flat_increment=None, max_depth=None, callback=wbt_callback
    )


def wbt_callback(value):
    if not re.search(wbt_pattern, value):
        print("Whitebox fill_depressions tool: " + value)


def fill_depressions_pyflwdir(workspace, branch_zero_id):
    '''
    Wrapper around pyflwdir fill_depressions method:
    https://deltares.github.io/pyflwdir/latest/_generated/pyflwdir.dem.fill_depressions.html#pyflwdir-dem-fill-depressions
    '''

    if branch_zero_id:
        input_dem = os.path.join(workspace, f'dem_burned_{branch_zero_id}.tif')
        output_dem = os.path.join(workspace, f'dem_burned_filled_{branch_zero_id}.tif')
    else:
        input_dem = os.path.join(workspace, 'dem_burned.tif')
        output_dem = os.path.join(workspace, 'dem_burned_filled.tif')

    with rasterio.open(input_dem, "r") as src:
        elevtn = src.read(1)
        # nodata = src.nodata
        profile = src.profile
        # transform = src.transform
        # crs = src.crs

    output = pyflwdir.dem.fill_depressions(
        elevtn, outlets='edge', idxs_pit=None, nodata=-9999.0, max_depth=-1.0, elv_max=None, connectivity=8
    )

    # Output is two arrays, the 0 index is elevtn_out: 2D array - Depression filled elevation
    #  and 1st index is d8: 2D array of uint8 - D8 flow directions
    dem_burned_filled = output[0]

    profile.update(dtype=dem_burned_filled.dtype)

    with rasterio.open(output_dem, 'w', **profile, BIGTIFF='YES') as dst:
        dst.write(dem_burned_filled, 1)


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Use appropriate python library\'s fill depression method')
    parser.add_argument('-w', '--workspace', help='Workspace', required=True)
    parser.add_argument(
        '-b',
        '--branch_zero_id',
        help='If branch_zero_id is provided, update output path',
        required=False,
        default=None,
    )
    parser.add_argument('-m', '--method', help='Method to use; WBT or pyflwdir', required=True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # rename variable inputs
    workspace = args['workspace']
    branch_zero_id = args['branch_zero_id']
    method = args['method']

    ## Run WBT fill_depressions method
    if method == "wbt":
        print("Using WBT Fill Depressions method")
        fill_depressions_wbt(workspace, branch_zero_id)

    # Run pyflwdir fill_depressions method (some 3m resolution DEM and 1m DEM)
    if method == "pyflwdir":
        print("Using Pyflwdir Fill Depressions method")
        fill_depressions_pyflwdir(workspace, branch_zero_id)
