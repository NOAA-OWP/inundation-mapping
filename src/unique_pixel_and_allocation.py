#!/usr/bin/env python3

import argparse
import os

import numpy as np
import rasterio
import whitebox


def stream_pixel_zones(stream_pixels, unique_stream_pixels):
    '''
    This function will assign a unique ID for each stream pixel and writes to file. It then uses this raster to run GRASS r.grow.distance tool to create the allocation and proximity rasters required to complete the lateral thalweg conditioning.

    Parameters
    ----------
    stream_pixels : STR
        Path to stream raster with value of 1. For example, demDerived_streamPixels.tif.
    unique_stream_pixels : STR
        Output path of raster containing unique ids for each stream pixel.

    Returns
    -------
    distance_grid : STR
        Path to output proximity raster (in meters).
    allocation_grid : STR
        Path to output allocation raster.

    '''
    # Set wbt envs
    wbt = whitebox.WhiteboxTools()
    wbt.set_verbose_mode(False)
    wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))

    workspace = os.path.dirname(unique_stream_pixels)
    base = os.path.basename(unique_stream_pixels)
    distance_grid = os.path.join(workspace, os.path.splitext(base)[0] + '_dist.tif')
    allocation_grid = os.path.join(workspace, os.path.splitext(base)[0] + '_allo.tif')

    # Import stream pixel raster
    with rasterio.open(stream_pixels) as temp:
        streams_profile = temp.profile
        streams = temp.read(1)

    # Create array that matches shape of streams raster with unique values for each cell. Dataype is float64.
    unique_vals = np.arange(streams.size, dtype='float64').reshape(*streams.shape)

    # At streams return the unique array value otherwise return 0 values
    stream_pixel_values = np.where(streams == 1, unique_vals, 0)

    del streams, unique_vals

    # Reassign dtype to be float64 (needs to be float64)
    streams_profile.update(dtype='float64')

    # Output to raster
    with rasterio.Env():
        with rasterio.open(unique_stream_pixels, 'w', **streams_profile) as raster:
            raster.write(stream_pixel_values, 1)

    # Compute allocation and proximity grids.
    wbt.euclidean_distance(stream_pixels, distance_grid)
    wbt.euclidean_allocation(unique_stream_pixels, allocation_grid)

    with rasterio.open(allocation_grid) as allocation_ds:
        allocation = allocation_ds.read(1)
        allocation_profile = allocation_ds.profile

    # Add stream channel ids
    allocation = np.where(allocation > 0, allocation, stream_pixel_values)

    del stream_pixel_values

    with rasterio.open(allocation_grid, 'w', **allocation_profile) as allocation_ds:
        allocation_ds.write(allocation, 1)

    del allocation

    return distance_grid, allocation_grid


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Produce unique stream pixel values and allocation/proximity grids'
    )
    parser.add_argument('-s', '--stream', help='raster to perform r.grow.distance', required=True)
    parser.add_argument(
        '-o', '--out', help='output raster of unique ids for each stream pixel', required=True
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # Rename variable inputs
    stream_pixels = args['stream']
    unique_stream_pixels = args['out']

    # Run stream_pixel_zones
    stream_pixel_zones(stream_pixels, unique_stream_pixels)
