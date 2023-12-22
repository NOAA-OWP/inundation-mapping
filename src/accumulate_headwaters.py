#!/usr/bin/env python3

import argparse
import os

import numpy as np
import pyflwdir
import rasterio as rio


def accumulate_flow(flow_direction_filename, headwaters_filename, flow_accumulation_filename):
    """
    Accumulate headwaters along the flow direction.

    Parameters
    ----------
    flow_direction_filename : str
        Flow direction filename
    headwaters_filename : str
        Headwaters filename
    flow_accumulation_filename : str
        Flow accumulation filename

    Returns
    -------
    numpy.ndarray
        Accumulated flow.
    """

    assert os.path.isfile(flow_direction_filename), 'Flow direction raster does not exist.'

    # Read the flow direction raster
    with rio.open(flow_direction_filename) as src:
        data = src.read(1)
        nodata = src.nodata
        profile = src.profile
        # transform = src.transform
        # crs = src.crs
        # latlon = crs.to_epsg() == 4326

    # Convert the TauDEM flow direction raster to a pyflwdir flow direction array
    temp = data.copy()

    temp[data == 1] = 1
    temp[data == 2] = 128
    temp[data == 3] = 64
    temp[data == 4] = 32
    temp[data == 5] = 16
    temp[data == 6] = 8
    temp[data == 7] = 4
    temp[data == 8] = 2
    temp[data == nodata] = 247

    temp = temp.astype(np.uint8)

    flw = pyflwdir.from_array(temp, ftype='d8')

    # Read the flow direction raster
    with rio.open(headwaters_filename) as src:
        headwaters = src.read(1)
        nodata = src.nodata

    flowaccum = flw.accuflux(headwaters, nodata=nodata, direction='up')

    # Write the flow accumulation raster
    profile.update(dtype=flowaccum.dtype)
    with rio.open(flow_accumulation_filename, 'w', **profile) as dst:
        dst.write(flowaccum, 1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-fd', '--flow-direction-filename', help='Flow direction filename', required=True, type=str
    )
    parser.add_argument('-wg', '--headwaters-filename', help='Headwaters filename', required=True, type=str)
    parser.add_argument(
        '-fa', '--flow-accumulation-filename', help='Flow accumulation filename', required=True, type=str
    )

    args = parser.parse_args()

    accumulate_flow(**vars(args))
