#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import Point

from utils.shared_functions import getDriver
from utils.shared_variables import PREP_PROJECTION


gpd.options.io_engine = "pyogrio"


def convert_grid_cells_to_points(raster, index_option, output_points_filename=False):
    # Input raster
    if isinstance(raster, str):
        raster = rasterio.open(raster, 'r')

    elif isinstance(raster, rasterio.io.DatasetReader):
        pass

    else:
        raise TypeError("Pass raster dataset or filepath for raster")

    (upper_left_x, x_size, x_rotation, upper_left_y, y_rotation, y_size) = raster.get_transform()
    indices = np.nonzero(raster.read(1) >= 1)

    id = [None] * len(indices[0])
    points = [None] * len(indices[0])

    # Iterate over the Numpy points..
    i = 1
    for y_index, x_index in zip(*indices):
        x = x_index * x_size + upper_left_x + (x_size / 2)  # add half the cell size
        y = y_index * y_size + upper_left_y + (y_size / 2)  # to center the point
        points[i - 1] = Point(x, y)
        if index_option == 'reachID':
            reachID = np.array(
                list(raster.sample((Point(x, y).coords), indexes=1))
            ).item()  # check this; needs to add raster cell value + index
            id[i - 1] = reachID * 10000 + i  # reachID + i/100
        elif (index_option == 'featureID') | (index_option == 'pixelID'):
            id[i - 1] = i
        i += 1

    del raster

    pointGDF = gpd.GeoDataFrame({'id': id, 'geometry': points}, crs=PREP_PROJECTION, geometry='geometry')

    del id, points

    if output_points_filename is False:
        return pointGDF
    else:
        pointGDF.to_file(
            output_points_filename, driver=getDriver(output_points_filename), index=False, engine='fiona'
        )


if __name__ == '__main__':
    # Parse arguments
    """
    USAGE:
        reachID_grid_to_vector_points.py
            -r <flows_grid_IDs raster file>
            -i <reachID or featureID or pixelID>
            -p <output points filename>
    """

    parser = argparse.ArgumentParser(description='Converts a raster to points')
    parser.add_argument('-r', '--raster', help='Raster to be converted to points', required=True, type=str)
    parser.add_argument(
        '-i',
        '--index-option',
        help='Indexing option',
        required=True,
        type=str,
        choices=['reachID', 'featureID', 'pixelID'],
    )
    parser.add_argument(
        '-p',
        '--output-points-filename',
        help='Output points layer filename',
        required=False,
        type=str,
        default=False,
    )

    args = vars(parser.parse_args())

    raster = args['raster']
    index_option = args['index_option']
    output_points_filename = args['output_points_filename']

    convert_grid_cells_to_points(raster, index_option, output_points_filename)
