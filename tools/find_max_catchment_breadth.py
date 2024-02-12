#!/usr/bin/env python3

import argparse
import os
from glob import glob, iglob

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


gpd.options.io_engine = "pyogrio"
os.environ["PYOGRIO_USE_ARROW"] = 1


def Find_max_catchment_breadth(hydrofabric_dir):
    catchments_fileNames = glob_file_paths_for_catchments(hydrofabric_dir)

    list_with_max_lengths_within_files = []

    for idx, cfn in enumerate(catchments_fileNames):
        catchments = gpd.read_file(cfn)

        lengths = catchments.apply(get_length_and_width_of_poly, axis=1)

        max_length_within_file = lengths.median()

        list_with_max_lengths_within_files.append(max_length_within_file)

        print(idx, max_length_within_file)

    print(max(list_with_max_lengths_within_files))


def glob_file_paths_for_catchments(hydrofabric_dir):
    file_pattern_to_glob = os.path.join(
        hydrofabric_dir, '**', 'gw_catchments_reaches_filtered_addedAttributes_crosswalked*.gpkg'
    )

    catchments_fileNames = iglob(file_pattern_to_glob)

    return catchments_fileNames


def get_length_and_width_of_poly(geodataframe_row):
    poly = geodataframe_row['geometry']

    # get minimum bounding box around polygon
    box = poly.minimum_rotated_rectangle

    # get coordinates of polygon vertices
    x, y = box.exterior.coords.xy

    # get length of bounding box edges
    edge_length = (
        Point(x[0], y[0]).distance(Point(x[1], y[1])),
        Point(x[1], y[1]).distance(Point(x[2], y[2])),
    )

    # get length of polygon as the longest edge of the bounding box
    length = max(edge_length)

    # get width of polygon as the shortest edge of the bounding box
    # width = min(edge_length)

    return length


if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(
        description='Find the maximum breadth of catchments in given hydrofabric directory'
    )
    parser.add_argument('-y', '--hydrofabric-dir', help='Path to hydrofabric directory', required=True)

    args = vars(parser.parse_args())

    Find_max_catchment_breadth(**args)
