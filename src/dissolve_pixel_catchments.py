#!/usr/bin/env python3

import argparse
import os
import subprocess

import geopandas as gpd


gpd.options.io_engine = 'pyogrio'


def dissolve_pixel_catchments(pixel_catchments_raster, reaches_file, output_file):
    """
    Dissolve pixel catchments to reaches

    Parameters
    ----------
    pixel_catchments_raster : str
        Path to pixel catchments file
    reaches_file : str
        Path to reaches file
    output_file : str
        Path to output file
    """

    # Read the data
    assert os.path.exists(
        pixel_catchments_raster
    ), f'Pixel catchments file not found: {pixel_catchments_raster}'
    assert os.path.exists(reaches_file), f'Reaches file not found: {reaches_file}'

    pixel_catchments_vector = os.path.splitext(pixel_catchments_raster)[0] + ".gpkg"

    # Polygonize the pixel catchments
    subprocess.call(
        [
            "gdal_polygonize.py",
            "-q",
            "-8",
            "-f",
            "GPKG",
            pixel_catchments_raster,
            pixel_catchments_vector,
            "catchments",
            "HydroID",
        ]
    )

    pixel_catchments = gpd.read_file(pixel_catchments_vector)
    reaches = gpd.read_file(reaches_file)

    pixel_catchments = pixel_catchments.sjoin(reaches, how='left', predicate='intersects')

    # Dissolve gage watersheds to reaches
    dissolved = pixel_catchments[['id', 'geometry']].dissolve(by='id')
    dissolved = dissolved.rename(columns={'id': 'HydroID'})

    # Save the dissolved GeoDataFrame
    dissolved.to_file(output_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dissolve pixel catchments (gage watersheds) to reaches')
    parser.add_argument('-gw', '--pixel-catchments-raster', type=str, help='Path to pixel catchments raster')
    parser.add_argument('-r', '--reaches-file', type=str, help='Path to reaches file')
    parser.add_argument('-o', '--output-file', type=str, help='Path to output file')

    args = vars(parser.parse_args())

    dissolve_pixel_catchments(**args)
