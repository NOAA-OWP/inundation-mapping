#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd

from stream_branches import StreamBranchPolygons, StreamNetwork


gpd.options.io_engine = "pyogrio"


def buffer_stream_branches(
    streams_file: str,
    branch_id_attribute: str,
    buffer_distance: int,
    stream_polygons_file: str = None,
    dem_domain: str = None,
    verbose: bool = False,
):
    if os.path.exists(streams_file):
        # load file
        stream_network = StreamNetwork.from_file(
            filename=streams_file,
            branch_id_attribute=branch_id_attribute,
            values_excluded=None,
            attribute_excluded=None,
            verbose=verbose,
        )

        # make stream polygons
        stream_polys = StreamBranchPolygons.buffer_stream_branches(
            stream_network, buffer_distance=buffer_distance, verbose=verbose
        )

        # Clip to DEM domain
        if os.path.exists(dem_domain):
            dem_domain = gpd.read_file(dem_domain)
            stream_polys.geometry = gpd.clip(stream_polys, dem_domain).geometry

        stream_polys.write(stream_polygons_file, verbose=verbose)


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description="Generates branch polygons")
    parser.add_argument("-a", "--dem-domain", help="DEM domain file", required=False, type=str)
    parser.add_argument("-s", "--streams-file", help="Streams file to branch", required=True)
    parser.add_argument("-i", "--branch-id-attribute", help="Attribute with branch ids", required=True)
    parser.add_argument(
        "-d",
        "--buffer-distance",
        help="Distance to buffer branches to create branch polygons",
        required=True,
        type=int,
    )
    parser.add_argument(
        "-b", "--stream-polygons-file", help="Branch polygons out file name", required=False, default=None
    )
    parser.add_argument(
        "-v", "--verbose", help="Verbose printing", required=False, default=None, action="store_true"
    )

    # extract to dictionary
    args = vars(parser.parse_args())

    buffer_stream_branches(**args)
