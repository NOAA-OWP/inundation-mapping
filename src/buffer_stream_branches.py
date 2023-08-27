#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd

from stream_branches import StreamBranchPolygons, StreamNetwork


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='Generates branch polygons')
    parser.add_argument('-a', '--dem-domain', help='DEM domain file', required=False, type=str)
    parser.add_argument('-s', '--streams', help='Streams file to branch', required=True)
    parser.add_argument('-i', '--branch-id', help='Attribute with branch ids', required=True)
    parser.add_argument(
        '-d',
        '--buffer-distance',
        help='Distance to buffer branches to create branch polygons',
        required=True,
        type=int,
    )
    parser.add_argument(
        '-b', '--branches', help='Branch polygons out file name', required=False, default=None
    )
    parser.add_argument(
        '-v', '--verbose', help='Verbose printing', required=False, default=None, action='store_true'
    )

    # extract to dictionary
    args = vars(parser.parse_args())

    (streams_file, branch_id_attribute, buffer_distance, stream_polygons_file, dem_domain, verbose) = (
        args["streams"],
        args["branch_id"],
        args["buffer_distance"],
        args["branches"],
        args['dem_domain'],
        args["verbose"],
    )

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
