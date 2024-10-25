#!/usr/bin/env python3

import argparse

import geopandas as gpd


gpd.options.io_engine = "pyogrio"


def Edit_points(
    stream_reaches,
    branch_id_attribute,
    reach_points,
    out_reach_points=None,
    out_pixel_points=None,
    verbose=False,
):
    if verbose:
        print("Editing points files ...")

    if verbose:
        print("Loading files ...")
    stream_reaches = gpd.read_file(stream_reaches)
    stream_reaches = stream_reaches.astype({'HydroID': int})

    reach_points = gpd.read_file(reach_points)
    reach_points['HydroID'] = reach_points['id'].copy()

    # merge
    if verbose:
        print("Merging points ...")
    reach_points = reach_points.merge(
        stream_reaches.loc[:, ["HydroID", branch_id_attribute]], how='inner', on='HydroID'
    )

    # join on HydroID to add branch_id
    if out_reach_points is not None:
        reach_points.to_file(out_reach_points, driver='GPKG', index=False, engine='fiona')

    # make pixel points
    if verbose:
        print("Generating pixel points ...")

    pixel_points = reach_points.copy()
    pixel_points['id'] = list(range(1, len(pixel_points) + 1))

    if out_pixel_points is not None:
        pixel_points.to_file(out_pixel_points, driver='GPKG', index=False, engine='fiona')

    return (reach_points, pixel_points)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Edit points to include branch ids')
    parser.add_argument('-i', '--stream-reaches', help='Input stream network', required=True)
    parser.add_argument(
        '-b', '--branch-id-attribute', help='Name of the branch attribute desired', required=True
    )
    parser.add_argument('-r', '--reach-points', help='Name of the branch attribute desired', required=True)
    parser.add_argument(
        '-o', '--out-reach-points', help='Output stream network', required=False, default=None
    )
    parser.add_argument(
        '-p', '--out-pixel-points', help='Dissolved output stream network', required=False, default=None
    )
    parser.add_argument(
        '-v', '--verbose', help='Verbose output', required=False, default=False, action='store_true'
    )

    args = vars(parser.parse_args())

    Edit_points(**args)
