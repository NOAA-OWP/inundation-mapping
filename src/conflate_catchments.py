#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import pandas as pd


gpd.options.io_engine = "pyogrio"


def conflate_catchments(
    reference_catchments_filename: str, target_catchments_filename: str, out_filename: str
):
    """
    Conflate reference (e.g., NWM feature_ids) to target (e.g., NHDPlus) network

    Parameters
    ----------
    reference_catchments_filename : str
        Reference catchments filename
    target_catchments_filename : str
        Target catchments filename
    out_filename : str
        Output filename
    """

    reference_catchments = gpd.read_file(reference_catchments_filename)
    target_catchments = gpd.read_file(target_catchments_filename)

    # Conflate NWM feature_ids to NHDPlus network
    conflated_catchments = gpd.sjoin(target_catchments, reference_catchments, how="left", op="intersects")

    # Drop unneeded columns
    conflated_catchments = conflated_catchments.drop(columns=["index_right"])

    # Write to file
    conflated_catchments.to_file(out_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Conflate NWM feature_ids to NHDPlus network')
    parser.add_argument('-ref', '--reference-catchments-filename', help='Reference catchments filename')
    parser.add_argument('-target', '--target-catchments-filename', help='Target catchments filename')
    parser.add_argument('-out', '--out-filename', help='Output filename')

    args = vars(parser.parse_args())

    conflate_catchments(**args)
