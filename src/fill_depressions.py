#!/usr/bin/env python3

import argparse
import os

import rasterio as rio
import whitebox


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)


def fill_depressions(dem_filename: str, out_filename: str):
    """
    Fill depressions in DEM

    Parameters
    ----------
    dem_filename : str
        DEM filename
    out_filename : str
        Out filename
    """

    assert os.path.isfile(dem_filename), 'ERROR: DEM file not found: ' + str(dem_filename)

    # Fill depressions
    if (
        wbt.fill_depressions(dem_filename, out_filename, fix_flats=True, flat_increment=None, max_depth=None)
        != 0
    ):
        raise Exception('ERROR: WhiteboxTools fill_depressions failed')

    # Convert from double to float
    with rio.open(out_filename) as src:
        profile = src.profile
        profile.update(dtype=rio.float32, count=1, compress="lzw")

        data = src.read(1).astype(rio.float32)

    # Write output
    with rio.open(out_filename, "w", **profile) as dst:
        dst.write(data, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill depressions in DEM")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument("-out", "--out-filename", help="Out filename", required=True, type=str)

    args = vars(parser.parse_args())

    fill_depressions(**args)
