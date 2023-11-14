#!/usr/bin/env python3

import argparse

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

    wbt.fill_depressions(dem_filename, out_filename, fix_flats=True, flat_increment=None, max_depth=None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill depressions in DEM")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument("-out", "--out-filename", help="Out filename", required=True, type=str)

    args = vars(parser.parse_args())

    fill_depressions(**args)
