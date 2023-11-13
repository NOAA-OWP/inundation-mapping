#!/usr/bin/env python3

import argparse

import rasterio as rio
import whitebox


wbt = whitebox.WhiteboxTools()


def flowdir_d8(dem_filename: str, out_filename: str):
    """
    Compute flow direction using D8 method

    Parameters
    ----------
    dem_filename : str
        DEM filename
    out_filename : str
        Out filename

    """

    wbt.d8_pointer(dem_filename, out_filename, esri_pntr=False)

    with rio.open(out_filename) as src:
        profile = src.profile

        dem = src.read(1)

    data = dem.copy()

    # Reclassify WhiteboxTools flow direction to TauDEM flow direction
    data[dem == 1] = 2
    data[dem == 2] = 1
    data[dem == 4] = 8
    data[dem == 8] = 7
    data[dem == 16] = 6
    data[dem == 32] = 5
    data[dem == 64] = 4
    data[dem == 128] = 3

    del dem

    with rio.open(out_filename, "w", **profile) as dst:
        profile.update(dtype=rio.int16, count=1, compress="lzw")
        dst.write(data, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute flow direction using D8 method")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument("-out", "--out-filename", help="Out filename", required=True, type=str)

    args = vars(parser.parse_args())

    flowdir_d8(**args)
