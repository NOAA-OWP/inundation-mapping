#!/usr/bin/env python3

import argparse
import os

import rasterio as rio
import whitebox


def accumulate_headwaters(dem_filename: str, loading_filename: str, out_filename: str):
    wbt = whitebox.WhiteboxTools()

    working_dir = os.path.dirname(loading_filename)

    efficiency_filename = os.path.join(working_dir, "efficiency.tif")
    absorption_filename = os.path.join(working_dir, "absorption.tif")

    with rio.open(loading_filename) as src:
        profile = src.profile
        profile.update(dtype=rio.float32, count=1, compress="lzw")

        data = src.read(1).astype(rio.float32)

    efficiency = data**0
    absorption = data * 0.0

    with rio.open(efficiency_filename, "w", **profile) as dst:
        dst.write(efficiency, 1)
    with rio.open(absorption_filename, "w", **profile) as dst:
        dst.write(absorption, 1)

    wbt.d8_mass_flux(dem_filename, loading_filename, efficiency_filename, absorption_filename, out_filename)

    os.remove(efficiency_filename)
    os.remove(absorption_filename)

    with rio.open(out_filename) as src:
        profile = src.profile
        profile.update(dtype=rio.float32, count=1, compress="lzw", nodata=-1)

        data = src.read(1).astype(rio.float32)

    data[data < 0] = -1

    with rio.open(out_filename, "w", **profile) as dst:
        # dst.nodata = -1
        dst.write(data, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Accumulate headwaters")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument("-wg", "--loading-filename", help="Loading filename", required=True, type=str)
    parser.add_argument("-out", "--out-filename", help="Out filename", required=True, type=str)

    args = vars(parser.parse_args())

    accumulate_headwaters(**args)
