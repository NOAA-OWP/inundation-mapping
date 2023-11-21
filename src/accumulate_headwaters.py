#!/usr/bin/env python3

import argparse
import os

import rasterio as rio
import whitebox


wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)


def accumulate_headwaters(
    dem_filename: str, loading_filename: str, flow_accumulation_filename: str, stream_pixel_filename: str
):
    """
    Accumulate and threshold headwaters to produce stream pixels

    Parameters
    ----------
    dem_filename : str
        DEM filename
    loading_filename : str
        Loading filename
    flow_accumulation_filename : str
        Flow accumulation filename
    stream_pixel_filename : str
        Stream pixel filename
    """

    working_dir = os.path.dirname(loading_filename)

    with rio.open(loading_filename) as src:
        profile = src.profile
        profile.update(dtype=rio.float32, count=1, compress="lzw")

        data = src.read(1).astype(rio.float32)

    # Create intermediate files
    efficiency_filename = os.path.join(working_dir, "efficiency.tif")
    absorption_filename = os.path.join(working_dir, "absorption.tif")

    efficiency = data**0
    absorption = data * 0.0

    with rio.open(efficiency_filename, "w", **profile) as dst:
        dst.write(efficiency, 1)
    with rio.open(absorption_filename, "w", **profile) as dst:
        dst.write(absorption, 1)

    # Accumulate headwaters
    if (
        wbt.d8_mass_flux(
            dem_filename,
            loading_filename,
            efficiency_filename,
            absorption_filename,
            flow_accumulation_filename,
        )
        != 0
    ):
        raise Exception("ERROR: WhiteboxTools d8_mass_flux failed")

    # Remove intermediate files
    os.remove(efficiency_filename)
    os.remove(absorption_filename)

    # Read flow accumulated headwaters
    with rio.open(flow_accumulation_filename) as src:
        profile = src.profile
        profile.update(dtype=rio.float32, count=1, compress="lzw", nodata=-1)

        data = src.read(1).astype(rio.float32)

    # Update nodata values
    data[data < 0] = -1

    # Threshold accumulations
    data[data > 0] = 1

    # Write output
    with rio.open(stream_pixel_filename, "w", **profile) as dst:
        dst.write(data, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Accumulate and threshold headwaters")
    parser.add_argument("-dem", "--dem-filename", help="DEM filename", required=True, type=str)
    parser.add_argument("-wg", "--loading-filename", help="Loading filename", required=True, type=str)
    parser.add_argument(
        "-fa", "--flow-accumulation-filename", help="Flow accumulation filename", required=True, type=str
    )
    parser.add_argument(
        "-stream", "--stream-pixel-filename", help="Stream pixel filename", required=True, type=str
    )
    args = vars(parser.parse_args())

    accumulate_headwaters(**args)
