#!/usr/bin/env python3

import argparse
import os
import traceback
from glob import glob

import numpy as np
import rioxarray as rxr
import xarray as xr


def convert_to_int16(branch_dir: str):
    """
    Method to convert gage watershed hydro id and relative elevation model datasets from float32 to int16

    Parameters
    ----------
    branch_dir : str
        Directory containing hydrofabric data

    """

    hydroid_prefix = None

    # Get gage watershed catchments and rems for the appropriate branch (or * for all branches)
    catchments = glob(f"{branch_dir}/gw_catchments_reaches_filtered_addedAttributes_*.tif")
    rems = glob(f"{branch_dir}/rem_zeroed_masked_*.tif")

    hydroid_prefix_path = os.path.join(branch_dir, 'hydroid_prefix.txt')

    # Iterate through each pair of gw catchments and rems
    for c, r in zip(catchments, rems):
        rem = rxr.open_rasterio(r)

        # Save original as another file to be deleted by deny list or saved
        rem.rio.to_raster(r.replace('.tif', '_float32.tif'), driver="COG")
        nodata, crs = rem.rio.nodata, rem.rio.crs

        # Preserve the second highest possible number for int16, use the highest number for nodata
        rem.data = xr.where(rem > 32.766, 32.766, rem)
        rem.data = xr.where(rem >= 0, np.round(rem * 1000), 32767)

        rem = rem.astype(np.int16)
        rem.rio.write_nodata(32767, inplace=True)
        rem.rio.write_crs(crs, inplace=True)

        rem.rio.to_raster(r, dtype=np.int16, driver="COG")

        catchment = rxr.open_rasterio(c)

        if hydroid_prefix is None:
            hydroid_prefix = str(int(np.floor(catchment.max() / 10000)))

        # Save original as another file to be deleted by deny list or saved
        catchment.rio.to_raster(c.replace('.tif', '_int32.tif'), driver="COG")

        # Preserve the last four digits only since the first four of HydroIDs are ubiquitous amongst all HUC08
        nodata, crs = catchment.rio.nodata, catchment.rio.crs
        catchment.data = xr.where(catchment != nodata, catchment - int(hydroid_prefix), catchment)

        catchment = catchment.astype(np.int16)
        catchment.rio.write_nodata(nodata, inplace=True)
        catchment.rio.write_crs(crs, inplace=True)

        catchment.rio.to_raster(c, dtype=np.int16, driver="COG")

        print(hydroid_prefix_path, 'exists:', os.path.exists(hydroid_prefix_path))
        if not os.path.exists(hydroid_prefix_path):
            with open(hydroid_prefix_path, 'w') as file:
                file.write(hydroid_prefix)


if __name__ == "__main__":

    """
    Example Usage:

    python ./convert_to_int16.py
        -b ../outputs/fim_outputs/12090301/0
    """

    # Parse arguments
    parser = argparse.ArgumentParser(description="Convert float32 and int32 datasets to int16")

    parser.add_argument(
        "-b", "--branch_dir", help="REQUIRED: Id of branch to process (or * for all)", required=True
    )

    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        convert_to_int16(**args)

    except Exception:
        print("The following error has occured:\n", traceback.format_exc())
