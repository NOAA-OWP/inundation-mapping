#!/usr/bin/env python3

import argparse
import traceback
from glob import glob

import numpy as np
import rioxarray as rxr
import xarray as xr


def convert_to_int16(huc_dir: str, branch: str):
    """
    Method to convert gage watershed hydro id and relative elevation model datasets from float32 to int16

    Parameters
    ----------
    huc_dir : str
        Directory containing hydrofabric data
    branch : str
        ID of branch to convert (or * to convert them all)

    """

    # Get gage watershed catchments and rems for the appropriate branch (or * for all branches)
    catchments = glob(f"{huc_dir}/branches/{branch}/gw_catchments_reaches_filtered_addedAttributes_*.tif")
    rems = glob(f"{huc_dir}/branches/{branch}/rem_zeroed_masked_*.tif")

    # Iterate through each pair of gw catchments and rems
    for c, r in zip(catchments, rems):
        rem = rxr.open_rasterio(r)

        # Save original as another file to be deleted by deny list or saved
        rem.rio.to_raster(r.replace('.tif', '_float32.tif'), driver="COG")
        nodata, crs = rem.rio.nodata, rem.rio.crs

        # Preserve the second highest possible number for int16, use the highest number for nodata
        rem = xr.where(rem > 32.766, 32.766, rem)
        rem = xr.where(rem >= 0, np.round(rem * 1000), 32767)

        rem = rem.astype(np.int16)
        rem = rem.rio.write_nodata(32767)
        rem = rem.rio.write_crs(crs)

        rem.rio.to_raster(r, dtype=np.int16, driver="COG")

        catchments = rxr.open_rasterio(c)

        # Save original as another file to be deleted by deny list or saved
        catchments.rio.to_raster(c.replace('.tif', '_int32.tif'), driver="COG")

        # Preserve the last four digits only since the first four of HydroIDs are ubiquitous amongst all HUC08
        nodata, crs = catchments.rio.nodata, catchments.rio.crs
        catchments.data = xr.where(
            catchments != nodata, catchments - np.round(catchments.max() / 10000) * 10000, catchments
        )

        catchments = catchments.astype(np.int16)
        catchments = catchments.rio.write_nodata(nodata)
        catchments = catchments.rio.write_crs(crs)

        catchments.rio.to_raster(c, dtype=np.int16, driver="COG")


if __name__ == "__main__":

    """
    Example Usage:

    python ./convert_to_int16.py
        -h ../outputs/fim_outputs
        -b 0
    """

    # Parse arguments
    parser = argparse.ArgumentParser(description="Convert float32 and int32 datasets to int16")

    parser.add_argument(
        "-h", "--huc_dir", help="REQUIRED: Directory with FIM output data for HUC", required=True
    )

    parser.add_argument(
        "-b", "--branches", help="REQUIRED: Id of branch to process (or * for all)", required=True
    )

    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        convert_to_int16(**args)

    except Exception:
        print("The following error has occured:\n", traceback.format_exc())
