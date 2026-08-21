#!/usr/bin/env python3

import argparse
from pathlib import Path

import numpy as np
import rasterio as rio


def convert_array_to_int16(arr: np.ndarray, nodata_val: float = -9999.0) -> tuple[np.ndarray, int]:
    """Converts a float or int32 raster array to int16 in RAM while preserving nodata values."""
    int16_nodata = -32768
    out_arr = np.full_like(arr, fill_value=int16_nodata, dtype=np.int16)

    # Valid data mask
    valid_mask = (arr != nodata_val) & (~np.isnan(arr))

    # Scale / clip to Int16 numeric limits
    clipped_vals = np.clip(arr[valid_mask], -32767, 32767)
    out_arr[valid_mask] = np.round(clipped_vals).astype(np.int16)

    return out_arr, int16_nodata


def convert_raster_file_to_int16_in_memory(raster_path: str) -> None:
    """Reads a raster file, converts its band array to Int16 in RAM, and overwrites the file."""
    path = Path(raster_path)
    if not path.is_file():
        return

    with rio.open(path) as src:
        profile = src.profile.copy()
        nodata_val = src.nodata if src.nodata is not None else -9999.0
        arr = src.read(1)

    int16_arr, int16_nodata = convert_array_to_int16(arr, nodata_val=nodata_val)

    profile.update(dtype=rio.int16, nodata=int16_nodata, count=1)

    with rio.open(path, "w", **profile) as dst:
        dst.write(int16_arr, 1)


def convert_to_int16_directory(branch_dir: str) -> None:
    """File/Directory CLI wrapper that finds catchments and REM rasters and converts them to Int16."""
    b_path = Path(branch_dir)
    if not b_path.is_dir():
        return

    # Find candidate rasters matching gw_catchments and rem_ zeroed rasters
    for raster_file in b_path.glob("*.tif"):
        if "gw_catchments" in raster_file.name or "rem_zeroed" in raster_file.name:
            convert_raster_file_to_int16_in_memory(str(raster_file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert REM and GW Catchments rasters to Int16")
    parser.add_argument("-b", "--branch-dir", required=True, help="Path to branch directory")

    args = vars(parser.parse_args())
    convert_to_int16_directory(branch_dir=args["branch_dir"])
