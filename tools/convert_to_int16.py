#!/usr/bin/env python3

import sys

import numpy as np
import rasterio as rio


def convert_raster_file_to_int16_in_memory(
    raster_path: str, scale_factor: float = 1000.0, nodata_out: int = -9999
) -> None:
    """Converts a floating-point HAND/REM raster (meters) to Int16 millimeters (mm)

    matching the dev baseline statistics (Min: 0, Max: 32766, NoData: -9999).
    """
    with rio.open(raster_path, "r") as src:
        profile = src.profile.copy()
        arr = src.read(1)
        nodata_in = src.nodata

    # Mask out background, negative, and NoData pixels
    if nodata_in is not None:
        invalid_mask = (arr == nodata_in) | (arr < 0) | (arr <= -9000.0) | np.isnan(arr)
    else:
        invalid_mask = (arr < 0) | (arr <= -9000.0) | np.isnan(arr)

    # Initialize destination array filled with -9999 (Int16)
    out_arr = np.full(arr.shape, fill_value=nodata_out, dtype=np.int16)

    # Scale valid elevations from meters to millimeters and clip to [0, 32766]
    valid_mask = ~invalid_mask
    scaled_mm = np.round(arr[valid_mask] * scale_factor)
    out_arr[valid_mask] = np.clip(scaled_mm, 0, 32766).astype(np.int16)

    # Update raster profile for Int16 output
    profile.update(dtype=rio.int16, nodata=nodata_out, compress="LZW", BIGTIFF="YES")

    with rio.open(raster_path, "w", **profile) as dst:
        dst.write(out_arr, 1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for file_path in sys.argv[1:]:
            convert_raster_file_to_int16_in_memory(file_path)
