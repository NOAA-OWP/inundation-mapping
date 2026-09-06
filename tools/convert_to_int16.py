#!/usr/bin/env python3

import os
import sys

import numpy as np
import rasterio as rio


def convert_raster_file_to_int16_in_memory(
    raster_path: str, scale_factor: float = 1000.0, nodata_out: int = 32767
) -> None:
    """Converts a floating-point HAND/REM raster (meters) to Int16 millimeters (mm)

    matching the dev baseline statistics (Min: 0, Max: 32766, NoData: 32767).
    """
    # 1. Preserve native float32 raster prior to conversion (Dev truth line)
    float32_path = raster_path.replace(".tif", "_float32.tif")
    if not os.path.exists(float32_path):
        with rio.open(raster_path, "r") as src:
            profile = src.profile.copy()
            arr = src.read(1)
            with rio.open(float32_path, "w", **profile) as dst_f32:
                dst_f32.write(arr, 1)

    # 2. Read float32 meter raster and convert to int16 millimeters
    with rio.open(float32_path, "r") as src:
        profile = src.profile.copy()
        arr = src.read(1)
        nodata_in = src.nodata

    if nodata_in is not None:
        invalid_mask = (arr == nodata_in) | (arr < 0) | (arr <= -9000.0) | np.isnan(arr)
    else:
        invalid_mask = (arr < 0) | (arr <= -9000.0) | np.isnan(arr)

    # Initialize destination array filled with 32767 (Dev NoData)
    out_arr = np.full(arr.shape, fill_value=nodata_out, dtype=np.int16)

    # Cap at 32.766 meters (32,766 mm) to fit signed Int16 range below NoData 32767
    valid_mask = ~invalid_mask
    capped_meters = np.where(arr[valid_mask] > 32.766, 32.766, arr[valid_mask])
    scaled_mm = np.round(capped_meters * scale_factor)
    out_arr[valid_mask] = np.clip(scaled_mm, 0, 32766).astype(np.int16)

    # Update profile to Int16 LZW Tiled with NoData = 32767
    profile.update(dtype=rio.int16, nodata=nodata_out, compress="LZW", tiled=True, BIGTIFF="YES")

    # 3. Overwrite primary file with Int16 raster
    with rio.open(raster_path, "w", **profile) as dst:
        dst.write(out_arr, 1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for file_path in sys.argv[1:]:
            convert_raster_file_to_int16_in_memory(file_path)
