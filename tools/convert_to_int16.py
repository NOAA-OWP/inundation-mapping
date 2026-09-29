#!/usr/bin/env python3

import os
import sys

import numpy as np
import rasterio as rio
import rioxarray as rxr
import xarray as xr

from utils.fim_enums import FIM_exit_codes


def convert_raster_file_to_int16_in_memory(
    branch_dir: str,
    catchment_path: str,
    raster_path: str,
    scale_factor: float = 1000.0,
    nodata_out: int = 32767,
) -> None:
    """Converts a floating-point HAND/REM raster (meters) to Int16 millimeters (mm)

    matching the dev baseline statistics (Min: 0, Max: 32766, NoData: 32767).
    """

    catchment = rxr.open_rasterio(catchment_path)

    # Check if converting data is possible
    if (np.unique(catchment).shape[0] > 32766) | (len(str(int(np.max(catchment)))) > 8):

        print(
            "Catchment raster has either more than 32766 unique HydroIDs or has HydroIDs with more",
            "than 8 digits.  Please adjust data accordingly before running Int16 data conversion.",
        )
        sys.exit(FIM_exit_codes.CANNOT_CONVERT_HYDROIDS_TO_INT16.value)

    # Save a copy
    catchment.rio.to_raster(catchment_path.replace('.tif', '_int32.tif'), compress="LZW", tiled=True)

    hydroid_prefix = str(int(np.floor(catchment.max() / 10000)))

    # Preserve the last four digits only since the first four of HydroIDs are ubiquitous amongst all HUC08
    nodata, crs = catchment.rio.nodata, catchment.rio.crs
    catchment.data = xr.where(catchment != nodata, catchment - int(hydroid_prefix) * 10000, catchment)

    catchment = catchment.astype(np.int16)
    catchment.rio.write_nodata(nodata, inplace=True)
    catchment.rio.write_crs(crs, inplace=True)

    catchment.rio.to_raster(catchment_path, dtype=np.int16, compress="LZW", tiled=True)

    hydroid_prefix_path = os.path.join(branch_dir, 'hydroid_prefix.txt')
    if not os.path.exists(hydroid_prefix_path):
        with open(hydroid_prefix_path, 'w') as file:
            file.write(hydroid_prefix)

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
