#!/usr/bin/env python3

"""
Preprocess National Elevation Dataset (NED) snapshots from NHDPlus V2.1.
Handles reprojection, elevation unit conversion (cm -> m), and domain VRT generation.
"""

import argparse
import os
import sys

import rasterio as rio
from osgeo import gdal

from data.create_vrt_file import create_vrt_file


gdal.UseExceptions()


def preprocess_nhdplus_ned(
    dem_file: str,
    target_dem_folder: str,
    target_folder: str,
    huc: str,
    region: str,
    target_crs_number: str,
    vrt_basename: str,
    vrt_output_dir: str,
    vrt_suffix: str = "",
):
    """Reprojects NED snapshot DEM, converts cm to m, and builds domain VRT."""
    os.makedirs(target_dem_folder, exist_ok=True)
    os.makedirs(target_folder, exist_ok=True)

    target_name = f"{region}_{target_crs_number}"
    target_crs = f"EPSG:{target_crs_number}"
    output_nodata = -999999

    intermediate_dem_path = os.path.join(target_folder, f"elev_cm_{target_name}.tif")

    if not os.path.exists(dem_file):
        sys.exit(f"Input DEM path '{dem_file}' does not exist.")

    print(f"[{region}] Reprojecting and resampling NED DEM to 10m...")
    warp_options = gdal.WarpOptions(
        format="GTiff",
        dstSRS=target_crs,
        xRes=10,
        yRes=10,
        resampleAlg="bilinear",
        outputType=gdal.GDT_Float32,
        options=["-overwrite"],
    )
    gdal.Warp(intermediate_dem_path, dem_file, options=warp_options)

    final_huc_dem = os.path.join(target_dem_folder, f"HUC8_{huc}_dem.tif")
    print(f"[{region}] Converting DEM from cm to m: {final_huc_dem}")
    with rio.open(intermediate_dem_path, "r") as dem:
        elevation_data = dem.read(1).astype(rio.float32)
        elevation_data = elevation_data / 100.0
        elevation_data[elevation_data < -999] = output_nodata

        meta = dem.meta.copy()
        meta.update({"dtype": rio.float32, "nodata": output_nodata})

        with rio.open(final_huc_dem, "w", **meta) as dst:
            dst.write(elevation_data, 1)

    print(f"[{region}] Generating VRT file: {vrt_basename}...")
    create_vrt_file(target_dem_folder, vrt_basename, vrt_output_dir, vrt_suffix, run_polygonize=True)


def main():
    parser = argparse.ArgumentParser(
        description="Reproject, convert units, and build VRT for NED snapshot DEMs."
    )
    parser.add_argument("-m", "--dem_file", type=str, required=True, help="Input raw elev_cm raster path")
    parser.add_argument(
        "-t", "--target_dem_folder", type=str, required=True, help="Directory to save final HUC8 DEM"
    )
    parser.add_argument("-f", "--target_folder", type=str, required=True, help="Staging scratch directory")
    parser.add_argument("-u", "--huc", type=str, required=True, help="HUC8 identifier")
    parser.add_argument("-r", "--region", type=str, required=True, help="Region name")
    parser.add_argument("-c", "--target_crs_number", type=str, required=True, help="Target EPSG code")
    parser.add_argument("-b", "--vrt_basename", type=str, required=True, help="Basename of VRT")
    parser.add_argument("-o", "--vrt_output_dir", type=str, required=True, help="Output folder for VRT")
    parser.add_argument("-s", "--vrt_suffix", type=str, default="", help="Suffix for VRT file")

    args = parser.parse_args()
    preprocess_nhdplus_ned(**vars(args))


if __name__ == "__main__":
    main()
