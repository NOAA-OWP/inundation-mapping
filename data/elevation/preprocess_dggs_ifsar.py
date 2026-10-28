#!/usr/bin/env python3

"""
Preprocess State of Alaska DGGS IFSAR DTM data.
Extracts nested sub-zips, creates a reprojected 10m mosaic,
and clips individual HUC8 DEMs.
"""

import glob
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone

from osgeo import gdal

import src.utils.shared_functions as sf


gdal.UseExceptions()


def unzip(zip_path, extract_to="."):
    """Extracts a large or stubborn ZIP file using native system tools via subprocess."""
    if shutil.which("7z"):
        print("Using native 7z for extraction...")
        cmd = ["7z", "x", zip_path, f"-o{extract_to}", "-y"]
    elif shutil.which("unzip"):
        print("7z not found. Falling back to native unzip...")
        cmd = ["unzip", "-o", zip_path, "-d", extract_to]
    else:
        print("Error: Neither '7z' nor 'unzip' utilities are installed on this system.", file=sys.stderr)
        print("Please run: sudo apt install p7zip-full unzip", file=sys.stderr)
        return False

    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Extraction completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Extraction failed with exit code {e.returncode}", file=sys.stderr)
        return False


def preprocess_dggs_ifsar(
    input_dem_zip_file,
    out_dem_folder,
    region,
    date,
    hucs,
    inputs_dir,
    target_crs_number="3338",
    target_res=10,
):
    """
    Ingests and processes Alaska DGGS IFSAR custom download packages.

    Parameters:
    -----------
    input_dem_zip_file : str
        Path to raw downloaded DTM zip archive from AK DGGS.
    out_dem_folder : str
        Output directory for staging, extraction, logs, and temporary mosaic.
    region : str
        Region identifier (e.g., 'Fairbanks', 'Juneau').
    date : str
        Date tag associated with the run.
    hucs : list of str
        List of HUC8 IDs to clip and generate.
    inputs_dir : str
        Base inputs directory containing boundary files (e.g., WBD cutlines).
    target_crs_number : str or int
        Target EPSG code (default: '3338').
    target_res : int
        Output cell resolution in target CRS units (default: 10m).
    """
    overall_start_time = datetime.now(timezone.utc)
    file_dt_string = overall_start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"ifsar_downloaded_{region}-{file_dt_string}.log"
    log_file_path = os.path.join(out_dem_folder, log_file_name)
    logger_name = f"ifsar_downloaded_{region}"
    file_logger = sf.setup_mp_file_logger(log_file_path, logger_name)

    sf.l_print(f"Processing DGGS IFSAR data in {out_dem_folder}", file_logger, "info")

    os.makedirs(out_dem_folder, exist_ok=True)

    if not os.path.exists(input_dem_zip_file):
        print(
            f"ERROR: {input_dem_zip_file} does not exist. It needs to be downloaded from AK DGGS (https://elevation.alaska.gov)",
            file=sys.stderr,
        )
        sys.exit(1)

    unzip(input_dem_zip_file, out_dem_folder)

    output_mosaic = os.path.join(out_dem_folder, f"{region}_ifsar_DTM_{target_crs_number}.tif")

    absolute_out_folder = os.path.abspath(out_dem_folder)
    extracted_zip_dir = os.path.join(absolute_out_folder, "dds4", "ifsar", "dtm")
    zip_pattern = os.path.join(extracted_zip_dir, "*.zip")
    zip_files = glob.glob(zip_pattern)

    src_files_to_mosaic = []
    print(f"Scanning {len(zip_files)} extracted sub-archives for TIFFs...")

    for sub_zip_path in zip_files:
        try:
            with zipfile.ZipFile(sub_zip_path, "r") as sub_zip:
                tif_names = [name for name in sub_zip.namelist() if name.lower().endswith((".tif", ".tiff"))]

                for tif_name in tif_names:
                    vsi_path = f"/vsizip/{sub_zip_path}/{tif_name}"
                    src_files_to_mosaic.append(vsi_path)
        except Exception as e:
            print(f"Warning: Could not read sub-archive {sub_zip_path}: {e}")

    if not src_files_to_mosaic:
        raise FileNotFoundError(
            f"\n[ERROR] No .tif files found inside extracted sub-zips in: {extracted_zip_dir}\n"
        )

    print(f"Successfully mapped {len(src_files_to_mosaic)} virtual TIFF datasets.")
    print(f"Mosaicing, reprojecting, and resampling to {target_res}m...")

    try:
        warp_options = gdal.WarpOptions(
            format="GTiff",
            srcSRS=f"EPSG:{target_crs_number}",
            dstSRS=f"EPSG:{target_crs_number}",
            xRes=target_res,
            yRes=target_res,
            resampleAlg="bilinear",
            outputType=gdal.GDT_Float32,
            options=["-overwrite", "-srcnodata", "-10000", "-dstnodata", "-999999"],
        )

        gdal.Warp(output_mosaic, src_files_to_mosaic, options=warp_options)
        print(f"Intermediate mosaic saved to {output_mosaic}")

        for huc in hucs:
            cutline_path = os.path.join(inputs_dir, "wbd", "HUC8_Alaska", f"HUC8_{huc}.gpkg")
            dest_dir = os.path.join(inputs_dir, "dems", "ifsar_dtm", region, date)
            os.makedirs(dest_dir, exist_ok=True)
            dest_ds = os.path.join(dest_dir, f"HUC8_{huc}_dem.tif")

            if not os.path.exists(cutline_path):
                print(f"Warning: Cutline {cutline_path} not found. Skipping HUC {huc}.")
                continue

            huc_warp_options = gdal.WarpOptions(
                format="GTiff",
                outputType=gdal.GDT_Float32,
                resampleAlg=gdal.GRA_Bilinear,
                cutlineDSName=cutline_path,
                cropToCutline=True,
                cutlineBlend=6,
                srcSRS=f"EPSG:{target_crs_number}",
                dstSRS=f"EPSG:{target_crs_number}",
                xRes=target_res,
                yRes=target_res,
                targetAlignedPixels=True,
                creationOptions=[
                    "BLOCKXSIZE=256",
                    "BLOCKYSIZE=256",
                    "TILED=YES",
                    "COMPRESS=LZW",
                    "BIGTIFF=YES",
                ],
            )

            print(f"Clipping DEM for HUC8: {huc} -> {dest_ds}")
            gdal.Warp(dest_ds, output_mosaic, options=huc_warp_options, overwrite=True)

        if os.path.exists(output_mosaic):
            os.remove(output_mosaic)

    except Exception as e:
        raise RuntimeError(f"GDAL Warp processing failed: {e}")
