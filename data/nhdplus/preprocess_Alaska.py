#!/usr/bin/env python3

# Before using this file, ifsar DTM file needs to be downloaded from AK DGGS at https://elevation.alaska.gov.
# This file will be named custom_download.zip

import argparse
import glob
import os
import shutil
import subprocess
import sys

import geopandas as gpd
import rasterio as rio
from osgeo import gdal
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT

from data.create_vrt_file import create_vrt_file
from data.nfhl.download_fema_nfhl import download_nfhl_wrapper
from data.usgs.acquire_and_preprocess_3dep_dems import __polygonize
from src.derive_headwaters import findHeadWaterPoints


def unzip(zip_path, extract_to="."):
    """
    Extracts a large or stubborn ZIP file using native system tools via subprocess.
    """
    # 1. Check if '7z' is available (preferred for large files)
    if shutil.which("7z"):
        print("Using native 7z for extraction...")
        cmd = ["7z", "x", zip_path, f"-o{extract_to}", "-y"]
        # -y automatically answers 'yes' to any overwrite prompts

    # 2. Fallback to standard 'unzip'
    elif shutil.which("unzip"):
        print("7z not found. Falling back to native unzip...")
        cmd = ["unzip", "-o", zip_path, "-d", extract_to]
        # -o overwrites existing files without prompting

    else:
        print("Error: Neither '7z' nor 'unzip' utilities are installed on this system.", file=sys.stderr)
        print("Please run: sudo apt install p7zip-full unzip", file=sys.stderr)
        return False

    try:
        # Run the command and capture errors if it fails
        # result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Extraction completed successfully!")
        return True

    except subprocess.CalledProcessError as e:
        print(f"Extraction failed with exit code {e.returncode}", file=sys.stderr)
        print(f"STDOUT:\n{e.stdout}", file=sys.stderr)
        print(f"STDERR:\n{e.stderr}", file=sys.stderr)
        return False


# Example usage:
# extract_large_zip("my_massive_file.zip", "/path/to/output_folder")


def preprocess_dem(input_dem_zip_file, out_dem_folder, region, target_crs_number='3338'):
    """
    Preprocess ifsar DTM 5 meter to 10 meter DEM
    """

    ### UNZIP AND MOSAIC TILES
    dem_root_raw = os.path.dirname(input_dem_zip_file)

    os.makedirs(dem_root_raw, exist_ok=True)

    # 0. MANUALLY Download ifsar DTM 5 meter from AK DGGS [https://elevation.alaska.gov] to input_dem_zip_file
    if not os.path.exists(input_dem_zip_file):
        print(
            f'ERROR: {input_dem_zip_file} does not exist. It needs to be downloaded from AK DGGS (https://elevation.alaska.gov)'
        )
        sys.exit(1)

    # Unzip to zip_dir
    unzip(input_dem_zip_file, out_dem_folder)

    # 1. Configuration (Double-check this path!)
    output_mosaic = os.path.join(out_dem_folder, f'{region}_ifsar_DTM_{target_crs_number}.tif')
    target_crs = CRS.from_epsg(target_crs_number)
    target_res = 10

    # Convert to absolute path to avoid relative path confusion
    zip_dir = os.path.dirname(input_dem_zip_file)
    absolute_zip_dir = os.path.abspath(zip_dir)
    zip_pattern = os.path.join(absolute_zip_dir, "*.zip")
    zip_files = glob.glob(zip_pattern)

    # --- DIAGNOSTIC CHECK ---
    if not zip_files:
        raise FileNotFoundError(
            f"\n[ERROR] No .zip files found matching the pattern: {zip_pattern}\n"
            f"Please verify that the directory exists and contains your .zip archives."
        )

    src_files_to_mosaic = []
    for zip_path in zip_files:
        base_name = os.path.splitext(os.path.basename(zip_path))[0]
        vsi_path = f"/vsizip/{zip_path}/{base_name}.tif"
        src_files_to_mosaic.append(vsi_path)

    opened_datasets = []
    vrt_datasets = []

    try:
        # 2. Open files and handle CRS check/reprojection dynamically
        for path in src_files_to_mosaic:
            try:
                src = rio.open(path)
                opened_datasets.append(src)
            except Exception as e:
                print(f"Skipping {path} because it couldn't be opened. Error: {e}")
                continue

            if src.crs != target_crs:
                print(f"Reprojecting on-the-fly: {os.path.basename(path)}")
                vrt = WarpedVRT(src, crs=target_crs)
                vrt_datasets.append(vrt)
            else:
                print(f"Already EPSG:3338: {os.path.basename(path)}")
                vrt_datasets.append(src)

        # Double check we have valid opened datasets before merging
        if not vrt_datasets:
            raise ValueError("No valid TIFF datasets were successfully opened.")

        # 3. Merge
        print(f"\nMosaicing and resampling {len(vrt_datasets)} files to {target_res}m...")
        mosaic, out_trans = merge(
            vrt_datasets,
            res=target_res,  # Forces the output cell size to 10x10
            resampling=Resampling.bilinear,  # Smooths the 5m data into 10m cells (use Resampling.nearest for discrete/classified data)
        )

        # 4. Define output metadata
        out_meta = vrt_datasets[0].meta.copy()
        out_meta.update(
            {
                "driver": "GTiff",
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": out_trans,
                "crs": target_crs,
            }
        )

        # 5. Write the final result
        with rio.open(output_mosaic, "w", **out_meta) as dest:
            dest.write(mosaic)

        print(f"\nSuccess! Mosaic saved to {output_mosaic}")

    finally:
        # Clean up everything safely
        for vrt in vrt_datasets:
            if isinstance(vrt, WarpedVRT):
                vrt.close()
        for src in opened_datasets:
            src.close()

    create_vrt_file(out_dem_folder, 'hand_seamless_3dep_dems.vrt')

    # Create DEM_Domain.gpkg
    __polygonize(out_dem_folder)


def preprocess_streams(region, hucs, target_crs_number, inputs_dir, reference_fabric_file):
    """
    Preprocess Alaska streams for a specific region.

    Parameters:
        region : str
            The region to preprocess. Options are: 'Fairbanks', 'Juneau'
        huc : str
            The HUC identifier.
        target_crs_number : str
            The target CRS number.
        inputs_dir : str
            The directory containing input data files.
        reference_fabric_file : str
            The name of the streams data.
    """

    # Convert input flowpathss to necessary format
    if not os.path.exists(reference_fabric_file):
        sys.exit(f"reference fabric file {reference_fabric_file} does not exist. Exiting...")
    reference_fabric_folder = os.path.dirname(reference_fabric_file)
    flowpaths = gpd.read_file(reference_fabric_file, layer='flowpaths')
    target_crs = CRS.from_epsg(target_crs_number)
    if flowpaths.crs != target_crs:
        flowpaths = flowpaths.to_crs(epsg=target_crs_number)

    flowpaths = flowpaths.rename(
        columns={'flowpath_id': 'ID', 'flowpath_toid': 'to', 'streamorder': 'order_'}
    )

    # Derive headwater points
    headwater_points = findHeadWaterPoints(flowpaths)
    headwater_points.to_file(
        os.path.join(reference_fabric_folder, 'flowpaths_headwaters_Alaska.gpkg'), driver='GPKG'
    )

    # Extract and reproject Alaska waterbodies
    lakes = gpd.read_file(reference_fabric_file, layer='lakes')
    lakes = lakes.rename(columns={'Hylak_id': 'LakeID'})
    lakes = lakes.to_crs(epsg=target_crs_number)
    lakes = lakes[['LakeID', 'geometry']]
    lakes.to_file(os.path.join(reference_fabric_folder, 'lakes_Alaska.gpkg'), driver='GPKG')

    flowpaths = flowpaths.sjoin(lakes, how='left', predicate='intersects')
    flowpaths = flowpaths.rename(columns={'LakeID': 'Lake'})
    flowpaths['Lake'] = flowpaths['Lake'].fillna(-9999).astype(int)

    flowpaths.to_file(os.path.join(reference_fabric_folder, 'flowpaths_Alaska.gpkg'), driver='GPKG')

    # Extract and reproject Alaska catchments
    catchments = gpd.read_file(reference_fabric_file, layer='divides')
    catchments = catchments.rename(columns={'divide_id': 'ID'})
    catchments = catchments.to_crs(epsg=target_crs_number)
    catchments.to_file(os.path.join(reference_fabric_folder, 'catchments_Alaska.gpkg'), driver='GPKG')

    # Extract and reproject WBD
    wbd_dir = os.path.join(inputs_dir, 'wbd')
    wbd = os.path.join(wbd_dir, 'WBD_Alaska_3338.gpkg')
    if not os.path.exists(wbd_dir):
        os.makedirs(wbd_dir)
    if not os.path.exists(wbd):
        sys.exit(f"WBD file {wbd} does not exist. Exiting...")
    WBD = gpd.read_file(wbd, columns=['HUC8'])
    WBD = WBD.to_crs(epsg=target_crs_number)
    WBD.to_file(f'{inputs_dir}/wbd/WBD_{region}_{target_crs_number}.gpkg', layer='WBDHU8', driver='GPKG')

    download_nfhl_wrapper(
        huc_list=hucs, output_folder=os.path.join(inputs_dir, 'fema/nfhl', region), num_processes=14
    )


if __name__ == "__main__":
    """
    preprocess_Alaska.py
        --region 'Fairbanks'
        --inputs_dir '/data/inputs'
        --reference_fabric_folder os.path.join(inputs_dir, 'GEOGLOWS')
        --reference_fabric_filename 'ak_tests_BETA_AK_reference_fabric_fairbanks_juneau.gpkg'
        --target_crs_number 3338
    """
    parser = argparse.ArgumentParser(description="Preprocess Alaska data for a specified region.")
    parser.add_argument(
        "-r",
        "--region",
        type=str,
        required=True,
        help="The region to preprocess. Options are: 'Fairbanks', 'Juneau'",
    )
    parser.add_argument(
        "-i", "--inputs_dir", type=str, required=True, help="The directory containing input data files."
    )
    parser.add_argument(
        '-c', '--target_crs_number', type=int, required=False, default='3338', help='EPSG CRS number'
    )
    parser.add_argument('-t', '--reference_fabric_folder', type=str, help='Folder for streams outputs')
    parser.add_argument('-s', '--reference_fabric_filename', type=str, help='Name of streams file')
    parser.add_argument('-d', '--input_dem_zip_file', type=str, required=True, help='Input DEM ZIP file')
    parser.add_argument('-e', '--out_dem_folder', type=str, required=True, help='Out DEM folder')

    args = vars(parser.parse_args())

    if args['region'] not in ['Fairbanks', 'Juneau']:
        sys.exit("Region not recognized. Options are: 'Fairbanks', 'Juneau'")

    inputs_dir = args['inputs_dir']
    region = args['region']
    target_crs_number = args['target_crs_number']
    reference_fabric_file = args['reference_fabric_file']
    input_dem_zip_file = args['input_dem_zip_file']
    out_dem_folder = args['out_dem_folder']

    # ### Unzip, merge tiles, and reproject/rescale DEMs
    preprocess_dem(input_dem_zip_file, out_dem_folder, region, target_crs_number)

    if region == 'Fairbanks':
        hucs = ['19080306', '19080307']
    elif region == 'Juneau':
        hucs = ['19010301']

    preprocess_streams(region, hucs, target_crs_number, inputs_dir, reference_fabric_file)
