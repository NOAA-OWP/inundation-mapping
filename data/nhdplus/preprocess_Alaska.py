#!/usr/bin/env python3

# Before using this file, ifsar DTM file needs to be downloaded from AK DGGS at https://elevation.alaska.gov.
# This file will be named custom_download.zip

import argparse
import glob
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import rasterio as rio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT

import src.utils.shared_functions as sf
from data.create_vrt_file import create_vrt_file
from data.nfhl.download_fema_nfhl import download_nfhl_wrapper
from src.derive_headwaters import findHeadWaterPoints
from src.utils.polygonize_raster import polygonize
from src.utils.shared_functions import FIM_Helpers as fh


def __polygonize(target_output_folder_path, file_logger):

    # TODO: Jun 2025: Find a way to speed this up  (add MP or MT???)
    # Can likely just send the mp/mt send back the gpkg, add it to an array, then concat, and dissolve
    """
    Create a polygon of 3DEP domain from individual HUC DEMS which are then dissolved into a single polygon

    Note: If you have to re-run this tool to repair some DEMs, this section must be re-run and is by default.

    """
    dem_domain_file = os.path.join(target_output_folder_path, 'DEM_Domain.parquet')

    msg = f" - Polygonizing -- {dem_domain_file} - Started (be patient, it can take a while)"
    sf.l_print(msg, file_logger, "info")

    start_time = datetime.now(timezone.utc)
    sf.l_print(f"Polygonation start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")

    dem_files = glob.glob(os.path.join(target_output_folder_path, '*.tif'))

    if len(dem_files) == 0:
        raise Exception("There are no DEMs to polygonize")

    dem_files.sort()

    dem_parquets = gpd.GeoDataFrame()

    for n, dem_file in enumerate(dem_files):
        sf.l_print(f"Polygonizing: {dem_file}", file_logger, "info")
        edge_tif = f'{os.path.splitext(dem_file)[0]}_edge.tif'
        edge_parquet = f'{os.path.splitext(edge_tif)[0]}.parquet'

        # Calculate a constant valued raster from valid DEM cells
        if not os.path.exists(edge_tif):
            subprocess.run(
                [
                    'gdal_calc.py',
                    '-A',
                    dem_file,
                    f'--outfile={edge_tif}',
                    '--calc=where(A > -900, 1, 0)',
                    '--co',
                    'BIGTIFF=YES',
                    '--co',
                    'NUM_THREADS=ALL_CPUS',
                    '--co',
                    'TILED=YES',
                    '--co',
                    'COMPRESS=LZW',
                    '--co',
                    'SPARSE_OK=TRUE',
                    '--type=Byte',
                    '--quiet',
                ]
            )

        # Polygonize constant valued raster
        # subprocess.run(['gdal_polygonize.py', '-8', edge_tif, '-q', '-f', 'GPKG', edge_parquet])
        polygonize(edge_tif, edge_parquet, connectivity=8, quiet=True)

        gdf = gpd.read_parquet(edge_parquet)

        if n == 0:
            dem_parquets = gdf
        else:
            dem_parquets = pd.concat([dem_parquets, gdf])

        os.remove(edge_tif)
        os.remove(edge_parquet)

    dem_parquets['DN'] = 1
    dem_dissolved = dem_parquets.dissolve(by='DN')
    dem_dissolved.to_parquet(dem_domain_file)

    if not os.path.exists(dem_domain_file):
        sf.l_print(f" - Polygonizing -- {dem_domain_file} - Failed", file_logger, "error")
    else:
        sf.l_print(f" - Polygonizing -- {dem_domain_file} - Complete", file_logger, "info")

    end_time = datetime.now(timezone.utc)
    sf.l_print(f"Polygonization end time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")
    sf.l_print(fh.print_date_time_duration(start_time, end_time, print_dur_msg=False), file_logger, "info")


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

    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    file_dt_string = overall_start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"ifsar_downloaded-{file_dt_string}.log"
    log_file_path = os.path.join(out_dem_folder, log_file_name)
    file_logger = sf.setup_mp_file_logger(log_file_path, "ifsar_downloaded")

    # sf.l_print(f"Downloading to {out_dem_folder}", file_logger, "info")

    # ### UNZIP AND MOSAIC TILES
    # os.makedirs(out_dem_folder, exist_ok=True)

    # # 0. MANUALLY Download ifsar DTM 5 meter from AK DGGS [https://elevation.alaska.gov] to input_dem_zip_file
    # if not os.path.exists(input_dem_zip_file):
    #     print(
    #         f'ERROR: {input_dem_zip_file} does not exist. It needs to be downloaded from AK DGGS (https://elevation.alaska.gov)'
    #     )
    #     sys.exit(1)

    # # Unzip the parent zip file into out_dem_folder
    # unzip(input_dem_zip_file, out_dem_folder)

    # # 1. Configuration
    # output_mosaic = os.path.join(out_dem_folder, f'{region}_ifsar_DTM_{target_crs_number}.tif')
    # target_crs = CRS.from_epsg(target_crs_number)
    # target_res = 10

    # # 2. Track down the EXTRACTED sub-zips inside out_dem_folder
    # import zipfile
    # absolute_out_folder = os.path.abspath(out_dem_folder)

    # # Locate the nested directory structure on disk
    # extracted_zip_dir = os.path.join(absolute_out_folder, "dds4", "ifsar", "dtm")
    # zip_pattern = os.path.join(extracted_zip_dir, "*.zip")
    # zip_files = glob.glob(zip_pattern)

    # src_files_to_mosaic = []

    # print(f"Scanning {len(zip_files)} extracted sub-archives for TIFFs...")

    # # Step B: Peek inside each sub-zip on disk to find its inner .tif
    # for sub_zip_path in zip_files:
    #     try:
    #         with zipfile.ZipFile(sub_zip_path, 'r') as sub_zip:
    #             # Find the .tif file inside (case-insensitive)
    #             tif_names = [
    #                 name for name in sub_zip.namelist()
    #                 if name.lower().endswith('.tif') or name.lower().endswith('.tiff')
    #             ]

    #             for tif_name in tif_names:
    #                 # Construct the single virtual path targeting the extracted sub-zip on disk
    #                 # Syntax: /vsizip/{absolute_path_to_sub_zip}/{tif_name}
    #                 vsi_path = f"/vsizip/{sub_zip_path}/{tif_name}"
    #                 src_files_to_mosaic.append(vsi_path)
    #     except Exception as e:
    #         print(f"Warning: Could not read sub-archive {sub_zip_path}: {e}")

    # # --- DIAGNOSTIC CHECK ---
    # if not src_files_to_mosaic:
    #     raise FileNotFoundError(
    #         f"\n[ERROR] No .tif files could be mapped inside the extracted sub-zips in: {extracted_zip_dir}\n"
    #     )

    # print(f"Successfully mapped {len(src_files_to_mosaic)} sub-zip TIFF datasets for mosaicing.")

    # opened_datasets = []
    # vrt_datasets = []

    # try:
    #     # 2. Open files and handle CRS check/reprojection dynamically
    #     for path in src_files_to_mosaic:
    #         try:
    #             src = rio.open(path)
    #             opened_datasets.append(src)
    #         except Exception as e:
    #             print(f"Skipping {path} because it couldn't be opened. Error: {e}")
    #             continue

    #         if src.crs != target_crs:
    #             print(f"Reprojecting on-the-fly: {os.path.basename(path)}")
    #             vrt = WarpedVRT(src, crs=target_crs)
    #             vrt_datasets.append(vrt)
    #         else:
    #             print(f"Already EPSG:3338: {os.path.basename(path)}")
    #             vrt_datasets.append(src)

    #     # Double check we have valid opened datasets before merging
    #     if not vrt_datasets:
    #         raise ValueError("No valid TIFF datasets were successfully opened.")

    #     # 3. Merge
    #     print(f"\nMosaicing and resampling {len(vrt_datasets)} files to {target_res}m...")
    #     mosaic, out_trans = merge(
    #         vrt_datasets,
    #         res=target_res,  # Forces the output cell size to 10x10
    #         resampling=Resampling.bilinear,  # Smooths the 5m data into 10m cells (use Resampling.nearest for discrete/classified data)
    #     )

    #     # 4. Define output metadata
    #     out_meta = vrt_datasets[0].meta.copy()
    #     out_meta.update(
    #         {
    #             "driver": "GTiff",
    #             "height": mosaic.shape[1],
    #             "width": mosaic.shape[2],
    #             "transform": out_trans,
    #             "crs": target_crs,
    #         }
    #     )

    #     # 5. Write the final result
    #     with rio.open(output_mosaic, "w", **out_meta) as dest:
    #         dest.write(mosaic)

    #     print(f"\nSuccess! Mosaic saved to {output_mosaic}")

    # finally:
    #     # Clean up everything safely
    #     for vrt in vrt_datasets:
    #         if isinstance(vrt, WarpedVRT):
    #             vrt.close()
    #     for src in opened_datasets:
    #         src.close()

    # create_vrt_file(out_dem_folder, 'hand_seamless_3dep_dems.vrt')

    # Create DEM_Domain.gpkg
    __polygonize(out_dem_folder, file_logger)


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
    if lakes.crs != target_crs:
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
    if catchments.crs != target_crs:
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
    if WBD.crs != target_crs:
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
    parser.add_argument('-s', '--reference_fabric_file', type=str, help='Name of streams file')
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
