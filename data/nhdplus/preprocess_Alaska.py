#!/usr/bin/env python3

# Before using this file, ifsar DTM file needs to be downloaded from AK DGGS
# at https://elevation.alaska.gov. This file will be named custom_download.zip

import argparse
import glob
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from osgeo import gdal
from rasterio.crs import CRS

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

    # 1. Initialize an empty list instead of an empty GeoDataFrame
    gdfs = []

    for n, dem_file in enumerate(dem_files):
        sf.l_print(f"Polygonizing: {dem_file}", file_logger, "info")
        edge_tif = f'{os.path.splitext(dem_file)[0]}_edge.tif'
        edge_parquet = f'{os.path.splitext(edge_tif)[0]}.parquet'

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

        polygonize(edge_tif, edge_parquet, connectivity=8, quiet=True)

        gdf = gpd.read_parquet(edge_parquet)

        # 2. Simply append to the list
        gdfs.append(gdf)

        os.remove(edge_tif)
        os.remove(edge_parquet)

    # 3. Concatenate once and explicitly wrap in a GeoDataFrame to satisfy Pylance
    if gdfs:
        dem_parquets = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    else:
        dem_parquets = gpd.GeoDataFrame()

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


def preprocess_dem(input_dem_zip_file, out_dem_folder, region, target_crs_number='3338'):
    """
    Preprocess ifsar DTM 5 meter to 10 meter DEM
    """
    overall_start_time = datetime.now(timezone.utc)
    file_dt_string = overall_start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"ifsar_downloaded-{file_dt_string}.log"
    log_file_path = os.path.join(out_dem_folder, log_file_name)
    file_logger = sf.setup_mp_file_logger(log_file_path, "ifsar_downloaded")

    sf.l_print(f"Downloading to {out_dem_folder}", file_logger, "info")

    ### UNZIP AND MOSAIC TILES
    os.makedirs(out_dem_folder, exist_ok=True)

    if not os.path.exists(input_dem_zip_file):
        print(
            f'ERROR: {input_dem_zip_file} does not exist. It needs to be downloaded from AK DGGS (https://elevation.alaska.gov)'
        )
        sys.exit(1)

    unzip(input_dem_zip_file, out_dem_folder)

    output_mosaic = os.path.join(out_dem_folder, f'{region}_ifsar_DTM_{target_crs_number}.tif')
    target_res = 10

    absolute_out_folder = os.path.abspath(out_dem_folder)

    extracted_zip_dir = os.path.join(absolute_out_folder, "dds4", "ifsar", "dtm")
    zip_pattern = os.path.join(extracted_zip_dir, "*.zip")
    zip_files = glob.glob(zip_pattern)

    src_files_to_mosaic = []
    print(f"Scanning {len(zip_files)} extracted sub-archives for TIFFs...")

    for sub_zip_path in zip_files:
        try:
            with zipfile.ZipFile(sub_zip_path, 'r') as sub_zip:
                tif_names = [
                    name
                    for name in sub_zip.namelist()
                    if name.lower().endswith('.tif') or name.lower().endswith('.tiff')
                ]

                for tif_name in tif_names:
                    vsi_path = f"/vsizip/{sub_zip_path}/{tif_name}"
                    src_files_to_mosaic.append(vsi_path)
        except Exception as e:
            print(f"Warning: Could not read sub-archive {sub_zip_path}: {e}")

    if not src_files_to_mosaic:
        raise FileNotFoundError(
            f"\n[ERROR] No .tif files could be mapped inside the extracted sub-zips in: {extracted_zip_dir}\n"
        )

    print(f"Successfully mapped {len(src_files_to_mosaic)} sub-zip TIFF datasets for mosaicing.")

    # CRITICAL FIX: Every rasterio loop has been deleted from here to release file locks!
    print(f"\nMosaicing, reprojecting, and resampling {len(src_files_to_mosaic)} files to 10m...")

    try:
        # Convert all config constraints into clean options parameters
        warp_options = gdal.WarpOptions(
            format="GTiff",
            dstSRS=f"EPSG:{target_crs_number}",
            xRes=target_res,
            yRes=target_res,
            resampleAlg="bilinear",
            outputType=gdal.GDT_Float32,
            options=[
                '-overwrite',
                '-srcnodata',
                '-10000',  # Targets any explicit -10000 values
                '-dstnodata',
                '-999999',  # Forces unassigned space and empty borders to -999999
            ],
        )

        # Warp crunches straight from the unlocked virtual paths
        gdal.Warp(output_mosaic, src_files_to_mosaic, options=warp_options)
        print(f"\nSuccess! Mosaic saved to {output_mosaic}")

    except Exception as e:
        raise RuntimeError(f"GDAL Warp processing failed: {e}")

    create_vrt_file(out_dem_folder, 'hand_seamless_3dep_dems.vrt')
    __polygonize(out_dem_folder, file_logger)


def preprocess_streams(region, hucs, target_crs_number, inputs_dir, reference_fabric_file):
    """
    Preprocess Alaska streams for a specific region.
    """
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

    headwater_points = findHeadWaterPoints(flowpaths)
    headwater_points.to_file(
        os.path.join(reference_fabric_folder, f'flowpaths_headwaters_{region}.gpkg'), driver='GPKG'
    )

    lakes = gpd.read_file(reference_fabric_file, layer='lakes')
    lakes = lakes.rename(columns={'Hylak_id': 'LakeID'})
    if lakes.crs != target_crs:
        lakes = lakes.to_crs(epsg=target_crs_number)
    lakes = lakes[['LakeID', 'geometry']]
    lakes.to_file(os.path.join(reference_fabric_folder, f'lakes_{region}.gpkg'), driver='GPKG')

    flowpaths = flowpaths.sjoin(lakes, how='left', predicate='intersects')
    flowpaths = flowpaths.rename(columns={'LakeID': 'Lake'})
    flowpaths['Lake'] = flowpaths['Lake'].fillna(-9999).astype(int)

    flowpaths.to_file(os.path.join(reference_fabric_folder, f'flowpaths_{region}.gpkg'), driver='GPKG')

    catchments = gpd.read_file(reference_fabric_file, layer='divides')
    catchments = catchments.rename(columns={'divide_id': 'ID'})
    if catchments.crs != target_crs:
        catchments = catchments.to_crs(epsg=target_crs_number)
    catchments.to_file(os.path.join(reference_fabric_folder, f'catchments_{region}.gpkg'), driver='GPKG')

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
    parser = argparse.ArgumentParser(description="Preprocess Alaska data for a specified region.")
    parser.add_argument("-r", "--region", type=str, required=True, help="Options: 'Fairbanks', 'Juneau'")
    parser.add_argument("-i", "--inputs_dir", type=str, required=True, help="Input files folder directory")
    parser.add_argument(
        '-c', '--target_crs_number', type=int, required=False, default='3338', help='EPSG code'
    )
    parser.add_argument('-s', '--reference_fabric_file', type=str, help='Streams file path')
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

    # preprocess_dem(input_dem_zip_file, out_dem_folder, region, target_crs_number)

    if region == 'Fairbanks':
        hucs = ['19080306', '19080307']
    elif region == 'Juneau':
        hucs = ['19010301']

    preprocess_streams(region, hucs, target_crs_number, inputs_dir, reference_fabric_file)
