#!/usr/bin/env python3

import argparse
import os
import sys

import geopandas as gpd
import py7zr
import rasterio as rio
from osgeo import gdal

from data.create_vrt_file import create_vrt_file
from data.nfhl.download_fema_nfhl import download_nfhl_wrapper
from data.usgs.acquire_and_preprocess_3dep_dems import polygonize
from src.derive_headwaters import findHeadWaterPoints


def preprocess_nhdplus(region: str, inputs_dir: str):
    """
    Preprocess NHDPlus data for a specific region.

    Parameters:
        region : str
            The region to preprocess. Options are: 'AmericanSamoa', 'Guam'
        inputs_dir : str
            The directory containing input data files.

    # Datasets downloaded from https://www.epa.gov/waterdata/
    """

    if region == 'Guam':
        region_code = '22GU'
        region_number = '22a'
        huc = '22010000'
        target_crs_number = '6637'
        nhdplus_root_raw = f'{inputs_dir}/nhdplus/'
        dem_file = f'{nhdplus_root_raw}/NHDPlusPI/NHDPlus{region_code}/NEDSnapshot/Ned{region_number}/elev_cm'
        nhd_path = f'{nhdplus_root_raw}/NHDPlusPI/NHDPlus{region_code}/NHDSnapshot/Hydrography'

    elif region == 'AmericanSamoa':
        region_code = '22AS'
        region_number = '22c'
        huc = '22030001'
        target_crs_number = '32702'
        nhdplus_root_raw = f'{inputs_dir}/nhdplus/'
        nhdplus_path = f'{nhdplus_root_raw}/NHDPlusPI/NHDPlus{region_code}'
        dem_file = f'{nhdplus_path}/NEDSnapshot/ned{region_number}/elev_cm'
        nhd_path = f'{nhdplus_path}/NHDSnapshot/hydrography'

    os.makedirs(nhdplus_root_raw, exist_ok=True)

    preprocess_region(
        region,
        region_code,
        region_number,
        huc,
        target_crs_number,
        inputs_dir,
        nhdplus_root_raw,
        nhdplus_path,
        dem_file,
        nhd_path,
    )


def preprocess_region(
    region,
    region_code,
    region_number,
    huc,
    target_crs_number,
    inputs_dir,
    nhdplus_root_raw,
    nhdplus_path,
    dem_file,
    nhd_path,
):
    """
    Preprocess NHDPlus data for a specific region.

    Parameters:
        region : str
            The region to preprocess. Options are: 'AmericanSamoa', 'Guam'
        region_code : str
            The region code.
        huc : str
            The HUC identifier.
        target_crs_number : str
            The target CRS number.
        inputs_dir : str
            The directory containing input data files.
        nhdplus_root : str
            The root directory for NHDPlus data.
        nhdplus_path : str
            The path to the NHDPlus data.
        dem_file : str
            The path to the DEM file.
        nhd_path : str
            The path to the NHD data.

    """
    target_name = f"{region}_{target_crs_number}"
    target_folder = f'{inputs_dir}/nhdplus/{target_name}'
    target_dem_folder = f'{inputs_dir}/dems/3dep_dems/10m_{region}'
    output_nodata = -999999

    os.makedirs(target_dem_folder, exist_ok=True)

    # Define the target CRS
    target_crs = f"EPSG:{target_crs_number}"

    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        if not os.path.exists(target_folder):
            sys.exit(f"Target folder {target_folder} does not exist. Exiting...")

    # Download and extract NHDPlus data
    NHDPlus_urls = [
        # f'https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus22AS/NHDPlusV21_PI_22AS_22c_NEDSnapshot_01.7z'
        f'https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_{region_number}_NEDSnapshot_01.7z',
        f'https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_NHDPlusAttributes_02.7z',
        f'https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_NHDPlusCatchment_01.7z',
        f'https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_NHDSnapshot_01.7z',
        f'https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_WBDSnapshot_02.7z',
    ]

    for url in NHDPlus_urls:
        # archive_path = os.path.join(target_folder, os.path.basename(url))

        # Download url to archive_path
        archive_path = os.path.join(nhdplus_root_raw, os.path.basename(url))
        if not os.path.exists(archive_path):
            os.system(f'wget -O {archive_path} {url}')
        else:
            print(f"Archive '{archive_path}' already exists. Skipping download.")
            continue
        if not os.path.exists(archive_path):
            sys.exit(f"Archive file {archive_path} does not exist. Exiting...")
        print(f"Downloaded archive to '{archive_path}'")

        # Extract the archive to target_folder
        print(f"Extracting '{archive_path}' to '{nhdplus_root_raw}'...")

        try:
            with py7zr.SevenZipFile(archive_path, 'r') as archive:
                archive.extractall(path=nhdplus_root_raw)
            print(f"Successfully extracted '{archive_path}' to '{nhdplus_root_raw}'")
        except py7zr.Bad7zFile:
            print(f"Error: '{archive_path}' is not a valid 7z file.")
        except FileNotFoundError:
            print(f"Error: Archive file not found at '{archive_path}'.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    if not os.path.exists(dem_file):
        sys.exit(f"Input dem_file {dem_file} does not exist. Exiting...")

    # Define input and output file paths
    output_dem_path = os.path.join(
        target_folder, os.path.splitext(os.path.basename(dem_file))[0] + f"_{region}_{target_crs_number}.tif"
    )

    # Open the input dem_file dataset
    input_dataset = gdal.Open(dem_file)

    # Reproject the dem_file using gdal.Warp()
    reprojected_dataset = gdal.Warp(output_dem_path, input_dataset, dstSRS=target_crs, xRes=10, yRes=10)

    if reprojected_dataset is None:
        sys.exit(f"Reprojection failed for {dem_file}. Exiting...")

    # Close the datasets to ensure changes are written to disk
    reprojected_dataset = None
    input_dataset = None

    # Convert DEM from cm to m
    with rio.open(os.path.join(target_folder, f'elev_cm_{target_name}.tif'), 'r+') as dem:
        elevation_data = dem.read(1).astype(rio.float32)
        elevation_data = elevation_data / 100.0

        elevation_data[elevation_data < -999] = output_nodata

        meta = dem.meta
        meta.update({'dtype': rio.float32, 'nodata': output_nodata})
        print(meta)

        # Convert from cm to m
        with rio.open(os.path.join(target_dem_folder, f'HUC8_{huc}_dem.tif'), 'w', **meta) as dst:
            dst.write(elevation_data, 1)

    create_vrt_file(target_dem_folder, 'hand_seamless_3dep_dems.vrt')

    # Create DEM_Domain.gpkg
    polygonize(target_dem_folder)

    # Extract and reproject NHDPlus streams
    nhd_flowline = os.path.join(nhd_path, 'NHDFlowline.shp')

    if not os.path.exists(nhd_flowline):
        sys.exit(f"NHDFlowline file {nhd_flowline} does not exist. Exiting...")
    NHDFlowline = gpd.read_file(nhd_flowline)
    NHDFlowline_geometry = NHDFlowline.force_2d()
    NHDFlowline.geometry = NHDFlowline_geometry
    NHDFlowline = NHDFlowline.to_crs(epsg=target_crs_number)

    # Add ['ID', 'to', order_'] attributes from NHDPlus
    PlusFlow_dbf = gpd.read_file(f'{nhdplus_path}/NHDPlusAttributes/PlusFlow.dbf')
    PlusFlowlineVAA_dbf = gpd.read_file(f'{nhdplus_path}/NHDPlusAttributes/PlusFlowlineVAA.dbf')

    NHDFlowline = NHDFlowline.merge(
        PlusFlow_dbf[['FROMCOMID', 'TOCOMID']], left_on='ComID', right_on='FROMCOMID', how='left'
    )
    NHDFlowline = NHDFlowline.merge(PlusFlowlineVAA_dbf[['ComID', 'StreamOrde']], on='ComID', how='left')
    NHDFlowline = NHDFlowline.rename(columns={'ComID': 'ID', 'TOCOMID': 'to', 'StreamOrde': 'order_'})

    # Set 'to' attribute for Coastline outlets
    coastline_ids = NHDFlowline[NHDFlowline['FCode'] == 56600]['ID'].tolist()  # Find Coastline IDs
    NHDFlowline.loc[NHDFlowline['to'].isin(coastline_ids), 'to'] = (
        0  # Set 'to' = 0 for NHDFlowlines that flow to Coastline (FCode == 56600)
    )
    NHDFlowline = NHDFlowline[NHDFlowline['FCode'] != 56600]  # Remove Coastlines

    # Derive headwater points
    headwater_points = findHeadWaterPoints(NHDFlowline)
    headwater_points.to_file(
        os.path.join(target_folder, f'NHDFlowline_headwaters_{target_name}.gpkg'), driver='GPKG'
    )

    # Extract and reproject NHDPlus waterbodies
    nhd_waterbody = os.path.join(nhd_path, 'NHDWaterbody.shp')
    if not os.path.exists(nhd_waterbody):
        sys.exit(f"NHDWaterbody file {nhd_waterbody} does not exist. Exiting...")
    NHDWaterbody = gpd.read_file(nhd_waterbody)
    NHDWaterbody = NHDWaterbody.rename(columns={'ComID': 'LakeID'})
    NHDWaterbody = NHDWaterbody.to_crs(epsg=target_crs_number)
    NHDWaterbody = NHDWaterbody[['LakeID', 'geometry']]
    NHDWaterbody.to_file(f'{inputs_dir}/nhdplus/{target_name}/NHDWaterbody_{target_name}.gpkg', driver='GPKG')

    NHDFlowline = NHDFlowline.sjoin(NHDWaterbody, how='left', predicate='intersects')
    NHDFlowline = NHDFlowline.rename(columns={'LakeID': 'Lake'})
    NHDFlowline['Lake'] = NHDFlowline['Lake'].fillna(-9999).astype(int)

    NHDFlowline.to_file(os.path.join(target_folder, f'NHDFlowline_{target_name}.gpkg'), driver='GPKG')

    # Extract and reproject NHDPlus catchments
    nhd_catchment = f'{nhdplus_path}/NHDPlusCatchment/Catchment.shp'
    if not os.path.exists(nhd_catchment):
        sys.exit(f"NHDCatchment file {nhd_catchment} does not exist. Exiting...")
    NHDCatchment = gpd.read_file(nhd_catchment)
    NHDCatchment = NHDCatchment.rename(columns={'FeatureID': 'ID'})
    NHDCatchment = NHDCatchment.to_crs(epsg=target_crs_number)
    NHDCatchment.to_file(f'{inputs_dir}/nhdplus/{target_name}/NHDCatchment_{target_name}.gpkg', driver='GPKG')

    # Extract and reproject WBD
    wbd = f'{nhdplus_path}/WBDSnapshot/WBD/WBD_Subwatershed.shp'
    if not os.path.exists(wbd):
        sys.exit(f"WBD file {wbd} does not exist. Exiting...")
    WBD = gpd.read_file(wbd, columns=['HUC_8'])
    WBD = WBD.rename(columns={'HUC_8': 'HUC8'})
    WBD = WBD.dissolve(by='HUC8')
    WBD = WBD.to_crs(epsg=target_crs_number)
    if not os.path.exists(f'{inputs_dir}/wbd'):
        os.makedirs(f'{inputs_dir}/wbd')
    WBD.to_file(f'{inputs_dir}/wbd/WBD_{target_name}.gpkg', layer='WBDHU8', driver='GPKG')

    download_nfhl_wrapper(huc_list=[huc], output_folder=f"{inputs_dir}/fema/nfhl/{region}", num_processes=14)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preprocess NHDPlus data for a specified region.")
    parser.add_argument(
        "-r",
        "--region",
        type=str,
        required=True,
        help="The region to preprocess. Options are: 'AmericanSamoa', 'Guam'",
    )
    parser.add_argument(
        "-i", "--inputs_dir", type=str, required=True, help="The directory containing input data files."
    )

    args = parser.parse_args()

    if args.region not in ['AmericanSamoa', 'Guam']:
        sys.exit("Region not recognized. Options are: 'AmericanSamoa', 'Guam'")

    preprocess_nhdplus(**vars(args))
