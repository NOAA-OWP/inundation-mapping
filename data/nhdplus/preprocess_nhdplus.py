#!/usr/bin/env python3

import os
import sys

from acquire_and_preprocess_3dep_dems import polygonize

import geopandas as gpd
from osgeo import gdal
import rasterio as rio

sys.path.append('/foss_fim/data/usgs')

region = 'Guam'
region_code = '22GU'
region_number = '22a'

huc = '22010000'

target_crs_number = '6637'
target_name = f"{region}_{target_crs_number}"
target_folder = f'/data/inputs/nhdplus/{target_name}'
target_dem_folder = '/data/inputs/dems/3dep_dems/10m_{region}'

# Reproject rasters
rasters_list = [
    f'/data/inputs/nhdplus/NHDPlus{region_code}/NHDPlusFdrFac{region_number}/fdr',
    f'/data/inputs/nhdplus/NHDPlus{region_code}/NEDSnapshot/Ned{region_number}/elev_cm',
]

# Define the target CRS
target_crs = f"EPSG:{target_crs_number}"

if not os.path.exists(target_folder):
    os.makedirs(target_folder)
    if not os.path.exists(target_folder):
        sys.exit(f"Target folder {target_folder} does not exist. Exiting...")

for raster in rasters_list:
    if not os.path.exists(raster):
        sys.exit(f"Input raster {raster} does not exist. Exiting...")

    # Define input and output file paths
    output_raster_path = os.path.join(
        target_folder, os.path.splitext(os.path.basename(raster))[0] + f"_{region}_{target_crs_number}.tif"
    )

    # Open the input raster dataset
    input_dataset = gdal.Open(raster)

    # Reproject the raster using gdal.Warp()
    reprojected_dataset = gdal.Warp(output_raster_path, input_dataset, dstSRS=target_crs, xRes=10, yRes=10)

    if reprojected_dataset is None:
        sys.exit(f"Reprojection failed for {raster}. Exiting...")

    # Close the datasets to ensure changes are written to disk
    reprojected_dataset = None
    input_dataset = None

# Convert DEM from cm to m
with rio.open(os.path.join(target_folder, f'elev_cm_{target_name}.tif'), 'r+') as dem:
    elevation_data = dem.read(1).astype(rio.float32)
    elevation_data = elevation_data / 100.0

    nodata = -9999.0
    elevation_data[elevation_data < nodata] = nodata

    meta = dem.meta
    meta.update({'dtype': rio.float32, 'nodata': nodata})
    print(meta)

    # Convert from cm to m
    with rio.open(os.path.join(target_dem_folder, f'HUC8_{huc}_dem.tif'), 'w', **meta) as dst:
        dst.write(elevation_data, 1)

# Create DEM_Domain.gpkg
polygonize(target_dem_folder)

# Extract and reproject NHDPlus streams
nhd_flowline = f'/data/inputs/nhdplus/NHDPlus{region_code}/NHDSnapshot/Hydrography/NHDFlowline.shp'
NHDFlowline = gpd.read_file(nhd_flowline)
if not os.path.exists(nhd_flowline):
    sys.exit(f"NHDFlowline file {nhd_flowline} does not exist. Exiting...")
NHDFlowline = NHDFlowline.to_crs(epsg=6637)
NHDFlowline = NHDFlowline[NHDFlowline['FCode'] != 56600]  # Remove Coastlines

# Add ['ID', 'to', order_'] attributes from NHDPlus
PlusFlow_dbf = gpd.read_file('/data/inputs/nhdplus/NHDPlus22GU/NHDPlusAttributes/PlusFlow.dbf')
PlusFlowlineVAA_dbf = gpd.read_file('/data/inputs/nhdplus/NHDPlus22GU/NHDPlusAttributes/PlusFlowlineVAA.dbf')

NHDFlowline = NHDFlowline.merge(
    PlusFlow_dbf[['FROMCOMID', 'TOCOMID']], left_on='ComID', right_on='FROMCOMID', how='left'
)
NHDFlowline = NHDFlowline.merge(PlusFlowlineVAA_dbf[['ComID', 'StreamOrde']], on='ComID', how='left')
NHDFlowline = NHDFlowline.rename(columns={'ComID': 'ID', 'TOCOMID': 'to', 'StreamOrde': 'order_'})

NHDFlowline.to_file(
    os.path.join(target_folder, f'NHDFlowline_{target_name}.gpkg'), layer='NHDFlowline', driver='GPKG'
)

# Extract and reproject NHDPlus catchments
nhd_catchment = '/data/inputs/nhdplus/NHDPlus22GU/NHDPlusCatchment/Catchment.shp'
if not os.path.exists(nhd_catchment):
    sys.exit(f"NHDCatchment file {nhd_catchment} does not exist. Exiting...")
NHDCatchment = gpd.read_file(nhd_catchment)
NHDCatchment = NHDCatchment.to_crs(epsg=6637)
NHDCatchment.to_file(
    f'/data/inputs/nhdplus/Guam_6637/NHDCatchment_{target_name}.gpkg', layer='NHDCatchment', driver='GPKG'
)
