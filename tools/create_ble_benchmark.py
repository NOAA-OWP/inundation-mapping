#!/usr/bin/env python3

import os
import argparse
import numpy as np
import pandas as pd
from osgeo import gdal
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
from glob import glob
from preprocess_benchmark import preprocess_benchmark_static


def create_ble_benchmark(input_file, save_folder, reference_raster, benchmark_folder=None):
    """
    This function will download and preprocess BLE benchmark datasets for purposes of evaluating FIM output. A benchmark dataset will be transformed using properties (CRS, resolution) from an input reference dataset. The benchmark raster will also be converted to a boolean (True/False) raster with inundated areas (True or 1) and dry areas (False or 0).
    
    Parameters
    ----------
    input_file: str
        Path to input file (e.g. EBFE_urls_20230608.csv)
    save_folder: str
        Path to save folder
    reference_raster: str
        Path to reference raster
    out_raster_path: str, optional
        Path to the output raster. The default is None.

    Returns
    -------
    None
    """

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # EBFE_urls_20230608.xlsx acquired from FEMA (fethomps@usgs.gov)

    # NOTE: Need to convert .xlsx to .csv prior to running this script (or use pandas.read_excel but need to install xlrd or openpyxl first)

    data = pd.read_csv(input_file, header=None, names=['size', 'units', 'URL'])

    # Subset Spatial Data URLs
    spatial_df = data[data['URL'].str.contains('SpatialData') & data['URL'].str.contains('13020102')] #TODO: Remove hard-coded HUC

    # Convert size to MiB
    spatial_df['MiB'] = np.where(spatial_df['units']=='GiB', spatial_df['size'] * 1000, spatial_df['size'])

    spatial_df = spatial_df.reset_index()

    spatial_df = download_and_extract_rasters(spatial_df, save_folder)


    # TODO: tools/preprocess_benchmark.py
    for i, row in spatial_df.iterrows():
        for benchmark_raster in row['rasters']:
            magnitude = '100yr' if 'BLE_DEP01PCT' in benchmark_raster else '500yr'

            out_raster_dir = os.path.join(benchmark_folder, magnitude)
            if not os.path.exists(out_raster_dir):
                os.makedirs(out_raster_dir)
                
            out_raster_path = os.path.join(out_raster_dir, f"ble_huc_{row['HUC']}_extent_{magnitude}.tif")
            
            preprocess_benchmark_static(benchmark_raster, reference_raster, out_raster_path)

    # TODO: tools/create_flow_forecast_file.py


def download_and_extract_rasters(spatial_df, save_folder):
    """
    Download and extract rasters from URLs in spatial_df. Extracted rasters will be saved to save_folder.

    Parameters
    ----------
    spatial_df: pandas.DataFrame
        DataFrame containing URLs to download and extract
    save_folder: str
        Path to save folder

    Returns
    -------
    spatial_df: pandas.DataFrame
        Updated DataFrame containing HUC and HUC_Name
    out_files: list
        List of paths to extracted rasters
    """
    depth_rasters = ['BLE_DEP0_2PCT', 'BLE_DEP01PCT']
    hucs = []
    huc_names = []
    out_files = []

    # Download and unzip each file
    for i, row in spatial_df.iterrows():
        # Extract HUC and HUC Name from URL
        huc, huc_name = os.path.basename(os.path.dirname(row['URL'])).split('_')
        hucs.append(huc)
        huc_names.append(huc_name)

        # Download and unzip file
        save_file = os.path.join(save_folder, os.path.basename(row['URL']))
        if not os.path.exists(save_file):
            http_response = urlopen(row['URL'])
            zipfile = ZipFile(BytesIO(http_response.read()))
            zipfile.extractall(path=save_file)

        gdb_folder = os.path.join(save_file, 'Spatial')
        gdb_list = glob(os.path.join(gdb_folder, '*.gdb'))
        if len(gdb_list) == 1:
            out_list = []
            src_ds = gdal.Open(gdb_list[0])
            subdatasets = src_ds.GetSubDatasets()

            # Find depth rasters
            for depth_raster in depth_rasters:
                out_file = os.path.join(save_folder, f'{depth_raster}_{huc}.tif')

                if not os.path.exists(out_file):
                    # Read raster data from GDB
                    print(f'Reading {depth_raster} for {huc}')
                    depth_raster_path = [item[0] for item in subdatasets if depth_raster in item[1]][0]
                    data_ds = gdal.Open(depth_raster_path, gdal.GA_ReadOnly)
                    data = data_ds.ReadAsArray()

                    # Save as GeoTIFF
                    driver = gdal.GetDriverByName('GTiff')
                    out_ds = driver.Create(out_file, data.shape[1], data.shape[0], 1, gdal.GDT_Float32)
                    out_ds.SetGeoTransform(data_ds.GetGeoTransform())
                    out_ds.SetProjection(data_ds.GetProjection())
                    out_ds.GetRasterBand(1).WriteArray(data)
                    out_ds = None

                out_list.append(out_file)

        out_files.append(out_list)

    spatial_df['HUC'] = hucs
    spatial_df['HUC_Name'] = huc_names
    spatial_df['rasters'] = out_files

    return spatial_df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create BLE benchmark files')
    parser.add_argument('-i', '--input-file', type=str, help='Input file', required=True)
    parser.add_argument('-s', '--save-folder', type=str, help='Output folder', required=True)
    parser.add_argument('-r', '--reference-raster', type=str, help='Reference raster', required=True)
    parser.add_argument('-o', '--benchmark-folder', type=str, help='Benchmark folder', required=True)

    args = vars(parser.parse_args())

    create_ble_benchmark(**args)