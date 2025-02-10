#!/usr/bin/env python3

import argparse
import os
from datetime import datetime
from glob import glob
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import gemgis
import numpy as np
import pandas as pd
import rasterio
from create_flow_forecast_file import create_flow_forecast_file
from preprocess_benchmark import preprocess_benchmark_static


def create_ble_benchmark(
    input_file: str,
    save_folder: str,
    reference_folder: str,
    benchmark_folder: str,
    nwm_geopackage: str,
    ble_xs_layer_name: str,
    nwm_stream_layer_name: str,
    nwm_feature_id_field: str,
    huc: str = None,
):
    """
    Downloads and preprocesses BLE benchmark datasets for purposes of evaluating FIM output.
    A benchmark dataset will be transformed using properties (CRS, resolution) from an input reference
    dataset.  The benchmark raster will also be converted to a boolean (True/False) raster with inundated
    areas (True or 1) and dry areas (False or 0).

    As the reference_raster is required for preprocessing, it is assumed that fim_pipeline.py has previously
    been run for the HUCs being processed.

    Parameters
    ----------
    input_file: str
        Path to input file (XLSX or CSV) with list of URL(s) to files to download
        (e.g. EBFE_urls_20230608.xlsx)
    save_folder: str
        Path to save folder where the downloaded ZIP files and extracted depth rasters will be saved
        (e.g., /data/temp/ble_downloads).
        This folder will be created if it doesn't exist.
    reference_folder: str
        Path to reference raster (e.g., /data/outputs/fim_4_3_12_0). This folder must exist
        (e.g., created by running fim_pipeline.py).
    benchmark_folder: str
        Path to the benchmark folder (e.g., /data/test_cases/ble_test_cases/validation_data_ble).
        This folder will be created if it doesn't exist.

    Returns
    -------
    None
    """

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # EBFE_urls_20230608.xlsx acquired from FEMA (fethomps@usgs.gov)
    ext = os.path.splitext(input_file)[1]
    if ext == '.xlsx':
        data = pd.read_excel(input_file, header=None, names=['size', 'units', 'URL'])
    elif ext == '.csv':
        data = pd.read_csv(input_file, header=None, names=['size', 'units', 'URL'])

    # Subset Spatial Data URLs
    spatial_df = data[data['URL'].str.contains('SpatialData')]
    if huc is not None:
        spatial_df = spatial_df[spatial_df['URL'].str.contains(huc)]

    spatial_df = spatial_df.reset_index()

    # Convert size to MiB
    spatial_df['MiB'] = np.where(spatial_df['units'] == 'GiB', spatial_df['size'] * 1000, spatial_df['size'])

    spatial_df, ble_geodatabase = download_and_extract_rasters(spatial_df, save_folder)

    for i, row in spatial_df.iterrows():
        huc = row['HUC']
        # reference_raster is used to set the metadata for benchmark_raster
        reference_raster = os.path.join(reference_folder, f'{huc}/branches/0/rem_zeroed_masked_0.tif')
        for benchmark_raster in row['rasters']:
            magnitude = '100yr' if 'BLE_DEP01PCT' in benchmark_raster else '500yr'

            out_raster_dir = os.path.join(benchmark_folder, huc, magnitude)
            if not os.path.exists(out_raster_dir):
                os.makedirs(out_raster_dir)

            out_raster_path = os.path.join(out_raster_dir, f"ble_huc_{huc}_extent_{magnitude}.tif")

            # Make benchmark inundation raster
            preprocess_benchmark_static(benchmark_raster, reference_raster, out_raster_path)

            create_flow_forecast_file(
                huc,
                ble_geodatabase,
                nwm_geopackage,
                benchmark_folder,
                ble_xs_layer_name,
                nwm_stream_layer_name,
                nwm_feature_id_field,
            )


def download_and_extract_rasters(spatial_df: pd.DataFrame, save_folder: str):
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

    depth_rasters = ['RasterBLE_DEP0_2PCT', 'RasterBLE_DEP01PCT']

    # Download and unzip each file
    hucs = []
    huc_names = []
    out_files = []
    out_list = []
    for i, row in spatial_df.iterrows():
        # Extract HUC and HUC Name from URL
        url = row['URL']
        huc, huc_name = os.path.basename(os.path.dirname(url)).split('_')
        hucs.append(huc)
        huc_names.append(huc_name)

        # Download and unzip file
        save_file = os.path.join(save_folder, os.path.basename(url))
        if not os.path.exists(save_file):
            http_response = urlopen(url)
            zipfile = ZipFile(BytesIO(http_response.read()))
            zipfile.extractall(path=save_file)

        gdb_list = glob(os.path.join(save_file, '**', '*.gdb'), recursive=True)

        if len(gdb_list) == 1:
            ble_geodatabase = gdb_list[0]

            huc_save_folder = os.path.join(save_folder, huc)
            if not os.path.exists(huc_save_folder):
                os.makedirs(huc_save_folder)

            gemgis.raster.read_raster_gdb(path=ble_geodatabase, path_out=huc_save_folder + '/')

            # Find depth rasters
            for depth_raster in depth_rasters:
                out_file = os.path.join(save_folder, huc, depth_raster + '.tif')

                if os.path.exists(out_file):
                    out_list.append(out_file)

        if len(out_list) > 0:
            out_files.append(out_list)

    spatial_df['HUC'] = hucs
    spatial_df['HUC_Name'] = huc_names
    spatial_df['rasters'] = out_files

    return spatial_df, ble_geodatabase


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create BLE benchmark files',
        usage='''python3 /foss_fim/data/ble/ble_benchmark/create_ble_benchmark.py
                                                -i /data/inputs/ble/ble_benchmark/EBFE_urls_20230608.xlsx
                                                -s /data/temp/ble_benchmark
                                                -r /data/outputs/my_run/
                                                -o /data/test_cases/ble_test_cases/validation_data_ble
                                                -n /data/inputs/nwm_hydrofabric/nwm_flows.gpkg
                                                -u 12030105
                                            ''',
    )
    parser.add_argument('-i', '--input-file', type=str, help='Input file', required=True)
    parser.add_argument('-s', '--save-folder', type=str, help='Output folder', required=True)
    parser.add_argument('-r', '--reference-folder', type=str, help='Reference folder', required=True)
    parser.add_argument('-o', '--benchmark-folder', type=str, help='Benchmark folder', required=True)
    parser.add_argument('-n', '--nwm-geopackage', type=str, help='NWM streams geopackage', required=True)
    parser.add_argument(
        '-u',
        '--huc',
        type=str,
        help='Run a single HUC. If not supplied, it will run all HUCs in the input file',
        required=False,
    )
    parser.add_argument(
        '-xs',
        '--ble-xs-layer-name',
        help='BLE cross section layer. Default layer is "XS" (sometimes it is "XS_1D").',
        required=False,
        default='XS',
    )
    parser.add_argument(
        '-l',
        '--nwm-stream-layer-name',
        help='NWM streams layer. Default layer is "nwm_streams"',
        required=False,
        default='nwm_streams',
    )
    parser.add_argument(
        '-id',
        '--nwm-feature-id-field',
        help='id field for nwm streams. Not required if NWM v2.1 is used (default id field is "ID")',
        required=False,
        default='ID',
    )

    args = vars(parser.parse_args())

    start_time = datetime.now()
    print('Pulling BLE Benchmark data...  \n')

    create_ble_benchmark(**args)

    end_time = datetime.now()
    print('\n Finished Pulling BLE Benchmark data \n', 'Total Time: ', end_time - start_time)
