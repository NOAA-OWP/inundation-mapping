#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import sys
import traceback
from datetime import datetime
from glob import glob
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen
from zipfile import ZipFile

import numpy as np
import pandas as pd

# import fiona
import rasterio
from create_flow_forecast_file import create_flow_forecast_file
from osgeo import gdal
from preprocess_benchmark import preprocess_benchmark_static

from utils.shared_validators import is_valid_huc


gdal.UseExceptions()


def __setup_logger(output_folder_path):
    start_time = datetime.now()
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"create_ble_benchmark-{file_dt_string}.log"
    log_file_path = os.path.join(output_folder_path, log_file_name)

    # logging.basicConfig(
    #     format='%(asctime)s -- ',
    #     level=logging.INFO,
    #     datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s >> %(message)s", "%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


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

    __setup_logger(save_folder)

    start_time = dt.datetime.now(dt.timezone.utc)

    print("==================================")
    logging.info("Starting load of BLE benchmark data")
    logging.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    print()

    # Latest is:
    # EBFE_urls_20240426.xlsx acquired from FEMA (fethomps@usgs.gov) (Florence Thompson)

    # *** Note: with each new file we get from Florence, you might have to adjust the columns below.
    ext = os.path.splitext(input_file)[1]
    if ext == '.xlsx':
        data = pd.read_excel(input_file, header=0, names=['date', 'size', 'units', 'URL'])
    elif ext == '.csv':
        data = pd.read_csv(input_file, header=None, names=['size', 'units', 'URL'])

    # Subset Spatial Data URLs
    spatial_df = data[data['URL'].str.contains('SpatialData')]

    if huc is not None:
        spatial_df = spatial_df[spatial_df['URL'].str.contains(huc)]

    spatial_df.reset_index(inplace=True)

    # Convert size to MiB
    spatial_df['MiB'] = np.where(spatial_df['units'] == 'GiB', spatial_df['size'] * 1000, spatial_df['size'])

    # create some new empty columns
    num_cols = len(spatial_df.columns)
    spatial_df.insert(num_cols, "HUC", "")
    spatial_df.insert(num_cols + 1, "HUC_Name", "")
    spatial_df.insert(num_cols + 2, "raster_paths", "")

    spatial_df, ble_geodatabase = download_and_extract_rasters(spatial_df, save_folder)

    spatial_df = spatial_df[spatial_df['HUC'] != ""]
    spatial_df = spatial_df[spatial_df['raster_paths'] != ""]
    spatial_df.reset_index(inplace=True)

    print()
    logging.info("=======================================")
    logging.info("")
    logging.info("Creating flow files")
    logging.info("")

    num_recs = len(spatial_df)
    for i, row in spatial_df.iterrows():
        huc = row['HUC']
        logging.info("++++++++++++++++++++++++++")
        logging.info(f"HUC is {huc}")
        print(f" Flow file {i+1} of {num_recs}")

        # Reference_raster is used to set the metadata for benchmark_raster
        reference_raster = os.path.join(reference_folder, f'{huc}/branches/0/rem_zeroed_masked_0.tif')
        if os.path.exists(reference_raster) is False:
            logging.info(
                f" {huc} : Raster path of {reference_raster} does not exist." " Might be a non-applicable HUC"
            )
            continue

        for benchmark_raster in row['raster_paths']:

            print(f" {huc} : benchmark raster is {benchmark_raster}")
            magnitude = '100yr' if 'BLE_DEP01PCT' in benchmark_raster else '500yr'

            out_raster_dir = os.path.join(benchmark_folder, huc, magnitude)
            if not os.path.exists(out_raster_dir):
                os.makedirs(out_raster_dir)

            out_raster_path = os.path.join(out_raster_dir, f"ble_huc_{huc}_extent_{magnitude}.tif")

            # Make benchmark inundation raster
            logging.info(f" {huc} : magnitude : {magnitude} : preprocessing stats")
            try:
                preprocess_benchmark_static(benchmark_raster, reference_raster, out_raster_path)

            except Exception:
                logging.info(f"HUC {huc} - An error with preprocessing benchmark stats")
                logging.info(traceback.format_exc())
                continue

            logging.info(f" {huc} : magnitude : {magnitude} : creating flow file")
            try:
                create_flow_forecast_file(
                    huc,
                    ble_geodatabase,
                    nwm_geopackage,
                    benchmark_folder,
                    ble_xs_layer_name,
                    nwm_stream_layer_name,
                    nwm_feature_id_field,
                )
            except Exception:
                logging.info(f"HUC {huc} - An error with creating flow files has occured.")
                logging.info(traceback.format_exc())
                continue

        logging.info(f"HUC {huc} flowfiles complete")

    # Get time metrics
    end_time = dt.datetime.now(dt.timezone.utc)
    logging.info("")
    print("==================================")
    logging.info("Complete")
    logging.info(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")

    time_delta = end_time - start_time
    total_seconds = int(time_delta.total_seconds())

    ___, rem_seconds = divmod(total_seconds, 60 * 60 * 24)
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

    logging.info(f"Duration: {time_fmt}")
    print()
    return


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

    depth_rasters = ['BLE_DEP0_2PCT', 'BLE_DEP01PCT']

    # Download and unzip each file
    hucs = []
    # huc_names = []
    # out_files = []
    # out_list = []

    logging.info("=======================================")
    logging.info("Extracting and downloading spatial data")

    num_recs = len(spatial_df)
    logging.info(f"  Number of HUC records to download is {num_recs}")
    print()
    for idx, row in spatial_df.iterrows():

        # Extract HUC and HUC Name from URL
        url = row['URL']
        print(f" Downloading {idx + 1} of {num_recs}")

        logging.info("")
        logging.info("++++++++++++++++++++++++++")
        logging.info(f"URL is {url}")

        huc, huc_name = os.path.basename(os.path.dirname(url)).split('_')
        # hucs.append(huc)
        # huc_names.append(huc_name)

        # Check to ensure it is a valid HUC (some are not)
        is_ble_huc_valid, ___ = is_valid_huc(huc)

        if not is_ble_huc_valid:
            logging.info(f"{huc} : Invalid huc")
            # out_files.append("")
            continue

        spatial_df.at[idx, 'HUC'] = huc
        spatial_df.at[idx, 'HUC_Name'] = huc_name

        hucs.append(huc)
        # huc_names.append(huc_name)

        # logging.info
        logging.info(f"{huc} : Downloading and extracting for {huc_name}")

        # Download file
        save_file = os.path.join(save_folder, os.path.basename(url))
        logging.info(f"{huc} : Saving download file to {save_file}")
        if not os.path.exists(save_file):
            http_response = urlopen(url)
            zipfile = ZipFile(BytesIO(http_response.read()))
            zipfile.extractall(path=save_file)
        else:
            logging.info(f"{huc} : File already exists. Skipping download.")

        # Loading gdb
        logging.info(f"{huc} : Loading gdb")
        gdb_list = glob(os.path.join(save_file, '**', '*.gdb'), recursive=True)

        if len(gdb_list) == 1:
            ble_geodatabase = gdb_list[0]
            src_ds = gdal.Open(ble_geodatabase, gdal.GA_ReadOnly)
            subdatasets = src_ds.GetSubDatasets()

            src_ds = None

            # Find depth rasters
            raster_paths = []
            for depth_raster in depth_rasters:
                out_file = os.path.join(save_folder, f'{huc}_{depth_raster}.tif')

                if not os.path.exists(out_file):
                    # Read raster data from GDB
                    logging.info(f'{huc} : Reading {depth_raster}')
                    depth_raster_path = [item[0] for item in subdatasets if depth_raster in item[1]][0]

                    if extract_raster(huc, depth_raster_path, out_file) is False:
                        continue

                raster_paths.append(out_file)

                # out_list.append(out_file)

            if len(raster_paths) > 0:
                spatial_df.at[idx, 'raster_paths'] = raster_paths

            # huc_names.append(huc_name)
            # out_files.append(str(huc_rasters))

        # if len(out_list) > 0:
        #    out_files.append(out_list)

    if len(hucs) == 0:
        logging.info("No valid rasters remain. System aborted.")
        sys.exit(1)

    # this is ugly

    # print(spatial_df)

    # print("final hucs")
    # print(hucs)
    # print(huc_names)
    # print(out_files)

    # spatial_df['HUC'] = hucs
    # spatial_df['HUC_Name'] = huc_names
    # spatial_df['raster_paths'] = out_files

    # print(out_files)

    spatial_df.to_csv(os.path.join(save_folder, "spatial_db_w_rasters.csv"), header=True, index=False)
    print()
    logging.info("Extracting and downloading spatial data - COMPLETE")
    print()

    return spatial_df, ble_geodatabase


def extract_raster(huc: str, in_raster: str, out_raster: str):
    """
    Extract raster from GDB and save to out_raster

    Parameters
    ----------
    in_raster: str
        Path to input raster
    out_raster: str
        Path to output raster

    Returns
    -------
    True = raster saved
    """

    data_ds = gdal.Open(in_raster, gdal.GA_ReadOnly)
    gt = data_ds.GetGeoTransform()

    # Why are we using GDAL? becuase it is loading directliy from a gdb
    # ie) in_raster is OpenFileGDB:"/data/test_cases/rob_ble_reload/
    # download_files_test_large/08040301_SpatialData.zip/Spatial/LA_LowerRed_BLE.gdb":BLE_DEP0_2PCT

    # TODO: May 15, 2024
    # Some of them come in as 5m resolution is too big and memory failes. It will load but ReadAsArray fails
    # we tried some gdal warp and various things to get it work.
    # For now, we will just a have to skip anyting that is not 10m.

    logging.info(f"{huc} : Raster reesolution is {gt[1]}")

    if int(gt[1]) != 10:
        #     # resample
        #     layer_name = in_raster
        #     # rename the file
        #     resampled_file_name = file_name.replace(".tif", "_resampled.tif")

        #     print(f"in : {in_raster} and resample : {resampled_file_name}")

        #     print(f" --- gdal wrap : {dt.datetime.now(dt.timezone.utc).strftime('%H:%M:%S')}")
        #     #gdal.WarpOptions(geoloc=True, format='GTiff', xRes=10.0, yRes=10.0)
        #     # kwargs = {'format': 'GTiff', 'geoloc': True}
        #     #gdal.Warp(in_raster, resampled_file_name, geoloc=True, format='GTiff', xRes=10, yRes=10)
        #     gdal.Warp(in_raster, resampled_file_name, format='GTiff')

        #     print(f" --- gdal re-open : {dt.datetime.now(dt.timezone.utc).strftime('%H:%M:%S')}")
        #     # reload the resampled version
        #     data_ds = gdal.Open(in_raster, gdal.GA_ReadOnly)
        #     print(f" --- gdal re-open : {dt.datetime.now(dt.timezone.utc).strftime('%H:%M:%S')}")
        #     gt = data_ds.GetGeoTransform()
        #     print(gt)

        logging.info(
            f"{huc} dropping raster not resolution of 10 m. It is {gt[1]} : raster input = {in_raster}"
        )
        return False

    data = data_ds.ReadAsArray()
    nodata = data_ds.GetRasterBand(1).GetNoDataValue()
    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(out_raster, data.shape[1], data.shape[0], 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(data_ds.GetGeoTransform())
    out_ds.SetProjection(data_ds.GetProjection())
    out_ds.GetRasterBand(1).WriteArray(data)
    out_ds.GetRasterBand(1).SetNoDataValue(nodata)
    data_ds = None
    out_ds = None

    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create BLE benchmark files',
        usage='''python3 /foss_fim/data/ble/ble_benchmark/create_ble_benchmark.py
                                                -i /data/inputs/ble/ble_benchmark/EBFE_urls_20240416.xlsx
                                                -s /data/temp/ble_benchmark
                                                -r /data/outputs/fim_test_BED/
                                                -o /data/test_cases/test/ble_test_cases/validation_data_ble
                                                -n /data/inputs/nwm_hydrofabric/nwm_flows.gpkg
                                                -u 12040101
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

    create_ble_benchmark(**args)
