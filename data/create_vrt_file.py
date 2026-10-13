#!/usr/bin/env python3

import argparse
import logging
import os
from datetime import datetime, timezone

from osgeo import gdal

import src.utils.shared_functions as sf
from src.utils.polygonize_raster import polygonize_extent
from src.utils.shared_functions import FIM_Helpers as fh


def create_vrt_file(
    src_directories, vrt_basename, vrt_output_dir=None, vrt_suffix=None, run_polygonize=False
):
    '''
    Overview
    ----------
    Takes all .tif files in one or more given directories, generates a single relative VRT, and optionally a DEM_Domain parquet.
    '''

    if not src_directories:
        raise ValueError('At least one source directory must be specified.')

    src_directories = [os.path.abspath(d) for d in src_directories]
    for src_dir in src_directories:
        if not os.path.exists(src_dir):
            raise ValueError(f'Source directory "{src_dir}" does not exist.')

    # -------------------
    # Resolve Output Directory Route
    if not vrt_output_dir:
        # Default to the first source directory if no output folder is provided
        vrt_out_dir = src_directories[0]
    else:
        # Resolve path as a directory
        vrt_out_dir = os.path.abspath(vrt_output_dir)

    os.makedirs(vrt_out_dir, exist_ok=True)

    # -------------------
    # Clean basename and resolve suffix string
    base_name_clean, _ = os.path.splitext(vrt_basename)
    suffix_str = f'_{vrt_suffix}' if vrt_suffix else ""

    vrt_file_name = f"{base_name_clean}{suffix_str}.vrt"
    target_vrt_file_path = os.path.join(vrt_out_dir, vrt_file_name)

    # -------------------
    # Setup logs
    start_time = datetime.now(timezone.utc)
    fh.print_start_header('Creating vrt file', start_time)

    __setup_logger(vrt_out_dir)
    logging.info(f"Saving VRT to {target_vrt_file_path}")

    # -------------------
    # Processing
    all_tif_files = []
    for src_dir in src_directories:
        logging.info(f"Searching for .tif files in: {src_dir}")
        tif_files = fh.get_file_names(src_dir, '.tif')

        for f in tif_files:
            full_path = f if os.path.isabs(f) else os.path.join(src_dir, f)
            all_tif_files.append(os.path.abspath(full_path))

    if not all_tif_files:
        raise FileNotFoundError("No .tif files were found in any of the specified directories.")

    # 1. Build the VRT
    __create_vrt(all_tif_files, target_vrt_file_path)

    # 2. Patch XML paths
    __force_relative_paths(target_vrt_file_path, all_tif_files)

    end_time = datetime.now(timezone.utc)
    fh.print_end_header('Finished creating vrt file', start_time, end_time)
    logging.info(fh.print_date_time_duration(start_time, end_time))

    # 3. Optional Polygonize Step
    if run_polygonize:
        parquet_file_name = f"DEM_Domain{suffix_str}.parquet"
        dem_domain_output_path = os.path.join(vrt_out_dir, parquet_file_name)
        logging.info(f"Polygonize flag detected. Saving parquet domain to {dem_domain_output_path}")

        file_logger = logging.getLogger()
        polygonize_extent(all_tif_files, dem_domain_output_path, file_logger, sf=sf, fh=fh)
    else:
        logging.info("Skipping domain parquet creation (use -p / --polygonize to enable).")


def __create_vrt(tif_file_names, target_vrt_file_path):
    logging.info("Building VRT with source files:")
    for file_name in tif_file_names:
        logging.info(f" - {file_name}")

    result = gdal.BuildVRT(target_vrt_file_path, tif_file_names)
    logging.info(f"GDAL build dataset result: {result}")


def __force_relative_paths(vrt_file_path, tif_file_names):
    logging.info("Modifying VRT XML to force relative paths...")
    vrt_dir = os.path.dirname(os.path.abspath(vrt_file_path))

    with open(vrt_file_path, 'r') as f:
        xml_content = f.read()

    for abs_tif_path in tif_file_names:
        rel_tif_path = os.path.relpath(abs_tif_path, start=vrt_dir)

        old_abs_tag_0 = f'<SourceFilename relativeToVRT="0">{abs_tif_path}</SourceFilename>'
        old_abs_tag_1 = f'<SourceFilename relativeToVRT="1">{abs_tif_path}</SourceFilename>'

        new_rel_tag = f'<SourceFilename relativeToVRT="1">{rel_tif_path}</SourceFilename>'

        xml_content = xml_content.replace(old_abs_tag_0, new_rel_tag)
        xml_content = xml_content.replace(old_abs_tag_1, new_rel_tag)

    with open(vrt_file_path, 'w') as f:
        f.write(xml_content)

    logging.info("VRT XML successfully patched with relative paths!")


def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"vrt_build-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    logger = logging.getLogger()
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)

    logging.info(f'Started : {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create a VRT and optional DEM_Domain GeoParquet from source datasets.'
    )

    parser.add_argument(
        '-s',
        '--src_directories',
        nargs='+',
        help='Space-separated list of directories where the .tif files exist.',
        required=True,
    )
    parser.add_argument(
        '-n',
        '--vrt_output_dir',
        help='The output directory where the VRT (and optional parquet) will be saved.',
        required=False,
        default=None,
    )
    parser.add_argument('-b', '--vrt_basename', help='The base name for the VRT file', required=True)
    parser.add_argument(
        '-x',
        '--vrt_suffix',
        help='Optional suffix for the VRT and DEM domain files (example: "Alaska_3338_20260708")',
        required=False,
        default=None,
    )
    parser.add_argument(
        '-p',
        '--polygonize',
        dest='run_polygonize',
        action='store_true',
        help='If specified, also generate the DEM_Domain GeoParquet file.',
        required=False,
    )

    args = vars(parser.parse_args())

    create_vrt_file(**args)
