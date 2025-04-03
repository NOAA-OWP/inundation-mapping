#!/usr/bin/env python3

import argparse
import logging
import os
from datetime import datetime, timezone

from osgeo import gdal

from utils.shared_functions import FIM_Helpers as fh


def create_vrt_file(src_directory, vrt_file_name):
    '''
    Overview
    ----------
    Takes all .tif files in a given directory and creates a vrt from them.
    Note: This assumes all .tifs share a common directory.

    Parameters
    ----------
        - src_directory (str):
            Location where the .tifs are at
        - vrt_file_name (str):
            The name of the vrt file to be created. Note: it will be in the same
            directory as the tifs.
    '''

    # -------------------
    # Validation
    if not os.path.exists(src_directory):
        raise ValueError(f'src_directory value of {src_directory} not set to a valid path')

    if (vrt_file_name is None) or (vrt_file_name == ""):
        raise ValueError('vrt_file_name not defined.')

    if not vrt_file_name.endswith(".vrt"):
        vrt_file_name += ".vrt"

    # -------------------

    target_vrt_file_path = os.path.join(src_directory, vrt_file_name)

    # -------------------
    # setup logs
    start_time = datetime.now(timezone.utc)
    fh.print_start_header('Creating vrt file', start_time)

    __setup_logger(src_directory)
    logging.info(f"Saving vrt to {target_vrt_file_path}")

    # -------------------
    # processing

    tif_file_names = fh.get_file_names(src_directory, '.tif')

    __create_vrt(tif_file_names, target_vrt_file_path)

    end_time = datetime.now(timezone.utc)
    fh.print_end_header('Finished creating vrt file', start_time, end_time)
    logging.info(fh.print_date_time_duration(start_time, end_time))


def __create_vrt(tif_file_names, target_vrt_file_path):
    logging.info("Files included:")
    for file_name in tif_file_names:
        logging.info(f" - {file_name}")

    result = gdal.BuildVRT(target_vrt_file_path, tif_file_names)
    logging.info(result)


def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"vrt_build-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)

    logging.info(f'Started : {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


if __name__ == '__main__':
    # Sample Usage:
    #    python3 /foss_fim/data/create_vrt_file.py
    #    -s /data/inputs/3dep_dems/10m_5070/
    #    -n "fim_seamless_3dep_dem_10m_5070.vrt"

    parser = argparse.ArgumentParser(description='Create a vrt using all tifs in a given directory')

    parser.add_argument(
        '-s',
        '--src_directory',
        help='A directory of where the .tif files '
        'files exist. If the -f (tif-file) param is empty then all .tif files '
        'in this directory will be used.',
        required=True,
    )

    parser.add_argument(
        '-n',
        '--vrt_file_name',
        help='Name of the vrt file (name only) to be created. '
        'Note: it will be created in the source directory.',
        required=True,
    )

    args = vars(parser.parse_args())

    create_vrt_file(**args)
