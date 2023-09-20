#!/usr/bin/env python3

import argparse
import logging
import multiprocessing
import os
import re
import shutil
import sys
from datetime import datetime
from multiprocessing import Pool

import rasterio
from inundate_mosaic_wrapper import produce_mosaicked_inundation
from osgeo import gdal, ogr
from rasterio.merge import merge

from utils.shared_functions import FIM_Helpers as fh
from utils.shared_variables import PREP_PROJECTION, elev_raster_ndv


# INUN_REVIEW_DIR = r'/data/inundation_review/inundation_nwm_recurr/'
# INUN_OUTPUT_DIR = r'/data/inundation_review/inundate_nation/'
# INPUTS_DIR = r'/data/inputs'
# OUTPUT_BOOL_PARENT_DIR = '/data/inundation_review/inundate_nation/bool_temp/
# DEFAULT_OUTPUT_DIR = '/data/inundation_review/inundate_nation/mosaic_output/'


def inundate_nation(fim_run_dir, output_dir, magnitude_key, flow_file, huc_list, inc_mosaic, job_number):
    assert os.path.exists(flow_file), f"ERROR: could not find the flow file: {flow_file}"

    if job_number > available_cores:
        job_number = available_cores - 1
        print(
            "Provided job number exceeds the number of available cores. "
            + str(job_number)
            + " max jobs will be used instead."
        )

    fim_version = os.path.basename(os.path.normpath(fim_run_dir))
    logging.info(f"Using fim version: {fim_version}")
    output_base_file_name = magnitude_key + "_" + fim_version
    # print(output_base_file_name)

    __setup_logger(output_dir, output_base_file_name)

    start_dt = datetime.now()

    logging.info(f"Input FIM Directory: {fim_run_dir}")
    logging.info(f"output_dir: {output_dir}")
    logging.info(f"magnitude_key: {magnitude_key}")
    logging.info(f"flow_file: {flow_file}")
    logging.info(f"inc_mosaic: {str(inc_mosaic)}")

    print("Preparing to generate inundation outputs for magnitude: " + magnitude_key)
    print("Input flow file: " + flow_file)

    magnitude_output_dir = os.path.join(output_dir, output_base_file_name)

    if not os.path.exists(magnitude_output_dir):
        print("Creating new output directory for raw mosaic files: " + magnitude_output_dir)
        os.mkdir(magnitude_output_dir)
    else:
        # we need to empty it. we will kill it and remake it (using rmtree to force it)
        shutil.rmtree(magnitude_output_dir, ignore_errors=True)
        os.mkdir(magnitude_output_dir)

    if huc_list is None:
        huc_list = []
        for huc in os.listdir(fim_run_dir):
            # if (
            #     huc != 'logs'
            #     and huc != 'branch_errors'
            #     and huc != 'unit_errors'
            #     and os.path.isdir(os.path.join(fim_run_dir, huc))
            # ):
            if re.match(r'\d{8}', huc):
                huc_list.append(huc)
    else:
        for huc in huc_list:
            assert os.path.isdir(
                fim_run_dir + os.sep + huc
            ), f'ERROR: could not find the input fim_dir location: {fim_run_dir + os.sep + huc}'

    print("Inundation raw mosaic outputs here: " + magnitude_output_dir)

    run_inundation([fim_run_dir, huc_list, magnitude_key, magnitude_output_dir, flow_file, job_number])

    # Perform mosaic operation
    if inc_mosaic:
        fh.print_current_date_time()
        logging.info(datetime.now().strftime("%Y_%m_%d-%H_%M_%S"))
        print("Performing bool mosaic process...")
        logging.info("Performing bool mosaic process...")

        output_bool_dir = os.path.join(output_dir, "bool_temp")
        if not os.path.exists(output_bool_dir):
            os.mkdir(output_bool_dir)
        else:
            # we need to empty it. we will kill it and remake it (using rmtree to force it)
            shutil.rmtree(output_bool_dir, ignore_errors=True)
            os.mkdir(output_bool_dir)

        procs_list = []
        for rasfile in os.listdir(magnitude_output_dir):
            if rasfile.endswith(".tif") and "extent" in rasfile:
                # p = magnitude_output_dir + rasfile
                procs_list.append([magnitude_output_dir, rasfile, output_bool_dir])

        # Multiprocess --> create boolean inundation rasters for all hucs
        if len(procs_list) > 0:
            with Pool(processes=job_number) as pool:
                pool.map(create_bool_rasters, procs_list)
        else:
            msg = f"Did not find any valid FIM extent rasters: {magnitude_output_dir}"
            print(msg)
            logging.info(msg)

        # now cleanup the raw mosiac directories
        shutil.rmtree(output_bool_dir, ignore_errors=True)

    # now cleanup the raw mosiac directories
    shutil.rmtree(magnitude_output_dir, ignore_errors=True)

    fh.print_current_date_time()
    logging.info(logging.info(datetime.now().strftime("%Y_%m_%d-%H_%M_%S")))
    end_time = datetime.now()
    logging.info(fh.print_date_time_duration(start_dt, end_time))


def run_inundation(args):
    """
    This script is a wrapper for the inundate function and is designed for multiprocessing.

    Args:
        args (list): [fim_run_dir (str), huc_list (list), magnitude (str),
            magnitude_output_dir (str), forecast (str), job_number (int)]

    """

    fim_run_dir = args[0]
    huc_list = args[1]
    magnitude = args[2]
    magnitude_output_dir = args[3]
    forecast = args[4]
    job_number = args[5]

    # Define file paths for use in inundate().

    inundation_raster = os.path.join(magnitude_output_dir, magnitude + "_inund_extent.tif")

    print("Running the NWM recurrence intervals for HUC inundation (extent) for magnitude: " + str(magnitude))

    produce_mosaicked_inundation(
        fim_run_dir,
        huc_list,
        forecast,
        inundation_raster=inundation_raster,
        num_workers=job_number,
        remove_intermediate=True,
        verbose=True,
        is_mosaic_for_branches=True,
    )


def create_bool_rasters(args):
    in_raster_dir = args[0]
    rasfile = args[1]
    output_bool_dir = args[2]

    print("Calculating boolean inundate raster: " + rasfile)
    p = in_raster_dir + os.sep + rasfile
    raster = rasterio.open(p)
    profile = raster.profile
    array = raster.read()
    del raster
    array[array > 0] = 1
    array[array <= 0] = 0
    # And then change the band count to 1, set the
    # dtype to uint8, and specify LZW compression.
    profile.update(
        driver="GTiff",
        height=array.shape[1],
        width=array.shape[2],
        tiled=True,
        nodata=0,
        blockxsize=512,
        blockysize=512,
        dtype="int8",
        compress="lzw",
    )
    with rasterio.open(output_bool_dir + os.sep + "bool_" + rasfile, "w", **profile) as dst:
        dst.write(array.astype(rasterio.int8))


def __setup_logger(output_folder_path, log_file_name_key):
    start_time = datetime.now()
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"{log_file_name_key}-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format="%(message)s")

    # yes.. this can do console logs as well, but it can be a bit unstable and ugly

    logging.info(f'Started : {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


if __name__ == "__main__":
    """
    Sample usage:
    python3 /foss_fim/tools/inundate_nation.py
        -r /outputs/fim_4_0_9_2 -m 100_0
        -f /data/inundation_review/inundation_nwm_recurr/nwm_recurr_flow_data/nwm21_17C_recurr_100_0_cms.csv
        -s
        -j 10
    outputs become /data/inundation_review/inundate_nation/100_0_fim_4_0_9_2_mosiac.tif (.log, etc)

    python3 /foss_fim/tools/inundate_nation.py
        -r /outputs/fim_4_0_9_2
        -m hw
        -f /data/inundation_review/inundation_nwm_recurr/nwm_recurr_flow_data/nwm_high_water_threshold_cms.csv
        -s
        -j 10
    outputs become /data/inundation_review/inundate_nation/hw_fim_4_0_9_2_mosiac.tif (.log, etc)

    If run on UCS2, you can map docker as -v /dev_fim_share../:/data -v /local...outputs:/outputs
    -v .../inundation-mapping/:/foss_fim as normal.
    """

    available_cores = multiprocessing.cpu_count()

    # Parse arguments.
    parser = argparse.ArgumentParser(
        description='Inundation mapping for FOSS FIM using streamflow '
        'recurrence interflow data. Inundation outputs are stored in the '
        '/inundation_review/inundation_nwm_recurr/ directory.'
    )

    parser.add_argument(
        '-r',
        '--fim-run-dir',
        help='Name of directory containing outputs '
        'of fim_run.sh (e.g. data/ouputs/dev_abc/12345678_dev_test)',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output-dir',
        help='Optional: The path to a directory to write the '
        'outputs. If not used, the inundation_nation directory is used by default '
        'ie) /data/inundation_review/inundate_nation/',
        default='/data/inundation_review/inundate_nation/',
        required=False,
    )

    parser.add_argument(
        '-m',
        '--magnitude_key',
        help='used in output folders names and temp files, '
        'added to output_file_name_key ie 100_0, 2_0, hw, etc)',
        required=True,
    )

    parser.add_argument(
        '-f',
        '--flow_file',
        help='the path and flow file to be used. '
        'ie /data/inundation_review/inundation_nwm_recurr/nwm_recurr_flow_data/'
        'nwm_high_water_threshold_cms.csv',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--huc-list',
        help='OPTIONAL: HUC list to run specified HUC(s).Specifiy multiple hucs single space delimited'
        '--> 12090301 12090302. Default (no huc list provided) will use hucs found in -r directory',
        required=False,
        default='all',
        nargs='+',
    )

    parser.add_argument(
        '-s',
        '--inc_mosaic',
        help='Optional flag to produce mosaic of FIM extent rasters',
        action='store_true',
    )

    parser.add_argument('-j', '--job-number', help='The number of jobs', required=False, default=1, type=int)

    args = vars(parser.parse_args())

    inundate_nation(**args)
