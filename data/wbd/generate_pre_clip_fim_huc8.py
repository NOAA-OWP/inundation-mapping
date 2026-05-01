#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import random
import shlex
import shutil
import subprocess
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
from clip_vectors_to_wbd import subset_vector_layers
from dotenv import load_dotenv

from src.utils.shared_functions import FIM_Helpers as fh


"""
TODO: Feb 4. 2026:
    - Update to add new shared_functions mp and logging. We may or may not need the mp log merging.
      We never fully testing MP' all logging to a single parent file. Likely won't work, but TBD.
    - This appears to be an "include"
"""

'''
    Overview:
      This script was created to absolve run_huc.sh from getting the huc level WBD layer, calling
      clip_vectors_to_wbd.py, and clipping the WBD for every run, which added a significant amount of
      processing time for each HUC8. Using this script, we generate the necessary pre-clipped .gpkg files
      for the rest of the processing steps.

      Read in environment variables from src/bash_variables.env & config/params_template.env.
      Parallelize the creation of .gpkg files per HUC:
        Get huc level WBD layer from National, call the subset_vector_layers function, and clip the wbd.
        A plethora gpkg files per huc are generated (see args to subset_vector_layers)
        and placed within the output directory specified as the <outputs_dir> argument.


    Usage:
        generate_pre_clip_fim_huc8.py
            -n /data/inputs/pre_clip_huc8/202540423
            -u /data/inputs/huc_lists/full_huc_list.lst
            -j 6
            -o

    Notes:
      - If running this script to generate new data, modify the pre_clip_huc_dir variable in
        src/bash_variables.env to the corresponding outputs_dir argument after running and testing this script.
      - It can handle all hucs based on the huc list (-u) value passed in. ie) Conus, AK, GU and AS at one run.

    Don't worry about error logs saying "Failed to auto identify EPSG: 7"
'''

srcDir = os.getenv('srcDir')
projectDir = os.getenv('projectDir')

load_dotenv(f'{srcDir}/bash_variables.env')
load_dotenv(f'{projectDir}/config/params_template.env')

# Variables from src/bash_variables.env
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')  # Alaska
GUAM_CRS = os.getenv('GUAM_CRS')  # Guam
AMERICAN_SAMOA_CRS = os.getenv('AMERICAN_SAMOA_CRS')  # American Samoa

input_WBD_gdb = os.getenv('input_WBD_gdb')
input_WBD_gdb_Alaska = os.getenv('input_WBD_gdb_Alaska')  # Alaska
input_WBD_gdb_Guam = os.getenv('input_WBD_gdb_Guam')  # Guam
input_WBD_gdb_AmericanSamoa = os.getenv('input_WBD_gdb_AmericanSamoa')  # American Samoa

input_DEM_domain = os.getenv('input_DEM_domain')
input_DEM_domain_Alaska = os.getenv('input_DEM_domain_Alaska')  # Alaska
input_DEM_domain_Guam = os.getenv('input_DEM_domain_Guam')  # Guam
input_DEM_domain_AmericanSamoa = os.getenv('input_DEM_domain_AmericanSamoa')  # American Samoa

input_landsea = os.getenv('input_landsea')
input_landsea_Alaska = os.getenv('input_landsea_Alaska')  # Alaska
input_landsea_Guam = os.getenv('input_landsea_Guam')  # Guam
input_landsea_AmericanSamoa = os.getenv('input_landsea_AmericanSamoa')  # American Samoa

input_GL_boundaries = os.getenv('input_GL_boundaries')

wbd_buffer_distance = float(os.getenv('wbd_buffer'))


LOG_FILE_PATH = ""

# CLI options: user selects layers to preclip; all others default to copy.
args_preclip_options = [
    "boundaries",
    "nwm_lakes",
    "nwm_streams_headwater",
    "nwm_catchments",
    "levees",
    "osm_bridges",
    "osm_roads",
    "buildings",
]


def __setup_logger(outputs_dir, huc=None, is_multi_proc=False):
    '''
    Set up logging to file. Since log file includes the date, it will be overwritten if this
    script is run more than once on the same day.
    '''
    global LOG_FILE_PATH

    datetime_now = dt.datetime.now(dt.timezone.utc)
    file_dt_string = datetime_now.strftime("%Y_%m_%d-%H_%M_%S")

    if huc is None:
        log_file_name = f"generate_pre_clip_{file_dt_string}.log"
    else:
        log_file_name = f"mp_{huc}_generate_pre_{file_dt_string}.log"

    LOG_FILE_PATH = os.path.join(outputs_dir, log_file_name)

    if not os.path.exists(outputs_dir):
        os.makedirs(outputs_dir, exist_ok=True)

    if os.path.exists(LOG_FILE_PATH):
        os.remove(LOG_FILE_PATH)

    # set up logging to file
    logging.basicConfig(
        filename=LOG_FILE_PATH, level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S'
    )

    # If true, console text will be displayed twice, once from the parent and the other from the MP
    if is_multi_proc is False:

        # set up logging to console
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        # set a format which is simpler for console use
        # add the handler to the root logger
        logging.getLogger('').addHandler(console)


def __merge_mp_logs(log_dir, parent_log_file):
    # Locks for all logs starting with the phrase mp*
    log_file_list = list(Path(log_dir).rglob("mp_*"))

    if len(log_file_list) > 0:
        log_file_list.sort()
    else:
        print("No multi-proc files to merge")
        return

    with open(parent_log_file, 'a') as main_log:
        # Iterate through list
        for temp_log_file in log_file_list:
            # Open each file in read mode
            with open(temp_log_file) as infile:
                main_log.write(infile.read())
            os.remove(temp_log_file)


def pre_clip_hucs_from_wbd(
    outputs_dir, huc_list, number_of_jobs, overwrite, preclipping_flags, copy_from_dir, cli_args=None
):
    '''
    The function is the main driver of the program to iterate and parallelize writing/copying
    pre-clipped HUC8 vector files.

    Inputs:
    - outputs_dir:                    Output directory to stage pre-clipped vectors.
    - huc_list:                       List of Hucs to generate pre-clipped .gpkg files.
    - number_of_jobs:                 Amount of cpus used for parallelization.
    - overwrite:                      Overwrite existing HUC directories containing stage vectors.
    - preclipping_flags               A dictionary indicating which vector data to preclip vs. copy
    - copy_from_dir                   directory with pre-clipped data to copy instead of re-clipping


    Processing:
    - Validate number_of_jobs based on cpus availabe on current system.
    - Set up logging.
    - Read in HUC list.
    - If HUC level output directory is existant, delete it, if not, create a new directory.
    - Parallelize the processing of .gpkg creation of the hucs_to_pre_clip_list using multiprocessing.Pool.
        (call to huc_level_clip_vectors_to_wbd)
    - For each HUC in the list, use preclipping_flags to either preclip new vector data or copy from existing data

    Outputs:
    - New directory for each HUC, which contains 10-12 .gpkg files
    - Write to log file in $pre_clip_huc_dir
    '''

    # Validation
    total_cpus_available = os.cpu_count() - 2
    if number_of_jobs > total_cpus_available:
        print(
            f'Provided: -j {number_of_jobs}, which is greater than than amount of available cpus -2: '
            f'{total_cpus_available - 2} will be used instead.'
        )
        number_of_jobs = total_cpus_available

    # Read in huc_list file and turn into a list data structure
    if os.path.exists(huc_list):
        hucs_to_pre_clip_list = open(huc_list).read().splitlines()
    else:
        logging.info("The huclist is not valid. Please check <huc_list> argument.")
        raise Exception("The huclist is not valid. Please check <huc_list> argument.")

    if os.path.exists(outputs_dir) and not overwrite:
        raise Exception(
            f"The directory: {outputs_dir} already exists. Use 'overwrite' argument if the intent"
            " is to re-generate all of the data. "
        )

    # Set up logging and set start_time
    __setup_logger(outputs_dir)
    start_time = dt.datetime.now(dt.timezone.utc)

    logging.info("==================================")
    if cli_args:
        logging.info(f"CLI invocation: {cli_args}")
    logging.info(f"Starting Pre-clip based on huc list of\n    {huc_list}")
    logging.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info("")

    # report which vector data are being copied instead of pre-clipping
    copied_layers = [layer for layer, preclip_status in preclipping_flags.items() if not preclip_status]
    clipped_layers = [layer for layer, preclip_status in preclipping_flags.items() if preclip_status]

    if copied_layers:
        logging.info("The following layers will be copied instead of clipping:")
        for layer in copied_layers:
            logging.info(f" - {layer}")

    else:
        logging.info(
            "All vector layers will be newly clipped/transferred — no copies from previously pre-clipped "
            "data were requested."
        )

    if clipped_layers:
        logging.info("The following layers will be newly clipped/transferred:")
        for layer in clipped_layers:
            logging.info(f" - {layer}")
    else:
        logging.info(
            "You have requested for all layers to be copied. No need to use this tool for a simple copying!"
        )
        sys.exit(0)

    hucs_to_pre_clip_list.sort()

    # Iterate over the huc_list argument and create a directory for each huc.
    for huc in hucs_to_pre_clip_list:
        if os.path.isdir(os.path.join(outputs_dir, huc)):
            shutil.rmtree(os.path.join(outputs_dir, huc))
            os.mkdir(os.path.join(outputs_dir, huc))
            logging.info(
                f"\n\t Output Directory: {outputs_dir}/{huc} exists.  It will be overwritten, and the "
                f"newly generated huc level files will be output there. \n"
            )

        elif not os.path.isdir(os.path.join(outputs_dir, huc)):
            os.mkdir(os.path.join(outputs_dir, huc))

    # Build arguments (procs_list) for each process to execute (huc_level_clip_vectors_to_wbd)
    procs_list = []
    for huc in hucs_to_pre_clip_list:
        procs_list.append([huc, outputs_dir, preclipping_flags, copy_from_dir])

    # Parallelize each huc in hucs_to_parquet_list
    logging.info('Parallelizing HUC level wbd pre-clip vector creation. ')

    # TODO: Mar 5, 2024: Python native logging does not work well with Multi-proc. We will
    # likely eventually drop in ras2fim's logging system.
    # The log files for each multi proc has tons and tons of duplicate lines crossing mp log
    # processes, but does always log correctly back to the parent log

    # TODO; Feb 2026: Bolt in alis MP and logging from shared_functions.

    failed_HUCs_list = []  # On return from the MP, if it returns a HUC number, that is a failed huc
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        futures = {}
        for huc in hucs_to_pre_clip_list:
            args = {
                "huc": huc,
                "outputs_dir": outputs_dir,
                "copy_from_dir": copy_from_dir,
                "preclipping_flags": preclipping_flags,
            }
            future = executor.submit(huc_level_clip_vectors_to_wbd, **args)
            futures[future] = future

        for future in as_completed(futures):
            if future is not None:
                if not future.exception():
                    failed_huc = future.result()
                    if failed_huc.lower() != "success" and failed_huc.isnumeric():
                        failed_HUCs_list.append(failed_huc)
                else:
                    raise future.exception()

    print("Merging MP log files")
    __merge_mp_logs(outputs_dir, LOG_FILE_PATH)

    if len(failed_HUCs_list) > 0:
        logging.info("\n+++++++++++++++++++")
        logging.info("HUCs that failed to process are: ")
        huc_error_msg = "  -- "
        for huc in failed_HUCs_list:
            huc_error_msg += f"{huc}, "
        logging.info(huc_error_msg)
        print("  See logs for more details on each HUC fail")
        print(
            "\n\nOften you can just create a new huc list with the fails, re-run to a"
            " temp directory and recheck if errors still exists. Sometimes multi-prod can create"
            " contention errors.\nFor each HUC that is successful, you can just copy it back"
            " into the original full pre-clip folder.\n"
        )

        logging.info("+++++++++++++++++++")

    # Get time metrics
    end_time = dt.datetime.now(dt.timezone.utc)
    end_time_string = end_time.strftime("%m/%d/%Y %H:%M:%S")
    logging.info(f"\n Ended: {end_time_string} \n")

    # Calculate duration
    time_duration = end_time - start_time
    logging.info('==========================================================================')
    logging.info(
        f"\t Completed writing all huc level files \n"
        f"\t \t TOTAL RUN TIME: {str(time_duration).split('.')[0]}"
    )
    logging.info('==========================================================================')


def huc_level_clip_vectors_to_wbd(huc, outputs_dir, copy_from_dir, preclipping_flags):
    '''
    TODO a limitation of this additional function is that we cannot use subset_vector_layers() by itself
    because some parts of the preclipped data are created in this function before calling subset_vector_layers().

    Create pre-clipped vectors at the huc level. Necessary to have this as an additional
    function for multiprocessing. This is mostly a wrapper for the subset_vector_layers() method in
    clip_vectors_to_wbd.py.

    Inputs:
    - huc:                           Individual HUC to generate vector files for.
    - outputs_dir:                   Output directory to stage pre-clipped vectors.
    - copy_from_dir (str): Directory containing pre-clipped vector data to copy from (if applicable).
    - preclipping_flags (dict): Dictionary with 8 boolean flags indicating which vector layers to preclip vs. copy.

    Processing:

    - Define LandSea water body mask.
    - Generates  wbd.gpkg and wbd_buffered.gpkg
    - Subset Vector Layers - (call subset_vector_layers function in data/wbd/clip_vectors_to_wbd.py file)
    - Clip WBD - TODO update this to use geopandas
        Creation of wbd8_clp.gpkg using the executable: "ogr2ogr .... -clipsrc ..."

    Outputs:
    - .If successful, then it returns the word sudcess. If it fails, return the huc number.
    '''

    # small random time stagger to help MP not all hitting the the WBD at the same.
    # random between 0 and 30 seconds
    time.sleep(random.randint(0, 40))

    in_error = False
    huc_processing_start = dt.datetime.now(dt.timezone.utc)

    # with this in Multi-proc, it needs it's own logger and unique logging file. rethink. see todo above.

    __setup_logger(outputs_dir, huc, True)
    logging.info(f"Start Processing {huc}")

    try:
        huc_directory = os.path.join(outputs_dir, huc)

        # SET VARIABLES AND FILE INPUTS #
        hucUnitLength = len(huc)
        huc2Identifier = huc[:2]
        input_NHD_WBHD_layer = f"WBDHU{hucUnitLength}"

        # Check whether the HUC is in Alaska or not and assign the CRS and filenames accordingly
        if huc2Identifier == '19':
            huc_CRS = ALASKA_CRS
            input_WBD_filename = input_WBD_gdb_Alaska
            dem_domain = input_DEM_domain_Alaska
        elif huc == '22010000':  # Guam
            huc_CRS = GUAM_CRS
            input_WBD_filename = input_WBD_gdb_Guam
            dem_domain = input_DEM_domain_Guam
        elif huc == '22030001':  # American Samoa
            huc_CRS = AMERICAN_SAMOA_CRS
            input_WBD_filename = input_WBD_gdb_AmericanSamoa
            dem_domain = input_DEM_domain_AmericanSamoa
        else:
            huc_CRS = DEFAULT_FIM_PROJECTION_CRS
            input_WBD_filename = input_WBD_gdb
            dem_domain = input_DEM_domain

        # Define the landsea waterbody mask using either Great Lakes or Ocean polygon input #
        if huc2Identifier == "04":
            input_LANDSEA = f"{input_GL_boundaries}"
        elif huc2Identifier == "19":
            input_LANDSEA = input_landsea_Alaska
        elif huc == '22010000':
            input_LANDSEA = input_landsea_Guam
        elif huc == '22030001':
            input_LANDSEA = input_landsea_AmericanSamoa
        else:
            input_LANDSEA = input_landsea

        logging.info(f"-- {huc} : Get WBD")

        wbd_filename = "wbd.gpkg"
        wbd_buffer_filename = "wbd_buffered.gpkg"

        if not preclipping_flags['boundaries']:
            output_filenames = {
                "wbd": "wbd.gpkg",
                "wbd_buffered": "wbd_buffered.gpkg",
                "wbd8_clp": "wbd8_clp.gpkg",
                "wbd_buffered_streams": "wbd_buffered_streams.gpkg",
                "LandSea_subset": "LandSea_subset.gpkg",
            }
            for vector_item in ['wbd', 'wbd_buffered', 'wbd8_clp', 'wbd_buffered_streams', 'LandSea_subset']:
                src = os.path.join(copy_from_dir, huc, output_filenames[vector_item])
                dst = os.path.join(huc_directory, output_filenames[vector_item])
                if os.path.exists(src):
                    logging.info(f"Copying {vector_item} for {huc} (from previous output).")
                    shutil.copy2(src, dst)
                else:
                    logging.warning(f"Missing file: {vector_item} for {huc} not found at {src}.")
        else:
            # Read the input layer from the input WBD file using sql to be more efficient
            sql = f"SELECT * FROM \"{input_NHD_WBHD_layer}\" WHERE HUC8 = '{huc}'"
            logging.info(f"Using WBD source for {huc}: {input_WBD_filename}")
            wbd = gpd.read_file(input_WBD_filename, sql=sql, columns=['HUC8', 'fimid', 'fossid', 'geometry'])
            wbd = wbd.to_crs(huc_CRS)

            # make sure the HUC boundary does not extend beyond DEM
            logging.info(f"Using DEM domain source for {huc}: {dem_domain}")
            dem_domain_gdf = gpd.read_file(dem_domain, engine="pyogrio", use_arrow=True)
            wbd = gpd.clip(wbd, dem_domain_gdf)

            logging.info(f"Create wbd buffer for {huc}")
            wbd_buffer = wbd.copy()
            wbd_buffer.geometry = wbd_buffer.geometry.buffer(wbd_buffer_distance, resolution=32)

            wbd_buffer = gpd.clip(wbd_buffer, dem_domain_gdf)

            # first Make the streams buffer smaller than the wbd_buffer so streams don't reach the edge of the DEM
            logging.info(f"Create stream buffer for {huc}")
            wbd_streams_buffer = wbd_buffer.copy()
            wbd_streams_buffer.geometry = wbd_streams_buffer.geometry.buffer(
                -8 * float(os.getenv('res')), resolution=32
            )

            wbd_streams_buffer = wbd_streams_buffer[['geometry']]
            wbd_streams_buffer.to_file(
                os.path.join(huc_directory, 'wbd_buffered_streams.gpkg'),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )

            # Clip landsea before saving wbd and wbd_buffer
            logging.info(f"Clip ocean water polygon for {huc}")
            logging.info(f"Using landsea source for {huc}: {input_LANDSEA}")
            landsea = gpd.read_file(input_LANDSEA, mask=wbd_buffer, engine="fiona")

            # some hucs do not have landsea
            if not landsea.empty:
                landsea.to_file(
                    os.path.join(huc_directory, "LandSea_subset.gpkg"),
                    driver='GPKG',
                    index=False,
                    crs=huc_CRS,
                    engine="fiona",
                )

                # Exclude landsea area from WBD and wbd_buffer
                wbd = wbd.overlay(landsea, how='difference')
                wbd_buffer = wbd_buffer.overlay(landsea, how='difference')

            del landsea

            wbd.to_file(
                os.path.join(huc_directory, wbd_filename),
                layer='WBDHU8',
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )

            wbd_buffer = wbd_buffer[['geometry']]

            wbd_buffer.to_file(
                os.path.join(huc_directory, wbd_buffer_filename),
                driver='GPKG',
                index=False,
                crs=huc_CRS,
                engine="fiona",
            )

            # Clip WBD8 ##
            logging.info(f"-- {huc} : Creating WBD buffer and clip version")

            clip_wbd8_subprocess = subprocess.run(
                [
                    'ogr2ogr',
                    '-f',
                    'GPKG',
                    '-t_srs',
                    huc_CRS,
                    '-clipsrc',
                    f'{huc_directory}/wbd_buffered.gpkg',
                    '-nlt',
                    'PROMOTE_TO_MULTI',
                    f'{huc_directory}/wbd8_clp.gpkg',
                    input_WBD_filename,
                    input_NHD_WBHD_layer,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                universal_newlines=True,
            )

            sub_proc_stdout = clip_wbd8_subprocess.stdout
            sub_proc_stderror = clip_wbd8_subprocess.stderr
            if sub_proc_stderror.lower().startswith("warning") is True:
                logging.info(f"Warning from ogr clip: {sub_proc_stderror}")
            elif sub_proc_stderror != "":
                raise Exception(
                    f"*** {huc} : Error WBD buffer and clip version from ogr for {huc}:"
                    f" Details: {sub_proc_stderror}\n"
                )
            else:
                msg = f"-- {huc} : Creating WBD buffer and clip version - Complete"
                if sub_proc_stdout != "":  # warnings?
                    msg += f": {sub_proc_stdout}"

                logging.info(msg)

        subset_vector_layers(
            huc=huc,
            wbd_filename=wbd_filename,
            wbd_buffer_filename=wbd_buffer_filename,
            huc_directory=huc_directory,
            copy_from_dir=copy_from_dir,
            preclipping_flags=preclipping_flags,
        )

        logging.info(f"-- {huc} : Completing Get Vector Layers and Subset:\n")

    except Exception:
        logging.info(f"*** An error occurred while processing {huc}")
        logging.info(traceback.format_exc())
        logging.info("")
        in_error = True

    huc_processing_end = dt.datetime.now(dt.timezone.utc)
    time_duration = huc_processing_end - huc_processing_start
    duraton_msg = f"-- {huc} : run time is {str(time_duration).split('.')[0]}\n"
    logging.info(duraton_msg)

    # if in error return the failed huc number
    if in_error is True:
        return huc
    else:
        return "Success"


if __name__ == '__main__':

    # NOTE: Super important: Make sure the bash_variables are correct before doing pre-clip.
    # It pulls a wide number of values from there.
    # Especially if you can a directory for a new data load.
    #     ie) DEMS at data/inputs/3dep_dems/10m_5070/20240916/
    #

    # This tool can be run just one for all 4 region types (CONUS, AK, GU and AS)
    # as it relys on the HUC list submitted and likely will submit full_huc_list.lst

    '''
    Notes:
    Five boundary coverage files are managed using one args of -> --boundaries.
        - wbd.gpkg
        - wbd_buffered.gpkg
        - wbd8_clp.gpkg
        - LandSea_subset.gpkg
        - wbd_buffered_streams.gpkg

    Two stream-related files below are managed using one args of -> --nwm_streams_headwater
        - nwm_subset_streams.gpkg
        - nwm_headwater_points_subset.gpkg

    The following files are managed with 6 args for pre-clipping/transferring vs copying.
    If layer args are omitted, copy from previous preclips is used by default.
            - "nwm_lakes_proj_subset.gpkg"        ->  --nwm_lakes
            - "nwm_catchments_proj_subset.gpkg"   ->  --nwm_catchments
            - levee files (3 files)               ->  --levees
            - "osm_bridges_subset.gpkg"           ->  --osm_bridges
                Transfers recently pulled, already HUC-level bridge files from osm_bridges_modified_dir.
            - "osm_roads_subset.gpkg"             ->  --osm_roads
            - "buildings_subset.gpkg"             ->  --buildings


    Example 1:
        where we preclip/transfer all vector data for requested HUCs (no copying from previous preclips):

        python foss_fim/data/wbd/generate_pre_clip_fim_huc8.py \
          -u /data/inputs/huc_lists/full_huc_list.lst \
          -n /data/data/inputs/pre_clip_huc8/20250923/ \
          --boundaries --nwm_lakes --nwm_streams_headwater \
          --nwm_catchments --levees --osm_bridges \
          --osm_roads --buildings

    Example 2:
        if you want to preclip/transfer only a subset and copy the rest from previous preclips:
        Always add --copy_from_dir followed by path to the previous preclips results.
        Then provide one or multiple preclip args.

        python foss_fim/data/wbd/generate_pre_clip_fim_huc8.py \
            -u /data/inputs/huc_lists/full_huc_lists.lst \
            -n outputs/preclips/test6_2/ \
            -o \
            --copy_from_dir data/inputs/pre_clip_huc8/20250218/ \
            --osm_roads \
            --buildings    '''

    parser = argparse.ArgumentParser(
        description='This script gets WBD layer, calls the clip_vectors_to_wbd.py script, and clips the wbd. '
        'A plethora gpkg files per huc are generated (see args to subset_vector_layers), and placed within '
        'the output directory specified as the <outputs_dir> argument.',
        usage='''
            ./generate_pre_clip_fim_huc8.py
                -n /data/inputs/pre_clip_huc8/20240927
                -u /data/inputs/huc_lists/full_huc_list.lst
                -j 6
                -o
                --copy_from_dir data/inputs/pre_clip_huc8/20250218/
                --nwm_catchments --levees
        ''',
    )

    parser.add_argument('-n', '--outputs_dir', help='Directory to output all of the HUC level .gpkg files')
    parser.add_argument('-u', '--huc_list', help='List of HUCs to genereate pre-clipped vectors for.')

    parser.add_argument(
        '-j',
        '--number_of_jobs',
        help='OPTIONAL: Number of cores/processes (default=4). This is a memory intensive '
        'script, and the multiprocessing will crash if too many CPUs are used. It is recommended to provide '
        'half the amount of available CPUs. The network speed of your EC2 is the primary factor.',
        type=int,
        required=False,
        default=4,
    )
    parser.add_argument(
        '-o',
        '--overwrite',
        help='OPTIONAL: Overwrite the file if already existing? (default false)',
        action='store_true',
    )

    parser.add_argument(
        "--copy_from_dir", help="Directory to copy files from (enables selective copying)", default=None
    )

    # For any provided layer argument, that layer is newly clipped/transferred. Missing args default to copy.
    for arg_option in args_preclip_options:
        arg_help = f"preclip {arg_option} instead of copying"
        if arg_option == 'osm_bridges':
            arg_help = "transfer recently pulled osm_bridges instead of copying from previous preclips"
        parser.add_argument(
            f"--{arg_option}", action="store_true", help=arg_help
        )

    args = vars(parser.parse_args())

    if not os.path.exists(args['huc_list']):
        print(f"Error: HUC list file {args['huc_list']} does not exist.")
        exit(0)

    # Build internal preclip flags (True means preclip/transfer, False means copy).
    # New CLI semantics: layer args opt-in to preclipping/transferring; missing args default to copy.
    preclipping_flags = {}
    for arg_option in args_preclip_options:
        preclipping_flags[arg_option] = args.get(arg_option, False)

    # If nothing is selected for preclipping/transferring, this becomes a full-copy operation.
    # This tool is intended for preclipping workflows, not copy-only runs.
    if not any(preclipping_flags.values()):
        print(
            "No preclip layer args were provided. This would copy all vectors, which does not make sense for this tool."
        )
        exit(0)

    # copy_from_dir is required whenever at least one layer will be copied.
    if not all(preclipping_flags.values()) and not args['copy_from_dir']:
        print(
            "Error: one or more layer args were not provided, so those layers would be copied by default.\n"
            "Provide --copy_from_dir for those copied layers, or provide all 8 layer args to preclip/transfer everything."
        )
        exit(0)

    try:
        pre_clip_hucs_from_wbd(
            outputs_dir=args["outputs_dir"],
            huc_list=args["huc_list"],
            number_of_jobs=args["number_of_jobs"],
            overwrite=args["overwrite"],
            preclipping_flags=preclipping_flags,
            copy_from_dir=args["copy_from_dir"],
            cli_args=shlex.join(sys.argv),
        )
    except Exception:
        logging.info(traceback.format_exc())
        end_time = dt.datetime.now(dt.timezone.utc)
        logging.info(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
