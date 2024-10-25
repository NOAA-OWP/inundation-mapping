#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import shutil
import subprocess
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from clip_vectors_to_wbd import subset_vector_layers
from dotenv import load_dotenv

from utils.shared_functions import FIM_Helpers as fh


'''
    Overview:
      This script was created to absolve run_unit_wb.sh from getting the huc level WBD layer, calling
      clip_vectors_to_wbd.py, and clipping the WBD for every run, which added a significant amount of
      processing time for each HUC8. Using this script, we generate the necessary pre-clipped .gpkg files
      for the rest of the processing steps.

      Read in environment variables from src/bash_variabls.env & config/params_template.env.
      Parallelize the creation of .gpkg files per HUC:
        Get huc level WBD layer from National, call the subset_vector_layers function, and clip the wbd.
        A plethora gpkg files per huc are generated (see args to subset_vector_layers)
        and placed within the output directory specified as the <outputs_dir> argument.

    Usage:
        generate_pre_clip_fim_huc8.py
            -n /data/inputs/pre_clip_huc8/24_04_23
            -u /data/inputs/huc_lists/included_huc8_withAlaska.lst
            -j 6
            -o

    Notes:
      If running this script to generate new data, modify the pre_clip_huc_dir variable in
      src/bash_variables.env to the corresponding outputs_dir argument after running and testing this script.
      The newly generated data should be created in a new folder using the format <year_month_day>
             (i.e. September 26, 2023 would be 23_09_26)

    Don't worry about error logs saying "Failed to auto identify EPSG: 7"
'''

srcDir = os.getenv('srcDir')
projectDir = os.getenv('projectDir')

load_dotenv(f'{srcDir}/bash_variables.env')
load_dotenv(f'{projectDir}/config/params_template.env')

# Variables from src/bash_variables.env
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')  # alaska

inputsDir = os.getenv('inputsDir')

input_WBD_gdb = os.getenv('input_WBD_gdb')
input_WBD_gdb_Alaska = os.getenv('input_WBD_gdb_Alaska')  # alaska

input_DEM = os.getenv('input_DEM')
input_DEM_Alaska = os.getenv('input_DEM_Alaska')  # alaska

input_DEM_domain = os.getenv('input_DEM_domain')
input_DEM_domain_Alaska = os.getenv('input_DEM_domain_Alaska')  # alaska

input_nwm_lakes = os.getenv('input_nwm_lakes')
input_nwm_catchments = os.getenv('input_nwm_catchments')
input_nwm_catchments_Alaska = os.getenv('input_nwm_catchments_Alaska')

input_NLD = os.getenv('input_NLD')
input_NLD_Alaska = os.getenv('input_NLD_Alaska')

input_levees_preprocessed = os.getenv('input_levees_preprocessed')
input_levees_preprocessed_Alaska = os.getenv('input_levees_preprocessed_Alaska')

input_GL_boundaries = os.getenv('input_GL_boundaries')
input_nwm_flows = os.getenv('input_nwm_flows')
input_nwm_flows_Alaska = os.getenv('input_nwm_flows_Alaska')  # alaska
input_nwm_headwaters = os.getenv('input_nwm_headwaters')
input_nwm_headwaters_Alaska = os.getenv('input_nwm_headwaters_Alaska')

input_nld_levee_protected_areas = os.getenv('input_nld_levee_protected_areas')
input_nld_levee_protected_areas_Alaska = os.getenv('input_nld_levee_protected_areas_Alaska')

input_osm_bridges = os.getenv('osm_bridges')

# Variables from config/params_template.env
wbd_buffer = os.getenv('wbd_buffer')
wbd_buffer_int = int(wbd_buffer)

LOG_FILE_PATH = ""


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
        os.mkdir(outputs_dir)

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


def pre_clip_hucs_from_wbd(outputs_dir, huc_list, number_of_jobs, overwrite):
    '''
    The function is the main driver of the program to iterate and parallelize writing
    pre-clipped HUC8 vector files.

    Inputs:
    - outputs_dir:                    Output directory to stage pre-clipped vectors.
    - huc_list:                       List of Hucs to generate pre-clipped .gpkg files.
    - number_of_jobs:                 Amount of cpus used for parallelization.
    - overwrite:                      Overwrite existing HUC directories containing stage vectors.


    Processing:
    - Validate number_of_jobs based on cpus availabe on current system.
    - Set up logging.
    - Read in HUC list.
    - If HUC level output directory is existant, delete it, if not, create a new directory.
    - Parallelize the processing of .gpkg creation of the hucs_to_pre_clip_list using multiprocessing.Pool.
        (call to huc_level_clip_vectors_to_wbd)

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
    logging.info(f"Starting Pre-clip based on huc list of\n    {huc_list}")
    logging.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info("")

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
        # logging.info(f"Generating vectors for {huc}. ")
        procs_list.append([huc, outputs_dir])

        # procs_list.append([huc, outputs_dir, wbd_alaska_file])

    # Parallelize each huc in hucs_to_parquet_list
    logging.info('Parallelizing HUC level wbd pre-clip vector creation. ')
    # with Pool(processes=number_of_jobs) as pool:
    #    pool.map(huc_level_clip_vectors_to_wbd, procs_list)

    # TODO: Mar 5, 2024: Python native logging does not work well with Multi-proc. We will
    # likely eventually drop in ras2fim's logging system.
    # The log files for each multi proc has tons and tons of duplicate lines crossing mp log
    # processes, but does always log correctly back to the parent log

    failed_HUCs_list = []  # On return from the MP, if it returns a HUC number, that is a failed huc
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        futures = {}
        for huc in hucs_to_pre_clip_list:
            args = {"huc": huc, "outputs_dir": outputs_dir}
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
        logging.info("HUCs that failed to proccess are: ")
        huc_error_msg = "  -- "
        for huc in failed_HUCs_list:
            huc_error_msg += f"{huc}, "
        logging.info(huc_error_msg)
        print("  See logs for more details on each HUC fail")
        print(
            "\n\nOften you can just create a new huc list with the fails, re-run to a"
            " temp directory and recheck if errors still exists. Sometimes multi-prod can create"
            " contention errors.\nFor each HUC that is sucessful, you can just copy it back"
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


def huc_level_clip_vectors_to_wbd(huc, outputs_dir):
    '''
    Create pre-clipped vectors at the huc level. Necessary to have this as an additional
    function for multiprocessing. This is mostly a wrapper for the subset_vector_layers() method in
    clip_vectors_to_wbd.py.

    Inputs:
    - huc:                           Individual HUC to generate vector files for.
    - outputs_dir:                   Output directory to stage pre-clipped vectors.

    Processing:
    - Define (unpack) arguments.
    - Define LandSea water body mask.
    - Use subprocess.run to get the WBD for specified HUC - (ogr2ogr called to generate wbd.gpkg)
    - Subset Vector Layers - (call subset_vector_layers function in data/wbd/clip_vectors_to_wbd.py file)
    - Clip WBD -
        Creation of wbd8_clp.gpkg & wbd_buffered.gpkg using the executable: "ogr2ogr .... -clipsrc ..."

    Outputs:
    - .If successful, then it returns the word sudcess. If it fails, return the huc number.
    '''

    in_error = False
    huc_processing_start = dt.datetime.now(dt.timezone.utc)
    # with this in Multi-proc, it needs it's own logger and unique logging file.
    __setup_logger(outputs_dir, huc, True)
    logging.info(f"Start Processing {huc}")

    try:

        huc_directory = os.path.join(outputs_dir, huc)

        # SET VARIABLES AND FILE INPUTS #
        hucUnitLength = len(huc)
        huc2Identifier = huc[:2]

        # Check whether the HUC is in Alaska or not and assign the CRS and filenames accordingly
        if huc2Identifier == '19':
            huc_CRS = ALASKA_CRS
            input_NHD_WBHD_layer = 'WBD_National_South_Alaska'
            input_WBD_filename = input_WBD_gdb_Alaska
            wbd_gpkg_path = f'{inputsDir}/wbd/WBD_National_South_Alaska.gpkg'
        else:
            huc_CRS = DEFAULT_FIM_PROJECTION_CRS
            input_NHD_WBHD_layer = f"WBDHU{hucUnitLength}"
            input_WBD_filename = input_WBD_gdb
            wbd_gpkg_path = f'{inputsDir}/wbd/WBD_National.gpkg'

        # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
        if huc2Identifier == "04":
            input_LANDSEA = f"{input_GL_boundaries}"
            # print(f'Using {input_LANDSEA} for water body mask (Great Lakes)')
        elif huc2Identifier == "19":
            input_LANDSEA = f"{inputsDir}/landsea/water_polygons_alaska.gpkg"
            # print(f'Using {input_LANDSEA} for water body mask (Alaska)')
        else:
            input_LANDSEA = f"{inputsDir}/landsea/water_polygons_us.gpkg"

        logging.info(f"-- {huc} : Get WBD")

        # TODO: Use Python API (osgeo.ogr) instead of using ogr2ogr executable
        get_wbd_subprocess = subprocess.run(
            [
                'ogr2ogr',
                '-f',
                'GPKG',
                '-t_srs',
                huc_CRS,
                f'{huc_directory}/wbd.gpkg',
                input_WBD_filename,
                input_NHD_WBHD_layer,
                '-where',
                f"HUC{hucUnitLength}='{huc}'",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True,
        )

        sub_proc_stdout = get_wbd_subprocess.stdout
        sub_proc_stderror = get_wbd_subprocess.stderr
        if sub_proc_stderror.lower().startswith("warning") is True:
            logging.info(f"Warning from ogr creating wbd file: {sub_proc_stderror}")
        elif sub_proc_stderror != "":
            raise Exception(
                f"*** {huc} : Error creating wbd file from ogr for {huc}:" f" Details: {sub_proc_stderror}\n"
            )
        else:
            msg = f"-- {huc} : Creating {huc_directory}/wbd.gpkg - Complete"
            if sub_proc_stdout != "":  # warnings?
                msg += f": {sub_proc_stdout}"

            logging.info(msg)

        logging.info(f"-- {huc} : Get Vector Layers and Subset")

        # Subset Vector Layers (after determining whether it's alaska or not)
        if huc2Identifier == '19':
            # Yes Alaska
            subset_vector_layers(
                subset_nwm_lakes=f"{huc_directory}/nwm_lakes_proj_subset.gpkg",
                subset_nwm_streams=f"{huc_directory}/nwm_subset_streams.gpkg",
                hucCode=huc,
                subset_nwm_headwaters=f"{huc_directory}/nwm_headwater_points_subset.gpkg",
                wbd_buffer_filename=f"{huc_directory}/wbd_buffered.gpkg",
                wbd_streams_buffer_filename=f"{huc_directory}/wbd_buffered_streams.gpkg",
                wbd_filename=f"{huc_directory}/wbd.gpkg",
                dem_filename=input_DEM_Alaska,
                dem_domain=input_DEM_domain_Alaska,
                nwm_lakes=input_nwm_lakes,
                nwm_catchments=input_nwm_catchments_Alaska,
                subset_nwm_catchments=f"{huc_directory}/nwm_catchments_proj_subset.gpkg",
                nld_lines=input_NLD_Alaska,
                nld_lines_preprocessed=input_levees_preprocessed_Alaska,
                landsea=input_LANDSEA,
                nwm_streams=input_nwm_flows_Alaska,
                subset_landsea=f"{huc_directory}/LandSea_subset.gpkg",
                nwm_headwaters=input_nwm_headwaters_Alaska,
                subset_nld_lines=f"{huc_directory}/nld_subset_levees.gpkg",
                subset_nld_lines_preprocessed=f"{huc_directory}/3d_nld_subset_levees_burned.gpkg",
                wbd_buffer_distance=wbd_buffer_int,
                levee_protected_areas=input_nld_levee_protected_areas_Alaska,
                subset_levee_protected_areas=f"{huc_directory}/LeveeProtectedAreas_subset.gpkg",
                osm_bridges=input_osm_bridges,
                subset_osm_bridges=f"{huc_directory}/osm_bridges_subset.gpkg",
                is_alaska=True,
                huc_CRS=huc_CRS,  # TODO: simplify
            )

        else:
            # Not Alaska
            subset_vector_layers(
                subset_nwm_lakes=f"{huc_directory}/nwm_lakes_proj_subset.gpkg",
                subset_nwm_streams=f"{huc_directory}/nwm_subset_streams.gpkg",
                hucCode=huc,
                subset_nwm_headwaters=f"{huc_directory}/nwm_headwater_points_subset.gpkg",
                wbd_buffer_filename=f"{huc_directory}/wbd_buffered.gpkg",
                wbd_streams_buffer_filename=f"{huc_directory}/wbd_buffered_streams.gpkg",
                wbd_filename=f"{huc_directory}/wbd.gpkg",
                dem_filename=input_DEM,
                dem_domain=input_DEM_domain,
                nwm_lakes=input_nwm_lakes,
                nwm_catchments=input_nwm_catchments,
                subset_nwm_catchments=f"{huc_directory}/nwm_catchments_proj_subset.gpkg",
                nld_lines=input_NLD,
                nld_lines_preprocessed=input_levees_preprocessed,
                landsea=input_LANDSEA,
                nwm_streams=input_nwm_flows,
                subset_landsea=f"{huc_directory}/LandSea_subset.gpkg",
                nwm_headwaters=input_nwm_headwaters,
                subset_nld_lines=f"{huc_directory}/nld_subset_levees.gpkg",
                subset_nld_lines_preprocessed=f"{huc_directory}/3d_nld_subset_levees_burned.gpkg",
                wbd_buffer_distance=wbd_buffer_int,
                levee_protected_areas=input_nld_levee_protected_areas,
                subset_levee_protected_areas=f"{huc_directory}/LeveeProtectedAreas_subset.gpkg",
                osm_bridges=input_osm_bridges,
                subset_osm_bridges=f"{huc_directory}/osm_bridges_subset.gpkg",
                is_alaska=False,
                huc_CRS=huc_CRS,  # TODO: simplify
            )

        logging.info(f"-- {huc} : Completing Get Vector Layers and Subset:\n")

        ## Clip WBD8 ##
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
                f'{huc_directory}/wbd8_clp.gpkg',
                wbd_gpkg_path,
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
    # Especially if you can a diretory for a new data load.
    #     ie) DEMS at data/inputs/3dep_dems/10m_5070/20240916/
    #
    # You have to run this twice, once for Alaska and once for CONUS
    # but make sure put both results the same folder
    # and you will need to submit the two HUC lists
    # SouthernAlaska_HUC8.lst
    # included_huc8.lst

    parser = argparse.ArgumentParser(
        description='This script gets WBD layer, calls the clip_vectors_to_wbd.py script, and clips the wbd. '
        'A plethora gpkg files per huc are generated (see args to subset_vector_layers), and placed within '
        'the output directory specified as the <outputs_dir> argument.',
        usage='''
            ./generate_pre_clip_fim_huc8.py
                -n /data/inputs/pre_clip_huc8/20240927
                -u /data/inputs/huc_lists/included_huc8_withAlaska.lst
                -j 6
                -o
        ''',
    )

    parser.add_argument(
        '-n',
        '--outputs_dir',
        help='Directory to output all of the HUC level .gpkg files. Use the format: '
        '<year_month_day> (i.e. September 26, 2024 would be 20240926)',
    )
    parser.add_argument('-u', '--huc_list', help='List of HUCs to genereate pre-clipped vectors for.')

    parser.add_argument(
        '-j',
        '--number_of_jobs',
        help='OPTIONAL: Number of cores/processes (default=4). This is a memory intensive '
        'script, and the multiprocessing will crash if too many CPUs are used. It is recommended to provide '
        'half the amount of available CPUs.',
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

    args = vars(parser.parse_args())

    try:
        pre_clip_hucs_from_wbd(**args)
    except Exception:
        logging.info(traceback.format_exc())
        end_time = dt.datetime.now(dt.timezone.utc)
        logging.info(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
