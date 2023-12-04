#!/usr/bin/env python3

import argparse
import datetime as dt
import logging
import os
import shutil
import subprocess
from multiprocessing import Pool

from clip_vectors_to_wbd import subset_vector_layers
from dotenv import load_dotenv


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
            -wbd /data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg
            -n /data/inputs/pre_clip_huc8/24_3_20
            -u /data/inputs/huc_lists/included_huc8.lst
            -j 6
            -o

    Notes:
      If running this script to generate new data, modify the pre_clip_huc_dir variable in
      src/bash_variables.env to the corresponding outputs_dir argument after running and testing this script.
      The newly generated data should be created in a new folder using the format <year_month_day>
             (i.e. September 26, 2023 would be 23_9_26)
'''

srcDir = os.getenv('srcDir')
projectDir = os.getenv('projectDir')

load_dotenv(f'{srcDir}/bash_variables.env')
load_dotenv(f'{projectDir}/config/params_template.env')

# Variables from src/bash_variables.env
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
inputsDir = os.getenv('inputsDir')
input_WBD_gdb = os.getenv('input_WBD_gdb')
input_DEM = os.getenv('input_DEM')
input_DEM_domain = os.getenv('input_DEM_domain')
input_nwm_lakes = os.getenv('input_nwm_lakes')
input_nwm_catchments = os.getenv('input_nwm_catchments')
input_NLD = os.getenv('input_NLD')
input_levees_preprocessed = os.getenv('input_levees_preprocessed')
input_GL_boundaries = os.getenv('input_GL_boundaries')
input_nwm_flows = os.getenv('input_nwm_flows')
input_nwm_headwaters = os.getenv('input_nwm_headwaters')
input_nld_levee_protected_areas = os.getenv('input_nld_levee_protected_areas')

# Variables from config/params_template.env
wbd_buffer = os.getenv('wbd_buffer')
wbd_buffer_int = int(wbd_buffer)


def __setup_logger(outputs_dir):
    '''
    Set up logging to file. Since log file includes the date, it will be overwritten if this
    script is run more than once on the same day.
    '''
    datetime_now = dt.datetime.now(dt.timezone.utc)
    curr_date = datetime_now.strftime("%m_%d_%Y")

    log_file_name = f"generate_pre_clip_fim_huc8_{curr_date}.log"

    log_file_path = os.path.join(outputs_dir, log_file_name)

    if not os.path.exists(outputs_dir):
        os.mkdir(outputs_dir)

    if os.path.exists(log_file_path):
        os.remove(log_file_path)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    # Print start time
    start_time_string = datetime_now.strftime("%m/%d/%Y %H:%M:%S")
    logging.info('==========================================================================')
    logging.info("\n generate_pre_clip_fim_huc8.py")
    logging.info(f"\n \t Started: {start_time_string} \n")


def pre_clip_hucs_from_wbd(wbd_file, outputs_dir, huc_list, number_of_jobs, overwrite):
    '''
    The function is the main driver of the program to iterate and parallelize writing
    pre-clipped HUC8 vector files.

    Inputs:
    - wbd_file:                       Default take from src/bash_variables.env, or provided as argument.
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
    total_cpus_available = os.cpu_count()
    if number_of_jobs > total_cpus_available:
        print(
            f'Provided: -j {number_of_jobs}, which is greater than than amount of available cpus -2: '
            f'{total_cpus_available - 2} will be used instead.'
        )
        number_of_jobs = total_cpus_available - 2

    # Set up logging and set start_time
    __setup_logger(outputs_dir)
    start_time = dt.datetime.now(dt.timezone.utc)

    # Read in huc_list file and turn into a list data structure
    if os.path.exists(huc_list):
        hucs_to_pre_clip_list = open(huc_list).read().splitlines()
    else:
        logging.info("The huclist is not valid. Please check <huc_list> arguemnt.")
        raise Exception("The huclist is not valid. Please check <huc_list> arguemnt.")

    if os.path.exists(outputs_dir) and not overwrite:
        raise Exception(
            f"The directory: {outputs_dir} already exists. Use 'overwrite' argument if the intent"
            " is to re-generate all of the data. "
        )

    # Iterate over the huc_list argument and create a directory for each huc.
    for huc in hucs_to_pre_clip_list:
        if os.path.isdir(os.path.join(outputs_dir, huc)):
            shutil.rmtree(os.path.join(outputs_dir, huc))
            os.mkdir(os.path.join(outputs_dir, huc))
            logging.info(
                f"\n\t Output Directory: {outputs_dir}/{huc} exists.  It will be overwritten, and the "
                f"newly generated huc level files will be output there. \n"
            )
            print(
                f"\n\t Output Directory: {outputs_dir}/{huc} exists.  It will be overwritten, and the "
                f"newly generated huc level files will be output there. \n"
            )

        elif not os.path.isdir(os.path.join(outputs_dir, huc)):
            os.mkdir(os.path.join(outputs_dir, huc))
            logging.info(f"Created directory: {outputs_dir}/{huc}, huc level files will be written there.")
            print(f"Created directory: {outputs_dir}/{huc}, huc level files will be written there.")

    # Build arguments (procs_list) for each process to execute (huc_level_clip_vectors_to_wbd)
    procs_list = []
    for huc in hucs_to_pre_clip_list:
        print(f"Generating vectors for {huc}. ")
        procs_list.append([huc, outputs_dir, wbd_file])

    # Parallelize each huc in hucs_to_parquet_list
    logging.info('Parallelizing HUC level wbd pre-clip vector creation. ')
    print('Parallelizing HUC level wbd pre-clip vector creation. ')
    with Pool(processes=number_of_jobs) as pool:
        pool.map(huc_level_clip_vectors_to_wbd, procs_list)

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

    print("\n\t Completed writing all huc level files \n")
    print(f"\t \t TOTAL RUN TIME: {str(time_duration).split('.')[0]}")


def huc_level_clip_vectors_to_wbd(args):
    '''
    Create pre-clipped vectors at the huc level. Necessary to have this as an additional
    function for multiprocessing. This is mostly a wrapper for the subset_vector_layers() method in
    clip_vectors_to_wbd.py.

    Inputs:
    - huc:                           Individual HUC to generate vector files for.
    - outputs_dir:                   Output directory to stage pre-clipped vectors.
    - input_WBD_filename:            Filename of WBD to generate pre-clipped .gpkg files.

    Processing:
    - Define (unpack) arguments.
    - Define LandSea water body mask.
    - Use subprocess.run to get the WBD for specified HUC - (ogr2ogr called to generate wbd.gpkg)
    - Subset Vector Layers - (call subset_vector_layers function in data/wbd/clip_vectors_to_wbd.py file)
    - Clip WBD -
        Creation of wbd8_clp.gpkg & wbd_buffered.gpkg using the executable: "ogr2ogr .... -clipsrc ..."

    Outputs:
    - .gpkg files* dependant on HUC's WBD (*differing amount based on individual huc)
    '''

    # We have to explicitly unpack the args from pool.map()
    huc = args[0]
    outputs_dir = args[1]
    input_WBD_filename = args[2]

    huc_directory = os.path.join(outputs_dir, huc)

    # SET VARIABLES AND FILE INPUTS #
    hucUnitLength = len(huc)
    huc2Identifier = huc[:2]
    input_NHD_WBHD_layer = f"WBDHU{hucUnitLength}"

    # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
    if huc2Identifier == "04":
        input_LANDSEA = f"{input_GL_boundaries}"
        print(f'Using {input_LANDSEA} for water body mask (Great Lakes)')
    else:
        input_LANDSEA = f"{inputsDir}/landsea/water_polygons_us.gpkg"

    print(f"\n Get WBD {huc}")

    # TODO: Use Python API (osgeo.ogr) instead of using ogr2ogr executable
    get_wbd_subprocess = subprocess.run(
        [
            'ogr2ogr',
            '-f',
            'GPKG',
            '-t_srs',
            DEFAULT_FIM_PROJECTION_CRS,
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

    msg = get_wbd_subprocess.stdout
    print(msg)
    logging.info(msg)

    if get_wbd_subprocess.stderr != "":
        if "ERROR" in get_wbd_subprocess.stderr.upper():
            msg = (
                f" - Creating -- {huc_directory}/wbd.gpkg"
                f"  ERROR -- details: ({get_wbd_subprocess.stderr})"
            )
            print(msg)
            logging.error(msg)
    else:
        msg = f" - Creating -- {huc_directory}/wbd.gpkg - Complete \n"
        print(msg)
        logging.info(msg)

    msg = f"Get Vector Layers and Subset {huc}"
    print(msg)
    logging.info(msg)

    # Subset Vector Layers
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
    )

    msg = f"\n\t Completing Get Vector Layers and Subset: {huc} \n"
    print(msg)
    logging.info(msg)

    ## Clip WBD8 ##
    print(f" Clip WBD {huc}")

    clip_wbd8_subprocess = subprocess.run(
        [
            'ogr2ogr',
            '-f',
            'GPKG',
            '-t_srs',
            DEFAULT_FIM_PROJECTION_CRS,
            '-clipsrc',
            f'{huc_directory}/wbd_buffered.gpkg',
            f'{huc_directory}/wbd8_clp.gpkg',
            f'{inputsDir}/wbd/WBD_National.gpkg',
            'WBDHU8',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        universal_newlines=True,
    )

    msg = clip_wbd8_subprocess.stdout
    print(msg)
    logging.info(msg)

    if clip_wbd8_subprocess.stderr != "":
        if "ERROR" in clip_wbd8_subprocess.stderr.upper():
            msg = (
                f" - Creating -- {huc_directory}/wbd.gpkg"
                f"  ERROR -- details: ({clip_wbd8_subprocess.stderr})"
            )
            print(msg)
            logging.error(msg)
    else:
        msg = f" - Creating -- {huc_directory}/wbd.gpkg - Complete"
        print(msg)
        logging.info(msg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This script gets WBD layer, calls the clip_vectors_to_wbd.py script, and clips the wbd. '
        'A plethora gpkg files per huc are generated (see args to subset_vector_layers), and placed within '
        'the output directory specified as the <outputs_dir> argument.',
        usage='''
            ./generate_pre_clip_fim_huc8.py
                -wbd /data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg
                -n /data/inputs/pre_clip_huc8/24_3_20
                -u /data/inputs/huc_lists/included_huc8.lst
                -j 6
                -o
        ''',
    )
    parser.add_argument(
        '-wbd',
        '--wbd_file',
        help='.wbd file to clip into individual HUC.gpkg files. Default is $input_WBD_gdb from src/bash_variables.env.',
        default=input_WBD_gdb,
    )
    parser.add_argument(
        '-n',
        '--outputs_dir',
        help='Directory to output all of the HUC level .gpkg files. Use the format: '
        '<year_month_day> (i.e. September 26, 2023 would be 23_9_26)',
    )
    parser.add_argument('-u', '--huc_list', help='List of HUCs to genereate pre-clipped vectors for.')
    parser.add_argument(
        '-j',
        '--number_of_jobs',
        help='OPTIONAL: number of cores/processes (default=4). This is a memory intensive '
        'script, and the multiprocessing will crash if too many CPUs are used. It is recommended to provide '
        'half the amount of available CPUs.',
        type=int,
        required=False,
        default=4,
    )
    parser.add_argument(
        '-o',
        '--overwrite',
        help='Overwrite the file if already existing? (default false)',
        action='store_true',
    )

    args = vars(parser.parse_args())

    pre_clip_hucs_from_wbd(**args)
