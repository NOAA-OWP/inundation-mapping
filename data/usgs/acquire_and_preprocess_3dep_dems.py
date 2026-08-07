#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import subprocess
import sys
import traceback

# from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd

import src.utils.shared_functions as sf
import src.utils.shared_validators as val
from data.create_vrt_file import create_vrt_file
from src.utils.polygonize_raster import polygonize_raster
from src.utils.shared_functions import FIM_Helpers as fh


gpd.options.io_engine = "pyogrio"


'''
TODO:
    - Add input args for resolution size, which means URL and block size also have to be parameterized.

    - Add an arg to skip straight to polygonize as we had a conus run that failed after dems but in polygonize.

    - the -lf system needs testing.

    - Add MP or MT to polygonize

'''

# local constants (until changed to input param)
# This URL is part of a series of vrt data available from USGS via an S3 Bucket.
# for more info see: "http://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/".
# The odd folder numbering is a translation of arc seconds with 13m  being 1/3 arc second or 10 meters.
# 10m = 13 (1/3 arc second)
__USGS_3DEP_10M_VRT_URL = (
    r'/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'
)


def acquire_and_preprocess_3dep_dems(
    extent_file_path,
    target_output_folder_path='',
    number_of_jobs=1,
    repair=False,
    skip_polygons=False,
    target_projection='EPSG:5070',
    lst_file_names='all',
):
    '''
    Overview
    ----------
    This will download 3dep rasters from USGS using USGS vrts.
    By default USGS 3Dep stores all their rasters in lat/long (northing and easting).
    By us downloading the rasters using WBD HUC8 (or whatever domain) clips and gdal, we can
     accomplish a few extra steps.
        1) Ensure the projection types that are downloaded are consistant and controlled.
        2) Ensure we are adjusting blocksizes, compression and other raster params.
        3) Create the 3dep rasters in the size we want.

    Notes:
        - It really can be used for any huc size or any extent poly as long as it is 10m.

        - As this is a very low use tool, most values such as the USGS vrt path, output
          file names, etc are all hardcoded

        - We have a separate tool to create a VRT of any folder of rasters.

    - Jan 2026: This tool wants one folder path (-e) which has a set of WBD polys per huc. When the tool starts
        it looks at the -lh values which defaults to "all", which means load in each file name in that folder
        as source file names for individual dem file. For each file name, it drops the extension and makes the
        new DEM tif file as its original file names plus _dem.tif.
            ie) HUC8_12090301.gpkg becomes HUC8_12090301_dem.tif.
        While not manditory, our convention is HUC8_{huc number}.gpkg.  In the past we have used HUC6 wbd
        gpkg and really any size we want, just noting that the larger the physical spatial size of the .gpkg
        means a longer download and output file size natually.

        When you explicitly use the -lh flag, it will use jsut that -lh value which is a file name in that source
        directory, regardless how many files are in that folder.  ie) -lh HUC8_12090301.gpkg

        While we could put Guam and AK in one folder in the wbd directories, it is easier to split them into
        seperate folders for consistancy.

    There is a README.txt file in the /data/inputs/WBDs_for_DEM_downloading which talks the four extent folders.

    Parameters
    ----------
        - extent_file_path (str):
            Location of where the extent files that are to be used as clip extent against
            the USGS 3Dep vrt url.
            ie) /data/inputs/wbd/WBDs_for_DEM_downloading/HUC8_CONUS/

        - target_output_folder_path (str):
            The output location of the new 3dep dem files.

        - number_of_jobs (int):
            This program supports multiple procs if multiple procs/cores are available.

        - repair (True / False):
            If repair is True then look for output DEMs that are missing.
            This happens often as there can be instabilty when running long running processes.
            USGS calls and networks can blip and some of the full BED can take many, many hours.
            It will also look for DEMs that were missed entirely on previous runs.

        - skip_polygons (bool)
             If True, then we will not attempt to create polygon files for each dem file. If false,
             an domain gpkg which covers the extent of all included features merged. It will automatically
             be named DEM_Domain.parquet and saved in the same folder as the target_output_folder_path.

        - target_projection (String)
             Projection of the output DEMS and polygons (if included)

        - lst_file_names (string)
             If the lst_file_names value is used, it will look for those file names in the loaded source folder
             and only process ones that match. If the -lf arg is not passed, this value becomes "all"
             and it looks for all .gpkg files in the extent folder.
    '''
    # -------------------
    # Validation
    total_cpus_available = os.cpu_count() - 1
    if number_of_jobs > total_cpus_available:
        raise ValueError(
            f'The number of jobs provided: {number_of_jobs} ,'
            ' exceeds your machine\'s available CPU count minus one.'
            ' Please lower the number of jobs'
            ' value accordingly.'
        )

    if number_of_jobs > 15:
        print("")
        print(f"You have asked for {number_of_jobs} jobs\n")
        print("For each core, it opens up another extenal connection.")
        print(" But if you try to download more files simultaneously, many files an be partially downloaded")
        print(
            " with no notification or warning. It is recommended to slow down the job numbers to ensure stability."
        )
        print("")
        print(" Type 'CONTINUE' if you want to keep your original job numbers")
        print("      'MAX' if you want change your job count to 15")
        print("      any other value to abort.")
        resp = input(">> ").lower()

        if resp == "max":
            number_of_jobs = 15
        elif resp != "continue":
            print("Program aborted")
            sys.exit(1)
        print(f".. Continuing with {number_of_jobs} jobs")
    print("")

    if not os.path.exists(extent_file_path):
        raise ValueError(f'extent_file_path value of {extent_file_path}' ' not set to a valid path')

    if not os.path.exists(target_output_folder_path):
        # It is ok if the child diretory does not exist, but the parent folder must
        # parent directory
        parent_dir = os.path.abspath(os.path.join(target_output_folder_path, os.pardir))
        print(parent_dir)
        if not os.path.exists(parent_dir):
            raise ValueError(
                f"For the output path of {target_output_folder_path}, the child directory"
                " need not exist but the parent folder must."
            )
        os.makedirs(target_output_folder_path, exist_ok=True)

    # I don't need the crs_number for now
    crs_is_valid, err_msg, crs_number = val.is_valid_crs(target_projection)
    if crs_is_valid is False:
        raise ValueError(err_msg)

    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    fh.print_start_header('Acquire and process USGS DEM Started', overall_start_time)
    file_dt_string = overall_start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"3Dep_downloaded-{file_dt_string}.log"
    log_file_path = os.path.join(target_output_folder_path, log_file_name)
    file_logger = sf.setup_mp_file_logger(log_file_path, "3Dep_downloaded")

    sf.l_print(f"Downloading to {target_output_folder_path}", file_logger, "info")

    # -------------------
    # processing

    failed_file_names = []

    # Get the WBD .gpkg files (or clip extent)
    extent_file_names_raw = fh.get_file_names(extent_file_path, 'gpkg')
    sf.l_print(f"Extent files coming from {extent_file_path}", file_logger, "info")

    # If a file name list is specified, only keep the specified files
    lst_file_names = lst_file_names.split()
    extent_file_names = []
    if 'all' not in lst_file_names:
        for file_name in extent_file_names_raw:
            file_path = os.path.join(extent_file_path, file_name)
            extent_file_names.extend(file_path)

        if len(extent_file_names) == 0:
            raise ValueError("After applying the file name filter list, there are no files to process.")
    else:
        extent_file_names = extent_file_names_raw

    extent_file_names.sort()

    # download dems, setting projection, block size, etc
    failed_file_names = __download_usgs_dems(
        extent_file_names, target_output_folder_path, number_of_jobs, repair, target_projection, file_logger
    )

    # TODO: Jan 28, 2026: Something failed in a conus run of polygonize, but all of the dems
    # above were just fine.
    # Manually hacked code to skip to polygonize. Put in an argument later to do this, polygonize only.
    if skip_polygons is False:
        if len(failed_file_names) > 0:
            msg = "Errors have occurred while downloading. Polygonizating can not be completed."
            " Program aborted."
            file_logger.critical(msg)
            sf.l_print(msg, file_logger, "critical")
        else:
            __polygonize(target_output_folder_path, file_logger)
    else:
        file_logger.info("polygonize skipped")

    sf.l_print("==========================================================", file_logger, "info")
    end_time = datetime.now(timezone.utc)
    sf.l_print("-- Acquire and process USGS DEM Completed", file_logger, "info")
    sf.l_print(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")
    sf.l_print(fh.print_date_time_duration(overall_start_time, end_time, False), file_logger, "info")

    print()
    print(
        '---- NOTE: Remember to scan the log file for any failures. If you find errors in the'
        ' log file, delete the output file and repair. When running in repair mode, it will scan for'
        ' missing files.'
    )
    print()


def __download_usgs_dems(
    extent_files, output_folder_path, number_of_jobs, repair, target_projection, file_logger
):
    '''
    Process:
    ----------
    download the actual raw (non reprojected files) from the USGS
    based on stated embedded arguments

    Parameters
    ----------
        - fl (object of fim_logger (must have been created))
        - remaining params are defined in acquire_and_preprocess_3dep_dems

    Notes
    ----------
        - pixel size set to 10 x 10 (m)
        - block size (256) (sometimes we use 512)
        - cblend 6 add's a small buffer when pulling down the tif (ensuring seamless
          overlap at the borders.)

    '''

    sf.l_print("==========================================================", file_logger, "info")
    sf.l_print("-- Downloading USGS DEMs Starting", file_logger, "info")

    start_time = datetime.now(timezone.utc)
    sf.l_print(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")

    base_cmd = 'gdalwarp {0} {1}'
    base_cmd += ' -cutline {2} -crop_to_cutline -ot Float32 -r bilinear'
    base_cmd += ' -of "GTiff" -overwrite -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256"'
    base_cmd += ' -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -tr 10 10 -tap'
    base_cmd += ' -t_srs {3} -cblend 6'

    """
    e.q. gdalwarp
       /vs/icurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt
       /data/inputs/usgs/3dep_dems/10m/HUC8_12090301_dem.tif
       -cutline /data/inputs/wbd/HUC8/HUC8_12090301.gpkg
       -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256"
       -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -tr 10 10 -tap -t_srs ESRI:102039 -cblend 6
    """

    tasks_args_list = []
    for extent_file in extent_files:
        basic_file_name = os.path.basename(extent_file).split('.')[0]
        tasks_args_list.append(
            {
                'basic_file_name': basic_file_name,
                'extent_file': extent_file,
                'output_folder_path': output_folder_path,
                'download_url': __USGS_3DEP_10M_VRT_URL,
                'target_projection': target_projection,
                'base_cmd': base_cmd,
                'repair': repair,
            }
        )

    sf.l_print(f"Processing {len(tasks_args_list)} files with {number_of_jobs} workers", file_logger, "info")

    # === Run jobs in parallel ===
    mp_results = sf.run_with_mp(
        task_function=__download_usgs_dem_file,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=number_of_jobs,
        task_id_key="basic_file_name",  # used for task id---must be one of the keys from args dict
        show_progress=False,
    )

    # the value for success could be "False", "True", or "Skipped"
    # rtn_dic = {"success": "False", "basic_file_name": basic_file_name}
    # basic_file_name is the file name without the extension
    failed_file_names = []
    for task_id, task_results in mp_results.items():
        if task_results["success"] == "False":
            failed_file_names.append(task_id)

    if len(failed_file_names) > 0:
        file_names = ', ' in (failed_file_names)
        sf.l_print(
            f"Some DEM downloads have failed. Here are files that need review for errors: {file_names}",
            file_logger,
            "info",
        )

    end_time = datetime.now(timezone.utc)
    sf.l_print("-- Downloading USGS DEM Completed", file_logger, "info")
    sf.l_print(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")
    sf.l_print(fh.print_date_time_duration(start_time, end_time, print_dur_msg=False), file_logger, "info")

    return failed_file_names


def __download_usgs_dem_file(
    basic_file_name,
    extent_file,
    output_folder_path,
    download_url,
    target_projection,
    base_cmd,
    repair,
    file_logger,
    screen_queue,
    task_id,
):
    '''
        Process:
        ----------
            Downloads just one dem file from USGS. This is setup as a method
            to allow for multi-processing.


        Parameters:
        ----------
            - extent_file (str)
                 When the dem is downloaded, it is clipped against this extent (.gpkg) file.
            - output_folder_path (str)
                 Location of where the output file will be stored
            - download_url (str)
                 URL for the USGS download site (note: Should include '/vsicurl/' at the
                 front of the URL)
            - target_projection (str)
                ie) EPSG:5070 or EPSG:2276, etc
            - base_cmd (str)
                 The basic GDAL command with string formatting wholes for key values.
            - repair (bool)
                 If True, and the file does not exist, it will attempt to download.

        Returns:
        ----------
            A dictionary  object with args of:
                "success":  ("True" / "False" or "Skipped")
                "basic_file_name":  "{basic_file_name} or "skipped"
    }
    '''

    sf.l_print(f"Processing {basic_file_name}", file_logger, "info", screen_queue)
    # basic_file_name = os.path.basename(extent_file).split('.')[0]
    target_file_name_raw = f"{basic_file_name}_dem.tif"  # as downloaded
    target_file = os.path.join(output_folder_path, target_file_name_raw)

    # Yes.. use string bool
    # the value for success could be "False", "True", or "Skipped"
    # basic_file_name is the file name without the extension
    rtn_dic = {"success": "False", "basic_file_name": basic_file_name}

    # 1 means it was handled successfully
    # 0 means if failed but do not shut down the mp
    # -1 means catestrophic fail and shut down the mp
    # This is part of the convention of using run_with_mp
    processed_successfully = 1  # True

    if repair and os.path.exists(target_file):
        msg = f" - Downloading -- {target_file_name_raw} - Skipped (already exists (see repair flag))"
        sf.l_print(msg, file_logger, "info", screen_queue)
        rtn_dic["success"] = "Skipped"
        return processed_successfully, rtn_dic

    file_dt_string = datetime.now(timezone.utc).strftime("%Y_%m_%d-%H_%M_%S")
    msg = f" - Downloading -- {target_file_name_raw} - Started: {file_dt_string}"
    sf.l_print(msg, file_logger, "info", screen_queue)

    cmd = base_cmd.format(download_url, target_file, extent_file, target_projection)
    # print(f"cmd is {cmd}")

    # didn't use Popen becuase of how it interacts with multi proc
    # was creating some issues. Run worked much better.

    try:

        result = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True,
        )

        if result.returncode == 0:
            msg = f" - Downloading -- {target_file_name_raw} - Complete"
            sf.l_print(msg, file_logger, "info", screen_queue)
            rtn_dic["success"] = "True"

        else:
            msg = (
                f" - Downloading -- {target_file_name_raw}"
                f" -- ERROR -- Details: Return code = {result.returncode}"
            )
            f" ; Return Error Msg = {result.stderr}."
            sf.l_print(msg, file_logger, "error", screen_queue)
            if os.path.is_file(target_file):
                msg = f"Deleting invalid file - {target_file}"
            sf.l_print(msg, file_logger, "error", screen_queue)
            os.remove(target_file)
            rtn_dic["success"] = "False"
            processed_successfully = 0  # False

    except Exception:
        msg = f"An exception occurred while downloading file {target_file_name_raw}."
        sf.l_print(msg, file_logger, "critical", screen_queue)
        sf.l_print(traceback.format_exc(), file_logger, "critical", screen_queue)
        processed_successfully = -1  # Critical fail, stop the MP

    return processed_successfully, rtn_dic


def __polygonize(target_output_folder_path, file_logger):

    # TODO: Jun 2025: Find a way to speed this up  (add MP or MT???)
    # Can likely just send the mp/mt send back the gpkg, add it to an array, then concat, and dissolve
    """
    Create a polygon of 3DEP domain from individual HUC DEMS which are then dissolved into a single polygon

    Note: If you have to re-run this tool to repair some DEMs, this section must be re-run and is by default.

    """
    dem_domain_file = os.path.join(target_output_folder_path, 'DEM_Domain.parquet')

    msg = f" - Polygonizing -- {dem_domain_file} - Started (be patient, it can take a while)"
    sf.l_print(msg, file_logger, "info")

    start_time = datetime.now(timezone.utc)
    sf.l_print(f"Polygonation start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")

    dem_files = glob.glob(os.path.join(target_output_folder_path, '*_dem.tif'))

    if len(dem_files) == 0:
        raise Exception("There are no DEMs to polygonize")

    dem_files.sort()

    dem_parquets = gpd.GeoDataFrame()

    for n, dem_file in enumerate(dem_files):
        sf.l_print(f"Polygonizing: {dem_file}", file_logger, "info")
        edge_tif = f'{os.path.splitext(dem_file)[0]}_edge.tif'
        edge_parquet = f'{os.path.splitext(edge_tif)[0]}.parquet'

        # Calculate a constant valued raster from valid DEM cells
        if not os.path.exists(edge_tif):
            subprocess.run(
                [
                    'gdal_calc.py',
                    '-A',
                    dem_file,
                    f'--outfile={edge_tif}',
                    '--calc=where(A > -900, 1, 0)',
                    '--co',
                    'BIGTIFF=YES',
                    '--co',
                    'NUM_THREADS=ALL_CPUS',
                    '--co',
                    'TILED=YES',
                    '--co',
                    'COMPRESS=LZW',
                    '--co',
                    'SPARSE_OK=TRUE',
                    '--type=Byte',
                    '--quiet',
                ]
            )

        # Polygonize constant valued raster
        polygonize_raster(edge_tif, edge_parquet, field_name="HydroID", connectivity=8, quiet=True)

        gdf = gpd.read_parquet(edge_parquet)

        if n == 0:
            dem_parquets = gdf
        else:
            dem_parquets = pd.concat([dem_parquets, gdf])

        os.remove(edge_tif)
        os.remove(edge_parquet)

    dem_parquets['DN'] = 1
    dem_dissolved = dem_parquets.dissolve(by='DN')
    dem_dissolved.to_file(dem_domain_file)

    if not os.path.exists(dem_domain_file):
        sf.l_print(f" - Polygonizing -- {dem_domain_file} - Failed", file_logger, "error")
    else:
        sf.l_print(f" - Polygonizing -- {dem_domain_file} - Complete", file_logger, "info")

    end_time = datetime.now(timezone.utc)
    sf.l_print(f"Polygonization end time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")
    sf.l_print(fh.print_date_time_duration(start_time, end_time, print_dur_msg=False), file_logger, "info")


if __name__ == '__main__':
    '''
    sample usage (min params): (AK)
        python3 /foss_fim/data/usgs/acquire_and_preprocess_3dep_dems.py
            -e /data/inputs/wbd/WBDs_for_DEM_downloading/HUC8_South_Alaska/ -proj "EPSG:3338"
            -t /data/inputs/dems/3dep_dems/10m_South_Alaska/20250301
            -j 6

    Pathing and epsg for the regions are:

    -e /data/inputs/wbd/WBDs_for_DEM_downloading/HUC8_CONUS/ -proj "EPSG:5070"
    -e /data/inputs/wbd/WBDs_for_DEM_downloading/HUC8_South_Alaska/ -proj "EPSG:3338"
    -e /data/inputs/wbd/WBDs_for_DEM_downloading/HUC8_American_Samoa/ -proj "EPSG:32702"
    -e /data/inputs/wbd/WBDs_for_DEM_downloading/HUC8_Guam/ -proj "EPSG:6637"


    (adding -rp if you are in repair mode)


    *** Keep the job number at 6 as the network can't handle anymore than that anyways ***


    Notes:
      - There is alot to know, so read the notes in the functions and top of the page above.
        Especailly about the -rp (repair) system.

      - Keep the job numbers low, too many of them can result in incompleted downloads for a HUC.
        Run this on a Prod / Thor sized machine as network speed it they key element.
        You can watch your servers network guage in the top right of your screen to help sort out
        what works best for throttling versus stability.

      - It is not un-common for some DEMs to not all download correctly on each pass.
        Review the output files and the logs so you know which are missing. Delete the ones in the outputs
        that are in error. Then run the tool again wihth the -rp flag (repair) which will fill in the holes.

      - This is a very low use tool. So for now, this only can load 10m (1/3 arc second) and is using
        hardcoded paths for the wbd gpkg to be used for clipping (no buffer for now).
        Also hardcoded usgs 3dep urls, etc.  Minor
        upgrades can easily be made for different urls, output folder paths, huc units, etc
        as/if needed (command line params)

      - Each output file will be the name of the input poly plus "_dem.tif". ie) if the wbd gpkg
        is named named "HUC8_12090301", then the output file name will be "HUC8_12090301_dem.tif"
        Or depends what file name you sent in for the boundary:

    FIM uses vrt's primariy for DEMs but this tool only downloads and preps the DEMs but does
    not create the vrt. That is done using the create_vrt_file.py tool.


    ++++++++++++++++++++++++++++++++++++++++++++
    SUPER IMPORTANT:
        - Make sure you run this tool for each of the 4 regions.
        - Then create the four new matching VRTs. create_vrt_file.py
        - There is a downstream tool that re-run when we get new DEMs
             - make_dem_dif_for_bridges.py (which also needs the four regions run)
    ++++++++++++++++++++++++++++++++++++++++++++
    '''

    parser = argparse.ArgumentParser(
        description='Acquires and preprocesses USGS 3Dep dems'
        '... Note. It we give it too many jobs and with each making'
        ' and call to the internet based on our limited EC2 pipe,'
        ' some can fail. It is suggested to keep job numbers low.'
    )

    parser.add_argument(
        '-e',
        '--extent_file_path',
        help='REQUIRED: location the gpkg files that will'
        ' are being used as clip regions (ie: huc8_*.gpkg).'
        ' All gpkgs in this folder will be used.',
        required=True,
    )

    parser.add_argument(
        '-t',
        '--target_output_folder_path',
        help='REQUIRED: location of where the 3dep files will be saved.',
        required=True,
        default='',
    )

    parser.add_argument(
        '-proj', '--target_projection', help='REQUIRED: Desired output CRS. ie) EPSG:5070', required=True
    )

    parser.add_argument(
        '-j',
        '--number_of_jobs',
        help='OPTIONAL: Number of (jobs) cores/processes to used.',
        required=False,
        default=1,
        type=int,
    )

    parser.add_argument(
        '-rp',
        '--repair',
        help='OPTIONAL: If included, it process only file names missing output DEMs'
        ' which happen. Read all inline notes about this feature.',
        required=False,
        action='store_true',
        default=False,
    )

    parser.add_argument(
        '-sp',
        '--skip_polygons',
        help='OPTIONAL: If this flag is included, polygons of the dems will not be made.',
        required=False,
        action='store_true',
        default=False,
    )

    parser.add_argument(
        '-lf',
        '--lst_file_names',
        help='OPTIONAL: Space-delimited list of files names in the source directory to do acquire for.'
        ' Defaults to all gpkg file names in the extent folder',
        required=False,
        default='all',
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    acquire_and_preprocess_3dep_dems(**args)
