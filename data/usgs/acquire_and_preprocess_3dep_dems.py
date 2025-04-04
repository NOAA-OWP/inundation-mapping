#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import subprocess
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd

import utils.shared_functions as sf
import utils.shared_validators as val
from utils.shared_functions import FIM_Helpers as fh


gpd.options.io_engine = "pyogrio"


'''
TODO:
    - Add input args for resolution size, which means URL and block size also hve to be parameterized.
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
    lst_hucs='all',
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

        - Currently there is no tools to extract the WBD HUC8's that are applicable. You will want
          a gkpg for each download extent (ie. HUC8, HUC6, whatever. Make a folder of each extent
          file you want. ie) a folder of WBD HUC8's. One gkpg per HUC8 (or whatever size you like)

        - When we originally used this tool for CONUS+, we loaded them in HUC6's, but now
          with selected HUC8's for South Alaska, we will use input extent files as WBD HUC8.

        - We run conus at HUC6 as there is often a number of download fails requiring our tool to run in -r (repair)
          mode to fix it. HUC8's are more files and allow for more possible errors. (usually just communication fails)

        - We have a separate tool to create a VRT of any folder of rasters.

    Parameters
    ----------
        - extent_file_path (str):
            Location of where the extent files that are to be used as clip extent against
            the USGS 3Dep vrt url.
            ie) /data/inputs/wbd/HUC6

        - target_output_folder_path (str):
            The output location of the new 3dep dem files.

        - number_of_jobs (int):
            This program supports multiple procs if multiple procs/cores are available.

        - repair (True / False):
            If repair is True then look for output DEMs that are missing or are too small (under 10mg).
            This happens often as there can be instabilty when running long running processes.
            USGS calls and networks can blip and some of the full BED can take many, many hours.
            It will also look for DEMs that were missed entirely on previous runs.

        - skip_polygons (bool)
             If True, then we will not attempt to create polygon files for each dem file. If false,
             an domain gpkg which covers the extent of all included features merged. It will automatically
             be named DEM_Domain.gkpg and saved in the same folderd as the target_output_folder_path.

        - target_projection (String)
             Projection of the output DEMS and polygons (if included)

        - lst_hucs (string)
             If the lst_hucs value is used, it will look for those HUCs in the loaded file list and
             only process ones that match.
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
    start_time = datetime.now(timezone.utc)
    fh.print_start_header('Loading 3dep dems', start_time)

    # print(f"Downloading to {target_output_folder_path}")
    __setup_logger(target_output_folder_path)
    logging.info(f"Downloading to {target_output_folder_path}")

    # -------------------
    # processing

    # Get the WBD .gpkg files (or clip extent)
    extent_file_names_raw = fh.get_file_names(extent_file_path, 'gpkg')
    msg = f"Extent files coming from {extent_file_path}"
    print(msg)
    logging.info(msg)

    # If a HUC list is specified, only keep the specified HUCs
    lst_hucs = lst_hucs.split()
    extent_file_names = []
    if 'all' not in lst_hucs:
        for huc in lst_hucs:
            if len(huc) != 6 and len(huc) != 8:
                raise ValueError("HUC values from the list should be 6 or 8 digits long")
            extent_file_names.extend([x for x in extent_file_names_raw if huc in x])

        if len(extent_file_names) == 0:
            raise ValueError(
                "After applying the huc filter list, there are no files to process."
                " All files were checked based on the pattern of _(huc number)"
            )
    else:
        extent_file_names = extent_file_names_raw

    extent_file_names.sort()

    # download dems, setting projection, block size, etc
    __download_usgs_dems(
        extent_file_names, target_output_folder_path, number_of_jobs, repair, target_projection
    )

    if skip_polygons is False:
        polygonize(target_output_folder_path)

    end_time = datetime.now(timezone.utc)
    fh.print_end_header('Loading 3dep dems complete', start_time, end_time)

    print()
    print(
        '---- NOTE: Remember to scan the log file for any failures. If you find errors in the'
        ' log file, delete the output file and repair.'
    )
    print()
    logging.info(fh.print_date_time_duration(start_time, end_time))


def __download_usgs_dems(extent_files, output_folder_path, number_of_jobs, repair, target_projection):
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

    print("==========================================================")
    print("-- Downloading USGS DEMs Starting")

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

    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        executor_dict = {}

        for idx, extent_file in enumerate(extent_files):
            download_dem_args = {
                'extent_file': extent_file,
                'output_folder_path': output_folder_path,
                'download_url': __USGS_3DEP_10M_VRT_URL,
                'target_projection': target_projection,
                'base_cmd': base_cmd,
                'repair': repair,
            }

            try:
                future = executor.submit(download_usgs_dem_file, **download_dem_args)
                executor_dict[future] = extent_file
            except Exception as ex:
                summary = traceback.StackSummary.extract(traceback.walk_stack(None))
                print(f"*** {ex}")
                print(''.join(summary.format()))

                logging.critical(f"*** {ex}")
                logging.critical(''.join(summary.format()))

                sys.exit(1)

        # Send the executor to the progress bar and wait for all tasks to finish
        sf.progress_bar_handler(executor_dict, "Downloading USGG 3Dep Dems")

    print("-- Downloading USGS DEMs Completed")
    logging.info("-- Downloading USGS DEMs Completed")
    print("==========================================================")


def download_usgs_dem_file(
    extent_file, output_folder_path, download_url, target_projection, base_cmd, repair
):
    '''
    Process:
    ----------
        Downloads just one dem file from USGS. This is setup as a method
        to allow for multi-processing.


    Parameters:
    ----------
        - extent_file (str)
             When the dem is downloaded, it is clipped against this extent (.gkpg) file.
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
             If True, and the file does not exist or is too small (under 10mb),
             it will attempt to download.

    '''

    basic_file_name = os.path.basename(extent_file).split('.')[0]
    target_file_name_raw = f"{basic_file_name}_dem.tif"  # as downloaded
    target_path_raw = os.path.join(output_folder_path, target_file_name_raw)

    # It does happen where the final output size can be very small (or all no-data)
    # which is related to to the spatial extents of the dem and the vrt combined.
    # so, super small .tifs are correct.

    if (repair) and (os.path.exists(target_path_raw)):
        if os.stat(target_path_raw).st_size < 1000000:
            os.remove(target_path_raw)
        else:
            msg = f" - Downloading -- {target_file_name_raw} - Skipped (already exists (see retry flag))"
            print(msg)
            logging.info(msg)
            return

    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")

    msg = f" - Downloading -- {target_file_name_raw} - Started: {file_dt_string}"
    print(msg)
    logging.info(msg)

    cmd = base_cmd.format(download_url, target_path_raw, extent_file, target_projection)
    # print(f"cmd is {cmd}")

    # didn't use Popen becuase of how it interacts with multi proc
    # was creating some issues. Run worked much better.

    try:
        process = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True,
        )

        msg = process.stdout
        print(msg)
        logging.info(msg)

        if process.stderr != "":
            if "ERROR" in process.stderr.upper():
                msg = f" - Downloading -- {target_file_name_raw}" f"  ERROR -- details: ({process.stderr})"
                print(msg)
                logging.error(msg)
                os.remove(target_path_raw)
        else:
            msg = f" - Downloading -- {target_file_name_raw} - Complete"
            print(msg)
            logging.info(msg)
    except Exception:
        msg = "An exception occurred while downloading files."
        print(msg)
        print(traceback.format_exc())
        logging.critical(traceback.format_exc())
        sys.exit(1)


def polygonize(target_output_folder_path):
    """
    Create a polygon of 3DEP domain from individual HUC DEMS which are then dissolved into a single polygon
    """
    dem_domain_file = os.path.join(target_output_folder_path, 'DEM_Domain.gpkg')

    msg = f" - Polygonizing -- {dem_domain_file} - Started (be patient, it can take a while)"
    print(msg)
    logging.info(msg)

    dem_files = glob.glob(os.path.join(target_output_folder_path, '*_dem.tif'))

    if len(dem_files) == 0:
        raise Exception("There are no DEMs to polygonize")

    dem_gpkgs = gpd.GeoDataFrame()

    for n, dem_file in enumerate(dem_files):
        print(f"Polygonizing: {dem_file}")
        edge_tif = f'{os.path.splitext(dem_file)[0]}_edge.tif'
        edge_gpkg = f'{os.path.splitext(edge_tif)[0]}.gpkg'

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
        subprocess.run(['gdal_polygonize.py', '-8', edge_tif, '-q', '-f', 'GPKG', edge_gpkg])

        gdf = gpd.read_file(edge_gpkg)

        if n == 0:
            dem_gpkgs = gdf
        else:
            dem_gpkgs = pd.concat([dem_gpkgs, gdf])

        os.remove(edge_tif)
        os.remove(edge_gpkg)

    dem_gpkgs['DN'] = 1
    dem_dissolved = dem_gpkgs.dissolve(by='DN')
    dem_dissolved.to_file(dem_domain_file, driver='GPKG', engine='fiona')

    if not os.path.exists(dem_domain_file):
        msg = f" - Polygonizing -- {dem_domain_file} - Failed"
        print(msg)
        logging.error(msg)
    else:
        msg = f" - Polygonizing -- {dem_domain_file} - Complete"
        print(msg)
        logging.info(msg)


def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"3Dep_downloaded-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    logging.info(f'Started (UTC): {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


if __name__ == '__main__':
    '''
    sample usage (min params):
        python3 /foss_fim/data/usgs/acquire_and_preprocess_3dep_dems.py
            -e /data/inputs/wbd/wbd/HUC8_South_Alaska/ -proj "EPSG:3338"
            -t /data/inputs/dems/3dep_dems/10m_South_Alaska/20250301
            -j 6

    or
        python3 /foss_fim/data/usgs/acquire_and_preprocess_3dep_dems.py
            -e /data/inputs/wbd/HUC6_5070/ -proj "EPSG:5070"
            -t /data/inputs/dems/3dep_dems/10m_5070/20250301 -r -j 6

    *** Keep the job number at 6 as the network can't handle anymore than that anyways ***

    Notes:
      - There is alot to know, so read the notes in the functions above.

      - Keep the job numbers low, too many of them can result in incompleted downloads for a HUC.
        Becuase of this.. it does not need to be run on a prod machine.

      - It is very common for not all DEMs to not all download correctly on each pass.
        Review the output files and the logs so you know which are missing. Delete the ones in the outputs
        that are in error. Then run the tool again wihth the -r flag (repair) which will fill in the wholes

        This is also why we run it at HUC6 as it is easier to trace for failed files. We get alot of
        communication error during downloads.

      - This is a very low use tool. So for now, this only can load 10m (1/3 arc second) and is using
        hardcoded paths for the wbd gpkg to be used for clipping (no buffer for now).
        Also hardcoded usgs 3dep urls, etc.  Minor
        upgrades can easily be made for different urls, output folder paths, huc units, etc
        as/if needed (command line params)

      - Each output file will be the name of the input poly plus "_dem.tif". ie) if the wbd gpkg
        is named named "HUC8_12090301", then the output file name will be "HUC8_12090301_dem.tif"
        Or depends what file name you sent in for the boundary: ie) HUC6_120903 becomes HUC6_120903_dem.tif

    IMPORTANT:
    (Sept 2022): we do not process HUC8 of 22x (misc US pacific islands).
    We left in HUC8 of 19x (alaska) as we hope to get there in the semi near future
    They need to be removed from the input src clip directory in the first place.
    They can not be reliably removed in code at this time.

    (Update Nov 2023): South Alaska (not all of HUC2 = 19) is now included but not all of Alaska.
    A separate output directory will be keep for South Alaska and will use EPSG:3338 versus the FIM
    default of EPSG:5070

    (Update Jan 2025): In previous runs, pre Alaska, gpkg's from HUC6_5070 were feed in as an arg. This
    resulted in creating 5070 DEMS for all fim related for HUC6 which included all of AK. However,
    now Alaska has been pulled out and we run this acquire script just for AK. As now, I (Rob) will
    manually delete all of the 19x gpkg files from the HUC6_5070 to help with confusion for the next time
    we do want to reload DEMS.
    '''

    """
    *****************************

    ### IMPORTANT: Sep 13, 2024: FIM uses vrt's primariy for DEMs but this tool only downloads and preps the DEMs but does
    not create the vrt. That is done using the create_vrt_file.py tool.

    """

    parser = argparse.ArgumentParser(description='Acquires and preprocesses USGS 3Dep dems')

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
        help='OPTIONAL: If included, it process only HUCs missing output DEMs or if the output DEM'
        ' is too small (under 10 MB), which does happen. Read all inline notes about this feature',
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
        '-proj',
        '--target_projection',
        help='OPTIONAL: Desired output CRS. Defaults to EPSG:5070',
        required=False,
        default='EPSG:5070',
    )

    parser.add_argument(
        '-lh',
        '--lst_hucs',
        help='OPTIONAL: Space-delimited list of HUCs to do acquire for.'
        ' If a value exists, it will check the file names of the extent dir for files that have'
        ' have that huc number in it. Careful using HUC6 versus HUC8 values. Defaults to all HUCs',
        required=False,
        default='all',
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    acquire_and_preprocess_3dep_dems(**args)
