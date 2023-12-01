#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import shutil
import subprocess
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime

import geopandas as gpd
import pandas as pd

import utils.shared_functions as sf
import utils.shared_validators as val
from utils.shared_functions import FIM_Helpers as fh


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
    retry=False,
    skip_polygons=False,
    target_projection='EPSG:5070',
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
        - We have a separate tool to create a VRT of any folder of rasters.

    Parameters
    ----------
        - extent_file_path (str):
            Location of where the extent files that are to be used as clip extent against
            the USGS 3Dep vrt url.
            ie) /data/inputs/wbd/HUC6

        - target_output_folder_path (str):
            The output location of the new 3dep dem files. When the param is not submitted,
            it will be sent to /data/input/usgs/3dep_dems/10m/.

        - number_of_jobs (int):
            This program supports multiple procs if multiple procs/cores are available.

        - retry (True / False):
            If retry is True and the file exists (either the raw downloaded DEM and/or)
            the projected one, then skip it

        - skip_polygons (bool)
             If True, then we will not attempt to create polygon files for each dem file. If false,
             an domain gpkg which covers the extent of all included features merged. It will automatically
             be named DEM_Domain.gkpg and saved in the same folderd as the target_output_folder_path.

        - target_projection (String)
            Projection of the output DEMS and polygons (if included)


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

    if not os.path.exists(extent_file_path):
        raise ValueError(f'extent_file_path value of {extent_file_path}' ' not set to a valid path')

    if (target_output_folder_path is None) or (target_output_folder_path == ""):
        target_output_folder_path = os.environ['usgs_3dep_dems_10m']

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
        os.makedirs(target_output_folder_path)
    else:  # path exists
        if not retry:
            file_list = os.listdir(target_output_folder_path)
            if len(file_list) > 0:
                print()
                msg = f"The target output folder of {target_output_folder_path} appears to not be empty.\n\n"
                "Do you want to empty the folder first?\n"
                "  -- Type 'overwrite' if you want to empty the folder and continue.\n"
                "  -- Type any other value to abort and stop the program.\n"
                "  ?="

                resp = input(msg).lower()
                if resp == "overwrite":
                    shutil.rmtree(target_output_folder_path)
                    os.mkdir(target_output_folder_path)
                else:
                    print("Program stopped\n")
                    sys.exit(0)
        else:  # might want to retry but the folder isn't there yet
            # It is ok if the child diretory does not exist, but the parent folder must
            # parent directory, we want to reset it
            parent_dir = os.path.abspath(os.path.join(target_output_folder_path, os.pardir))
            print(parent_dir)
            if not os.path.exists(parent_dir):
                raise ValueError(
                    f"For the output path of {target_output_folder_path}, the child directory"
                    " need not exist but the parent folder must."
                )
            shutil.rmtree(target_output_folder_path)
            os.mkdir(target_output_folder_path)

    # I don't need the crs_number for now
    crs_is_valid, err_msg, crs_number = val.is_valid_crs(target_projection)
    if crs_is_valid is False:
        raise ValueError(err_msg)

    # -------------------
    # setup logs
    start_time = datetime.utcnow()
    fh.print_start_header('Loading 3dep dems', start_time)

    # print(f"Downloading to {target_output_folder_path}")
    __setup_logger(target_output_folder_path)
    logging.info(f"Downloading to {target_output_folder_path}")

    # -------------------
    # processing

    # Get the WBD .gpkg files (or clip extent)
    extent_file_names = fh.get_file_names(extent_file_path, 'gpkg')
    msg = f"Extent files coming from {extent_file_path}"
    print(msg)
    logging.info(msg)

    # download dems, setting projection, block size, etc
    __download_usgs_dems(
        extent_file_names, target_output_folder_path, number_of_jobs, retry, target_projection
    )

    if skip_polygons is False:
        polygonize(target_output_folder_path)

    end_time = datetime.utcnow()
    fh.print_end_header('Loading 3dep dems', start_time, end_time)

    print()
    print(
        '---- NOTE: Remember to scan the log file for any failures. If you find errors in the'
        ' log file, delete the output file and retry'
    )
    print()
    logging.info(fh.print_date_time_duration(start_time, end_time))


def __download_usgs_dems(extent_files, output_folder_path, number_of_jobs, retry, target_projection):
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
    base_cmd += ' -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -tr 10 10'
    base_cmd += ' -t_srs {3} -cblend 6'

    """
    e.q. gdalwarp
       /vs/icurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt
       /data/inputs/usgs/3dep_dems/10m/HUC8_12090301_dem.tif
       -cutline /data/inputs/wbd/HUC8/HUC8_12090301.gpkg
       -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256"
       -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -tr 10 10 -t_srs ESRI:102039 -cblend 6
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
                'retry': retry,
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


def download_usgs_dem_file(extent_file, output_folder_path, download_url, target_projection, base_cmd, retry):
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
        - retry (bool)
             If True, and the file exists, downloading will be skipped.

    '''

    basic_file_name = os.path.basename(extent_file).split('.')[0]
    target_file_name_raw = f"{basic_file_name}_dem.tif"  # as downloaded
    target_path_raw = os.path.join(output_folder_path, target_file_name_raw)

    # It does happen where the final output size can be very small (or all no-data)
    # which is related to to the spatial extents of the dem and the vrt combined.
    # so, super small .tifs are correct.

    if (retry) and (os.path.exists(target_path_raw)):
        msg = f" - Downloading -- {target_file_name_raw} - Skipped (already exists (see retry flag))"
        print(msg)
        logging.info(msg)
        return

    msg = f" - Downloading -- {target_file_name_raw} - Started"
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

    msg = f" - Polygonizing -- {dem_domain_file} - Started"
    print(msg)
    logging.info(msg)

    dem_files = glob.glob(os.path.join(target_output_folder_path, '*_dem.tif'))

    if len(dem_files) == 0:
        raise Exception("There are no DEMs to polygonize")

    dem_gpkgs = gpd.GeoDataFrame()

    for n, dem_file in enumerate(dem_files):
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
    dem_dissolved.to_file(dem_domain_file, driver='GPKG')

    if not os.path.exists(dem_domain_file):
        msg = f" - Polygonizing -- {dem_domain_file} - Failed"
        print(msg)
        logging.error(msg)
    else:
        msg = f" - Polygonizing -- {dem_domain_file} - Complete"
        print(msg)
        logging.info(msg)


def __setup_logger(output_folder_path):
    start_time = datetime.utcnow()
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
            -e /data/inputs/wbd/HUC6_ESPG_5070/
            -t /data/inputs/3dep_dems/10m_5070/
            -r
            -j 20

    Notes:
      - This is a very low use tool. So for now, this only can load 10m (1/3 arc second) and is using
        hardcoded paths for the wbd gpkg to be used for clipping (no buffer for now).
        Also hardcoded usgs 3dep urls, etc.  Minor
        upgrades can easily be made for different urls, output folder paths, huc units, etc
        as/if needed (command line params)
      - The output path can be adjusted in case of a test reload of newer data for 3dep.
        The default is /data/input/usgs/3dep_dems/10m/
      - Each output file will be the name of the input poly plus "_dem.tif". ie) if the wbd gpkg
        is named named "HUC8_12090301", then the output file name will be "HUC8_12090301_dem.tif"
      - While you can (and should use more than one job number (if manageable by your server)),
        this tool is memory intensive and needs more RAM then it needs cores / cpus. Go ahead and
        anyways and increase the job number so you are getting the most out of your RAM. Or
        depending on your machine performance, maybe half of your cpus / cores. This tool will
        not fail or freeze depending on the number of jobs / cores you select.

    IMPORTANT:
    (Sept 2022): we do not process HUC2 of 22 (misc US pacific islands).
    We left in HUC2 of 19 (alaska) as we hope to get there in the semi near future
    They need to be removed from the input src clip directory in the first place.
    They can not be reliably removed in code.

    (Update Nov 2023): South Alaska (not all of HUC2 = 19) is now included but not all of Alaska.
    A separate output directory will be keep for South Alaska and will use EPSG:3338 versus the FIM
    default of EPSG:5070
    '''

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
        '-j',
        '--number_of_jobs',
        help='OPTIONAL: Number of (jobs) cores/processes to used.',
        required=False,
        default=1,
        type=int,
    )

    parser.add_argument(
        '-r',
        '--retry',
        help='OPTIONAL: If included, it will skip files that already exist.'
        'Default is all will be loaded/reloaded.',
        required=False,
        action='store_true',
        default=False,
    )

    parser.add_argument(
        '-t',
        '--target_output_folder_path',
        help='OPTIONAL: location of where the 3dep files will be saved.',
        required=False,
        default='',
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

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    acquire_and_preprocess_3dep_dems(**args)
