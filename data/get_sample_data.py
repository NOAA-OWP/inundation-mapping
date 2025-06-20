#!/usr/bin/env python3

import argparse
import logging
import os
import re
import shutil
import subprocess
import traceback

from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

from utils.shared_functions import FIM_Helpers as fh


def get_sample_data(
    hucs,
    data_path: str,
    output_root_folder: str,
    input_root: str = '/data',
    use_s3: bool = False,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
):
    """
    Create input data for the flood inundation model

    Parameters
    ----------
    hucs : str
        HUC(s) to process
    data_path : str
        Path to the input data
    output_root_folder : str
        Path to save the output data
    use_s3 : bool
        Download data from S3 (default is False)
    """

    def __get_validation_hucs(root_dir: str, org: str):
        """
        Get the list of HUCs for validation

        Parameters
        ----------
        root_dir : str
            Root directory
        org : str
            Organization name
        """

        if use_s3:
            return list(
                set(
                    [
                        d.key.split('/')[4]
                        for d in s3_resource.Bucket(bucket).objects.filter(
                            Prefix=f'{root_dir}/test_cases/{org}_test_cases/validation_data_{org}'
                        )
                        if re.match(r'^\d{8}$', d.key.split('/')[4])
                    ]
                )
            )
        else:
            return [
                d
                for d in os.listdir(f'/data/test_cases/{org}_test_cases/validation_data_{org}')
                if re.match(r'^\d{8}$', d)
            ]

    def __copy_validation_data(org: str, huc: str, data_path: str, output_data_path: str):
        """
        Make the path to the validation data

        Parameters
        ----------
        org : str
            Organization name
        huc : str
            HUC
        input_path : str
            Path to the input data
        output_data_path : str
            Path to save the output data
        """

        validation_path = f'test_cases/{org}_test_cases/validation_data_{org}/{huc}'

        output_validation_path = os.path.join(output_data_path, validation_path)
        os.makedirs(output_validation_path, exist_ok=True)

        __copy_folder(os.path.join(data_path, validation_path), output_validation_path)

    def __copy_file(input_file: str, output_path: str, input_root: str, bucket_path: str = None):
        """
        Copies a file if it doesn't already exist

        Parameters
        ----------
        input_file : str
            Path to the input data
        output_path : str
            Path to save the output data
        input_root : str
            input_file root directory (default is '/data')
        """

        input_path, basename = os.path.split(input_file)

        # Strip bucket path if use_s3 is True
        if use_s3:
            output_file = input_file.removeprefix(bucket_path)[1:]
            output_file = os.path.join(output_path, output_file)

        else:
            output_file = input_file.replace(input_root, output_path)

        output_path = os.path.split(output_file)[0]

        if not os.path.exists(os.path.join(output_path, basename)):
            input_file = os.path.join(input_path, basename)
            logging.info(f"... Copying {input_file} to {output_path}")
            os.makedirs(output_path, exist_ok=True)
            if use_s3:
                try:
                    s3.download_file(
                        bucket, os.path.join(input_path, basename), os.path.join(output_path, basename)
                    )
                except Exception as e:
                    logging.error(f"... Error downloading {os.path.join(input_path, basename)}: {e}")
                    if not os.listdir(output_path):
                        os.rmdir(output_path)
            else:
                if os.path.exists(input_file):
                    shutil.copy2(input_file, output_path)
                else:
                    logging.warning(f"{input_file} does not exist."
                                    " Note: Not all HUCs may have this file.")

            return os.path.join(output_path, basename)

        else:
            logging.info(f"{os.path.join(output_path, basename)} already exists.")

    def __copy_folder(input_path: str, output_path: str, input_root: str = None, bucket_path: str = None):
        """
        Copies a folder if it doesn't already exist

        Parameters
        ----------
        input_path : str
            Path to the input data
        output_path : str
            Path to save the output data
        input_root : str
            input_file root directory (default is '/data')
        """

        if input_root:
            # Make sure input root ends with a '/'
            if input_root[-1] != '/':
                input_root = input_root + '/'

            # # Strip bucket path if use_s3 is True
            if use_s3:
                input_dir = input_path.removeprefix(bucket_path)[1:]
            else:
                input_dir = input_path.removeprefix(input_root)
            
            output_path = os.path.join(output_path, input_dir)

        if use_s3:
            logging.info(f"Downloading folder: {input_path} to {output_path}")
            download_s3_folder(bucket, input_path, output_path)
        else:
            logging.info(f"Copying folder: {input_path} to {output_path}")
            shutil.copytree(input_path, output_path, dirs_exist_ok=True)

    def download_s3_folder(bucket_name: str, s3_folder: str, local_dir: str = None):
        """
        Download the contents of a folder directory

        Parameters
        ----------
        bucket_name:
            the name of the s3 bucket
        s3_folder:
            the folder path in the s3 bucket
        local_dir:
            a relative or absolute directory path in the local file system
        """

        Bucket = s3_resource.Bucket(bucket_name)
        for obj in Bucket.objects.filter(Prefix=s3_folder):
            target = (
                obj.key if local_dir is None else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
            )
            if not os.path.exists(os.path.dirname(target)):
                os.makedirs(os.path.dirname(target))
            if obj.key[-1] == '/':
                continue
            Bucket.download_file(obj.key, target)

    def __create_vrt(input_file: str, use_s3: bool, bucket_path: str = None):
        """
        Creates a VRT file from a list of input files

        Parameters
        ----------
        input_file : str
            Path to the input data
        output_path : str
            Path to save the output data
        input_root : str
            input_file root directory (default is '/data')
        """

        # Strip bucket path if use_s3 is True
        if use_s3:
            if not bucket_path:
                raise ValueError('Bucket path is required when using S3')
            input_file = input_file.removeprefix(bucket_path)[1:]
            output_VRT_file = os.path.join(output_root_folder, input_file)
        else:
            output_VRT_file = input_file.replace(data_path, output_root_folder)

        command = ['gdalbuildvrt', output_VRT_file]
        dem_dirname = os.path.dirname(output_VRT_file)

        dem_list = [os.path.join(dem_dirname, x) for x in os.listdir(dem_dirname) if x.endswith(".tif")]
        command.extend(dem_list)
        subprocess.call(command)

    # =======================
    # Main Logic Body

    if not os.path.exists(output_root_folder):
        os.makedirs(output_root_folder, exist_ok=True)

    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    # print(f"Downloading to {target_output_folder_path}")
    __setup_logger(output_root_folder, "get_sample_data")
    logging.info(f"Starting gettng sample data")
    logging.info(f"Start time: {overall_start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(f"Copying files/folders from {data_path} to {output_root_folder}")

    if use_s3:
        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError('AWS access key ID and secret access key are required when using S3')

        s3 = boto3.client(
            's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
        )
        s3_resource = boto3.resource(
            's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
        )

        if data_path.startswith('s3://'):
            data_path = data_path[5:]

        bucket, bucket_path = data_path.split('/', 1)
        input_path = os.path.join(bucket_path, 'inputs')

    else:
        input_path = os.path.join(data_path, 'inputs')

        if not os.path.exists(input_path):
            raise FileNotFoundError(f'{input_path} does not exist')

        bucket_path = None

    # Set inputsDir for the bash scripts
    os.environ['inputsDir'] = input_path
    root_dir = os.path.split(input_path)[0]    

    load_dotenv('/foss_fim/src/bash_variables.env')

    # -------------------
    # TODO: Do we want to move away from using these global variables?
    INPUT_DEM_DOMAIN = os.environ["input_DEM_domain"]
    INPUT_DEM_DOMAIN_ALASKA = os.environ["input_DEM_domain_Alaska"]
    INPUT_DEM = os.environ['input_DEM']
    INPUT_DEM_ALASKA = os.environ['input_DEM_Alaska']
    INPUT_LANDSEA = os.environ['input_landsea']
    INPUT_LANDSEA_ALASKA = os.environ['input_landsea_Alaska']
    INPUT_NLD_LEVEE_PROTECTED_AREAS = os.environ["input_nld_levee_protected_areas"]
    INPUT_NLD_LEVEE_PROTECTED_AREAS_ALASKA = os.environ["input_nld_levee_protected_areas_Alaska"]
    INPUT_NWM_LAKES = os.environ['input_nwm_lakes']
    INPUT_NWM_LAKES_ALASKA = os.environ['input_nwm_lakes_Alaska']
    INPUT_GL_BOUNDARIES = os.environ["input_GL_boundaries"]
    INPUT_WBD_GDB_ALASKA = os.environ["input_WBD_gdb_Alaska"]
    NWM_RECUR_FILE = os.environ["nwm_recur_file"]
    INPUT_CALIB_POINTS_DIR = os.environ["input_calib_points_dir"]
    # INPUT_BRIDGE_ELEV_DIFF = os.environ["input_bridge_elev_diff"]
    # INPUT_BRIDGE_ELEV_DIFF_ALASKA = os.environ["input_bridge_elev_diff_alaska"]

    ## ===============================
    ## Not HUC specific files or specific to either CONUS or AK

    ## validation data (not huc specific)
    logging.info(f"Downloading validation data (alpha test) files, if applicable")
    validation_hucs = {}
    orgs = ['ble', 'nws', 'usgs', 'ras2fim']
    for org in orgs:
        validation_hucs[org] = __get_validation_hucs(root_dir, org)

        os.makedirs(
            os.path.join(output_root_folder, f'test_cases/{org}_test_cases/validation_data_{org}'),
            exist_ok=True,
        )

    # Copy WBD (needed for post-processing)
    __copy_file(os.environ["input_WBD_gdb"], output_root_folder, input_root, bucket_path)

    ## ahps_sites
    __copy_file(os.environ["nws_lid"], output_root_folder, input_root, bucket_path)

    ## huc_lists
    __copy_folder(os.path.join(input_path, 'huc_lists'), output_root_folder, input_root, bucket_path)

    ## nld
    __copy_file(os.environ["input_NLD"], output_root_folder, input_root, bucket_path)

    ## levees_preprocessed
    __copy_file(os.environ["input_levees_preprocessed"], output_root_folder, input_root, bucket_path)

    ## rating_curve
    __copy_file(os.environ["bankfull_flows_file"], output_root_folder, input_root, bucket_path)

    ## bathymetry_adjustment and calibration files
    __copy_file(os.environ["bathy_file_ehydro"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["bathy_file_aibased"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["mannN_file_aibased"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["vmann_input_file"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["iris_sword_slope"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["man_calb_file"], output_root_folder, input_root, bucket_path)

    ## recurr_flows
    __copy_file(NWM_RECUR_FILE, output_root_folder, input_root, bucket_path)

    recurr_intervals = ['2', '5', '10', '25', '50']
    for recurr_interval in recurr_intervals:
        __copy_file(
            os.path.join(os.path.split(NWM_RECUR_FILE)[0], f'nwm3_17C_recurr_{recurr_interval}_0_cms.csv'),
            output_root_folder,
            input_root,
            bucket_path,
        )

    # ++++++++++++++++++++++
    # TODO: Jun 18, 2025: Fix coming for hardcoded usgs_gages.gpkg file
    # ++++++++++++++++++++++


    ## usgs_gages
    __copy_file(
        os.path.join(input_path, 'usgs_gages', 'usgs_gages.gpkg'), output_root_folder, input_root, bucket_path
    )
    __copy_file(os.environ["usgs_rating_curve_csv"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["usgs_acceptable_gages_path"], output_root_folder, input_root, bucket_path)    

    # ---------------
    # TODO: check this for multiple hucs being submitted at one time.    

    # This part has inputs that are specific to AK or CONUS
    # If we get more than one CONUS or one AK, there will be some duplication in coping (for now, fix later)
    # Not all VRTs are required. Depends if AK or CONUS has dems or bridge dem diffs.
    # if the vrt_file values are empty, no need to make a new vrt for it.
    dem_vrt_file_conus = ""
    dem_vrt_file_alaska = ""

    # Some vrts may stay empty, if the HUC doesn't have a file (ie.. a huc without bridge data)
    bridge_dem_dif_vrt_file_conus = ""
    bridge_dem_dif_vrt_file_alaska = ""

    for huc in hucs:
        huc2Identifier = huc[:2]

        # Check whether the HUC is in Alaska or not and assign the CRS and filenames accordingly
        if huc2Identifier == '19':
            dem_vrt_file_alaska = INPUT_DEM_ALASKA
            input_DEM_domain = INPUT_DEM_DOMAIN_ALASKA
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC8_{huc}_dem.tif')
            input_NWM_lakes = INPUT_NWM_LAKES_ALASKA
            input_NLD_levee_protected_areas = INPUT_NLD_LEVEE_PROTECTED_AREAS_ALASKA
            input_LANDSEA = INPUT_LANDSEA_ALASKA

            # only copy if we need an AK WBD (yes. possible overwriting)
            __copy_file(INPUT_WBD_GDB_ALASKA, output_root_folder, input_root, bucket_path)

            # Need to make our own vrt for dem diff
            # This will the name of the rebuilt vrt
            bridge_dem_dif_vrt_file_alaska = os.environ["input_bridge_elev_diff_alaska"]
            input_DEM_diff_tifs = os.path.join(
                 os.path.split(bridge_dem_dif_vrt_file_alaska)[0], f'HUC8_{huc}_dem_diff.tif'
            )
            __copy_file(input_DEM_diff_tifs, output_root_folder, input_root, bucket_path)
            input_osm_bridges = os.environ["osm_bridges_alaska"]
            input_osm_roads = os.environ["osm_roads_alaska"]

        else:
            dem_vrt_file_conus = INPUT_DEM
            input_DEM_domain = INPUT_DEM_DOMAIN
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC6_{huc[:6]}_dem.tif')

            input_NWM_lakes = INPUT_NWM_LAKES
            input_NLD_levee_protected_areas = INPUT_NLD_LEVEE_PROTECTED_AREAS

            bridge_dem_dif_vrt_file_conus = os.environ["input_bridge_elev_diff"]
            input_DEM_diff_tifs = os.path.join(
                os.path.split(bridge_dem_dif_vrt_file_conus)[0], f'HUC6_{huc[:6]}_dem_diff.tif'
            )
            __copy_file(input_DEM_diff_tifs, output_root_folder, input_root, bucket_path)            
            input_osm_bridges = os.environ["osm_bridges"]
            input_osm_roads = os.environ["osm_roads"]

            # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
            if huc2Identifier == "04":
                input_LANDSEA = INPUT_GL_BOUNDARIES
            else:
                input_LANDSEA = INPUT_LANDSEA

        ## ===============================
        ## Not HUC specific files, but specific to either CONUS or AK

        # Copying files that are specific to AK or CONUS
        # Yes.. many might be copied more than once if more than one huc exists in CONUS or AK
        # dems
        __copy_file(input_DEM_domain, output_root_folder, input_root, bucket_path)
        __copy_file(input_DEM_file, output_root_folder, input_root, bucket_path)

        # lakes
        ## nwm_hydrofabric
        __copy_file(input_NWM_lakes, output_root_folder, input_root, bucket_path)

        ## landsea mask
        __copy_file(input_LANDSEA, output_root_folder, input_root, bucket_path)

        ## nld_vectors
        __copy_file(input_NLD_levee_protected_areas, output_root_folder, input_root, bucket_path)

        # bridge and road data
        __copy_file(input_osm_bridges, output_root_folder, input_root, bucket_path)
        __copy_file(input_osm_roads, output_root_folder, input_root, bucket_path)

        ## ===============================
        ## HUC specific files
        __copy_file(
            os.path.join(INPUT_CALIB_POINTS_DIR, f'{huc}.parquet'),
            output_root_folder,
            input_root,
            bucket_path,
        )

        __copy_file(
            os.path.join(os.environ["input_fema_flood_hazard_zones"], f'nfhl_{huc}.gpkg'),
            output_root_folder,
            input_root,
            bucket_path,
        )

        ## pre_clip_huc8
        __copy_folder(
            os.path.join(os.environ["pre_clip_huc_dir"], huc), output_root_folder, input_root, bucket_path
        )

        for org in orgs:
            if huc in validation_hucs[org]:
                if use_s3:
                    __copy_validation_data(org, huc, bucket_path, output_root_folder)
                else:
                    __copy_validation_data(org, huc, data_path, output_root_folder)

        ## ras2fim
        ras2fim_input_dir = os.path.join(os.environ["ras2fim_input_dir"], huc)
        # we do not want it to create an empty dir
        if os.path.exists(ras2fim_input_dir):
            __copy_file(
                os.path.join(ras2fim_input_dir, os.environ["ras_rating_curve_csv_filename"]),
                output_root_folder,
                input_root,
                bucket_path,
            )
            __copy_file(
                os.path.join(ras2fim_input_dir, os.environ["ras_rating_curve_gpkg_filename"]),
                output_root_folder,
                input_root,
                bucket_path,
            )

    # create DEM VRTs
    # We may not necesarily need vrts for everyone. ie) not all HUCs have bridges
    # we not have any AK or maybe AK and CONUS
    if dem_vrt_file_conus != "":
        logging.info(f"Creating CONUS DEM vrt file")
        __create_vrt(dem_vrt_file_conus, use_s3, bucket_path)

    if dem_vrt_file_alaska != "":
        logging.info(f"Creating Alaska DEM vrt file")
        __create_vrt(dem_vrt_file_alaska, use_s3, bucket_path)

    # Bridge dem diff vrts
    if bridge_dem_dif_vrt_file_conus != "":
        logging.info(f"Creating CONUS Bridge DEM Diff vrt file")
        __create_vrt(bridge_dem_dif_vrt_file_conus, use_s3, bucket_path)

    if bridge_dem_dif_vrt_file_alaska != "":
        logging.info(f"Creating Alaska Bridge DEM Diff vrt file")
        __create_vrt(bridge_dem_dif_vrt_file_alaska, use_s3, bucket_path)


    logging.info("==========================================================")
    end_time = datetime.now(timezone.utc)
    logging.info("-- Starting gettng sample data completed")
    logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(fh.print_date_time_duration(overall_start_time, end_time, False))


def __setup_logger(output_folder_path, prepend_file_name):

    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"{prepend_file_name}-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)


if __name__ == '__main__':

    """
    Sample Usage:
    python /foss_fim/data/get_sample_data.py -u 03100204 -i /data -o /outputs/sample-data
    """

    parser = argparse.ArgumentParser(description='Create input data for the flood inundation model')
    parser.add_argument('-u', '--hucs', nargs='+', help='HUC to process')
    parser.add_argument('-i', '--data-path', help='Path to the input data')
    parser.add_argument('-o', '--output-root-folder', help='Path to save the output data')
    parser.add_argument('-r', '--input-root', help='Root directory of the input data', default='/data')
    parser.add_argument('-s3', '--use-s3', action='store_true', help='Download data from S3')
    parser.add_argument('-ak', '--aws-access-key-id', help='AWS access key ID', required=False)
    parser.add_argument('-sk', '--aws-secret-access-key', help='AWS secret access key', required=False)

    args = parser.parse_args()

    get_sample_data(**vars(args))

