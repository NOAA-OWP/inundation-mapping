#!/usr/bin/env python3

import argparse
import os
import re
import shutil
import subprocess

import boto3
from dotenv import load_dotenv


# from tools_shared_variables import INPUTS_DIR, OUTPUTS_DIR, TEST_CASES_DIR


def get_sample_data(hucs, data_path: str, output_root_folder: str, use_s3: bool = False):
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
    """

    def get_validation_hucs(root_dir: str, org: str):
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

    def copy_validation_data(org: str, huc: str, data_path: str, output_data_path: str):
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

        copy_folder(os.path.join(data_path, validation_path), output_data_path)

    def copy_file(input_file: str, output_path: str, input_root: str = '/data'):
        """
        Copies a file if it doesn't already exist

        Parameters
        ----------
        input_file : str
            Path to the input data
        output_path : str
            Path to save the output data
        input_root : str
            input_file root directory
        """

        input_path, basename = os.path.split(input_file)

        output_file = input_file.replace(input_root, output_path)
        output_path = os.path.split(output_file)[0]

        if not os.path.exists(os.path.join(output_path, basename)):
            print(f"Copying {os.path.join(input_path, basename)} to {output_path}")
            os.makedirs(output_path, exist_ok=True)
            if use_s3:
                s3.download_file(
                    bucket, os.path.join(input_path, basename), os.path.join(output_path, basename)
                )
            else:
                shutil.copy2(os.path.join(input_path, basename), output_path)

            return os.path.join(output_path, basename)

        else:
            print(f"{os.path.join(output_path, basename)} already exists.")

    def copy_folder(input_path, output_path, input_root='/data'):
        """
        Copies a folder if it doesn't already exist

        Parameters
        ----------
        input_path : str
            Path to the input data
        output_path : str
            Path to save the output data
        """

        output_path = input_path.replace(input_root, output_path)

        if use_s3:
            print(f"Downloading {input_path} to {output_path}")
            download_s3_folder(bucket, input_path, output_path)
        else:
            print(f"Copying {input_path} to {output_path}")
            shutil.copytree(input_path, output_path, dirs_exist_ok=True)

    def download_s3_folder(bucket_name, s3_folder, local_dir=None):
        """
        Download the contents of a folder directory
        Args:
            bucket_name: the name of the s3 bucket
            s3_folder: the folder path in the s3 bucket
            local_dir: a relative or absolute directory path in the local file system
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

    if use_s3:
        s3 = boto3.client('s3')
        s3_resource = boto3.resource('s3')

        if data_path.startswith('s3://'):
            data_path = data_path[5:]

        bucket, bucket_path = data_path.split('/', 1)
        input_path = os.path.join(bucket_path, 'inputs')

    else:
        input_path = os.path.join(data_path, 'inputs')

        if not os.path.exists(input_path):
            raise FileNotFoundError(f'{input_path} does not exist')

    # Set inputsDir for the bash scripts
    os.environ['inputsDir'] = input_path
    load_dotenv('/foss_fim/src/bash_variables.env')
    PRE_CLIP_HUC_DIR = os.environ["pre_clip_huc_dir"]
    INPUT_NLD = os.environ["input_NLD"]
    INPUT_LEVEES_PREPROCESSED = os.environ["input_levees_preprocessed"]
    INPUT_NLD_LEVEE_PROTECTED_AREAS = os.environ["input_nld_levee_protected_areas"]
    INPUT_NWM_LAKES = os.environ['input_nwm_lakes']
    INPUT_GL_BOUNDARIES = os.environ["input_GL_boundaries"]
    INPUT_WBD_GDB = os.environ["input_WBD_gdb"]
    INPUT_WBD_GDB_ALASKA = os.environ["input_WBD_gdb_Alaska"]
    BANKFULL_FLOWS_FILE = os.environ["bankfull_flows_file"]
    INPUT_CALIB_POINTS_DIR = os.environ["input_calib_points_dir"]
    USGS_RATING_CURVE_CSV = os.environ["usgs_rating_curve_csv"]
    BATHYMETRY_FILE = os.environ["bathymetry_file"]
    OSM_BRIDGES = os.environ["osm_bridges"]
    VMANN_INPUT_FILE = os.environ["vmann_input_file"]
    RAS2FIM_INPUT_DIR = os.environ["ras2fim_input_dir"]
    NWM_RECUR_FILE = os.environ["nwm_recur_file"]

    root_dir = os.path.split(input_path)[0]

    ## test_cases
    validation_hucs = {}
    orgs = ['ble', 'nws', 'usgs', 'ras2fim']
    for org in orgs:
        validation_hucs[org] = get_validation_hucs(root_dir, org)

        os.makedirs(os.path.join(output_root_folder, 'test_cases', f'{org}_test_cases'), exist_ok=True)

    for huc in hucs:
        huc2Identifier = huc[:2]

        # Check whether the HUC is in Alaska or not and assign the CRS and filenames accordingly
        if huc2Identifier == '19':
            wbd_gpkg_path = INPUT_WBD_GDB_ALASKA
            input_LANDSEA = f"{input_path}/landsea/water_polygons_alaska.gpkg"
            input_DEM = os.environ['input_DEM_Alaska']
            input_DEM_domain = os.environ["input_DEM_domain_Alaska"]
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC8_{huc}_dem.tif')
        else:
            wbd_gpkg_path = INPUT_WBD_GDB
            input_DEM = os.environ['input_DEM']
            input_DEM_domain = os.environ["input_DEM_domain"]
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC6_{huc[:6]}_dem.tif')

            # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
            if huc2Identifier == "04":
                input_LANDSEA = INPUT_GL_BOUNDARIES
            else:
                input_LANDSEA = f"{input_path}/landsea/water_polygons_us.gpkg"

        ## wbd
        copy_file(wbd_gpkg_path, output_root_folder)
        copy_file(input_LANDSEA, output_root_folder)

        # dem
        copy_file(input_DEM_file, output_root_folder)

        # create VRT
        print('Creating VRT')
        output_VRT_file = input_DEM.replace(data_path, output_root_folder)
        command = ['gdalbuildvrt', output_VRT_file]
        dem_dirname = os.path.dirname(output_VRT_file)
        dem_list = [os.path.join(dem_dirname, x) for x in os.listdir(dem_dirname) if x.endswith(".tif")]
        command.extend(dem_list)
        subprocess.call(command)

        ## pre_clip_huc8
        copy_folder(os.path.join(PRE_CLIP_HUC_DIR, huc), output_root_folder)

        ## validation data
        for org in orgs:
            if huc in validation_hucs[org]:
                copy_validation_data(org, huc, data_path, output_root_folder)

    ## ahps_sites
    copy_file(os.path.join(input_path, 'ahps_sites', 'nws_lid.gpkg'), output_root_folder)

    ## bathymetry_adjustment
    copy_file(BATHYMETRY_FILE, output_root_folder)
    ## huc_lists
    copy_folder(os.path.join(input_path, 'huc_lists'), output_root_folder)

    ## nld
    copy_file(INPUT_NLD, output_root_folder)

    ## nld_vectors
    copy_file(INPUT_NLD_LEVEE_PROTECTED_AREAS, output_root_folder)

    ## levees_preprocessed
    copy_file(INPUT_LEVEES_PREPROCESSED, output_root_folder)

    ## nwm_hydrofabric
    copy_file(INPUT_NWM_LAKES, output_root_folder)

    ## rating_curve
    copy_file(BANKFULL_FLOWS_FILE, output_root_folder)

    ## recurr_flows
    copy_file(NWM_RECUR_FILE, output_root_folder)

    recurr_intervals = ['2', '5', '10', '25', '50']
    for recurr_interval in recurr_intervals:
        copy_file(
            os.path.join(os.path.split(NWM_RECUR_FILE)[0], f'nwm3_17C_recurr_{recurr_interval}_0_cms.csv'),
            output_root_folder,
        )

    copy_file(VMANN_INPUT_FILE, output_root_folder)

    copy_folder(INPUT_CALIB_POINTS_DIR, output_root_folder)

    ## usgs_gages
    copy_file(os.path.join(input_path, 'usgs_gages', 'usgs_gages.gpkg'), output_root_folder)

    copy_file(USGS_RATING_CURVE_CSV, output_root_folder)

    ## osm bridges
    copy_file(OSM_BRIDGES, output_root_folder)

    ## ras2fim
    copy_folder(os.path.join(RAS2FIM_INPUT_DIR), output_root_folder)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create input data for the flood inundation model')
    parser.add_argument('-u', '--hucs', nargs='+', help='HUC to process')
    parser.add_argument('-i', '--data-path', help='Path to the input data')
    parser.add_argument('-o', '--output-root-folder', help='Path to save the output data')
    # parser.add_argument('-s3', '--use-s3', action='store_true', help='Download data from S3')

    args = parser.parse_args()

    get_sample_data(**vars(args))
