#!/usr/bin/env python3

import argparse
import os
import re
import shutil
import subprocess

import boto3
from dotenv import load_dotenv


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

        __copy_folder(os.path.join(data_path, validation_path), output_data_path, data_path)

    def __copy_file(input_file: str, output_path: str, input_root: str):
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

        output_file = input_file.replace(input_root, output_path)
        output_path = os.path.split(output_file)[0]

        if not os.path.exists(os.path.join(output_path, basename)):
            print(f"Copying {os.path.join(input_path, basename)} to {output_path}")
            os.makedirs(output_path, exist_ok=True)
            if use_s3:
                try:
                    s3.download_file(
                        bucket, os.path.join(input_path, basename), os.path.join(output_path, basename)
                    )
                except Exception as e:
                    print(f"Error downloading {os.path.join(input_path, basename)}: {e}")
                    os.rmdir(output_path)
            else:
                if os.path.exists(os.path.join(output_path, basename)):
                    shutil.copy2(os.path.join(input_path, basename), output_path)
                else:
                    print(f"{os.path.join(input_path, basename)} does not exist.")

            return os.path.join(output_path, basename)

        else:
            print(f"{os.path.join(output_path, basename)} already exists.")

    def __copy_folder(input_path: str, output_path: str, input_root: str = None):
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

            output_path = os.path.join(output_path, input_path.removeprefix(input_root))

        if use_s3:
            print(f"Downloading {input_path} to {output_path}")
            download_s3_folder(bucket, input_path, output_path)
        else:
            print(f"Copying {input_path} to {output_path}")
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

    # Set inputsDir for the bash scripts
    os.environ['inputsDir'] = input_path

    load_dotenv('/foss_fim/src/bash_variables.env')

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

    root_dir = os.path.split(input_path)[0]

    ## test_cases
    validation_hucs = {}
    orgs = ['ble', 'nws', 'usgs', 'ras2fim']
    for org in orgs:
        validation_hucs[org] = __get_validation_hucs(root_dir, org)

        os.makedirs(
            os.path.join(output_root_folder, f'test_cases/{org}_test_cases/validation_data_{org}'),
            exist_ok=True,
        )

    # Copy WBD (needed for post-processing)
    __copy_file(os.environ["input_WBD_gdb"], output_root_folder, input_root)
    ## ahps_sites
    __copy_file(os.environ["nws_lid"], output_root_folder, input_root)

    ## bathymetry_adjustment
    __copy_file(os.environ["bathymetry_file"], output_root_folder, input_root)
    ## huc_lists
    __copy_folder(os.path.join(input_path, 'huc_lists'), output_root_folder, input_root)

    ## nld
    __copy_file(os.environ["input_NLD"], output_root_folder, input_root)

    ## levees_preprocessed
    __copy_file(os.environ["input_levees_preprocessed"], output_root_folder, input_root)

    ## rating_curve
    __copy_file(os.environ["bankfull_flows_file"], output_root_folder, input_root)

    ## recurr_flows
    __copy_file(NWM_RECUR_FILE, output_root_folder, input_root)

    recurr_intervals = ['2', '5', '10', '25', '50']
    for recurr_interval in recurr_intervals:
        __copy_file(
            os.path.join(os.path.split(NWM_RECUR_FILE)[0], f'nwm3_17C_recurr_{recurr_interval}_0_cms.csv'),
            output_root_folder,
            input_root,
        )

    __copy_file(os.environ["vmann_input_file"], output_root_folder, input_root)

    ## usgs_gages
    __copy_file(os.path.join(input_path, 'usgs_gages', 'usgs_gages.gpkg'), output_root_folder, input_root)

    __copy_file(os.environ["usgs_rating_curve_csv"], output_root_folder, input_root)

    ## osm bridges
    __copy_file(os.environ["osm_bridges"], output_root_folder, input_root)

    for huc in hucs:
        huc2Identifier = huc[:2]

        # Check whether the HUC is in Alaska or not and assign the CRS and filenames accordingly
        if huc2Identifier == '19':
            input_LANDSEA = INPUT_LANDSEA_ALASKA
            input_DEM = INPUT_DEM_ALASKA
            input_DEM_domain = INPUT_DEM_DOMAIN_ALASKA
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC8_{huc}_dem.tif')
            input_NWM_lakes = INPUT_NWM_LAKES_ALASKA
            input_NLD_levee_protected_areas = INPUT_NLD_LEVEE_PROTECTED_AREAS_ALASKA

            __copy_file(INPUT_WBD_GDB_ALASKA, output_root_folder, input_root)

        else:
            input_DEM = INPUT_DEM
            input_DEM_domain = INPUT_DEM_DOMAIN
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC6_{huc[:6]}_dem.tif')
            input_NWM_lakes = INPUT_NWM_LAKES
            input_NLD_levee_protected_areas = INPUT_NLD_LEVEE_PROTECTED_AREAS

            # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
            if huc2Identifier == "04":
                input_LANDSEA = INPUT_GL_BOUNDARIES
            else:
                input_LANDSEA = INPUT_LANDSEA

        ## landsea mask
        __copy_file(input_LANDSEA, output_root_folder, input_root)

        # dem
        __copy_file(input_DEM_domain, output_root_folder, input_root)
        __copy_file(input_DEM_file, output_root_folder, input_root)

        # lakes
        ## nwm_hydrofabric
        __copy_file(input_NWM_lakes, output_root_folder, input_root)

        ## nld_vectors
        __copy_file(input_NLD_levee_protected_areas, output_root_folder, input_root)

        # create VRT
        print('Creating VRT')
        if use_s3:
            output_VRT_file = input_DEM.replace(input_root, output_root_folder)
        else:
            output_VRT_file = input_DEM.replace(data_path, output_root_folder)

        command = ['gdalbuildvrt', output_VRT_file]
        dem_dirname = os.path.dirname(output_VRT_file)
        dem_list = [os.path.join(dem_dirname, x) for x in os.listdir(dem_dirname) if x.endswith(".tif")]
        command.extend(dem_list)
        subprocess.call(command)

        __copy_file(os.path.join(INPUT_CALIB_POINTS_DIR, f'{huc}.parquet'), output_root_folder, input_root)

        ## ras2fim
        ras2fim_input_dir = os.path.join(os.environ["ras2fim_input_dir"], huc)
        __copy_file(
            os.path.join(ras2fim_input_dir, os.environ["ras_rating_curve_csv_filename"]),
            output_root_folder,
            input_root,
        )
        __copy_file(
            os.path.join(ras2fim_input_dir, os.environ["ras_rating_curve_gpkg_filename"]),
            output_root_folder,
            input_root,
        )

        __copy_file(
            os.path.join(os.environ["ras2fim_input_dir"], huc, os.environ["ras_rating_curve_gpkg_filename"]),
            output_root_folder,
            input_root,
        )

        ## pre_clip_huc8
        __copy_folder(os.path.join(os.environ["pre_clip_huc_dir"], huc), output_root_folder, input_root)

        ## validation data
        for org in orgs:
            if huc in validation_hucs[org]:
                if use_s3:
                    __copy_validation_data(org, huc, bucket_path, output_root_folder)
                else:
                    __copy_validation_data(org, huc, data_path, output_root_folder)


if __name__ == '__main__':
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

    # python /foss_fim/data/get_sample_data.py -u 03100204 -i /data -o /outputs/sample-data
