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
    output_root_folder: str="sample_data",
    input_root: str="inputs",
    aws_access_key_id: str="",
    aws_secret_access_key: str="",
):
    """
    Create input data for the flood inundation model

    Parameters
    ----------
    hucs : str
        HUC(s) to process
    data_path : str
        Path to the root data (s3 path s3://bucket/and parent folder. The input_root and output_root_folder
        are relative to this path
    output_root_folder : str
        Path to save the output data
    input_root: str
        Path to get the input data
    aws_access_key_id:
        Required 
    aws_secret_access_key:
        Required 
    """

    ####################
    # TODO: Needs input validation code

    if data_path.startswith("s3://") or data_path.startswith("S3://"):
        # use_s3 = True
        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError('AWS access key ID and secret access key are required when using S3')
        if data_path.startswith("S3://"):
            data_path.replace("S3://", "s3://")
    else:
        # Mar 2025: Local usage temporarily disabled
        # use_s3 = False
        raise ValueError("The data path must be an s3 address, bucket and parent folder."
                         " The parent folder needs to have an inputs and output folder somewhere in "
                         " in folders under it. ie) s3://noaa-nws-owp-fim/hand_fim/")


    def __get_validation_hucs(root_dir: str, org: str):
        """
        Get the list of HUCs for validation

        Parameters
        ----------
        root_dir : str
            Root directory
        org : str
            Organization name (test case source.. ble, usgs, etc)

        Note: In some S3 buckets, IFC may not exist. Also in some buckets
            there may not necessarily be any test_case_data
        """

#         if use_s3:
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
        # else:
        #     return [
        #         d
        #         for d in os.listdir(f'{root_dir}/test_cases/{org}_test_cases/validation_data_{org}')
        #         if re.match(r'^\d{8}$', d)
        #     ]

    def __copy_validation_data(org: str, huc: str, data_path: str, output_data_path: str):
        """
        Make the path to the validation data

        Parameters
        ----------
        org : str
            Organization name (benchmark source (ble, usgs, etc))
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

    def __copy_file(input_file: str, output_path: str, input_path: str, bucket_path: str = None):
        """
        Copies a file if it doesn't already exist

        Parameters
        ----------
        input_file : str
            Path to the input data
        output_path : str
            Path to save the output data
        input_path : str
            input_file root directory
        """

        input_path, basename = os.path.split(input_file)

        # Strip bucket path if use_s3 is True
        if use_s3:
            output_file = input_file.removeprefix(bucket_path)[1:]
            output_file = os.path.join(output_path, output_file)

        else:
            output_file = input_file.replace(input_path, output_path)

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
                    if not os.listdir(output_path):
                        os.rmdir(output_path)
            else:
                if os.path.exists(os.path.join(output_path, basename)):
                    shutil.copy2(os.path.join(input_path, basename), output_path)
                else:
                    print(f"{os.path.join(input_path, basename)} does not exist.")

            return os.path.join(output_path, basename)

        else:
            print(f"{os.path.join(output_path, basename)} already exists.")

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
            input_file root directory
        """

        # Make sure input root ends with a '/'
        if input_root[-1] != '/':
            input_root = input_root + '/'

        # Strip bucket path if use_s3 is True
        if use_s3:
            input_root = input_path.removeprefix(bucket_path)[1:]

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

    def __create_VRT(vrt_file: str):
        if use_s3:
            vrt_file = vrt_file.removeprefix(bucket_path)[1:]
            output_VRT_file = os.path.join(output_root_folder, vrt_file)
        else:
            output_VRT_file = vrt_file.replace(data_path, output_root_folder)

        command = ['gdalbuildvrt', output_VRT_file]
        dem_dirname = os.path.dirname(output_VRT_file)

        dem_list = [os.path.join(dem_dirname, x) for x in os.listdir(dem_dirname) if x.endswith(".tif")]
        command.extend(dem_list)
        subprocess.call(command)

    if use_s3:

        s3 = boto3.client(
            's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
        )
        s3_resource = boto3.resource(
            's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
        )

        if data_path.startswith('s3://'):
            data_path = data_path[5:]

        bucket, bucket_path = data_path.split('/', 1)
        input_path = os.path.join(bucket_path, input_root)

    else:

        if data_path.startswith != "/":
            data_path = f"/{data_path}"

        input_path = os.path.join(data_path, input_root)

        if not os.path.exists(input_path):
            raise FileNotFoundError(f'{input_path} does not exist')

        bucket_path = None

    # Set inputsDir for the bash scripts
    os.environ['inputsDir'] = input_path
    root_dir = data_path  # yes.. redundant. TODO fix this.    

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
    INPUT_BRIDGE_ELEV_DIFF = os.environ["input_bridge_elev_diff"]
    INPUT_BRIDGE_ELEV_DIFF_ALASKA = os.environ["input_bridge_elev_diff_alaska"]
    INPUT_OSM_BRIDGES = os.environ["osm_bridges"]
    INPUT_OSM_BRIDGES_ALASKA = os.environ["osm_bridges_alaska"]

    # Copy WBD (needed for post-processing)
    __copy_file(os.environ["input_WBD_gdb"], output_root_folder, input_root, bucket_path)

    ## ahps_sites
    __copy_file(os.environ["nws_lid"], output_root_folder, input_root, bucket_path)

    ## bathymetry_adjustment
    __copy_file(os.environ["bathy_file_ehydro"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["bathy_file_aibased"], output_root_folder, input_root, bucket_path)
    __copy_file(os.environ["mannN_file_aibased"], output_root_folder, input_root, bucket_path)

    ## huc_lists
    __copy_folder(os.path.join(input_path, 'huc_lists'), output_root_folder, input_root, bucket_path)

    ## nld
    __copy_file(os.environ["input_NLD"], output_root_folder, input_root, bucket_path)

    ## levees_preprocessed
    __copy_file(os.environ["input_levees_preprocessed"], output_root_folder, input_root, bucket_path)

    ## rating_curve
    __copy_file(os.environ["bankfull_flows_file"], output_root_folder, input_root, bucket_path)

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

    __copy_file(os.environ["vmann_input_file"], output_root_folder, input_root, bucket_path)

    ## usgs_gages
    __copy_file(
        os.path.join(input_path, 'usgs_gages', 'usgs_gages.gpkg'), output_root_folder, input_root, bucket_path
    )

    __copy_file(os.environ["usgs_rating_curve_csv"], output_root_folder, input_root, bucket_path)

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
            input_OSM_bridges = INPUT_OSM_BRIDGES_ALASKA
            input_bridge_elev = INPUT_BRIDGE_ELEV_DIFF_ALASKA

            __copy_file(INPUT_WBD_GDB_ALASKA, output_root_folder, input_root, bucket_path)

        else:
            input_DEM = INPUT_DEM
            input_DEM_domain = INPUT_DEM_DOMAIN
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC6_{huc[:6]}_dem.tif')
            input_NWM_lakes = INPUT_NWM_LAKES
            input_NLD_levee_protected_areas = INPUT_NLD_LEVEE_PROTECTED_AREAS
            input_OSM_bridges = INPUT_OSM_BRIDGES
            input_bridge_elev = INPUT_BRIDGE_ELEV_DIFF

            # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
            if huc2Identifier == "04":
                input_LANDSEA = INPUT_GL_BOUNDARIES
            else:
                input_LANDSEA = INPUT_LANDSEA

        input_bridge_elev_diff = os.path.join(
            os.path.split(input_bridge_elev)[0], f'HUC6_{huc[:6]}_dem_diff.tif'
        )
        __copy_file(input_bridge_elev_diff, output_root_folder, input_root, bucket_path)

        ## landsea mask
        __copy_file(input_LANDSEA, output_root_folder, input_root, bucket_path)

        # dem
        __copy_file(input_DEM_domain, output_root_folder, input_root, bucket_path)
        __copy_file(input_DEM_file, output_root_folder, input_root, bucket_path)

        # lakes
        ## nwm_hydrofabric
        __copy_file(input_NWM_lakes, output_root_folder, input_root, bucket_path)

        ## nld_vectors
        __copy_file(input_NLD_levee_protected_areas, output_root_folder, input_root, bucket_path)

        ## osm_bridges
        __copy_file(input_OSM_bridges, output_root_folder, input_root, bucket_path)

        # create VRTs
        print('Creating VRTs')
        __create_VRT(input_DEM)
        __create_VRT(input_bridge_elev)

        __copy_file(
            os.path.join(INPUT_CALIB_POINTS_DIR, f'{huc}.parquet'),
            output_root_folder,
            input_root,
            bucket_path,
        )

        ## ras2fim
        ras2fim_input_dir = os.path.join(os.environ["ras2fim_input_dir"], huc)
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

        __copy_file(
            os.path.join(os.environ["ras2fim_input_dir"], huc, os.environ["ras_rating_curve_gpkg_filename"]),
            output_root_folder,
            input_root,
            bucket_path,
        )

        ## pre_clip_huc8
        __copy_folder(
            os.path.join(os.environ["pre_clip_huc_dir"], huc), output_root_folder, input_root, bucket_path
        )

        ## test_cases validation data (not huc specific)

        # TODO: validation data may or may not exist. 

        validation_hucs = {}
        orgs = ['ble', 'nws', 'usgs', 'ras2fim']  # skip IFC
        for org in orgs:
            validation_hucs[org] = __get_validation_hucs(root_dir, org)

            os.makedirs(
                os.path.join(output_root_folder, f'test_cases/{org}_test_cases/validation_data_{org}'),
                exist_ok=True,
            )

        # ## validation data
        # for org in orgs:
        #     if huc in validation_hucs[org]:
        #         if use_s3:
        #             __copy_validation_data(org, huc, bucket_path, output_root_folder)
        #         else:
        #             __copy_validation_data(org, huc, data_path, output_root_folder)


if __name__ == '__main__':

    # This tool can be only be used against s3 to pull data and make a sample input folder on your 
    # local machine.

    # Samples (with min args)
    #    S3:
    #       python /foss_fim/data/get_sample_data.py
    #           -u 03100204
    #           -i 's3://noaa-nws-owp-fim/hand_fim'
    #           -o "c://my_fim_data" or "//home/myuser/my_fim_data"
    #           -ak '{some AWS Access key value}'
    #           -sk '{some AWS Secret key value}'    

    parser = argparse.ArgumentParser(
        description='Create input data for the flood inundation model')
    
    parser.add_argument(
        '-u',
        '--hucs',
        nargs='+',
        help='REQUIRED: HUC(s) to process',
        required=True,
    )

    parser.add_argument(
        '-i', 
        '--data-path',
        help="REQUIRED: Root directory for the input and output paths. ie) 's3://noaa-nws-owp-fim/hand_fim/'"
        " The 'r' input root and 'o' output-root-folder is relative this this root dir.",
        required=True,
    )

    parser.add_argument(
        '-ak',
        '--aws-access-key-id',
        help="REQUIRED: AWS access key id",
        required=True,
        default="",
        )
    
    parser.add_argument(
        '-sk',
        '--aws-secret-access-key',
        help='REQUIRED: AWS secret access key',
        required=True,
        default="",
        )

    parser.add_argument(
        '-o',
        '--output-root-folder',
        help="REQUIRED: Local folder where your sample input data will saved."
        " ie) c://my_fim_data" or "//home/myuser/my_fim_data",
        default="",
        required=True,
        )

    parser.add_argument(
        '-r',
        '--input-root',
        help="OPTIONAL: Root directory of the input data. Defaults to 'inputs'",
        default='inputs',
        required=False,
        )

    args = parser.parse_args()

    get_sample_data(**vars(args))
 