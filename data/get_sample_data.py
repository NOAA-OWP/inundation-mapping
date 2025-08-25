#!/usr/bin/env python3

import argparse
import logging
import os
import re
import shutil
import subprocess
# import traceback

from datetime import datetime, timezone

# import boto3
from dotenv import load_dotenv

import data.aws.s3_shared_functions as s3_sf
from utils.shared_functions import FIM_Helpers as fh

# GLOBAL VARIABLES
S3_CLIENT = None
S3_BUCKET_NAME = ""
USE_S3 = False

def get_sample_data(
    hucs,
    src_data_path: str,
    output_root_folder: str,
    use_s3: bool = False,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_region: str = None,
):
    """
    Create input data for the flood inundation model

    Parameters
    ----------
    hucs : str
        HUC(s) to process
    src_data_path : str
        Path to the input data (could be an s3 or local path)
    output_root_folder : str
        Path to save the output data
    use_s3 : bool
        Download data from S3 (default is False)
    """

    # =======================
    # Main Logic Body

    # =======================
    # Validation
    if output_root_folder.lower().startswith("s3:"):
        raise ValueError("Sorry. The output root folder can not be an s3 path, only a local path")

    # Note: if you are using s3, you do not automatically have to provide s3 keys as the server
    # may have various ways to authenticate to AWS incuding an credential file (defaulted profile)
    if use_s3 and not src_data_path.lower().startswith("s3:"):
        raise ValueError("Sorry. You have included the '-s3' (use-s3) flag, but the '-i' source data"
                         " path does not start with s3://")

    # =======================
    # This will be a local path only and not a s3 path
    if not os.path.exists(output_root_folder):
        os.makedirs(output_root_folder, exist_ok=True)

    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    # print(f"Downloading to {target_output_folder_path}")
    __setup_logger(output_root_folder, "get_sample_data")
    logging.info(f"Starting gettng sample data")
    logging.info(f"Start time: {overall_start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(f"Copying files/folders from {src_data_path} to {output_root_folder}")

    global S3_BUCKET_NAME
    global USE_S3
    USE_S3 = use_s3
    if USE_S3:
        # ie) From s3://some_bucket/hand_fim
        #  returns: 'some_bucket', 'hand_fim', 'hand_fim/inputs'
        S3_BUCKET_NAME, s3_root_path, src_input_root = __setup_aws_values(aws_access_key_id,
                                                                       aws_secret_access_key,
                                                                       aws_region,
                                                                       src_data_path)
        src_root_dir = s3_root_path

    else:
        if not os.path.exists(src_data_path):
            raise FileNotFoundError(f'{src_data_path} does not exist')

        # ie /data/fim_test
        src_root_dir = src_data_path.rstrip("/")
        src_input_root = os.path.join(src_data_path, 'inputs')

        if not os.path.exists(src_input_root):
            raise FileNotFoundError(f'{src_input_root} does not exist')

    # This path can be an s3 path, without bucket name, or a local path
    # Note: output_root_folder is always local
    # ie) data/inputs  or foss-fim/inputs (as in s3://fim-dev/foss-fim/inputs 
    #        without the bucket name)
    os.environ['inputsDir'] = src_input_root
    trg_input_root = os.path.join(output_root_folder, "inputs")

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

    orgs = ['ble', 'nws', 'usgs', 'ras2fim']  # Do not include IFC
    for org in orgs:
        validation_hucs[org] = __get_validation_hucs(src_root_dir, org)

        os.makedirs(
            os.path.join(output_root_folder, f'test_cases/{org}_test_cases/validation_data_{org}'),
            exist_ok=True,
        )

    # Copy WBD (needed for post-processing)
    __copy_file(os.environ["input_WBD_gdb"], trg_input_root, src_input_root)

    ## ahps_sites
    __copy_file(os.environ["nws_lid"], trg_input_root, src_input_root)

    ## huc_lists
    __copy_folder(os.path.join(src_input_root, 'huc_lists'), trg_input_root, src_input_root)

    ## nld
    __copy_file(os.environ["input_NLD"], trg_input_root, src_input_root)

    ## levees_preprocessed
    __copy_file(os.environ["input_levees_preprocessed"], trg_input_root, src_input_root)

    ## rating_curve
    __copy_file(os.environ["bankfull_flows_file"], trg_input_root, src_input_root)

    ## bathymetry_adjustment and calibration files
    __copy_file(os.environ["bathy_file_ehydro"], trg_input_root, src_input_root)
    __copy_file(os.environ["bathy_file_aibased"], trg_input_root, src_input_root)
    __copy_file(os.environ["mannN_file_aibased"], trg_input_root, src_input_root)
    __copy_file(os.environ["vmann_input_file"], trg_input_root, src_input_root)
    __copy_file(os.environ["iris_sword_slope"], trg_input_root, src_input_root)
    __copy_file(os.environ["man_calb_file"], trg_input_root, src_input_root)

    ## recurr_flows
    __copy_file(os.environ["nwm_recur_file"], trg_input_root, src_input_root)

    recurr_intervals = ['2', '5', '10', '25', '50']
    for recurr_interval in recurr_intervals:
        __copy_file(
            os.path.join(os.path.split(NWM_RECUR_FILE)[0], f'nwm3_17C_recurr_{recurr_interval}_0_cms.csv'),
            output_root_folder,
            src_input_root,
            bucket_root_path,
        )

    ## usgs_gages
    __copy_file(os.environ["usgs_gages_file"], trg_input_root, src_input_root)
    __copy_file(os.environ["usgs_rating_curve_csv"], trg_input_root, src_input_root)
    __copy_file(os.environ["usgs_acceptable_gages_path"], trg_input_root, src_input_root)

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
            dem_vrt_file_alaska = os.environ['input_DEM_Alaska']
            input_DEM_domain = os.environ["input_DEM_domain_Alaska"]
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC8_{huc}_dem.tif')
            input_NWM_lakes = os.environ['input_nwm_lakes_Alaska']
            input_NLD_levee_protected_areas = os.environ["input_nld_levee_protected_areas_Alaska"]
            input_LANDSEA = os.environ['input_landsea_Alaska']

            # only copy if we need an AK WBD (yes. possible overwriting)
            __copy_file(os.environ["input_WBD_gdb_Alaska"], trg_input_root, src_input_root)

            # Need to make our own vrt for dem diff
            # This will the name of the rebuilt vrt
            bridge_dem_dif_vrt_file_alaska = os.environ["input_bridge_elev_diff_alaska"]
            input_DEM_diff_tifs = os.path.join(
                 os.path.split(bridge_dem_dif_vrt_file_alaska)[0], f'HUC8_{huc}_dem_diff.tif'
            )
            __copy_file(input_DEM_diff_tifs, trg_input_root, src_input_root)
            input_osm_bridges = os.environ["osm_bridges_alaska"]
            input_osm_roads = os.environ["osm_roads_alaska"]

        else:
            dem_vrt_file_conus = os.environ['input_DEM']
            input_DEM_domain = os.environ["input_DEM_domain"]
            input_DEM_file = os.path.join(os.path.split(input_DEM_domain)[0], f'HUC6_{huc[:6]}_dem.tif')

            input_NWM_lakes = os.environ['input_nwm_lakes']
            input_NLD_levee_protected_areas = os.environ["input_nld_levee_protected_areas"]

            bridge_dem_dif_vrt_file_conus = os.environ["input_bridge_elev_diff"]
            input_DEM_diff_tifs = os.path.join(
                os.path.split(bridge_dem_dif_vrt_file_conus)[0], f'HUC6_{huc[:6]}_dem_diff.tif'
            )
            __copy_file(input_DEM_diff_tifs, trg_input_root, src_input_root)
            input_osm_bridges = os.environ["osm_bridges"]
            input_osm_roads = os.environ["osm_roads"]

            # Define the landsea water body mask using either Great Lakes or Ocean polygon input #
            if huc2Identifier == "04":
                input_LANDSEA = os.environ["input_GL_boundaries"]
            else:
                input_LANDSEA = os.environ['input_landsea']

        ## ===============================
        ## Not HUC specific files, but specific to either CONUS or AK

        # Copying files that are specific to AK or CONUS
        # Yes.. many might be copied more than once if more than one huc exists in CONUS or AK
        # dems
        __copy_file(input_DEM_domain, trg_input_root, src_input_root)
        __copy_file(input_DEM_file, trg_input_root, src_input_root)

        # lakes
        ## nwm_hydrofabric
        __copy_file(input_NWM_lakes, trg_input_root, src_input_root)

        ## landsea mask
        __copy_file(input_LANDSEA, trg_input_root, src_input_root)

        ## nld_vectors
        __copy_file(input_NLD_levee_protected_areas, trg_input_root, src_input_root)

        # bridge and road data
        __copy_file(input_osm_bridges, trg_input_root, src_input_root)
        __copy_file(input_osm_roads, trg_input_root, src_input_root)

        ## ===============================
        ## HUC specific files
        __copy_file(
            os.path.join(os.environ["input_calib_points_dir"], f'{huc}.parquet'),
            trg_input_root,
            src_input_root
        )

        __copy_file(
            os.path.join(os.environ["input_fema_flood_hazard_zones"], f'nfhl_{huc}.gpkg'),
            trg_input_root,
            src_input_root
        )

        ## pre_clip_huc8
        __copy_folder(
            os.path.join(os.environ["pre_clip_huc_dir"], huc), trg_input_root, src_input_root
        )

        for org in orgs:
            if huc in validation_hucs[org]:
                __copy_validation_data(org, huc, src_data_path, trg_input_root)

        ## ras2fim
        ras2fim_input_dir = os.path.join(os.environ["ras2fim_input_dir"], huc)
        # we do not want it to create an empty dir
        if os.path.exists(ras2fim_input_dir):
            __copy_file(
                os.path.join(ras2fim_input_dir, os.environ["ras_rating_curve_csv_filename"]),
                trg_input_root,
                src_input_root
            )
            __copy_file(
                os.path.join(ras2fim_input_dir, os.environ["ras_rating_curve_gpkg_filename"]),
                trg_input_root,
                src_input_root
            )
    #   End of huc specific downloads

    # create DEM VRTs
    # We may not necesarily need vrts for everyone. ie) not all HUCs have bridges
    # we not have any AK or maybe AK and CONUS
    if dem_vrt_file_conus != "":
        logging.info(f"Creating CONUS DEM vrt file")
        __create_vrt(dem_vrt_file_conus)

    if dem_vrt_file_alaska != "":
        logging.info(f"Creating Alaska DEM vrt file")
        __create_vrt(dem_vrt_file_alaska)

    # Bridge dem diff vrts
    if bridge_dem_dif_vrt_file_conus != "":
        logging.info(f"Creating CONUS Bridge DEM Diff vrt file")
        __create_vrt(bridge_dem_dif_vrt_file_conus)

    if bridge_dem_dif_vrt_file_alaska != "":
        logging.info(f"Creating Alaska Bridge DEM Diff vrt file")
        __create_vrt(bridge_dem_dif_vrt_file_alaska)


    logging.info("==========================================================")
    end_time = datetime.now(timezone.utc)
    logging.info("-- Completed getting sample data")
    logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(fh.print_date_time_duration(overall_start_time, end_time, False))



def __get_validation_hucs(root_dir: str, org: str):
    """
    Get the list of HUCs for validation

    Parameters
    ----------
    root_dir : str
        Root directory (s3 or local. if s3 it is past the bucket name)
    org : str
        Organization name
    """

    folder_path = f'{root_dir}/test_cases/{org}_test_cases/validation_data_{org}'

    if USE_S3:
        return s3_sf.get_folder_list(S3_CLIENT, S3_BUCKET_NAME, folder_path)
    else:
        return [
            d
            for d in os.listdir(folder_path)
            if re.match(r'^\d{8}$', d)
        ]


def __copy_validation_data(org: str, huc: str, src_data_path: str, output_data_path: str):
    """
    Make the path to the validation data

    Parameters
    ----------
    org : str
        Organization name
    huc : str
        HUC
    src_data_path : str
        Path to the root folder in front of the /test_cases folder
    output_data_path : str
        Path to save the output data
    """

    validation_path = f'test_cases/{org}_test_cases/validation_data_{org}/{huc}'

    output_validation_path = os.path.join(output_data_path, validation_path)
    os.makedirs(output_validation_path, exist_ok=True)

    __copy_folder(os.path.join(src_data_path, validation_path), output_validation_path)
    

def __copy_file(src_file_path, trg_root_path, src_root_path):
    """
    Always overwrites (allows for updates at a later time if the source was updated)

    The file name will and basic folder path will always be maintained.

    For the src_file, the 'src_root_path' will be replaced the trg_root_path.
        ie)
        src_file_path = /my_fim_folder/my_data/inputs/osm/conus_bridge_file.gpkg
        src_root_path = /my_fim_folder/my_data/inputs
        trg_root_path = /data/inputs
        Final target becomes = /data/inputs/osm/conus_bridge_file.gpkg

   
    Parameters
    ----------
    src_file_path : str
        If local, it is the full path to the src file.
            ie) /my_fim_folder/data/inputs/osm/conus_bridge_file.gpkg
        If S3, it already has the bucket removed.
            ie) /foss-fim/inputs/osm/conus_bridge_file.gpkg
    src_root_path:
        src_root_path = /my_fim_folder/my_data
        ie) If s3, do not include the bucket name
    trg_root_path : str
        Path to save the output file. 
        Might be a docker path, ie /data
        Note: must be a local path
    """

    if not src_root_path.endswith("/"):
        src_root_path = src_root_path + "/"

    if not trg_root_path.startswith("/"):
        trg_root_path = trg_root_path + "/"

    trg_file_path = src_file_path.replace(src_root_path, trg_root_path)

    if not trg_file_path.startswith("/"):
        trg_file_path = "/" + trg_file_path

    # Not automaticallly the same value as the trg_root_data_path
    trg_dir_path = os.path.dirname(trg_file_path)

    # will overwrite always
    if not os.path.exists(trg_dir_path):
        os.makedirs(trg_dir_path, exist_ok=True)

        if USE_S3:  # src is S3, not target
            s3_sf.download_s3_file(S3_CLIENT, S3_BUCKET_NAME, src_file_path, trg_file_path)
        else:  # source is local
            shutil.copy2(src_file_path, trg_file_path)



# TODO: finish this for s3

def __copy_folder(src_folder_path: str, trg_root_path: str, src_root_path: str = None):
    """
    Overwrites files in case this is run as an udpate from previous runs (updated versions)

    Is recursive

    For the src_folder_path, the 'src_root_path' will be replaced the trg_root_path.
        ie)
        src_folder_path = /my_fim_folder/my_data/inputs/osm
        src_root_path = /my_fim_folder/my_data
        trg_root_path = /data
        Final target becomes = /data/inputs/osm
    
    Note: When using S3, the src_root_path must be the path starting after the bucket name
        but does not actualy include the bucket name.
        ie) when full path is s3://{some_bucket}/noaa_fim/inputs, then the src_root_path
        becomes noaa_fim/inputs

    Parameters
    ----------
    src_folder_path : str
        If local, it is the full path to the src file.
            ie) /my_fim_folder/data/inputs/osm
        If S3, it already has the bucket removed.
            ie) /foss-fim/inputs/osm
    src_root_path:
        src_root_path = /my_fim_folder/my_data
        ie) If s3, do not include the bucket name
    trg_root_path : str
        Path to save the output file. 
        Might be a docker path, ie /data
        Note: must be a local path
    """

    if not src_root_path.endswith("/"):
        src_root_path = src_root_path + "/"

    if not trg_root_path.startswith("/"):
        trg_root_path = trg_root_path + "/"

    trg_folder_path = src_folder_path.replace(src_root_path, trg_root_path)

    if USE_S3:  # for src only, not target
        logging.info(f"Downloading folder: {src_folder_path} to {trg_folder_path}")
        # download_s3_folder(bucket_name, src_folder_path, output_path)
        s3_sf.download_s3_folder(S3_CLIENT, S3_BUCKET_NAME, src_folder_path, trg_folder_path)
    else:
        logging.info(f"Copying folder: {src_folder_path} to {trg_folder_path}")
        shutil.copytree(src_folder_path, trg_folder_path, dirs_exist_ok=True)


# def download_s3_folder(bucket_name: str, s3_folder: str, local_dir: str = None):
#     """
#     Download the contents of a folder directory

#     Parameters
#     ----------
#     bucket_name:
#         the name of the s3 bucket
#     s3_folder:
#         the folder path in the s3 bucket
#     local_dir:
#         a relative or absolute directory path in the local file system
#     """

#     Bucket = S3_RESOURCE_OBJ.Bucket(bucket_name)
#     for obj in Bucket.objects.filter(Prefix=s3_folder):
#         target = (
#             obj.key if local_dir is None else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
#         )
#         if not os.path.exists(os.path.dirname(target)):
#             os.makedirs(os.path.dirname(target))
#         if obj.key[-1] == '/':
#             continue
#         Bucket.download_file(obj.key, target)

def __create_vrt(vrt_file: str):
    """
    Creates a VRT file from a list of input files

    All files must be already downloaded to a local drive to use this tool

    Parameters
    ----------
    vrt_file : str
        Full path to the input data
    """

    command = ['gdalbuildvrt', vrt_file]
    dem_dirname = os.path.dirname(vrt_file)

    dem_list = [os.path.join(dem_dirname, x) for x in os.listdir(dem_dirname) if x.endswith(".tif")]
    command.extend(dem_list)
    subprocess.call(command)



# It is possible that some of these values relating to authenication can be empty
# and still be allowed to create a valid client. See s3_shared_functions.create_boto3_s3_client
# for more details.
def __setup_aws_values(aws_access_key_id, aws_secret_access_key, aws_region, s3_path):

        """
        Processing
            
        Input:
            - aws_access_key_id: 
            - aws_secret_access_key
            - aws_region: ie.. us-east-1
            - s3_path:  ie) s3://{some_bucket}/hand_data.

        """

        bucket_name, data_path = s3_sf.parse_bucket_and_folder_name(s3_path)

        if not data_path.endswith("inputs"):
            input_path = data_path + "/inputs"
        else:
            input_path = data_path

        logging.info("Setting up S3 connection")

        # It is possible that the user might not use explicit keys, but implicit keys
        # such as the default credentials file. So do not test for keys
        globals()['S3_CLIENT'] = s3_sf.create_boto3_s3_client(aws_access_key_id,
                                                              aws_secret_access_key,
                                                              aws_region)

        # This can return a return_code of non 0 which means something failed
        # which can be that the bucket doesn't exist, folder does not exist, 
        # authenication errors or various things
        does_folder_exist, return_code = s3_sf.is_valid_s3_folder(os.environ["S3_CLIENT"],
                                                                  bucket_name,
                                                                  data_path)
        if does_folder_exist == False:
            # we want to handle this particular exception ourselves
            if return_code == 1051:  # Folder not found
                raise Exception(f"The S3 folder path of {input_path} does not exist."
                                " Please check the spelling (case-sensitive) or pathing")
            else:
                raise Exception("An error has occurred: "
                                f"Details: {s3_sf.get_error_msg_description(return_code)}")

        # # Jun 25 2025: Technically we now have two open s3 connections, which is fine.
        # # Maybe later we do all calls via the boto3.client but this is fine as we don't need to redo all of the s3
        # # calls to be based on client as it has different object calls. We can likely upgrade s3_shared_functions to
        # # make it easier for this app and others to make s3 calls. I will leave commented code inline at the s3_shared_functions
        # # code in case we make that jump. I expect more scripts to start usign s3 calls in the near future.
        # globals()['S3_RESOURCE_OBJ'] = boto3.resource(
        #     's3',
        #     aws_access_key_id = aws_access_key_id,
        #     aws_secret_access_key = aws_secret_access_key,
        #     region_name = aws_region
        # )        

        # ie) From s3://some_bucket/hand_fim
        #  returns: 'some_bucket', 'hand_fim', 'hand_fim/inputs'   
        return bucket_name, data_path, input_path
    

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
    This script is designed to use a inputs directory with all FIM input data and can copy out just the
    files it needs to work with the HUC number you provide. This dramatically reduces the amount of
    data you will need to work with a particular HUC.

    This script can be used in a couple of ways:
    - If you have local input data, you can use the regular data mounts to the full, none filtered
      inputs directory. You can either download the entire S3 inputs directory, such as ESIP or have it
      availabe inside FIM networks. ie) if you are an NOAA / Fim Team member and are getting the data
      from our EFS drive.
    
    - Or you can have this get your inputs by having it call the ESIP S3 bucket. If you do get your input
      data from ESIP S3, make sure you have the s3 credentials which you an get from us if you don't
      already have a set.

    Note: For the -i (src_data_path) value, whether it be an local path or s3 path, we will assume it has two
       folders under it named "inputs" and "test_cases". So your argument value should be similar to
       s3://{somebucket}/hand_data  or //my_fim_data/test_huc or c:/noaa/fim_data

       This pattern will be true for the -o (output_path). We will automatically add folders of "inputs" under
       it. We will only add a "test_case" directory if we have benchmark data for that HUC.

    Sample Usages:
       - Against local drives for input.
           python /foss_fim/data/get_sample_data.py -u 12090301 -i /data -o /outputs/sample-data

       - Against an S3 bucket.
            python /foss_fim/data/get_sample_data.py -u 12090301 \
                  -i 's3://{bucket_name}/hand_fim' -o /outputs/sample-data \
                  -s3 -ak '{an aws access key ID} -sk '{an aws secret access key} -sr 'us-east-1'
                  
    """

    parser = argparse.ArgumentParser(description='Create input data for the flood inundation model')
    parser.add_argument('-u', '--hucs', nargs='+', help='HUC to process')
    parser.add_argument('-i', '--src-data-path', help='Path to the source input and test_case data folders.'
                        ' Please read the inline code by this argparses code to see more detailed'
                        ' information on this data path usage.', required=True)
    parser.add_argument('-o', '--output-root-folder', help='Path to save the output data'
                        ' Please read the inline code by this argparses code to see more detailed'
                        ' information on the output data path usage.', required=True)
    parser.add_argument('-s3', '--use-s3', action='store_true', help='Add flag if downloading data from S3')
    parser.add_argument('-ak', '--aws-access-key-id', help='AWS access key ID', required=False)
    parser.add_argument('-sk', '--aws-secret-access-key', help='AWS secret access key', required=False)
    parser.add_argument('-sr', '--aws-region', help='AWS region (ie. us-east-1)', required=False)

    args = parser.parse_args()

    get_sample_data(**vars(args))

