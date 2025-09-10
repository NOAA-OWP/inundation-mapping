#!/usr/bin/env python3

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

import data.aws.s3_shared_functions as s3_sf
import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh


# GLOBAL VARIABLES
S3_CLIENT = None
S3_BUCKET_NAME = ""
USE_S3 = False

# These are the root src and target adjusted paths.
# Do not let them have things like "inputs" or "test_cases".
# Just things like /data, /data/myfim/, or foss_fim (as in s3://{somebucket}/foss_fim).
# It should always have a starting and ending slash even if s3 (yes.. s3)
#    due to string replacement.
SRC_ROOT_PATH = ""
TRG_ROOT_PATH = ""


def get_sample_data(
    huc,
    src_data_path: str,
    output_root_folder: str,
    use_s3: bool,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str,
):
    """
    Create input data for the flood inundation model

    Parameters
    ----------
    huc : str
        HUC to process
    src_data_path : str
        Path to the input data (could be an s3 or local path)
    output_root_folder : str
        Path to save the output data
    use_s3 : bool
        Download data from S3 (default is False)
    """

    globals()['USE_S3'] = use_s3

    # These are the root src and target adjusted paths.
    # do not let them have things like "inputs" or "test_cases"
    # Just things like /data, /data/myfim/, or foss_fim (as in s3://{somebucket}/foss_fim)

    print("Starting getting sample data")

    # =======================
    # Validation

    if output_root_folder.lower().startswith("s3:"):
        raise ValueError("Sorry. The output root folder can not be an s3 path, only a local path")

    # strip off the "inputs" dir if they submitted that as well
    # we always want the data path to be the parent level of "inputs"
    src_data_path = src_data_path.rstrip("/")
    if src_data_path.endswith("inputs"):
        src_data_path = src_data_path.replace("inputs", "")

    # we come out of here with src_root_dir being used versus src_data_path
    if USE_S3:
        # Note: if you are using s3, you do not automatically have to provide s3 keys as the server
        # may have various ways to authenticate to AWS incuding an credential file (defaulted profile).
        # Yes.. we want the full path to make sure they add the bucket name.
        if not src_data_path.lower().startswith("s3:"):
            raise ValueError(
                "Sorry. You have included the '-s3' (use-s3) flag, but the '-i' source data"
                " path does not start with s3://"
            )

        # change case just in case s3 comes in as S3
        # This will be broken part to bucket, root path later
        s3_full_path = src_data_path.replace("S3://", "s3://")

        # ie) From s3://some_bucket/hand_fim
        # Also validates input folder exists and test cases folder
        src_data_path = __setup_aws_values(aws_access_key_id, aws_secret_access_key, aws_region, s3_full_path)

    else:
        if not os.path.exists(src_data_path):
            raise Exception(f'{src_data_path} does not exist')

    __setup_root_paths(src_data_path, output_root_folder)

    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    sf.setup_file_logger(TRG_ROOT_PATH, "get_sample_data")
    logging.info(f"Start time: {overall_start_time.strftime('%m/%d/%Y %H:%M:%S')}")

    print("********")
    logging.info(f"Copying files/folders from {src_data_path} to {output_root_folder}")

    print("Note: Some files / folders are very large and can take a number of minutes")
    print("********")
    time.sleep(
        5
    )  # let them have 5 seconds to read the message which also allows them time to look at pathing

    load_dotenv('/foss_fim/src/bash_variables.env')

    # data that is applicable to all hand areas and not specific to a region
    logging.info("+++ Getting data applicable to all HUCs")
    get_HAND_region_data()

    huc2Identifier = huc[:2]

    if huc2Identifier == '19':
        # data specific to the AK region
        logging.info("+++ Getting data applicable to Alaska and its HUCs")
        get_AK_region_data(huc)

    else:
        # data specific to the CONUS region
        logging.info("+++ Getting data applicable to all CONUS and its HUCs")
        get_CONUS_region_data(huc)

    logging.info("+++ Getting benchmark validation data where huc applicable")
    get_validation_data(huc)

    logging.info("==========================================================")
    end_time = datetime.now(timezone.utc)
    logging.info("-- Completed getting sample data")
    logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(fh.print_date_time_duration(overall_start_time, end_time, False))


# for data that is not specific to CONUS or AK
def get_HAND_region_data():

    __copy_folder('/data/inputs/huc_lists')

    __copy_file(os.environ["bathy_file_ehydro"])
    __copy_file(os.environ["bathy_file_aibased"])
    __copy_file(os.environ["mannN_file_aibased"])
    __copy_file(os.environ["iris_sword_slope"])

    __copy_file(os.environ["nws_lid"])

    __copy_file(os.environ["bankfull_flows_file"])
    __copy_file(os.environ["vmann_input_file"])

    __copy_file(os.environ["nwm_recur_file"])

    # and also the specific interval files
    recurr_intervals = ['2', '5', '10', '25', '50']
    for recurr_interval in recurr_intervals:
        cp_file = os.path.join(
            os.path.split(os.environ["nwm_recur_file"])[0], f'nwm3_17C_recurr_{recurr_interval}_0_cms.csv'
        )
        __copy_file(cp_file)

    __copy_file(os.environ["usgs_gages_file"])
    __copy_file(os.environ["usgs_rating_curve_csv"])
    __copy_file(os.environ["usgs_acceptable_gages_path"])


# for data that is specific to CONUS
def get_AK_region_data(huc):

    # ---------------------
    # not specifically for this specific region but huc specific
    __copy_folder(os.path.join(os.environ["pre_clip_huc_dir"], huc))
    __copy_file(os.path.join(os.environ["input_fema_flood_hazard_zones"], f'nfhl_{huc}.gpkg'))
    __copy_file(os.path.join(os.environ["input_calib_points_dir"], f'{huc}.parquet'))

    # ----------------
    # huc DEM
    __copy_file(os.environ["input_DEM_domain_Alaska"])  # DEM_Domain.gpkg

    # specific DEM for the HUC
    huc_DEM_domain_dir = os.path.split(os.environ["input_DEM_Alaska"])[0]
    huc_DEM_file = os.path.join(huc_DEM_domain_dir, f'HUC8_{huc}_dem.tif')
    __copy_file(huc_DEM_file)

    # __copy_file(os.environ["input_DEM_Alaska"])  # dem vrt
    # for now.. rebuild our own custom vrt. If other dems already exist, it will include them
    __create_vrt(os.environ["input_DEM_Alaska"])

    # ----------------
    # huc bridge DEM
    huc_bridge_DEM_diff_dir = os.path.split(os.environ["input_bridge_elev_diff_alaska"])[0]
    huc_bridge_DEM_file = os.path.join(huc_bridge_DEM_diff_dir, f'HUC8_{huc}_dem_diff.tif')
    __copy_file(huc_bridge_DEM_file)

    # __copy_file(os.environ["input_bridge_elev_diff_alaska"])
    # for now.. rebuild our own custom vrt. If other dems already exist, it will include them
    __create_vrt(os.environ["input_bridge_elev_diff_alaska"])

    # ---------------------
    # data but not huc specific
    __copy_file(os.environ["osm_bridges_alaska"])
    __copy_file(os.environ["osm_roads_alaska"])

    __copy_file(os.environ["input_WBD_gdb_Alaska"])

    __copy_file(os.environ["input_landsea_Alaska"])

    __copy_file(os.environ["input_NLD_Alaska"])

    __copy_file(os.environ["input_levees_preprocessed_Alaska"])
    __copy_file(os.environ["input_nld_levee_protected_areas_Alaska"])

    print("\n #### The catchments file is big and will take a few mins #### ")
    __copy_file(os.environ["input_nwm_catchments"])
    __copy_file(os.environ["input_nwm_flows_Alaska"])
    __copy_file(os.environ["input_nwm_headwaters_Alaska"])
    __copy_file(os.environ["input_nwm_lakes_Alaska"])


# for data that is specific to CONUS
def get_CONUS_region_data(huc):

    # ---------------------
    # some of files/folders in this section are not specifically for this specific region but huc specific

    __copy_folder(os.path.join(os.environ["pre_clip_huc_dir"], huc))

    # ----------------
    # huc DEM
    __copy_file(os.environ["input_DEM_domain"])  # DEM_Domain.gpkg

    # specific DEM for the HUC
    huc_DEM_domain_dir = os.path.split(os.environ["input_DEM"])[0]
    huc_DEM_file = os.path.join(huc_DEM_domain_dir, f'HUC6_{huc[:6]}_dem.tif')
    __copy_file(huc_DEM_file)

    # __copy_file(os.environ["input_DEM"])  # dem vrt
    # for now.. rebuild our own custom vrt. If other dems already exist, it will include them
    __create_vrt(os.environ["input_DEM"])

    # ----------------
    # huc bridge DEM
    huc_bridge_DEM_diff_dir = os.path.split(os.environ["input_bridge_elev_diff"])[0]
    huc_bridge_DEM_file = os.path.join(huc_bridge_DEM_diff_dir, f'HUC6_{huc[:6]}_dem_diff.tif')
    __copy_file(huc_bridge_DEM_file)

    # __copy_file(os.environ["input_bridge_elev_diff"])
    # for now.. rebuild our own custom vrt. If other dems already exist, it will include them
    __create_vrt(os.environ["input_bridge_elev_diff"])

    __copy_file(os.environ["osm_bridges"])
    __copy_file(os.environ["osm_roads"])

    __copy_file(os.environ["input_WBD_gdb"])

    __copy_file(os.environ["input_landsea"])

    __copy_file(os.environ["input_NLD"])

    __copy_file(os.environ["input_levees_preprocessed"])
    __copy_file(os.environ["input_nld_levee_protected_areas"])

    print("\n #### The catchments file is big and will take a few mins #### ")
    __copy_file(os.environ["input_nwm_catchments"])

    __copy_file(os.environ["input_nwm_flows"])
    __copy_file(os.environ["input_nwm_headwaters"])
    __copy_file(os.environ["input_nwm_lakes"])

    # Not needed by anyone other than CONUS
    __copy_file(os.environ["input_GL_boundaries"])

    __copy_file(os.path.join(os.environ["input_fema_flood_hazard_zones"], f'nfhl_{huc}.gpkg'))

    __copy_file(os.path.join(os.environ["input_calib_points_dir"], f'{huc}.parquet'))

    ras2fim_huc_input_dir = os.path.join(os.environ["ras2fim_input_dir"], huc)
    # we do not want it to create an empty dir if the huc is not applicable
    # covers the files inside of it (if ..rating_curve_table.csv and ..rating_curve_points.gpkg)
    if os.path.exists(ras2fim_huc_input_dir):
        __copy_folder(os.path.join(os.environ["ras2fim_input_dir"], huc))

    # Not needed by anyone other than CONUS
    __copy_file(os.environ["man_calb_file"])  # houston


def get_validation_data(huc):

    validation_data_orgs = ['ble', 'nws', 'usgs', 'ras2fim']  # Do not include IFC

    for org in validation_data_orgs:
        # For each HUC, most do not have any benchmark data and of those who do,
        #    most do not have all orgs (ble, usgs.. etc)

        huc_valication_path = f'{SRC_ROOT_PATH}test_cases/{org}_test_cases/validation_data_{org}/{huc}'
        if USE_S3:
            if s3_sf.does_s3_folder_exist(S3_CLIENT, S3_BUCKET_NAME, huc_valication_path):
                __copy_folder(huc_valication_path)
        else:
            # huc_valication_path = f'{SRC_ROOT_PATH}huc_valication_path'
            if os.path.exists(huc_valication_path):
                __copy_folder(huc_valication_path)


def __copy_file(src_file_path):
    """
    Always overwrites (allows for updates at a later time if the source was updated)

    The file name will and basic folder path will always be maintained.


    For the src_file_path, the 'src_root_path' will be replaced the trg_root_path.
        ie)
        src_file_path = data/inputs/osm/conus_bridge_file.gpkg
        SRC_ROOT_PATH = /data/
        TRG_ROOT_PATH = /my_fim/data/
        trg_file_path becomes = /my_fim/data/inputs/osm/conus_bridge_file.gpkg

        or (ie.. maybe an s3 path)
        src_file_path = /noaa_owp/fim_data/inputs/osm/conus_bridge_file.gpkg
        SRC_ROOT_PATH = /noaa_owp/fim_data/
        TRG_ROOT_PATH = /my_fim/data/
        trg_file_path becomes = /my_fim/data/inputs/osm/conus_bridge_file.gpkg

    Parameters
    ----------
    src_file_path : str
        If local, it is the full path to the src file.
            ie) /my_fim_folder/data/inputs/osm/conus_bridge_file.gpkg
        If S3, it should already have the bucket removed.
            ie) /foss-fim/inputs/osm/conus_bridge_file.gpkg
            if the value came from bash_varibles, then it comes
            in with "data" which we have to change the s3 root path
    """
    if not src_file_path.startswith("/"):
        src_file_path = "/" + src_file_path

    # Most input paths come from bash_variables which start as /data/
    # but in S3, that may not be true
    # if SRC_ROOT_PATH != "/data/":
    #     src_file_path = src_file_path.replace("/data/", SRC_ROOT_PATH)

    # compensates for inputs coming from bash_variables as /data/
    if USE_S3 and src_file_path.startswith("/data/"):
        src_file_path = src_file_path.replace("/data/", SRC_ROOT_PATH)

    trg_file_path = src_file_path.replace(SRC_ROOT_PATH, TRG_ROOT_PATH)

    # This can result in empty folder if no files were actualy found for it
    # but we will let it make the empty folder as it might help find problems
    # if they exists with file downloads
    trg_dir_path = os.path.dirname(trg_file_path) + "/"
    # will overwrite always
    if not os.path.exists(trg_dir_path):
        os.makedirs(trg_dir_path, exist_ok=True)

    if USE_S3:  # src is S3, not target

        logging.info(f"Downloading file: s3://{S3_BUCKET_NAME}{src_file_path} to {trg_file_path}")

        did_file_exist = s3_sf.download_s3_file(S3_CLIENT, S3_BUCKET_NAME, src_file_path, trg_file_path)
        if not did_file_exist:
            logging.warning("... Skipping file copy, file does not exist in s3")
        # else: assume it downloaded successfully

    else:  # source is local
        logging.info(f"Copying file: {src_file_path} to {trg_file_path}")
        buffer_size = 20 * 1024 * 1024  # 20 MiB, up from the default of 1 MiB
        if os.path.isfile(src_file_path):
            # This is much faster than .copy(), .copy2(), or copyfile()
            with open(src_file_path, 'rb') as fsrc:
                with open(trg_file_path, 'wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst, length=buffer_size)
        else:
            logging.warning("... Skipping file copy, file does not exist")


def __copy_folder(src_folder_path):
    """
    Overwrites files in case this is run as an udpate from previous runs (updated versions)

    Is recursive

    For the src_folder_path, the 'src_root_path' will be replaced the trg_root_path.
        ie)
        src_folder_path = /my_fim_folder/my_data/inputs/osm
        SRC_ROOT_PATH = /my_fim_folder/my_data/
        TRG_ROOT_PATH = /data/
        Final target becomes = /data/inputs/osm

    Note: When using S3, the src_root_path must be the path starting after the bucket name
        but does not actualy include the bucket name.
        ie) when full path is s3://{some_bucket}/noaa_fim/inputs, then the src_root_path
        becomes noaa_fim/inputs

    Parameters
    ----------
    src_folder_path : str
        If local, it is the full path to the src file.
            ie) /my_fim_folder/data/inputs/osm or /data/inputs/osm
        If S3, it already has the bucket removed.
            ie) /foss-fim/inputs/osm
    """

    if not src_folder_path.startswith("/"):
        src_folder_path = "/" + src_folder_path

    if not src_folder_path.endswith("/"):
        src_folder_path += "/"

    # Most input paths come from bash_variables which start as /data/
    # but in S3, that may not be true, also adjusts for s3 pathing where applicable
    # if SRC_ROOT_PATH != "/data/":
    #     src_folder_path = src_folder_path.replace("/data/", SRC_ROOT_PATH)

    # compensates for inputs coming from bash_variables as /data/
    if USE_S3 and src_folder_path.startswith("/data/"):
        src_folder_path = src_folder_path.replace("/data/", SRC_ROOT_PATH)

    # This can result in empty folder if no files were actualy found for it
    # but we will let it make the empty folder as it might help find problems
    # if they exists with file downloads
    trg_folder_path = src_folder_path.replace(SRC_ROOT_PATH, TRG_ROOT_PATH)
    if not os.path.exists(trg_folder_path):
        os.makedirs(trg_folder_path, exist_ok=True)

    if USE_S3:  # for src only, not target
        logging.info(f"Downloading folder: s3://{S3_BUCKET_NAME}{src_folder_path} to {trg_folder_path}")

        did_at_one_file_download = s3_sf.download_s3_folder(
            S3_CLIENT, S3_BUCKET_NAME, src_folder_path, trg_folder_path
        )
        if not did_at_one_file_download:
            logging.warning("... Skipping copying folder, it was empty in s3")
    else:
        logging.info(f"Copying folder: {src_folder_path} to {trg_folder_path}")
        if os.path.exists(src_folder_path):
            shutil.copytree(src_folder_path, trg_folder_path, dirs_exist_ok=True)
        else:
            logging.warning("... Skipping copy. Source folder does not exist")


def __create_vrt(src_vrt_file_path):
    """
    Creates a VRT file from a list of input files.
    We need a custom VRT based on what was copied over to the dem dirs.

    We assume the file(s) are already there.
    We follow the same basic pattern as copy_file.

    Parameters
    ----------
    src_vrt_file_path : str
        Path to the input data
    """

    if not src_vrt_file_path.startswith("/"):
        src_vrt_file_path = "/" + src_vrt_file_path

    # compensates for inputs coming from bash_variables as /data/
    if USE_S3 and src_vrt_file_path.startswith("/data/"):
        src_vrt_file_path = src_vrt_file_path.replace("/data/", SRC_ROOT_PATH)

    trg_file_path = src_vrt_file_path.replace(SRC_ROOT_PATH, TRG_ROOT_PATH)

    command = ['gdalbuildvrt', trg_file_path]
    dem_dirname = os.path.dirname(trg_file_path)

    dem_list = [os.path.join(dem_dirname, x) for x in os.listdir(dem_dirname) if x.endswith(".tif")]
    if len(dem_list) > 0:
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

    bucket_name, src_root_dir = s3_sf.parse_bucket_and_folder_name(s3_path)
    globals()['S3_BUCKET_NAME'] = bucket_name

    input_path = src_root_dir + "/inputs"
    test_case_path = src_root_dir + "/test_cases"

    logging.info("Setting up S3 connection")

    # It is possible that the user might not use explicit keys, but implicit keys
    # such as the default credentials file. So do not test for keys
    # All errors are thrown as Exceptions
    is_success, return_msg, globals()['S3_CLIENT'] = s3_sf.create_boto3_s3_client(
        aws_access_key_id, aws_secret_access_key, aws_region
    )
    if not is_success:
        raise Exception(return_msg)

    is_success, return_msg = s3_sf.does_s3_bucket_exist(globals()['S3_CLIENT'], bucket_name)
    if not is_success:
        logging.error(return_msg)
        print("program aborted")
        sys.exit(1)

    # check that the "inputs" dir exists
    print(f"Validating s3://{bucket_name}{input_path} s3 folder exists")
    does_inputs_folder_exist = s3_sf.does_s3_folder_exist(S3_CLIENT, S3_BUCKET_NAME, input_path)
    if not does_inputs_folder_exist:
        msg = f"The S3 folder path of {input_path} does not exist."
        " Please check the spelling (case-sensitive) or pathing."
        print(msg)
        print("program aborted")
        sys.exit(1)

    # check that the "test_cases" dir exists
    print(f"Validating s3://{bucket_name}{test_case_path} s3 folder exists")
    does_testcase_folder_exist = s3_sf.does_s3_folder_exist(S3_CLIENT, S3_BUCKET_NAME, test_case_path)
    if not does_testcase_folder_exist:
        msg = f"The S3 folder path of {test_case_path} does not exist."
        " Please check the spelling (case-sensitive) or pathing."
        print(msg)
        print("program aborted")
        sys.exit(1)

    return src_root_dir


def __setup_root_paths(src_data_path, output_root_folder):

    # Add starting and ending slashes if not already there
    if not src_data_path.startswith("/"):
        src_data_path = "/" + src_data_path

    if not src_data_path.endswith("/"):
        src_data_path += "/"

    if not output_root_folder.startswith("/"):
        output_root_folder = "/" + output_root_folder

    if not output_root_folder.endswith("/"):
        output_root_folder += "/"

    # This will be a local path only and not a s3 path
    if not os.path.exists(output_root_folder):
        os.makedirs(output_root_folder, exist_ok=True)

    # can be set only once
    globals()['SRC_ROOT_PATH'] = src_data_path
    globals()['TRG_ROOT_PATH'] = output_root_folder


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

    Note to FIM Dev team members:
       We do have a set of special credentials you can use for internal tests. Just ask.

    Sample Usages:
       - Against local drives for input.
           python /foss_fim/data/get_sample_data.py -u 12090301 -i /data -o /outputs/sample-data

       - Against an S3 bucket.
            python /foss_fim/data/get_sample_data.py -u 12090301 \
                  -i 's3://{bucket_name}/hand_fim' -o /outputs/sample-data \
                  -s3 -sa '{an aws access key ID} -sk '{an aws secret access key} -sr 'us-east-1'

    """

    """
    CRITICAL NOTE: To test accuratly, make sure our /data docker mount is to this sample input folder
    ie) -v ~/sample_tests_1/:/data
    """
    parser = argparse.ArgumentParser(description='Create input data for the flood inundation model')
    parser.add_argument('-u', '--huc', default='', help='HUC to process', required=True)
    parser.add_argument(
        '-i',
        '--src-data-path',
        help='Path to the source input and test_case data folders.'
        ' \n Please read the inline code by this argparses code to see more detailed'
        ' information on this data path usage.',
        required=True,
    )
    parser.add_argument(
        '-o',
        '--output-root-folder',
        help='Path to save the output data.'
        ' \n Please read the inline code by this argparses code to see more detailed'
        ' information on the output data path usage.',
        required=True,
    )
    parser.add_argument('-s3', '--use-s3', action='store_true', help='Add flag if downloading data from S3')
    parser.add_argument('-sa', '--aws-access-key-id', help='AWS access key ID', required=False, default="")
    parser.add_argument(
        '-sk', '--aws-secret-access-key', help='AWS secret access key', required=False, default=""
    )
    parser.add_argument('-sr', '--aws-region', help='AWS region (ie. us-east-1)', required=False, default="")

    args = parser.parse_args()

    get_sample_data(**vars(args))
