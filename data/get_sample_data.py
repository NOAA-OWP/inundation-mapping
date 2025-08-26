#!/usr/bin/env python3

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime, timezone

# import boto3
from dotenv import load_dotenv

import data.aws.s3_shared_functions as s3_sf
import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh


# GLOBAL VARIABLES
S3_CLIENT = None
S3_BUCKET_NAME = ""
USE_S3 = False


def get_sample_data(
    hucs,
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
    hucs : str
        HUC(s) to process
    src_data_path : str
        Path to the input data (could be an s3 or local path)
    output_root_folder : str
        Path to save the output data
    use_s3 : bool
        Download data from S3 (default is False)
    """

    global S3_BUCKET_NAME
    global USE_S3
    USE_S3 = use_s3

    # =======================
    # Validation

    if output_root_folder.lower().startswith("s3:"):
        raise ValueError("Sorry. The output root folder can not be an s3 path, only a local path")

    # strip off the "inputs" dir if they submitted that as well
    # we always want the data path to be the parent level of "inputs"
    if src_data_path.endswith("inputs/") or src_data_path.endswith("inputs"):
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
        #  returns: 'some_bucket', 'hand_fim', 'hand_fim/inputs'
        # Also validates input folder exists (but not test cases)
        # if the s3_full_path(src_data_path) comes in with the word 'inputs' on it,
        # we strip if off so we can get to test_cases as well.
        S3_BUCKET_NAME, src_root_dir, src_input_root, src_test_case_dir = __setup_aws_values(
            aws_access_key_id, aws_secret_access_key, aws_region, s3_full_path
        )

    else:
        if not os.path.exists(src_data_path):
            raise Exception(f'{src_data_path} does not exist')

        src_root_dir = src_data_path

        src_input_root = os.path.join(src_data_path, 'inputs')
        if not os.path.exists(src_input_root):
            raise Exception(f'{src_input_root} does not exist')

        src_test_case_dir = os.path.join(src_root_dir, 'test_cases')
        if not os.path.exists(src_test_case_dir):
            raise Exception(f'{src_test_case_dir} does not exist')

    # =====================
    # Add starting andending slashes if not already there
    if not src_root_dir.startswith("/"):
        src_root_dir = "/" + src_root_dir

    if not src_root_dir.endswith("/"):
        src_root_dir += "/"

    # ----------
    if not src_input_root.startswith("/"):
        src_input_root = "/" + src_input_root

    if not src_input_root.endswith("/"):
        src_input_root += "/"

    # This path can be an s3 path, without bucket name, or a local path
    # Note: output_root_folder is always local
    # ie) data/inputs  or foss-fim/inputs/ (as in s3://fim-dev/foss-fim/inputs
    #        without the bucket name)
    # we need the end slash stripped off but only for the enviro version
    # see bash_variables.
    os.environ['inputsDir'] = src_input_root.rstrip("/")

    # ----------
    if not src_test_case_dir.startswith("/"):
        src_test_case_dir = "/" + src_test_case_dir

    if not src_test_case_dir.endswith("/"):
        src_test_case_dir += "/"

    # =======================
    # setup root outputs pathing
    if not output_root_folder.startswith("/"):
        output_root_folder = "/" + output_root_folder

    if not output_root_folder.endswith("/"):
        output_root_folder += "/"

    # This will be a local path only and not a s3 path
    if not os.path.exists(output_root_folder):
        os.makedirs(output_root_folder, exist_ok=True)

    # Make the test_case dir even if it does not get any data it in
    # so users know that part did work.
    if not os.path.exists(src_test_case_dir):
        os.makedirs(src_test_case_dir, exist_ok=True)

    # this includes slashes on the end
    trg_input_root = os.path.join(output_root_folder, "inputs")

    # -------------------
    # setup logs
    overall_start_time = datetime.now(timezone.utc)
    sf.setup_file_logger(output_root_folder, "get_sample_data")
    logging.info("Starting getting sample data")
    logging.info(f"Start time: {overall_start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(f"Copying files/folders from {src_data_path} to {output_root_folder}")

    load_dotenv('/foss_fim/src/bash_variables.env')

    validation_data_orgs = ['ble', 'nws', 'usgs', 'ras2fim']  # Do not include IFC

    ## ===============================
    ## Not HUC specific files or specific to either CONUS or AK

    # Copy WBD (needed for post-processing)
    __copy_file(os.environ["input_WBD_gdb"], src_input_root, trg_input_root)

    ## ahps_sites
    __copy_file(os.environ["nws_lid"], src_input_root, trg_input_root)

    ## huc_lists
    __copy_folder(os.path.join(src_input_root, 'huc_lists'), src_input_root, trg_input_root)

    ## nld
    __copy_file(os.environ["input_NLD"], src_input_root, trg_input_root)
    __copy_file(os.environ["input_levees_preprocessed"], src_input_root, trg_input_root)
    __copy_file(os.environ["bankfull_flows_file"], src_input_root, trg_input_root)

    ## bathymetry_adjustment and calibration files
    __copy_file(os.environ["bathy_file_ehydro"], src_input_root, trg_input_root)
    __copy_file(os.environ["bathy_file_aibased"], src_input_root, trg_input_root)
    __copy_file(os.environ["mannN_file_aibased"], src_input_root, trg_input_root)
    __copy_file(os.environ["vmann_input_file"], src_input_root, trg_input_root)
    __copy_file(os.environ["iris_sword_slope"], src_input_root, trg_input_root)
    __copy_file(os.environ["man_calb_file"], src_input_root, trg_input_root)

    ## recurr_flows
    __copy_file(os.environ["nwm_recur_file"], src_input_root, trg_input_root)

    recurr_intervals = ['2', '5', '10', '25', '50']
    for recurr_interval in recurr_intervals:
        cp_file = os.path.join(
            os.path.split(os.environ["nwm_recur_file"])[0], f'nwm3_17C_recurr_{recurr_interval}_0_cms.csv'
        )
        __copy_file(cp_file, src_input_root, trg_input_root)

    ## usgs_gages
    __copy_file(os.environ["usgs_gages_file"], src_input_root, trg_input_root)
    __copy_file(os.environ["usgs_rating_curve_csv"], src_input_root, trg_input_root)
    __copy_file(os.environ["usgs_acceptable_gages_path"], src_input_root, trg_input_root)

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

    # TODO; check this for 0 padding and 19
    for huc in hucs:
        logging.info(f"*** Copying {huc} specific files/folders **** ")
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
            __copy_file(os.environ["input_WBD_gdb_Alaska"], src_input_root, trg_input_root)

            # Need to make our own vrt for dem diff
            # This will the name of the rebuilt vrt
            bridge_dem_dif_vrt_file_alaska = os.environ["input_bridge_elev_diff_alaska"]
            input_DEM_diff_tifs = os.path.join(
                os.path.split(bridge_dem_dif_vrt_file_alaska)[0], f'HUC8_{huc}_dem_diff.tif'
            )
            __copy_file(input_DEM_diff_tifs, src_input_root, trg_input_root)
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
            __copy_file(input_DEM_diff_tifs, src_input_root, trg_input_root)
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
        __copy_file(input_DEM_domain, src_input_root, trg_input_root)
        __copy_file(input_DEM_file, src_input_root, trg_input_root)

        # lakes
        ## nwm_hydrofabric
        __copy_file(input_NWM_lakes, src_input_root, trg_input_root)

        ## landsea mask
        __copy_file(input_LANDSEA, src_input_root, trg_input_root)

        ## nld_vectors
        __copy_file(input_NLD_levee_protected_areas, src_input_root, trg_input_root)

        # bridge and road data
        __copy_file(input_osm_bridges, src_input_root, trg_input_root)
        __copy_file(input_osm_roads, src_input_root, trg_input_root)

        ## ===============================
        ## HUC specific files
        __copy_file(
            os.path.join(os.environ["input_calib_points_dir"], f'{huc}.parquet'),
            src_input_root,
            trg_input_root,
        )

        __copy_file(
            os.path.join(os.environ["input_fema_flood_hazard_zones"], f'nfhl_{huc}.gpkg'),
            src_input_root,
            trg_input_root,
        )

        ## pre_clip_huc8
        __copy_folder(os.path.join(os.environ["pre_clip_huc_dir"], huc), src_input_root, trg_input_root)

        # ras2fim
        ras2fim_huc_input_dir = os.path.join(os.environ["ras2fim_input_dir"], huc)
        # we do not want it to create an empty dir
        if os.path.exists(ras2fim_huc_input_dir):
            __copy_file(
                os.path.join(ras2fim_huc_input_dir, os.environ["ras_rating_curve_csv_filename"]),
                src_input_root,
                trg_input_root,
            )
            __copy_file(
                os.path.join(ras2fim_huc_input_dir, os.environ["ras_rating_curve_gpkg_filename"]),
                src_input_root,
                trg_input_root,
            )

        logging.info("Downloading validation data (alpha test) files, if applicable")
        for org in validation_data_orgs:
            # For each HUC, most do not have any benchmark data and of those who do,
            #    most do not have all orgs (ble, usgs.. etc)

            huc_valication_path = f'{src_test_case_dir}{org}_test_cases/validation_data_{org}/{huc}'
            # src_folder_path = f'{src_root_dir}/{huc_valication_path}'
            if USE_S3:
                if s3_sf.does_s3_folder_exist(S3_CLIENT, S3_BUCKET_NAME, huc_valication_path):
                    __copy_folder(huc_valication_path, src_root_dir, output_root_folder)
            else:
                if os.path.exists(huc_valication_path):
                    __copy_folder(huc_valication_path, src_root_dir, output_root_folder)

    #   End of huc specific downloads

    # copy DEM VRTs  (yes.. after HUC sets)
    # We may not necesarily need vrts for everyone. ie) not all HUCs have bridges
    # we not have any AK or maybe AK and CONUS
    if dem_vrt_file_conus != "":
        __copy_file(dem_vrt_file_conus, src_input_root, trg_input_root)

    if dem_vrt_file_alaska != "":
        __copy_file(dem_vrt_file_alaska, src_input_root, trg_input_root)

    # Bridge dem diff vrts
    if bridge_dem_dif_vrt_file_conus != "":
        __copy_file(bridge_dem_dif_vrt_file_conus, src_input_root, trg_input_root)

    if bridge_dem_dif_vrt_file_alaska != "":
        __copy_file(bridge_dem_dif_vrt_file_conus, src_input_root, trg_input_root)

    logging.info("==========================================================")
    end_time = datetime.now(timezone.utc)
    logging.info("-- Completed getting sample data")
    logging.info(f"End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info(fh.print_date_time_duration(overall_start_time, end_time, False))


def __copy_file(src_file_path, src_root_path, trg_root_path):
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
    if not src_root_path.startswith("/"):
        src_root_path = "/" + src_root_path

    if not trg_root_path.startswith("/"):
        trg_root_path = "/" + trg_root_path

    if not src_root_path.endswith("/"):
        src_root_path = src_root_path + "/"

    if not trg_root_path.endswith("/"):
        trg_root_path = trg_root_path + "/"

    trg_file_path = src_file_path.replace(src_root_path, trg_root_path)

    if not trg_file_path.startswith("/"):
        trg_file_path = "/" + trg_file_path

    # Not automaticallly the same value as the trg_root_data_path
    trg_dir_path = os.path.dirname(trg_file_path) + "/"

    # will overwrite always
    if not os.path.exists(trg_dir_path):
        os.makedirs(trg_dir_path, exist_ok=True)

    if USE_S3:  # src is S3, not target
        logging.info(f"Downloading file: s3://{S3_BUCKET_NAME}/{src_file_path} to {trg_file_path}")

        did_file_exist = s3_sf.download_s3_file(S3_CLIENT, S3_BUCKET_NAME, src_file_path, trg_file_path)
        if not did_file_exist:
            logging.warning("... Skipping file copy, file does not exist in s3")
    else:  # source is local
        logging.info(f"Copying file: {src_file_path} to {trg_file_path}")
        if os.path.exists(src_file_path):
            shutil.copy2(src_file_path, trg_file_path)
        else:
            logging.warning("... Skipping file copy, file does not exist")


def __copy_folder(src_folder_path: str, src_root_path: str, trg_root_path: str):
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
    if not src_folder_path.startswith("/"):
        src_folder_path = "/" + src_folder_path

    if not src_root_path.startswith("/"):
        src_root_path = "/" + src_root_path

    if not trg_root_path.startswith("/"):
        trg_root_path = trg_root_path + "/"

    if not src_root_path.endswith("/"):
        src_root_path = src_root_path + "/"

    if not trg_root_path.endswith("/"):
        trg_root_path = trg_root_path + "/"

    trg_folder_path = src_folder_path.replace(src_root_path, trg_root_path)

    if USE_S3:  # for src only, not target
        logging.info(f"Downloading folder: s3://{S3_BUCKET_NAME}/{src_folder_path} to {trg_folder_path}")

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

    bucket_name, root_data_path = s3_sf.parse_bucket_and_folder_name(s3_path)

    test_case_path = ""

    input_path = root_data_path + "/inputs"
    test_case_path = root_data_path + "/test_cases"

    logging.info("Setting up S3 connection")

    # It is possible that the user might not use explicit keys, but implicit keys
    # such as the default credentials file. So do not test for keys
    # All errors are thrown as Exceptions
    is_success, return_code, globals()['S3_CLIENT'] = s3_sf.create_boto3_s3_client(
        aws_access_key_id, aws_secret_access_key, aws_region
    )
    if not is_success:
        raise Exception(
            "An error has occurred. Check arguments or aws credentials."
            f":  Details: {s3_sf.get_descriptive_error_msg(return_code)}"
        )

    # This can return a return_code of non 0 which means something failed
    # which can be that the bucket doesn't exist, folder does not exist,
    # authenication errors or various things
    does_folder_exist, return_code = s3_sf.does_s3_folder_exist(
        os.environ["S3_CLIENT"], bucket_name, input_path
    )
    if not does_folder_exist:
        # we want to handle this particular exception ourselves
        if return_code == 1051:  # Folder not found
            raise Exception(
                f"The S3 folder path of {input_path} does not exist."
                " Please check the spelling (case-sensitive) or pathing."
            )
        else:
            raise Exception(
                "An error has occurred: " f";  Details: {s3_sf.get_descriptive_error_msg(return_code)}."
            )

    does_testcase_folder_exist, rtn_code = s3_sf.does_s3_folder_exist(
        S3_CLIENT, S3_BUCKET_NAME, test_case_path
    )
    if not does_testcase_folder_exist:
        # Some exception auto pass through, but some we want to manage the messages
        # we can use the default AWS messages, or create out own.
        print(s3_sf.get_descriptive_error_msg(rtn_code))
        print("program aborted")
        sys.exit(1)

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

    # ie) From s3://some_bucket/foss_fim
    #  returns: 'some_bucket', 'foss_fim', 'foss_fim/inputs'
    return bucket_name, root_data_path, input_path, test_case_path


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
    parser.add_argument(
        '-i',
        '--src-data-path',
        help='Path to the source input and test_case data folders.'
        ' Please read the inline code by this argparses code to see more detailed'
        ' information on this data path usage.',
        required=True,
    )
    parser.add_argument(
        '-o',
        '--output-root-folder',
        help='Path to save the output data'
        ' Please read the inline code by this argparses code to see more detailed'
        ' information on the output data path usage.',
        required=True,
    )
    parser.add_argument('-s3', '--use-s3', action='store_true', help='Add flag if downloading data from S3')
    parser.add_argument('-ak', '--aws-access-key-id', help='AWS access key ID', required=False)
    parser.add_argument('-sk', '--aws-secret-access-key', help='AWS secret access key', required=False)
    parser.add_argument('-sr', '--aws-region', help='AWS region (ie. us-east-1)', required=False)

    args = parser.parse_args()

    get_sample_data(**vars(args))
