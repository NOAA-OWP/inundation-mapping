#!/usr/bin/env python3

import argparse
import logging
import os
import pickle
import random
import shutil
import sys
import time
import traceback
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

import data.wrds.download_process_wrds as dpw
import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf
import tools.catfim.generate_categorical_fim_flows as gcf
from src.utils.shared_variables import VIZ_PROJECTION
from tools.tools_shared_functions import (
    aggregate_wbd_hucs,
    filter_nwm_segments_by_stream_order,
    get_datum,
    get_nwm_segs,
    get_thresholds,
    ngvd_to_navd_ft,
)
from tools.tools_shared_variables import (
    acceptable_alt_acc_thresh,
    acceptable_alt_meth_code_list,
    acceptable_coord_acc_code_list,
    acceptable_coord_method_code_list,
    acceptable_site_type_list,
)


gpd.options.io_engine = "pyogrio"

"""_summary_

    A sample model HUC folder can be found at /....(data)/catfim/rob_tests/new_arc_test1_flow_based
      - it has a single sites files which combines all of the attribute files for each site into its own huc file
        updateing it as it is being processed.
      - may / may not have one HUC level master or split level threashold / discharge data ??

      - It also can be using the mapped / status colums as it goes

      - When the huc is finished be processed, it's output files sit ready for post processing to merge with the rest of the huc files.

    Overall processing steps (tenatively)

    Will call generate_categorical_fim_flows and generate_categorical_fim when applicable.


    
    7: Various meta and threshold processing? including validation of data ?

    8: Figure out stages and if SB also figure out stages.

    9: Data adjustments or rejections ? (might be higher or even need more here)

    10: If FB, Load branch and HAND data? (rems and hydrotables), liekly all done via inundation scripts

    11: Create inundation tifs if applicable and roll them up if branch tifs?
        FB: Call inundation.py ?
        SB: Do our own inundation like we currently do?
    
    12: make extent polys

    13: Finalize any data

    14: Make final library files for this HUC

"""


def process_huc(huc, output_folder):
    """_summary_

    Notes:
        - When we iterate from the huc list from generate_categorical_fim.py, we always overwrite all HUC folders.
          The overwrite flag here is really just for testing that script by itself at command level.


    Raises:
        Exception: _description_
        Exception: _description_
    """

    is_logging_loaded = False

    # load our standard bash_variables.env
    # we do need some args later such as input_wbd_layer and likely others
    load_dotenv('/foss_fim/src/bash_variables.env')

    # ---------------------
    # load the runtime_args.env, error if it does not exist. It should give us all values we need
    # See generate_categorical_fim.py -> save_env_args(output_path)
    # We will also do some validation in it as well.

    print("================================")
    print(f"Starting process_huc for {huc}")
    print("")

    __load_runtime_args(output_folder)

    huc_path, output_folder = __validate_inputs(
        huc, output_folder
    )  # also validates some bash_variables if it needs any.

    try:

        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

        # Validate that we have that as a HUC in the fim_dir.
        # Helping sort out if even a valid HUC was submitted

        catfim_type_name = ""
        catfim_type = os.getenv('CATFIM_TYPE')
        if catfim_type == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        # ---------------------
        # Setup logging. It should make its own huc log folder inside the parent "logs" folder

        # TODO: Why are my logs read only for all but the owner? other apps don't I think.
        # I can not delete them to cleanup if I want too. huh? Better check other apps that use setup_file_logger
        log_file_dir = os.path.join(huc_path, "logs")
        log_file_path = sf.setup_file_logger(log_file_dir, f"process_huc_{huc}")

        print("")
        logging.info(f"Processing {catfim_type_name} catfim fim for HUC: {huc} ;  {dt_string} (UTC)")
        print("")
        print(f"... Logs for this HUC will be saved to {log_file_path}")

        # Cleaning up some previous files and folders for new runs. By being specific we can keep debugging files
        # from previous runs.
        # Some of these are only for stage and some ony for stage, but we will keep it as one just for simplicity
        output_mapping_dir = os.path.join(huc_path, "mapping")
        flows_data_dir = os.path.join(huc_path, "flow_data")
        sites_file_path = os.path.join(huc_path, f"{huc}_sites.gpkg")
        library_file_path = os.path.join(huc_path, f"{huc}_library.gpkg")
        discharge_file_path = os.path.join(huc_path, "fb_discharge_values.csv")

        __set_start_files_folders(
            catfim_type,
            output_mapping_dir,
            flows_data_dir,
            sites_file_path,
            library_file_path,
            discharge_file_path
        )

        # =========================================
        # Let's get the meta and points
        section_start_dt = datetime.now(timezone.utc)

        logging.info("loading sites meta data")
        # These are filtered to huc level
        metadata_json, sites_gdf = csf.get_metadata(huc, huc_path, output_folder)

        # Lets write what we have raw from meta data
        sites_gdf = __setup_sites_gdf(sites_gdf, os.getenv('CATFIM_TYPE'))

        # Now compare that huc_dictionary to restricted sites
        valid_nwm_lids, sites_gdf = __check_for_resticted_sites(
            sites_gdf, os.getenv('CATFIM_TYPE'), huc, sites_file_path
        )
        # lets save the sites gpkg we are at this point
        logging.info(f"Saving sites, pre flow and mapping, at {sites_file_path}")
        sites_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", encoding="utf-8")

        logging.info(f"{len(valid_nwm_lids)} sites remaining after validation: {valid_nwm_lids}")
        print("")


        # =========================================
        # Let's get the Threshold data
        section_start_dt = datetime.now(timezone.utc)
        logging.info("loading flow and threshold data for all valid sites")

        # ---------------------
        # Get threshold data
        # Note: it is possible get_threshold_data can come back empty if huc has no site(s) threshold data
        # It has stages and flows for all sites in this huc
         #stages_dict, flows_dict = gcf.get_threshold_data(huc, huc_path, valid_nwm_lids)
        # yes.. it is ok that thresholds_merged_df is empty for now
        thresholds_merged_df = gcf.get_threshold_data(huc, huc_path, valid_nwm_lids)
        # thresholds_stages_df, thresholds_flows_df = gcf.get_threshold_data(huc, huc_path, valid_nwm_lids)

        # =========================================
        # Processing Threshold data  (figure stages and calc flow data for inundation)
        section_start_dt = datetime.now(timezone.utc)
        logging.info("Processing flow and threshold data for all valid sites")


        # Dec 22, 2025: Emily.. stop here for now. :)
        print("--------------")
        print("Ok.. let's stop here for now")
        sys.exit(0)



        # we do have at least one threshold record
        # library is not yet a gdf as it has no geometry
        sites_gdf, library_df = gcf.process_theshold_data(catfim_type,
                                                          valid_nwm_lids,
                                                          sites_gdf,
                                                          huc,
                                                          huc_path,
                                                          thresholds_merged_df,
                                                          metadata_json)

        logging.info(f"End processing flow and threshold data for huc {huc}")
        duration_msg = sf.calculate_duration_msg(section_start_dt)
        logging.info(duration_msg)


        # # Temp debugging
        # print("--------------")
        # print("Ok.. let's stop here for now")
        # sys.exit(0)

        # ---------------------
        # Data adjustments or rejections ? (might be higher or even need more here)

        # ---------------------
        # If FB, Load branch and HAND data? (rems and hydrotables), liekly all done via inundation scripts

        # ---------------------
        # Create inundation tifs if applicable and roll them up if branch tifs?
        #    FB: Call inundation.py ?
        #     SB: Do our own inundation like we currently do?

        # ---------------------
        # Make extent polys

        # ---------------------
        # Finalize any data

        # ---------------------
        # Make final library files for this HUC

        logging.info(f"Updating sites gdf with finalized site data at {sites_file_path}")
        # For the sites gpkg, leave the column named as nws_lid, we can change it to ahps_lid later in post processing.
        # hummmmm
        sites_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", encoding="utf-8")

        logging.info(f"End processing for huc {huc}")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)

        # nothing to return as of now
        # but generate_categorical_fim.py can if it has value to return.
        # if you use "return" and it is AWS, it will not error out.
        # yes.. we want a "return", but may/may not have a value.
        # if we add one, keep it a simple data type (str, int, float)
        return huc

    except Exception:
        trace_error = traceback.format_exc()

        err_msg = f"A critical error has occurred while processing {huc}. Detail: {trace_error}"
        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # do we re-throw the error? gcf, aws, or cmd line? hummm


def __setup_sites_gdf(sites_gdf, catfim_type):

    # Start building up the new sites / meta file. We can adjust the status as we go.

    # sites_gdf = gpd.GeoDataFrame()

    # move it to the first column (easier to read the outputs)
    ahps_col = sites_gdf.pop('nws_lid')
    sites_gdf.insert(0, 'nws_lid', ahps_col)

    # move the huc column as well, but we do need to keep it
    huc_col = sites_gdf.pop('HUC8')
    sites_gdf.insert(1, 'HUC8', huc_col)

    # add new columns
    sites_gdf.insert(loc=2, column="mapped", value="not set")
    sites_gdf.insert(loc=3, column="status", value="not set")
    sites_gdf.insert(loc=4, column="warnings", value="")

    # adjust and/or rename some columns
    # Note: Yes... we are renaming 'identifiers_nws_lid': 'nws_lid'.
    # At the very end, we will rename it to ahps_lid.
    # Maybe we fix it someday, but not now. Too many other things going on.
    sites_gdf.rename(
        columns={'identifiers_nwm_feature_id': 'nwm_seg', 'identifiers_usgs_site_code': 'usgs_gage'},
        inplace=True
    )

    # Drop list fields if invalid
    # downstream_nwm_features and upstream_nwm_features are lists and gpkg does not like it
    # hummm... or maybe jsut when it is null? both of those nodes are 

    # it is because it has a lists value or unkeyed json node. How do we fix this?  TBD. Check other code
    # note in this file.
    sites_gdf = sites_gdf.drop(['downstream_nwm_features', 'upstream_nwm_features'], axis=1, errors='ignore')
    sites_gdf = sites_gdf.astype({'metadata_sources': str,
                                    'nwm_feature_data_downstream_feature_id': str,
                                    'nws_data_county_code': str,
                                    'nwm_feature_data_nhd_waterbody_comid': str,
                                    'nws_data_latitude': str,
                                    'nws_data_longitude': str,
                                    'nws_data_zero_datum': str,
                                    'nwm_feature_data_stream_order': str,
                                    })

    sites_gdf.reset_index(inplace=True)


    # NOTE: if you get errors saying: Skipping field because of invalid value:
    # There are a couple of possible reasons. Data type mismatch, None in a float/int column and the most
    # common is a list object in a meta gdf field. To fix it, generaally just make it a string or drop it.
    # We have both above.
    # Nov 6, 2025: We have appx 15 fields that fail but not on all recs. Let's try to change all columns to string
    # and see if that helps.

    # We need a better answer here as we do want some columns to non string

    # Dec 4, 2025, we may no longer need this. We saw the problem with 12090301, failing saying invalid key
    # Dec 10, 2025 - ya.. we do still need this but why now?
    # # Convert all non-geometry columns to string
    # for col in sites_gdf.columns:
    #     if col != sites_gdf.geometry.name:  # Exclude the geometry column
    #         sites_gdf[col] = sites_gdf[col].astype(str)
    #         sites_gdf[col].fillna('', inplace=True)

    # Some SB specific columns we want to create now and populate later.
    if catfim_type == 'sb':
        sites_gdf['acceptable_coord_acc_code_list'] = ""
        sites_gdf['acceptable_coord_method_code_list'] = ""
        sites_gdf['acceptable_alt_acc_thresh'] = 0.0
        sites_gdf['acceptable_alt_meth_code_list'] = ""
        sites_gdf['acceptable_site_type_list'] = ""

    return sites_gdf


def __check_for_resticted_sites(sites_gdf, catfim_type, huc, sites_file_path):
    # ---------------------
    # Get list of applicable sites, valid sites for this HUCs from master sites metadata
    #   Watching for excluded sites from restricted sites csv.
    df_restricted_sites = __load_restricted_sites(catfim_type)

    # Update some of the meta.gdf records if they are in df_restricted_sites
    # Check whether the LIDs is in the restricted sites list
    # meta_gdf is likely pretty small by now, only sites for this HUC
    # Likely a smarter way to do this as well.. lambda? Could do a join but we have
    # dup column names we would have to cleanup.
    valid_nwm_lids = []
    for index, row in sites_gdf.iterrows():
        lid = row["nws_lid"].upper()
        is_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]
        if len(is_restrict_lid) > 0:
            # what if it comes back with more than one? if so.. it is a bug in the list
            sites_gdf.at[index, "status"] = is_restrict_lid.iloc[0]['restricted_reason']
            sites_gdf.at[index, "mapped"] = "no"
        else:
            valid_nwm_lids.append(lid)

    # Save the meta file we have with the new error messages, then abort.
    if len(valid_nwm_lids) == 0:
        msg = f"All sites associated to HUC {huc} are retricted. No more processing will continue. Aborting."
        logging.critical(msg)
        
        sites_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", encoding="utf-8")
        # graceful exit is fine here. We don't need to crash it or through an exception.
        # sys.exit(0)  # humm.. or do we let this throw the exception for MP?
        raise Exception(msg)

    return valid_nwm_lids, sites_gdf


def __load_restricted_sites(catfim_type):
    """
    Previously, only stage based used this. It is now being used by stage-based and flow-based (1/24/25)

    The 'catfim_type' column can have three different values: 'stage', 'flow', and 'both'. This determines
    whether the site should be filtered out for stage-based CatFIM, flow-based CatFIM, or both of them.

    Returns: a dataframe for the restricted lid and the reason why:
        'nws_lid', 'restricted_reason'
    """

    file_name = "ahps_restricted_sites.csv"
    current_script_folder = os.path.dirname(__file__)
    file_path = os.path.join(current_script_folder, file_name)

    df_restricted_sites = pd.read_csv(file_path, dtype=str)

    df_restricted_sites['nws_lid'].fillna("", inplace=True)
    df_restricted_sites['restricted_reason'].fillna("", inplace=True)
    df_restricted_sites['catfim_type'].fillna("", inplace=True)

    # remove extra empty spaces on either side of all cellls
    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.strip()
    df_restricted_sites['restricted_reason'] = df_restricted_sites['restricted_reason'].str.strip()
    df_restricted_sites['catfim_type'] = df_restricted_sites['catfim_type'].str.strip()

    # Need to drop the comment lines before doing any more processing
    df_restricted_sites.drop(
        df_restricted_sites[df_restricted_sites.nws_lid.str.startswith("#")].index, inplace=True
    )

    # Filter df_restricted_sites by CatFIM type
    if catfim_type == 'sb':  # Keep rows where 'catfim_type' is either 'stage' or 'both'
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['stage', 'both'])]

    else:
        df_restricted_sites = df_restricted_sites[df_restricted_sites['catfim_type'].isin(['flow', 'both'])]

    df_restricted_sites['nws_lid'] = df_restricted_sites['nws_lid'].str.upper()

    # Clean up dataframe
    for ind, row in df_restricted_sites.iterrows():
        nws_lid = row['nws_lid']
        restricted_reason = row['restricted_reason']

        # if len(nws_lid) != 5:
        #     logging.warning(f"This lid value of '{nws_lid}' is invalid.")
        if restricted_reason == "":
            restricted_reason = "From the ahps_restricted_sites,"
            " the site will not be mapped, but a reason has not be provided."
            df_restricted_sites.at[ind, 'restricted_reason'] = "Restricted Site - " + restricted_reason

            # FLOG.warning(f"{restricted_reason}. Lid is '{nws_lid}'")
            # Humm.. how do we log this? screen is ok, but log isn't (MP versus non MP)
            # can we try just using the "logging" instance? Let's try it and see what happens
            logging.warning(f"{restricted_reason}. Lid is '{nws_lid}'")

        continue
    # end loop

    # Remove catfim_type column
    df_restricted_sites = df_restricted_sites.drop('catfim_type', axis=1)

    return df_restricted_sites


def __validate_inputs(huc, output_folder):

    # This validates some inputs but also copies key files around.

    # TODO: valdiate huc value (8 numeric maybe and starts with 0, 1, or 2) ????

    if not output_folder or output_folder == "":
        raise ValueError("output_folder argument can not be None or empty.")
    if output_folder.endswith("/"):  # strip it off the end
        output_folder = output_folder[:-1]

    # does it already have the subfolder of "hucs"? strip it for now temporarily
    if output_folder.endswith("hucs"):
        output_folder = output_folder[:-4]

    if not os.path.exists(
        output_folder
    ):  # the hucs subfolder may/may not exist but the root output folder must
        raise ValueError(
            f"output_folder of {output_folder} does not exist." " Please check pathing including case."
        )

    # Validate data exists in the fim_run_dir and it includes this HUC.
    # This may seem reduntant as it was checked (sort of) in generate_categorical_fim.py.
    # However, this one is HUC specific and it is possible that a HUC
    # can be run after generate_categorical_fim.py was run and add more HUC on the fly.
    # If this HUC did not previous exist or failed, you can re-run this script independantly
    # then run post processing again.
    fim_run_dir = os.getenv("FIM_RUN_DIR")
    if not fim_run_dir:
        raise ValueError(
            "The enviro value for FIM_RUN_DIR does not exist or is empty. It was loaded"
            " and included in the runtime_arg enviro file. Check pathing and variables."
        )
    fim_run_huc_path = os.path.join(fim_run_dir, huc)
    if not os.path.exists(fim_run_huc_path):
        raise FileNotFoundError(
            "This script needs to talk to its HUC in the fim_run_dir, but the folder"
            f" {fim_run_huc_path} does not exist. Please check pathing (with case)."
        )

    # branch_dir = os.path.join(fim_run_huc_path, 'branches')
    # if not os.path.exists(branch_dir):
    #     raise FileNotFoundError(
    #         "This script needs to talk to branches in its fim_run_dir / HUC in the fim_run_dir,"
    #         f"but the folder " {branch_dir} does not exist. Please check pathing (with case)."
    #     )


    # do we validate other key files? branches exist? what if it was a bad huc in the first place?

    # TODO: Validate key bash_variable values? path the meta adn threshold files?  Better yet, Emily's tool shoudl do that when we call her things

    # No need to validate any of the runtime_args as they were validated when it was created. (likely)

    # ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301
    huc_path = os.path.join(output_folder, "hucs", huc)
    os.makedirs(huc_path, exist_ok=True)
    os.chmod(huc_path, 0o777)

    return huc_path, output_folder


def __load_runtime_args(output_folder):
    '''
    Variables loaded (example)
        CATFIM_TYPE=fb
        ENV_FILE="/data/config/fim_enviro_values.env"
        SEARCH=5
        NWM_METAFILE_PATH=""
        NWM_SITES_PATH=""
        GET_NEW_META_DATA=False
        THRESHOLD_FILE_PATH=""
        GET_NEW_THRESHOLD_DATA=False
        FIM_RUN_DIR="/data/previous_fim/hand_4_8_7_2"
        PAST_MAJOR_INTERVAL_CAP=5
    '''

    args_file_name = "runtime_args.env"
    args_file = os.path.join(output_folder, args_file_name)

    if not os.path.exists(args_file):
        raise ValueError(f"Unable to find the runtime_args.env at {output_folder}")

    # use load_env, and pull out just the variables it needs.
    load_dotenv(args_file)

    # Let's change GET_NEW_META_DATA and GET_NEW_THRESHOLD_DATA to true booleans


def __set_start_files_folders(
    catfim_type,
    output_mapping_dir,
    flows_data_dir,
    sites_file_path,
    library_file_path,
    # huc_threshold_data_file_path,
    discharge_file_path,
):

    # Notes: We no longer need an "attributes" folder or a csv per lid in it.

    # ================================
    # CLEANUP
    # we specifically cleanup specific files and folders in case the developer wants to keep
    # some other previous test files for secondary runs.

    # This does completely remove and cleanup the mapping folder.

    # Already exists? remove it, it will have gpkg's and tif's for this HUC in it.
    shutil.rmtree(output_mapping_dir, ignore_errors=True)
    os.mkdir(output_mapping_dir)

    if os.path.isfile(sites_file_path):
        os.remove(sites_file_path)

    if os.path.isfile(library_file_path):
        os.remove(library_file_path)

    # TODO: Come back and readd this (huc level one only, not the WRDS or newly downloaded WRDS version)
    # if os.path.isfile(huc_threshold_data_file_path):
    #     os.remove(huc_threshold_data_file_path)

    if catfim_type == 'fb':
        if os.path.isfile(discharge_file_path):
            os.remove(discharge_file_path)

        shutil.rmtree(flows_data_dir, ignore_errors=True)
        # os.mkdir(flows_data_dir)   # add when we need it in fb
    # else:

    # TODO: Always keeps the logs folder and maybe nothing else?
    # certainly not meta or threshold files.

    return  # It is ok to return nothing to help show where a function finishes


if __name__ == '__main__':

    '''
    Sample
    python /foss_fim/tools/catfim/catfim_process_huc.py -u 12090301 -t /data/catfim/hand_4_8_7_2
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM for a HUC')

    # Most args will be in the runtime_arg.env created in the generate_categorical_fim.py
    # This script will already know where to look for the runtime_args.env file

    # We need only the huc number and the output path for args

    # Note: We always want to overwrite.

    parser.add_argument("-u", "--huc", help="REQUIRED: HUC8 Number", required=True, type=str)

    parser.add_argument(
        '-t',
        '--output-folder',
        help='REQUIRED: Target location, Where the output folder will be.'
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )

    args = vars(parser.parse_args())

    process_huc(**args)
