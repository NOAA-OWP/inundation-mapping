#!/usr/bin/env python3
 
import os
import argparse
import logging
import pickle
import shutil
import sys
import traceback

from datetime import datetime, timezone
from dotenv import load_dotenv

import geopandas as gpd
import pandas as pd

import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf

from src.utils.shared_variables import VIZ_PROJECTION
from tools_shared_functions import (
    filter_nwm_segments_by_stream_order,
    get_datum,
    get_nwm_segs,
    get_thresholds,
    ngvd_to_navd_ft,
)
from tools_shared_variables import (
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

    huc_path, output_folder = __validate_inputs(huc, output_folder)  # also validates some bash_variables if it needs any.

    wrds_api_base_url = csf.load_fim_global_env_values(os.getenv('ENV_FILE'))

    # TODO: With this huc being processed in MP and all HUCs sharing the same physical meta and threshold files, we 
    #   are going to have problems with race conditions. After we validate, make copies into the local HUC dir
    #   temporary so it can use them for processing.
    #   HUMM... google if we will have a race condition on copying the file.. I dont' think so but double check it

    try:

        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")
        
        # Validate that we have that as a HUC in the fim_dir. 
        # Helping sort out if even a valid HUC was submitted
        
        catfim_type_name = ""
        catfim_type=os.getenv('CATFIM_TYPE')
        if catfim_type == 'sb':
            catfim_type_name = "stage_based"
        else:
            catfim_type_name = "flow_based"

        # ---------------------
        # Setup logging. It should make its own huc log folder inside the parent "logs" folder
        log_file_dir = os.path.join(huc_path, f"{huc}_logs")
        log_file_path = sf.setup_file_logger(log_file_dir, f"process_huc")
        print(f"Logs for this HUC will be saved to {log_file_path}")

        print("")
        logging.info(f"Processing {catfim_type_name} catfim fim for HUC: {huc} ;  {dt_string} (UTC)")
        print("")
        
        output_mapping_dir = os.path.join(huc_path, "mapping")
                
        # FB uses a discharge_file but SB does not. Easiest to clean the folder completely up regardless of type.
        discharge_file_path, sites_file_path, library_file_path = __set_start_files_folders(huc_path, output_mapping_dir)
    

        # Change to new download_process_wrds.py file
        metadata_url = f'{wrds_api_base_url}/metadata'
        huc_dictionary, meta_gdf = csf.get_meta_and_huc_data(output_folder,
                                                             metadata_url,
                                                             os.getenv('SEARCH'),
                                                             os.getenv('NWM_METAFILE_PATH'),
                                                             os.getenv('GET_NEW_META_DATA'),
                                                             [huc])
        if len(huc_dictionary) == 0:
            msg = f"HUC number of {huc} is invalid or does not have any nwm sites associated to it."
            logging.critical(msg)
            raise Exception(msg)

        if len(huc_dictionary) > 1:
            # ie: {'12090301': ['BRTT2', 'CBST2', 'LGRT2', 'SMIT2']}
            msg = "An internal error has occurred. Expected 0 or 1 records back."
            logging.critical(msg)
            raise Exception(msg)

        # Make a simple list of just the site_ids
        # Note: Should not be any recs coming in from meta_gdf that have dup nws_lid values
        huc_nws_lids = huc_dictionary[huc]

        # It is ok to continue anyways as it will just log that it has none found.
        # Is this even possible at this point? doesn't really matter.
        logging.info(f"{len(huc_nws_lids)} sites found before validation: {huc_nws_lids}")
        
        # Start building up the new sites / meta file. We can adjust the status as we go.        
        meta_gdf = __setup_sites_gdf(meta_gdf, catfim_type)
        
        # validation of the valid_nwm_lids already be done, so neither of these variables should be empty.
        valid_nwm_lids, meta_gdf = __check_for_resticted_sites(meta_gdf, catfim_type, huc, sites_file_path)
        logging.info(f"{len(valid_nwm_lids)} sites remaining after validation: {valid_nwm_lids}")
        
        # Temp debugging
        print("--------------")
        print("Ok.. let's stop here for now")   
        __save_sites_file(meta_gdf, sites_file_path)
        # graceful exit is fine here. We don't need to crash it or through an exception.
        sys.exit(0)        
        
        # ---------------------
        # Get threshold data


        # ---------------------
        # threshold data and flow data, if applicable using shared various files. ?? Both need threshold but flow data
        # but flow data for FB comes from the HAND dataset, for SB it comes from WRDS?  check this..

        
        
        # ---------------------    
        # Various meta and threshold processing? including validation of data ?

        
        # ---------------------
        # ? When / how do the points get added to sites?
        
        # ---------------------    
        # Figure out categories. (ie.. action, moderate, etc) - SB to also figure out intervals?
        
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
        
        
        logging.info(f"End processing for huc {huc}")
        sf.print_andor_log_duration(overall_start_time, True, True, logging.getLogger())
        
        # nothing to return as of now
        # but generate_categorical_fim.py can if it has value to return.
        # if you use "return" and it is AWS, it will not error out.
        # yes.. we want a "return", but may/may not have a value. 
        # if we add one, keep it a simple data type (str, int, float)
        return
        
        
    except Exception:
        trace_error = traceback.format_exc()
        
        err_msg = f"A critical error has occurred while processing {huc}. Detail: {trace_error}"
        
        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)
        
        # do we re-throw the error? gcf, aws, or cmd line? hummm


# def __emilys_get_threshold_data(get_new_threshold_data,
#                               threshold_file_path,
#                               huc_lid_dict):

    
# ROB: This needs some cleanup
#     Processing:
#         If 
    
#     if get_new_threshold_data is False:
#         if threshold_file_path is None or threshold_file_path == "":
#             threshold_file_path = os.getenv("nwm_threshold_file")  # get from Bash_variables

#         if os.path.exists(threshold_file_path):
#             raise Exception....
#          
#         Load the provided or default bash_variables pickle file

        # if os.path.isfile(threshold_file_path):
        #     logging.info("Loading threshold file from {threshold_file_path}")
        #     with open(threshold_file_path, "rb") as p_handle:
        #         output_meta_list = pickle.load(p_handle)
        # else:
        #     raise Exception(f"threshold_file_path at {threshold_file_path} does not exist")
        
#     else:  # go get it from Emily  get_new_threshold_data == True:
#         threshold_url = f'{os.getenv("API_BASE_URL")}/nws_threshold'

#         label='catfim_{huc number?}' # TEMP (whatever)  (hummm... do I want huc name as mine will only ever have one huc at a time.)
#         label_with_date = dw.label_data_file(label, lst_hucs)
#         output_thresholds_filename = f'thresholds{label_with_date}.pkl'
#         threshold_file_path = os.path.join(output_catfim_dir, output_thresholds_filename)

        # current dev flow based get_thresholds
            # stages, flows, threshold_count = get_thresholds(
            #     threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
            # )

        # current dev stage based get_thresholds
            # thresholds, flows, threshold_count = get_thresholds(
            #    threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
            #)

#         # Download thresholds. Catfim will only need to get one HUC at a time.
#         thresholds, flows, threshold_count = dw.download_all_thresholds(threshold_file_path, threshold_url, huc_lid_dict)
#  
#    return thresholds, flows, threshold_count


# TODO: Nov 7, 2025: Emily will have a replacement for this, so it will go away
def __get_threshold_data(threshold_url,
                        threshold_file_path,
                        get_new_threshold_data,
                        huc):
    '''
    Runs for both stage and flow. Loads and filters NWM threshold


    NEEDS UPDATING          

    This function checks if a local threshold pickle file exists. If it does, the metadata is loaded from the file.
    Otherwise, it downloads metadata from the specified URL using two API calls: one for all forecast points and
    another for all points in OCONUS regions (HI, PR, AK). The results are combined, and duplicate or None-valued
    NWS LIDs are filtered out. The filtered metadata is then saved to a pickle file for future use.          
          
    Returns:
        thresholds, flows, threshold_count
          
    '''
    
    save_pickle_file = False
    if get_new_threshold_data is False:
        if not threshold_file_path:
            # get the bash_variables value
            threshold_file_path = os.getenv("nwm_threshold_file")
            save_pickle_file = True  # Save a copy if it came from bash_variables
            
        if not os.path.isfile(threshold_file_path):
            raise Exception(f"threshold file at {threshold_file_path} does not exist")

        # Read pickle file and get a list of unique HUCs
        with open(threshold_file_path, 'rb') as f:
            loaded_data = pickle.load(f)

        hucs = loaded_data['huc'].unique().tolist()
        threshold_hucs= [str(num).zfill(8) for num in hucs]

        # Get the source (since it might be Manual_Input)
        # data_source = loaded_data['source'].tolist()[0]
   
        if huc not in threshold_hucs:
            # TODO; do we want this as an exception? or just logging.error so the program can graceful
            # exist the MP.
            raise Exception(f"{huc}  is not present in the threshold file ({threshold_file_path}): ")
    else:
        # Get thresholds (stages) and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
        # Note: SB calls the variable of "threshold" here as "thresholds" but FB called it stages previously
        thresholds, flows, threshold_count = get_thresholds(
            threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
        )
 
# used by flow based only. hummm... likely not
# def get_flow_data():  use generate_categorical_fim_flows to get this.


def __setup_sites_gdf(meta_gdf, catfim_type):

    # Start building up the new sites / meta file. We can adjust the status as we go.
    
    # add new columns
    meta_gdf["mapped"] = "no"  # definately want to start with "yes" and change to "no" if/as required.
            
    # Allows us to change this along the way if we need too
    # and if the status is not been changed, then at the very end, we can change it to an empty
    # string (aka.. all went perfectly well)
    meta_gdf["status"] = "value not set"  # allows us to change this along the way if we need too
    
    # This is a temp column to help sort out errors versus warning when we
    # change mapped value to yes.    
    meta_gdf["warnings"] = ""
    
    # adjust and/or rename some columns
    # Note: Yes... we are renaming 'identifiers_nws_lid': 'nws_lid'.
    # At the very end, we will rename it to ahps_lid.
    # Maybe we fix it someday, but not now. Too many other things going on.
    meta_gdf.rename(
        columns={
            'identifiers_nwm_feature_id': 'nwm_seg',
            'identifiers_nws_lid': 'nws_lid',
            'identifiers_usgs_site_code': 'usgs_gage',
        },
        inplace=True,
    )
    meta_gdf['nws_lid'] = meta_gdf['nws_lid'].str.lower()
    
    # Drop list fields if invalid
    # downstream_nwm_features and upstream_nwm_features are lists and gpkg does not like it
    meta_gdf = meta_gdf.drop(['downstream_nwm_features'], axis=1, errors='ignore')
    meta_gdf = meta_gdf.drop(['upstream_nwm_features'], axis=1, errors='ignore')
    meta_gdf = meta_gdf.astype({'metadata_sources': str})
    
    # NOTE: if you get errors saying: Skipping field because of invalid value:
    # There are a couple of possible reasons. Data type mismatch, None in a float/int column and the most
    # common is a list object in a meta gdf field. To fix it, generaally just make it a string or drop it.
    # We have both above.
    # Nov 6, 2025: We have appx 15 fields that fail but not on all recs. Let's try to change all columns to string
    # and see if that helps.
    
    # Convert all non-geometry columns to string
    for col in meta_gdf.columns:
        if col != meta_gdf.geometry.name:  # Exclude the geometry column
            meta_gdf[col] = meta_gdf[col].astype(str)
            meta_gdf[col].fillna('', inplace=True)
    
    # Some SB specific columns we want to create now and populate later.
    if catfim_type == 'sb':
        meta_gdf['acceptable_coord_acc_code_list'] = ""
        meta_gdf['acceptable_coord_method_code_list'] = ""
        meta_gdf['acceptable_alt_acc_thresh'] = 0.0
        meta_gdf['acceptable_alt_meth_code_list'] = ""
        meta_gdf['acceptable_site_type_list'] = ""
        
    viz_out_gdf = meta_gdf.to_crs(VIZ_PROJECTION)
        
    return viz_out_gdf


def __check_for_resticted_sites(meta_gdf, catfim_type, huc, sites_file_path):
    # ---------------------
    # Get list of applicable sites, valid sites for this HUCs from master sites metadata
    #   Watching for excluded sites from restricted sites csv.
    df_restricted_sites = csf.load_restricted_sites(catfim_type)

    # Update some of the meta.gdf records if they are in df_restricted_sites
    # Check whether the LIDs is in the restricted sites list
    # meta_gdf is likely pretty small by now, only sites for this HUC
    # Likely a smarter way to do this as well.. lambda? Could do a join but we have
    # dup column names we would have to cleanup.
    valid_nwm_lids = []
    for index, row in meta_gdf.iterrows():
        lid = row["nws_lid"]
        is_restrict_lid = df_restricted_sites.loc[df_restricted_sites['nws_lid'] == lid.upper()]
        if len(is_restrict_lid) > 0:
            # what if it comes back with more than one? if so.. it is a bug in the list
            meta_gdf.at[index, "status"] = is_restrict_lid.iloc[0]['restricted_reason']
            meta_gdf.at[index, "mapped"] = "no"
        else:
            valid_nwm_lids.append(lid)

    # Save the meta file we have with the new error messages, then abort.
    
    if len(valid_nwm_lids) == 0:
        msg = f"All sites associated to HUC {huc} are retricted. No more processing will continue. Aborting."
        logging.critical(msg)
        __save_sites_file(meta_gdf, sites_file_path)
        # graceful exit is fine here. We don't need to crash it or through an exception.
        sys.exit(0)

    return valid_nwm_lids, meta_gdf
    

def __save_sites_file(meta_gdf, sites_file_path):
    
    logging.info(f"Saving sites file to {sites_file_path} and a csv version as well.")

    # Do not attempt to mess with any columns at this point, as we may choose to save the file at
    # multiple points through the process before all status are completed.    
    
    meta_gdf.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)
    meta_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", encoding="utf-8")

    # Save a csv version as well
    nws_lid_csv_file_path = sites_file_path.replace(".gpkg", ".csv")
    meta_gdf.to_csv(nws_lid_csv_file_path)


def __validate_inputs(huc, output_folder):

    
    # TODO: valdiate huc value (8 numeric maybe and starts with 0, 1, or 2) ???? 
    
    if not output_folder:  # exists or empty
        raise ValueError("output_folder argument can not be None or empty.")
    if output_folder.endswith("/"):  # strip it off the end
        output_folder = output_folder[:-1]

    # does it already have the subfolder of "hucs"? strip it for now temporarily
    if output_folder.endswith("hucs"):
        output_folder = output_folder[:-4]

    if not os.path.exists(output_folder):  # the hucs subfolder may/may not exist but the root output folder must
        raise ValueError(f"output_folder of {output_folder} does not exist."
        " Please check pathing including case.")
    
    # Validate data exists in the fim_run_dir and it includes this HUC.
    # This may seem reduntant as it was checked (sort of) in generate_categorical_fim.py.
    # However, this one is HUC specific and it is possible that a HUC
    # can be run after generate_categorical_fim.py was run and add more HUC on the fly.
    # If this HUC did not previous exist or failed, you can re-run this script independantly
    # then run post processing again.
    fim_run_dir = os.getenv("FIM_RUN_DIR")
    if not fim_run_dir:
        raise Exception("The enviro value for FIM_RUN_DIR does not exist or is empty. It was loaded"
                        " and included in the runtime_arg enviro file. Check pathing and variables.")
    fim_run_huc_path = os.path.join(fim_run_dir, huc)
    if not os.path.exists(fim_run_huc_path):
        raise ValueError("This script needs to talk to its HUC in the fim_run_dir, but the folder of"
                         f" {fim_run_huc_path} does not exist. Please check pathing (with case).")
    
    # do we validate other key files? branches exist? what if it was a bad huc in the first place?
    
    # TODO: Validate key bash_variable values? path the meta adn threshold files?  Better yet, Emily's tool shoudl do that when we call her things
    
    # No need to validate any of the runtime_args as they were validated when it was created. (likely)

    # ie: /data/catfim/hand_4_8_7_2_stage_based/hucs/12090301
    huc_path = os.path.join(output_folder, "hucs", huc)
    os.makedirs(huc_path, exist_ok=True)

    return huc_path, output_folder


def __load_runtime_args(output_folder):
    
    '''
    Variables loaded (example)
        CATFIM_TYPE=fb
        ENV_FILE="/data/config/fim_enviro_values.env"
        SEARCH=5
        NWM_METAFILE_PATH=""
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
   
        
def __set_start_files_folders(huc_path, output_mapping_dir):

    '''
    Notes: We no longer need an "attributes" folder or a csv per lid in it. 
    
    '''

    # Note: all key other variables have already been validated

    # ================================
    # CLEANUP
    # Remove all files / folders except anything in the log folder, we keep that one only.
    # remove discharge_file_name, sites_file_name, libary_file_name if they already exist
    
    # FB may have this file, but SB won't. We will clean it up either way.
    discharge_file_path = os.path.join(huc_path, "discharge_values.csv")
    if os.path.isfile(discharge_file_path):
        os.remove(discharge_file_path)

    sites_file_path = os.path.join(huc_path, "sites.gpkg")
    if os.path.isfile(sites_file_path):
        os.remove(sites_file_path)

    library_file_path = os.path.join(huc_path, "library.gpkg")
    if os.path.isfile(library_file_path):
        os.remove(library_file_path)

    # Already exists? remove it, it will have gpkg's and tif's for this HUC in it.
    if os.path.exists(output_mapping_dir): 
        shutil.rmtree(output_mapping_dir, ignore_errors=True)
    os.mkdir(output_mapping_dir)

    # Always keeps the logs folder
    
    return discharge_file_path, sites_file_path, library_file_path


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
