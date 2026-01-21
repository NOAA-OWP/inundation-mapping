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

import catfim.generate_categorical_fim_flows as gcf
import catfim.generate_categorical_fim_mapping as gcfm
import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv

import data.wrds.download_process_wrds as dpw
import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf
from src.utils.shared_variables import VIZ_PROJECTION
from tools.tools_shared_functions import get_datum, ngvd_to_navd_ft
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
        updating it as it is being processed.
      - may / may not have one HUC level master or split level threshold / discharge data ??

      - The sites_gdf, is updated and passed through most code, updating the mapped and status column as it goes
        based on issues found. It also has temp warnings column for things like "missing action stage", which existed
        in prior versions. At the end, it will be rolled into status column

      - When the huc is finished be processed, it's output files sit ready for post processing to merge with the rest of the huc files.

"""

# TODO: Dec 2025: For all of the possible error messages that could be used with the message text changing
# per version, I wonder if we should add a "status_code" column to the sites.gdf, so catfim compare can
# compare codes and not text values. Most of the status messages have been preserved, but likely some changes.
# Even if we add status_codes now, it won't really have its true value until two version down the road
# as the current 4.8.7.2 does not have that column unless we build a temp retro fit to those files.


def process_huc(huc, output_folder):
    """
    Wrapper for all HUC-specific CatFIM processing.

    Steps:

    - Setup for HUC processing, creates HUC folders if they do not exist
    - Loads site metadata and validates sites
    - Retrieves and processes threshold data if valid sites remain for HUC
    - Processes threshold data (creates a HUC-level library file with all of 
        the site/magnitude combinations that are still valid up to this point
        and creates the flow data for FB)
    - Runs CatFIM mapping


    Notes:

        TODO: do we want an overwrite system?

        TODO: is this what we want for nws_lid and ahps_lid ?? HUMMM
        All processing throughout the code will use the column / object names of nws_lid (sometimes just lid)
        and it will always be upper case.
        At the finalization, we will change all output files (sites and library) from nws_lid to
        ahps_lid and change it to lower case.


    Raises:

        TODO: what do we want to do with exceptions.. see notes at the bottom of this function.
        Exception: _description_ (any? depends on how we want to handle exceptions for both AWS and
            generate_categorial_fim.py HUC iterator.)
    """

    # ---------------------
    # Setup for HUC processing

    is_logging_loaded = False

    # Load the standard bash_variables.env
    # Note: we do need some args later such as input_wbd_layer and likely others
    load_dotenv('/foss_fim/src/bash_variables.env')

    # Load the runtime_args.env, error if it does not exist. 
    # It should give us all values we need. We will also do some validation in it as well.
    # See generate_categorical_fim.py -> save_env_args(output_path)
    csf.load_runtime_args(output_folder)

    print("================================")
    print(f"Starting process_huc for {huc}")
    print("")

    # Validate input parameters and bash variables and return cleaned-up paths
    # Helping sort out if even a valid HUC was submitted (and that that we have
    # that as a HUC in the fim_dir)
    huc_path, output_folder = csf.validate_inputs(
        huc, output_folder
    )

    # Create the huc folder if it does not exist
    os.makedirs(huc_path, exist_ok=True, mode=0o777)

    try:
        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

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
        is_logging_loaded = True # TODO: is this correct?

        print("")
        logging.info(f"Processing {catfim_type_name} CatFIM for HUC: {huc} ;  {dt_string} (UTC)")
        print("")
        print(f"... Logs for this HUC will be saved to {log_file_path}")


        # Notes on cleaning up previous files: 
        # - Cleaning up some previous files and folders for new runs. By being specific we can keep debugging files
        #   from previous runs.
        #
        # - We will want to make sure we have a pretty robust cleanup process, because we wouldn't want to 
        #   accidentally leave behind site or library files and accidentally merge them into the final outputs. -E
        #
        # - Some of folders are only for stage and some only for flow, but we will keep it as one process just for simplicity
        # - Note: later.. catfim post processing will look for only files starting with a huc number and
        #    either _sites.gpkg or _library.gpkg

        # Create mapping and temp folders if they do not exist
        output_mapping_dir = os.path.join(huc_path, "mapping")
        output_temp_dir = os.path.join(huc_path, "temp")

        # Notes on file naming conventions: 
        # - Not all intermediates will go to temp. It depends if we want to keep it long term or not.
        # - Make sure that only the final edition of the product ends in _sites.gpkg or _library.gpkg
        #   as catfim_post_processing.py will look for those conventions.
        #   Don't let any intermediates folow that convention exactly in this root huc dir.
        #   Always add something after _sites and _library

        # Yes.. Some of these variables are duplicated in gen..mapping.py to keep it independent

        # Create file path variables
        sites_file_path = os.path.join(huc_path, f"{huc}_sites.gpkg")
        sites_file_post_mapping_path = os.path.join(output_mapping_dir, f"sites_post_mapping.gpkg")
        library_file_path = os.path.join(huc_path, f"{huc}_library.gpkg")
        library_pre_inun_file_path = os.path.join(output_temp_dir, "library_pre_inundation.csv")
        library_post_mapping_file_path = os.path.join(output_temp_dir, "library_post_mapping.gpkg")

        # TODO: change the cleanup to some grep getting all files, then compare
        # to a list of files to cleanup. Some other py files create intermediate files.

        # Remove some preexisting files and folders from prior runs
        __set_start_files_folders(output_mapping_dir, output_temp_dir, sites_file_path, library_file_path)

        # =========================================
        # Load site metadata and validate sites
        section_start_dt = datetime.now(timezone.utc)
        continue_processing = True

        # Jan 2026: This was in previous code but not used # TODO: Clean up
        # fim_inputs_csv_path = os.path.join(fim_run_dir, 'fim_inputs.csv')
        # if not os.path.exists(fim_inputs_csv_path):
        #     raise ValueError(f"{fim_inputs_csv_path} not found. Verify that you have the correct input files.")

        logging.info("Loading sites metadata...")

        # Get HUC-level metadata and save it as a GeoDataFrame
        metadata_json, sites_gdf = csf.get_metadata(huc, huc_path, output_folder)
        sites_gdf = __setup_sites_gdf(sites_gdf, os.getenv('CATFIM_TYPE'))

        # Check for restricted sites and updates the sites GeoDataFrame accordingly
        valid_nwm_lids, sites_gdf = csf.check_for_restricted_sites(sites_gdf, os.getenv('CATFIM_TYPE'))
        # Previously had huc and sites_file_path as inputs but they weren't used so removed 1/13/26 # TODO: Clean up

        # If no valid sites remain, save the meta file we have with the new error messages, then abort.
        if len(valid_nwm_lids) == 0:
            msg = f"All sites associated to HUC {huc} are retricted. No more processing will continue."
            logging.info(msg)

            # Update mapping status and save the final sites file
            __update_sites_mapping_status(catfim_type, sites_file_path, "", "", sites_gdf, True)

            # Notes: 
            # - use the final file name of "{huc}_sites.library" is used as it is the one that catfim post will
            #   look for it. all sites should have had the status already updated.
            # - graceful exit is fine here. We don't need to crash it or through an exception.
            # sys.exit(0)  # humm.. or do we let this throw the exception for MP?

            continue_processing = False

        # =========================================
        # Retrieve and process threshold data if valid sites remain for HUC
        if continue_processing is True:

            # Save sites to a file checkpoint (Yes.. to the master copy)
            logging.info(f"Saving sites, pre flow and mapping, at {sites_file_path}")
            sites_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False)

            logging.info(f"{len(valid_nwm_lids)} sites remaining after validation: {valid_nwm_lids}")
            print("")

            # =========================================
            # Retrieve threshold data

            section_start_dt = datetime.now(timezone.utc)
            logging.info("Loading flow and threshold data for all valid sites...")

            # Get threshold data (stages and flows) for all valid sites in this HUC
            threshold_huc_df, data_source = gcf.get_threshold_data(huc, huc_path, valid_nwm_lids)

            # Note: It is possible threshold_huc_df can come back empty if huc has no site(s) with threshold data. 
            # It is okay if this df is empty for now.

            # Notes on the data_source column:
            #   The data_source is a column from the original threshold dataset. It often contains values
            #   such as 'Manual_Input' and/or values such as:
            #       NWS-NRLDB generally for stage data and USGS Rating Depot for Flow data or a combination
            #
            #   It is important for us to know if the data_source is Manual_Input for stage-based CatFIM, 
            #   because that prompts us to skip some of the elevation filtering if it is manual input.
            #   TODO: Is this the right answer?
            #
            #   While somewhat inefficent, we will add this to the sites_gdf column of threshold_data_source
            #   for simplicity of copying around and using against logic when needed and then we can drop it at the end.

            # =========================================
            # Process threshold data (creates a HUC-level library file with all of the site/magnitude combinations
            # that are still valid up to this point and creates the flow data for FB)

            section_start_dt = datetime.now(timezone.utc)
            logging.info("Processing initial flow and threshold data for all valid sites...")

            sites_gdf, huc_library_df = gcf.process_threshold_data(
                catfim_type,
                valid_nwm_lids,
                sites_gdf,
                huc,
                huc_path,
                output_temp_dir,
                threshold_huc_df,
                metadata_json,
            )

            # CatFIM Reorg. Note (Jan 26): We no longer need attribute files or the attribute folder.
            #    The data in those files were mostly duplicate data from the sites_gdf
            #    and the magnitude data already present in the threshold file.

            # Notes on the library file at this point:
            # - At this point, we do have at least one threshold record
            # - Library file was created and saved to disk
            # - It is not yet a gdf because it has no geometry; we will add geometry later in mapping
            # - Has the same pattern and columns that we use for final library files and is based on 
            #   only the lids and mag types that are still valid up to this point
            # - Some library site/magnitude combos may still be rejected and removed based on logic down the road
            # - Contains records for up to 5 magnitude types per lid (ie. ABCD1/action  or EFGH1/record)
            # - For SB, it does not yet contain any of the interval records because future
            #   logic might limit more mag types.

            # TODO: has_error system? or at least a way to see if the huc_library_df allows us to keep 
            # track of which lids / mags are still valid for processing. Some may fail in other places 
            # down the road and will be removed from the huc_library_df. For SB, it will add
            # some interval recs when applicable

            logging.info("End of initial processing of flow and threshold data.")
            duration_msg = sf.calculate_duration_msg(section_start_dt)
            logging.info(duration_msg)

            # ---------------------
            # Save a copy of the sites_gdf at this point
            # We may have some or all sites that have failed, so we want the master sites rec saved

            # sites_file_path_post_threshold = os.path.join(output_temp_dir, "sites_post_threshold.gpkg") # TODO: Clean up
            # logging.info(f"Saving sites data post threshold processing at {sites_file_path_post_threshold}")

            logging.info(f"Saving sites data post threshold processing at {sites_file_path}")
            sites_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False)

            if len(huc_library_df) > 0:
                huc_library_df.to_csv(library_pre_inun_file_path, index=False)
                logging.info(f"Saving initial library file to {library_pre_inun_file_path}")
            else:
                logging.warning("There are no valid huc_library recs at this point. Skipping to finalization")
                continue_processing = False

        if continue_processing is True:

            # Check for valid LIDs and abort processing if no valid sites remain

            # If there's no valid LIDs we won't have any library files, but we still need to finalize 
            # sites.gdf because it will still be part of the final product rollup.

            valid_lids = sites_gdf.loc[sites_gdf["mapped"] != "no"]["nws_lid"].values.tolist()
            if len(valid_lids) == 0:
                logging.info("There are no remaining sites to process skipping to sites finalization")
                __update_sites_mapping_status(catfim_type, sites_file_path, "", "", sites_gdf, True)
                continue_processing = False

        # Temp debugging # TODO: Clean up
        # print("--------------")
        # print("Ok.. let's stop here for now. Everything for FB and SB should be working at some level"
        #       " by this point")
        # sys.exit(0)

        # Process stage-based elevation data
        if continue_processing is True and os.getenv('CATFIM_TYPE') == "sb":

            logging.info("Start processing stage-based elevation data")

            # TODO: Discuss: what is a good name (__process_elevations) for this. Maybe a new file called catfim_data_processing.py? (but leave mapping to focus on inundation)
            # Should we keep all of the functions in this file for processing flow / stage / threshold data? or maybe a seperate file?
            # I'm partial to not making a new file, because we already have so many. But I could be convinced otherwise. -Emily

            # Process the elevation data
            sites_gdf, huc_library_df, has_critical_error = __process_elevations(
                sites_gdf, huc_library_df, huc, huc_path, output_temp_dir
            )

            # Abort early if there's a critical error
            if has_critical_error is True:
                logging.info(
                    "Critical error found and aborting processing"
                    " and logged already in __process_elevations."
                    "  Skipping to sites finalization."
                )
                __update_sites_mapping_status(catfim_type, sites_file_path, "", "", sites_gdf, True)

                # We abort early and this function should take care of that even with early abort.
                # Depending on where we failed, we may have something in the huc_library_df
                continue_processing = False

            else:
                # Checkpoint
                logging.info(f"Saving sites data post elevation processing at {sites_file_path}")
                sites_gdf.to_file(
                    sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False
                )

                if len(huc_library_df) > 0:
                    __update_library_csv(catfim_type, sites_gdf, huc_library_df, library_pre_inun_file_path)

                # We may have dropped some library recs above. See if there are any left and abort if not.
                if len(huc_library_df) == 0:
                    logging.warning(
                        "After SB elevations processing, there are no more library recs to process."
                        " Skipping to finalization."
                    )
                    __update_sites_mapping_status(catfim_type, sites_file_path, "", "", sites_gdf, True)
                    continue_processing = False

        # else:  # continue_processing and Flow based. I am not sure there is anything to do here actually.
        # Likely can go straight to inundation
        # ---------------------
        # If FB, load branch and HAND data? (rems and hydrotables), liekly all done via inundation scripts
        # hummmmm # TODO: Discuss?



        # Temp debugging # TODO: Clean up 
        sites_mapping_file_path = os.path.join(output_mapping_dir, f"sites_mapping.gpkg")
        sites_gdf.to_file(
            sites_mapping_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False
        )
        print(f'catfim_type: {catfim_type}') # TEMP DEBUG
        print(f'huc: {huc}') # TEMP DEBUG            
        print(f'huc_path: {huc_path}') # TEMP DEBUG
        print(f'output_mapping_dir: {output_mapping_dir}') # TEMP DEBUG
        print(f'output_temp_dir: {output_temp_dir}') # TEMP DEBUG
        print(f'sites_mapping_file_path: {sites_mapping_file_path}') # TEMP DEBUG
        print(f'library_pre_inun_file_path: {library_pre_inun_file_path}') # TEMP DEBUG
        print(f'library_post_mapping_file_path: {library_post_mapping_file_path}') # TEMP DEBUG

        print("--------------")
        print("exit CatFIM process HUC right before process_mapping()")
        sys.exit(0)

        # Start CatFIM mapping for the HUC
        if continue_processing is True:

            # We can also add temp additional columns in the mapping version of sites if it helps
            # with mapping processing, we just have to make they are removed after finalization here.
            # Mapping has to be fully independent.

            sites_mapping_file_path = os.path.join(output_mapping_dir, f"sites_mapping.gpkg")
            sites_gdf.to_file(
                sites_mapping_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False
            )

            # CatFIM Reorg Notes (Jan 26):
            # We don't really want to return anything, unless it is a catastrophic exception.
            # We know the final files that should exist if all went well from mapping so we
            # we can just reload them when we are ready.
            # and Yes... we will be reloading and replacing our sites_gdf from the mapping version
            # as it might have updated it.
            # Mapping will know the file names for the segments and discharge
            
            # Process CatFIM mapping
            gcfm.process_mapping(
                catfim_type,
                # huc,
                huc_path,
                output_mapping_dir,
                output_temp_dir,
                sites_mapping_file_path,
                library_pre_inun_file_path,
                library_post_mapping_file_path,
            )

            # Load up the temp updated mapping sites gdf which may have been updated in mapping
            if os.path.isfile(sites_file_post_mapping_path):
                sites_gdf = gpd.read_file(sites_file_post_mapping_path, engine='fiona')

            # else:
            # todo: hummm.. if it does not exist we know something significant failed
            # How do we want to handle that? Just call __update_sites ?? # TODO: Decide

            # and let us finalize in __update_status here?
            # We don't want mapping to ever change the copy coming in here as we need
            # to be able to re-run mapping as many times as it wants. It will be updating
            # the sites file as it progresses but will make its own called site_post_processing.gpkg
            # for this code to pick up later in our finalization.

            # TODO: Decide: Do we want any safety checks?

        # ---------------------
        # Finalize all final sites and library files for this HUC
        # Assumes all logging or finalization was jumped to earlier.
        if continue_processing is True:
            #
            # # TODO: HUMM... is there a smarter answer? Decide

            __update_sites_mapping_status(
                catfim_type,
                sites_file_path,
                library_file_path,
                library_post_mapping_file_path,
                sites_gdf,
                False,
            )

        logging.info(f"End processing for huc {huc}")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)

    except Exception:
        trace_error = traceback.format_exc()

        err_msg = f"A critical error has occurred while processing {huc}. Detail: {trace_error}"
        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # do we re-throw the error? gcf, aws, or cmd line? hummm # TODO: Decide

    # CatFIM reorg notes: 
    # nothing to return as of now
    # but generate_categorical_fim.py can if it has value to return.
    # if you use "return" and it is AWS, it will not error out.
    # yes.. we want a "return", but may/may not have a value.
    # if we add one, keep it a simple data type (str, int, float)

    #  hummm
    return huc


def __process_elevations(sites_gdf, huc_library_df, huc, huc_path, output_temp_dir):
    """
    Only used by stage-based CatFIM.

    Provides site-specific datum adjustment and elevation data repairs as needed.

    Arguments:

    Returns: # TODO: Fill out docstring

    



    CatFIM Reorg Notes (Jan 26):
    
    data_source comes from the original threshold dataset. It was put into a temp column
    in the sites_gdf under the name of threshold_data_source. We can use it for processing logic.
    Later we can drop the columns before the final huc catfim outputs.
    This can be values of "Manual_Input" and/or values such as:
        NWS-NRLDB generally for stage data and USGS Rating Depot for Flow data.

    Same as before, the huc_library_df is a the starting framework for each lid and magnitude
    that is still valid by this point. More tests may drop some of the records.
    ie.. maybe some lid with an action stage fails a test, assuming that action record was
    there in the first place. Some columns in the library df will be updated such as the
    datum and altitude columns. For records that fail, we will just keep updating the
    sites.gdf as we go and at the end of this function, we will drop lid/mags that no longer apply

    TODO: this is still a wip.
    data_source = threshold_huc_df["source_stage"]

    huc_library_df should not be empty by now, but some lids may not have all or some library recs
    for each stage, of course. ie) a lid / stage might have already failed for various reasons before
    getting here. If it is, we should not bother coming in here as all sites have already failed for
    various reasones.

    However.. threshold_huc_df has not be filtered at this point and we need to watch for -1, 0, etc
    TODO: Do we need to even use threshold_huc_df anymore by this point? maybe just the huc_library_df?
    I don't think there is anything left in the threshold_huc_df that is not already in applicable
    huc_library_df recs where it still qualifies.

    This portion of code can add or update columns to the library csv or the sites gpkg as it needs
    It just has to makes sure drops any temp columns in finalization.

    """

    data_source = "WRDS"
    has_critical_error = False

    library_pre_inun_file_path = os.path.join(output_temp_dir, "library_pre_inundation.csv")

    # Initialize output dataframes
    updated_huc_library_df = pd.DataFrame() # a replacement huc_library_df
    acceptable_usgs_elev_df = pd.DataFrame()  # empty in case we do not load it via non Manual Input

    # Get a list of sites that are still valid (there should be at least one)
    valid_lids = sites_gdf.loc[sites_gdf["mapped"] != "no"]["nws_lid"].values.tolist()

    # Read in the USGS elevation data and create the exclusion status column
    if data_source != 'Manual_Input':  # Note: Manual input data does not use usgs_elev_table

        # ------------------------
        # CatFIM Reorg Note (Jan 26): The usgs elev table was previously in iterate_through_huc_stage_based 
        # but wasn't used until later in the code in that function. Now it is done much earlier.

        # Get the USGS elevation table
        usgs_elev_table_file_name = 'usgs_elev_table.csv'
        src_usgs_elev_table = os.path.join(os.getenv("FIM_RUN_DIR"), huc, usgs_elev_table_file_name)

        # if data_source != 'Manual_Input' and not os.path.isfile(src_usgs_elev_table): # TODO: Check with Rob, but I don't think we need the first part of this line - Emily
        if not os.path.isfile(src_usgs_elev_table):
            msg = "Internal Error: Missing key data from HUC record (usgs_elev_table missing)"
            raise Exception(msg)

        # Make a copy to the local HUC folder to lower the chance of file collisions.
        # (Copying it to HUC folder and not a temp drive.)
        local_copy_usgs_elev_table = os.path.join(huc_path, usgs_elev_table_file_name)
        shutil.copyfile(src_usgs_elev_table, local_copy_usgs_elev_table)

        # TODO: this seems a bit weird. hummm.
        # and has data for all lids in this huc. We are not using it for validation, just loading it for all sites

        # Read back in the local copy of the USGS elevation table
        usgs_elev_df = None # TODO: is this line necessary?
        usgs_elev_df = pd.read_csv(local_copy_usgs_elev_table)  # Only used here

        # ------------------------
        # Creates an updated USGS elevation table with a descriptive USGS exclusion status column
        # Note: Doesn't filter the df, that happens next in __adj_dem_elevation_val()
        acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df)

        if acceptable_usgs_elev_df is None or len(acceptable_usgs_elev_df) == 0:
            msg = "Unable to find gage data"  # TODO: USGS Gage Method: Update this error message to be more descriptive
            logging.error(f"{msg} for all lids")

            # If this happens, all sites in this HUC will fail and have this same message, so we can update them all
            sites_gdf["mapped"] = 'no'
            sites_gdf["status"] = msg
            has_critical_error = True

            # we need to clear the huc_library_df as there no longer will be any to be inundated
            # just delete the file.
            # Hummm.. critical error? we jsut need a way to tell the calling code to stop further processing
            # and skip to sites finalization.  Bad variable name maybe? # TODO: Decide (chat with Rob)

            return sites_gdf, [], has_critical_error  # This will stop further processing downstream
        # else: continue on

    else:  # If the source is manual input, we skip the above elevation filtering
        logging.info("Skipping elevation checks and datum adjustment for Manual Input source")

    # TODO: skip here and load it later when we need it - Discuss what this means? -E
    # we have already validated that the huc folder exists and we can validate
    # each huc's branches if/when it gets there.
    # humm? or do we?  Coudl the huc have been a bad huc in the fim run dir?
    # maybe we do check it, but figure out how to handle the site.gpkg statuses
    # branch_dir = os.path.join(fim_dir, huc, 'branches')
    # if not os.path.exists(branch_dir):
    #     msg = ":branch directory missing"
    #     # all_messages.append(huc + msg)
    #     MP_LOG.warning(huc + msg)
    #     skip_lid_process = Truer, huc, 'branches')

    # TODO: Safety check: Do a quick check to make sure there are no huc_library records with -1 in the stage column.
    # should not be any.


    for lid in valid_lids:

        # Find the single lid record from the sites_gdf and applicable library rows.
        # You can update the iloc'd record and replace the original row in the
        # parent as long as you can find the correct index

        # Careful about using these two, wahat your function scoping rules for df's being
        # updated in functions. # TODO: Rob - clarify comment!

        lid_sites_gdf = sites_gdf.loc[sites_gdf["nws_lid"] == lid].copy()
        lid_library_df = huc_library_df[huc_library_df["nws_lid"] == lid].copy()

        if len(lid_sites_gdf) != 1:
            raise Exception("Internal error: There should be exactly one lid rec here")

        if len(lid_library_df) == 0:
            # If not, the sites should already have been updated to mapped is no and correct
            # status message applied.
            raise Exception("Internal error: There should be at least one valid lid library rec by now")

        # Make an "rfc_stage" column (for documentation of the data source)
        lid_library_df['rfc_stage'] = lid_library_df['stage']

        # TODO: rfc_stage, but final library calls this rfs_stage (typo?)
        # uncorrect WRDS value before we adjusted it for inundation
        # Changed this to rfc_stage for processing. Fix in finalization?

        # Get the site altitude from the USGS data
        lid_altitude = lid_sites_gdf.iloc[0]['usgs_data_altitude']
        if lid_altitude is None or lid_altitude == 0:
            # Jan 2026: In previous versions not all recs stopped here when this failed
            # some continued on and ultimatly failed down the road.
            msg = 'AHPS site altitude value is invalid'
            logging.warning(f"{lid}: {msg}")
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
            continue

        # Note from Rob: From previous code, there was flaws in this code as far as what data was created or available at the end
        # of the this Manual_Input code.  Or was there? # TODO: Double check outputs at the end of this

        # Adjust the elevation value (if data is not from Manual Input)
        if data_source != 'Manual_Input':

            # Get the dem_adj_elevation value from usgs_elev_table.csv.
            # Prioritize the value that is not from branch 0.

            # TODO: Clean up - this code is just re-written below with adjusted inputs and outputs 
            # sites_gdf, lid_usgs_elev, dem_eval_messages = __adj_dem_elevation_val(sites_gdf,
            #     acceptable_usgs_elev_df, lid
            # )
            # all_messages = all_messages + dem_eval_messages
            # if len(dem_eval_messages) > 0:
            #     continue

            # Get the DEM-adjusted elevation value (prioritize the val that isn't from branch 0)
            lid_usgs_elev, err_msg = __adj_dem_elevation_val(acceptable_usgs_elev_df, lid)
            if err_msg != "":
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
                continue

            # Jan 2026: we don't this use __filter_bad_usgs_gage_data() anymore as it is duplicate fundamentally already done
            # in  __adj_dem_elevation_val
            # Filter out sites that don't have "good" data
            # err_msg = __filter_bad_usgs_gage_data(lid_sites_gdf, lid)  # logging done inside this one
            # if err_msg != "":
            #     sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
            #     continue

            # hummm... threshold_huc_df can stil include some recs with -1, 0 and None

            # Determine the vertical datum adjustment (ft) to convert the datum of the rating curve to NAVD88
            datum_adj_ft, err_msg = __adjust_datum_ft(lid_sites_gdf, lid_library_df, lid)
            if err_msg != "":
                # l=Logging was already done in __adjust_datum_ft - some are logged as warnings and some as errors
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
                continue

        else:  # If source is manual input, skip the above elevation filtering
            lid_altitude = float(lid_altitude)  # LID altitude is expected to be in meters
            lid_usgs_elev = (
                lid_altitude * 0.3048
            )  # lid_altitude is now in meters to match non-manual input units
            # TODO: Automate conversion?

            datum_adj_ft = 0  # no datum adjustment for manual input

            # Manual input notes - # TODO: Check this, clean up notes
            # # From previous code, there was flaws in this code as far as what data was created or available at the end
            # # of the this Manual_Input code. Or was there? 
            # Update all library (stage) recs for this lid as they will all be the same for each mag per lid.
            # ie) (some site).action (mag) and all its intervals will have the same datum_adj_ft, lid_alt_ft
            # lid_alt_m, and lid_usgs_elev. Later, per stage, we will calc datum_adj_wse_ft and
            # datum_adj_wse_m which add stage value to it.

        # Add datum adjustment info to site dataframe
        lid_library_df['datum_adj_ft'] = datum_adj_ft
        lid_library_df['lid_alt_ft'] = lid_altitude
        lid_library_df['lid_alt_m'] = lid_altitude * 0.3048
        lid_library_df['lid_usgs_elev'] = lid_usgs_elev  # temp column for processing

        # TODO: plug this code section into mapping when we get there and update the library rec
        # datum_adj_wse = stage_val + datum_adj_ft + lid_altitude
        # datum_adj_wse_m = datum_adj_wse * 0.3048  # Convert ft to m

        # For now, lets # TODO: Rob what did you mean here?

        # segments (feature id list) are already loaded much earlier
        # and save to a file for mapping to pick up later

        # =====================
        # Check for large discrepancies between the elevation values from WRDS and HAND.
        #   (because that could cause bad mapping.) Manual_Input will have no elev 
        #   disparity because it's from the the same value.

        # TODO: test this elevation_diff code -Rob

        # Calculate the elevation difference between the two elevation values
        elevation_diff = lid_usgs_elev - (lid_altitude * 0.3048)
        diff_rounded = round(elevation_diff, 2)
        # Note: elevation_diff and diff_rounded are used for these tests only
        # and are not part of any logic or are saved to any df's

        # Log minor elevation difference information - not an error, just for reference (maybe remove later)
        if elevation_diff > 0:
            logging.warning(f"{lid}: USGS elev is higher than HAND elev by {diff_rounded} ft")
        elif elevation_diff < 0:
            logging.warning(f"{lid}: USGS elev is lower than HAND elev by {abs(diff_rounded)} ft")

        # Throw an error for elevation differences greater than 10 meters
        if abs(elevation_diff) > 10:
            err_msg = 'Large discrepancy in elevation estimates from gage and HAND'
            logging.warning(f"{lid}: {err_msg}")

            # We will clean up the huc_library folder shortly
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
            continue

        # Log a warning for elevation differences of 5-10 meters (but continue on)
        elif abs(elevation_diff) > 5:
            err_msg = f':Moderate discrepancy ({diff_rounded} ft) in elevation estimates from gage and HAND'
            logging.warning(f"{lid}: {err_msg}")
            # sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg] # TODO: Clean up

        # =====================
        # Check whether stage value is actually a WSE value, and fix if needed

        # The lid_library_df has a rec per stage that is still valid at this point.
        # It already dropped all recs with -1, 0 or None

        # Get lowest stage value
        lowest_stage_val = lid_library_df['stage'].min()
        
        # lowest_stage_val = lid_library_df[lid_library_df['stage'] != -1]['stage'].min() # TODO: Clean up
        # stage_values_df, valid_stage_names, stage_warning_msg, err_msg = __calc_stage_values(
        #     categories, thresholds
        # )

        maximum_stage_threshold = 250  # TODO: Move to a variables file?
        if (lowest_stage_val > lid_altitude) and (lowest_stage_val > maximum_stage_threshold):
            lid_library_df['stage'] = lid_library_df['stage'] - lid_altitude
            logging.info(
                f"{lid}: Lowest stage val > elev and higher than max stage thresh. Subtracted"
                " elev from stage vals to fix."
            )

        updated_huc_library_df = pd.concat([updated_huc_library_df, lid_library_df], ignore_index=True)

    if len(updated_huc_library_df) > 0:
        updated_huc_library_df.to_csv(library_pre_inun_file_path, index=False)
        logging.info(
            f"Saving updated stage based library after elevations file to {library_pre_inun_file_path}"
        )

    return sites_gdf, updated_huc_library_df, has_critical_error


def __create_acceptable_usgs_elev_df(usgs_elev_df):
    '''
    Used in stage-based CatFIM.

    Creates an updated USGS elevation table with a descriptive USGS exclusion status column.

    The function checks each row of the input DataFrame for:
        - Acceptable USGS data altitude method code
        - Acceptable USGS site type
        - Acceptable USGS altitude accuracy threshold

    For each criterion not met, a corresponding message is appended to the 'usgs_exclusion_status' column.
    If all criteria are met, the status is set to 'acceptable'.
    In case of missing columns or errors, the original DataFrame is returned and errors are logged.

    Args:
        usgs_elev_df (pd.DataFrame): DataFrame containing USGS elevation data with required columns.
        huc_lid_id (str): Identifier for the HUC/LID, used for logging.

    Returns:
        pd.DataFrame: DataFrame with an added 'usgs_exclusion_status' column indicating acceptability.
    
    CatFIM Reorg Notes (Jan 2026):
    
    This basic testing of the site and alt codes was basically done twice in the old code
    and it is now consolidated to just happen here.
    
    '''

    acceptable_usgs_elev_df = None

    try:
        # Create data messages
        acceptable_msg = ''
        unacceptable_alt_meth_msg = 'Unacceptable USGS data altitude method: '
        unacceptable_site_type_msg = 'Unacceptable USGS site type: '
        unacceptable_alt_acc_msg = 'Unacceptable USGS altitude accuracy threshold: '

        # Create columns for whether the USGS data meets each criterion
        msg1 = np.where(
            usgs_elev_df['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list),
            acceptable_msg,
            unacceptable_alt_meth_msg + usgs_elev_df['usgs_data_alt_method_code'].astype(str) + ', ',
        )
        msg2 = np.where(
            usgs_elev_df['usgs_data_site_type'].isin(acceptable_site_type_list),
            acceptable_msg,
            unacceptable_site_type_msg + usgs_elev_df['usgs_data_site_type'].astype(str) + ', ',
        )
        msg3 = np.where(
            usgs_elev_df['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh,
            acceptable_msg,
            unacceptable_alt_acc_msg + usgs_elev_df['usgs_data_alt_accuracy_code'].astype(str) + ', ',
        )

        status_df = pd.DataFrame({'msg1': msg1, 'msg2': msg2, 'msg3': msg3})

        # Create detailed USGS exclusion status
        usgs_elev_df['usgs_exclusion_status'] = status_df['msg1'] + status_df['msg2'] + status_df['msg3']

        # If it doesn't have anything for the exclusion criteria, set the usgs_exclusion_status to acceptable
        # CatFIM will only be processed for sites with a usgs_exclusion_status of 'acceptable'
        usgs_elev_df['usgs_exclusion_status'] = usgs_elev_df['usgs_exclusion_status'].replace(
            '', 'acceptable'
        )

        # Copy df to de-fragment and rename
        acceptable_usgs_elev_df = usgs_elev_df.copy()

    except Exception as ex:
        # Not sure any of the sites actually have those USGS-related
        # columns in this particular file, so just assume it's fine to use

        # print("(Various columns related to USGS probably not in this csv)")
        # print(f"Exception: \n {repr(e)} \n")
        msg = "An error has occurred while working with the usgs_elev table"
        logging.critical(msg)
        logging.critical(traceback.format_exc())
        raise ex

    return acceptable_usgs_elev_df


def __adjust_datum_ft(lid_sites_gdf, lid_library_df, lid):
    '''
    Used in stage-based CatFIM.

    Determines the vertical datum adjustment (in feet) to convert the datum of the
    rating curve to NAVD88.

    Uses the rating curve source and metadata to get the correct vertical datum and CRS.

    It applies custom workarounds for known sites with special datum or CRS requirements,
    and attempts to compute the adjustment using the NOAA VDatum service when necessary.

    Args:
        - lid_sites_gdf
        - lid_library_df
        - lid

    Returns:
        tuple:
            - datum_adj_ft (float or None): The vertical datum adjustment in feet to convert to NAVD88,
              or None if adjustment could not be determined.
            - err_msg
    Notes:
        - Special handling is included for sites with known datum or CRS issues.
        - If the datum is already NAVD88 or equivalent, the adjustment is 0.0.
        - If the datum is NGVD29 or similar, an adjustment is attempted using the NOAA VDatum service.
        - If errors occur during adjustment, appropriate messages are logged and returned.
        - While we do not update the lid_sites_gdf here, we do the logging as sometimes it is an error
          and sometimes a warning.

    TODO: Aug 2024: This whole parts needs revisiting. Lots of lid data has changed and this
    is all likely very old. (Need to revisit special cases.)
    '''

    datum_adj_ft = -99999.0
    err_msg = ""

    # ---------------------------
    # Determine source of interpolated threshold flows, this will be the rating curve that will be used.

    # TODO: Why are we using flow source data to do logic calcs and not the stage columns
    # see the notes below about using the rating_curve_source - Discuss

    # Yes... flow data even though we are processing stage data, see notes lower
    # lid_flow_data = lid_library_df.loc[
    #     lid_library_df['nws_lid'] == lid & lid_library_df['threshold_type'] == 'flows'
    # ]

    # Get the rating curve source
    rating_curve_source = lid_library_df.iloc[0]["q_src"]
    if rating_curve_source is None or rating_curve_source == "":
        err_msg = 'No source for rating curve'
        logging.warning(f"{lid}: {err_msg}")
        return datum_adj_ft, err_msg

    # rating_curve_source = flows.get('source')
    # MP_LOG.trace(f"{huc_lid_id} : rating_curve_source is {rating_curve_source}")

    # Get the datum and adjust to NAVD if necessary.
    # This is getting it from the flow (q) columns. Is that really right? # TODO: Double check
    # is seems to reeally only need the q_src but not the other two q (flow) values.
    # This forces an error if no flow data comes in.

    # TODO: We need to look more into this and also look deeper at the rating curve values
    # see library_pre_inundation.csv the original huc threshold file.
    # Jan 2026: Function was renamed from get_datum to __get_datum_from_df
    nws_datum_info, usgs_datum_info = __get_datum_from_df(lid_sites_gdf)
    if rating_curve_source == 'USGS Rating Depot':
        datum_data = usgs_datum_info
    elif rating_curve_source == 'NRLDB':
        datum_data = nws_datum_info

    # If datum not supplied, skip to new site
    datum = datum_data.get('datum', None)
    if datum is None:
        err_msg = 'Datum info unavailable'
        logging.warning(f"{lid}: {err_msg}")
        return datum_adj_ft, err_msg

    # ---------------------------
    # Perform site-specific data corrections 

    # Jan 2026: There was a piece of code here that was for bmbp1 but another set of
    # related code lower also addresses that site. Merged code with other BMBP1 test.
    #     if lid == 'bmbp1': # TODO: Clean up
    #       rating_curve_source = 'NRLDB'

    # SPECIAL CASE: Custom workaround these sites have faulty crs from WRDS. CRS needed for NGVD29
    #   conversion to NAVD88
    # USGS info indicates NAD83 for site: bgwn7, fatw3, mnvn4, nhpp1, pinn4, rgln4, rssk1, sign4, smfn7,
    #   stkn4, wlln7
    # Assumed to be NAD83 (no info from USGS or NWS data): dlrt2, eagi1, eppt2, jffw3, ldot2, rgdt2
    if lid.lower() in [
        'bgwn7',
        'dlrt2',
        'eagi1',
        'eppt2',
        'fatw3',
        'jffw3',
        'ldot2',
        'mnvn4',
        'nhpp1',
        'pinn4',
        'rgdt2',
        'rgln4',
        'rssk1',
        'sign4',
        'smfn7',
        'stkn4',
        'wlln7',
    ]:
        datum_data.update(crs='NAD83')

    # SPECIAL CASE: Site BMBP1
    if lid.lower() == 'bmbp1':
        # SPECIAL CASE: Workaround for "bmbp1" where the only valid datum is from NRLDB (USGS datum is null).
        # Modifying rating curve source will influence the rating curve and
        #   datum retrieved for benchmark determinations.
        rating_curve_source = 'NRLDB'

        # SPECIAL CASE: Workaround for bmbp1; CRS supplied by NRLDB is mis-assigned (NAD29) and
        #   is actually NAD27.
        # This was verified by converting USGS coordinates (in NAD83) for bmbp1 to NAD27 and
        #   it matches NRLDB coordinates.
        datum_data.update(crs='NAD27')

    # SPECIAL CASE: Custom workaround these sites have poorly defined vcs from WRDS. VCS needed to ensure
    #   datum reported in NAVD88.
    # If NGVD29 it is converted to NAVD88.
    # bgwn7, eagi1 vertical datum unknown, assume navd88
    # fatw3 USGS data indicates vcs is NAVD88 (USGS and NWS info agree on datum value).
    # wlln7 USGS data indicates vcs is NGVD29 (USGS and NWS info agree on datum value).
    if lid.lower() in ['bgwn7', 'eagi1', 'fatw3']:
        datum_data.update(vcs='NAVD88')
    elif lid.lower() == 'wlln7':
        datum_data.update(vcs='NGVD29')

    # ---------------------------
    # Get datum adjustment to convert elev to NAVD88 (if elev data is in NGVD29)
    # Uses NOAA VDatum API

    # TODO: Does this will work calling ngvd_to_navd_ft when in EC2's? Can it talk to that
    # service from EC2's? Check this.

    # Set default datum_adj_ft to 0.0
    datum_adj_ft = 0.0

    crs = datum_data.get('crs')

    # Get the datum adjustment to convert NGVD to NAVD
    if datum_data.get('vcs') in ['NGVD29', 'NGVD 1929', 'NGVD,1929', 'NGVD OF 1929', 'NGVD']:
        try:
            datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data)
        except Exception as ex:
            err_msg = f"ERROR: {lid}: ngvd_to_navd_ft"
            logging.error(err_msg)
            logging.error(traceback.format_exc())
            ex = str(ex)
            if crs is None:
                err_msg = 'NOAA VDatum adjustment error, CRS is missing'
                logging.error(f"{lid}: {err_msg}")
            if 'HTTPSConnectionPool' in ex:
                time.sleep(10)  # Maybe the API needs a break, so wait 10 seconds
                try:
                    datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data)
                except Exception:
                    err_msg = ':NOAA VDatum adjustment error, possible API issue'
                    logging.error(f"{lid}: {err_msg}")
            if 'Invalid projection' in ex:
                err_msg = f'NOAA VDatum adjustment error, invalid projection: crs={crs}'
                logging.error(f"{lid}: {err_msg}")

    return datum_adj_ft, err_msg


def __get_datum_from_df(lid_sites_gdf):
    '''
    This extracts key meta data related to datums from a dataframe and
    builds dictionaries. The incoming metadata_df basically just the original
    metadata json that has been normalized and put into dataframe columns.
    This gets data for both NWS and USGS.

    This also assume exactly one metadata data record.

    Given a record from the metadata endpoint, retrieve important information
    related to the datum and site from both NWS and USGS sources. This information
    is saved to a dictionary with common keys. USGS has more data available so
    it has more keys.

    NOTE: Some columns have already been renamed ????

    Parameters
    ----------
    metadata_df : Dataframe
        Single record dataframe made from a single lid metadata json dataset.

    Returns
    -------
    nws_datums : DICT
        Dictionary of NWS data.
    usgs_datums : DICT
        Dictionary of USGS Data.

    '''

    if lid_sites_gdf is None:
        raise Exception("The metadata df can not be None")
    if len(lid_sites_gdf) != 1:
        raise Exception(f"Metadata should contain exactly one record but has {len(lid_sites_gdf)} records")

    # Get site and datum information from nws sub-dictionary. Use consistent naming between USGS and NWS sources.
    # **** NWS
    nws_datums = {}
    nws_datums['nws_lid'] = lid_sites_gdf['nws_lid'].item()
    nws_datums['usgs_site_code'] = lid_sites_gdf['identifiers_usgs_site_code'].item()
    nws_datums['state'] = lid_sites_gdf['nws_data_state'].item()
    nws_datums['datum'] = lid_sites_gdf['nws_data_zero_datum'].item()
    nws_datums['vcs'] = lid_sites_gdf['nws_data_vertical_datum_name'].item()
    nws_datums['lat'] = lid_sites_gdf['nws_data_latitude'].item()
    nws_datums['lon'] = lid_sites_gdf['nws_data_longitude'].item()
    nws_datums['crs'] = lid_sites_gdf['nws_data_horizontal_datum_name'].item()
    nws_datums['source'] = 'nws_data'

    # Get site and datum information from usgs_data sub-dictionary. Use consistent naming between USGS and NWS sources.
    # **** USGS
    usgs_datums = {}
    usgs_datums['nws_lid'] = lid_sites_gdf['nws_lid'].item()
    usgs_datums['usgs_site_code'] = lid_sites_gdf['identifiers_usgs_site_code'].item()
    usgs_datums['active'] = lid_sites_gdf['usgs_data_active'].item()
    usgs_datums['state'] = lid_sites_gdf['usgs_data_state'].item()
    usgs_datums['datum'] = lid_sites_gdf['usgs_data_altitude'].item()
    usgs_datums['vcs'] = lid_sites_gdf['usgs_data_alt_datum_code'].item()
    usgs_datums['datum_acy'] = lid_sites_gdf['usgs_data_alt_accuracy_code'].item()
    usgs_datums['datum_meth'] = lid_sites_gdf['usgs_data_alt_method_code'].item()
    usgs_datums['lat'] = lid_sites_gdf['usgs_data_latitude'].item()
    usgs_datums['lon'] = lid_sites_gdf['usgs_data_longitude'].item()
    usgs_datums['crs'] = lid_sites_gdf['usgs_data_latlon_datum_name'].item()
    usgs_datums['source'] = 'usgs_data'

    return nws_datums, usgs_datums


def __adj_dem_elevation_val(acceptable_usgs_elev_df, lid):
    '''
    Used in stage-based CatFIM.

    Retrieves the DEM-adjusted elevation value for a given USGS gage site (LID) from the provided DataFrame,
    and checks for exclusion criteria or data issues.

    Args:
        acceptable_usgs_elev_df (pd.DataFrame): DataFrame containing USGS gage information, including
            'nws_lid', 'levpa_id', 'dem_adj_elevation', and 'usgs_exclusion_status' columns.
        lid (str): The NWS LID to look up.

    Returns:
        tuple:
            - lid_usgs_elev (float): The DEM-adjusted elevation value for the specified LID, or 0 if not found or excluded.
            - err_msg: might be empty

    Notes:
        - If the LID is not found, excluded, or has an elevation of 0, appropriate messages are logged and returned.
        - If multiple entries exist for the LID, the one with a non-zero 'levpa_id' is used.
        - Exclusion status other than 'acceptable' will result in an early return with a message.
    '''

    # MP_LOG.trace(locals())

    lid_usgs_elev = 0
    err_msg = ""
    try:
        # Check for USGS elevation data that matches the LID
        matching_rows = acceptable_usgs_elev_df.loc[acceptable_usgs_elev_df['nws_lid'] == lid.upper()]

        # Check if the site is not in the USGS data table (in our data)
        if len(matching_rows) == 0:
            err_msg = 'Gage not in HAND usgs gage records, likely due to exclusion criteria'
            logging.warning(f"{lid}: {err_msg}")
            return lid_usgs_elev, err_msg

        # Get the USGS data for the site
        if len(matching_rows) == 2:
            # If there are two level paths, use the one that is not branch 0 (there will never be more than two)
            lid_info = acceptable_usgs_elev_df.loc[
                (acceptable_usgs_elev_df['nws_lid'] == lid.upper())
                & (acceptable_usgs_elev_df['levpa_id'] != 0)
            ]
        else:
            # Get the site that matches the LID
            lid_info = acceptable_usgs_elev_df.loc[acceptable_usgs_elev_df['nws_lid'] == lid.upper()]

        # Get elevation and exclusion status
        lid_usgs_elev = lid_info['dem_adj_elevation'].values[0]
        usgs_exclusion_status = lid_info['usgs_exclusion_status'].values[0]

        # If there is an exclusion status other than 'acceptable,' return the status
        # Uses [:-2] to exclude the last comma and space in the string
        if usgs_exclusion_status != 'acceptable':
            err_msg = 'Gage excluded due to the following criteria -- ' + usgs_exclusion_status[:-2]
            logging.warning(f"{lid}: {err_msg}")
            return lid_usgs_elev, err_msg

        # Check whether DEM adjusted elevation is 0 or not set
        if lid_usgs_elev == 0:
            err_msg = 'DEM adjusted elevation is 0 or not set'
            logging.warning(f"{lid}: {err_msg}")
            return lid_usgs_elev, err_msg

    except IndexError:  # Occurs when LID is missing from table (yes. warning)
        err_msg = 'Error when extracting dem adjusted elevation value'
        logging.warning(f"{lid}: {err_msg}")
        logging.warning(traceback.format_exc())

    # TODO: We need to figure out how to get trace working
    # logging.trace(f"{lid}: lid_usgs_elev is {lid_usgs_elev}")

    logging.info(f"{lid}: lid_usgs_elev is {lid_usgs_elev}")

    return lid_usgs_elev, err_msg


"""
def __filter_bad_usgs_gage_data(lid_sites_gdf, lid): # TODO: Clean up

    # We wil do logging here as some are warnings, some are errors
    # and we want logging to ber

    # Those three usgs_data columms in are lid_sites_gdf, so can pull it from there
    # to test it and send back an error message if it fails those tests.

    err_msgs = []
    try:
        alt_method_code = lid_sites_gdf.iloc[0]['usgs_data_alt_method_code']
        if alt_method_code not in acceptable_alt_meth_code_list:
            err_msg = "Not in acceptable alt method codes"
            err_msgs.append(err_msg)
            logging.warning(f"{lid}: {err_msg}")

        site_type = lid_sites_gdf.iloc[0]['usgs_data_site_type']
        if site_type not in acceptable_site_type_list:
            err_msg = "Not in acceptable site type codes"
            err_msgs.append(err_msg)
            logging.warning(f"{lid}: {err_msg}")

        alt_accuracy_code = lid_sites_gdf.iloc[0]['usgs_data_alt_accuracy_code']
        if alt_accuracy_code is None or alt_accuracy_code == "":
            err_msg = "USGS data Alt Accuracy code not available"
            err_msgs.append(err_msg)
            logging.warning(f"{lid}: {err_msg}")

        if not float(alt_accuracy_code) <= acceptable_alt_acc_thresh:
            err_msg = "Not in acceptable threshold range"
            err_msgs.append(err_msg)
            logging.warning(f"{lid}: {err_msg}")

        err_msg
        err_msg = '; '.join(err_msgs)  # concat the messages

    except Exception:

        # TODO: Why was this caught and allowed to continue in old code?

        err_msg = "Error filtering out 'bad' data in the usgs data"
        logging.error(f"{lid}: {err_msg}")
        logging.error(traceback.format_exc())

    return err_msg
"""

"""
# This is no longer used here but a version of it is now in mapping.
def __calc_stage_values(thresholds):
    '''
    Used in stage-based CatFIM.

    Calculates stage values for flood categories based on provided thresholds.

    Args:
        thresholds (dict): Dictionary mapping stage names to their threshold values (anywhere from 0 to 5 stages).

    Returns:
        stage_values_df (pandas.DataFrame): DataFrame with rows for each stage and
            their corresponding values (defaulted to -1 if missing or invalid).
        valid_stage_names (list): List of stage names with valid threshold values.

        err_msg (str): Error message if all stages are missing or invalid.
            In theory, this was already done in earlier steps
        
        For stages 

    Notes:
        - Stages with missing or invalid threshold values are assigned -1.
        - If all five stages are invalid, returns None for the DataFrame and an error message.
        - Warning messages are formatted with "---" to indicate missing stage data.

    '''

    # ++++++++++++++++++++++++++++++++++++++++++

    # HOLD !!!!!!
    # I am thinking I am going to change where these are calcuated and when.
    # ++++++++++++++++++++++++++++++++++++++++++



    # Set default values
    err_msg = ""
    warning_msg = ""
    default_stage_data = [['action', -1], ['minor', -1], ['moderate', -1], ['major', -1], ['record', -1]]
    valid_stage_names = []

    # Setting up a default df (not counting record)
    stage_values_df = pd.DataFrame(default_stage_data, columns=['stage_name', 'stage_value'])

    for stage in categories:

        if stage in thresholds:
            stage_val = thresholds[stage]
            if stage_val is not None and stage_val != "" and stage_val > 0:
                stage_values_df.loc[stage_values_df.stage_name == stage, 'stage_value'] = stage_val
                valid_stage_names.append(stage)

    invalid_stages_df = stage_values_df[stage_values_df["stage_value"] <= 0]

    if len(invalid_stages_df) == 5:
        err_msg = ':All threshold values are unavailable or invalid'  # already formatted
        return None, [], "", err_msg

    # Yes.. a bit weird, we are going to put three dashs in front of the message
    # to help show it is valid even with a missing stage msg.
    # any other record with a status value that is not "Good"
    # or does not start with a --- is assumed to be possibly bad (not mapped)
    warning_msg = ""

    for ind, stage_row in invalid_stages_df.iterrows():
        if warning_msg == "":
            warning_msg = f":---Missing stage data for {stage_row['stage_name']}"
        else:
            warning_msg += f"; {stage_row['stage_name']}"

    return stage_values_df, valid_stage_names, warning_msg, err_msg
"""


def __update_library_csv(catfim_type, sites_gdf, huc_library_df, library_pre_inun_file_path):
    # TODO: Remove unused catfim_type var?
    """
    Updates the library CSV to remove recs for sites that failed in __process_elevations().

    """
    if len(huc_library_df) > 0:
        # Get a list of invalid sites
        invalid_lids = sites_gdf.loc[sites_gdf["mapped"] == "no"]["nws_lid"].values.tolist()
        if len(invalid_lids) > 0:
            indices_to_drop = huc_library_df[huc_library_df['nws_lid'].isin(invalid_lids)].index

            # All records might have been dropped earlier or were not here in the first place
            if len(indices_to_drop) > 0:
                huc_library_df = huc_library_df.drop(indices_to_drop)
                if len(huc_library_df) > 0:
                    huc_library_df.to_csv(library_pre_inun_file_path, index=False)
                    logging.info(f"Updating library csv file at {library_pre_inun_file_path}")

    # Note: We will check for count of remaining library recs when we return


def __setup_sites_gdf(sites_gdf, catfim_type):
    """
    Setup and prepare a GeoDataFrame of sites for CATFIM processing.

    Parameters
    ----------
    sites_gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing site information with columns such as 'nws_lid',
        'HUC8', and various NWS and NWM metadata fields.
    catfim_type : str
        The type of CATFIM processing, 'sb' or 'fb'. 
        Determines whether SB-specific validation columns are added.
    
    Returns
    -------
    geopandas.GeoDataFrame
        The processed sites GeoDataFrame with:
        - Columns reordered: 'nws_lid' first, 'HUC8' second
        - New columns added: 'mapped', 'status', 'warnings'
        - Incompatible list fields dropped: 'downstream_nwm_features', 'upstream_nwm_features'
        - Data types converted to string for GeoPackage compatibility
        - (SB only) Additional columns for coordinate accuracy, altitude accuracy, and site type validation

    Notes
    -----   
    
    Jan 6, 2026: In earlier versions, building up this data was done in two differnt places
    one in the SB logic flow another one was basically identical in FB.
    Some of these columns were updated or added through the code as it progressed.
    But almost all of it was meta data that we knew up front, so we load what we
    already know at this point instead of piecemail.


    This function starts building up the new sites / meta file. We can adjust the status as we go.


    """

    # Move LID column to be first (easier to read the outputs)
    # Will be renamed to ahps_lid later in the process
    ahps_col = sites_gdf.pop('nws_lid')
    sites_gdf.insert(0, 'nws_lid', ahps_col)

    # Move the huc column as well, but we do need to keep it
    huc_col = sites_gdf.pop('HUC8')
    sites_gdf.insert(1, 'HUC8', huc_col)

    # Add new processing-specific columns
    sites_gdf.insert(loc=2, column="mapped", value="not set")
    sites_gdf.insert(loc=3, column="status", value="not set")
    sites_gdf.insert(loc=4, column="warnings", value="")

    # Drop list fields (downstream_nwm_features and upstream_nwm_features) if invalid
    # because the are lists and the gpkg does not like it?
    # hummm... or maybe just delete it when it is null? both of those nodes are
    # it is because it has a lists value or unkeyed json node. How do we fix this?  TBD. Check other code
    # note in this file.
    # TODO: Decide what to do with downstream_nwm_features and upstream_nwm_features columns

    sites_gdf = sites_gdf.drop(['downstream_nwm_features', 'upstream_nwm_features'], axis=1, errors='ignore')
    sites_gdf = sites_gdf.astype(
        {
            'metadata_sources': str,
            'nwm_feature_data_downstream_feature_id': str,
            'nws_data_county_code': str,
            'nwm_feature_data_nhd_waterbody_comid': str,
            'nws_data_latitude': str,
            'nws_data_longitude': str,
            'nws_data_zero_datum': str,
            'nwm_feature_data_stream_order': str,
        }
    )

    # sites_gdf.reset_index(drop=True, inplace=True)

    # TODO: re-eval the data_source system.
    # We temp add a column named threshold_data_source, which tracks the original source from the threshold dataset
    # The sites_gdf is updated later, the column will be filled. Yes, it is not the most efficent system, but makes
    # as each rec in the sites_gdf will have the same value. It does however, make it easier to pass the data through
    # the system. When we save the final sites_gdf, we will drop the column.
    # This column is only used by SB processing at this time.
    # HUMMMM !!!!
    # sites_gdf['threshold_data_source'] = ""

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

    # Some SB specific columns we want to create now, filling in what we know now.

    # Create stage-based specific validation columns
    if catfim_type == 'sb':
        sites_gdf['acceptable_coord_acc_code_list'] = str(acceptable_coord_acc_code_list)
        sites_gdf['acceptable_coord_method_code_list'] = str(acceptable_coord_method_code_list)
        sites_gdf['acceptable_alt_acc_thresh'] = float(acceptable_alt_acc_thresh)
        sites_gdf['acceptable_alt_meth_code_list'] = str(acceptable_alt_meth_code_list)
        sites_gdf['acceptable_site_type_list'] = str(acceptable_site_type_list)

    return sites_gdf


def __set_start_files_folders(output_mapping_dir, output_temp_dir, sites_file_path, library_file_path):
    """
    Clean up specific output folders leftover from previous CatFIM runs.

    Removes the mapping folder, compiled sites and library gpkgs and CSVs.

    TODO: Decide about how we should clean up the logs.
    Always keeps the logs folder and maybe nothing else?
    Certainly removing not meta or threshold files.

    TODO:
    Update this to remove the mapping and temp dir, plus all files at this huc_path level.
    Leaving all other folders untouched.

    TODO: Rename this function to be something more clear? Maybe __cleanup_previous_outputs()?
    
    """

    # ================================
    # CLEANUP
    # we specifically cleanup specific files and folders in case the developer wants to keep
    # some other previous test files for secondary runs.

    # This does completely remove and cleanup the mapping folder.

    # Already exists? remove it, it will have gpkg's and tif's for this HUC in it.
    shutil.rmtree(output_mapping_dir, ignore_errors=True)
    os.mkdir(output_mapping_dir)

    shutil.rmtree(output_temp_dir, ignore_errors=True)
    os.mkdir(output_temp_dir)

    if os.path.isfile(sites_file_path):
        os.remove(sites_file_path)

    if os.path.isfile(library_file_path):
        os.remove(library_file_path)

    # if os.path.isfile(discharge_file_path):
    #     os.remove(discharge_file_path)

    # if os.path.isfile(segments_file_path):
    #     os.remove(segments_file_path)

    # if os.path.isfile(huc_thresholds_file_path):
    #     os.remove(huc_thresholds_file_path)

    # if os.path.isfile(library_pre_inun_file_path):
    #     os.remove(library_pre_inun_file_path)

    return  # returns nothing, just a way to help show the end of the function


# This update the sites mapping but also cleans up the library gpkg if applicable
def __update_sites_mapping_status(
    catfim_type,
    sites_file_path,
    library_file_path,
    library_post_mapping_file_path,
    sites_gdf,
    skip_library=False,
):
    '''
    Used in both stage- and flow-based CatFIM.

    Updates the mapping status and status messages for CatFIM sites based on the presence of valid inundation GeoPackage files.

    We look for the library_post_mapping_file_path (a intermediate processing file) and if it exists and all is well
    it will save it as the library_file_path. library_file_path is in the format of {huc}_library.gpkg which is the format
    that catfim_post_processing.py will look for. We only want that file there if all is well.


    Raises:
        SystemExit: If the input sites file does not exist, is empty, or no valid inundation files are found.

    Notes:
        - We should have only two values for mapped, either no, or "not set"
        - If we have a value in the warning column and the mapped is 'not set', then copy messages
          to status, then mapped becomes 'Good'.
          If no value in warning, and mapped is 'not set', then status becomes 'Good'.

    CatFIM Reorg Jan 2026:

    TODO:
    This needs to be rethought.

      - Any time we deliminate all needs to make it to mapping, update the sites file.
      - What do we want to do if we have a catestrophic fail? do we rename the {huc}_sites.gpkg so
        it is not included in the final catfim post processing?
      - When we start mapping, a copy of the sites file will be made calling it someting like
        sites_pre_mapping.gpkg. While mapping is processing, it will never update the
        sites_pre_mapping so the mapping code can be run multiple time. However, when mapping
        is done, it will finish with a sites_post_mapping. It will contain any updated statuses.
      - When we come back from mapping, we check to see if the sites_post_mapping.gkpg and load it in
        which becomes our new final {huc}_sites.gpkg
      - Regardless when we hit __updated_sites.. (or a similar) function, we need to update that
        file only once for column renamed, status updates, etc.
      - But.. how do we handle it while go through all processing?  Do we have a different WIP
        sites file that we keep building on as the master?  Then when we get here we look figure
        out if we aborted / stopped prior to mapping, then use the most recent temp sites.gpkg
        as a starting point for the final?  Do we look for the post procesing version of it?
      - Do we try to do that all in this function? or do we break it up for something like a function
        called __sites_finalization?
      - Do we make sure mapping updates the sites_post_processing to update the "mapped" = yes
        and status = good?  Do we let it update the status column from the warnings column?
        Come to think of it.. that might be good. That way, in theory if we get here and we find
        some that are "not set", that might indicate a code fail.. .humm.... moo-ha-ha (lol)
        That is probably better than looking for the existiance of a library file.

    At a min... we need to plug in the mapping code to make it's own sites_post_mapping so this
    function can decide what to do with it.

    Note: We do have some last minute library finalization we will need. Humm... how does that
    work if we re-run mapping.  What about the last minute case change for the nws_lid column and
    rerun of mapping? hummm.

    Note: the nws_lid column will never get renamed here. Let catfim_post_processing rename
      those columns to ahps_lid when it gets there.
        
    '''

    if len(sites_gdf) == 0:
        msg = f"sites_gdf is empty. Path is {sites_file_path}. Program aborted."
        logging.critical(msg)
        raise Exception(msg)

    # ------------------------------------
    # FINALIZING the sites table
    logging.info(f"Updating sites gdf with finalized site data at {sites_file_path}")
    # For the sites gpkg, leave the column named as nws_lid, we can change it to ahps_lid later in post processing.
    # # TODO hummmmm

    try:
        # We load the library file. It might not be there if something failed in inundation or we early
        # aborts, such as all sites failed the restricted tests.
        # It might also be there but be empty.

        huc_library_gdf = None

        # If the status of all of the sites records is already no, then no need to continue with the library
        valid_lids = sites_gdf.loc[sites_gdf["mapped"] == "not set"]["nws_lid"].values.tolist()
        if not skip_library:
            if library_post_mapping_file_path != "":
                raise Exception(
                    "Internal Error: You have asked to not skip saving the library"
                    " but the library_post_mapping_file_path is empty"
                )

            if library_file_path != "":
                raise Exception(
                    "Internal Error: You have asked to not skip saving the library"
                    " but the library_file_path is empty"
                )

            if len(valid_lids) != 0:
                if not os.path.exists(library_post_mapping_file_path):
                    skip_library = True
                    # TODO: hummm... woudl we have any recs with not set and we are missing the post mapping file?
                    logging.warning(
                        f"The working library file of {library_post_mapping_file_path} does not exist."
                        " This could be correct when there are not any sites that had qualifying library data. "
                        " It could also be code error. If you are unsure, please check the warning / error"
                        "  log files and/or the sites file to be sure."
                    )
                else:
                    huc_library_gdf = gpd.read_file(library_post_mapping_file_path, engine='fiona')
                    if len(huc_library_gdf) == 0:
                        skip_library = True
                        logging.warning(
                            f"The working library file of {library_post_mapping_file_path} is empty and"
                            " a finalized copy will not be created."
                        )
                    else:
                        skip_library = False
            else:  # no valid sites left.
                skip_library = True

        # TODO: Do we want to a code safety check for each all sites that are mapped = no
        # to make sure it has a status? ie) is not 'not set' or ''?

        # All ones with 'mapped' = 'not set' need the 'Good' message added.
        for lid in valid_lids:
            lid_site = sites_gdf.loc[sites_gdf["nws_lid"] == lid].copy()
            status_val = lid_site.iloc[0]['status']
            warning_val = lid_site.iloc[0]['warnings']

            if skip_library is False:
                # Means something failed in mapping, but the reason was not recorded with each sites rec
                # TODO: How do we know if it is a bug that resulted in the missing library file.. hummm
                msg = 'Site resulted with no valid inundated files'
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
                logging.error(msg)
                continue

            # TODO: We need to check the library file to make sure we have at least one rec
            # for this site. If not.. something went wrong in our code likely.

            if status_val != "":
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, 'status'] = warning_val
            else:
                # all are good by this time.
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['yes', 'Good']
        # end of for loop for valid sites

        # need to do a bit of cleanup before saving the final copy

        # TODO: do we want lower case lid values the entire way through? I went with Upper at this point
        # Same for the library data?
        sites_gdf['nws_lid'] = sites_gdf['nws_lid'].str.lower()

        # FB and SB sites and library outputs call it ahps_lid instead of nws_lid. why?
        # well.. we process throughout as nws_lid
        # don't rename that column here. Leave that for post processing
        # sites_gdf.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)
        sites_gdf.rename(
            columns={'identifiers_nwm_feature_id': 'nwm_seg', 'identifiers_usgs_site_code': 'usgs_gage'},
            inplace=True,
        )

        sites_gdf = sites_gdf.drop(columns=['warnings'], errors='ignore')

        logging.info(
            f"Saving the adjusted final copy of the sites file to {sites_file_path}"
            " for catfim post processing to load"
        )
        sites_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False)

        # Let catfim_post_processing make the final CSV (rolled up for all hucs). We don't need one here.

        if skip_library is False:
            # ----------------------------
            # Finalization of the library file.

            if catfim_type == "sb":
                huc_library_gdf.rename(
                    columns={
                        'datum_adj_ft': 'dtm_adj_ft',
                        'dadj_w_ft': 'datum_adj_wse_ft',
                        'dadj_w_m': 'dadj_w_m',
                    },
                    inplace=True,
                )

            # TODO: is there any last minute changes we need? WIP columns to drop?
            # we save it as the final adjusted name library file using the format of {huc}_library.gpkg,
            # which is the format catfim_post_processing looks for. (_library.gpkg)

            # TODO:
            # What do we want to do with the sites 3 stage columns (stage, stage_umi, s_src)
            # if they are -1 or empty?  Especially with stage being a double.

            # What about the q, q_uni and q_src, which are the three flow based values
            # from the threshold data.
            # in current code, SB has all three of the q columns as string and they can be empty
            # but in FB, the "q" col is a float. But it can be -1 as well.
            # HUMMM.

            # TODO:
            # See other notes about this column. Do we want to keep it or drop it?
            # sites_gdf.drop("threshold_data_source", axis=1, inplace=True, e

            logging.info(
                f"Saving name adjusted final copy of the library files to {library_file_path}"
                " for catfim post processing to load"
            )
            huc_library_gdf.to_file(
                library_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False
            )
            # Note: We do not create a CSV here because post-processing will do that.

        else:
            logging.info("Skipping library finalization.")

    except Exception:
        logging.critical(traceback.format_exc())

        # TODO: Humm.. Do we rethrow? or just log and stop
        # what will AWS do if we let a exception go back out. It is fine if the exception is

    return  # nothing


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
