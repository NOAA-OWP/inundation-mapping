#!/usr/bin/env python3

import argparse
import logging
import os
import random
import shutil
import time
import traceback
from datetime import datetime, timezone

import catfim.generate_categorical_fim_flows as gcf
import catfim.generate_categorical_fim_mapping as gcfm
import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv

import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf
from src.utils.shared_variables import VIZ_PROJECTION
from tools.tools_shared_functions import ngvd_to_navd_ft
from tools.tools_shared_variables import (
    acceptable_alt_acc_thresh,
    acceptable_alt_meth_code_list,
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


def process_huc(huc, output_folder):
    '''
    Wrapper for all HUC-specific CatFIM processing.

    Steps:

    - Setup for HUC processing, creates HUC folders if they do not exist
    - Loads site metadata and validates sites
    - Retrieves and processes threshold data if valid sites remain for HUC
    - Processes threshold data (creates a HUC-level library file with all of
        the site/magnitude combinations that are still valid up to this point
        and creates the flow data for FB)
    - Runs CatFIM mapping

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code for the area to process.
    output_folder - STR
        Filepath of the CatFIM output folder for the current run.

    Returns
    -------
    huc - STR
        Hydrologic unit code.
    is_success - BOOL
        Indicator of whether the HUC processing encountered errors.

    '''

    # Setup for HUC processing
    is_logging_loaded = False
    is_success = True

    try:
        # =========================================

        # A bit of start staggering to help not overload the MP (0.1 milliseconds to 2 secs)
        time_delay_mms = random.randint(100, 2000) / 1000
        time.sleep(time_delay_mms)

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
        huc_path, output_folder = csf.validate_huc_inputs(huc, output_folder)

        # Create the huc folder if it does not exist
        os.makedirs(huc_path, exist_ok=True, mode=0o777)

        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

        catfim_type = os.getenv('CATFIM_TYPE')

        # =========================================
        # Setup logging and output folders

        # Create filepath variables
        (
            output_mapping_dir,
            output_temp_dir,
            output_log_dir,
            sites_pre_mapping_file_path,
            sites_post_mapping_file_path,
            library_pre_mapping_file_path,
            library_post_mapping_file_path,
        ) = csf.make_huc_mapping_filepaths(huc, catfim_type, huc_path)

        # Remove some preexisting files and folders from prior runs
        # __clean_up_previous_outputs(output_mapping_dir, output_temp_dir) # Moved to gen cat fim

        # Make HUC mapping folders
        make_huc_mapping_folders(output_mapping_dir, output_temp_dir, output_log_dir)

        # TODO: AWS BUG Jan 2026 - Why are my logs read only for all but the owner? other apps don't I think.
        # I can not delete them to cleanup if I want too. huh? Better check other apps that use setup_file_logger

        # HUC level logs will be initially saved in the temp dir and then they will be copied into the HUC/logs folder
        # at the end of processing (which will help us ensure that the logs we compile up are from the current run)
        log_file_path = sf.setup_file_logger(output_temp_dir, f"process_huc_{huc}")
        is_logging_loaded = True

        logging.info("")
        logging.info(f"Starting HUC-level CatFIM for HUC {huc} ;  {dt_string} (UTC)")
        print(
            f"... Logs for this HUC will be saved to {log_file_path} for now, but copied over to {huc}/logs later"
        )
        print("")

        # ---

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


        # Notes on file naming conventions:
        # - Not all intermediates will go to temp. It depends if we want to keep it long term or not.
        # - Make sure that only the final edition of the product ends in _sites.gpkg or _library.gpkg # TODO: Update, I think we've changed this
        #   as catfim_post_processing.py will look for those conventions.
        #   Don't let any intermediates folow that convention exactly in this root huc dir.
        #   Always add something after _sites and _library

        # =========================================
        # Load site metadata and validate sites
        section_start_dt = datetime.now(timezone.utc)
        continue_processing = True

        logging.info(f"{huc} - Loading sites metadata...")

        # Get HUC-level metadata and save it as a GeoDataFrame
        metadata_json, sites_gdf = csf.get_huc_metadata(huc, huc_path)
        sites_gdf = __setup_sites_gdf(sites_gdf, os.getenv('CATFIM_TYPE'))

        # Check for restricted sites and updates the sites GeoDataFrame accordingly
        valid_nwm_lids, sites_gdf = csf.check_for_restricted_sites(sites_gdf, os.getenv('CATFIM_TYPE'))
        # TODO: Test that restricted sites are being removed as expected

        # If no valid sites remain, save the meta file we have with the new error messages, then abort.
        if len(valid_nwm_lids) == 0:
            logging.info(
                f"All sites associated to HUC {huc} are restricted. No more processing will continue."
            )

            # Update mapping status and save the final sites file
            csf.finalize_sites_mapping_status(
                huc,
                catfim_type,
                sites_post_mapping_file_path,
                library_post_mapping_file_path,
                sites_gdf,
                None,
            )
            continue_processing = False

        # =========================================
        # Retrieve and process threshold data if valid sites remain for HUC
        if continue_processing is True:

            # Save sites to a file checkpoint (Yes.. to the master copy)
            logging.info(f"{huc} - Saving sites, pre flow and mapping, at {sites_pre_mapping_file_path}")
            sites_gdf.to_file(
                sites_pre_mapping_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False
            )

            logging.info(f"{huc} - {len(valid_nwm_lids)} sites remaining after validation: {valid_nwm_lids}")
            print("")

            # ---------------------
            # Retrieve and process threshold data

            section_start_dt = datetime.now(timezone.utc)
            logging.info(f"{huc} - Loading flow and threshold data for all valid sites...")

            # Get threshold data (stages and flows) for all valid sites in this HUC
            threshold_huc_df = gcf.get_threshold_data(huc, huc_path, valid_nwm_lids)

            # Note: It is possible threshold_huc_df can come back empty if huc has no site(s) with threshold data.
            # It is okay if this df is empty for now.

            # Process threshold data (creates a HUC-level library file with all of the site/magnitude combinations
            # that are still valid up to this point and creates the flow data for FB)

            section_start_dt = datetime.now(timezone.utc)
            logging.info(f"{huc} - Processing initial flow and threshold data for all valid sites...")

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

            # Save a copy of the sites_gdf at this point
            # We may have some or all sites that have failed, so we want the master sites rec saved

            logging.info(
                f"{huc} - Saving sites data post threshold processing at {sites_pre_mapping_file_path}"
            )
            sites_gdf.to_file(
                sites_pre_mapping_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", index=False
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

            duration_msg = sf.calculate_duration_msg(section_start_dt)
            logging.info(f"{huc} - End of initial processing of flow and threshold data - {duration_msg}")

            # ---------------------
            # Validate and save the library outputs at this point, skip to the end if there's no sites or library records

            if len(huc_library_df) > 0:
                huc_library_df.to_csv(library_pre_mapping_file_path, index=False)
                logging.info(f"{huc} - Saving initial library file to {library_pre_mapping_file_path}")
            else:
                logging.warning(
                    f"{huc} - There are no valid huc_library recs at this point. Skipping to finalization"
                )

                # Update mapping status and save the final sites file
                csf.finalize_sites_mapping_status(
                    huc,
                    catfim_type,
                    sites_post_mapping_file_path,
                    library_post_mapping_file_path,
                    sites_gdf,
                    None,
                )
                continue_processing = False


            # Check for valid LIDs and abort processing if no valid sites remain
            # If there's no valid LIDs we won't have any library files, but we still need to finalize
            # sites.gdf because it will still be part of the final product rollup.
            if continue_processing == True:

                valid_lids = sites_gdf.loc[sites_gdf["mapped"] != "no"]["nws_lid"].values.tolist()

                if len(valid_lids) == 0:
                    logging.info(
                        f"{huc} - There are no remaining sites to process. Skipping to finalization."
                    )

                    # Update mapping status and save the final sites file
                    csf.finalize_sites_mapping_status(
                        huc,
                        catfim_type,
                        sites_post_mapping_file_path,
                        library_post_mapping_file_path,
                        sites_gdf,
                        None,
                    )
                    continue_processing = False

        else:
            logging.info(f"{huc} - Continue processing is False, bypassing threshold data processing.")

        # =========================================
        # Process stage-based elevation data

        if catfim_type == "sb":
            if continue_processing is True:

                logging.info(f"{huc} - Start processing stage-based elevation data")

                # TODO: Discuss: what is a good name (__process_elevations) for this. Maybe a new file called catfim_data_processing.py? (but leave mapping to focus on inundation)
                # Should we keep all of the functions in this file for processing flow / stage / threshold data? or maybe a seperate file?
                # I'm partial to not making a new file, because we already have so many. But I could be convinced otherwise. -Emily

                # Process the elevation data
                sites_gdf, huc_library_df, has_critical_error = __process_elevations(
                    sites_gdf, huc_library_df, huc, huc_path, output_temp_dir, library_pre_mapping_file_path
                )

                # Abort early if there's a critical error
                if has_critical_error is True:
                    logging.info(
                        f"{huc} - Critical error found and aborting processing"
                        " and logged already in __process_elevations."
                        "  Skipping to sites finalization."
                    )

                    # Update mapping status and save the final sites file
                    csf.finalize_sites_mapping_status(
                        huc,
                        catfim_type,
                        sites_post_mapping_file_path,
                        library_post_mapping_file_path,
                        sites_gdf,
                        None,
                    )
                    continue_processing = False

                    # We abort early and this function should take care of that even with early abort.
                    # Depending on where we failed, we may have something in the huc_library_df

                else:
                    # Checkpoint
                    logging.info(
                        f"{huc} - Saving sites data post-elevation processing at {sites_pre_mapping_file_path}"
                    )
                    sites_gdf.to_file(
                        sites_pre_mapping_file_path,
                        driver='GPKG',
                        crs=VIZ_PROJECTION,
                        engine="fiona",
                        index=False,
                    )

                    if len(huc_library_df) > 0:
                        __update_library_csv(sites_gdf, huc_library_df, library_pre_mapping_file_path)

                    # We may have dropped some library recs above. See if there are any left and abort if not.
                    if len(huc_library_df) == 0:
                        logging.warning(
                            f"{huc} - After SB elevations processing, there are no more library recs to process."
                            " Skipping to finalization."
                        )

                        # Update mapping status and save the final sites file
                        csf.finalize_sites_mapping_status(
                            huc,
                            catfim_type,
                            sites_post_mapping_file_path,
                            library_post_mapping_file_path,
                            sites_gdf,
                            None,
                        )
                        continue_processing = False

            else:
                logging.info(
                    f"{huc} - Continue processing is False, bypassing stage-based elevation data processing."
                )
        # else: if it is flow-based, we skip the elevation processing

        # =========================================
        # Start CatFIM mapping for the HUC
        if continue_processing is True:

            # We can also add temp additional columns in the mapping version of sites if it helps
            # with mapping processing, we just have to make they are removed after finalization here.
            # Mapping has to be fully independent.

            # CatFIM Reorg Notes (Jan 26):
            # We don't really want to return anything, unless it is a catastrophic exception.
            # We know the final files that should exist if all went well from mapping so we
            # we can just reload them when we are ready.
            # and Yes... we will be reloading and replacing our sites_gdf from the mapping version
            # as it might have updated it.
            # Mapping will know the file names for the segments and discharge

            # Process CatFIM mapping
            gcfm.process_mapping(
                huc,
                huc_path,
                catfim_type,
                output_mapping_dir,
                output_temp_dir,
                sites_pre_mapping_file_path,
                sites_post_mapping_file_path,
                library_pre_mapping_file_path,
                library_post_mapping_file_path,
            )
        
            # Currently we don't do any safety checks here because it
            # wouldn't change how we wrapped up processing and issues
            # will be checked for shortly.

            # =========================================
            # Finalize all final sites and library files for this HUC
            # Assumes all logging or finalization was jumped to earlier.
            
            # TODO: Decide if we want a separate final and post-mapping filepath....
            # We don't want mapping to ever change the copy coming in here as we need
            # to be able to re-run mapping as many times as it wants. It will be updating
            # the sites file as it progresses but will make its own called site_post_processing.gpkg
            # for this code to pick up later in our finalization.
            # For now, we do NOT have a separate final HUC filepath.

            csf.finalize_sites_mapping_status(
                huc,
                catfim_type,
                sites_post_mapping_file_path,  # Output path
                library_post_mapping_file_path,  # Output path
                sites_post_mapping_file_path,  # Data to finalize
                library_post_mapping_file_path,  # Data to finalize
            )

        else:
            logging.info(
                f"{huc} - Continue processing is False, bypassing mapping and post-mapping finalization."
            )

        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(f"{huc} - End HUC-level processing - {duration_msg}")
        logging.info("")

    except Exception as ex:
        is_success = False
        trace_error = traceback.format_exc()

        err_msg = f"{huc} - A critical error has occurred in HUC-level processing. Detail: {trace_error}"
        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # re-raise the exception, mostly for AWS
        raise ex

    return huc, is_success


def __process_elevations(
    sites_gdf, huc_library_df, huc, huc_path, output_temp_dir, library_pre_mapping_file_path
):
    """
    Only used by stage-based CatFIM.

    Provides site-specific datum adjustment and elevation data repairs as needed.

    Arguments
    ---------
    sites_gdf - GeoPandas GeoDataFrame
        Sites data table.
    huc_library_df - Pandas DataFrame
        Library data table.
    huc - STR
        Hydrologic Unit Code.
    huc_path - STR
        Filepath to the HUC-specific CatFIM output folder.
    output_temp_dir - STR
        Filepath to the HUC-level temp directory.
    library_pre_mapping_file_path - STR
        Filepath to the library pre-mapping file.

    Returns
    -------
    sites_gdf - GeoPandas GeoDataFrame
        Sites data table.
    updated_huc_library_df - Pandas DataFrame
        Library data table with updated elevations.
    has_critical_error - BOOL
        Indication of whether a critical error has occurred,

    """

    has_critical_error = False

    # Get data download source from the library df
    if 'Manual_Input' in huc_library_df['s_src'].values:
        download_source = "Manual_Input"
    else:
        download_source = "WRDS"
    # logging.info(f"{huc} - Data downloaded from {download_source}") # TEMP DEBUG

    # Initialize output dataframes
    updated_huc_library_df = pd.DataFrame()  # a replacement huc_library_df
    acceptable_usgs_elev_df = pd.DataFrame()  # empty in case we do not load it via non Manual Input

    # Get a list of sites that are still valid (there should be at least one)
    valid_lids = sites_gdf.loc[sites_gdf["mapped"] != "no"]["nws_lid"].values.tolist()

    # Read in the USGS elevation data and create the exclusion status column
    if download_source != 'Manual_Input':  # Note: Manual input data does not use usgs_elev_table

        # ------------------------
        # CatFIM Reorg Note (Jan 26): The usgs elev table was previously in iterate_through_huc_stage_based
        # but wasn't used until later in the code in that function. Now it is done much earlier.

        # Get the USGS elevation table (it is already filtered to the HUC level and has data for all sites in this HUC)
        usgs_elev_table_file_name = 'usgs_elev_table.csv'
        src_usgs_elev_table = os.path.join(os.getenv("FIM_RUN_DIR"), huc, usgs_elev_table_file_name)

        if not os.path.isfile(src_usgs_elev_table):
            msg = "HUC-level USGS elevation table missing from FIM run directory"
            logging.error(f"{huc} - {msg}")

            # If this happens, all sites in this HUC will fail and have this same message, so we can update them all
            sites_gdf = csf.update_line_status_or_warning("all", sites_gdf, msg, set_mapped_to_no=True)
            has_critical_error = True  # This will stop further processing downstream

            return sites_gdf, [], has_critical_error

        # Make a copy to the local HUC folder to lower the chance of file collisions.
        # (Copying it to HUC folder and not a temp drive.)
        local_copy_usgs_elev_table = os.path.join(huc_path, usgs_elev_table_file_name)
        shutil.copyfile(src_usgs_elev_table, local_copy_usgs_elev_table)

        # Read back in the local copy of the USGS elevation table
        usgs_elev_df = None
        usgs_elev_df = pd.read_csv(local_copy_usgs_elev_table)  # Only used here

        # Check if there are any records in the USGS elevation table, if not log an error and update all sites to failed with the same message. We can check for this before we do the filtering below, which will give us a more clear error message if there is no data at all for any sites in the HUC. If we wait to check after the filtering, we might have a more generic error message that just says "no usable elevation data" which would be less clear if the issue is that there is no data at all vs. there is data but it just doesn't meet our criteria.
        if len(usgs_elev_df) == 0:
            msg = f"USGS elevation table (from FIM outputs) empty for HUC {huc}"
            logging.error(
                f"{huc} - {msg}, expected at path {src_usgs_elev_table}. If file is there, an error occurred while copying to {huc_path}"
            )

            # If this happens, all sites in this HUC will fail and have this same message, so we can update them all
            sites_gdf = csf.update_line_status_or_warning("all", sites_gdf, msg, set_mapped_to_no=True)
            has_critical_error = True  # This will stop further processing downstream

            return sites_gdf, [], has_critical_error

        # Give error if needed.
        # THEN we can filter below using the __create_acceptable_usgs_elev_df() function and give an
        # error if that filtering leaves us with no usable elevation data.

        # ------------------------
        # Creates an updated USGS elevation table with a descriptive USGS exclusion status column
        # Note: Doesn't filter the df, that happens next in __adj_dem_elevation_val()
        acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df)

        # Error out if the acceptable elev df is gone or empty. This would probably
        # only occur if a code error occurred, so this message is worth looking into
        # if it appears.
        if len(acceptable_usgs_elev_df) == 0:
            msg = "USGS elevation table empty for HUC after processing exclusion criteria."  # was "Unable to find gage data"
            logging.error(f"{huc} - {msg}")
            # If this happens, all sites in this HUC will fail and have this same message, so we can update them all
            sites_gdf = csf.update_line_status_or_warning("all", sites_gdf, msg, set_mapped_to_no=True)
            has_critical_error = True  # This will stop further processing downstream
            return sites_gdf, [], has_critical_error

    else:  # If the source is manual input, we skip the above elevation filtering
        logging.info(f"{huc} - Skipping elevation checks and datum adjustment for Manual Input source")

    # TODO: Safety check: Do a quick check to make sure there are no huc_library records with -1 in the stage column.
    # should not be any.

    for lid in valid_lids:

        # Find the single lid record from the sites_gdf and applicable library rows.
        # You can update the iloc'd record and replace the original row in the
        # parent as long as you can find the correct index

        # Careful about using these two, wahat your function scoping rules for df's being
        # updated in functions.

        lid_sites_gdf = sites_gdf.loc[sites_gdf["nws_lid"] == lid].copy()
        lid_library_df = huc_library_df[huc_library_df["nws_lid"] == lid].copy()

        if len(lid_sites_gdf) != 1:
            raise Exception(f"{huc} - Internal error: There should be exactly one lid rec here")

        if len(lid_library_df) == 0:
            # If not, the sites should already have been updated to mapped is no and correct
            # status message applied.
            raise Exception(
                f"{huc} - Internal error: There should be at least one valid lid library rec by now"
            )

        # Make an "rfc_stage" column (for documentation of the data source)
        lid_library_df['rfc_stage'] = lid_library_df['stage']

        # TODO: rfc_stage, but final library calls this rfs_stage (typo?)
        # uncorrect WRDS value before we adjusted it for inundation
        # Changed this to rfc_stage for processing. Fix in finalization?

        # Get the site altitude from the USGS data
        # lid_altitude = lid_sites_gdf.iloc[0]['usgs_data_altitude'] # Previous method - elevation was pulled from the USGS columns (even if the datum and rating curve were from NRLDB)
        
        # We need to be getting the elev that matches the rating curve that we're using, not just hard coding to USGS I think!!
        # New as of Spring '26
        rating_curve_source = lid_library_df.iloc[0]["q_src"]

        # Get the site elevation from the same source as the rating curve source
        # lid_altitude_ft should be in FT, whether it's from USGS or NRLDB (was lid_altitude)
        if rating_curve_source == 'USGS Rating Depot':
            lid_altitude_ft = lid_sites_gdf['usgs_data_altitude'].item()
        elif rating_curve_source == 'NRLDB':
            lid_altitude_ft = lid_sites_gdf['nws_data_zero_datum'].item()
        elif rating_curve_source == 'Manual_Input':
            lid_altitude_ft = lid_sites_gdf['usgs_data_altitude'].item()
        else:
            err_msg = f'Unknown rating curve source: {rating_curve_source}, unable to get site elevation'
            logging.warning(f"{lid}: {err_msg}")
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, err_msg, set_mapped_to_no=True)
            continue

        lid_altitude_ft = float(lid_altitude_ft)  # Ensure it's a float for processing later

        logging.info(
            f"{lid}: Rating curve and elevation val source: {rating_curve_source}, site elev value: {lid_altitude_ft}"
        )  # TEMP DEBUG

        if lid_altitude_ft is None or lid_altitude_ft == 0:
            # Jan 2026: In previous versions not all recs stopped here when this failed
            # some continued on and ultimatly failed down the road.
            msg = f'AHPS site altitude value is invalid (value is {lid_altitude_ft})'
            logging.warning(f"{huc} - {lid}: {msg}")
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)
            continue

        # Adjust the elevation value (if data is not from Manual Input)
        if download_source != 'Manual_Input':

            # Get the DEM-adjusted elevation value (prioritize the val that isn't from branch 0)
            lid_usgs_elev, err_msg = __adj_dem_elevation_val(acceptable_usgs_elev_df, lid, huc)

            # Exit and update the line status if an error was returned from adjust DEM elevation val
            if err_msg != "":                
                sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, err_msg, set_mapped_to_no=True)
                continue

            # Jan 2026: Previously we ran __filter_bad_usgs_gage_data() here but we took
            # it out because it was duplicate functionality already done in  __adj_dem_elevation_val()

            # Determine the vertical datum adjustment (ft) to convert the datum of the rating curve to NAVD88
            datum_adj_nodata_value = csf.ELEV_NODATA_VALUE
            datum_adj_ft, err_msg = __adjust_datum_ft(
                lid_sites_gdf, lid_library_df, lid, datum_adj_nodata_value
            )

            # Exit and update line status if an error was returned from adjust datum
            if err_msg != "":
                sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, err_msg, set_mapped_to_no=True)
                continue

            # If an error wasn't returned but the datum adjustment value is equal to the nodata value,
            # that means something went wrong with the calculation and we should log an error and skip this site.
            if datum_adj_ft == datum_adj_nodata_value:
                msg = "Unable to determine datum adjustment for site, error occurred during datum adjustment calculation"
                logging.error(f"{huc} - {lid}: {msg}")
                sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)
                continue

        else:  # If source is manual input, skip the above elevation filtering
            # For manual input, we are not pulling the lid_usgs_elev vaalue from the USGS elevation table,
            # but rather we are just using the site elevation from the metadata input file. That means we
            # are not inundating the REMs with the elevation from the hydroconditioned FIM DEMs, which could
            # make the mapping slightly worse. If we end up creating the proper USGS elevation tables
            # for the manual input sites, this could change. TODO - incorporate usgs elevation tables if we end up creating them

            # Convert lid_altitude to meters to match non-manual input units
            lid_usgs_elev = csf.feet_to_meters(lid_altitude_ft)
            datum_adj_ft = 0  # no datum adjustment for manual input

        lid_altitude_m = csf.feet_to_meters(lid_altitude_ft)

        # Add datum adjustment info to site dataframe
        lid_library_df['datum_adj_ft'] = (
            datum_adj_ft  # Datum adjustment in feet (could be zero), created using WRDS metadata
        )
        lid_library_df['lid_alt_ft'] = (
            lid_altitude_ft  # Site elevation (zero datum) in feet, from WRDS metadata
        )
        lid_library_df['lid_alt_m'] = (
            lid_altitude_m  # Site elevation (zero datum) in meters, from WRDS metadata
        )
        lid_library_df['lid_usgs_elev'] = (
            lid_usgs_elev  # Site elevation in meters, from the hydroconditioned FIM DEMs (temp column for processing)
        )
        # Note: for lid_usgs_elev, USGS refers to the fact that the table is stored in the context of the USGS APHS sites,
        # not that the elevation is actually from the USGS

        # =====================
        # Check for large discrepancies between the elevation values from WRDS and HAND.
        #   (because that could cause bad mapping.) Manual_Input will have no elev
        #   disparity because it's from the the same value.

        # TODO: test this elevation_diff code -Rob
        logging.info('')  # TEMP DEBUG
        logging.info(f'{lid} - datum_adj_ft: {datum_adj_ft}')  # TEMP DEBUG
        logging.info(f'{lid} - lid_altitude_ft: {lid_altitude_ft}')  # TEMP DEBUG
        logging.info(f'{lid} - lid_altitude_m: {lid_altitude_m}')  # TEMP DEBUG
        logging.info(f'{lid} - lid_usgs_elev: {lid_usgs_elev}')  # TEMP DEBUG
        logging.info('')  # TEMP DEBUG


        # Calculate the elevation difference between the two elevation values
        elevation_diff_m = lid_usgs_elev - lid_altitude_m
        diff_rounded_m = round(elevation_diff_m, 2)
        # Note: elevation_diff_m and diff_rounded are used for these tests only
        # and are not part of any logic or are saved to any df's

        # Removed this logging for now, can use for debugging if needed. We will log the more important elevation differences below.
        # # Log minor elevation difference information - not an error
        # if elevation_diff_m > 0:
        #     logging.info(f"{huc} - {lid}: USGS elev is higher than HAND elev by {diff_rounded_m} m")
        # elif elevation_diff_m < 0:
        #     logging.info(f"{huc} - {lid}: USGS elev is lower than HAND elev by {abs(diff_rounded_m)} m")

        # Throw an error for elevation differences greater than 10 meters
        if abs(elevation_diff_m) > 10:
            # err_msg = f'Large discrepancy in elevation estimates from gage and HAND - {diff_rounded_m} m difference' # TODO: Clean up

            if lid_altitude_m > lid_usgs_elev:
                msg = f"Large discrepancy in elevation estimates: AHPS gage zero datum > HAND DEM elevation by {abs(diff_rounded_m)} m"
                logging.warning(f"{huc} : {lid} - {msg}")
                sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)

            elif lid_usgs_elev > lid_altitude_m:
                msg = f"Large discrepancy in elevation estimates: HAND DEM elevation > AHPS gage zero datum by {abs(diff_rounded_m)} m"
                logging.warning(f"{huc} : {lid} - {msg}")
                sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)

            # We will clean up the huc_library folder shortly
            continue

        # Log a warning for elevation differences of 5-10 meters (but continue on)
        elif abs(elevation_diff_m) > 5:

            if lid_altitude_m > lid_usgs_elev:
                msg = f"Moderate discrepancy in elevation estimates: AHPS gage zero datum > HAND DEM elevation by {abs(diff_rounded_m)} m"
                logging.warning(f"{huc} : {lid} - {msg}")

            elif lid_usgs_elev > lid_altitude_m:
                msg = f"Moderate discrepancy in elevation estimates: HAND DEM elevation > AHPS gage zero datum by {abs(diff_rounded_m)} m"
                logging.warning(f"{huc} : {lid} - {msg}")

        # =====================
        # Check whether stage value is actually a WSE value, and fix if needed

        # The lid_library_df has a rec per stage that is still valid at this point.
        # It already dropped all recs with -1, 0 or None

        # Get lowest stage value
        lowest_stage_val = lid_library_df['stage'].min()

        # Check if the lowest stage value is greater than the site altitude and also greater than the max stage threshold,
        # which would indicate that the stage value is actually being stored as a WSE value.
        # If this is the case, we will subtract the site elevation from all of the stage values to convert them back to stage values.
        if (lowest_stage_val > lid_altitude_ft) and (lowest_stage_val > csf.MAX_STAGE_THRESHOLD):
            new_stage_example = lowest_stage_val - lid_altitude_ft
            lid_library_df['stage'] = lid_library_df['stage'] - lid_altitude_ft
            logging.warning(
                f"{huc} - {lid}: Lowest stage val ({lowest_stage_val}) > elev ({lid_altitude_ft}) and > max stage threshold ({csf.MAX_STAGE_THRESHOLD}),"
                f" which indicates that the stage is being stored as WSE. Subtracted elev from stage vals to fix. Lowest stage is now {new_stage_example}"
            )

            # Update the sites gdf with a warning of this adjustment, but we won't set mapped to no
            msg = 'Stage value stored as WSE detected, adjusted val by subtracting site elevation.'
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=False)


        updated_huc_library_df = pd.concat([updated_huc_library_df, lid_library_df], ignore_index=True)

    if len(updated_huc_library_df) > 0:
        updated_huc_library_df.to_csv(library_pre_mapping_file_path, index=False)
        logging.info(
            f"{huc} - Saving updated stage based library after elevations file to {library_pre_mapping_file_path}"
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

    Arguments
    ----------
    usgs_elev_df - pd.DataFrame
        DataFrame containing USGS elevation data with required columns: usgs_data_alt_method_code, usgs_data_site_type, usgs_data_alt_accuracy_code
 
    Returns
    ---------
    acceptable_usgs_elev_df - pd.DataFrame
        DataFrame with an added 'usgs_exclusion_status' column indicating acceptability.

    CatFIM Reorg Notes (Jan 2026)
    -----------------------------

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
        msg = "An error has occurred inside create_acceptable_usgs_elev_df function."

        logging.critical(msg)
        logging.critical(traceback.format_exc())

        raise ex

    return acceptable_usgs_elev_df


def __adjust_datum_ft(lid_sites_gdf, lid_library_df, lid, datum_adj_nodata_value):
    '''
    Used in stage-based CatFIM.

    Determines the vertical datum adjustment (in feet) to convert the datum of the
    rating curve to NAVD88.

    Uses the rating curve source and metadata to get the correct vertical datum and CRS.

    It applies custom workarounds for known sites with special datum or CRS requirements,
    and attempts to compute the adjustment using the NOAA VDatum service when necessary.

    Arguments
    ----------
    lid_sites_gdf - GPD.GeoDataFrame
        Sites geodataframe for the site.
    lid_library_df - PD.DataFrame
        Library dataframe for the site.
    lid - STR
        Site ID.
    datum_adj_nodata_value - FLOAT
        A NoData value to return if the datum adjustment cannot be determined.

    Returns
    -------
    datum_adj_ft - float or None
        The vertical datum adjustment in feet to convert to NAVD88, or the nodata value if adjustment could not be determined.
    err_msg - STR
        Error message, if one appears. Otherwise just blank string

    Notes
    -----
        - Special handling is included for sites with known datum or CRS issues.
        - If the datum is already NAVD88 or equivalent, the adjustment is 0.0.
        - If the datum is NGVD29 or similar, an adjustment is attempted using the NOAA VDatum service.
        - If errors occur during adjustment, appropriate messages are logged and returned.
        - While we do not update the lid_sites_gdf here, we do the logging as sometimes it is an error
          and sometimes a warning.

    TODO: Aug 2024: This whole parts needs revisiting. Lots of lid data has changed and this
    is all likely very old. (Need to revisit special cases.)
    '''

    datum_adj_ft = datum_adj_nodata_value

    err_msg = ""

    # ---------------------------
    # Determine source of interpolated threshold flows, this will be the rating curve that will be used.

    # TODO: Why are we using flow source data to do logic calcs and not the stage columns # TODO: Check if this is still true?
    # see the notes below about using the rating_curve_source - Discuss

    # Yes... flow data even though we are processing stage data, see notes lower # TODO: Clean up comment?

    # Get the rating curve source
    rating_curve_source = lid_library_df.iloc[0]["q_src"]
    if rating_curve_source is None or rating_curve_source == "":
        err_msg = 'No source for rating curve'
        logging.warning(f"{lid}: {err_msg}")
        return datum_adj_ft, err_msg

    # Get the datum so we can adjust to NAVD if necessary.
    # This is getting it from the flow (q) columns

    # TODO: We need to look more into this and also look deeper at the rating curve values
    # see library_pre_inundation.csv the original huc threshold file.
    # Jan 2026: Function was renamed from get_datum to __get_datum_from_df

    # TODO: I don't think this is correct... we should be getting the datum from the
    # metadata file not a df, right?

    # Why did we swtich this from just using get_datum() from the shared functions file?

    nws_datum_info, usgs_datum_info = __get_datum_from_df(lid_sites_gdf)  # current reorg setup

    # If rating curve source is 'Manual_Input', we will not be running this function
    if rating_curve_source == 'USGS Rating Depot':
        datum_data = usgs_datum_info
    elif rating_curve_source == 'NRLDB':
        datum_data = nws_datum_info
    else:
        err_msg = f'Unknown rating curve source: {rating_curve_source}'
        logging.warning(f"{lid}: {err_msg}")
        return datum_adj_ft, err_msg

    logging.info(f"{lid}: Rating curve source is {rating_curve_source}")

    # ---------------------------
    # Perform site-specific data corrections

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
        datum_data.update(crs='NAD83') # TODO: Double check hardcoded adjustments
        logging.info(f"{lid}: adjust_datum_ft - CRS manually set to NAD83")

    # SPECIAL CASE: Site BMBP1
    if lid.lower() == 'bmbp1':  #TODO: Double check hardcoded adjustments
        # SPECIAL CASE: Workaround for "bmbp1" where the only valid datum is from NRLDB (USGS datum is null).
        # Modifying rating curve source will influence the rating curve and
        #   datum retrieved for benchmark determinations.
        rating_curve_source = 'NRLDB'

        # SPECIAL CASE: Workaround for bmbp1; CRS supplied by NRLDB is mis-assigned (NAD29) and
        #   is actually NAD27.
        # This was verified by converting USGS coordinates (in NAD83) for bmbp1 to NAD27 and
        #   it matches NRLDB coordinates.
        datum_data.update(crs='NAD27')
        logging.info(
            f"{lid}: adjust_datum_ft - CRS manually set to NAD27, rating curve source manually set to NRLDB"
        )


    # SPECIAL CASE: Custom workaround these sites have poorly defined vcs from WRDS. VCS needed to ensure
    #   datum reported in NAVD88.
    # If NGVD29 it is converted to NAVD88.
    # bgwn7, eagi1 vertical datum unknown, assume navd88
    # fatw3 USGS data indicates vcs is NAVD88 (USGS and NWS info agree on datum value).
    # wlln7 USGS data indicates vcs is NGVD29 (USGS and NWS info agree on datum value).
    if lid.lower() in ['bgwn7', 'eagi1', 'fatw3']:  # TODO: Double check hardcoded adjustments
        datum_data.update(vcs='NAVD88')
        logging.info(f"{lid}: adjust_datum_ft - VCS manually set to NAVD88")
    elif lid.lower() == 'wlln7':
        datum_data.update(vcs='NGVD29')
        logging.info(f"{lid}: adjust_datum_ft - VCS manually set to NGVD29")

    # ---------------------------
    # Check for unknown datums. If the datum is still unknown, we can't do datum
    # adjustments and the site should be skipped with a warning.

    # If datum not supplied, skip to new site
    datum = datum_data.get('datum', None)

    if datum is None:
        err_msg = f'Datum info unavailable from (source {rating_curve_source})'
        logging.warning(f"{lid}: {err_msg}")
        return datum_adj_ft, err_msg

    unknown_list = ['Unknown', 'UNK', 'UNKN', 'none', '']  # TODO: Add to shared variables

    if datum in unknown_list:
        err_msg = f'Datum info unavailable from (source {rating_curve_source}), datum is {datum}'
        logging.warning(f"{lid}: {err_msg}")
        return datum_adj_ft, err_msg

    # ---------------------------
    # Check for typos in the horizontal datum data
    # Temp workaround to handle incorrect WRDS horizontal datum entries. TODO: Remove once WRDS error is corrected?

    # These rules were developed by looking at the various ways that the horizontal datum was misspelled in the WRDS data
    # in March 2026. Should be periodically updated to make sure we capture additional "creative" spellings.
    #
    # Common NAD27 misspellings include:
    # - conflating 1927 and 1929
    # - conflating NAD and NGVD
    # - leaving out NAD or NGVD entirely
    # - typos in NAD or NGVD (eg. NVGD, NAAD27, NAVD27)
    #
    # Common NAD83 misspellings include:
    # - conflating NAD83 and NAVD88
    # - leaving out NAD or NAVD entirely
    # - typos in NAD or NAVD (eg. NADA 1983, NAD84, NAV83, NAVD 1988, NAD 88, NAVD 88)
    #
    # Possible typos that we aren't currently accepting:
    # - 'NGVD83' (because it mixes up NGVD29 and NAD83, so I'm not sure what it means)
    # - 'NADVD88', 'NAD983', 'NAC83' - they're just a bit far off from the correct values such that
    #   we aren't sure if they are typos or just something else. We can add them if we see them more
    #   in the future.

    # TODO: Replace with new function

    nad27_misspellings = [  # TODO: Add to shared variables
        'NGVD 1929',
        'NAD 1927',
        'NAD 1929',
        'NAD-27',
        '1929',
        'NAD1927',
        'NGVD29',
        '1927',
        'NGVD',
        'NVGD',
        'NAD 27',
        'NGDV 1929',
        'NGVD1929',
        'NAAD27',
        'NAD29',
        '1927 NGVD',
        '1929',
        'NVD 1929',
        'NAD1929',
        'NGVD1927',
        '1929 NGV',
        '1929 NGVD',
        'NA 1927',
        'NAVD27',
    ]

    nad83_misspellings = [
        'NAD 1983',
        'NAVD88',
        'NAD 83',
        'NAD1983',
        'WGS84',
        'NADA 1983',
        'NAV83',
        'NAVD 1988',
        '1988',
        'NAD 88',
        'NAVD 88',
        'nad83',
        'NAV-88',
        'NAVD83',
        'NGVD1988',
        'NAD84',
        'NAD 1988',
        '1988',
        'NAV83',
        'NAVD-88',
        'NAD88',
        'NAD87',
        'NAD893',
    ]

    ngvd29_misspellings = [
        'NGVD 1929',
        'NGVD,1929',
        'NGVD OF 1929',
        'NGVD',
        'USGS NAD 1929',
        'NGVD1929',
        'NGVD 29',
    ]

    # Fix misspelled CRSs that are actually NAD27
    if datum_data['crs'] in nad27_misspellings:
        logging.warning(f"{lid}: Typo found in horizontal CRS, changing {datum_data['crs']} to NAD27")
        datum_data.update(crs='NAD27')

    # Fix misspelled CRSs that are actually NAD83
    if datum_data['crs'] in nad83_misspellings:
        logging.warning(f"{lid}: Typo found in horizontal CRS, changing {datum_data['crs']} to NAD83")
        datum_data.update(crs='NAD83')

    # Fix misspelled vertical datums that are actually NGVD29
    if datum_data['vcs'] in ngvd29_misspellings:
        logging.warning(f"{lid}: Typo found in vertical CRS, changing {datum_data['vcs']} to NGVD29")
        datum_data.update(vcs='NGVD29')

    # ---------------------------
    # Get datum adjustment to convert elev to NAVD88 (if elev data is in NGVD29)
    # using the NOAA VDatum API

    # TODO: Does this will work calling ngvd_to_navd_ft when in EC2's? Can it talk to that
    # service from EC2's? Check this.

    # Jan 2026: Previously we set the default datum_adj_ft to 0.0 here and then only updated it
    # if we knew it was in NGVD29 and we could get a value from vdatum.
    # The problem with this is that if vdatum fails for some reason (eg. API is down,
    # or there is an error with the specific request), then the datum_adj_ft will be left at 0.0
    # and we will incorrectly assume that the site is in NAVD88 when it might not be.
    # By keeping the default at the nodata value, we can be sure that if it is 0.0,
    # it is because we know it is in navd88 and not just because vdatum failed and left the default value.

    crs = datum_data.get('crs')
    vcs = datum_data.get('vcs')

    # Get the datum adjustment to convert NGVD to NAVD
    if vcs == 'NGVD29':
        try:
            datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data)
            logging.info(f"{lid}: Datum adjustment from NGVD29 to NAVD88 is {datum_adj_ft} ft:")

        except Exception as ex:
            err_msg = f"{lid}: Exception occurred during vertical datum conversion (ngvd_to_navd_ft)"
            logging.error(err_msg)

            if crs is None:  # TODO: Should we check for this earlier? Any reason to wait until here?
                err_msg = 'NOAA VDatum adjustment error, CRS is missing'
                logging.error(f"{lid}: {err_msg}")
                # Skip traceback message because we know what went wrong
                # These errors indicate that the input WRDS data is incomplete

            elif crs in unknown_list:
                err_msg = f'NOAA VDatum adjustment error, CRS is invalid (crs: {crs})'
                logging.error(f"{lid}: {err_msg}")
                # Skip traceback message because we know what went wrong
                # These errors indicate that the input WRDS data is incomplete

            else:
                # Check VDatum API error message for clues
                logging.error(traceback.format_exc())
                ex = str(ex)

                if 'HTTPSConnectionPool' in ex:
                    time.sleep(10)  # Maybe the API needs a break, so wait 10 seconds
                    try:
                        datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data)
                    except Exception:
                        err_msg = ':NOAA VDatum adjustment error, possible API issue'
                        logging.error(f"{lid}: {err_msg}")
        
                elif 'Invalid projection' in ex:
                    err_msg = f'NOAA VDatum adjustment error, invalid projection: crs={crs}'
                    logging.error(f"{lid}: {err_msg}")

                else:
                    # TODO: This is a new error message, I'm really curious to see if it catches any additional sites
                    err_msg = 'NOAA VDatum adjustment error, unrecognized error'
                    logging.error(f"{lid}: {err_msg}")
                    logging.error(ex)

    elif vcs == 'NAVD88':
        logging.info(f"{lid}: Datum is already in NAVD88, no adjustment needed.")
        datum_adj_ft = 0.0

    else:
        # TODO: This is a new message, I'm really curious to see if it catches any additional sites
        logging.warning(
            f"{lid}: No datum adjustment perfomed due to unknown vertical datum. (CRS: {crs}, VCS: {vcs})"
        )
        datum_adj_ft = 0.0

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

    NOTE: Some columns have already been renamed ???? # TODO: Check, clean up?

    Arguments
    ---------
    metadata_df - Dataframe
        Single record dataframe made from a single lid metadata json dataset.

    Returns
    -------
    nws_datums - DICT
        Dictionary of NWS data.
    usgs_datums - DICT
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


def __adj_dem_elevation_val(acceptable_usgs_elev_df, lid, huc):
    '''
    Used in stage-based CatFIM.

    Retrieves the elevation value from the hydroconiditoned DEM created in the FIM pipeline for
    a given USGS gage site (LID) from the provided DataFrame, and checks for exclusion criteria or data issues.

    Arguments
    ----------
    acceptable_usgs_elev_df - pd.DataFrame
        DataFrame containing USGS gage information, including
        'nws_lid', 'levpa_id', 'dem_adj_elevation', and 'usgs_exclusion_status' columns.
    lid - str
        The NWS LID to look up.
    huc - str
        Hydrologic Unit Code.

    Returns
    --------
    lid_usgs_elev - float
        The DEM-adjusted elevation value for the specified LID, or 0 if not found or excluded.
    err_msg - STR
        Error message, if one arises. Otherwise, blank string.

    Notes
    ------
    - If the LID is not found, excluded, or has an elevation of 0, appropriate messages are logged and returned.
    - If multiple entries exist for the LID, the one with a non-zero 'levpa_id' is used.
    - Exclusion status other than 'acceptable' will result in an early return with a message.
    '''

    lid_usgs_elev = 0
    err_msg = ""

    try:
        # Check for USGS elevation data that matches the LID
        matching_rows = acceptable_usgs_elev_df.loc[acceptable_usgs_elev_df['nws_lid'] == lid.upper()]

        # Check if the site is not in the USGS data table (in our data)
        if len(matching_rows) == 0:
            # err_msg = 'Gage not in HAND usgs gage records, likely due to exclusion criteria' # previous error message
            err_msg = 'Gage not in USGS elevation data table, likely due to exclusion criteria or missing data'  # TODO: Update with more clarity
            # TODO: Essentially, this happens when the site has been excluded from the USSG elevation data for not meeting their exlcusion criteria.
            # We've tried to decrease the amount of sites we exclude at that previous download stage because that allows
            # us to be more explicit here about WHY we are excluding certain sites. But some still do get excluded at that stage and therefore
            # will get caught by this error. I guess the best thing to do here would be to go into the code that downloads that original
            # data and figure out what reasons it excludes sites still and then put that explanation here...
            logging.warning(f"{huc} : {lid} - {err_msg}")
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
            logging.warning(f"{huc} : {lid} - {err_msg}")
            return lid_usgs_elev, err_msg

        # Check whether DEM adjusted elevation is 0 or not set
        if lid_usgs_elev == 0:
            err_msg = 'DEM adjusted elevation is 0 or not set'
            logging.warning(f"{huc} : {lid} - {err_msg}")
            return lid_usgs_elev, err_msg

    except IndexError:  # Occurs when LID is missing from table (yes. warning)
        err_msg = 'Error when extracting dem adjusted elevation value'
        logging.warning(f"{huc} : {lid} - {err_msg}")
        logging.warning(traceback.format_exc())

    logging.info(f"{huc} : {lid} - lid_usgs_elev is {lid_usgs_elev}")

    return lid_usgs_elev, err_msg


def __update_library_csv(sites_gdf, huc_library_df, library_pre_mapping_file_path):
    '''
    Updates the library CSV to remove recs for sites that failed in __process_elevations().

    Arguments
    ---------
    sites_gdf - GPD.GeoDataFrame
        Sites geodataframe.
    huc_library_df - Pandas DataFrame
        Library data table.
    library_pre_mapping_file_path - STR
        Filepath to the library pre-mapping file.
    '''
    if len(huc_library_df) > 0:

        # Get a list of invalid sites
        invalid_lids = sites_gdf.loc[sites_gdf["mapped"] == "no"]["nws_lid"].values.tolist()
        if len(invalid_lids) > 0:
            indices_to_drop = huc_library_df[huc_library_df['nws_lid'].isin(invalid_lids)].index

            # All records might have been dropped earlier or were not here in the first place
            if len(indices_to_drop) > 0:
                huc_library_df = huc_library_df.drop(indices_to_drop)
                if len(huc_library_df) > 0:
                    huc_library_df.to_csv(library_pre_mapping_file_path, index=False)
                    logging.info(f"Updating library csv file at {library_pre_mapping_file_path}")

    # Note: We will check for count of remaining library recs when we return


def __setup_sites_gdf(sites_gdf, catfim_type):
    '''
    Setup and prepare a GeoDataFrame of sites for CATFIM processing.

    Arguments
    ----------
    sites_gdf - geopandas.GeoDataFrame
        A GeoDataFrame containing site information with columns such as 'nws_lid',
        'HUC8', and various NWS and NWM metadata fields.
    catfim_type - str
        The type of CATFIM processing, 'sb' or 'fb'.
        Determines whether SB-specific validation columns are added.

    Returns
    -------
    sites_gdf - geopandas.GeoDataFrame
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
    already know at this point instead of piecemeal.

    This function starts building up the new sites / meta file. We can adjust the status as we go.

    '''

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

    sites_gdf = sites_gdf.drop(
        ['downstream_nwm_features', 'upstream_nwm_features'], axis=1, errors='ignore'
    ) # TODO: maybe we don't drop this because then we wouldn't have to read the metadata json list in later when this data is needed...?
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

    # NOTE: if you get errors saying: Skipping field because of invalid value:
    # There are a couple of possible reasons. Data type mismatch, None in a float/int column and the most
    # common is a list object in a meta gdf field. To fix it, generaally just make it a string or drop it.
    # We have both above.
    # Nov 6, 2025: We have appx 15 fields that fail but not on all recs. Let's try to change all columns to string
    # and see if that helps.

    # We need a better answer here as we do want some columns to non string

    return sites_gdf


def make_huc_mapping_folders(output_mapping_dir, output_temp_dir, output_log_dir):
    '''
    # TODO: Update

    '''
    mode = 0o777  # allows read, write, and execute for all (rwxrwxrwx)
    os.makedirs(output_mapping_dir, exist_ok=True, mode=mode)
    os.makedirs(output_temp_dir, exist_ok=True, mode=mode)
    os.makedirs(output_log_dir, exist_ok=True, mode=mode)

    # os.chmod(path, mode)  # if needed, we can adjust permissions like this


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
