#!/usr/bin/env python3

import argparse
import logging
import numpy as np
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
import catfim.generate_categorical_fim_flows as gcf
from src.utils.shared_variables import VIZ_PROJECTION
from tools.tools_shared_functions import (
    get_datum,
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
        updating it as it is being processed.
      - may / may not have one HUC level master or split level threshold / discharge data ??

      - The sites_gdf, is updated and passed through most code, updating the mapped and status column as it goes
        based on issues found. It also has temp warnings column for things like "missing action stage", which existed
        in prior versions. At the end, it will be rolled into status column

      - When the huc is finished be processed, it's output files sit ready for post processing to merge with the rest of the huc files.

"""

# TODO: Dec 20225: For all of the possible error messages that could be used with the message text changing
# per version, I wonder if we should add a "status_code" column to the sites.gdf, so catfim compare can
# compare codes and not text values. Most of the status messages have been preserved, but likely some changes.
# Even if we add status_codes now, it won't really have its true value until two version down the road
# as the current 4.8.7.2 does not have that column unless we build a temp retro fit to those files.

def process_huc(huc, output_folder):
    """

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
        sites_file_path = os.path.join(huc_path, f"{huc}_sites.gpkg")
        library_file_path = os.path.join(huc_path, f"{huc}_library.gpkg")

        # TODO: change the cleanup to some grep getting all files, then compare
        # to a list of files to cleanup. Some other py files create intermediate files.

        # removes the three files/folders above, plus a few others named 
        # inside the function.
        __set_start_files_folders(
            catfim_type,
            huc_path,            
            output_mapping_dir,
            sites_file_path,
            library_file_path
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
        sites_file_path_pre_thresh = sites_file_path.replace(".gpkg", "_pre_threshold.gpkg")
        logging.info(f"Saving sites, pre flow and mapping, at {sites_file_path_pre_thresh}")
        sites_gdf.to_file(sites_file_path_pre_thresh, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", encoding="utf-8")

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
        # yes.. it is ok that thresholds_merged_df is empty for now


# TODO: Is this the right answer?
# The data_source is a column from the original threshold dataset. It often contains values
# such as 'Manual_Input' and/or values such as:
#     NWS-NRLDB generally for stage data and USGS Rating Depot for Flow data or a combination

        threshold_huc_df, data_source = gcf.get_threshold_data(huc, huc_path, valid_nwm_lids)

# While somewhat inefficent, we will add this to the sites_gdf column of threshold_data_source
# for simplicity of copying around and using against logic when needed.
# At the end, we can drop it at the end. 
# Note: It is only for SB processing at this time.
# sites_gdf['threshold_data_source'] = data_source



        # =========================================
        # Processing Threshold data  (figure stages and calc flow data for inundation)
        section_start_dt = datetime.now(timezone.utc)
        logging.info("Processing initial flow and threshold data for all valid sites")


        # Dec 24, 2025: Emily.. stop here for now. :)
        # print("--------------")
        # print("Ok.. let's stop here for now")
        # sys.exit(0)

        # Note: We no longer need attribute files or the attribute folder.
        #    The data in those files, were mostly dup data from the sites_gdf
        #    and the mag data already present in the threshold file.
        # we do have at least one threshold record
        # library is not yet a gdf as it has no geometry.

        # A library file was created and saved to disk. It a library data file
        # same as we use for final library files and is based on only lids
        # and mag types that qualified to this point.  Some library recs
        # may be rejected and removed based on logic down the road.
        # It will contain 0 to 5 mag type records per lid.
        #     ie) ABCD1/action  or EFGH1/record.
        # The same pattern and columns used in the final library files. Later we will add geometry.

        # For SB, it does not yet contain any of the interval records as future
        # logic might delimit more mag types.

        # TODO: has_error system? or at least a way to see if 
        sites_gdf = gcf.process_theshold_data(catfim_type,
                                              valid_nwm_lids,
                                              sites_gdf,
                                              huc,
                                              huc_path,
                                              threshold_huc_df,
                                              metadata_json)

        logging.info(f"End of initial processing flow and threshold data for huc {huc}")
        duration_msg = sf.calculate_duration_msg(section_start_dt)
        logging.info(duration_msg)

        # ---------------------
        # Save a copy of the sites_gdf up to this point.
        sites_file_path_post_threshold = sites_file_path.replace(".gkpg", "_threshold.gpkg")
        logging.info(f"Saving sites data post threshold processing at {sites_file_path_post_threshold}")
        sites_gdf.to_file(sites_file_path_post_threshold, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", encoding="utf-8")

        # Temp debugging
        print("--------------")
        print("Ok.. let's stop here for now")
        sys.exit(0)


        # See if we still have any valid lids
        # We won't have any library files, but still need to finalize sites.gdf
        # As it will still be part of the final product rollup
        valid_lids = sites_gdf.loc[sites_gdf["mapped"] != "no"]
        if len(valid_lids) == 0:
            logging.info("There are no remaining sites to process skipping to sites finalization")
            __finalize_outputs(sites_gdf, sites_file_path, True)



        # ---------------------
        # Data adjustments or rejections ? (might be higher or even need more here)
        # FB has none, but SB does have more data logic
        if os.getenv('CATFIM_TYPE') == "sb":
            logging.info("Start processing stage based data")
            
            
            # TODO: what is a good name for this.  Maybe a new file called catfim_data_processing.py? (but leave mapping to focus on inundation)
            section_start_dt = datetime.now(timezone.utc)
            
            sites_gdf, has_critical_error, lid_altitude, datum_adj_ft, lid_usgs_elev = __process_evalations(
                sites_gdf, threshold_huc_df, huc, huc_path)

            # ---------------------
            # Add Intervals


        # ---------------------
        # If FB, Load branch and HAND data? (rems and hydrotables), liekly all done via inundation scripts
        # hummmmm



        # ------------------
        # Manage_catfim_mapping
            # It will need only the huc and output file and it will reload all files
            # it needs. This allows it to be run independantly through command line.


            # Create inundation tifs if applicable and roll them up if branch tifs?
            #    FB: Call inundation.py ?
            #     SB: Do our own inundation like we currently do?

            # ---------------------
            # Make extent polys? (or already done in manage_mapping?)

        # ---------------------
        # Finalize all final sitess and library files for this HUC
        __finalize_outputs(sites_gdf, sites_file_path, False)



        logging.info(f"End processing for huc {huc}")
        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info(duration_msg)

        # nothing to return as of now
        # but generate_categorical_fim.py can if it has value to return.
        # if you use "return" and it is AWS, it will not error out.
        # yes.. we want a "return", but may/may not have a value.
        # if we add one, keep it a simple data type (str, int, float)
        return huc  #  ?? hummm

    except Exception:
        trace_error = traceback.format_exc()

        err_msg = f"A critical error has occurred while processing {huc}. Detail: {trace_error}"
        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # do we re-throw the error? gcf, aws, or cmd line? hummm

def __process_evalations(sites_gdf, threshold_huc_df, huc, huc_path):

    """
    Only used by SB.
    """

    # data_source comes from the original threshold dataset. It was put into a temp column
    # in the sites_gdf under the name of threshold_data_source. We can use it for processing logic.
    # Later we can drop the columns before the final huc catfim outputs.
    # This can be values of "Manual_Input" and/or values such as:
    #     NWS-NRLDB generally for stage data and USGS Rating Depot for Flow data.


    # TODO: this is still a wip.
    # data_source = threshold_huc_df["source_stage"]  # TODO: This is likely in the sites_gdf by now
    data_source = "WRDS"
    has_critical_error = False

    # get a list of sites that are still valid
    valid_lids = sites_gdf.loc[sites_gdf["mapped"] != "no"]

    if data_source != 'Manual_Input':  # Manual input data does not need usgs_elev_table

        usgs_elev_table_file_name = 'usgs_elev_table.csv'
        src_usgs_elev_table = os.path.join(os.getenv("FIM_RUN_DIR"), huc, usgs_elev_table_file_name)

        usgs_elev_df = None
        if data_source != 'Manual_Input' and not os.path.exists(src_usgs_elev_table):
            msg = "Internal Error: Missing key data from HUC record (usgs_elev_table missing)"
            raise Exception(msg)

        # we need to quickly make a copy and bring it here to lower the chance of file collisions.
        local_copy_usgs_elev_table = os.path.join(huc_path, usgs_elev_table_file_name)
        shutil.copyfile(src_usgs_elev_table, local_copy_usgs_elev_table)

        usgs_elev_df = pd.read_csv(local_copy_usgs_elev_table)

        # TODO: this seems a bit weird. hummm.
        acceptable_usgs_elev_df = __create_acceptable_usgs_elev_df(usgs_elev_df)
        if acceptable_usgs_elev_df is None or len(acceptable_usgs_elev_df) == 0:
            msg = "Unable to find gage data"  # TODO: USGS Gage Method: Update this error message to be more descriptive
            logging.error(f"{lid} : {msg}")

            # If this happens, all sites in this HUC will fail and have this same message, so we can update them all
            sites_gdf["mapped"] = 'no'
            sites_gdf["status"] = msg
            has_critical_error = True
            return sites_gdf, has_critical_error  # This will stop further processing downstream

        else:  # if source is manual input, we skip the above elevation filtering
            logging.info("Skipping elevation checks and datum adjustment for Manual Input source")


    # TODO: do we need this?, yes, but not until inundation. skip here and load it later when we need it
    # branch_dir = os.path.join(fim_dir, huc, 'branches')
    # if not os.path.exists(branch_dir):
    #     msg = ":branch directory missing"
    #     # all_messages.append(huc + msg)
    #     MP_LOG.warning(huc + msg)
    #     skip_lid_process = Truer, huc, 'branches')

    for lid in valid_lids:
    
        # Find the single lid record from the sites_gdf. Note: you can not update this rec, only the
        # parent site_gdf for the site. Use this for read-only.

        # *******************************
        # IMPORTANT - USE lid_sites_gdf FOR READ-ONLY. It is only for ease of data extraction
        # *******************************        
        lid_sites_gdf = sites_gdf.loc[sites_gdf["nws_lid"] == lid].copy()

        if len(lid_sites_gdf) != 1:
            raise Exception("Internal error: There should be exactly one lid rec here")

        lid_altitude = lid_sites_gdf.iloc[0]['usgs_data_altitude']
        if lid_altitude is None or lid_altitude == 0:
            msg = 'AHPS site altitude value is invalid'
            logging.warning(f"{lid} : {msg}")        
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', msg]
            continue

        if data_source != 'Manual_Input':

            # Get the dem_adj_elevation value from usgs_elev_table.csv.
            # Prioritize the value that is not from branch 0.

            # sites_gdf, lid_usgs_elev, dem_eval_messages = __adj_dem_evalation_val(sites_gdf, 
            #     acceptable_usgs_elev_df, lid
            # )
            # all_messages = all_messages + dem_eval_messages
            # if len(dem_eval_messages) > 0:
            #     continue

            lid_usgs_elev, err_msg = __adj_dem_evalation_val(acceptable_usgs_elev_df, lid)
            if err_msg != "":
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
                continue

            # Filter out sites that don't have "good" data
            # TODO: USGS Gage Method: It doens't seem like the below error messages are performing as expected....
            err_msg = __filter_bad_usgs_gage_data(lid_sites_gdf, lid)
            if err_msg != "":
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
                continue

            # Adjust datum of HAND grid based on elevation data from usgs_elev_table.csv.
            # datum_adj_ft, datum_messages = __adjust_datum_ft(flows, metadata, lid, huc_lid_id)
            # all_messages = all_messages + datum_messages
            # if datum_adj_ft is None:
            #     MP_LOG.warning(f"{huc_lid_id}: datum_adj_ft is None")
            #     continue

            datum_adj_ft, err_msg = __adjust_datum_ft(lid_sites_gdf, threshold_huc_df, lid)
            if err_msg != "":
                sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
                continue

        else:  # if source is manual input, we skip the above elevation filtering
            lid_altitude = float(lid_altitude)  # LID altitude is expected to be in meters
            lid_usgs_elev = (
                lid_altitude * 0.3048
            )  # lid_altitude is now in meters to match non-manual input units
            # TODO: Automate conversion?

            datum_adj_ft = 0  # no datum adjustment for manual input

        # segments are now loaded much earlier and will be handled when we do mapping

        # Check for large discrepancies between the elevation values from WRDS and HAND.
        #   Otherwise this causes bad mapping.
        # Manual_Input will have no elev disparity because it's from the the same value.
        elevation_diff = lid_usgs_elev - (lid_altitude * 0.3048)
        diff_rounded = round(elevation_diff, 2)

        # Log elevation difference information - not an error, just for reference (maybe remove later)
        if elevation_diff > 0:
            logging.warning(f"{lid}: USGS elev is higher than HAND elev by {diff_rounded} ft")
        elif elevation_diff < 0:
            logging.warning(
                f"{lid}: USGS elev is lower than HAND elev by {abs(diff_rounded)} ft"
            )

        if abs(elevation_diff) > 10:
            err_msg = 'Large discrepancy in elevation estimates from gage and HAND'
            logging.warning(f"{lid}: {err_msg}")
            sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
            continue
        elif abs(elevation_diff) > 5:
            err_msg = (
                f':Moderate discrepancy ({diff_rounded} ft) in elevation estimates from gage and HAND'
            )
            logging.warning(f"{lid}: {err_msg}")
            # sites_gdf.loc[sites_gdf["nws_lid"] == lid, ['mapped', 'status']] = ['no', err_msg]
            # just print as a warning for now (not appending to message)
            # We are not stopping

    return sites_gdf, has_critical_error, lid_altitude, datum_adj_ft, lid_usgs_elev



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
    '''
    acceptable_usgs_elev_df = None
    try:
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


def __adjust_datum_ft(lid_sites_gdf, threshold_huc_df, lid):
    '''
    Used in stage-based CatFIM.

    Determines the vertical datum adjustment (in feet) to convert the datum of the
    rating curve to NAVD88.

    Uses the rating curve source and metadata to get the correct vertical datum and CRS.

    It applies custom workarounds for known sites with special datum or CRS requirements,
    and attempts to compute the adjustment using the NOAA VDatum service when necessary.

    Args:
        flows (dict): Dictionary containing flow information, including the source of the rating curve.
        metadata (dict): Dictionary containing site metadata, including datum and CRS information.
        lid (str): Location identifier for the site.
        huc_lid_id (str): Combined HUC and location identifier for logging and messaging.
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

    TODO: Aug 2024: This whole parts needs revisiting. Lots of lid data has changed and this
    is all likely very old.
    '''

    err_msg = ""

    # TODO: Why are we using flow source data to do logic calcs and not the stage columns

    # Yes... flow data even though we are processing stage data
    lid_flow_data = threshold_huc_df.loc[threshold_huc_df['nws_lid'] == lid &
                                         threshold_huc_df['threshold_type'] == 'flows']
    
    if lid_flow_data.empty():
        raise Exception(f"{lid}: We should have exactly one flow record")


    datum_adj_ft = None
    ### --- Do Datum Offset --- ###
    # determine source of interpolated threshold flows, this will be the rating curve that will be used.
    # rating_curve_source = flows.get('source')
    rating_curve_source = lid_flow_data.iloc[0]["source"]

    # MP_LOG.trace(f"{huc_lid_id} : rating_curve_source is {rating_curve_source}")

    if rating_curve_source is None or rating_curve_source == "":
        err_msg = 'No source for rating curve'
        logging.warning(f"{lid}: {err_msg}")
        return None, err_msg

    # Get the datum and adjust to NAVD if necessary.
    nws_datum_info, usgs_datum_info = get_datum_from_df(lid_sites_gdf)
    if rating_curve_source == 'USGS Rating Depot':
        datum_data = usgs_datum_info
    elif rating_curve_source == 'NRLDB':
        datum_data = nws_datum_info

    # If datum not supplied, skip to new site
    datum = datum_data.get('datum', None)
    if datum is None:
        err_msg = 'Datum info unavailable'
        logging.warning(f"{lid}: {err_msg}")
        return None, err_msg
    
    # ___________________________________________________________________________________________________#
    # SPECIAL CASE: Workaround for "bmbp1" where the only valid datum is from NRLDB (USGS datum is null).
    # Modifying rating curve source will influence the rating curve and
    #   datum retrieved for benchmark determinations.

    # TODO: bug from previous versions. rating_curve_source no used after this making this
    # unused.
    if lid == 'bmbp1':
        rating_curve_source = 'NRLDB'
    # ___________________________________________________________________________________________________#

    # SPECIAL CASE: Custom workaround these sites have faulty crs from WRDS. CRS needed for NGVD29
    #   conversion to NAVD88
    # USGS info indicates NAD83 for site: bgwn7, fatw3, mnvn4, nhpp1, pinn4, rgln4, rssk1, sign4, smfn7,
    #   stkn4, wlln7
    # Assumed to be NAD83 (no info from USGS or NWS data): dlrt2, eagi1, eppt2, jffw3, ldot2, rgdt2
    if lid in [
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
    # ___________________________________________________________________________________________________#

    # SPECIAL CASE: Workaround for bmbp1; CRS supplied by NRLDB is mis-assigned (NAD29) and
    #   is actually NAD27.
    # This was verified by converting USGS coordinates (in NAD83) for bmbp1 to NAD27 and
    #   it matches NRLDB coordinates.
    if lid == 'bmbp1':
        datum_data.update(crs='NAD27')
    # ___________________________________________________________________________________________________#

    # SPECIAL CASE: Custom workaround these sites have poorly defined vcs from WRDS. VCS needed to ensure
    #   datum reported in NAVD88.
    # If NGVD29 it is converted to NAVD88.
    # bgwn7, eagi1 vertical datum unknown, assume navd88
    # fatw3 USGS data indicates vcs is NAVD88 (USGS and NWS info agree on datum value).
    # wlln7 USGS data indicates vcs is NGVD29 (USGS and NWS info agree on datum value).
    if lid in ['bgwn7', 'eagi1', 'fatw3']:
        datum_data.update(vcs='NAVD88')
    elif lid == 'wlln7':
        datum_data.update(vcs='NGVD29')
    # ___________________________________________________________________________________________________#

    # Adjust datum to NAVD88 if needed
    # Default datum_adj_ft to 0.0
    datum_adj_ft = 0.0
    crs = datum_data.get('crs')
    if datum_data.get('vcs') in ['NGVD29', 'NGVD 1929', 'NGVD,1929', 'NGVD OF 1929', 'NGVD']:
        # Get the datum adjustment to convert NGVD to NAVD.
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
            return None, err_msg

    return datum_adj_ft, err_msg


def get_datum_from_df(metadata_df):
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

    if metadata_df is None:
        raise Exception("The metadata df can not be None")
    if len(metadata_df) != 0:
        raise Exception(f"Metadata shoudl contain exactly one record but has {len(metadata_df)} records")

    # Get site and datum information from nws sub-dictionary. Use consistent naming between USGS and NWS sources.
    nws_datums = {}
    nws_datums['nws_lid'] = metadata_df['identifiers_nws_lid']
    nws_datums['usgs_site_code'] = metadata_df['identifiers_usgs_site_code']
    nws_datums['state'] = metadata_df['nws_data_state']
    nws_datums['datum'] = metadata_df['nws_data_zero_datum']
    nws_datums['vcs'] = metadata_df['nws_data_vertical_datum_name']
    nws_datums['lat'] = metadata_df['nws_data_latitude']
    nws_datums['lon'] = metadata_df['nws_data_longitude']
    nws_datums['crs'] = metadata_df['nws_data_horizontal_datum_name']
    nws_datums['source'] = 'nws_data'

    # Get site and datum information from usgs_data sub-dictionary. Use consistent naming between USGS and NWS sources.
    usgs_datums = {}
    usgs_datums['nws_lid'] = metadata_df['identifiers_nws_lid']
    usgs_datums['usgs_site_code'] = metadata_df['identifiers_usgs_site_code']
    usgs_datums['active'] = metadata_df['usgs_data_active']
    usgs_datums['state'] = metadata_df['usgs_data_state']
    usgs_datums['datum'] = metadata_df['usgs_data_altitude']
    usgs_datums['vcs'] = metadata_df['usgs_data_alt_datum_code']
    usgs_datums['datum_acy'] = metadata_df['usgs_data_alt_accuracy_code']
    usgs_datums['datum_meth'] = metadata_df['usgs_data_alt_method_code']
    usgs_datums['lat'] = metadata_df['usgs_data_latitude']
    usgs_datums['lon'] = metadata_df['usgs_data_longitude']
    usgs_datums['crs'] = metadata_df['usgs_data_latlon_datum_name']
    usgs_datums['source'] = 'usgs_data'

    return nws_datums, usgs_datums


def __adj_dem_evalation_val(acceptable_usgs_elev_df, lid):
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

        # Check if the site is not in the usgs table in our data
        if len(matching_rows) == 0:
            # msg = ':Gage not in HAND usgs gage records' # prev error message (deprecated May 2025)
            err_msg = 'Gage not in HAND usgs gage records, likely due to exclusion criteria'
            logging.warning(f"{lid}: {err_msg}")
            return lid_usgs_elev, err_msg

        # It means there are two level paths, use the one that is not 0 (there will never be more than two)
        if len(matching_rows) == 2:
            # Get the site that does not have a levpa_id of zero and matches the LID
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

    # todo: We need to figure out how to get trace working
    logging.trace(f"{lid} : lid_usgs_elev is {lid_usgs_elev}")
    logging.info(f"{lid} : lid_usgs_elev is {lid_usgs_elev}")

    return lid_usgs_elev, err_msg


def __filter_bad_usgs_gage_data(lid_sites_gdf, lid):

    # We wil do logging here as some are warnings, some are errors
    # and we want logging to ber 

    err_msg = ""
    try:
        alt_method_code = lid_sites_gdf.iloc[0]['usgs_data_alt_method_code']
        if not alt_method_code in acceptable_alt_meth_code_list:
            err_msg = "Not in acceptable alt method codes"
            logging.warning(f"{lid}: {err_msg}")
            return err_msg

        site_type = lid_sites_gdf.iloc[0]['usgs_data_site_type']
        if site_type in acceptable_site_type_list:
            err_msg = "Not in acceptable site type codes"
            logging.warning(f"{lid}: {err_msg}")
            return err_msg
        
        alt_accuracy_code = lid_sites_gdf.iloc[0]['alt_accuracy_code']
        if not float(alt_accuracy_code) <= acceptable_alt_acc_thresh:
            err_msg = "Not in acceptable threshold range"
            logging.warning(f"{lid}: {err_msg}")
            return err_msg
        
    except Exception:
        err_msg = "Error filtering out 'bad' data in the usgs data"
        logging.error(f"{lid}: {err_msg}")

    return err_msg


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

    # # adjust and/or rename some columns
    # # At the very end, we will rename it to ahps_lid.
    # # Maybe we fix it someday, but not now. Too many other things going on.

    # Drop list fields if invalid
    # downstream_nwm_features and upstream_nwm_features are lists and gpkg does not like it
    # hummm... or maybe just when it is null? both of those nodes are 

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
            f"output_folder of {output_folder} does not exist. Please check pathing including case."
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
    os.makedirs(huc_path, exist_ok=True, mode=0o777)

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
    huc_path,
    output_mapping_dir,
    sites_file_path,
    library_file_path,
):

    discharge_file_path = os.path.join(huc_path, "flow_discharges.csv")  # only used by FB

    #  f"{huc}_library_threshold.csv")

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

    # TODO: Always keeps the logs folder and maybe nothing else?
    # certainly not meta or threshold files.

    # returns nothing


def __finalize_outputs(sites_gdf, sites_file_path, process_sites_only):

    # ------------------------------------
    # FINALIZING the sites table
    logging.info(f"Updating sites gdf with finalized site data at {sites_file_path}")
    # For the sites gpkg, leave the column named as nws_lid, we can change it to ahps_lid later in post processing.
    # hummmmm


    # TODO: do we want lower case lid values the entire way through?
    # Same for the library data?
    sites_gdf['nws_lid'] = sites_gdf['nws_lid'].str.lower()
    
    # FB and SB sites and library outputs call it ahps_lid instead of nws_lid. why?
    # well.. we process throughout as nws_lid
    sites_gdf.rename(columns={'nws_lid': 'ahps_lid'},inplace=True)


    # TODO:
    # What do we want to do with the sites 3 stage columns (stage, stage_umi, s_src)
    # if they are -1 or empty?  Especially with stage beign a double.

    # What about the q, q_uni and q_src, which are the three flow based values
    # from the threshold data.
    # in current code, SB has all three of the q columns as string and they can be empty
    # but in FB, the "q" col is a float. But it can be -1 as well. 
    # HUMMM.

    # TODO:
    # See other notes about this column. Do we want to keep it or drop it?
    # sites_gdf.drop("threshold_data_source", axis=1, inplace=True, errors='ignore')

    # TODO: append this to most status where mapped = no:
    # "Site resulted with no valid inundated files"

    sites_gdf.rename(
        columns={'identifiers_nwm_feature_id': 'nwm_seg', 'identifiers_usgs_site_code': 'usgs_gage'},
        inplace=True
    )

    # TODO: Update the sites recs if the mapped is still at 'not set' and status messages is also 'not set'.
    # if both are true, then we can changed mapped to yes and status to good.
    # If the warnings column has a value and mapped is yes, then copy that warnings column to status
    # Remove the warnings column.

    sites_gdf.to_file(sites_file_path, driver='GPKG', crs=VIZ_PROJECTION, engine="fiona", encoding="utf-8")
    # TODO: also save it as a csv


    # ------------------------------------
    # FINALIZING the library data saving it as a csv and gpkg

    if process_sites_only is False:
        print("we will get here")

    else:
        logging.info("There are no library files to process. Skipping library finalization")

    # returns nothing



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
