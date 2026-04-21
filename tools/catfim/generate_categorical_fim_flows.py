#!/usr/bin/env python3

import logging
import os
import pickle
import random
import shutil
import time
import traceback
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from tools_shared_functions import filter_nwm_segments_by_stream_order, flow_data, get_nwm_segs

# foss_fim imports
import data.wrds.download_process_wrds as dpw
import tools.catfim.catfim_shared_functions as csf


gpd.options.io_engine = "pyogrio"


"""
TODO: Clean up once no longer needed

Oct/Nov/Dec 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing

Tenative notes:
    - Some of the functions in here may move or be split to smaller functions.

    - This can be use by any other file when applicable, but anything needed by catfim_post_processing.py
      should be moved into that file.

    - Many or all of the functions in here, may move into catfim_process_huc.py potentially. Possible
      some may move into generate_categorical_fim.py, but more likely some of it's logic relating to flows,
      meta and thresholds wil move here.

    - This can be responsible for getting meta data, threashold and flow data for both FB and SB

    - For now, some data may come from
      WRDS, but will save pickle / parquet files when applicable for all HUCs to use for it's processing.
      Later, the process of getting data from WRDS will be split to an independant tools in our code "data"
      folders. When that happens, this may no longer be needed here, other than flow data? or making version
      copied of the WRDS files?

    - Meta and threshold data loaded here, are applicable to all HUCs and sites and will not be filtered in any
      way. It will still, of course, continue to honor WRDS filters that apply to non CONUS sites/HUCs.

"""

"""
Sites_gdf:
    As with all catfim files, after the initial sites_gdf is created with geometry, each file
    and function, will update it as it moves along. There are a wide number of reasons that a
    site may be changed to mapped = "no".  Each function will know why and will update
    the HUC sites.gdf and will handle "mapped, status, and warnings if applicable"

    At this point, the warning column is only for string saying things like:
    missing acion and minor stage data, etc. Later the messages column will be rolled
    into the final status column, same as before
"""

# TODO: This file should be renamed to something like generate_categorical_fim_thresholds
# as "flows" is not descriptive enough. Note: Renaming it will maintian its history.


def get_threshold_data(huc, huc_path, valid_nwm_lids):
    '''
    Gets the threshold data, either by downloading it from the WRDS API
    or by accessing it from the pre-downloaded thresholds pickle file.

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code
    huc_path - STR
        Path to the HUC-specific CatFIM outputs folder.
    valid_nwm_lids - STR
        List of site IDs that are not in the restricted sites list.

    Returns
    -------
    threshold_huc_df - Pandas DataFrame
        Dataframe of the thresholds for the HUC.

    '''

    # Stagger the start time to to help not overload the MP (0.1 milliseconds to 4 secs)
    time_delay_mms = random.randint(100, 4000) / 1000
    time.sleep(time_delay_mms)

    huc_thresholds_file_path = os.path.join(huc_path, f"{huc}_thresholds.csv")

    # If download threshold data option used, get threshold data from WRDS API
    if os.getenv('GET_NEW_THRESHOLD_DATA') is True:

        # Get WRDS base URL from the ENV_FILE
        api_base_url = ""
        api_base_url = csf.load_fim_global_env_values(os.getenv('ENV_FILE'))

        threshold_url = f'{api_base_url}/nws_threshold'

        # Build a new huc_lid_dict (because we only want to get thresholds for sites that have
        # not been filtered out at this point)
        huc_lid_dict = {}
        for lid in valid_nwm_lids:
            huc_lid_dict[lid] = huc

        # Note: We need to download threshold data using the LID rather than the HUC
        # because the HUC columns are sometimes incomplete or missing on WRDS.
        # Generating our own HUC dictionary is a much more reliable method
        # than relying on the HUC column in WRDS.

        # Create a filepath for the threshold data
        date_formatted = datetime.now(timezone.utc).strftime("%Y%m%d")
        local_copy_threshold_file_path = os.path.join(huc_path, f'nwm_threshold_data_{date_formatted}.pkl')

        # Download all thresholds for the HUC's valid LIDs
        return_msgs = dpw.download_all_thresholds(local_copy_threshold_file_path, threshold_url, huc_lid_dict)

        # Handle any messages returned from threshold download (return_msgs is a list, might have warnings/errors)
        if len(return_msgs) > 0:

            # TODO: This seems a bit bumpy but good enough for now. No idea on a better answer short of
            # custom exceptions.

            for msg in return_msgs:
                if "warning" in msg.lower():
                    logging.warning(msg)
                elif "error" in msg.lower():
                    raise Exception(msg)
                else:
                    logging.info(msg)
    else:
        # If the threshold file already exists...
        # Temporarily copy the threshold file to the local huc dir to avoid MP file collisions.
        # The random time delay should help prevent multiple processes from trying to copy
        # the same file at the exact same time.

        # Get threshold filepath from ENV_FILE
        threshold_file_path = os.getenv('THRESHOLD_FILE_PATH')
        if os.path.isfile(threshold_file_path) is False:
            raise FileNotFoundError(f"Error: Expected the threshold file at {threshold_file_path}")

        # Copy to threshold file to local huc dir
        src_file_name = os.path.basename(threshold_file_path)
        local_copy_threshold_file_path = os.path.join(huc_path, src_file_name)  # Now using the new huc copy
        shutil.copyfile(threshold_file_path, local_copy_threshold_file_path)

    # Load the threshold data, get the source, and filter the data to just the valid lids for the huc
    # Note: The source is important for differentiating processing for manual input vs wrds
    threshold_all_sites_df = pd.DataFrame()
    with open(local_copy_threshold_file_path, "rb") as p_handle:

        # Load threshold pkl file (it is a pkl file even though it has a df in it, mainly
        # to keep things consistent with the metadata.pkl file handling)
        threshold_all_sites_df = pickle.load(p_handle)

    # At this point, we have a threshold file in the HUC directory. If it was downloaded
    # from WRDS it will already be filtered to the HUC. If it was pre-loaded, it probably has threshold
    # data for multiple HUCs. Could move this to the part of the processing that copies the
    # pre-loaded data. But it also doesn't hurt to have it here.

    # Filter threshold data to be just the valid LIDs for this HUC
    # TODO: is this additional filtering step needed? it might be redundant? (might just need to reset index)
    threshold_huc_df = threshold_all_sites_df[threshold_all_sites_df['nws_lid'].isin(valid_nwm_lids)].copy()

    # TODO: do we need to reset index? or does copy() above do that for us?
    # threshold_huc_df.reset_index(drop=True, inplace=True)

    # TODO: upper case or lower case always? (see other notes around the code about lower or upper)
    # I think it is possible to not have threshold data for all of this huc's list.
    # change all nws_lid values to lower
    # threshold_all_sites_df['nws_lid'] = threshold_all_sites_df['nws_lid'].str.lower()

    # If no valid LIDs remain, return a blank value
    if len(threshold_huc_df) == 0:
        logging.warning(
            f"{huc} - No threshold data available for any valid sites in this HUC. Returning empty dataframe."
        )
        return []

    # Ensure the mag type columns are floats and rounded to 2 decimals # TODO: Make the threshold NA val (-1) into a variable?
    stage_types = ['action', 'minor', 'moderate', 'major', 'record']
    threshold_huc_df[stage_types] = threshold_huc_df[stage_types].fillna(value=-1).astype(float).round(2)

    # Ensure the units and source columns are strings and not empty
    str_columns = ['units', 'source']
    threshold_huc_df[str_columns] = threshold_huc_df[str_columns].fillna(value="").astype(str)

    # Drop threshold dataframe columns that will not be used
    threshold_huc_df = threshold_huc_df.drop(columns=['low', 'bankfull', 'huc', 'flood'], errors='ignore')

    # Move the nws_lid column to be the first column (makes it easier to read the outputs)
    nws_lid_col = threshold_huc_df.pop('nws_lid')
    threshold_huc_df.insert(0, 'nws_lid', nws_lid_col)

    # Save a copy of the filtered threshold data
    threshold_huc_df.to_csv(huc_thresholds_file_path, index=False)

    return threshold_huc_df


def process_threshold_data(
    catfim_type, valid_lids, sites_gdf, huc, huc_path, output_temp_dir, threshold_huc_df, metadata_json
):
    '''
    By this point some lids have been dropped (such as the restricted sites).

    This function creates a HUC-level library file with all of the site/magnitude combinations
    that are still valid up to this point (some site/magnitude combinations will still be dropped
    later on in processing).

    The HUC-level library file will be saved as a CSV because it does not have the geometry yet.

    For FB, it will also create a flow_discharge.csv file (pre Jan 2026 was saved in the flows folder,
    now is saved in the HUC-level data).

    Notes on the create HUC library data functions:
    - These will save intermediate library data for all lids and mag types that are valid by this point.
    - More logic will be done later, which may drop some of the library recs.
    - All returning df's will be saved as a csv at the end of this function. That helps
       with abstraction allowing the mapping code to function more independantly and it
       can reload files as needed and also give us checkpoint data.
    - These are split to SB and FB mostly because messages are different.

    Notes from CatFIM refactoring (Jan 2026):
    - This function is mostly equivalent to generate_flows in previous versions, but some processing
      that was previously done here has now been moved earlier in the code, such as dropping the
      restricted sites.
    - Removed the attributes files because that data already exists in the sites_gdf and the threshold
      data file. The attributes file had a bunch of duplicate data and mostly only needed
      the stage level data for each mag. It also includes the three "q" (discharge) columns which is the flow data.

    Arguments
    ---------
    catfim_type - STR
        'sb' or 'fb'
    valid_lids - LIST
        List of sites that have not encountered an error.
    sites_gdf - Geopandas GeoDataFrame
        Geodataframe containing the sites for the HUC.
    huc - STR
        Hydrologic Unit Code.
    huc_path - STR
        Filepath to the HUC-specific CatFIM outputs.
    output_temp_dir - STR
        Filepath to the HUC-specific CatFIM temp directory.
    threshold_huc_df - Pandas DataFrame
        Dataframe containing the flow or stage thresholds.
    metadata_json - LIST of JSON
        List of JSONs containing the site metadata.

    Returns
    -------
    sites_gdf - Geopandas GeoDataFrame
        Geodataframe containing the sites for the HUC, updated.
    huc_library_df - Pandas DataFrame
        Initial table for the library output.
    '''

    # ================================
    # Validate function inputs

    # Log an error and exit if the metadata JSON is empty
    if len(metadata_json) == 0:
        msg = f'{huc} - Process Threshold Data - Metadata JSON empty, unable to process threshold data.'
        logging.error(msg)
        return sites_gdf, None

    # Log an error and exit if the valid_lids is empty
    if len(valid_lids) == 0:
        msg = f'{huc} - Process Threshold Data - Valid LIDs list is empty, unable to process threshold data.'
        logging.error(msg)
        return sites_gdf, None

    # Log an error and exit if the threshold_huc_df is empty
    if len(threshold_huc_df) == 0:
        msg = f'{huc} - Process Threshold Data - threshold_huc_df is empty, unable to process threshold data.'
        logging.error(msg)
        return sites_gdf, None

    # Log an error and exit if the sites_gdf is empty
    if len(sites_gdf) == 0:
        msg = f'{huc} - Process Threshold Data - sites_gdf is empty, unable to process threshold data.'
        logging.error(msg)
        return sites_gdf, None

    # ================================
    # Create filepath variables
    discharge_file_path = os.path.join(huc_path, "flow_discharges.csv")  # only needed for FB
    segments_file_path = os.path.join(huc_path, "features_segments.csv")

    # ================================
    # Load the flows dataframe based on the HUC

    if huc[:4] == '2201':  # Guam
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nhd_flows_Guam'))
    elif huc[:4] == '2203':  # American Samoa
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nhd_flows_AmericanSamoa'))
    elif huc[:2] == '19':  # Alaska
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nwm_flows_Alaska'))
    else:  # CONUS + Hawaii + Puerto Rico
        nwm_flows_region_df = gpd.read_file(os.getenv('input_nwm_flows'))  # might be slow, it is 1.8 GiB

    # Note: 'else' assumes that any HUC not meeting the above criteria is CONUS, HI, or PR. If an
    # valid HUC makes it this far (unlikely), it will fall out later in the processing.

    # TODO: Rob: This should be changed to loading something_path at the HUC level for flow data,
    # because the CONUS flow file it is 1.6 GiB and is a bit slow to load.
    #
    # Emily: I'm not certain that any of our FIM output flowlines files will be correct for this
    # application. However, we could potentially use the preclip workflow to create
    # HUC-level flow files for each HUC. I think this is a task to add to the CatFIM Epic.

    # ================================
    # Compile intermediate library data for all valid site/magnitude combinations in the HUC
    # Note: Some site/magnitude combos could still be dropped later

    if catfim_type == "sb":
        # Create and process stage-based threshold data
        sites_gdf, huc_library_df, huc_segments_df = __create_sb_huc_library_data(
            huc, valid_lids, sites_gdf, threshold_huc_df, metadata_json, nwm_flows_region_df
        )
        # Note: This will not create the interval records at this point. It will do it much later
        # after it has passed a number of tests per mag.

    else:
        # Create and process flow-based threshold and discharge data
        sites_gdf, huc_library_df, huc_segments_df, huc_discharges_df = __create_fb_huc_library_data(
            huc, valid_lids, sites_gdf, threshold_huc_df, metadata_json, nwm_flows_region_df
        )

        # Save discharge dataframe (it is ok if this is empty, no need to throw error)
        if len(huc_discharges_df) > 0:
            huc_discharges_df.to_csv(discharge_file_path, index=False)
            logging.info(f"Saving discharge file to {discharge_file_path}")

    # Save the segments files
    if len(huc_segments_df) > 0:
        huc_segments_df.to_csv(segments_file_path, index=False)
        logging.info(f"Saving segment file to {segments_file_path}")

    # Note: It is ok if they are empty. Errors have been handled already for the sites_gdf.
    # This will auto have filtered out some recs based on applicable stages and/or flows
    # that have not met conditions such as -1, 0 or null.

    return sites_gdf, huc_library_df


# We are still talking about raw threshold data at this point
def __create_sb_huc_library_data(
    huc, valid_lids, sites_gdf, threshold_huc_df, metadata_json, nwm_flows_region_df
):
    '''
    Iterate through valid sites and compile all available stage thresholds.

    Each site can have up to 5 site/magnitude combinations (one per mag type).

    During processing some mags (flows/stages) might be rejected, in which case the library
    record will be dropped OR may not be created at all (depending on when the rejection happens).

    Each site/magnitude combination will be processed and eventually all valid library files will
    be rolled up into a HUC-level file. This file will include all columns needed for the rest of
    program processing.

    In later steps, more of those library records may be dropped based on further logic.

    The mapped, status, and warning columns of the master HUC-level sites_gdf will be updated as
    needed at each subsequent processing step.

    At this time, the warning column is just for listing when a magnitude is missing for a site but
    the site has at least one other valid magnitude. (For example: "Missing stage (or flow) data for
    action; minor; moderate; major.") This warning column will be merged into the status column at
    the end if the warning is still applicable.

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code.
    valid_lids - list
        List of valid NWS LID identifiers to process.
    sites_gdf - GeoDataFrame
        Master GeoDataFrame containing site information with columns for 'nws_lid', 'mapped', 'status', and 'warning'.
    threshold_huc_df - DataFrame
        HUC-level threshold data containing 'nws_lid', stage, and flow information for each site.
    metadata_json - list
        List of metadata JSONs containing site information indexed by 'identifiers']['nws_lid'].
    nwm_flows_region_df - DataFrame
        NWM flow region data used for segment retrieval.

    Returns
    -------
    sites_gdf - GeoDataFrame
        Updated with 'mapped' and 'status' columns reflecting processing results for each LID.
    huc_library_df - DataFrame
        Aggregated library records for all processed LIDs and magnitudes, containing all columns needed for downstream processing.
    huc_segments_df - DataFrame
        DataFrame of feature_ids and associated LIDs extracted during segment processing.

    CatFIM Reorg Notes (Jan 2026)
    ----------------------------
    - Previous CatFIM versions had attribute files. Attribute files are no longer needed
      because the data in those files is already present in the sites_gdf, metadata, and
      threshold files. This gives us everything we need to start library records.

    - There is some duplication here with __create_fb_library_data. The main reason it is
      split up between flow- and stage-based CatFIM is because the status messages created
      in this section of code are almost always different between the two CatFIM types.
      Since status messages are useful for tracking new bugs and errors between versions,
      we've opted to keep this processing separate for now in the interest of maintaining
      consistency with the status messaging. In the future, we can look into optimising this
      a bit more (aka making it into just one function for flow- and stage-based CatFIM).

    - TODO: of course, SB needs the data from the stage row, but down the road
      it also nees some data from the flow row. See __adjust_datum_ft.
      SO.. As we iterate through lids, makes sure they have both rows.

    SB will use the segments file, but segments will be created anyways as a checkpoint.
    TODO: Rob, do we mean that SB will NOT use the segments file? typo?
    '''

    # Initialize output dataframes
    huc_library_df = pd.DataFrame()
    huc_segments_df = pd.DataFrame()

    # Iterate through valid sites and compile all available stage thresholds
    for lid in valid_lids:

        print("")
        logging.info(f"{huc} : {lid} - Processing threshold data...")

        # ---------------------
        # Processing data and tests at the site level before processing at the magnitude level

        # Check if the site is missing all threshold (stage, flow) data
        # This means that there's no rows for the site in the threshold df (not even ones with the NA -1 val)
        lid_threshold_data = threshold_huc_df.loc[threshold_huc_df['nws_lid'] == lid].copy()
        if lid_threshold_data.empty:
            msg = 'No flow or stage thresholds available for site'  # was 'No thresholds for required categories found on WRDS API'
            logging.warning(f"{huc} : {lid} - {msg}, no rows for this site in the threshold df")
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)
            continue

        # TODO: Does this make sense? for FB, we say just:  'Missing all flow data'
        # msg FB version = 'No thresholds for required categories found on WRDS API'

        # -------------------------
        # TODO: Decide if we need to keep the q column.
        # SB does not need the flow ("q") columns but will populate it in library recs if it exists

        # -------------------------
        # Get metadata for site
        lid_metadata = next((item for item in metadata_json if item['identifiers']['nws_lid'] == lid), False)

        # TODO: Don't we already have all of the metadata in sites? For now, leave it so we can use
        # some of the tools_shared_functions for getting the segments

        # ---------------------------
        # Get the stream segments for the site
        segments_lst = __get_segments(lid_metadata, nwm_flows_region_df)

        # Update the mapped and status columns of the sites_gdf if we are missing NWM stream segments
        if not segments_lst or len(segments_lst) == 0:
            err_msg = 'Missing nwm stream segments'
            logging.warning(f'{huc} : {lid} - {err_msg}')
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, err_msg, set_mapped_to_no=True)
            continue

        elif len(segments_lst) > 0:
            # Turn segments_lst from a simple list of feature IDs into a dataframe and add a lid column
            lid_seg_df = pd.DataFrame(data=segments_lst, columns=["feature_id"])
            lid_seg_df["lid"] = lid

            # Add the HUC segments for this site to the output huc_segments_df
            huc_segments_df = pd.concat([huc_segments_df, lid_seg_df], ignore_index=True)

        # Create a library of all available threshold data for each site/magnitude combination for this LID
        # At this point, the lid, interval, and elevation data is all nodata values (-9999.0)
        sites_gdf, lid_library_df = __get_sb_library_data_per_lid(huc, lid, sites_gdf, lid_threshold_data)

        # Add LID library data to the output huc_libary_df
        if len(lid_library_df) > 0:
            huc_library_df = pd.concat([huc_library_df, lid_library_df], ignore_index=True)
    # End lid loop

    # Note: It is ok if the library and segments are empty at this point.
    # We assume the errors have been recorded and handled in the sites_gdf.
    return sites_gdf, huc_library_df, huc_segments_df


# We are still talking about raw threshold data at this point
def __get_sb_library_data_per_lid(huc, lid, sites_gdf, lid_threshold_data):
    '''
    Create a dataframe with the basic five library recs when applicable, based on their stage and flow values.
    We will do SB intervals later in mapping.

    For now this is for stage-based only. The flow-based version of this processing is found in
    __get_fb_discharge_and_library_data_per_lid. Most of the tests between the two are the same,
    the messages are just a bit different and that function also handles discharge data.

    Processes
    ---------
    - To get here, we already validated the sites for restricted sites and it does have a least some flow threshold data.

    - Processes a lid for SB and builds up the library (attribute) data for each magnitude_type for this lid.
    Later, it will all be rolled up to one HUC level attlibrary file.

    - It also creates full df schema that can be directly used to create library files later. Note:
    The columns returned here include some additional columns then previous versions
    in order to be re-used for library data rows.

    - As always, the sites_gdf is constantly being updated for mapping and status messages. When a mag is
    missing, the sites_gdf be updated with the normal "missing action, minor.. .message"

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code.
    lid - STR
        Site ID.
    sites_gdf - Geopandas GeoDataFrame
        Table of sites.
    lid_threshold_data - Pandas Dataframe
        Threshold HUC df, subsetted for the specific site.

    Returns
    -------
    sites_gdf - Geopandas GeoDataFrame
        Table of sites, updated.
    lid_library_df - Pandas Dataframe
        Table of library data for eventual mapping.


    CatFIM Reorg Notes (Jan 26)
    ---------------------------

    There is some duplication here with flow-based processing. The main reason it is
    split up between flow- and stage-based CatFIM is because the status messages created
    in this section of code are almost always different between the two CatFIM types.
    Since status messages are useful for tracking new bugs and errors between versions,
    we've opted to keep this processing separate for now in the interest of maintaining
    consistency with the status messaging. In the future, we can look into optimizing this
    a bit more (aka making it into just one function for flow- and stage-based CatFIM).

    # procesing each magnitude in here, now that the tests that are not mag specific are done
    # It will append data to the library csv as it goes along.
    # This will auto have filtered out some stages based on various conditions such as -1, 0 or null

    '''
    logging.info(f"{huc} : {lid} - Building the initial library df of all applicable magnitudes...")

    # ======================
    # Initialize output dataframe
    lid_library_df = pd.DataFrame()

    # Copy the site data from the sites_gdf
    # Note: You cannot update this rec, only the parent site_gdf for the site. Use lid_sites_gdf for read-only.
    lid_sites_gdf = sites_gdf.loc[sites_gdf["nws_lid"] == lid].copy()

    # Make a dictionary of available stages for the site
    stages = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'stages'].to_dict(
        orient='records'
    )[0]

    # Note:
    # The SB version of this code reads in both stages and flows, but here we
    # only need to read in stages. FB does use some stage values, but only to
    # add to the library tables. It does not use the value in calcs.
    # These are lid-specific threshold data. These lists may change as processing
    # continues and some get rejected.

    # -----------------------------------
    # Iterate through magnitudes and create records for each valid site/magnitude combination

    invalid_stages = []
    for magnitude_type in csf.MAGNITUDES_TYPES:
        try:
            # -------------
            logging.info(f"{huc} : {lid} : {magnitude_type} - Building initial library rec")

            # Get stage value (will be float type, rounded to 2 decimal points)
            stage_value = stages[magnitude_type]

            # Exit if stage value is invalid (Value of -1 or nodata)
            if stage_value == -1 or stage_value == 0:
                logging.warning(
                    f"{huc} : {lid} : {magnitude_type} - has an invalid or n/a stage value of {stage_value}"
                )
                invalid_stages.append(magnitude_type)
                continue

            # -------------
            # Create initial library record for the site/magnitude combination
            # Some of these site/magnitude records will be updated or rejected (removed) based on logic down the road.

            lid_mag_library_rec_df = __create_lid_mag_library_rec(
                "sb", lid, lid_sites_gdf, magnitude_type, lid_threshold_data
            )

            # TODO: Technically this is not the most efficent as for each mag type that is valid
            # by this point, they are all identical, except for the stage and flow value.
            # But.. for now, at least it is simple to follow.

            # Add library record for the site/magnitude combination to the output df
            # (we should always have one unless something catastrophic occurred)
            lid_library_df = pd.concat([lid_library_df, lid_mag_library_rec_df], ignore_index=True)

        except Exception as ex:
            msg = f'Error with stage/threshold processing of {stage_value} for {magnitude_type} stage'
            logging.critical(f"{huc} : {lid} : {magnitude_type} - {msg}")
            logging.critical(traceback.format_exc())
            raise ex

    # -------------
    # Give an error if all stages are unavailable, update mapped and status columns accordingly
    if len(invalid_stages) == 5:
        # msg = 'No valid flow values are available' # This was the message version in FB
        msg = 'No stage thresholds available for required categories (only invalid or NA flow vals found)'
        logging.warning(f"{huc} : {lid} - {msg}")
        sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)

        return sites_gdf, lid_library_df

    # Give warning if some stages are unavailable, update mapped and status columns accordingly
    elif len(invalid_stages) > 0:
        warning_mags = '; '.join(invalid_stages)
        warning_message = f"Missing stage data for {warning_mags}"
        logging.warning(f"{huc} : {lid} - {warning_message}")
        sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, warning_message, set_mapped_to_no=False)

    # Note: It is ok if lid_library_df goes back empty.
    return sites_gdf, lid_library_df


# We are still talking about raw threshold data at this point
def __create_fb_huc_library_data(
    huc, valid_lids, sites_gdf, threshold_huc_df, metadata_json, nwm_flows_region_df
):
    '''
    Attribute files are no longer needed as the data in those files were already present in sites_gdf,
    metadata and threshold files. This gives us everything we need to start library records.
    Each lid will create 0 to 5 library records initially (one per mag type).
    During processing some mags (flows/stages) might be rejected which will either drop a library
    record or not make it in the first place.

    Each lid, then mag will be processed, eventually rolling up all valid library files to a huc
    level file and will include all columns needed for the rest of program processing.

    In later steps, more of those library records may be dropped based on further logic.

    This master HUC level sites_gdf will be updated at all stages along the way to update
    the mapped, status and warning columns as applicable. At this time, the warning column
    is just for listing when a mag is missing for a lid but has at least one valid mag.
    ie) Missing stage (or flow) data for action; minor; moderate; major. That will be merged
    into the status column at the end if still applicable.

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code.
    valid_lids - list
        List of valid NWS LID identifiers to process.
    sites_gdf - Geopandas GeoDataFrame
        Table of sites.
    threshold_huc_df - DataFrame
        HUC-level threshold data containing 'nws_lid', stage, and flow information for each site.
    metadata_json - list
        List of metadata JSONs containing site information indexed by 'identifiers']['nws_lid'].
    nwm_flows_region_df - DataFrame
        NWM flow region data used for segment retrieval.
    
    Returns
    -------
    sites_gdf - Geopandas GeoDataFrame
        Table of sites, updated.
    huc_library_df - Pandas DataFrame
        Aggregated library records for all processed LIDs and magnitudes, containing all columns needed for downstream processing.
    huc_segments_df - Pandas DataFrame
        DataFrame of feature_ids and associated LIDs extracted during segment processing.
    huc_discharges_df - Pandas DataFrame
        DataFrame of flow discharge values for all associated LIDs.


    CatFIM Reorg. Notes (Jan 26)
    ----------------------------

    Yes... catfim_process_huc.py creates these files but only removing for restarts.
    Maybe it is ok to keep redefining as it won't change and helps for one less
    arg to pass around. It is not used by SB.

    Technically, we don't need both a discharge and a segment file as they have the same
    first two columns, but it is easier to follow as seperate files.

    '''

    # Initialize output dataframes
    huc_library_df = pd.DataFrame()
    huc_discharges_df = pd.DataFrame()  # formerly flow files in the flow dir
    huc_segments_df = pd.DataFrame()

    # Iterate through valid sites and compile all available flow thresholds
    for lid in valid_lids:

        print("")
        logging.info(f"{huc} : {lid} - Threshold data processing...")

        # ---------------------
        # Processing data and tests the lid level before processing at the magnitude level

        # Check the lid to see if it is missing all threshold (stage, flow) data.
        # This means that there's no rows for the site in the threshold df (not even ones with the NA -1 val)
        lid_threshold_data = threshold_huc_df.loc[threshold_huc_df['nws_lid'] == lid].copy()
        if lid_threshold_data.empty:
            msg = 'No flow or stage thresholds available for site'  # was 'Missing all flow data'
            logging.warning(f"{huc} : {lid} - {msg}, no rows for this site in the threshold df")
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)
            continue

        # -------------------------
        # Check if stage values are available for the site

        # Note: Even though FB does not actually use stage data in any calcs, it must have all 5 stage values to be valid
        # and in the FB library file, there are no records without "stage" values. # TODO: Sanity check this logic
        stages = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'stages'].to_dict(
            orient='records'
        )[0]

        # If no stages are available for site, log a warning and move on to next site
        # We do want to reject if they are all missing which would indicate a code problem ?? TODO: Sanity check this logic
        # TODO: Apr '26: adjusted wording and made it so that this is no longer unmapping a site...
        if all(stages.get(magnitude_type, None) is None for magnitude_type in csf.MAGNITUDES_TYPES):
            msg = 'No stage thresholds available for required categories'  # was 'Error getting flows values from WRDS API'
            logging.warning(f"{huc} : {lid} - {msg}")
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=False)
            # continue # TEMP DEBUG - maybe we don't want to exit if this happens?
            # TODO: See if any sites with this warning make valid or invalid sites later on 

        # -------------------------
        # Create flows dictionary for the site
        flows = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'flows'].to_dict(
            orient='records'
        )[0]

        # If all flows are missing for site, log a warning and move on to next site
        if all(flows.get(magnitude_type, None) is None for magnitude_type in csf.MAGNITUDES_TYPES):
            msg = 'No flow thresholds available for required categories'  # was "Missing all calculated flows for all stages"
            logging.warning(
                f"{huc} : {lid} - {msg}, rows for this site available in the threshold df but flow values for required categories are missing"
            )
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)
            continue

        # -------------------------
        # Get metadata for site
        lid_metadata = next((item for item in metadata_json if item['identifiers']['nws_lid'] == lid), False)

        # TODO: Don't we already have all of the metadata in sites? For now, leave it so we can use
        # some of the tools_shared_functions for getting the segments

        # ---------------------------
        # Get the stream segments for the site
        segments_lst = __get_segments(lid_metadata, nwm_flows_region_df)

        # Update the mapped and status columns of the sites_gdf if we are missing NWM stream segments
        if not segments_lst or len(segments_lst) == 0:
            err_msg = 'No NWM stream segments affiliated with site'  # previously said: 'Missing nwm stream segments'
            logging.warning(f'{huc} : {lid} - {err_msg}')
            sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, err_msg, set_mapped_to_no=True)
            continue

        elif len(segments_lst) > 0:
            # Turn segments_lst from a simple list of feature IDs into a dataframe and add a lid column
            lid_seg_df = pd.DataFrame(data=segments_lst, columns=["feature_id"])
            lid_seg_df["lid"] = lid

            # Add the HUC segments for this site to the output huc_segments_df
            huc_segments_df = pd.concat([huc_segments_df, lid_seg_df], ignore_index=True)

        # Only tests left to do are the magnitude-specific tests (all others are done)
        # It will append data to the fb library csv as it goes along.
        # We can assume we have segment data.

        # Create a library of all available threshold data for each site/magnitude combination for this LID
        sites_gdf, lid_library_df, lid_discharges_df = __get_fb_discharge_and_library_data_per_lid(
            huc, lid, sites_gdf, lid_threshold_data, segments_lst
        )

        # Add LID library data to the output huc_libary_df
        if len(lid_library_df) > 0:
            huc_library_df = pd.concat([huc_library_df, lid_library_df], ignore_index=True)

        # Add the LID discharges data to the output huc_discharges_df
        if len(lid_discharges_df) > 0:
            huc_discharges_df = pd.concat([huc_discharges_df, lid_discharges_df], ignore_index=True)
        # TODO: Decide if it is ok if lid_discharges_df is empty? maybe errors were caught in __get_fb_discharge
        # and the sites_gdf has already been updated?
    # End lid loop

    # Note: It is ok if the library and segments are empty at this point
    # We assume the errors have been recorded and handled for the sites_gdf
    return sites_gdf, huc_library_df, huc_segments_df, huc_discharges_df


# We are still talking about raw threshold data at this point
def __get_fb_discharge_and_library_data_per_lid(huc, lid, sites_gdf, lid_threshold_data, segments_lst):
    '''
    Assemble the flow thresholds and discharge data for each site.

    For now this is for flow-based only. The stage-based version of this processing is found in
    __get_sb_library_data_per_lid. Most of the tests between the two are the same, the messages
    are just a bit different and this function also handles discharge data.

    Process
    -------
    To get here, we already validated the sites for restricted sites and it does have a least some flow threshold data.

    Processes a lid for FB and builds up the library (attribute) data for each magnitude_type for this lid.
    Later, it will all be rolled up to one HUC level attlibrary file.

    It also creates full df schema that can be directly used to create library files later. Note:
    The columns returned here include some additional columns then previous versions
    in order to be re-used for library data rows.

    As always, the sites_gdf is constantly being updated for mapping and status messages. When a mag is
    missing, the sites_gdf be updated with the normal "missing action, minor.. .message"

    This function also creates the discharge df, previously called flow files,
    which previously was done via a folder structure of
    {huc}/{lid}/{mag}/file name:  ie)  flows/01020002/grnm1/action/grnm1_huc_01020002_flows_action.csv. This will
    flatten all discharges for all sites / mags into one csv at the HUC level.
    The flows folder is no longer needed. The discharge df for this lid returned will
    be rolled up to a HUC level discharge file.

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code.
    lid - STR
        Site ID.
    sites_gdf - Geopandas GeoDataFrame
        Table of sites.
    lid_threshold_data - Pandas Dataframe
        Threshold HUC df, subsetted for the specific site.
    segments_lst - List of String
        List of NWM Feature IDs relevant to the site.

    Returns
    -------
    sites_gdf - Geopandas GeoDataFrame
        Table of sites, updated.
    lid_library_df - Pandas DataFrame
        An library dataset for this lid with basic starting columns for all valid mags. If no recs returned, then
        the lid has no valid mags to process. It will return between 0 and 5 FB library rows in it.
    lid_discharges_df - Pandas DataFrame
        THe discharge dataset for this lid.

    CatFIM Reorg Notes (Jan 26)
    ---------------------------
    There is some duplication here with stage-based processing. The main reason it is
    split up between flow- and stage-based CatFIM is because the status messages created
    in this section of code are almost always different between the two CatFIM types.
    Since status messages are useful for tracking new bugs and errors between versions,
    we've opted to keep this processing separate for now in the interest of maintaining
    consistency with the status messaging. In the future, we can look into optimizing this
    a bit more (aka making it into just one function for flow- and stage-based CatFIM).
    '''

    logging.info(
        f"{huc} : {lid} - Building the initial library and discharge data all applicable magnitudes..."
    )

    # ======================
    # Initialize output dataframes
    lid_library_df = pd.DataFrame()
    lid_discharges_df = pd.DataFrame()

    # Copy the site data from the sites_gdf
    # Note: You can not update this rec, only the parent site_gdf for the site. Use lid_sites_gdf for read-only.
    lid_sites_gdf = sites_gdf.loc[sites_gdf["nws_lid"] == lid].copy()

    # Make a dictionary of available stages and flows for the site
    stages = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'stages'].to_dict(
        orient='records'
    )[0]
    flows = lid_threshold_data.loc[lid_threshold_data['threshold_type'] == 'flows'].to_dict(orient='records')[
        0
    ]
    # Note:
    # FB does use some stage values, but only to add to the library tables. It does not use the value in calcs.
    # These are lid-specific threshold data. These lists may change as processing continues and some get rejected.

    # -----------------------------------
    # Iterate through magnitudes and create data records for each valid site/magnitude combination
    # and get discharge values (previously saved in /flows/ folders, now saved in HUC-specific folders)

    invalid_flows = []
    for magnitude_type in csf.MAGNITUDES_TYPES:
        try:
            # -------------
            logging.info(
                f"{huc} : {lid} : {magnitude_type} - Building initial library rec and discharge data"
            )

            # Get flow value (will be float type, rounded to 2 decimal points)
            flow_value = flows[magnitude_type]

            # Exit if stage value is invalid (Value of -1 or nodata)
            if flow_value == -1 or flow_value == 0:
                logging.warning(
                    f"{huc} : {lid} : {magnitude_type} - Invalid or N/A flow value found: {flow_value}"
                )
                invalid_flows.append(magnitude_type)
                continue

            # -------------
            # Process discharge data

            # Given a list of NWM segments and a flow value in cfs, converts flow to cms and returns a formatted flow DF
            flow_info_df = flow_data(segments_lst, flow_value)

            if len(flow_info_df) > 0:
                flow_info_df["lid"] = lid
                flow_info_df["magnitude"] = magnitude_type
                lid_discharges_df = pd.concat([lid_discharges_df, flow_info_df], ignore_index=True)
            else:
                logging.warning(
                    f"{huc} : {lid} : {magnitude_type} - Failed to get segment flow data for magnitude type"
                )
                invalid_flows.append(magnitude_type)
                continue

            # While FB does not use logic against the stage columns, earlier versions
            # output files never had this as a blank value, so test for it
            stage_value = stages[magnitude_type]
            if stage_value == -1 or stage_value == 0:
                logging.warning(
                    f"{huc} : {lid} : {magnitude_type} - Invalid or N/A stage value found: {stage_value}"
                )
                # invalid_flows.append(magnitude_type)
                # continue # Jan '26: Removed this for now, because I think it should almost never even happen that we
                # would have a calc flow value but not a stage val (because the stage is used in WRDS to make the calc
                # flow val). Keeping the warning because it could help us trace something weird in the future.

            # -------------
            # Create initial library record for the site/magnitude combination
            # Some of these site/magnitude records will be updated or rejected (removed) based on logic down the road.

            lid_mag_library_rec_df = __create_lid_mag_library_rec(
                "fb", lid, lid_sites_gdf, magnitude_type, lid_threshold_data
            )

            # Notes:
            # FB does include the stage value in the final library columns but does not use the data for any logic

            # We will create an initial library rec for this lid and mag based on valid
            # lid and mag type.
            # Ultimately, only valid recs, will use the same library recs as the starting
            # point for all relavent library records. Some of these will be updated, rejected (removed)
            # at laster stages based on logic down the road.

            # TODO: Technically this is not the most efficent as for each mag type that is valid
            # by this point, they are all identical, except for the stage and flow value.
            # But.. for now, at least it is simple to follow.

            # Add library record for the site/magnitude combination to the output df
            # (we should always have one unless something catastrophic occurred)
            lid_library_df = pd.concat([lid_library_df, lid_mag_library_rec_df], ignore_index=True)

        except Exception as ex:
            msg = f'Error with flow/threshold processing of {flow_value} for {magnitude_type} stage'
            logging.critical(f"{huc} : {lid} : {magnitude_type} - {msg}")
            logging.critical(traceback.format_exc())
            raise ex

    # -------------
    # Give descriptive warnings if some or all flows are unavailable, update mapped and status columns accordingly

    if len(invalid_flows) == 5:
        # This means there were rows for the site in the threshold df but all the flow values were invalid (ie -1 or 0)
        msg = 'No flow thresholds available for required categories (only invalid or NA flow vals found)'  # Was 'No valid flow values are available'
        logging.warning(f"{huc} : {lid} - {msg}")
        sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, msg, set_mapped_to_no=True)
        return sites_gdf, lid_library_df, lid_discharges_df

    elif len(invalid_flows) > 0:
        warning_mags = ', '.join(invalid_flows)
        warning_message = f"Missing or invalid flow data for {warning_mags}"
        logging.warning(f"{huc} : {lid} - {warning_message}")
        sites_gdf = csf.update_line_status_or_warning(lid, sites_gdf, warning_message, set_mapped_to_no=False)

    # Note: It is ok if lid_library_df and/or lid_discharges_df goes back empty.
    return sites_gdf, lid_library_df, lid_discharges_df


# We are still talking about raw threshold data at this point
def __create_lid_mag_library_rec(catfim_type, lid, lid_sites_gdf, magnitude_type, lid_threshold_data):
    '''
    Function can handle both FB and SB.

    Process
    -------
    This creates just one new library df based on a lid and stage and flow
    At this point, not all columns are populated, mostly just ones needed for futher
    processing in place such as SB evelvations and inundation. Most
    columns for the library rows are from the meta table and most of them we
    will not populate them as they are not being used. We will only add the columns
    we need plus a few more for tracking. Later we will add most metadata fields.

    Some columns such a FB.stage are populated but not used later at any time.

    Arguments
    ---------
    catfim_type - STR
        'sb' or 'fb'
    lid - STR
        Site ID.
    lid_sites_gdf - Pandas GeoDataFrame
        Sites table, subsetted for the specific LID.
    magnitude_type - STR
        Flow magnitude type (i.e. action, minor, moderate, major, record).
    lid_threshold_data - Pandas Dataframe
        Threshold HUC df, subsetted for the specific site.

    Returns
    -------
    line_df - Pandas DataFrame
        A df with one single library row rec based on just one mag, which can be concat later.
        It assumes all validation has been already done. However, we can add some validation in here if we like.

    CatFIM Reorg Notes - Jan 26
    ----------------------------

    TODO: do we need any column validation in here? It's twin in both the SB and FB code does not appear to
    validate any.

    TODO: For SB, if the q_src is empty, it fill fail the record in the datum code
    we need to look into that
    '''

    # Get flow data from site threshold data
    q_uni = ""  # flow units
    q_src = ""  # flow source
    flow_value = ""  # flow value
    flow_data = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "flows"]
    if len(flow_data) > 0:
        q_uni = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "flows", 'units'].item()
        q_src = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "flows", 'source'].item()
        flow_value = lid_threshold_data.loc[
            lid_threshold_data["threshold_type"] == "flows", magnitude_type
        ].item()

    # Get stage data from site threshold data
    stage_uni = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "stages", 'units'].item()
    s_src = lid_threshold_data.loc[lid_threshold_data["threshold_type"] == "stages", 'source'].item()
    stage_value = lid_threshold_data.loc[
        lid_threshold_data["threshold_type"] == "stages", magnitude_type
    ].item()

    # Assign all columns that are applicable to both FB and SB
    # Note: Each col is added on at a time in case there is a cell problem
    line_df = pd.DataFrame({'nws_lid': [lid]})
    line_df['name'] = lid_sites_gdf.iloc[0]["name"]
    line_df['huc'] = lid_sites_gdf.iloc[0]["HUC8"]
    line_df['magnitude'] = magnitude_type
    line_df['stage'] = float(stage_value)  # are mag type specific stage values
    line_df['stage_uni'] = stage_uni
    line_df['s_src'] = s_src
    line_df['q'] = flow_value  # might be empty and yes.. it is a string for now
    line_df['q_uni'] = q_uni
    line_df['q_src'] = q_src

    # Note: In the Jan 26 updates we removed the section of code where the
    # broad metadata (WFO, RFC, state, county, time, coordinates) is added
    # to this DF. We will add the metadata in at the end of the HUC processing
    # in order to avoid passsing along so muuch data unnecessarily at the
    # site-level processing.

    if catfim_type == "sb":
        # Add columns needed for future SB processing

        # We keep this as the original value that came from the threshold but the stage
        # column could change based on calcs.
        line_df["rfc_stage"] = float(stage_value)

        line_df["datum_adj_ft"] = -9999.0
        line_df["datum_adj_wse_ft"] = -9999.0
        line_df["datum_adj_wse_m"] = -9999.0
        line_df["lid_alt_ft"] = -9999.0
        line_df["lid_alt_m"] = -9999.0

        line_df["is_interval"] = False
        line_df["interval_stage"] = None
        line_df["lid_usgs_elev"] = -9999.0  # This is a temp processing column

    return line_df


def __get_segments(lid_metadata, nwm_flows_region_df):
    '''
    Create a simple list of feature IDs for the stream segments
    relevant to the site. (Just a list, no other columns.)

    The output from this will be used to create a segments file for future
    processing and as checkpoint.

    FB will use the segment data now to sort out the discharge file.
    SB will load the segments file later for proceessing in mapping.

    This will help keep mapping segregated so it can be run as a standalone
    tool if needed. We are trying to keep everything needed for inundation
    inside the mapping.py file and only need a huc and output path.

    Arguments
    ---------
    lid_metadata - DataFrame
        Metadata for site
    nwm_flows_region_df - DataFrame
        National Water Model (NWM) flowline data specific to the region (CONUS, AK, PR, or GU)

    Returns
    -------
    segments_lst - LIST
        List of feature IDs for the stream segments relevant to the site

    '''
    # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
    unfiltered_segments = list(set(get_nwm_segs(lid_metadata)))

    desired_order = lid_metadata['nwm_feature_data']['stream_order']

    # Filter segments to be of like stream order.
    segments_lst = filter_nwm_segments_by_stream_order(
        unfiltered_segments, desired_order, nwm_flows_region_df
    )
    # Previous input was nwm_flows_df, but now it is region specific df (9/25/25)

    return segments_lst


# generate_categorical_fim_flows.py cannot be called from command line at this time.