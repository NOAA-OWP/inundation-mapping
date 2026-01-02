#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation
from rasterio.features import shapes
from rasterio.warp import Resampling, reproject
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from tools_shared_functions import mask_out_lakes
from tqdm import tqdm

import src.utils.fim_logger as fl
from src.utils.shared_functions import getDriver
from src.utils.shared_variables import VIZ_PROJECTION


"""
Oct/Nov 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing

Tenative notes:
    - Some of the functions in here may move or be split to smaller functions.

    - Data acquision such as meta, threshold or flows, should be moved to generate_categorical_fim_flows.py
      which has been renamed to generate_categorical_fim_thresholds.

    - This script will focus on inundation, tifs, gpkgs, etc. for both FB and SB.

    - Some current functions from other files such as generate_categorical_fim.py and maybe
      generate_categorical_fim_thresholds.py will be moved here if it is related to inundation,
      etc as mentioned above.


"""

# This file should focus primarily on inunation and creating tifs and final gpkgs for this HUC.
# However, some logic processing is done here as well. We might move some of that later.


'''

Aug 2024
This script was upgraded significantly with lots of misc TODO's embedded.
Lots of inline documenation needs updating as well

Oct 2025
Doc strings and improved documentation was added.

'''


# will become global once initiallized
FLOG = fl.FIM_logger()
MP_LOG = fl.FIM_logger()

gpd.options.io_engine = "pyogrio"


# Technically, this is once called as a non MP, but also called in an MP pool
# we will use an MP object either way
def produce_stage_based_lid_tifs(
    stage_val,
    datum_adj_ft,
    branch_dir,
    lid_usgs_elev,
    lid_altitude,
    fim_dir,
    segments,
    lid,
    huc,
    lid_directory,
    category,
    category_key,
    number_of_jobs,
    mp_parent_log_file,
    child_log_file_prefix,
):
    '''
    Only used for stage-based CatFIM.

    Generates stage-based inundation TIFF files for a given LID (Location ID) and category, mosaics branch-level inundation maps,
    and removes intermediary files. Handles multi-processing across branches and logs progress and warnings.

    Parameters
    ----------
    stage_val : float
        The stage value to use for inundation mapping.
    datum_adj_ft : float
        Datum adjustment in feet to be applied to the stage value.
    branch_dir : str
        Directory containing branch subdirectories for processing.
    lid_usgs_elev : float
        USGS elevation for the LID gage, used to calculate HAND stage.
    lid_altitude : float
        Altitude adjustment for the LID location.
    fim_dir : str
        Base directory for HAND FIM data.
    segments : list
        List of NWM segment feature IDs to process for inundation.
    lid : str
        Location ID string.
    huc : str
        Hydrologic Unit Code for the watershed.
    lid_directory : str
        Directory to store output TIFF files for the LID.
    category : str
        Category name for the inundation mapping (e.g., "action", "minor").
    category_key : str
        Key string representing the category and magnitude.
    number_of_jobs : int
        Number of parallel jobs to use for multi-processing.
    mp_parent_log_file : str
        Path to the parent log file for multi-processing logging.
    child_log_file_prefix : str
        Prefix for child log files in multi-processing.

    Returns
    -------
    messages : list
        List of warning or status messages generated during processing.
    hand_stage : int
        Computed HAND stage in millimeters.
    datum_adj_wse : float
        Datum-adjusted water surface elevation in feet.
    datum_adj_wse_m : float
        Datum-adjusted water surface elevation in meters.

    Notes
    -----
    - The function performs multi-processing across branches to generate inundation maps.
    - Branch-level TIFFs are mosaicked into a single extent TIFF for the LID and category.
    - Lakes are masked out from the final inundation array.
    - Intermediary branch TIFFs are deleted after merging to save space.
    - Logging is performed throughout for traceability and error handling.
    - If negative HAND stage or missing segments are detected, processing is skipped for those cases.

    '''

    MP_LOG.MP_Log_setup(mp_parent_log_file, child_log_file_prefix)

    messages = []

    huc_lid_cat_id = f"{huc} : {lid} : {category_key}"
    MP_LOG.trace(f"{huc_lid_cat_id}: Starting to create tifs")

    # Determine datum-offset water surface elevation (from above).
    datum_adj_wse = stage_val + datum_adj_ft + lid_altitude
    datum_adj_wse_m = datum_adj_wse * 0.3048  # Convert ft to m

    # Subtract HAND gage elevation from HAND WSE to get HAND stage.
    hand_stage_m = datum_adj_wse_m - lid_usgs_elev
    hand_stage = (
        hand_stage_m if str(huc)[:2] == '19' else round(hand_stage_m * 1000)
    )  # convert to mm to match HAND

    # If hand_stage is negative, write message and exit out
    if hand_stage < 0:
        msg = f": Negative hand stage ({hand_stage} mm) detected, no inundation possible"
        # messages.append(lid + msg)
        MP_LOG.warning(huc_lid_cat_id + msg)
        return messages, hand_stage, datum_adj_wse, datum_adj_wse_m

    # If no segments, write message and exit out
    if not segments or len(segments) == 0:
        msg = ':missing nwm segments'
        messages.append(lid + msg)
        MP_LOG.warning(huc_lid_cat_id + msg)
        return messages, hand_stage, datum_adj_wse, datum_adj_wse_m

    # Produce extent tif hand_stage. Multiprocess across branches.
    # branches = os.listdir(branch_dir)
    # MP_LOG.trace(f"{huc_lid_cat_id} branch_dir is {branch_dir}")

    branches = [x for x in os.listdir(branch_dir) if os.path.isdir(os.path.join(branch_dir, x))]
    branches.sort()
    # MP_LOG.trace(f"{huc_lid_cat_id} branches are {branches}")

    # This is an MP in an MP. We want this set of mp's to roll up to the
    # parent MP file, and not the full catfim parent log. We roll this child MP into
    # it's parent mp and later that parent MP will rollup to the catfim file.
    child_log_file_prefix = MP_LOG.MP_calc_prefix_name(MP_LOG.LOG_FILE_PATH, "MP_branch")
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        for branch in branches:
            msg_id_w_branch = f"{huc_lid_cat_id}: {branch}"
            # MP_LOG.trace(f"{msg_id_w_branch} : inundating branch")
            # Define paths to necessary files to produce inundation grids.
            full_branch_path = os.path.join(branch_dir, branch)
            rem_path = os.path.join(fim_dir, huc, full_branch_path, 'rem_zeroed_masked_' + branch + '.tif')
            catchments_path = os.path.join(
                fim_dir,
                huc,
                full_branch_path,
                'gw_catchments_reaches_filtered_addedAttributes_' + branch + '.tif',
            )
            hydrotable_path = os.path.join(fim_dir, huc, full_branch_path, 'hydroTable_' + branch + '.csv')

            # sometimes, these can fail to exist if a branchf initial failed during HAND generation
            if not os.path.exists(rem_path):
                msg = ":rem doesn't exist (could be bad branch)"
                # messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                continue
            if not os.path.exists(catchments_path):
                msg = ":catchments files don't exist (could be bad branch)"
                # messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                continue
            if not os.path.exists(hydrotable_path):
                msg = ":hydrotable doesn't exist (could be bad branch)"
                # messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                continue

            # Use hydroTable to determine hydroid_list from site_ms_segments.
            hydrotable_df = pd.read_csv(
                hydrotable_path, low_memory=False, dtype={'HUC': str, 'LakeID': float, 'subdiv_applied': int}
            )

            hydroid_list, lake_hydroid_list, nolake_hydroid_list = [], [], []

            # Determine hydroids at which to perform inundation
            for feature_id in segments:

                try:
                    subset_hydrotable_df = hydrotable_df[hydrotable_df['feature_id'] == int(feature_id)]

                    # List of HydroID's where the LakeID is greater than 0 (which shows that there's a lake)
                    lake_hydroid_list = list(
                        subset_hydrotable_df.loc[subset_hydrotable_df['LakeID'] > 0]['HydroID'].unique()
                    )

                    # If lakes are detected, add info to the log
                    if len(lake_hydroid_list) > 0:
                        MP_LOG.trace(
                            f"HydroIDs {lake_hydroid_list} removed from processing because they contain lakes. FeatureId is {feature_id}."
                        )

                    # List of HydroID's where there the LakeID is less than 0 (no lake, so we can inundate)
                    nolake_hydroid_list = list(
                        subset_hydrotable_df.loc[subset_hydrotable_df['LakeID'] < 0]['HydroID'].unique()
                    )

                    # Add HydroIDs without lakes to the list to process
                    hydroid_list += nolake_hydroid_list

                except IndexError:
                    MP_LOG.trace(
                        f"Index Error for {msg_id_w_branch}. FeatureId is {feature_id} : Continuing on."
                    )
                    pass

            # Create inundation maps with branch and stage data
            # only sites /categories that got this far are valid and can be inundated
            try:
                # MP_LOG.trace(f"{huc_lid_cat_id} : branch = {branch} :  Generating stage-based FIM")

                executor.submit(
                    produce_inundated_branch_tif,
                    rem_path,
                    catchments_path,
                    hydroid_list,
                    hand_stage,
                    lid_directory,
                    category_key,
                    huc,
                    lid,
                    branch,
                    MP_LOG.LOG_FILE_PATH,
                    child_log_file_prefix,
                )

            except Exception:
                msg = f':inundation failed at {category}'
                messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                MP_LOG.error(traceback.format_exc())

    # Nov 2024: Fix this. The logs get out of order as it is can be a MP in MP
    # hold on this
    # MP_LOG.merge_log_files(mp_parent_log_file, child_log_file_prefix, True)

    # -- MOSAIC -- #
    # Merge all rasters in lid_directory that have the same magnitude/category.
    path_list = []

    # we are looking for the branch files for the category/stage
    # or any given stage interval

    lid_dir_list = [x for x in os.listdir(lid_directory) if category_key in x]
    lid_dir_list.sort()  # To force branch 0 first in list, sort

    # MP_LOG.lprint(f"{huc}: {lid} : Merging branch files {huc_lid_cat_id}")
    # MP_LOG.trace(f"{huc_lid_cat_id} : lid_dir_list is... {lid_dir_list}")
    # MP_LOG.trace("")

    for f in lid_dir_list:
        path_list.append(os.path.join(lid_directory, f))

    # Merging all of the branch tifs into one lid_category tif
    if len(lid_dir_list) > 0:
        zero_branch_grid = path_list[0]
        zero_branch_src = rasterio.open(zero_branch_grid)
        zero_branch_array = zero_branch_src.read(1)
        summed_array = zero_branch_array  # Initialize it as the branch zero array

        output_tif = os.path.join(lid_directory, lid + '_' + category_key + '_extent.tif')
        MP_LOG.trace(f"{huc_lid_cat_id}: Merging all branches into output file to be saved as {output_tif}")

        # Loop through remaining items in list and sum them with summed_array
        for remaining_raster in path_list[1:]:

            remaining_raster_src = rasterio.open(remaining_raster)
            remaining_raster_array_original = remaining_raster_src.read(1)

            # TODO: Nov 2024: We should need to reproject at all (Research if this works wihtout it)
            # Reproject non-branch-zero grids so I can sum them with the branch zero grid
            remaining_raster_array = np.empty(zero_branch_array.shape, dtype=np.int8)
            reproject(
                remaining_raster_array_original,
                destination=remaining_raster_array,
                src_transform=remaining_raster_src.transform,
                src_crs=remaining_raster_src.crs,
                src_nodata=remaining_raster_src.nodata,
                dst_transform=zero_branch_src.transform,
                dst_crs=zero_branch_src.crs,
                dst_nodata=0,
                dst_resolution=zero_branch_src.res,
                resampling=Resampling.nearest,
            )
            # Sum rasters
            summed_array = summed_array + remaining_raster_array

        # Mask out the lakes from the inundation array
        summed_masked_array, mask_status = mask_out_lakes(summed_array, huc, zero_branch_src, fim_dir)
        MP_LOG.trace(f"{huc_lid_cat_id}: {mask_status}")

        del zero_branch_array, summed_array  # Clean up

        # Define path to merged file, in same format as expected by post_process_cat_fim_for_viz function
        profile = zero_branch_src.profile
        summed_masked_array = summed_masked_array.astype('uint8')
        with rasterio.open(output_tif, 'w', **profile) as dst:
            dst.write(summed_masked_array, 1)
            # MP_LOG.lprint(f"{huc_lid_cat_id}: branch rollup extent file saved at {output_tif}")

        # For space reasons, we need to delete all of the intermediary files such as:
        #    Stage: grmn3_action_extent_0.tif, grmn3_action_extent_1933000003.tif. The give aways are a number before
        #        the .tif
        #    Flows: allm1_action_12p0ft_extent_01010002_0.tif, allm1_action_12p0ft_extent_01010002_7170000001.tif
        #       your give away is to just delete any file that has the HUC number in teh file name
        # The intermediatary are all inundated branch tifs.

        # The ones we want to keep end at _extent.tif and remove ones that have _extent_*.tif
        # MP_LOG.lprint(f"{huc_lid_cat_id}: Removing interium inundated branch files")
        branch_tifs = glob.glob(f"{lid_directory}/{lid}_{category_key}_extent_*.tif")
        for tif_file in branch_tifs:
            os.remove(tif_file)

    # else:
    #     MP_LOG.warning(f"{huc}: {lid}: Merging {category_key} : no valid inundated branches")

    return messages, hand_stage, datum_adj_wse, datum_adj_wse_m


# This is part of an MP call and needs MP_LOG
def produce_inundated_branch_tif(
    rem_path,
    catchments_path,
    hydroid_list,
    hand_stage,
    lid_directory,
    category_key,
    huc,
    lid,
    branch,
    parent_log_output_file,
    child_log_file_prefix,
):
    '''
    Only used in stage-based CatFIM.

    Generates a binary inundation raster (GeoTIFF) for a given branch and stage,
    indicating inundated areas within specified catchments.

    This function reads a REM (Raster Elevation Model) and catchments raster, applies
    a stage threshold, and masks the result to catchments matching the provided hydroid
    list. The output is a raster where inundated cells are marked as 1 and non-inundated
    cells as 0.

    This is a form of inundation which is specific to CatFIM because we only have one
    flow value and the other FIM inundation tools are looking for flow files not single
    values.

    Parameters
    ----------
    rem_path : str
        Path to the REM raster file.
    catchments_path : str
        Path to the catchments raster file.
    hydroid_list : list of int
        List of hydroid identifiers to include in the mask.
    hand_stage : float or int
        Stage threshold value for inundation.
    lid_directory : str
        Directory where the output raster will be saved.
    category_key : str
        Category key used in the output file name.
    huc : str
        Hydrologic Unit Code for the region.
    lid : str
        Location identifier for the site.
    branch : str
        Branch identifier for the mapping.
    parent_log_output_file : str
        Path to the parent log output file for logging.
    child_log_file_prefix : str
        Prefix for child log files.

    Returns
    -------
    None
        The function saves the output raster to disk and does not return any value.

    Notes
    -----
    - The output raster is only generated if at least one cell is inundated.
    - Logging is set up for error and trace reporting.
    - Handles hydroid values by clipping to the last 4 digits for matching.
    - Both input rasters are expected to have a nodata value of 0.
    - A category can have different formats, depending if it is an interval or not or int or float.
        If it has a stage number it, it is an interval value, ie) action, action_24ft, action_24.6, or action_24.6ft

    '''

    try:
        # This is setting up logging for this function to go up to the parent
        MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)

        file_name = lid + '_' + category_key + '_extent_' + huc + '_' + branch
        output_tif = os.path.join(lid_directory, file_name + '.tif')

        # Open the REM and Catchments rasters (both have a nodata value of 0)
        rem_src = rasterio.open(rem_path)
        catchments_src = rasterio.open(catchments_path)
        rem_array = rem_src.read(1)
        catchments_array = catchments_src.read(1)

        # Use numpy.where operation to reclassify rem_path on the condition that the pixel values
        #   are <= to hand_stage and the catchments value is in the hydroid_list.

        is_alaska = str(huc)[:2] == '19'

        output_dtype = 'uint8' if is_alaska else 'int16'

        reclass_rem_array = np.where((rem_array <= hand_stage) & (rem_array != rem_src.nodata), 1, 0).astype(
            output_dtype
        )

        # The catchment_array has hydroid that have had the first 4 chars cut off
        # we need to the same for the hydroid's from the hydroid_list
        clipped_hydroid_list = []
        for i in hydroid_list:
            clipped_str = str(i) if is_alaska else str(i)[-4:]
            clipped_hydroid_list.append(int(clipped_str))

        # Create a mask of the catchments_array where the values are in the clipped_hydroid_list
        hydroid_mask = np.isin(catchments_array, clipped_hydroid_list)

        # Create a target array where the catchments_array is not nodata and the hydroid_mask is True
        target_catchments_array = np.where(
            ((hydroid_mask == True) & (catchments_array != catchments_src.nodata)), 1, 0
        ).astype(output_dtype)

        # Mask the reclass_rem_array with the target_catchments_array
        masked_reclass_rem_array = np.where(
            ((reclass_rem_array >= 1) & (target_catchments_array >= 1)), 1, 0
        ).astype(output_dtype)

        ## Debugging:

        # Change it all to either 1 or 0 (one being inundated)
        # masked_reclass_rem_array[np.where(masked_reclass_rem_array <= 0)] = 0
        # masked_reclass_rem_array[np.where(masked_reclass_rem_array > 0)] = 1

        # Save resulting array to new tif with appropriate name. ie) brdc1_record_extent_18060005.tif
        # to our mapping/huc/lid site

        # MP_LOG.trace(f"min of reclass_rem_array (min is {np.min(reclass_rem_array)} and max is {np.max(reclass_rem_array)}")
        # MP_LOG.trace(f"max of hydroid_mask {np.max(hydroid_mask)}")
        # MP_LOG.trace(f"min of target_catchments_array (min is {np.min(target_catchments_array)} and max is {np.max(target_catchments_array)}")
        # MP_LOG.trace(f"min of masked_reclass_rem_array (min is {np.min(masked_reclass_rem_array)} and max is {np.max(masked_reclass_rem_array)}")
        # MP_LOG.lprint(f"{huc}: masked_reclass_rem_array, is_all_zero is {is_all_zero} for {rem_path}")

        # Check if no cells were inundated (branches don't inundate as they are out of the extent area)
        is_all_zero = np.all(masked_reclass_rem_array == 0)

        if is_all_zero == False:
            with rasterio.Env():
                profile = rem_src.profile
                profile.update(dtype=rasterio.uint8)
                profile.update(nodata=0)

                # Replace any existing nodata values with the new one
                # masked_reclass_rem_array[masked_reclass_rem_array == profile["nodata"]] = 0

                with rasterio.open(output_tif, 'w', **profile) as dst:
                    dst.write(masked_reclass_rem_array, 1)

    except Exception:
        MP_LOG.error(f"{huc} : {lid} Error producing inundation maps with stage")
        MP_LOG.error(traceback.format_exc())

    return


# This is not part of an MP process, but needs to have FLOG carried over so this file can see it
def run_catfim_inundation(
    fim_run_dir, output_flows_dir, output_mapping_dir, job_number_huc, job_number_inundate, log_output_file
):
    '''
    Only used in flow-based CatFIM.

    Executes the inundation and mosaicking process for CatFIM mapping across multiple HUCs and AHPS sites.

    This function coordinates the parallel execution of inundation mapping tasks using a process pool, handling
    multiple HUCs and AHPS sites. It sets up logging, identifies valid HUC directories, and processes each AHPS site
    and magnitude threshold within those HUCs. For each combination, it submits an inundation mapping job to the
    process pool executor. Errors are logged and handled gracefully, and log files from child processes are merged
    into the parent log file upon completion.

    Args:
        fim_run_dir (str): Path to the directory containing HAND FIM run outputs for HUCs.
        output_flows_dir (str): Path to the directory containing flow outputs for valid HUCs.
        output_mapping_dir (str): Path to the directory where inundation mapping results will be saved.
        job_number_huc (int): Number of parallel jobs to run for HUC-level processing.
        job_number_inundate (int): Number of parallel jobs to run for inundation mapping within each HUC.
        log_output_file (str): Path to the log file for recording process output and errors.

    Returns:
        None

    Raises:
        SystemExit: If a critical error occurs during processing, the function logs the error and exits the program.
    '''

    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)

    print()
    FLOG.lprint(">>> Start Inundating and Mosaicking")

    # The output_flows_dir will only have HUCs that were valid which means
    # it will not include necessarily all hucs from the output run.
    source_flow_huc_dir_list = [
        x
        for x in os.listdir(output_flows_dir)
        if os.path.isdir(os.path.join(output_flows_dir, x)) and x[0] in ['0', '1', '2']
    ]
    fim_source_huc_dir_list = [
        x
        for x in os.listdir(fim_run_dir)
        if os.path.isdir(os.path.join(fim_run_dir, x)) and x[0] in ['0', '1', '2']
    ]
    # Log missing hucs
    # Depending on filtering, errors, and the valid_ahps_huc list at the start of the program
    # this list could have only a few matches
    missing_hucs = list(set(source_flow_huc_dir_list) - set(fim_source_huc_dir_list))

    missing_hucs = [huc for huc in missing_hucs if "." not in huc]
    # Loop through matching huc directories in the source_flow directory
    matching_hucs = list(set(fim_source_huc_dir_list) & set(source_flow_huc_dir_list))
    matching_hucs.sort()

    FLOG.trace(f"matching_hucs now are {matching_hucs}")

    child_log_file_prefix = FLOG.MP_calc_prefix_name(log_output_file, "MP_run_ind")
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        try:
            for huc in matching_hucs:

                # Get list of AHPS site directories
                huc_flows_dir = os.path.join(output_flows_dir, huc)

                # Map path to huc directory inside the mapping directory
                huc_mapping_dir = os.path.join(output_mapping_dir, huc)
                if not os.path.exists(huc_mapping_dir):
                    os.makedirs(huc_mapping_dir, exist_ok=True)

                # ahps_site_dir_list = os.listdir(ahps_site_dir)
                ahps_site_dir_list = [
                    x for x in os.listdir(huc_flows_dir) if os.path.isdir(os.path.join(huc_flows_dir, x))
                ]  # ahps folder names under the huc folder

                FLOG.trace(f"{huc} : ahps_site_dir_list is {ahps_site_dir_list}")

                # Loop through AHPS sites
                for ahps_id in ahps_site_dir_list:
                    # map parent directory for AHPS source data dir and list AHPS thresholds (act, min, mod, maj)
                    ahps_site_parent = os.path.join(huc_flows_dir, ahps_id)

                    # thresholds_dir_list = os.listdir(ahps_site_parent)
                    thresholds_dir_list = [
                        x
                        for x in os.listdir(ahps_site_parent)
                        if os.path.isdir(os.path.join(ahps_site_parent, x))
                    ]

                    # but we can just extract it from the csv files names which are
                    # patterned as: 04130003/chrn6/moderate/chrn6_huc_04130003_flows_moderate.csv

                    # Map parent directory for all inundation output files output files.
                    huc_site_mapping_dir = os.path.join(huc_mapping_dir, ahps_id)
                    if not os.path.exists(huc_site_mapping_dir):
                        os.makedirs(huc_site_mapping_dir, exist_ok=True)

                    # Loop through thresholds/magnitudes and define inundation output files paths

                    FLOG.trace(f"{huc}: {ahps_id} threshold dir list is {thresholds_dir_list}")

                    for magnitude in thresholds_dir_list:
                        if "." in magnitude:
                            continue

                        magnitude_flows_csv = os.path.join(
                            ahps_site_parent,
                            magnitude,
                            ahps_id + '_huc_' + huc + '_flows_' + magnitude + '.csv',
                        )
                        # print(f"magnitude_flows_csv is {magnitude_flows_csv}")
                        tif_name = ahps_id + '_' + magnitude + '_extent.tif'
                        output_extent_tif = os.path.join(huc_site_mapping_dir, tif_name)

                        FLOG.trace(f"Begin inundation for {tif_name}")
                        try:
                            executor.submit(
                                run_inundation,
                                magnitude_flows_csv,
                                huc,
                                huc_site_mapping_dir,
                                output_extent_tif,
                                ahps_id,
                                magnitude,
                                fim_run_dir,
                                job_number_inundate,
                                log_output_file,
                                child_log_file_prefix,
                            )

                        except Exception:
                            FLOG.critical(
                                "A critical error occured while attempting inundation"
                                f" for {huc} - {ahps_id}-- {magnitude}"
                            )
                            FLOG.critical(traceback.format_exc())
                            FLOG.merge_log_files(log_output_file, child_log_file_prefix)
                            sys.exit(1)

        except Exception:
            FLOG.critical("A critical error occured while attempting all hucs inundation")
            FLOG.critical(traceback.format_exc())
            FLOG.merge_log_files(log_output_file, child_log_file_prefix)
            sys.exit(1)

    # end of ProcessPoolExecutor

    # rolls up logs from child MP processes into this parent_log_output_file

    # hold on merging it up for now, to keep the overall log size down a little
    FLOG.merge_log_files(log_output_file, child_log_file_prefix, True)

    print()
    FLOG.lprint(">>> End Inundating and Mosaicking")

    return


# This is part of an MP Pool
def run_inundation(
    magnitude_flows_csv,
    huc,
    output_huc_site_mapping_dir,
    output_extent_tif,
    ahps_site,
    magnitude,
    fim_run_dir,
    job_number_inundate,
    parent_log_output_file,
    child_log_file_prefix,
):
    '''
    Only used in flow-based CatFIM.

    Runs the inundation mapping workflow for a given HUC and magnitude, including logging,
    inundation raster generation, mosaicking, and lake masking.

    Inundates each set based on the ahps/mangnitude list and for each segment in the the branch hydrotable.
    Then each set is inundated per branch and mosiaked for the AHPS site.

    Parameters:
        magnitude_flows_csv (str): Path to the CSV file containing magnitude flows for the forecast.
        huc (str): Hydrologic Unit Code (HUC) identifier for the region to process.
        output_huc_site_mapping_dir (str): Directory to store output HUC-site mapping files.
        output_extent_tif (str): Path to the output inundation extent raster (GeoTIFF).
        ahps_site (str): AHPS site identifier associated with the forecast.
        magnitude (str): Magnitude value for the forecast (e.g., flood stage).
        fim_run_dir (str): Directory containing FIM run data and hydrofabric.
        job_number_inundate (int): Number of worker processes to use for inundation generation.
        parent_log_output_file (str): Path to the parent log file for logging output.
        child_log_file_prefix (str): Prefix for child log files created by this function.

    Returns:
        None

    Side Effects:
        - Generates inundation extent raster and saves to output_extent_tif.
        - Logs progress and errors to the specified log files.
        - Masks lakes from the inundation raster.
        - Performs mosaicking of inundation rasters.
        - Returns early if errors occur or output raster is not created.

    Exceptions:
        Logs any exceptions encountered during processing and returns None.
    '''

    # Note: child_log_file_prefix is "MP_run_ind", meaning all logs created by this function start
    #  with the phrase "MP_run_ind"
    #  They will be rolled up into the parent_log_output_file
    # This is setting up logging for this function to go up to the parent
    MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)
    # MP_LOG.trace(locals())

    huc_dir = os.path.join(fim_run_dir, huc)
    # Why all high number for job_number_inundate? Inundate_gms has to create inundation for each
    # branch and merge them.
    try:
        MP_LOG.lprint(f"... Running Inundate_gms and mosiacking for {huc} : {ahps_site} : {magnitude}")
        map_file = Inundate_gms(
            hydrofabric_dir=fim_run_dir,
            forecast=magnitude_flows_csv,
            num_workers=job_number_inundate,
            hydro_table_df=None,
            hucs=huc,
            inundation_raster=output_extent_tif,
            depths_raster=None,
            verbose=False,
            log_file=None,
            output_fileNames=None,
            multi_process=True,
        )

        MP_LOG.trace(f"Mosaicking for {huc} : {ahps_site} : {magnitude}")
        Mosaic_inundation(
            map_file,
            mosaic_attribute='inundation_rasters',
            mosaic_output=output_extent_tif,
            mask=os.path.join(huc_dir, 'wbd.gpkg'),
            unit_attribute_name='huc8',
            nodata=-9999,
            workers=1,
            remove_inputs=False,
            subset=None,
            verbose=False,
        )

        MP_LOG.trace(f"Mosaicking complete for {huc} : {ahps_site} : {magnitude}")

        # Mask out lakes from inundated tif and re-save tif
        # TODO: Update to only run if lake detected?
        with rasterio.open(output_extent_tif, 'r+') as output_extent_src:
            output_extent_array = output_extent_src.read(1)
            output_extent_array_masked, mask_status = mask_out_lakes(
                output_extent_array, huc, output_extent_src, fim_run_dir
            )
            output_extent_src.write(output_extent_array_masked, 1)

        MP_LOG.trace(f"Lake masking complete for {huc} : {ahps_site} : {magnitude}")

    except Exception:
        # Log errors and their tracebacks
        MP_LOG.error(f"Exception: running inundation for {huc}")
        MP_LOG.error(traceback.format_exc())
        return

    # Inundation.py appends the huc code to the supplied output_extent_grid for stage-based.
    # Modify output_extent_grid to match inundation.py saved filename.
    # Search for this file, if it didn't create, send message to log file.

    # base_file_path, extension = os.path.splitext(output_extent_tif)
    # saved_extent_grid_filename = "{}_{}{}".format(base_file_path, huc, extension)

    # MP_LOG.trace(f"saved_extent_grid_filename is {saved_extent_grid_filename}")

    if not os.path.exists(output_extent_tif):
        MP_LOG.error(f"FAILURE_huc_{huc} - {ahps_site} - {magnitude} map failed to create")
        return

    return


# This is part of an MP Pool
# TODO: Aug 2024: job_number_inundate is not used well at all and is partially
# with more cleanup to do later. Partially removed now.
def post_process_huc(
    output_catfim_dir,
    ahps_dir_list,
    huc_dir,
    gpkg_dir,
    huc,
    parent_log_output_file,
    child_log_file_prefix,
    progress_stmt,
):
    '''
    Used in both flow-based and stage-based CatFIM.

    Post-processes inundation mapping results for a given HUC.

    This function performs post-processing tasks for a specified HUC, including:
    - Setting up logging for the process.
    - Iterating through AHPS site directories.
    - Identifying and processing rolled-up extent TIFF files for each AHPS site.
    - Checking for the existence of corresponding attributes CSV files.
    - Reformatting inundation maps using site-specific attributes.
    - Handling interval and non-interval stage-based TIFF files.
    - Logging warnings and errors as appropriate.

    Args:
        output_catfim_dir (str): Directory containing CatFIM output files.
        ahps_dir_list (list): List of AHPS site identifiers to process.
        huc_dir (str): Directory for the current HUC's data.
        gpkg_dir (str): Directory to store GeoPackage outputs.
        huc (str): Hydrologic Unit Code being processed.
        parent_log_output_file (str): Path to the parent log file for logging output.
        child_log_file_prefix (str): Prefix for child log files created by this function.
        progress_stmt (str): Statement describing the current progress for logging.

    Returns:
        None

    Raises:
        Exception: Logs and handles any exceptions that occur during processing.
    '''

    # Note: child_log_file_prefix is "MP_post_process_{huc}", meaning all logs created by this function start
    #  with the phrase "MP_post_process_{huc}". This one rollups up to the master catfim log
    # This is setting up logging for this function to go up to the parent
    try:
        MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)
        MP_LOG.lprint(f'Post Processing {huc} ...')
        MP_LOG.lprint(f'... {progress_stmt} ...')

        # Loop through ahps sites
        attributes_dir = os.path.join(output_catfim_dir, 'attributes')

        for ahps_lid in ahps_dir_list:
            tifs_to_reformat_list = []
            mapping_huc_lid_dir = os.path.join(huc_dir, ahps_lid)
            MP_LOG.trace(f"mapping_huc_lid_dir is {mapping_huc_lid_dir}")

            # aka. ends with "extent.tif" which means it is a rolled up version up for there branches
            tif_list = [x for x in os.listdir(mapping_huc_lid_dir) if ('extent.tif') in x]

            if len(tif_list) == 0:
                # This is perfectly fine for there to be none
                # MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {mapping_huc_lid_dir}")
                continue

            for tif in tif_list:
                tifs_to_reformat_list.append(os.path.join(mapping_huc_lid_dir, tif))

            if len(tifs_to_reformat_list) == 0:
                # MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {mapping_huc_lid_dir}")
                continue

            # Stage-Based CatFIM uses attributes from individual CSVs instead of the master CSV.
            nws_lid_attributes_filename = os.path.join(attributes_dir, ahps_lid + '_attributes.csv')

            # There may not necessarily be an attributes.csv for this lid, depending on how flow processing went
            # lots of lids fall out in the attributes or flow steps.
            if os.path.exists(nws_lid_attributes_filename) == False:
                MP_LOG.warning(f"{ahps_lid} has no attributes file (which may be perfectly fine)")
                continue

            # We are going to do an MP in MP.
            # child_log_file_prefix = MP_LOG.MP_calc_prefix_name(
            #    parent_log_output_file, "MP_reformat_tifs", huc
            # )
            # Weird case, we ahve to delete any of these files that might already exist (MP in MP)
            # Get parent log dir
            # log_dir = os.path.dirname(parent_log_output_file)
            # old_refomat_log_files = glob.glob(os.path.join(log_dir, 'MP_reformat_tifs_*'))
            # for log_file in old_refomat_log_files:
            #     os.remove(log_file)

            # we only have the rolled up, no branch versions by now
            for tif_to_process in tifs_to_reformat_list:
                # If not os.path.exists(tif_to_process):
                #    continue

                # If stage based, the file names looks like this:
                #      masm1_major_extent.tif  (non-interval, whole number)
                #      masm1_major_20.6_extent.tif  (non-interval, float)
                #      masm1_major_20.0ft_extent.tif (interval)
                # If flow based, the file name looks like this: masm1_action_extent.tif
                # MP_LOG.trace(f".. Tif to Process = {tif_to_process}")
                try:

                    tif_file_name = os.path.basename(tif_to_process)
                    file_name_parts = tif_file_name.split("_")
                    magnitude = file_name_parts[1]  # part 0 is the lid

                    # but if it doesn't have "fti" at the end it is not an interval

                    # careful. ft can be part of the site name, so only check part 3
                    interval_stage = None
                    is_interval = False
                    if len(file_name_parts) >= 3 and "fti" in file_name_parts[2]:
                        try:
                            stage_val = file_name_parts[2].replace("fti", "")
                            interval_stage = float(stage_val)
                            is_interval = True
                        except ValueError:
                            interval_stage = None
                            MP_LOG.error(
                                f"Value Error for {huc} - {ahps_lid} - magnitude {magnitude}"
                                f" at {mapping_huc_lid_dir}"
                            )
                            MP_LOG.error(traceback.format_exc())

                    reformat_inundation_maps(
                        ahps_lid,
                        tif_to_process,
                        gpkg_dir,
                        huc,
                        magnitude,
                        nws_lid_attributes_filename,
                        interval_stage,
                        is_interval,
                        parent_log_output_file,
                        child_log_file_prefix,
                    )
                except Exception:
                    MP_LOG.error(
                        f"An ind reformat map error occured for {huc} - {ahps_lid} - magnitude {magnitude}"
                    )
                    MP_LOG.error(traceback.format_exc())

            # rolls up logs from child MP processes into this parent_log_output_file
            # MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix, True)

        # TODO:  Roll up the independent related ahps gpkgs into a huc level gkpg, still in the gpkg dir
        # all of the gkpgs we want will have the huc number in front of it

    except Exception:
        MP_LOG.error(f"An error has occurred in post processing for {huc}")
        MP_LOG.error(traceback.format_exc())

    return


# This is not part of an MP process, but does need FLOG carried into it so it can use FLOG directly
def post_process_cat_fim_for_viz(
    catfim_method, output_catfim_dir, job_huc_ahps, catfim_version, model_version, log_output_file
):
    '''
    Used in both flow-based and stage-based CatFIM.

    Post-processes CatFIM outputs for visualization.

    This function performs the following steps:
    1. Sets up logging and prepares output directories.
    2. Identifies HUC/AHPS directories to process.
    3. Uses a process pool to post-process each HUC directory in parallel, converting TIF extents to polygons and saving as GeoPackage files.
    4. Merges all generated GeoPackage layers into a single GeoDataFrame.
    5. Optionally dissolves merged layers by AHPS and magnitude for flow-based methods.
    6. Cleans up unnecessary columns from the merged data.
    7. Adds model and product version metadata.
    8. Saves the final merged layer as both GeoPackage and CSV files for visualization.
    9. Rolls up logs from child processes into the main log file.

    Args:
        catfim_method (str): The method used for Categorical FIM (e.g., "flow_based").
        output_catfim_dir (str): Directory where Categorical FIM outputs are stored.
        job_huc_ahps (int): Number of parallel jobs for processing HUC/AHPS directories.
        catfim_version (str): Version identifier for the Categorical FIM product.
        model_version (str): Version identifier for the underlying model.
        log_output_file (str): Path to the log file for recording process output.

    Raises:
        Exception: If no HUC/AHPS directories are found or if no GeoPackage files are generated.

    Returns:
        None
    '''

    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)

    FLOG.lprint("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    FLOG.lprint("Start post processing TIFs (TIF extents into poly into gpkg)...")
    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    gpkg_dir = os.path.join(output_mapping_dir, 'gpkg')
    os.makedirs(gpkg_dir, exist_ok=True)

    huc_ahps_dir_list = [
        x
        for x in os.listdir(output_mapping_dir)
        if os.path.isdir(os.path.join(output_mapping_dir, x)) and x[0] in ['0', '1', '2', '9']
    ]

    # if we don't have a huc_ahps_dir_list, something went catestrophically bad
    if len(huc_ahps_dir_list) == 0:
        raise Exception("Critical Error: Not possible to be here with no huc/ahps list")

    num_hucs = len(huc_ahps_dir_list)
    huc_index = 0
    FLOG.lprint(f"Number of hucs to post process is {num_hucs}")

    # TODO: Sep 2024: we need to remove the process pool here and put it post_process_huc (for tif inundation)

    child_log_file_prefix = MP_LOG.MP_calc_prefix_name(log_output_file, "MP_post_process")
    with ProcessPoolExecutor(max_workers=job_huc_ahps) as huc_exector:
        for huc in huc_ahps_dir_list:
            FLOG.lprint(f"TIF post processing for {huc}")

            huc_dir = os.path.join(output_mapping_dir, huc)
            progress_stmt = f"index {huc_index + 1} of {num_hucs}"
            huc_index += 1

            try:
                ahps_dir_list = [x for x in os.listdir(huc_dir) if os.path.isdir(os.path.join(huc_dir, x))]
                # ahps_dir_list = os.listdir(huc_dir)
            except NotADirectoryError:
                FLOG.warning(f"{huc_dir} directory missing. Continuing on")
                continue

            # If there's no mapping for a HUC, delete the HUC directory.
            if len(ahps_dir_list) == 0:
                os.rmdir(huc_dir)
                FLOG.warning(f"no mapping for {huc}")
                continue

            huc_exector.submit(
                post_process_huc,
                output_catfim_dir,
                ahps_dir_list,
                huc_dir,
                gpkg_dir,
                huc,
                log_output_file,
                child_log_file_prefix,
                progress_stmt,
            )

    # end of ProcessPoolExecutor

    # rolls up logs from child MP processes into this parent_log_output_file
    FLOG.merge_log_files(FLOG.LOG_FILE_PATH, child_log_file_prefix, True)

    # Merge all layers
    gpkg_files = [x for x in os.listdir(gpkg_dir) if x.endswith('.gpkg')]
    FLOG.lprint(f"Merging {len(gpkg_files)} from layers in {gpkg_dir}")

    gpkg_files.sort()

    merged_layers_gdf = None
    ctr = 0
    num_gpkg_files = len(gpkg_files)
    for gpkg_file in gpkg_files:

        # for ctr, layer in enumerate(gpkg_files):
        # FLOG.lprint(f"Merging gpkg ({ctr+1} of {len(gpkg_files)} - {}")
        FLOG.trace(f"Merging gpkg ({ctr+1} of {num_gpkg_files} : {gpkg_file}")

        # Concatenate each /gpkg/{huc}_{aphs}_{magnitude}_extent.gpkg
        diss_extent_filename = os.path.join(gpkg_dir, gpkg_file)
        diss_extent_gdf = gpd.read_file(diss_extent_filename, engine='fiona')

        if 'interval_stage' in diss_extent_gdf.columns:
            # Update the stage column value to be the interval value if an interval values exists

            diss_extent_gdf.loc[diss_extent_gdf["interval_stage"] > 0, "stage"] = diss_extent_gdf[
                "interval_stage"
            ]

        if ctr == 0:
            merged_layers_gdf = diss_extent_gdf
        else:
            merged_layers_gdf = pd.concat([merged_layers_gdf, diss_extent_gdf])

        del diss_extent_gdf
        ctr += 1

    if merged_layers_gdf is None or len(merged_layers_gdf) == 0:
        raise Exception(f"No gpkgs found in {gpkg_dir}")

    # TODO: July 9, 2024: Consider deleting all of the interium .gpkg files in the gpkg folder.
    # It will get very big quick. But not yet.
    # shutil.rmtree(gpkg_dir)

    # Now dissolve based on ahps and magnitude (we no longer saved non dissolved versrons)
    # Aug 2024: We guessed on what might need to be dissolved from 4.4.0.0. In 4.4.0.0 there
    # are "_dissolved" versions of catfim files but no notes on why or how, but this script
    # did not do it. We are going to guess on what the dissolving rules are.
    if catfim_method == "flow_based":
        FLOG.lprint("Dissolving flow based catfim_libary by ahps and magnitudes")
        merged_layers_gdf = merged_layers_gdf.dissolve(by=['ahps_lid', 'magnitude'], as_index=False)

    if 'level_0' in merged_layers_gdf:
        merged_layers_gdf = merged_layers_gdf.drop(['level_0'], axis=1)

    if 'status' in merged_layers_gdf:
        merged_layers_gdf = merged_layers_gdf.drop(['status'], axis=1)

    if 'mapped' in merged_layers_gdf:
        merged_layers_gdf = merged_layers_gdf.drop(['mapped'], axis=1)

    output_file_name = f"{catfim_method}_catfim_library"

    merged_layers_gdf["model_version"] = model_version
    merged_layers_gdf["product_version"] = catfim_version

    gpkg_file_path = os.path.join(output_mapping_dir, f'{output_file_name}.gpkg')
    FLOG.lprint(f"Saving catfim library gpkg version to {gpkg_file_path}")
    merged_layers_gdf.to_file(gpkg_file_path, driver='GPKG', engine="fiona")

    csv_file_path = os.path.join(output_mapping_dir, f'{output_file_name}.csv')
    FLOG.lprint(f"Saving catfim library csv version to {csv_file_path}")
    merged_layers_gdf.to_csv(csv_file_path)

    FLOG.lprint("End post processing TIFs...")

    return


# This is part of an MP pool
def reformat_inundation_maps(
    ahps_lid,
    tif_to_process,
    gpkg_dir,
    huc,
    magnitude,
    nws_lid_attributes_filename,
    interval_stage,
    is_interval,
    parent_log_output_file,
    child_log_file_prefix,
):
    '''
    Used in both flow- and stage-based CatFIM.

    Converts an inundation raster (GeoTIFF) to a dissolved polygon GeoPackage with enriched attributes.

    This function reads an inundation raster file, extracts inundated areas as polygons, dissolves them into a single multipolygon,
    and joins additional attributes from a CSV file. The resulting GeoDataFrame is projected to Web Mercator and saved as a GeoPackage.
    Logging is performed throughout the process, and special handling is included for interval stages and empty rasters.

    Parameters
    ----------
    ahps_lid : str
        The AHPS location identifier for the site.
    tif_to_process : str
        Path to the inundation raster (GeoTIFF) file to process.
    gpkg_dir : str
        Directory where the output GeoPackage file will be saved.
    huc : str
        Hydrologic Unit Code (HUC) for the watershed.
    magnitude : str or int
        Magnitude or recurrence interval for the inundation scenario.
    nws_lid_attributes_filename : str
        Path to the CSV file containing NWS LID attributes for joining.
    interval_stage : float or None
        Stage value for interval scenarios; may be None.
    is_interval : bool
        Flag indicating whether the scenario is an interval.
    parent_log_output_file : str
        Path to the parent log file for logging output.
    child_log_file_prefix : str
        Prefix for child log files created by this function.

    Returns
    -------
    None
        The function saves the output GeoPackage file and logs messages, but does not return a value.

    Raises
    ------
    ValueError
        If assigning CRS to a GeoDataFrame without a geometry column.
    Exception
        For any other errors encountered during processing.

    Notes
    -----
    - If no inundated areas are found in the raster, a log message is written and the function returns early.
    - The output GeoPackage contains dissolved polygons with joined attributes and is projected to Web Mercator.
    - For interval scenarios, the 'stage_uncorrected' attribute is set to None to avoid confusion.
    - The interval stage might come in as null and that is ok

    '''

    # Note: child_log_file_prefix is "MP_reformat_tifs_{huc}", meaning all logs created by this
    # function start with the phrase will rollup to the master catfim logs

    # This is setting up logging for this function to go up to the parent
    MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)

    try:
        MP_LOG.trace(
            f"{huc} : {ahps_lid} : {magnitude} -- Start reformat_inundation_maps" " (tif extent to gpkg poly)"
        )
        # MP_LOG.trace(F"tif to process is {tif_to_process}")

        # Convert raster to shapes
        with rasterio.open(tif_to_process) as src:
            image = src.read(1)
            mask = image > 0

        # Aggregate shapes
        results = (
            {'properties': {'extent': 1}, 'geometry': s}
            for i, (s, v) in enumerate(shapes(image, mask=mask, transform=src.transform))
        )

        list_results = list(results)

        # Check whether any shapes were found in the inundated tifs
        # If not, log a message and return
        if len(list_results) == 0:
            MP_LOG.error(
                f"{huc} : {ahps_lid} : {magnitude} - No values above zero in inundated tif, "
                "so zero inundated shapes were found. See GitHub issue #1491 for details."
            )
            return

        # Convert list of shapes to polygon
        # lots of polys
        extent_poly = gpd.GeoDataFrame.from_features(list_results, crs=src.crs)

        # Dissolve polygons
        extent_poly_diss = extent_poly.dissolve(by='extent')

        # Update attributes
        extent_poly_diss = extent_poly_diss.reset_index(drop=True)
        extent_poly_diss['ahps_lid'] = ahps_lid
        extent_poly_diss['magnitude'] = magnitude
        extent_poly_diss['huc'] = huc
        extent_poly_diss['interval_stage'] = interval_stage
        extent_poly_diss['is_interval'] = is_interval

        # Project to Web Mercator
        extent_poly_diss = extent_poly_diss.to_crs(VIZ_PROJECTION)

        # Join attributes
        nws_lid_attributes_table = pd.read_csv(nws_lid_attributes_filename, dtype={'huc': str})
        nws_lid_attributes_table = nws_lid_attributes_table.loc[
            (nws_lid_attributes_table.magnitude == magnitude) & (nws_lid_attributes_table.nws_lid == ahps_lid)
        ]
        extent_poly_diss = extent_poly_diss.merge(
            nws_lid_attributes_table,
            left_on=['ahps_lid', 'magnitude', 'huc'],
            right_on=['nws_lid', 'magnitude', 'huc'],
        )
        # already has an ahps_lid column which we want and not the nws_lid column
        extent_poly_diss = extent_poly_diss.drop(columns='nws_lid')

        # Remove uncorrected stage from interval rows (to decrease potential for confusion)
        extent_poly_diss.loc[extent_poly_diss['is_interval'] == True, 'stage_uncorrected'] = None

        # Save dissolved multipolygon
        handle = os.path.split(tif_to_process)[1].replace('.tif', '')
        diss_extent_filename = os.path.join(gpkg_dir, f"{huc}_{handle}.gpkg")
        extent_poly_diss["geometry"] = [
            MultiPolygon([feature]) if type(feature) is Polygon else feature
            for feature in extent_poly_diss["geometry"]
        ]

        if not extent_poly_diss.empty:
            extent_poly_diss.to_file(
                diss_extent_filename, driver=getDriver(diss_extent_filename), index=False, engine='fiona'
            )
            # MP_LOG.trace(
            #    f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map saved"
            #    f" as {diss_extent_filename}"
            # )
        else:
            MP_LOG.error(f"{huc} : {ahps_lid} : {magnitude} tif to gpkg, geodataframe is empty")

    except ValueError as ve:
        msg = f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map"
        if "Assigning CRS to a GeoDataFrame without a geometry column is not supported" in ve:
            MP_LOG.warning(f"{msg} - Warning: details: {ve}")
        else:
            MP_LOG.error(f"{msg} - Exception")
            MP_LOG.error(traceback.format_exc())

    except Exception:
        MP_LOG.error(f"{huc} : {ahps_lid} : {magnitude} - Reformatted inundation map - Exception")
        MP_LOG.error(traceback.format_exc())

    return


# This is not part of an MP progress and simply needs the
# pointer of FLOG carried over here so it can use it directly.


# TODO: Aug, 2024. We need re-evaluate job numbers, see usage of job numbers below
def manage_catfim_mapping(
    fim_run_dir,
    output_flows_dir,
    output_catfim_dir,
    catfim_method,
    catfim_version,
    model_version,
    job_number_huc,
    job_number_inundate,
    log_output_file,
    step_number=1,
):
    '''
    Only used in flow-based CatFIM.

    Manages the workflow for generating categorical FIM (Flood Inundation Mapping) outputs,
    including running inundation mapping and post-processing for visualization.

    Parameters:
        fim_run_dir (str): Directory containing the FIM run data.
        output_flows_dir (str): Directory where flow outputs are stored.
        output_catfim_dir (str): Directory for storing categorical FIM outputs.
        catfim_method (str): Method used for categorical FIM generation.
        catfim_version (str): Version identifier for the categorical FIM process.
        model_version (str): Version identifier for the underlying model.
        job_number_huc (int): Number of jobs for HUC (Hydrologic Unit Code) processing.
        job_number_inundate (int): Number of jobs for inundation processing.
        log_output_file (str): Path to the log file for output messages.
        step_number (int, optional): Workflow step to execute. Defaults to 1.

    Returns:
        None

    Notes:
        - Initializes logging and manages workflow steps.
        - Runs inundation mapping if step_number <= 1.
        - Performs post-processing for visualization using multiple jobs.
        - Logs the elapsed time for the mapping process.
    '''

    # Adding a pointer in this file coming from generate_categorial_fim so they can share the same log file
    FLOG.setup(log_output_file)

    FLOG.lprint('Begin mapping')
    start = time.time()

    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    if not os.path.exists(output_mapping_dir):
        os.mkdir(output_mapping_dir)

    if step_number <= 1:
        run_catfim_inundation(
            fim_run_dir,
            output_flows_dir,
            output_mapping_dir,
            job_number_huc,
            job_number_inundate,
            FLOG.LOG_FILE_PATH,
        )
    else:
        FLOG.lprint("Skip running Inundation as Step > 1")

    # FLOG.lprint("Aggregating Categorical FIM")
    # Step 2
    # TODO: Aug 2024, so we need to clean it up
    # This step does not need a job_number_inundate as it can't really use it.
    # It processes primarily hucs and ahps in multiproc
    # for now, we will manually multiple the huc * 5 (max number of ahps types)
    ahps_jobs = job_number_huc * 5
    post_process_cat_fim_for_viz(
        catfim_method, output_catfim_dir, ahps_jobs, catfim_version, model_version, str(FLOG.LOG_FILE_PATH)
    )

    end = time.time()
    elapsed_time = (end - start) / 60
    FLOG.lprint(f"Finished mapping in {str(elapsed_time).split('.')[0]} minutes")

    return


if __name__ == '__main__':

    # TODO: Repair this as it can still be run from command line

    '''
    Sample
    python /foss_fim/tools/catfim/generate_categorical_fim_mapping.py -u 12090301 -t /data/catfim/hand_4_8_7_2
    '''

    # All files shoudl be in place such as the sites_gdf, threshold data files
    # and begining library data for this huc

    # Files required are:  __________________________ (list them here)
    #     {huc}_sites.gdf
    #     flow_discharge.csv  (if flow based)
    #     {huc}_library_threshold.csv  (TODO: seems like a bad file name)
    #     others?

    # Parse arguments
    parser = argparse.ArgumentParser(description='Categorical inundation mapping for FOSS FIM.')

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

    # # Parse arguments
    # parser = argparse.ArgumentParser(description='Categorical inundation mapping for FOSS FIM.')
    # parser.add_argument(
    #     '-r', '--fim-run-dir', help='Name of directory containing outputs of fim_run.sh', required=True
    # )
    # parser.add_argument(
    #     '-s',
    #     '--source-flow-dir',
    #     help='Path to directory containing flow CSVs to use to generate categorical FIM.',
    #     required=True,
    #     default="",
    # )
    # parser.add_argument(
    #     '-o',
    #     '--output-catfim-dir',
    #     help='Path to directory where categorical FIM outputs will be written.',
    #     required=True,
    #     default="",
    # )
    # parser.add_argument(
    #     '-jh',
    #     '--job-number-huc',
    #     help='Number of processes to use for huc processing. Default is 1.',
    #     required=False,
    #     default="1",
    #     type=int,
    # )
    # parser.add_argument(
    #     '-jn',
    #     '--job-number-inundate',
    #     help='OPTIONAL: Number of processes to use for inundating'
    #     ' HUC and inundation job numbers should multiply to no more than one less than the CPU count'
    #     ' of the machine. Defaults to 1.',
    #     required=False,
    #     default=1,
    #     type=int,
    # )

    # parser.add_argument(
    #     '-step',
    #     '--step_number',
    #     help='Using this option will write depth TIFFs.',
    #     required=False,
    #     default=1,
    #     type=int,
    # )

    # args = vars(parser.parse_args())

    # fim_run_dir = args['fim_run_dir']
    # source_flow_dir = args['source_flow_dir']
    # output_catfim_dir = args['output_catfim_dir']
    # job_number_huc = int(args['job_number_huc'])
    # job_number_inundate = int(args['job_number_inundate'])
    # step_num = args['step_number']

    # log_dir = os.path.join(output_catfim_dir, "logs")
    # log_output_file = FLOG.calc_log_name_and_path(log_dir, "gen_cat_mapping")

    # manage_catfim_mapping(
    #     source_flow_dir, output_catfim_dir, job_number_huc, job_number_inundate, log_output_file, step_num
    # )
