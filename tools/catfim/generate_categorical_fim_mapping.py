#!/usr/bin/env python3

import argparse
import glob
import os

# import shutil
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
from rasterio.warp import Resampling, calculate_default_transform, reproject
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from tqdm import tqdm

import utils.fim_logger as fl
from utils.shared_functions import getDriver
from utils.shared_variables import ALASKA_CRS, PREP_PROJECTION, VIZ_PROJECTION


# TODO: Aug 2024: This script was upgraded significantly with lots of misc TODO's embedded.
# Lots of inline documenation needs updating as well


# will become global once initiallized
FLOG = fl.FIM_logger()
MP_LOG = fl.FIM_logger()

gpd.options.io_engine = "pyogrio"


# Technically, this is once called as a non MP, but also called in an MP pool
# we will use an MP object either way
def produce_stage_based_catfim_tifs(
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
    number_of_jobs,
    parent_log_output_file,
    child_log_file_prefix,
):

    MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)

    messages = []

    huc_lid_cat_id = f"{huc} : {lid} : {category}"
    MP_LOG.trace(f"{huc_lid_cat_id}: Starting to create tifs")

    # Determine datum-offset water surface elevation (from above).
    datum_adj_wse = stage_val + datum_adj_ft + lid_altitude
    datum_adj_wse_m = datum_adj_wse * 0.3048  # Convert ft to m

    # Subtract HAND gage elevation from HAND WSE to get HAND stage.
    hand_stage = datum_adj_wse_m - lid_usgs_elev

    # TODO: see what happens if this is returned with tj

    # If no segments, write message and exit out
    if not segments or len(segments) == 0:
        msg = ':missing nwm segments'
        messages.append(lid + msg)
        MP_LOG.warning(huc_lid_cat_id + msg)
        return messages, hand_stage, datum_adj_wse, datum_adj_wse_m

    # Produce extent tif hand_stage. Multiprocess across branches.
    # branches = os.listdir(branch_dir)
    # MP_LOG.lprint(f"{huc_lid_cat_id} branch_dir is {branch_dir}")

    branches = [x for x in os.listdir(branch_dir) if os.path.isdir(os.path.join(branch_dir, x))]
    branches.sort()

    # We need to merge what we have up to this point.
    # MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix)
    # child_log_file_prefix = MP_LOG.MP_calc_prefix_name(parent_log_output_file, "MP_prod_huc_mag_stage", huc)
    child_log_file_prefix = MP_LOG.MP_calc_prefix_name(parent_log_output_file, "MP_prod_huc_mag_stage")
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        for branch in branches:
            msg_id_w_branch = f"{huc} - {branch} - {lid} - {category}"
            # MP_LOG.trace(f"{msg_id_w_branch} : inundating branches")
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

            if not os.path.exists(rem_path):
                msg = ":rem doesn't exist"
                messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                continue
            if not os.path.exists(catchments_path):
                msg = ":catchments files don't exist"
                messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                continue
            if not os.path.exists(hydrotable_path):
                msg = ":hydrotable doesn't exist"
                messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                continue

            # Use hydroTable to determine hydroid_list from site_ms_segments.
            hydrotable_df = pd.read_csv(
                hydrotable_path, low_memory=False, dtype={'HUC': str, 'LakeID': float, 'subdiv_applied': int}
            )
            hydroid_list = []

            # Determine hydroids at which to perform inundation
            for feature_id in segments:
                # print(f"... feature id is {feature_id}")
                try:
                    subset_hydrotable_df = hydrotable_df[hydrotable_df['feature_id'] == int(feature_id)]
                    hydroid_list += list(subset_hydrotable_df.HydroID.unique())
                except IndexError:
                    MP_LOG.trace(
                        f"Index Error for {huc} -- {branch} -- {category}. FeatureId is {feature_id} : Continuing on."
                    )
                    pass

            # Create inundation maps with branch and stage data
            # only sites /categories that got this far are valid and can be inundated
            try:
                MP_LOG.trace(f"{huc_lid_cat_id} : branch = {branch} :  Generating stage-based FIM")

                executor.submit(
                    produce_tif_per_huc_per_mag_for_stage,
                    rem_path,
                    catchments_path,
                    hydroid_list,
                    hand_stage,
                    lid_directory,
                    category,
                    huc,
                    lid,
                    branch,
                    parent_log_output_file,
                    child_log_file_prefix,
                )

            except Exception:
                msg = f': inundation failed at {category}'
                messages.append(lid + msg)
                MP_LOG.warning(msg_id_w_branch + msg)
                MP_LOG.error(traceback.format_exc())

    MP_LOG.merge_log_files(parent_log_output_file, child_log_file_prefix, True)

    # -- MOSAIC -- #
    # Merge all rasters in lid_directory that have the same magnitude/category.
    path_list = []

    MP_LOG.lprint(f"Merging files from {lid_directory}")
    lid_dir_list = os.listdir(lid_directory)

    MP_LOG.lprint(f"{huc}: {lid} : Merging {category}")
    # MP_LOG.trace("lid_dir_list is ")
    # MP_LOG.trace(lid_dir_list)
    # MP_LOG.lprint("")

    for f in lid_dir_list:
        if category in f:
            path_list.append(os.path.join(lid_directory, f))

    path_list.sort()  # To force branch 0 first in list, sort

    # MP_LOG.trace(f"len of path_list is {len(path_list)}")

    if len(path_list) > 0:
        zero_branch_grid = path_list[0]
        zero_branch_src = rasterio.open(zero_branch_grid)
        zero_branch_array = zero_branch_src.read(1)
        # zero_branch_array.nodata = 0
        summed_array = zero_branch_array  # Initialize it as the branch zero array

        output_tif = os.path.join(lid_directory, lid + '_' + category + '_extent.tif')
        # MP_LOG.trace(f"Output file to be saved is {output_tif}")

        # Loop through remaining items in list and sum them with summed_array
        for remaining_raster in path_list[1:]:

            remaining_raster_src = rasterio.open(remaining_raster)
            remaining_raster_array_original = remaining_raster_src.read(1)

            # Reproject non-branch-zero grids so I can sum them with the branch zero grid
            remaining_raster_array = np.empty(zero_branch_array.shape, dtype=np.int8)
            reproject(
                remaining_raster_array_original,
                destination=remaining_raster_array,
                src_transform=remaining_raster_src.transform,
                src_crs=remaining_raster_src.crs,
                src_nodata=remaining_raster_src.nodata,
                dst_transform=zero_branch_src.transform,
                dst_crs=zero_branch_src.crs,  # TODO: Accomodate AK projection?
                # dst_nodata=-1,
                dst_nodata=0,
                dst_resolution=zero_branch_src.res,
                resampling=Resampling.nearest,
            )
            # Sum rasters
            summed_array = summed_array + remaining_raster_array

        del zero_branch_array  # Clean up

        # Define path to merged file, in same format as expected by post_process_cat_fim_for_viz function

        profile = zero_branch_src.profile
        summed_array = summed_array.astype('uint8')
        with rasterio.open(output_tif, 'w', **profile) as dst:
            dst.write(summed_array, 1)
            MP_LOG.lprint(f"{output_tif} - saved")
        del summed_array
    else:
        MP_LOG.warning(f"{huc}: {lid}: Merging {category} : no valid inundated branches")

    return messages, hand_stage, datum_adj_wse, datum_adj_wse_m


# This is part of an MP call and needs MP_LOG


# This is a form of inundation which we are doing ourselves
# as we only have one flow value and our normal inundation tools
# are looking for files not single values
def produce_tif_per_huc_per_mag_for_stage(
    rem_path,
    catchments_path,
    hydroid_list,
    hand_stage,
    lid_directory,
    category,
    huc,
    lid,
    branch,
    parent_log_output_file,
    child_log_file_prefix,
):
    """
    # Open rem_path and catchment_path using rasterio.

    Note: category is not always just things like "action", "moderate", etc
       When using this intervals, it can look like action_24ft, moderate_26ft
    """

    try:
        # This is setting up logging for this function to go up to the parent
        MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)

        # MP_LOG.lprint("+++++++++++++++++++++++")
        # MP_LOG.lprint(f"At the start of producing a tif for {huc}")
        # MP_LOG.trace(locals())
        # MP_LOG.trace("+++++++++++++++++++++++")

        output_tif = os.path.join(
            lid_directory, lid + '_' + category + '_extent_' + huc + '_' + branch + '.tif'
        )

        # both of these have a nodata value of 0 (well.. not by the image but by cell values)
        rem_src = rasterio.open(rem_path)
        catchments_src = rasterio.open(catchments_path)
        rem_array = rem_src.read(1)
        catchments_array = catchments_src.read(1)

        # TEMP: look at a catchment and rem from the same branch.
        # Then look at a stage based 4.4.0.0 for this huc and see if we can figure out the
        # intended results. Are we trying for a image that makes all values below the hand stage
        # value to be a value (kinda like a 1 and 0 ?)

        # Use numpy.where operation to reclassify rem_path on the condition that the pixel values
        #   are <= to hand_stage and the catchments value is in the hydroid_list.

        reclass_rem_array = np.where((rem_array <= hand_stage) & (rem_array != rem_src.nodata), 1, 0).astype(
            'uint8'
        )

        # print(f"Hydroid_list is {hydroid_list} for {hand_stage}")

        hydroid_mask = np.isin(catchments_array, hydroid_list)

        target_catchments_array = np.where(
            ((hydroid_mask == True) & (catchments_array != catchments_src.nodata)), 1, 0
        ).astype('uint8')

        masked_reclass_rem_array = np.where(
            ((reclass_rem_array >= 1) & (target_catchments_array >= 1)), 1, 0
        ).astype('uint8')

        # change it all to either 1 or 0 (one being inundated)
        # masked_reclass_rem_array[np.where(masked_reclass_rem_array <= 0)] = 0
        # masked_reclass_rem_array[np.where(masked_reclass_rem_array > 0)] = 1

        # Save resulting array to new tif with appropriate name. ie) brdc1_record_extent_18060005.tif
        # to our mapping/huc/lid site
        is_all_zero = np.all(masked_reclass_rem_array == 0)

        # MP_LOG.lprint(f"{huc}: masked_reclass_rem_array, is_all_zero is {is_all_zero} for {rem_path}")

        # if not is_all_zero:
        # if is_all_zero is False: # this logic didn't let ANY files get saved
        #    'is False' means that the object does not exist and not that it really equals the value of False
        if is_all_zero == False:  # corrected logic
            output_tif = os.path.join(
                lid_directory, lid + '_' + category + '_extent_' + huc + '_' + branch + '.tif'
            )
            # MP_LOG.lprint(f" +++ Output_Tif is {output_tif}")
            with rasterio.Env():
                profile = rem_src.profile
                profile.update(dtype=rasterio.uint8)
                profile.update(nodata=0)

                # Replace any existing nodata values with the new one
                # masked_reclass_rem_array[masked_reclass_rem_array == profile["nodata"]] = 0

                with rasterio.open(output_tif, 'w', **profile) as dst:
                    # dst.nodata = 0
                    dst.write(masked_reclass_rem_array, 1)
        else:
            MP_LOG.trace(f"{huc} : {lid} : {category} : {branch} : inundation was all zero cells")

    except Exception:
        MP_LOG.error(f"{huc} : {lid} Error producing inundation maps with stage")
        MP_LOG.error(traceback.format_exc())

    return


# This is not part of an MP process, but needs to have FLOG carried over so this file can see it
def run_catfim_inundation(
    fim_run_dir, output_flows_dir, output_mapping_dir, job_number_huc, job_number_inundate, log_output_file
):
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
# It use used for flow-based
# It inundate each set based on the ahps/mangnitude list and for each segment in the
# the branch hydrotable
# Then each is inundated per branch and mosiaked for the ahps
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
    # Note: child_log_file_prefix is "MP_run_ind", meaning all logs created by this function start
    #  with the phrase "MP_run_ind"
    #  They will be rolled up into the parent_log_output_file
    # This is setting up logging for this function to go up to the parent\
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
            hucs=huc,
            inundation_raster=output_extent_tif,
            inundation_polygon=None,
            depths_raster=None,
            verbose=False,
            log_file=None,
            output_fileNames=None,
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

    # For space reasons, we need to delete all of the intermediary files such as:
    #    Stage: grmn3_action_extent_0.tif, grmn3_action_extent_1933000003.tif. The give aways are a number before
    #        the .tif
    #    Flows: allm1_action_12p0ft_extent_01010002_0.tif, allm1_action_12p0ft_extent_01010002_7170000001.tif
    #       your give away is to just delete any file that has the HUC number in teh file name
    # The intermediatary are all inundated branch tifs.

    # The ones we want to keep end at _extent.tif
    # branch_tifs = glob.glob(f"{output_huc_site_mapping_dir}/*_extent_*.tif")
    # for tif_file in branch_tifs:
    #     os.remove(tif_file)

    return


# This is part of an MP Pool
# TODO: Aug 2024: job_number_inundate is not used well at all and is partially
# with more cleanup to do later. Partially removed now.
def post_process_huc(
    output_catfim_dir,
    ahps_dir_list,
    huc_dir,
    gpkg_dir,
    fim_version,
    huc,
    parent_log_output_file,
    child_log_file_prefix,
    progress_stmt,
):

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

            # Append desired filenames to list. (notice.. no value after the word extent)
            # tif_list = [x for x in os.listdir(mapping_huc_lid_dir) if x.endswith("extent.tif")]  # doesn't match the old filenames

            # new logic actually finds the extent tifs
            # It will find only the mag rollups and not its branches

            # if stage based, the file names looks like this: masm1_major_20p0ft_extent.tif
            #    but there also is masm1_major_extent.tif, so we want both
            # if flow based, the file name looks like this: masm1_action_extent.tif

            tif_list = [x for x in os.listdir(mapping_huc_lid_dir) if ('extent.tif') in x]

            if len(tif_list) == 0:
                # This is perfectly fine for there to be none
                # MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {mapping_huc_lid_dir}")
                continue

            for tif in tif_list:
                if 'extent.tif' in tif:
                    # as we processing tifs for just one ahps at a time, we can further files that have that
                    # ahps_lid in it.
                    if ahps_lid in tif:
                        tifs_to_reformat_list.append(os.path.join(mapping_huc_lid_dir, tif))

            if len(tifs_to_reformat_list) == 0:
                # MP_LOG.warning(f">> no tifs found for {huc} {ahps_lid} at {mapping_huc_lid_dir}")
                continue

            # Stage-Based CatFIM uses attributes from individual CSVs instead of the master CSV.
            nws_lid_attributes_filename = os.path.join(attributes_dir, ahps_lid + '_attributes.csv')

            # There may not necessarily be an attributes.csv for this lid, depending on how flow processing went
            # lots of lids fall out in the attributes or flow steps.
            if os.path.exists(nws_lid_attributes_filename) is False:
                MP_LOG.warning(f"{ahps_lid} has no attributes file which may perfectly fine.")
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

            # with ProcessPoolExecutor(max_workers=job_number_inundate) as executor:
            # TODO:
            # Aug 2024, Using MP (job number inundate has very little value, drop it)
            # Clean up that ji MP
            #            with ProcessPoolExecutor(max_workers=job_number_inundate) as executor:
            for tif_to_process in tifs_to_reformat_list:
                # If not os.path.exists(tif_to_process):
                #    continue

                # If stage based, the file names looks like this: masm1_major_20p0ft_extent.tif
                #    but there also is masm1_major_extent.tif, so we want both
                # If flow based, the file name looks like this: masm1_action_extent.tif
                MP_LOG.trace(f".. Tif to Process = {tif_to_process}")
                try:

                    tif_file_name = os.path.basename(tif_to_process)
                    file_name_parts = tif_file_name.split("_")
                    magnitude = file_name_parts[1]

                    # stage based, ie grnm1_action_11p0ft_extent.tif
                    # careful. ft can be part of the site name
                    if len(file_name_parts) >= 3 and "ft" in file_name_parts[2]:
                        try:
                            stage_val = file_name_parts[2].replace('p', '.').replace("ft", "")
                            interval_stage = float(stage_val)
                        except ValueError:
                            interval_stage = None
                            MP_LOG.error(
                                f"Value Error for {huc} - {ahps_lid} - magnitude {magnitude}"
                                f" at {mapping_huc_lid_dir}"
                            )
                            MP_LOG.error(traceback.format_exc())
                    else:  # flow based. ie) cfkt2_action_extent.tif
                        interval_stage = None

                    reformat_inundation_maps(
                        ahps_lid,
                        tif_to_process,
                        gpkg_dir,
                        fim_version,
                        huc,
                        magnitude,
                        nws_lid_attributes_filename,
                        interval_stage,
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
    catfim_method, output_catfim_dir, job_huc_ahps, fim_version, log_output_file
):

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
                fim_version,
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

        # Concatenate each /gpkg/{aphs}_{magnitude}_extent_{huc}_dissolved.gpkg
        diss_extent_filename = os.path.join(gpkg_dir, gpkg_file)
        diss_extent_gdf = gpd.read_file(diss_extent_filename, engine='fiona')
        diss_extent_gdf['viz'] = 'yes'

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
    # else:
    #     FLOG.lprint("Dissolving stage based catfim_libary by ahps and magnitudes and interval value")
    #     merged_layers_gdf = merged_layers_gdf.dissolve(by=['ahps_lid',
    #                                                        'magnitude',
    #                                                        'interval_stage'], as_index=False)

    if 'level_0' in merged_layers_gdf:
        merged_layers_gdf = merged_layers_gdf.drop(['level_0'], axis=1)

    output_file_name = f"{catfim_method}_catfim_library"

    # TODO: Aug 2024: gpkg are not opening in qgis now? project, wkt, non defined geometry columns?
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
    fim_version,
    huc,
    magnitude,
    nws_lid_attributes_filename,
    interval_stage,
    parent_log_output_file,
    child_log_file_prefix,
):
    """_summary_
    Turns inundated tifs into dissolved polys gpkg with more attributes

    """
    # interval stage might come in as null and that is ok

    # Note: child_log_file_prefix is "MP_reformat_tifs_{huc}", meaning all logs created by this
    # function start with the phrase "MP_reformat_tifs_{huc}". This will rollup to the master
    # catfim logs

    # This is setting up logging for this function to go up to the parent
    MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix)

    try:
        MP_LOG.trace(
            f"{huc} : {ahps_lid} : {magnitude} -- Start reformat_inundation_maps" " (tif extent to gpkg poly)"
        )
        MP_LOG.trace(F"Tif to process is {tif_to_process}")

        # Convert raster to shapes
        with rasterio.open(tif_to_process) as src:
            image = src.read(1)
            mask = image > 0

        # Aggregate shapes
        results = (
            {'properties': {'extent': 1}, 'geometry': s}
            for i, (s, v) in enumerate(shapes(image, mask=mask, transform=src.transform))
        )

        # Convert list of shapes to polygon
        # lots of polys
        # extent_poly = gpd.GeoDataFrame.from_features(list(results), crs=PREP_PROJECTION)  # Previous code
        extent_poly = gpd.GeoDataFrame.from_features(
            list(results), crs=src.crs
        )  # Updating to fix AK proj issue, worked for CONUS and for AK!

        # extent_poly = gpd.GeoDataFrame.from_features(list(results))  # Updated to accomodate AK projection
        # extent_poly = extent_poly.set_crs(src.crs)  # Update to accomodate AK projection

        # Dissolve polygons
        extent_poly_diss = extent_poly.dissolve(by='extent')

        # Update attributes
        extent_poly_diss = extent_poly_diss.reset_index(drop=True)
        extent_poly_diss['ahps_lid'] = ahps_lid
        extent_poly_diss['magnitude'] = magnitude
        extent_poly_diss['version'] = fim_version
        extent_poly_diss['huc'] = huc
        extent_poly_diss['interval_stage'] = interval_stage

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

        # Save dissolved multipolygon
        handle = os.path.split(tif_to_process)[1].replace('.tif', '')
        diss_extent_filename = os.path.join(gpkg_dir, f"{huc}_{handle}_dissolved.gpkg")
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
# Used for Flow only
def manage_catfim_mapping(
    fim_run_dir,
    output_flows_dir,
    output_catfim_dir,
    catfim_method,
    job_number_huc,
    job_number_inundate,
    log_output_file,
    step_number=1,
):

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
    # Get fim_version.

    # run_dir_prefix = ""
    # if fim_run_dir.startswith("fim_"):
    #     run_dir_prefix = "fim_"
    # else:
    #     run_dir_prefix = "hand_"

    fim_version = os.path.basename(os.path.normpath(fim_run_dir))

    # Step 2
    # TODO: Aug 2024, so we need to clean it up
    # This step does not need a job_number_inundate as it can't really use it.
    # It processes primarily hucs and ahps in multiproc
    # for now, we will manually multiple the huc * 5 (max number of ahps types)
    ahps_jobs = job_number_huc * 5
    post_process_cat_fim_for_viz(
        catfim_method, output_catfim_dir, ahps_jobs, fim_version, str(FLOG.LOG_FILE_PATH)
    )

    end = time.time()
    elapsed_time = (end - start) / 60
    FLOG.lprint(f"Finished mapping in {str(elapsed_time).split('.')[0]} minutes")

    return


if __name__ == '__main__':

    """
    Sample Usage:
    python3 /foss_fim/tools/generate_categorical_fim_mapping.py -r "/outputs/rob_test_catfim_huc"
     -s "/data/catfim/rob_test/test_5_flow_based/flows" -o "/data/catfim/rob_test/test_5_flow_based"
     -jh 1 -jn 40

    """

    # Parse arguments
    parser = argparse.ArgumentParser(description='Categorical inundation mapping for FOSS FIM.')
    parser.add_argument(
        '-r', '--fim-run-dir', help='Name of directory containing outputs of fim_run.sh', required=True
    )
    parser.add_argument(
        '-s',
        '--source-flow-dir',
        help='Path to directory containing flow CSVs to use to generate categorical FIM.',
        required=True,
        default="",
    )
    parser.add_argument(
        '-o',
        '--output-catfim-dir',
        help='Path to directory where categorical FIM outputs will be written.',
        required=True,
        default="",
    )
    parser.add_argument(
        '-jh',
        '--job-number-huc',
        help='Number of processes to use for huc processing. Default is 1.',
        required=False,
        default="1",
        type=int,
    )
    parser.add_argument(
        '-jn',
        '--job-number-inundate',
        help='OPTIONAL: Number of processes to use for inundating'
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count'
        ' of the machine. Defaults to 1.',
        required=False,
        default=1,
        type=int,
    )
    # parser.add_argument(
    #     '-depthtif',
    #     '--write-depth-tiff',
    #     help='Using this option will write depth TIFFs.',
    #     required=False,
    #     action='store_true',
    # )

    parser.add_argument(
        '-step',
        '--step_number',
        help='Using this option will write depth TIFFs.',
        required=False,
        default=1,
        type=int,
    )

    args = vars(parser.parse_args())

    fim_run_dir = args['fim_run_dir']
    source_flow_dir = args['source_flow_dir']
    output_catfim_dir = args['output_catfim_dir']
    job_number_huc = int(args['job_number_huc'])
    job_number_inundate = int(args['job_number_inundate'])
    # depthtif = args['write_depth_tiff']
    step_num = args['step_number']

    log_dir = os.path.join(output_catfim_dir, "logs")
    log_output_file = FLOG.calc_log_name_and_path(log_dir, "gen_cat_mapping")

    manage_catfim_mapping(
        source_flow_dir, output_catfim_dir, job_number_huc, job_number_inundate, log_output_file, step_num
    )
