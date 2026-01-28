#!/usr/bin/env python3

import argparse
import glob
import shutil
import math

import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import shapes
from rasterio.warp import Resampling, reproject
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

import src.utils.shared_functions as sf
import tools.catfim.catfim_shared_functions as csf

from src.utils.shared_variables import VIZ_PROJECTION
from tools.inundate_gms import Inundate_gms
from tools.mosaic_inundation import Mosaic_inundation
from tools.tools_shared_functions import mask_out_lakes


"""
Oct/Nov 2025: Notes for MP and splitting logic layer reorg. ie) pre procesing, process hucs, post processing

Tenative notes:
    - Some of the functions in here may move or be split to smaller functions.

    - Data acquision such as meta, threshold or flows, should be moved to generate_categorical_fim_flows.py.

    - This script will focus on inundation, tifs, gpkgs, etc. for both FB and SB.

    - Some current functions from other files such as generate_categorical_fim.py and maybe
      generate_categorical_fim_flows.py will be moved here if it is related to inundation,
      etc as mentioned above.

This file should focus primarily on inunation and creating tifs and final gpkgs for this HUC.
However, some logic processing is done here as well. We might move some of that later.

"""

gpd.options.io_engine = "pyogrio"


# This function is only called when the tool is being run by itself from command line
def catfim_mapping(huc, output_folder):
    """
    Run CatFIM mapping from the command line for a HUC.
    
    Args:
        - huc (str) : 8-digit hydologic unit code
        - output_folder (str) : Filepath to CatFIM outputs.

    """
    is_logging_loaded = False

    print(f"Running mapping from command line for huc {huc}.")

    # ----------------------------
    print("Setting up environment and logging.")

    # Load CatFIM run arguments from the runtime_args.env file
    csf.load_runtime_args()


    # Validate input parameters and return normalized paths
    huc_path, output_folder = csf.validate_inputs(
        huc, output_folder
    )

    # ----------------------------
    # Set up logging


    # It just sets up a logger (its own?) hummmm 
    # TODO: Implement and test logging for command line run
    # is_logging_loaded = True


    # ----------------------------
    # Create filepaths

    # Yes.. these are duplicate from catfim_process_huc.py but have to be in order to use
    # this script independently
    output_mapping_dir = os.path.join(huc_path, "mapping")
    output_temp_dir = os.path.join(huc_path, "temp")

    sites_mapping_file_path = os.path.join(output_mapping_dir, f"sites_mapping.gpkg")
    library_pre_inun_file_path = os.path.join(output_temp_dir, "library_pre_inundation.csv")
    library_post_mapping_file_path = os.path.join(output_mapping_dir, "library_post_mapping.gpkg")

    # Remove and rebuild fresh new mapping and temp folders
    shutil.rmtree(output_mapping_dir, ignore_errors=True)
    os.mkdir(output_mapping_dir)

    shutil.rmtree(output_temp_dir, ignore_errors=True)
    os.mkdir(output_temp_dir)

    # Remove pre-existing post-mapping library file
    if os.path.isfile(library_post_mapping_file_path):
        os.remove(library_post_mapping_file_path)
    
    # ----------------------------
    # Process CatFIM mapping

    print("Begin mapping") # TODO: replace prints with logging?

    process_mapping(
        os.getenv('CATFIM_TYPE'),
        huc_path,
        output_mapping_dir,
        output_temp_dir,
        sites_mapping_file_path,
        library_pre_inun_file_path,
        library_post_mapping_file_path,
    )

    print("Completed running mapping from command line")

    # TODO: Wrap up logs?


# Main function for CatFIM mapping processing for a HUC
def process_mapping(
    catfim_type,
    huc,
    huc_path, # huc path {fim_run_dir}/{huc}
    output_mapping_dir, # dir where mapping outputs are stored - {huc_path}/mapping/
    output_temp_dir,  # TODO: decide, do we need this?
    sites_mapping_file_path,
    library_pre_inun_file_path,  # the csv / df version before we add geometry
    library_post_mapping_file_path,  # the gpkg version (once we've added geometry)
):
    
    """

    Handle the CatFIM mapping for a HUC.

    TODO: Add docstring


    CatFIM Reorg Notes (Jan 26):

    process_mapping is still heavily in progress as of 1/12/26



    Everything related to mapping starts here. SB and FB from catfim_process_huc or from
    mapping command line via catfim_mapping function

    at this point, you will need to load the sites gpkg that was copied as a workign version into
    the mapping dir (if we want to use that idea), but you should not need any threshold data files
    that has been loading, validated and some processing done and is now part of the library.csv (df)
    We know some library recs will be rejected in here and sites.gpkg updated for the reason why as we progress.
    We also know some mags or sites have been already rejected earlier and won't even have mag records
    for it in the library df.
    We can add temp columns to all df, .gpkgs, gdfs, etc at any time, we just have to clean them up later.
    That option helps keep each processing of each site/mag more independent if it helps.
    We also know SB will add some library interval recs.

    

    Jan 7, 2026: I am now thinking we do not want/need a temp directory and jsut keep the intermediates
    and checkpoint files in the root. There won't be that many of them. # TODO: Decide about temp file (i'm leaning towards remove too -E)
    We will continue to make subfolders and files inside the mappign folder with a similar structure
    to what we currently have. We may want to keep the subfodler structure as is as it is not too bad.

    That being said.. we can also look for opportunites to keep stuff in memory whenever reasonable
    as reading/writing to disk slow things down a little.  But. ease of usages and understanding of
    how the code works is far more important than optimization.

    If it helps, we can always make temp dfs with filtered sites data

    """

    # Add its own duration system and section start and close messages
    section_start_dt = datetime.now(timezone.utc)
    logging.info("Starting CatFIM HUC-level mapping")

    # --------------------------
    # Load mapping data for HUC

    print(f"{huc} - Mapping - Loading mapping data for HUC") # TODO: replace prints with logging?

    segments_file_path = os.path.join(huc_path, "features_segments.csv")
    sites_gdf, huc_library_df, huc_segments_df =  __load_mapping_data(
        huc_path,
        sites_mapping_file_path,
        segments_file_path,
        library_pre_inun_file_path)

    # --------------------------
    # Validate inputs

    validation_pass, validation_messages = __validate_mapping_data(huc, sites_gdf, huc_library_df, huc_segments_df)

    for msg in validation_messages:
        print(msg) # TODO: Implement logging

    if validation_pass == False:
        print(f"{huc} - Mapping - Validation failed in mapping for HUC {huc}") # TODO: Implement logging
        # TODO: exit? continue? do we need to update logs? do we need to update the library?

        # return ?

    # --------------------------
    # Mapping pre-processing (processing used by both SB and FB)

    # Get FIM run dir from env file # TODO: check that this works

    # csf.load_runtime_args(output_folder)
    
    fim_run_dir = os.getenv("FIM_RUN_DIR")
    fim_run_dir = '/data/previous_fim/hand_4_8_7_2' # TODO: Remove!!! TEMP DEBUG just keeping this in for testing so I can run process_mapping() individually
    print(f"{huc} - Mapping - FIM_RUN_DIR is {fim_run_dir}") # TEMP DEBUG

    # --------------------------
    # Split to SB- and FB-specific mapping processing

    if catfim_type == "sb":
        print(f"{huc} - Mapping - Beginning stage-based mapping") # TODO: replace prints with logging?

        # TODO: Decide: One thing not yet done is using the __calc_stage_values but do we still even need it?
        # and if so.. where.  The library df already has the stage values so I think we
        # don't need it anymore

        # Jan 26 - New function
        sites_gdf, huc_library_df = run_sb_mapping(
                                        huc,
                                        huc_path,
                                        sites_gdf,
                                        huc_library_df,
                                        output_mapping_dir,
                                        fim_run_dir,
                                        output_temp_dir,
                                        huc_segments_df,
                                        )

    elif catfim_type == "fb":
        print(f"{huc} - Mapping - Beginning flow-based mapping") # TODO: replace prints with logging?

        # Jan 26 - Renamed run_catfim_inundation() to run_fb_mapping() 
        sites_gdf, huc_library_df = run_fb_mapping(
                                        huc,
                                        huc_path,
                                        sites_gdf,
                                        huc_library_df,
                                        output_mapping_dir,
                                        fim_run_dir,
                                        output_temp_dir,
                                        )

    # At this point, we will have the inundated tifs for each valid site/magnitude combination
    # and the updated sites_gdf and huc_library_df. 

    # -----------------------------
    # HUC-level post-processing (for both FB and SB)
    # Note: Got rid of the post_process_cat_fim_for_viz function and just call post_process_huc() here

    # Post-process HUC
    huc_library_undissolved_gdf, huc_library_gdf = post_process_huc( # TODO: can remove undissolved as an output after development/debugging is done
        huc,
        huc_library_df,
        output_mapping_dir,
        catfim_type,
    )

    # TODO: add check to see if there are any sites left that have some inundated files
    # If not, the sites_gdf should already have the recs updated to know why

    # -----------------------------
    # Create final sites and library GDF

    # Get a list of sites that have at least one mapped geometry
    sites_with_valid_geoms_gdf = huc_library_gdf[~huc_library_gdf.geometry.is_empty & huc_library_gdf.geometry.notna()]
    mapped_sites_list = sites_with_valid_geoms_gdf['nws_lid'].unique().tolist()

    print(f"{huc} - Mapping - Mapped sites list: {mapped_sites_list}") ## TEMP DEBUG

    # Update mapping status in the sites gdf
    for index, row in sites_gdf.iterrows():
        lid = row["nws_lid"]

        if lid in mapped_sites_list:
            sites_gdf.at[index, "mapped"] = "yes"
            # sites_gdf.at[index, "status"] = # do we need to add a status here? probably not

    # Add metadata columns from the sites GDF to the library GDF
    huc_library_gdf = huc_library_gdf.merge(
        sites_gdf.drop(columns=['geometry']), 
        on='nws_lid', 
        how='left'
    )

    # TODO: if catfim_type == fb, remove interval stuff from the output dfs and csvs
    # do we need to remove from sites gdf too?
    if catfim_type == 'fb':
        if 'interval_stage' in huc_library_gdf.columns:
            huc_library_gdf.drop('interval_stage', axis=1, inplace=True)

            print('Dropped interval_stage col from HUC library GDF') ## TEMP DEBUG

        if 'is_interval' in huc_library_gdf.columns:
            huc_library_gdf.drop('is_interval', axis=1, inplace=True)

            print('Dropped is_interval col from HUC library GDF') ## TEMP DEBUG


    # -----------------------------
    # Save the HUC-level mapping outputs

    print()

    # For debugging purposes, save undissolved library TODO: TEMP DEBUG, Remove later?
    library_post_mapping_undissolved_file_path = os.path.join(output_temp_dir, f'library_post_mapping_undissolved_{huc}.gpkg')
    print(f"{huc} - Mapping - Saving undissolved library to {library_post_mapping_undissolved_file_path}") # TODO: Update logging
    huc_library_undissolved_gdf.to_file(library_post_mapping_undissolved_file_path, driver='GPKG', engine="fiona")

    # Save HUC library GPKG
    print(f"{huc} - Mapping - Saving HUC library to {library_post_mapping_file_path}") # TODO: Update logging
    huc_library_gdf.to_file(library_post_mapping_file_path, driver='GPKG', engine="fiona")

    # Save HUC library CSV
    library_post_mapping_csv_file_path = library_post_mapping_file_path.lower().replace("gpkg", "csv")
    print(f"{huc} - Mapping - Saving HUC library CSV to {library_post_mapping_csv_file_path}") # TODO: Update logging

    if 'geometry' in huc_library_gdf: 
        huc_library_gdf = huc_library_gdf.drop(['geometry'], axis=1)

    huc_library_gdf.to_csv(library_post_mapping_csv_file_path)

    # Save HUC sites GPKG
    print(f"{huc} - Mapping - Saving HUC sites GDF to {sites_mapping_file_path}") # TODO: Update logging
    sites_gdf.to_file(sites_mapping_file_path, driver='GPKG', engine="fiona")

    # Save HUC sites CSV
    sites_mapping_csv_file_path = sites_mapping_file_path.lower().replace("gpkg", "csv")
    print(f"{huc} - Mapping - Saving HUC sites CSV to {sites_mapping_csv_file_path}") # TODO: Update logging

    if 'geometry' in sites_gdf: 
        sites_gdf = sites_gdf.drop(['geometry'], axis=1)

    sites_gdf.to_csv(library_post_mapping_csv_file_path)

    # -----------------------------
    # TODO: Add final checks




    logging.info(f"{huc} - Mapping - End of CatFIM HUC-level mapping")
    duration_msg = sf.calculate_duration_msg(section_start_dt)
    logging.info(duration_msg)

    return


def run_fb_mapping(
    huc, 
    huc_path,
    sites_gdf,
    huc_library_df,
    output_mapping_dir, 
    fim_run_dir,
    output_temp_dir,
):
    '''
    Only used in flow-based CatFIM.
    Runs for each HUC.

    Processes:
    - Gets a list of sites in HUC
    - Loops through sites. For each site:
        - Gets list of magnitudes
        - Creates filepaths
        - Runs run_fb_inundation() for each site/magnitude combination to create inundation tifs 
            (using Inundate_gms, Mosaic_inundation, and mask_out_lakes)

    Analogous to run_sb_mapping() for stage-based CatFIM. Different from SB in that
    this function iterates by site/magnitude whereas run_sb_mapping() iterates by site. (TODO: could fix this? not sure if it's worth it)


    CatFIM Reorg Notes (Jan 26):

        This function was previously named run_catfim_inundation but was renamed to run_fb_mapping for clarity 
        (because it is only used in flow-based CatFIM).

    '''

    # TODO: Add a pointer in this file coming from generate_categorial_fim so they can share the same log file
    print()

    logging.info(f"{huc} - Mapping -  Start inundating and mosaicking")
    print(f"{huc} - Mapping -  Start inundating and mosaicking") # TEMP DEBUG


    # -----------------------
    # Get list of AHPS sites in HUC from the sites GDF (excluding sites where mapped = no, we only
    # want to map sites where mapped = not yet)
    ahps_sites_list = sites_gdf[sites_gdf['mapped'] != 'no']['nws_lid'].tolist()
    print(f"{huc} - Mapping - ahps_sites_list is {ahps_sites_list}")

    # -----------------------

    # Read thresholds CSV files to get list of magnitudes per site
    huc_thresholds_csv_path = os.path.join(huc_path, f'{huc}_thresholds.csv')
    huc_thresholds_df = pd.read_csv(huc_thresholds_csv_path)

    # Filter out rows where threshold_type is not "flows"
    huc_thresholds_df = huc_thresholds_df[huc_thresholds_df['threshold_type'] == 'flows']

    # Use pd.melt to pivot the df to long format
    huc_thresholds_long_df = pd.melt(
        huc_thresholds_df,
        id_vars=['nws_lid'],
        value_vars=['action', 'minor', 'moderate', 'major', 'record'],
        var_name='magnitude_type',
        value_name='magnitude_value'
    )

    # Remove rows where magnitude_value is -1.0 # TODO: Update with a variable?
    huc_thresholds_long_df = huc_thresholds_long_df[huc_thresholds_long_df['magnitude_value'] != -1.0]

    # Loop through AHPS sites
    for ahps_site in ahps_sites_list:

        # Get a list of available magnitudes
        huc_thresholds_long_df_site = huc_thresholds_long_df[huc_thresholds_long_df['nws_lid'] == ahps_site]
        magnitude_list = huc_thresholds_long_df_site['magnitude_type'].to_list()

        # Load the magnitude flows CSV for the HUC
        magnitude_flows_csv = os.path.join(huc_path, 'flow_discharges.csv')
        magnitude_flows_df = pd.read_csv(magnitude_flows_csv)

        # Loop through thresholds/magnitudes and define inundation output files paths
        print(f"{huc} : {ahps_site} - Magnitude list is {magnitude_list}")

        for magnitude in magnitude_list:
            if "." in magnitude: # TODO: Do we need this check?
                continue

            # Create a site/magnitude specific flows csv and drop unnecessary colunmns
            magnitude_flows_df_filtered = magnitude_flows_df[
                (magnitude_flows_df['lid'] == ahps_site) &
                (magnitude_flows_df['magnitude'] == magnitude)
            ]
            magnitude_flows_df_filtered = magnitude_flows_df_filtered.drop(columns=['lid', 'magnitude'])

            # Save filtered flows to CSV in the temp dir
            magnitude_flows_csv_path = os.path.join(output_temp_dir, f"{ahps_site}_{magnitude}_flows.csv")
            magnitude_flows_df_filtered.to_csv(magnitude_flows_csv_path, index=False)

            print(f"{huc} : {ahps_site} : {magnitude} - Saved flows to {magnitude_flows_csv_path}") # TEMP DEBUG

            # Define output inundation extent tif path # TODO: Decide if there's anything about the name I want to change
            tif_name = ahps_site + '_' + magnitude + '_extent.tif'
            output_extent_tif = os.path.join(output_mapping_dir, tif_name)

            print(f"{huc} : {ahps_site} : {magnitude} - Begin inundation for {tif_name}")
            try:
                # executor.submit( # TODO: decide about keeping MP here (removed for now)
                job_number_inundate = 1 # TODO: Decide about keeping MP here? (added a placeholder for now)

                run_fb_inundation(
                    huc,
                    ahps_site,
                    magnitude,
                    fim_run_dir,
                    magnitude_flows_csv_path,  # Can be a CSV path or a dataframe, using a csv path for now
                    output_extent_tif,
                    job_number_inundate,
                )

            except Exception:
                logging.critical(
                    "A critical error occurred while attempting inundation"
                    f" for {huc} - {ahps_site}-- {magnitude}"
                )
                print(traceback.format_exc())  # TEMP DEBUG
                logging.critical(traceback.format_exc())
                # logging.merge_log_files(log_output_file, child_log_file_prefix) # TODO: Update logging here
                sys.exit(1)

    print()
    # logging.info(f"{huc} - Mapping -  End inundating and mosaicking")
    print(f"{huc} - Mapping -  End inundating and mosaicking") # TEMP DEBUG


    return sites_gdf, huc_library_df


def run_fb_inundation( # renamed from run_inundation
    huc,
    ahps_site,
    magnitude,
    fim_run_dir,
    magnitude_flows_csv_path,
    output_extent_tif,
    job_number_inundate,
):
    '''
    Only used in flow-based CatFIM.

    Runs for each site/magnitude combination.

    Processes:
        - Inundate_gms()
        - Mosaic_inundation()
        - mask_out_lakes()

        -> returns inundated site/mag tifs with lakes masked out


    Runs the inundation mapping workflow for a given HUC and magnitude, including logging,
    inundation raster generation, mosaicking, and lake masking.

    Inundates each set based on the ahps/mangnitude list and for each segment in the the branch hydrotable.
    Then each set is inundated per branch and mosiaked for the AHPS site.

    Parameters: # TODO: Update docstring

    
    '''

    # Note: child_log_file_prefix is "MP_run_ind", meaning all logs created by this function start
    #  with the phrase "MP_run_ind" # TODO: Update logging
    #  They will be rolled up into the parent_log_output_file
    # This is setting up logging for this function to go up to the parent


    # TODO: decide... HUMM.... How do we want to handle exceptions in here? Let them out? 
    try:
        logging.info(f"{huc} : {ahps_site} : {magnitude} - Running Inundate_gms and mosiacking") # TODO: Update logging

        map_file = Inundate_gms(
            hydrofabric_dir=fim_run_dir,
            forecast=magnitude_flows_csv_path,
            num_workers=job_number_inundate, # TODO: keep multiproc? currently defaults to 1 
            hydro_table_df=None,
            hucs=huc,
            inundation_raster=output_extent_tif,
            depths_raster=None,
            verbose=False,
            log_file=None,
            output_fileNames=None,
            multi_process=True,
        )

        # ---------------------
        # Mosaic inundation tifs for lid/category

        logging.info(f"{huc} : {ahps_site} : {magnitude} - Starting to mosaic inundation")

        Mosaic_inundation(
            map_file,
            mosaic_attribute='inundation_rasters',
            mosaic_output=output_extent_tif,
            mask=os.path.join(fim_run_dir, 'wbd.gpkg'),
            unit_attribute_name='huc8',
            nodata=-9999,
            workers=1,
            remove_inputs=False,
            subset=None,
            verbose=False,
        )

        logging.info(f"{huc} : {ahps_site} : {magnitude} - Mosaic inundation complete")

        # ---------------------
        # Mask out lakes from inundated tif and re-save tif

        print(f"{huc} : {ahps_site} : {magnitude} - Masking out lakes from {output_extent_tif}") ## TEMP DEBUG

        # TODO: (fall 2026). Update to only run if lake detected? 
        with rasterio.open(output_extent_tif, 'r+') as output_extent_src:
            output_extent_array = output_extent_src.read(1)
            output_extent_array_masked, mask_status = mask_out_lakes(
                output_extent_array, huc, output_extent_src, fim_run_dir
            )
            output_extent_src.write(output_extent_array_masked, 1)

        logging.info(f"{huc} : {ahps_site} : {magnitude} - Lake masking complete")

        if mask_status:
            print(f'{huc} : {ahps_site} : {magnitude} - Masking status: {mask_status}') ## TODO: Update logging

    # TODO: Decide humm... do we keep the try catch here? do we even want one?
    # what do we want to do if a site, mag fails... dump the entire tool or
    # just log this site/mag?
    except Exception:
        # Log errors and their tracebacks

        logging.info(f"{huc} : {ahps_site} : {magnitude} - EXCEPTION OCCURRED: ") # TODO: update so it has 'critical' or 'error' somehow?
        logging.info(traceback.format_exc())
        return

    # Inundation.py appends the huc code to the supplied output_extent_grid for stage-based.
    # Modify output_extent_grid to match inundation.py saved filename.
    # Search for this file, if it didn't create, send message to log file.

    # base_file_path, extension = os.path.splitext(output_extent_tif)
    # saved_extent_grid_filename = "{}_{}{}".format(base_file_path, huc, extension)

    # MP_LOG.trace(f"saved_extent_grid_filename is {saved_extent_grid_filename}")

    if not os.path.exists(output_extent_tif): # TODO: Doublecheck that this is the way we want to check for success
        logging.error(f"{huc} : {ahps_site} : {magnitude} - FAILURE: map failed to create")
        # return

    # TODO: maybe add a bool for critical error for the try catch if we keep it?
    return


def run_sb_mapping(
    huc,
    huc_path,
    sites_gdf,
    huc_library_df,
    output_mapping_dir,
    fim_run_dir,
    output_temp_dir,
    huc_segments_df,
):
    """
    
    Only used in stage-based CatFIM.
    Runs for each HUC.

    Processes:
    - Gets a list of sites in HUC # TODO: Implement this 
    - Loops through sites. For each site, runs: (TODO: Add for each site Get list of magnitudes, Create filepaths? )
        - run_sb_inundation() (previously called produce_stage_based_lid_tifs)
          which returns inundated site/mag tifs for each site      
    
          
    CatFIM Reorg Notes (Jan 26):
        New... this is a wrapper for all things that SB needs do such
        as figure out intervals, iterate sites and lids and build the tifs

        TODO: figure out intervals

    """

    # TODO: Add duration system here

    # logging.info(f"{huc} - Mapping - Start inundating and mosaicking")
    print(f"{huc} - Mapping - Start inundating and mosaicking") # TEMP DEBUG

    # -----------------------
    # Get list of AHPS sites in HUC from the sites GDF 
    # (Exclude sites where mapped = no, we only want to map sites where mapped = not yet)
    ahps_sites_list = sites_gdf[sites_gdf['mapped'] != 'no']['nws_lid'].tolist()
    print(f"{huc} - Mapping - ahps_sites_list is {ahps_sites_list}")

    # Read thresholds CSV files to get list of magnitudes per site
    huc_thresholds_csv_path = os.path.join(huc_path, f'{huc}_thresholds.csv')
    huc_thresholds_df = pd.read_csv(huc_thresholds_csv_path)

    # Filter out rows where threshold_type is not "stage"
    huc_thresholds_df = huc_thresholds_df[huc_thresholds_df['threshold_type'] == 'stages']

    # Use pd.melt to pivot the df to long format
    huc_thresholds_long_df = pd.melt(
        huc_thresholds_df,
        id_vars=['nws_lid'],
        value_vars=['action', 'minor', 'moderate', 'major', 'record'],
        var_name='magnitude_type',
        value_name='magnitude_value'
    )

    # Remove rows where magnitude_value is -1.0 # TODO: Update with a variable?
    huc_thresholds_long_df = huc_thresholds_long_df[huc_thresholds_long_df['magnitude_value'] != -1.0]

    # Loop through AHPS sites
    for ahps_site in ahps_sites_list:

        # -----------------------
        # Inundate site using available stage magnitudes
    
        # Get a list of available magnitudes
        huc_thresholds_long_df_site = huc_thresholds_long_df[huc_thresholds_long_df['nws_lid'] == ahps_site]
        magnitude_list = huc_thresholds_long_df_site['magnitude_type'].to_list()

        # Loop through thresholds/magnitudes and define inundation output files paths
        print(f"{huc} : {ahps_site} - Magnitude list is {magnitude_list}")

        # Get segments for each AHPS site

        # Filter df by site
        site_segments_df = huc_segments_df[huc_segments_df['lid'] == ahps_site]

        # Make the feature_id column into a list
        segments = site_segments_df['feature_id'].tolist()

        # Iterate magnitudes to run inundation
        for magnitude in magnitude_list:
            if "." in magnitude: # TODO: Do we need this check? Is this where we check for intervals? 
                continue

            # Subset library df for site/magnitude and get the stage and other values
            site_mag_library_df = huc_library_df[
                (huc_library_df['nws_lid'] == ahps_site) & (huc_library_df['magnitude'] == magnitude)
            ]

            stage_val = site_mag_library_df['stage'].iloc[0]
            datum_adj_ft = site_mag_library_df['datum_adj_ft'].iloc[0]
            lid_usgs_elev = site_mag_library_df['lid_usgs_elev'].iloc[0]
            lid_altitude = site_mag_library_df['lid_alt_ft'].iloc[0] # TODO: Double check that this is correct variable

            # Calculate a portion of the file name which includes the category and
            # a formatted stage value (would include "i" if it were an interval file)
            category_key = __calculate_category_key(magnitude, stage_val, False)  # False = not an interval

            print(f"{huc} : {ahps_site} : {magnitude} - Begin inundation for {category_key}")

            # Create the inundation tifs for each site/mag combo for the lid (previously called produce_stage_based_lid_tifs)
            run_sb_inundation(
                huc,
                sites_gdf, 
                huc_library_df,
                stage_val,
                datum_adj_ft,
                lid_usgs_elev,
                lid_altitude,
                fim_run_dir,
                segments,
                ahps_site,
                output_mapping_dir,
                magnitude,
                category_key,
        )

        # -----------------------
        # Inundate site using additional stage intervals

        # Get non-record stages for the site and sort them by stage value
        non_rec_thresholds_df_unsorted_site = huc_thresholds_long_df_site[huc_thresholds_long_df_site["stage_name"] != 'record']
        non_rec_thresholds_df_site = non_rec_thresholds_df_unsorted_site.sort_values(
            by='stage_value'
        ).reset_index()

        # We already inundated and created files for the specific stages just not the intervals
        # Make list of interval recs to be created
        interval_list = []  # might stay empty

        huc_lid_id = f"{huc} : {ahps_site}" # TODO: take this out? or apply everywhere? it's just a label for logging

        # TODO: where should we get past_major_interval_cap from? do we even need this to be something we can change...?
        # because I honestly think we could just hard code it in...
        past_major_interval_cap = 5 # TODO: decide if this hard coding is ok

        if len(non_rec_thresholds_df_site) > 0:

            # Calculate intervals for the site
            interval_list = __calc_sb_intervals(
                non_rec_thresholds_df_site, past_major_interval_cap, huc_lid_id
            )

            # tif_child_log_file_prefix = MP_LOG.MP_calc_prefix_name( # TODO: Clean up
            #     parent_log_output_file, "MP_sb_interval_tifs"
            # )
            # Now we add the interval tifs but no interval tifs for the "record" stage if there is one.
            # with ProcessPoolExecutor(max_workers=job_number_intervals) as executor:
            #     try:


            # Iterate intervals to run inundation
            for interval_rec in interval_list:  # list of lists

                magnitude = interval_rec[0]  # stage name, was category
                interval_stage_val = interval_rec[1]

                # Calculate a portion of the file name which includes the category,
                # a formatted stage value, and a "i" to show it is an interval file
                category_key = __calculate_category_key(magnitude, interval_stage_val, True)

                print(f"{huc} : {ahps_site} : {magnitude} - Begin inundation for {category_key} (interval)")

                # Create the inundation tifs for each site/mag combo for the lid (previously called produce_stage_based_lid_tifs)
                run_sb_inundation(
                    huc,
                    sites_gdf,
                    huc_library_df,
                    interval_stage_val,
                    datum_adj_ft,
                    lid_usgs_elev,
                    lid_altitude,
                    fim_run_dir,
                    segments,
                    ahps_site,
                    output_mapping_dir,
                    magnitude,
                    category_key,
            )
                
                # TODO: Put run_sb_inundation back into a Try / Except statement?
                
                # except TypeError:  # sometimes the thresholds are Nonetypes # TODO: Implement or clean up
                #     MP_LOG.error(
                #         f"{huc_lid_id}: ERROR: type error in ProcessPool,"
                #         " likely in the interval code"
                #     )
                #     MP_LOG.error(traceback.format_exc())
                #     continue

                # except Exception:
                #     MP_LOG.critical(f"{huc_lid_id}: ERROR: ProcessPool has an error")
                #     MP_LOG.critical(traceback.format_exc())
                #     # merge MP Logs (Yes)
                #     MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix, True)
                #     sys.exit(1)

            # # merge MP Logs (merging MP into an MP (proc_pool in a proc_pool))
            # MP_LOG.merge_log_files(parent_log_output_file, tif_child_log_file_prefix, True)

        else:
            print(
                f"{huc_lid_id}: Skipping intervals as there are not any 'non-record' stages"
            )

    print()
    # logging.info(f"{huc} - Mapping - End inundating and mosaicking")
    print(f"{huc} - Mapping - End inundating and mosaicking") # TEMP DEBUG

    return sites_gdf, huc_library_df,
    # # TODO: return crit message (aka somethign to say abort it?) as well?
    # aka.. maybe somethign to say skip_making_final_library files? TBD







# stage_val, datum_adj_ft, usgs_elev, altitude should already be here
# mostly via library df. Likely be a bit more adj needs for
# stage specific alt adjustments, but is it done here?

# For the  datum_adj_wse, datum_adj_wse_m and hand_stage, just update the library df
# and return it.

# Category (mag specific? mabye that makes sense, TBD)
# Is there anyting in here that needs to update the sites.gdf, that could change mapping
# to no? If not... then don't pass it unless you want some data in it.
# Likely by now, everyting you need is in the library df.
# We will be updateing the datum_adj_wse, datum_adj_wse_m and hand_stage to the
# library df so we want to return it.
# PS.. what is hand_stage? Is this just an update to the library df "stage" column?

def run_sb_inundation( # Jan 26: was previously called produce_stage_based_lid_tifs
    huc,
    sites_gdf, # TODO: Remove if unused
    huc_library_df, # TODO: Remove if unused
    stage_val,
    datum_adj_ft,
    lid_usgs_elev,
    lid_altitude,
    fim_run_dir,
    segments,
    ahps_site,
    output_mapping_dir,
    magnitude,
    category_key,
):
    '''
    Only used for stage-based CatFIM.

    Runs for each site/magnitude combination.

    Processes:
        - calculate HAND stage
        - get hydrotable/branches. for each branch:
            - remove lake HydroIDs
            - inundate_sb() to create branch-level inundation tifs
        - mosaic_sb_inundation() to create lid/magnitude-level inundation tif
        - mask_out_lakes() 

        -> returns inundated site/mag tifs with lakes masked out

    Parameters # TODO: Update docstring
    ----------
    stage_val : float - The stage value to use for inundation mapping.
    datum_adj_ft : float - Datum adjustment in feet to be applied to the stage value.
    branch_dir : str - Directory containing branch subdirectories for processing.
    lid_usgs_elev : float - USGS elevation for the LID gage, used to calculate HAND stage.
    lid_altitude : float - Altitude adjustment for the LID location.
    fim_run_dir : str - Base directory for HAND FIM data.
    segments : list - List of NWM segment feature IDs to process for inundation.
    lid : str - Location ID string.
    # huc : str - Hydrologic Unit Code for the watershed.
    lid_directory : str - Directory to store output TIFF files for the LID.
    category : str - Category name for the inundation mapping (e.g., "action", "minor").
    category_key : str - Key string representing the category and magnitude.

    Returns
    -------
    messages : list - List of warning or status messages generated during processing.
    hand_stage : int - Computed HAND stage in millimeters.
    datum_adj_wse : float - Datum-adjusted water surface elevation in feet.
    datum_adj_wse_m : float - Datum-adjusted water surface elevation in meters.

    Notes
    -----
    - Branch-level TIFFs are mosaicked into a single extent TIFF for the LID and category.
    - Lakes are masked out from the final inundation array.
    - Intermediary branch TIFFs are deleted after merging to save space.
    - Logging is performed throughout for traceability and error handling.
    - If negative HAND stage or missing segments are detected, processing is skipped for those cases.

    '''

    # MP_LOG.MP_Log_setup(mp_parent_log_file, child_log_file_prefix)

    messages = [] # TODO: Decide if we're keeping the messages system

    # TODO: Decide if we want to implement this ID to FB CatFIM too?
    huc_lid_cat_id = f"{huc} : {ahps_site} : {magnitude}"

    # MP_LOG.trace(f"{huc_lid_cat_id} - Starting to create tifs") # TODO: Update logging
    print(f"{huc_lid_cat_id} - Starting to create tifs") # TODO: Update logging

    # ---------------------
    # Calculate HAND stage

    # Determine datum-offset water surface elevation
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
        messages.append(ahps_site + msg)
        # MP_LOG.warning(huc_lid_cat_id + msg) # TODO: Update logging
        print(huc_lid_cat_id + msg) # TODO: Update logging

        return messages, hand_stage, datum_adj_wse, datum_adj_wse_m

    # If no segments, write message and exit out
    # This has already been validated for records, not need to keep redoing it
    if not segments or len(segments) == 0:
        msg = ':missing nwm segments'
        messages.append(ahps_site + msg)
        # MP_LOG.warning(huc_lid_cat_id + msg) # TODO: Update logging
        print(huc_lid_cat_id + msg) # TODO: Update logging

        return messages, hand_stage, datum_adj_wse, datum_adj_wse_m

    # ---------------------
    # Get branches from FIM outputs

    branch_dir = os.path.join(fim_run_dir, huc, 'branches')

    # MP_LOG.trace(f"{huc_lid_cat_id} - Branch dir is {branch_dir}") # TODO: Update logging
    print(f"{huc_lid_cat_id} - Branch dir is {branch_dir}") # TODO: Update logging

    # Get branch list from FIM output directory
    branches = [x for x in os.listdir(branch_dir) if os.path.isdir(os.path.join(branch_dir, x))]
    branches.sort()

    # MP_LOG.trace(f"{huc_lid_cat_id} - Branches are {branches}") # TODO: Update logging
    print(f"{huc_lid_cat_id} - Branches are {branches}") # TODO: Update logging

    # ---------------------
    # Notes from previous MP
    # This WAS an MP in an MP. We want this set of mp's to roll up to the
    # parent MP file, and not the full catfim parent log. We roll this child MP into
    # it's parent mp and later that parent MP will rollup to the catfim file.

    # TODO: Decide on and implement multithreading
    # Jan 2026: We definatley want to take out multiprocessing, but maybe we can put in some multi-threading.
    # we might have to do some benchmark tests without MP or MT and come back to it.
    # It won't take much to drop in MT. MT does not require managing seperate logging files and can
    # easily pass objects, such as dataframes, back and forth.
    # Might be a great option here depends on volume and complexity of adding a "job number" system.
    # aka.. would we gain enough.. probably not.

    # Of course, we will need a branch iterator though

    # For now (Jan 15 2026) we will just run it single threaded, can implmement MT later if needed.

    # child_log_file_prefix = MP_LOG.MP_calc_prefix_name(MP_LOG.LOG_FILE_PATH, "MP_branch")
    # with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
    
    # ---------------------
    # Iterate through branches to produce the inundated branch tifs for the HAND stage
    
    for branch in branches:

        # Prepare branch-specific file paths

        msg_id_w_branch = f"{huc_lid_cat_id} : {branch}"
        # MP_LOG.trace(f"{msg_id_w_branch} - Inundating branch") # TODO: Update logging
        # print(f"{msg_id_w_branch} - Inundating branch") # TODO: Update logging

        # Define paths to necessary files to produce inundation grids.
        full_branch_path = os.path.join(branch_dir, branch)
        rem_path = os.path.join(fim_run_dir, huc, full_branch_path, 'rem_zeroed_masked_' + branch + '.tif')
        catchments_path = os.path.join(
            fim_run_dir,
            huc,
            full_branch_path,
            'gw_catchments_reaches_filtered_addedAttributes_' + branch + '.tif',
        )
        hydrotable_path = os.path.join(fim_run_dir, huc, full_branch_path, 'hydroTable_' + branch + '.csv')

        # NOTE: Jan 26 sometimes, these can fail to exist if a branchf initial failed during HAND generation
        # Do any of these ultimately change the sites gdf status / mapping columns? 
        # if so.. change it here (adjusting for the actual status message in the final library gpkg)

        if not os.path.exists(rem_path):
            msg = ":rem doesn't exist (could be bad branch)"
            # messages.append(ahps_site + msg)
            # MP_LOG.warning(msg_id_w_branch + msg) # TODO: Update logging
            print(msg_id_w_branch + msg) # TODO: Update logging
            continue
        if not os.path.exists(catchments_path):
            msg = ":catchments files don't exist (could be bad branch)"
            # messages.append(ahps_site + msg)
            # MP_LOG.warning(msg_id_w_branch + msg) # TODO: Update logging
            print(msg_id_w_branch + msg) # TODO: Update logging
            continue
        if not os.path.exists(hydrotable_path):
            msg = ":hydrotable doesn't exist (could be bad branch)"
            # messages.append(ahps_site + msg)
            # MP_LOG.warning(msg_id_w_branch + msg) # TODO: Update logging
            print(msg_id_w_branch + msg) # TODO: Update logging
            continue

        # Use hydroTable to determine hydroid_list from site_ms_segments.
        hydrotable_df = pd.read_csv(
            hydrotable_path, low_memory=False, dtype={'HUC': str, 'LakeID': float, 'subdiv_applied': int}
        )

        hydroid_list, lake_hydroid_list, nolake_hydroid_list = [], [], []

        # ---------------------
        # Determine hydroids at which to perform inundation, filter out lakes

        # TODO: segments here will need to be filtered to ahps_site specific. the segments file now has more columns

        for feature_id in segments:
            try:
                subset_hydrotable_df = hydrotable_df[hydrotable_df['feature_id'] == int(feature_id)]

                # List of HydroID's where the LakeID is greater than 0 (which shows that there's a lake)
                lake_hydroid_list = list(
                    subset_hydrotable_df.loc[subset_hydrotable_df['LakeID'] > 0]['HydroID'].unique()
                )

                # If lakes are detected, add info to the log
                if len(lake_hydroid_list) > 0:
                    # MP_LOG.trace( # TODO: Update logging
                    print(
                        f"HydroIDs {lake_hydroid_list} removed from processing because they contain lakes. FeatureId is {feature_id}."
                    )

                # List of HydroID's where there the LakeID is less than 0 (no lake, so we can inundate)
                nolake_hydroid_list = list(
                    subset_hydrotable_df.loc[subset_hydrotable_df['LakeID'] < 0]['HydroID'].unique()
                )

                # Add HydroIDs without lakes to the list to process
                hydroid_list += nolake_hydroid_list

            except IndexError:  # humm...
                # MP_LOG.trace( # TODO: Update logging
                print(
                    f"Index Error for {msg_id_w_branch}. FeatureId is {feature_id} : Continuing on."
                )
                pass

        # Create inundation maps with branch and stage data
        # NOTE: Only sites /categories that got this far are valid and can be inundated
        try:
            # MP_LOG.trace(f"{huc_lid_cat_id} : branch = {branch} :  Generating stage-based FIM")
            # print(f"{huc_lid_cat_id} : branch = {branch} :  Generating stage-based FIM (running inundate_sb)") # TODO: implement logging

            print(f"{huc_lid_cat_id} : {branch} - Producing inundated branch tifs")

            # executor.submit( # TODO: decide about keeping MP here. took out for now Jan 2026
                # inundate_sb,

            # Define output inundation extent tif path # TODO: Decide if there's anything about the name I want to change
            file_name = ahps_site + '_' + category_key + '_extent_' + huc + '_' + branch
            output_branch_tif = os.path.join(output_mapping_dir, file_name + '.tif')

            inundate_sb( # Jan 26: was previously called produce_inundated_branch_tif
                huc,
                ahps_site, # called lid inside function # TODO fix?
                hand_stage,
                rem_path,
                catchments_path,
                hydroid_list,
                output_branch_tif,
            )

        except Exception:
            # TODO: update sites.gdf ?
            msg = f':inundation failed at {magnitude} for branch'
            messages.append(ahps_site + msg)

            # MP_LOG.warning(msg_id_w_branch + msg) # TODO: Decide about MP, update logging
            # MP_LOG.error(traceback.format_exc()) # TODO: Decide about MP, update logging
            print(msg_id_w_branch + msg) # TODO: update logging
            print(traceback.format_exc()) # TODO: update logging

    # end of previous MP (removed Jan 2026)
    # end of branch loop

    # ---------------------
    # Mosaic inundation tifs for ahps_site/magnitude

    print(f"{huc} : {ahps_site} : {magnitude} - Starting to mosaic inundation")

    # NOTE: Jan 2026 - moved mosaic code into a function called mosaic_sb_inundation()
    # to match FB processing structure (analogous to Mosaic_inundation() function in FB CatFIM)
    
    # TODO: Maybe add a check that there are files to mosaic?

    output_extent_tif = mosaic_sb_inundation(ahps_site, 
                                             output_mapping_dir,
                                             category_key, 
                                             huc_lid_cat_id,
                                             huc,
                                             )

    print(f"{huc} : {ahps_site} : {magnitude} - Mosaic inundation completed")

    # ---------------------
    # Mask out lakes from inundated tif and re-save tif
    # Jan 2026 - moved lake masking to outside the mosaic function to match the fb processing

    print(f'{huc_lid_cat_id} - Masking out lakes from {output_extent_tif}') ## TEMP DEBUG

    with rasterio.open(output_extent_tif, 'r+') as output_extent_src:
        output_extent_array = output_extent_src.read(1)
        output_extent_array_masked, mask_status = mask_out_lakes(
            output_extent_array, huc, output_extent_src, fim_run_dir
        )
        output_extent_src.write(output_extent_array_masked, 1)


    print(f"{huc} : {ahps_site} : {magnitude} - Lake masking complete")

    if mask_status:
        print(f'{huc_lid_cat_id} - Masking status: {mask_status}') ## TODO: Update logging


    # else:
        # MP_LOG.warning(f"{huc}: {ahps_site}: Merging {category_key} : no valid inundated branches")
        # print(f"{huc}: {ahps_site}: Merging {category_key} : no valid inundated branches")

    if not os.path.exists(output_extent_tif): # TODO: Doublecheck that this is the way we want to check for success
        logging.error(f"{huc} : {ahps_site} : {magnitude} - FAILURE: map failed to create")
        # return

    return messages, hand_stage, datum_adj_wse, datum_adj_wse_m


def inundate_sb( # formerly called produce_inundated_branch_tif
    huc,
    lid,
    hand_stage,  # same as the "stage" column in the library df?, or maybe it is easier to just pass the value here
    rem_path,
    catchments_path,
    hydroid_list,
    output_branch_tif,
):
    '''
    Only used in stage-based CatFIM.

    Creates an inundated tif for a given site/magnitude/branch combination.

    Formerly called produce_inundated_branch_tif, updated Jan 2026 to match FB naming conventions.
    Analogous to Inundate_gms() in FB CatFIM.



    # Old docstring, needs to be simplified/clarified/updated # TODO: Update docstring

    This function reads a REM (Raster Elevation Model) and catchments raster, applies
    a stage threshold, and masks the result to catchments matching the provided hydroid
    list. The output is a raster where inundated cells are marked as 1 and non-inundated
    cells as 0.

    This is a form of inundation which is specific to CatFIM because we only have one
    flow value and the other FIM inundation tools are looking for flow files not single
    values.

    Parameters 
    ----------
    rem_path : str - Path to the REM raster file.
    catchments_path : str - Path to the catchments raster file.
    hydroid_list : list of int - List of hydroid identifiers to include in the mask.
    hand_stage : float or int - Stage threshold value for inundation.
    lid_directory : str - Directory where the output raster will be saved.
    category_key : str - Category key used in the output file name.
    huc : str - Hydrologic Unit Code for the region.
    lid : str - Location identifier for the site.
    branch : str - Branch identifier for the mapping.
    # parent_log_output_file : str - Path to the parent log output file for logging.
    # child_log_file_prefix : str - Prefix for child log files.

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

   
    Jan 2026 Development Notes:
    - need to update logging here and decide if we're returning messages or whatever
    - need to check that this function makes the correct assumptions about filepaths and folder structure
    
    '''

    try:
        # ---------------------
        # Set up logging and filepaths

        # This is setting up logging for this function to go up to the parent
        # MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix) # TODO: Update logging

        # file_name = lid + '_' + category_key + '_extent_' + huc + '_' + branch # will make and feed in tif name instead of this (to better match Inundate_gms)
        # output_tif = os.path.join(lid_directory, file_name + '.tif')

        # ---------------------
        # Inundate branch REM and mask out unwanted catchments

        # Open the REM and Catchments rasters (both have a nodata value of 0)
        rem_src = rasterio.open(rem_path)
        catchments_src = rasterio.open(catchments_path)
        rem_array = rem_src.read(1)
        catchments_array = catchments_src.read(1)

        # Adjust number dtype for output based on if Alaska or not
        is_alaska = str(huc)[:2] == '19'
        output_dtype = 'uint8' if is_alaska else 'int16'

        # Reclassify REM array based on hand_stage (to get inundated vs non-inundated cells)
        #   This creates an array where inundated cells are 1 and non-inundated cells are 0.
        #   Inundated cells occur where the cell elevation (rem_array) is less than the river 
        #   stage elevation (hand_stage).
        reclass_rem_array = np.where((rem_array <= hand_stage) & (rem_array != rem_src.nodata), 1, 0).astype(
            output_dtype
        )

        # Adjust hydroid values to mitigate differences in Alaska data structures.
        #   In our Alaska data, the catchment_array has hydroids that have had the first four 
        #   chars cut off, so we need to do the same for the hydroid's in the hydroid_list.
        clipped_hydroid_list = []
        for i in hydroid_list:
            clipped_str = str(i) if is_alaska else str(i)[-4:]
            clipped_hydroid_list.append(int(clipped_str))

        # Create a mask of the catchments_array where the values are in the clipped_hydroid_list.
        #   This mask will be used to remove lakes and catchments outside of the branch.
        hydroid_mask = np.isin(catchments_array, clipped_hydroid_list)

        # Use the hydroid_mask to create a new mask (target_catchments_array) containing cells that 
        #   are not nodata and the where hydroid_mask is True.
        #   This mask is identical to the hydroid_mask other than the fact that it also now removes
        #   nodata values and data type has been adjusted.
        target_catchments_array = np.where(
            ((hydroid_mask == True) & (catchments_array != catchments_src.nodata)), 1, 0
        ).astype(output_dtype)

        # Mask the reclass_rem_array with the target_catchments_array
        #   This creates an array where inundated cells are 1 and non-inundated cells are 0,
        #   and irrelevant areas are masked out.
        masked_reclass_rem_array = np.where(
            ((reclass_rem_array >= 1) & (target_catchments_array >= 1)), 1, 0
        ).astype(output_dtype)

        # ---------------------
        # Evaluate inundation result and save output tif if needed

        ## Debugging: # TODO: Clean up?

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

        # TODO: Update library df and maybe the sites gdf?

        if is_all_zero == False:
            with rasterio.Env():
                profile = rem_src.profile
                profile.update(dtype=rasterio.uint8)
                profile.update(nodata=0)

                # Replace any existing nodata values with the new one
                # masked_reclass_rem_array[masked_reclass_rem_array == profile["nodata"]] = 0

                with rasterio.open(output_branch_tif, 'w', **profile) as dst:
                    dst.write(masked_reclass_rem_array, 1)

                print(f'{huc} : {lid} - inundate_sb - Saving tif to {output_branch_tif}')

        # else: # is_all_zero = True:
                # print(f'inundate_sb - not saving tif because masked array was all zero') # Update logging

    except Exception:
        # MP_LOG.error(f"{huc} : {lid} Error producing inundation maps with stage") # TODO: Update logging
        # MP_LOG.error(traceback.format_exc()) # TODO: Update logging
        print(f"{huc} : {lid} Error producing inundation maps with stage") # TODO: Update logging
        print(traceback.format_exc()) # TODO: Update logging

    return


def mosaic_sb_inundation(
    lid, 
    output_mapping_dir,
    category_key,
    huc_lid_cat_id,
    huc,
):
    """
    Mosaics all branch inundation tifs for a given lid/magnitude combination into a single extent tif.

    Jan 2026 - turned into a new function, previously was just loose code in run_sb_inundation

    NOTE: This section is analogous to Mosaic_inundation() function in FB CatFIM

    Jan 2026 Development Notes:
    - need to update logging here and decide if we're returning messages or whatever
    - need to check that this function makes the correct assumptions about filepaths and folder structure
    - need to check that we are removing the correct intermediate files (and maybe decide if there's any change to that philolophy we want to make)
    
    
    """

    # Merge all rasters in output_mapping_dir that have the same magnitude/category.
    path_list = []

    # We are looking for the branch files for the category/stage (or any given stage interval)

    lid_dir_list = [x for x in os.listdir(output_mapping_dir) if category_key in x]
    lid_dir_list.sort()  # To force branch 0 first in list, sort

    for f in lid_dir_list:
        path_list.append(os.path.join(output_mapping_dir, f)) # TODO: Could these 4 lines be simplified?

    print(f"{huc_lid_cat_id} - Merging branch files") # TODO: Update logging
    print(f"{huc_lid_cat_id} - LID dir list is: {lid_dir_list}") # TODO: Update logging

    # Merging all of the branch tifs into one lid_category tif
    if len(lid_dir_list) > 0:
        zero_branch_grid = path_list[0]
        zero_branch_src = rasterio.open(zero_branch_grid)
        zero_branch_array = zero_branch_src.read(1)
        summed_array = zero_branch_array  # Initialize it as the branch zero array

        output_extent_tif = os.path.join(output_mapping_dir, lid + '_' + category_key + '_extent.tif')
        # MP_LOG.trace(f"{huc_lid_cat_id}: Merging all branches into output file to be saved as {output_extent_tif}") # TODO: Update logging

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

        # del zero_branch_array  # Clean up

        # Save summed array
        profile = zero_branch_src.profile
        summed_array = summed_array.astype('uint8')
        with rasterio.open(output_extent_tif, 'w', **profile) as dst:
            dst.write(summed_array, 1)

            print(f"{huc_lid_cat_id} - Branch rollup extent file saved at {output_extent_tif}") # TODO: Update logging
            # MP_LOG.lprint(f"{huc_lid_cat_id}: Branch rollup extent file saved at {output_extent_tif}") # TODO: Update logging

        # # Mask out the lakes from the inundation array 
        # # Jan 2026 - Moved lake masking to outside the mosaic function to match the fb processing
        # summed_masked_array, mask_status = mask_out_lakes(summed_array, huc, zero_branch_src, fim_run_dir)


        # -----------------------
        # Clean up old branch tifs

        # For space reasons, we need to delete all of the intermediary files such as:
        #    Stage: grmn3_action_extent_0.tif, grmn3_action_extent_1933000003.tif. The give aways are a number before
        #        the .tif
        #    Flows: allm1_action_12p0ft_extent_01010002_0.tif, allm1_action_12p0ft_extent_01010002_7170000001.tif
        #       your give away is to just delete any file that has the HUC number in teh file name
        # The intermediatary are all inundated branch tifs.
        #
        # The ones we want to keep end at _extent.tif and remove ones that have _extent_*.tif

        # MP_LOG.lprint(f"{huc_lid_cat_id} - Removing interium inundated branch files") # TODO: Update logging
        print(f"{huc_lid_cat_id} - Removing interium inundated branch files") # TODO: Update logging

        branch_tifs = glob.glob(f"{output_mapping_dir}/{lid}_{category_key}_extent_*.tif")
        for tif_file in branch_tifs:
            os.remove(tif_file)

        return output_extent_tif #, logs? # TODO: decide what to return, maybe logs?


def post_process_huc(
    huc,
    huc_library_df,
    output_mapping_dir,
    catfim_type,
):
    '''
    Used in both flow-based and stage-based CatFIM.

    Post-processes inundation mapping results for a given HUC.

    # TODO: Update docstrings
    # TODO: humm.. becuase we have the library.gpkg at this point and it knows which rec are
    # intervals versuses not.. is there an optimizaion option here?

    '''

    # Note: child_log_file_prefix is "MP_post_process_{huc}", meaning all logs created by this function start
    #  with the phrase "MP_post_process_{huc}". This one rollups up to the master catfim log
    # This is setting up logging for this function to go up to the parent


    try:

        # -----------------------
        # Iterate through tifs

        # Get a list of tifs (files ending with "extent.tif" means it is a rolled up version)
        tif_list = [x for x in os.listdir(output_mapping_dir) if ('extent.tif') in x]

        # Make the full filepaths
        tifs_to_reformat_list = []
        for tif in tif_list:
            tifs_to_reformat_list.append(os.path.join(output_mapping_dir, tif))

        if len(tifs_to_reformat_list) == 0:
            print(f">> no tifs found for {huc} at {output_mapping_dir}") # TODO: Update logging

            return None # TODO: What do we return in this case?

        # Iterate through the saved tifs to make a list of inundated multipolygons
        reformatted_geom_list = []

        for tif_to_process in tifs_to_reformat_list:
            # If not os.path.exists(tif_to_process):
            #    continue

            # If stage based, the file names looks like this:
            #      masm1_major_extent.tif  (non-interval, whole number) # TODO: Double check that this is still true
            #      masm1_major_20.6_extent.tif  (non-interval, float)
            #      masm1_major_20.0fti_extent.tif (interval)
            #
            # If flow based, the file name looks like this: 
            #      masm1_action_extent.tif

            # MP_LOG.trace(f".. Tif to Process = {tif_to_process}")
            try:

                # Get site, magnitude, and interval data from the tif name
                tif_file_name = os.path.basename(tif_to_process)
                file_name_parts = tif_file_name.split("_")
                nws_lid = file_name_parts[0]
                magnitude = file_name_parts[1]

                # Check whether the tif is an interval (indicated by "fti" in the file name)
                # (carefully, we only check part 3 because "ft" can be part of the site name)
                interval_stage = None
                is_interval = False
                if len(file_name_parts) >= 3 and "fti" in file_name_parts[2]:
                    try:
                        stage_val = file_name_parts[2].replace("fti", "")
                        interval_stage = float(stage_val) # TODO: make these two lines into one line? -> interval_stage = float(file_name_parts[2].replace("fti", ""))
                        is_interval = True

                    except ValueError:
                        interval_stage = None
                        # MP_LOG.error( # TODO:Update logging
                        print(
                            f"Error differentiating intervals from non-interval values for tif {tif_file_name}"
                        )
                        # MP_LOG.error(traceback.format_exc()) # TODO: Update logging
                        print(traceback.format_exc()) 


                # Convert the inundation raster tif into a dissolved inundation multipolygon
                # TODO: humm.. is there an optimization available here instead of processing each tif / mag at a time?
                extent_poly_diss = reformat_inundation_maps(
                        huc,
                        nws_lid,
                        magnitude,
                        tif_to_process,
                        interval_stage,
                        is_interval,
                )

                # Append the inundation multipolygon to the list
                reformatted_geom_list.append(extent_poly_diss)
                
            except Exception:
                # MP_LOG.error( # TODO: Update logging
                print(
                    f"An ind reformat map error occured for {huc} - {nws_lid} - magnitude {magnitude}"
                )
                # MP_LOG.error(traceback.format_exc()) # TODO: Update logging
                print(traceback.format_exc())

        # Make inundated multipolygon list into a dataframe
        reformatted_geom_list_df = pd.concat(reformatted_geom_list, ignore_index=True)

        # Join the inundated multipolgyon dataframe to the HUC library dataframe
        huc_library_df = huc_library_df.merge(reformatted_geom_list_df, on=['nws_lid', 'magnitude', 'interval_stage'], how='left') # TODO: test that this works for FB and SB

        # Make the HUC library df into a gdf
        huc_library_gdf = gpd.GeoDataFrame(huc_library_df, geometry='geometry', crs=VIZ_PROJECTION)

        # -----------------------
        # Dissolve based on site and magnitude (and interval stage?? TODO)
        # TODO: I'm not convinced that dissolving will change things in most cases... but maybe worth 
        # keeping for now? 

        print(f"{huc} - Post-Process HUC - Dissolving CatFIM library") # TODO: Update logging

        huc_library_undissolved_gdf = huc_library_gdf # TODO: might not need to output this after development, saving it just for now  TEMP DEBUG 

        # TODO: Decide if we need to differentiate by CatFIM type here or if we can just process all as one
        # Previous methods only had dissolving for flow-based, probably because the intervals could make
        # things confusing...
        if catfim_type == "fb":
            huc_library_gdf = huc_library_gdf.dissolve(by=['nws_lid', 'magnitude'], as_index=False)

        elif catfim_type == "sb": # prev versions did not dissolve SB... for good reason? or no?
            huc_library_gdf = huc_library_gdf.dissolve(by=['nws_lid', 'magnitude', 'interval_stage'], as_index=False)

        if len(huc_library_gdf) == 0:
            print(f"{huc} - Post-Process HUC - WARNING: Dissolved library empty") # TODO: Update logging

        if 'level_0' in huc_library_gdf: # TODO: Decide if we wanna keep this
            huc_library_gdf = huc_library_gdf.drop(['level_0'], axis=1)

    except Exception:
        # MP_LOG.error(f"An error has occurred in post processing for {huc}") # TODO: Update logging
        print(f"An error has occurred in post processing for {huc}")  # TODO: Update logging
        # MP_LOG.error(traceback.format_exc()) # TODO: Update logging
        print(traceback.format_exc()) # TODO: Update logging

    return huc_library_undissolved_gdf, huc_library_gdf


def reformat_inundation_maps(
    huc,
    nws_lid,
    magnitude,
    tif_to_process,
    interval_stage,
    is_interval,
):
    '''
    Used in both flow- and stage-based CatFIM.

    Convert the inundation raster tif into a dissolved inundation multipolygon.

    # TODO: Update docstring

    '''

    # Note: child_log_file_prefix is "MP_reformat_tifs_{huc}", meaning all logs created by this
    # function start with the phrase will rollup to the master catfim logs

    # This is setting up logging for this function to go up to the parent
    # MP_LOG.MP_Log_setup(parent_log_output_file, child_log_file_prefix) # TODO: Update logging

    try:
        # MP_LOG.trace( # TODO: Update logging
        print(
            f"{huc} : {nws_lid} : {magnitude} - Converting inundated tif to multipolygon"
        )

        # Convert raster to shapes
        with rasterio.open(tif_to_process) as src:
            image = src.read(1)
            mask = image > 0

        # Aggregate shapes
        results = (
            {'properties': {'extent': 1}, 'geometry': s}
            for i, (s, v) in enumerate(shapes(image, mask=mask, transform=src.transform))
        )

        # If no inundated shapes were created from the tifs, log a message and return
        list_results = list(results)
        if len(list_results) == 0:
            # MP_LOG.error( # TODO: Update logging
            print(
                f"{huc} : {nws_lid} : {magnitude} - No values above zero in inundated tif, "
                "so zero inundated shapes were found. See GitHub issue #1491 for details." # TODO: Is this GitHub issue still active? make sure error msg is up-to-date
            )
            return

        # Convert list of shapes to polygon
        extent_poly = gpd.GeoDataFrame.from_features(list_results, crs=src.crs)

        # Dissolve polygons
        extent_poly_diss = extent_poly.dissolve(by='extent')

        # Update attributes
        extent_poly_diss = extent_poly_diss.reset_index(drop=True)
        extent_poly_diss['nws_lid'] = nws_lid 
        extent_poly_diss['magnitude'] = magnitude
        extent_poly_diss['interval_stage'] = interval_stage
        extent_poly_diss['is_interval'] = is_interval
        # extent_poly_diss['huc'] = huc # TODO: Clean up if I decide it's not needed

        # Project to Web Mercator
        extent_poly_diss = extent_poly_diss.to_crs(VIZ_PROJECTION)

        # Convert the features to multipolygon if needed
        extent_poly_diss["geometry"] = [
            MultiPolygon([feature]) if type(feature) is Polygon else feature
            for feature in extent_poly_diss["geometry"]
        ]

        # if not extent_poly_diss.empty:
            # print(f"{huc} : {nws_lid} : {magnitude} Added geometry to library_df") # TODO: Update logging or clean up
        # else:
            # print(f"{huc} : {nws_lid} : {magnitude} tif to gpkg, geodataframe is empty") # TODO: Update logging or clean up

    except ValueError as ve:
        msg = f"{huc} : {nws_lid} : {magnitude} - Reformatted inundation map"
        if "Assigning CRS to a GeoDataFrame without a geometry column is not supported" in ve:
            print(f"{msg} - Warning: details: {ve}") # TODO: Update logging
        else:
            print(f"{msg} - Exception") # TODO: Update logging
            print(traceback.format_exc()) # TODO: Update logging

    except Exception:
        print(f"{huc} : {nws_lid} : {magnitude} - Reformatted inundation map - Exception") # TODO: Update logging
        print(traceback.format_exc()) # TODO: Update logging

    return extent_poly_diss


def __calculate_category_key(category, stage_value, is_interval_stage):
    '''
    Used in stage-based CatFIM.

    Calcuates the category key which becomes part of a file name
    Changes to an int if whole number only. ie.. we don't want 22.00 but 22.0, but keep 22.15
    category_key comes things like this: action, action_24.0ft, or action_24.6ft

    Args:
        category (str): The flood category (e.g., 'action', 'minor', etc.).
        stage_value (float): The stage value for the category.
        is_interval_stage (bool): Whether this is an interval stage (True) or a main stage (False).

    Returns:
        category_key (str): Category and stage value string for file naming.

    TODO: yes... this needs a better answer.
    '''
    category_key = category + "_"  # ie) action_

    if float(stage_value) % 1 == 0:  # then we have a whole number
        # then we will turn it into a int and manually add ".0" on it
        category_key += str(int(stage_value)) + ".0"
    else:
        category_key += "{:.2f}".format(stage_value)

    category_key += "ft"

    # The "i" in the end means it is an interval
    # Now we are action_24.0ft or action_24.6ft or action_24.65ft or action_24.0fti
    if is_interval_stage == True:
        category_key += "i"

    return category_key


def __calc_sb_intervals( # Was __calc_stage_intervals()
    non_rec_thresholds_df_site, # was non_rec_stage_values_df, 
    past_major_interval_cap, 
    huc_lid_id
): 
    '''
    Used in stage-based CatFIM.

    Calculate stage intervals for inundation mapping based on non-recurrent stage values.
    This function generates a list of intervals between stage values, rounding up to the next whole number
    where necessary, and ensures that intervals are unique and in order. For each stage, it determines the
    range of integer depths to be used for inundation calculations, up to the next stage or a specified cap
    for the last stage.

    Args:
        non_rec_thresholds_df_site (pd.DataFrame): DataFrame containing stage names and their corresponding stage values.
            Must have columns "magnitude_type" and "magnitude_value".
        past_major_interval_cap (int): The number of intervals to add beyond the last stage value.
        huc_lid_id (str): Identifier used for logging and tracing.

    Returns:
        list: A list of lists, where each sublist contains a stage name and an integer interval value,
              e.g., [["action", 21], ["action", 22], ...]. This represents the stage names and depths
              to be used for inundation mapping.

    TODO: Would be good to rethink how we calculate intervals and improve.
    
    '''
    interval_recs, stage_values_claimed = [], []

    print( # TODO: Update logging
        f"{huc_lid_id}: Calculating intervals for {non_rec_thresholds_df_site}"
    )

    num_stage_value_recs = len(non_rec_thresholds_df_site)
    print(f"{huc_lid_id}: num_stage_value_recs is {num_stage_value_recs}") # TODO: Update logging

    # Calculate the intervals for each magnitude in the df
    # Note: Records will be in order by stage value. We calculate intervals one magnitude at a time
    # so we can keep track of the magnitude name associated with the interval.
    for idx in non_rec_thresholds_df_site.index:

        row = non_rec_thresholds_df_site.loc[idx]
        cur_magnitude_name = row["magnitude_type"] # was cur_stage_name
        cur_stage_val = row["magnitude_value"]

        # MP_LOG.trace(f"{huc_lid_id}: interval calcs - non_rec_stage_value is idx: {idx}; {row}")

        # Calculate the intervals betwen the current and the next stage value.
        # For the current val, we need to round up because it is possible for stages to be decimals
        # (for example, if action is 2.4, and mod is 4.6, we want intervals at 3 and 4).
        # The highest value of the interval_list is not included # TODO: double check/reprase... I think what we mean here is that the max interval isn't included?

        # Create the minimum interval value
        # Note: We check whether the current stage val is an integer (i.e. 12.0) or a decimal (12.6)
        # because the intervals are integers and this helps us avoid duplicates.
        if float(cur_stage_val) % 1 == 0:
            # If it IS an integer, mark value as "claimed" and add 1 to create the minimum interval value
            # (Example: 3 -> 3 + 1 -> 4) 
            cur_stage_val = int(cur_stage_val)
            stage_values_claimed.append(cur_stage_val)
            min_interval_val = int(cur_stage_val) + 1
        else:
            # If it IS NOT an integer, round up to next whole number and add 1 to create the minimum interval value
            # (Example: 3.14 -> 4 + 1 -> 5)
            min_interval_val = math.ceil(cur_stage_val) + 1

        # Create the maximum interval value
        if idx < len(non_rec_thresholds_df_site) - 1:
            # If the record IS NOT the last stage value in the list, use the next stage value
            # as the maximum interval value here.
            next_stage_val = non_rec_thresholds_df_site.iloc[idx + 1]["stage_value"]
            max_interval_val = int(next_stage_val)
            # MP_LOG.trace(f"{huc_lid_id}: Next stage value is {max_interval_val}")

        else:
            # If the record IS the last record for the site, calculate the maximum interval 
            # value as the minimum interval value + the interval cap (default is 5).
            max_interval_val = int(min_interval_val) + past_major_interval_cap
            # MP_LOG.trace(f"{huc_lid_id}: Last rec and max_in is {max_interval_val}")

            # + 1 as the last interval is not included # TODO: Is this +1 actually implemented anywhere?

        # MP_LOG.lprint(f"{huc_lid_id}: {cur_magnitude_name} is {cur_stage_val} and"
        #               f"  min_interval_val is {min_interval_val} ; max interval value is {max_interval_val}")

        # Take the minimum and maximum interval values and get a list of the whole nummbers that
        # occur between them (Example: np.arange(1, 5) -> [1, 2, 3, 4]).
        interval_list = np.arange(min_interval_val, max_interval_val)

        # Add intervals to the output list (if they are not already claimed)
        # Previously some duplicate values would slip throguh, the stage_values_claimed
        # functionality mitigates that problem. 
        for int_val in interval_list:
            if int_val not in stage_values_claimed:
                interval_recs.append([cur_magnitude_name, int_val])
                # MP_LOG.trace(f"{huc_lid_id}: Added interval value of {int_val}")
                stage_values_claimed.append(int_val)

    # MP_LOG.lprint(f"{huc_lid_id} interval recs are {interval_recs}")

    return interval_recs


def __load_mapping_data(
    huc_path,
    sites_mapping_file_path,
    segments_file_path,
    library_pre_inun_file_path
):
    """
    Used for both SB and FB.
 
    Load data needed for CatFIM mapping. 
    It does not load discharge here as only FB needs that

    Jan 2026: This is new and needed.

    """

    # --------------------------
    # Load the temp sites_gdf. This is a copy for usage in mapping
    # It can be updated and later, catfim_process_huc.py will pick it up and reload it
    if not os.path.isfile(sites_mapping_file_path):
        raise Exception(f"Missing file, expected {sites_mapping_file_path}")
    sites_gdf = gpd.read_file(sites_mapping_file_path, engine='fiona')
    if len(sites_gdf) == 0:
        raise Exception("site_gdf should not be empty")

    # --------------------------
    # Load segments file.. they both need it
    segments_file_path = os.path.join(huc_path, "features_segments.csv")
    if not os.path.isfile(segments_file_path):
        raise Exception(f"Missing file, expected {segments_file_path}")
    huc_segments_df = pd.read_csv(segments_file_path)
    if len(huc_segments_df) == 0:
        raise Exception("segments file should not be empty by this point")

    # --------------------------
    # Load up the library csv that has been building up so far
    # This is not a gpkg yet, but a df instead, as it has no geometry yet.
    if not os.path.isfile(library_pre_inun_file_path):
        raise Exception(f"Missing file, expected {library_pre_inun_file_path}")
    huc_library_df = pd.read_csv(library_pre_inun_file_path)
    if len(huc_segments_df) == 0:
        raise Exception("segments file should not be empty by this point.")
    
    return sites_gdf, huc_library_df, huc_segments_df


def __validate_mapping_data(huc, sites_gdf, huc_library_df, huc_segments_df):
    """
    Used for both stage- and flow-based CatFIM.

    validate any inputs (such as branches maybe) when reasonable 
    so we can abort sooner than later. Not sure they is anything we can do

    """

    validation_pass = True
    validation_messages = []

    # Note sites where mapped = no
    # (sites that are unmapped but mappable will have the value 'not yet' in their mapped column)
    sites_no_mapping = sites_gdf.loc[sites_gdf['mapped'] == 'no']['nws_lid'].to_list()
    sites_no_mapping_status = sites_gdf.loc[sites_gdf['mapped'] == 'no']['status'].to_list()

    if len(sites_no_mapping) > 0:
        msg = f'{huc} - Mapping - The following sites will not be mapped:'
        validation_messages.append(msg)

        for index, site in enumerate(sites_no_mapping):
            msg = f"{site} - {sites_no_mapping_status[index]}"
            validation_messages.append(msg)

    # Give an error if important inputs are empty
    if len(sites_gdf) == 0:
        msg = f"{huc} - Mapping - WARNING: Sites GDF is empty."
        validation_messages.append(msg)
        validation_pass = False

    if len(huc_library_df) == 0:
        msg = f"{huc} - Mapping - WARNING: HUC library DF is empty."
        validation_messages.append(msg)
        validation_pass = False

    if len(huc_segments_df) == 0:
        msg = f"{huc} - Mapping - WARNING: HUC segments DF is empty."
        validation_messages.append(msg)
        validation_pass = False

    return validation_pass, validation_messages


if __name__ == '__main__':
    """
    TODO: Repair command line functionality
    TODO: Clean up docstring

    Sample
    python /foss_fim/tools/catfim/generate_categorical_fim_mapping.py -u 12090301 -t /data/catfim/hand_4_8_7_2

    All files shoudl be in place such as the sites_gdf, threshold data files
    and begining library data for this huc

    Files required are:  __________________________ (list them here)
        {huc}_sites.gdf
        flow_discharge.csv  (if flow based)
        {huc}_library_threshold.csv  (TODO: seems like a bad file name)
        others?

    Most args will be in the runtime_arg.env created in the generate_categorical_fim.py
    This script will already know where to look for the runtime_args.env file

    We need only the huc number and the output path for args

    Note: We always want to overwrite.

    """

    # Parse arguments
    parser = argparse.ArgumentParser(description='Categorical inundation mapping for FOSS FIM.')
    parser.add_argument("-u", "--huc", help="REQUIRED: HUC8 Number", required=True, type=str)
    parser.add_argument(
        '-t',
        '--output-folder',
        help='REQUIRED: Target location, Where the output folder will be.'
        'ie /data/catfim/hand_4_8_7_2 or /data/catfim/test/test1',
        required=True,
    )

    args = vars(parser.parse_args())

    catfim_mapping(**args)


