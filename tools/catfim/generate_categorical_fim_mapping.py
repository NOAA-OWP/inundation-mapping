#!/usr/bin/env python3

import argparse
import glob
import logging
import math
import os
import shutil
import sys
import traceback
import subprocess
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

This script focuses on handling HUC-level inundation for both flow- and stage-based CatFIM.
It intakes flows and FIM pipeline outputs and produces inundated TIFs and GPKGs for the
appropriate NWM reaches.

"""

gpd.options.io_engine = "pyogrio"


# Main function for CatFIM mapping processing for a HUC
def process_mapping(
    huc,
    huc_path,
    catfim_type,
    output_mapping_dir,
    output_temp_dir,
    sites_pre_mapping_file_path,
    sites_post_mapping_file_path,
    library_pre_mapping_file_path,
    library_post_mapping_file_path,
):
    '''
    Handles the CatFIM mapping for a HUC.

    Everything related to mapping starts here. SB and FB run it from catfim_process_huc or it can be
    run via the command line with the catfim_mapping function.

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code.
    huc_path - STR
        Path to HUC-specific directory. (i.e. {fim_run_dir}/{huc})
    catfim_type - STR
        Stage- or flow-based indicator. (i.e. 'sb' or 'fb')
    output_mapping_dir - STR
        Path to HUC-specific mapping directory. (i.e. {huc_path}/mapping/)
    output_temp_dir - STR
        Path to HUC-specific mapping directory. (i.e. {huc_path}/temp/)
    sites_pre_mapping_file_path - STR
        Path to HUC-specific sites file (before mapping).
    sites_post_mapping_file_path - STR
        Path to HUC-sepcific sites file (after mapping).
    library_pre_mapping_file_path - STR
        Path to the the csv / df version of the library before we add geometry.
    library_post_mapping_file_path - STR
        Path to the the gpkg version of the library (once we've added geometry).

    '''

    # Add its own duration system and section start and close messages
    section_start_dt = datetime.now(timezone.utc)
    logging.info(f"{huc} - Mapping - Starting CatFIM HUC-level mapping")

    # Initialize values
    sites_gdf, huc_library_df, huc_library_gdf = None, None, None

    # --------------------------
    # Load and validate mapping data

    try:
        logging.info("")
        logging.info(f"{huc} - Mapping - Loading mapping data for HUC")

        sites_gdf, huc_library_df, huc_segments_df, fim_run_dir = __load_validate_mapping_data(
            huc, huc_path, sites_pre_mapping_file_path, library_pre_mapping_file_path
        )

    except Exception as ex:
        logging.critical(
            f"{huc} - Mapping - A critical error occurred while loading HUC mapping inputs: {ex}"
        )
        logging.critical(traceback.format_exc())

    # --------------------------
    # Split to SB- and FB-specific mapping

    try:
        logging.info("")
        if catfim_type == "sb":
            logging.info(f"{huc} - Mapping - Beginning stage-based mapping")

            # Jan 26 - New function
            sites_gdf, huc_library_df = run_sb_mapping(  # need reg output path?
                huc, huc_path, sites_gdf, huc_library_df, output_mapping_dir, fim_run_dir, huc_segments_df
            )

        elif catfim_type == "fb":
            logging.info(f"{huc} - Mapping - Beginning flow-based mapping")

            # Jan 26 - Renamed run_catfim_inundation() to run_fb_mapping()
            sites_gdf, huc_library_df = run_fb_mapping(
                huc, huc_path, sites_gdf, huc_library_df, output_mapping_dir, fim_run_dir, output_temp_dir
            )

    except Exception as ex:
        logging.critical(f"{huc} - Mapping - A critical error occurred while running mapping: {ex}")
        logging.critical(traceback.format_exc())

    # At this point, we will have the inundated tifs for each valid site/magnitude combination
    # and the updated sites_gdf and huc_library_df.

    # -----------------------------
    # Post-process HUC
    # Note: Got rid of the post_process_cat_fim_for_viz function and just call post_process_huc_mapping() here

    try:
        logging.info("")
        logging.info(f"{huc} - Mapping - Post-processing HUC mapping")
        sites_gdf, huc_library_gdf = post_process_huc_mapping(
            huc, catfim_type, sites_gdf, huc_library_df, output_mapping_dir
        )

    except Exception as ex:
        logging.critical(f"{huc} - Mapping - A critical error occurred during mapping post-processing:")
        logging.critical(f"Exception: {ex}")
        logging.critical(traceback.format_exc())

    # -----------------------------
    # Save the HUC-level mapping outputs
    try:
        logging.info(" ")
        logging.info(f"{huc} - Mapping - Saving HUC outputs post-mapping")

        __save_huc_outputs_post_mapping(
            huc, sites_gdf, huc_library_gdf, sites_post_mapping_file_path, library_post_mapping_file_path
        )

    except Exception as ex:
        logging.critical(f"{huc} - Mapping - A critical error occurred while saving mapping outputs: {ex}")
        logging.critical(traceback.format_exc())

    # -----------------------------
    logging.info(" ")
    duration_msg = sf.calculate_duration_msg(section_start_dt)
    logging.info(f"{huc} - Mapping - End of CatFIM HUC-level mapping - {duration_msg}")
    logging.info(" ")

    return


def run_fb_mapping(
    huc, huc_path, sites_gdf, huc_library_df, output_mapping_dir, fim_run_dir, output_temp_dir
):
    '''
    Only used in flow-based CatFIM.
    Runs for each HUC.

    Processes
    ---------
    - Gets a list of sites in HUC
    - Loops through sites. For each site:
        - Gets list of magnitudes
        - Creates filepaths
        - Runs run_fb_inundation() for each site/magnitude combination to create inundation tifs
            (using Inundate_gms, Mosaic_inundation, and mask_out_lakes)

    Analogous to run_sb_mapping() for stage-based CatFIM. Different from SB in that
    this function iterates by site/magnitude whereas run_sb_mapping() iterates by site.

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code.
    huc_path - STR
        Path to the HUC-specific CatFIM output file.
    sites_gdf - Geopandas GeoDataFrame
        Table of sites and data for the given HUC.
    huc_library_df - Pandas DataFrame
        Table of site/magnitude combinations for the given HUC.
    output_mapping_dir - STR
        Filepath to the HUC-specific mapping folder.
    fim_run_dir - STR
        Filepath to the HUC-specific mapping folder.
    output_temp_dir - STR
        Filepath to the HUC-specific temp folder.

    Returns
    -------
    sites_gdf - Geopandas GeoDataFrame
        Table of sites and data for the given HUC, updated.
    huc_library_df - Pandas DataFrame
        Table of site/magnitude combinations for the given HUC, updated.

    CatFIM Reorg Notes (Jan 26)
    --------------------------
    This function was previously named run_catfim_inundation but was renamed to run_fb_mapping for clarity
    (because it is only used in flow-based CatFIM).

    '''

    inundate_hand = bool(os.getenv("INUNDATE_HAND"))
    inundate_hr = bool(os.getenv("INUNDATE_HR"))
    hr_preference = bool(os.getenv("HR_PREFERENCE"))


    logging.info(" ")
    logging.info(f"{huc} - Mapping -  Start inundating and mosaicing...")

    logging.info(f"Inundate HAND: {inundate_hand}; Inundate HEC-RAS: {inundate_hr}")

    # -----------------------
    # Get list of AHPS sites in HUC from the sites GDF (excluding sites where mapped = no, we only
    # want to map sites where mapped = not yet)
    ahps_sites_list = sites_gdf[sites_gdf['mapped'] != 'no']['nws_lid'].tolist()
    logging.info(f"{huc} - Mapping - ahps_sites_list is {ahps_sites_list}")

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
        value_name='magnitude_value',
    )

    # Remove rows where magnitude_value is -1.0 (THRESH_NODATA_VALUE) these are where it was NaN in the database
    huc_thresholds_long_df = huc_thresholds_long_df[
        huc_thresholds_long_df['magnitude_value'] != csf.THRESH_NODATA_VALUE
    ]

    # Loop through AHPS sites
    for ahps_site in ahps_sites_list:

        logging.info(" ")
        logging.info(f"{huc} : {ahps_site}")

        hr_site_tifs_produced = bool(False)  # initialize value for site
        inundate_hand_for_site = bool(False)  # initialize value for site

        # Get a list of available magnitudes
        huc_thresholds_long_df_site = huc_thresholds_long_df[huc_thresholds_long_df['nws_lid'] == ahps_site]
        magnitude_list = huc_thresholds_long_df_site['magnitude_type'].to_list()

        # Loop through thresholds/magnitudes and define inundation output files paths
        logging.info(f"{huc} : {ahps_site} - Magnitude list is {magnitude_list}")

        # Determine whether to inundate HEC-RAS for the site
        if inundate_hr is True:
            # TODO: Do we also want to check that the inputs were made successfully here?

            # Get model list from huc_controls_df
            huc_models_df = pd.read_csv(os.path.join(huc_path, f"{huc}_hr_sites_models.csv")) # TODO: Decide on permanent path
            sites_models_df = huc_models_df[huc_models_df['nws_lid'] == ahps_site]

            # Check whether there are HEC-RAS models available for the site
            if len(sites_models_df) > 0:
                # If there's sites available, run HR
                inundate_hr_for_site = bool(True)

            elif len(sites_models_df) == 0:
                inundate_hr_for_site = bool(False)
                logging.info(f"{huc} : {ahps_site} - No HEC-RAS models found for site")

        if inundate_hr_for_site is True:
            logging.info(" ")
            logging.info(f"{huc} : {ahps_site} - HEC-RAS inundation...")

            flows2fim_path = "/projects/catfim_hecras_fb/flows2fim_030/flows2fim" # TODO: pull from .env
            model_list = sites_models_df[sites_models_df['nws_lid'] == ahps_site]['model_collection'].unique().tolist()

            logging.info(f"{huc} : {ahps_site} - Found the following model(s): {model_list}")

            # Iterate through magnitudes and models
            for model_name in model_list:
                for magnitude in magnitude_list:
                    if "." in magnitude:
                        continue

                    logging.info(" ")
                    logging.info(f"{huc} : {ahps_site} - {magnitude}")
                    logging.info(f'Inundating HEC-RAS tifs for {ahps_site} - {magnitude} - {model_name}...')

                    # TODO: do we want to subset the HUC controls stuff here?

                    # Get site/magnitude/model-specific controls CSV
                    controls_filename = f"{ahps_site}_{magnitude}_{model_name}_controls.csv"
                    controls_csv = os.path.join(output_temp_dir, controls_filename) # TODO: update controls_path to be the temp folder path?

                    huc_controls_csv_path = os.path.join(huc_path, f"{huc}_controls.csv")
                    huc_controls_df = pd.read_csv(huc_controls_csv_path)

                    # Get the value of the collection_parent_folder column in the huc_controls_df for the rows matching the site/magnitude/model combination (there should only be one unique value, but just in case, we'll take the first one)
                    collection_parent_folder = huc_controls_df[
                        (huc_controls_df['nws_lid'] == ahps_site) &
                        (huc_controls_df['magnitude'] == magnitude) &
                        (huc_controls_df['model_collection'] == model_name)
                    ]['collection_parent_folder'].unique()[0]

                    # Get the path to the library extent
                    library_extent_path = os.path.join(collection_parent_folder, 'collections', model_name, 'library_extent')

                    # Define output inundation extent tif path
                    tif_name = ahps_site + '_' + magnitude + '_' + model_name + '_extent_hr.tif'
                    output_extent_tif = os.path.join(output_mapping_dir, tif_name)

                    logging.info(f"{huc} : {ahps_site} : {magnitude} - Begin HEC-RAS inundation for {tif_name}")

                    subprocess_cmd = [
                        flows2fim_path,
                        'fim',
                        '-lib', library_extent_path,
                        '-c', controls_csv,
                        '-fmt', 'cog',
                        '-o', output_extent_tif,
                        '-type', 'extent'
                    ]
                    try:
                        # Use subprocess to run flows2fim fim
                        subprocess.run(subprocess_cmd)  # TODO: Implement additional error catching?

                    except Exception:
                        logging.critical(
                            "A critical error occurred while attempting HEC-RAS inundation"
                            f" for {huc} - {ahps_site} - {magnitude}"
                        )
                        logging.critical(traceback.format_exc())
                        sys.exit(1)
            # End of HEC-RAS model/magnitude loop

            hr_site_tifs_produced = True # TODO: is there a better way to check for success?
            hr_site_tifs_produced = bool(hr_site_tifs_produced)

        # End of HEC-RAS inundation for site

        # Determine whether to run HAND for the site
        if inundate_hand is True and hr_preference is False:
            # Inundate HAND for a site if inundate_hand is True and hr_preference is False (run models for everyone! doesn't matter whether HR tifs were produced!)
            inundate_hand_for_site = True

        if inundate_hand is True and hr_preference is True:
            # If inundate_hand is True HR preference is True, we only inundate a site if HR tifs were not created
                if hr_site_tifs_produced is False:
                    # Run HAND because no HR tifs were produced (because HR failed or had no models available)
                    inundate_hand_for_site = True
                # else:  # Do not run HAND because HR tifs were already produced

        # We do not inundate HAND for the site if:
        # - inundate_hand is False
        # - inundate_hand is True but hr_preference is True and we already have HR tifs produced for the site

        logging.info(f"{huc} : {ahps_site} - hr_preference: {hr_preference}")  # TEMP DEBUG
        logging.info(f"{huc} : {ahps_site} - inundate_hand: {inundate_hand}")  # TEMP DEBUG
        logging.info(f"{huc} : {ahps_site} - hr_site_tifs_produced: {hr_site_tifs_produced}")  # TEMP DEBUG
        logging.info(f"{huc} : {ahps_site} - inundate_hand_for_site: {inundate_hand_for_site}")  # TEMP DEBUG

        if inundate_hand_for_site is True:
            logging.info(f"{huc} : {ahps_site} - HAND inundation...")  # TEMP DEBUG

            # Load the magnitude flows CSV for the HUC
            magnitude_flows_csv = os.path.join(huc_path, 'flow_discharges.csv')
            magnitude_flows_df = pd.read_csv(magnitude_flows_csv)

            for magnitude in magnitude_list:
                if "." in magnitude:
                    continue

                logging.info(" ")
                logging.info(f"{huc} : {ahps_site} - {magnitude}")

                # Create a site/magnitude specific flows csv and drop unnecessary colunmns
                magnitude_flows_df_filtered = magnitude_flows_df[
                    (magnitude_flows_df['lid'] == ahps_site) & (magnitude_flows_df['magnitude'] == magnitude)
                ]
                magnitude_flows_df_filtered = magnitude_flows_df_filtered.drop(columns=['lid', 'magnitude'])

                # Save filtered flows to CSV in the temp dir
                magnitude_flows_csv_path = os.path.join(output_temp_dir, f"{ahps_site}_{magnitude}_flows.csv")
                magnitude_flows_df_filtered.to_csv(magnitude_flows_csv_path, index=False)

                # logging.info(
                #     f"{huc} : {ahps_site} : {magnitude} - Saved flows to {magnitude_flows_csv_path}"
                # )  # Verbose - for debugging

                # Define output inundation extent tif path
                tif_name = ahps_site + '_' + magnitude + '_extent.tif'
                output_extent_tif = os.path.join(output_mapping_dir, tif_name)

                logging.info(f"{huc} : {ahps_site} : {magnitude} - Begin HAND inundation for {tif_name}")
                try:
                    # executor.submit(  # TODO: decide about keeping MP here (removed for now)
                    job_number_inundate = 1  # TODO: Decide about keeping MP here? (added a placeholder for now)

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
                        "A critical error occurred while attempting HAND inundation"
                        f" for {huc} - {ahps_site} - {magnitude}"
                    )
                    logging.critical(traceback.format_exc())
                    sys.exit(1)
            # End of HAND magnitude loop


    # End of site loop

    logging.info(" ")
    logging.info(f"{huc} - Mapping - End of inundating and mosaicing")

    return sites_gdf, huc_library_df


def run_fb_inundation(  # renamed from run_inundation
    huc, ahps_site, magnitude, fim_run_dir, magnitude_flows_csv_path, output_extent_tif, job_number_inundate
):
    '''
    Only used in flow-based CatFIM.

    Runs for each site/magnitude combination.

    Runs the inundation mapping workflow for a given HUC and magnitude, including logging,
    inundation raster generation, mosaicking, and lake masking.

    Inundates each set based on the ahps/mangnitude list and for each segment in the the branch hydrotable.
    Then each set is inundated per branch and mosiaked for the AHPS site.

    Processes
    ---------
        - Inundate_gms()
        - Mosaic_inundation()
        - mask_out_lakes()
        -> returns inundated site/mag tifs with lakes masked out

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code.
    ahps_site - STR
        Site ID.
    magnitude - STR
        Inundation category (i.e. action, minor, major, moderate, or record)
    fim_run_dir - STR
        Filepath to the FIM results to inundate.
    magnitude_flows_csv_path - STR
        Filepath to the magnitude flows CSV for the given HUC/Site/Magnitude combination.
    output_extent_tif - STR
        Filepath to save the inundated extent tif.
    job_number_inundate - STR
        Number of multiprocessing jobs to give the inundation tool (defaults to 1).
    '''

    try:
        logging.info(f"{huc} : {ahps_site} : {magnitude} - Running Inundate_gms and mosiacing")

        map_file = Inundate_gms(
            hydrofabric_dir=fim_run_dir,
            forecast=magnitude_flows_csv_path,
            num_workers=job_number_inundate,  # TODO: keep multiproc? currently defaults to 1
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
            mask=os.path.join(fim_run_dir, huc, 'wbd.gpkg'),
            unit_attribute_name='huc8',
            nodata=csf.ELEV_NODATA_VALUE,
            workers=1,
            remove_inputs=False,
            subset=None,
            verbose=False,
        )

        logging.info(f"{huc} : {ahps_site} : {magnitude} - Mosaic inundation complete")

        # ---------------------
        # Mask out lakes from inundated tif and re-save tif

        logging.info(
            f"{huc} : {ahps_site} : {magnitude} - Masking out lakes from {os.path.basename(output_extent_tif)}"
        )

        with rasterio.open(output_extent_tif, 'r+') as output_extent_src:
            output_extent_array = output_extent_src.read(1)
            output_extent_array_masked, mask_status = mask_out_lakes(
                output_extent_array, huc, output_extent_src, fim_run_dir
            )
            output_extent_src.write(output_extent_array_masked, 1)

        logging.info(f'{huc} : {ahps_site} : {magnitude} - {mask_status}')

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

        logging.info(f'{huc} : {ahps_site} : {magnitude} - Removing interium inundated branch files')

        output_extent_branch_tif = output_extent_tif.replace(".tif", "_*.tif")
        branch_tifs = glob.glob(output_extent_branch_tif)

        for tif_file in branch_tifs:
            os.remove(tif_file)

    # TODO: Decide humm... do we keep the try catch here? do we even want one?
    # what do we want to do if a site, mag fails... dump the entire tool or
    # just log this site/mag?
    except Exception as ex:
        # Log errors and their tracebacks

        logging.critical(f"{huc} : {ahps_site} : {magnitude} - EXCEPTION OCCURRED: ")
        logging.critical(traceback.format_exc())

        # re-raise the exception, mostly for AWS
        raise ex

    # Inundation.py appends the huc code to the supplied output_extent_grid for stage-based.
    # Modify output_extent_grid to match inundation.py saved filename.
    # Search for this file, if it didn't create, send message to log file.

    # base_file_path, extension = os.path.splitext(output_extent_tif)
    # saved_extent_grid_filename = "{}_{}{}".format(base_file_path, huc, extension)

    if not os.path.exists(output_extent_tif):
        # TODO: Doublecheck that this is the way we want to check for success
        logging.critical(f"{huc} : {ahps_site} : {magnitude} - FAILURE: map failed to create")
    # TODO: maybe add a bool for critical error for the try catch if we keep it?
    return


def run_sb_mapping(
    huc, huc_path, sites_gdf, huc_library_df, output_mapping_dir, fim_run_dir, huc_segments_df
):
    '''

    Only used in stage-based CatFIM.
    Runs for each HUC.

    Processes
    ---------
    - Gets a list of sites in HUC
    - Loops through sites. For each site: (TODO: Add for each site Get list of magnitudes, Create filepaths? )
        - Gets a list of magnitudes we have stage vals for
        - Loops through magnitudes. For each magnitude, runs:
            - run_sb_inundation() (previously called produce_stage_based_lid_tifs), which makes inundated site/mag tifs
        - Creates a list of stage intervals
        - Loops through intervals. For each interval, runs:
            - run_sb_inundation() to create inundated site/mag tifs

    Arguments
    ---------
    huc - STR
        Hydrologic Unit Code
    huc_path - STR
        Path to HUC-specific CatFIM outputs.
    sites_gdf - Geopandas GeoDataFrame
        Table of sites and data for the given HUC.
    huc_library_df - Pandas DataFrame
        Table of site/magnitude combinations for the given HUC.
    output_mapping_dir - STR
        Filepath to the HUC-specific mapping folder.
    fim_run_dir - STR
        Filepath to the HUC-specific FIM run outputs folder.
    huc_segments_df - Pandas DataFrame
        Table of NWM stream segments for the given HUC.

    Returns
    -------
    sites_gdf - Geopandas GeoDataFrame
        Table of sites and data for the given HUC, updated.
    huc_library_df - Pandas DataFrame
        Table of site/magnitude combinations for the given HUC, updated.

    CatFIM Reorg Notes (Jan 26)
    ---------------------------
    New... this is a wrapper for all things that SB needs do such
    as figure out intervals, iterate sites and lids and build the tifs

    '''

    # TODO: Add duration system here?

    logging.info(f"{huc} - Mapping - Start inundating and mosaicing")

    # -----------------------
    # Get list of AHPS sites in HUC from the sites GDF
    # (Exclude sites where mapped = no, we only want to map sites where mapped = not yet)
    ahps_sites_list = sites_gdf[sites_gdf['mapped'] != 'no']['nws_lid'].tolist()
    logging.info(f"{huc} - Mapping - ahps_sites_list is {ahps_sites_list}")

    # Read thresholds CSV files to get list of magnitudes per site
    huc_thresholds_csv_path = os.path.join(huc_path, f'{huc}_thresholds.csv')
    huc_thresholds_df = pd.read_csv(huc_thresholds_csv_path)

    # Filter out rows where threshold_type is not "stages"
    huc_thresholds_df = huc_thresholds_df[huc_thresholds_df['threshold_type'] == 'stages']

    # Use pd.melt to pivot the df to long format
    huc_thresholds_long_df = pd.melt(
        huc_thresholds_df,
        id_vars=['nws_lid'],
        value_vars=['action', 'minor', 'moderate', 'major', 'record'],
        var_name='magnitude_type',
        value_name='magnitude_value',
    )

    # Remove rows where magnitude_value is -1.0 - THRESH_NODATA_VALUE, these are where it was NaN in the database
    huc_thresholds_long_df = huc_thresholds_long_df[
        huc_thresholds_long_df['magnitude_value'] != csf.THRESH_NODATA_VALUE
    ]

    # Loop through AHPS sites
    for ahps_site in ahps_sites_list:

        logging.info("")
        logging.info(f"{huc} : {ahps_site}")

        # -----------------------
        # Inundate site using available stage magnitudes

        # Get a list of available magnitudes
        huc_thresholds_long_df_site = huc_thresholds_long_df[huc_thresholds_long_df['nws_lid'] == ahps_site]
        magnitude_list = huc_thresholds_long_df_site['magnitude_type'].to_list()

        # Loop through thresholds/magnitudes and define inundation output files paths
        logging.info(f"{huc} : {ahps_site} - Magnitude list is {magnitude_list}")

        # Get segments for each AHPS site

        # Filter df by site
        site_segments_df = huc_segments_df[huc_segments_df['lid'] == ahps_site]

        # Make the feature_id column into a list
        segments = site_segments_df['feature_id'].tolist()

        # If no segments, write message and exit out
        if not segments or len(segments) == 0:
            msg = 'Missing NWM stream segments for site'
            logging.warning(f"{huc} : {ahps_site} - msg")
            sites_gdf = csf.update_line_status_or_warning(ahps_site, sites_gdf, msg, set_mapped_to_no=True)
            continue

        # Create a placeholder for if we detect a negative stage
        negative_stage_detected = False

        # Iterate magnitudes to run inundation
        for magnitude in magnitude_list:
            if "." in magnitude:
                continue

            if negative_stage_detected:
                continue  # skip the rest of the magnitudes if a negative stage was detected in a previous magnitude

            logging.info("")
            logging.info(f"{huc} : {ahps_site} : {magnitude}")

            # Subset library df for site/magnitude and get the stage and other values
            site_mag_library_df = huc_library_df[
                (huc_library_df['nws_lid'] == ahps_site) & (huc_library_df['magnitude'] == magnitude)
            ]

            stage_val = site_mag_library_df['stage'].iloc[0]

            # Datum adjustment in feet (could be zero), created using WRDS metadata
            datum_adj_ft = site_mag_library_df['datum_adj_ft'].iloc[0]

            # Site elevation in meters, from the hydroconditioned FIM DEMs (temp column for processing)
            lid_usgs_elev = site_mag_library_df['lid_usgs_elev'].iloc[0]

            # Site elevation (zero datum) in feet, from WRDS metadata
            # TODO: Double check that this is correct variable to use here - lid_alt_ft
            lid_altitude = site_mag_library_df['lid_alt_ft'].iloc[0]

            # Calculate a portion of the file name which includes the category and
            # a formatted stage value (would include "i" if it were an interval file)
            category_key = __calculate_category_key(magnitude, stage_val, False)  # False = not an interval

            logging.info(f"{huc} : {ahps_site} : {magnitude} - Begin inundation for {category_key}")

            # Create the inundation tifs for each site/mag combo for the lid (previously called produce_stage_based_lid_tifs)
            hand_stage, datum_adj_wse, datum_adj_wse_m = run_sb_inundation(
                huc,
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

            # Update the huc_library_df with the new info from run_sb_inundation (datum_adj_wse, datum_adj_wse_m, hand_stage)
            huc_library_df.loc[
                (huc_library_df['nws_lid'] == ahps_site) & (huc_library_df['magnitude'] == magnitude),
                'datum_adj_wse_ft',
            ] = datum_adj_wse

            huc_library_df.loc[
                (huc_library_df['nws_lid'] == ahps_site) & (huc_library_df['magnitude'] == magnitude),
                'datum_adj_wse_m',
            ] = datum_adj_wse_m

            huc_library_df.loc[
                (huc_library_df['nws_lid'] == ahps_site) & (huc_library_df['magnitude'] == magnitude),
                'hand_stage',
            ] = hand_stage
            # We aren't updating sites df with this info because I don't think it is needed? Add here if needed.

            # Check if the HAND stage is negative, and if so, log a warning and skip the rest of the magnitudes for this site
            if hand_stage < 0:
                negative_stage_detected = True

        # End magnitude loop

        # If a negative HAND stage is detected, cease with processing the rest of the magnitudes
        # because if the HAND stage is negative at the lower stage magnitudes, then it will be
        # misleadingly under-inundating for the higher stage magnitudes.
        if negative_stage_detected:
            msg = "Negative HAND stage detected during processing, likely due to elevation data issues"
            logging.warning(
                f"{huc} : {ahps_site} - {msg}; Skipping mapping higher stages and intervals for this site to avoid misleading under-inundation."
            )
            sites_gdf = csf.update_line_status_or_warning(ahps_site, sites_gdf, msg, set_mapped_to_no=True)
            continue

        # -----------------------
        # Inundate site using additional stage intervals

        # Get non-record stages for the site and sort them by stage value
        non_rec_thresholds_df_unsorted_site = huc_thresholds_long_df_site[
            huc_thresholds_long_df_site["magnitude_type"] != 'record'
        ]
        non_rec_thresholds_df_site = non_rec_thresholds_df_unsorted_site.sort_values(
            by='magnitude_value'
        ).reset_index()

        # Make a list of interval recs to be created
        interval_list = []  # might stay empty
        huc_lid_id = f"{huc} : {ahps_site}"

        # Load interval cap val from environment
        past_major_interval_cap = os.getenv("PAST_MAJOR_INTERVAL_CAP")
        try:
            past_major_interval_cap = int(past_major_interval_cap)
        except ValueError:
            msg = f"Invalid value for PAST_MAJOR_INTERVAL_CAP: {past_major_interval_cap}"
            logging.error(msg)
            raise Exception(msg)

        # If non-record magnitudes are available, proceed with inundating intervals
        if len(non_rec_thresholds_df_site) > 0:

            # Calculate intervals for the site
            interval_list = __calc_sb_intervals(
                non_rec_thresholds_df_site, past_major_interval_cap, huc_lid_id
            )

            # Jan 2026 CatFIM Reorg: Previously we used a ProcessPoolExecutor here to iterate the interval list,
            # but it has been taken out for now.

            # Iterate intervals to run inundation
            for interval_rec in interval_list:  # list of lists

                magnitude = interval_rec[0]  # stage name, was category
                interval_stage_val = interval_rec[1]

                logging.info("")
                logging.info(f"{huc} : {ahps_site} : {magnitude} - interval {interval_stage_val} ft")

                # Calculate a portion of the file name which includes the category,
                # a formatted stage value, and a "i" to show it is an interval file
                category_key = __calculate_category_key(magnitude, interval_stage_val, True)

                logging.info(f"{huc} : {ahps_site} : {magnitude} - Begin inundation for {category_key}")

                # Create the inundation tifs for each site/mag combo for the lid (previously called produce_stage_based_lid_tifs)
                run_sb_inundation(
                    huc,
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

        else:
            logging.info(f"{huc_lid_id}: Skipping intervals as there are not any 'non-record' stages")

    logging.info(f"{huc} - Mapping - End inundating and mosaicing")

    return sites_gdf, huc_library_df


def run_sb_inundation(
    huc,
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

    Runs for each huc/site/magnitude combination.

    Processes
    ---------
        - calculate HAND stage
        - get hydrotable/branches. for each branch:
            - remove lake HydroIDs
            - inundate_sb() to create branch-level inundation tifs
        - mosaic_sb_inundation() to create lid/magnitude-level inundation tif
        - mask_out_lakes()

        -> returns inundated site/mag tifs with lakes masked out

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code
    stage_val - float
        The stage value to use for inundation mapping.
    datum_adj_ft - float
        Datum adjustment in feet to be applied to the stage value.
    lid_usgs_elev - float
        USGS elevation for the LID gage, used to calculate HAND stage.
    lid_altitude - float
        Altitude adjustment for the LID location.
    fim_run_dir - str
        Base directory for HAND FIM data.
    segments - list
        List of NWM segment feature IDs to process for inundation.
    ahps_site - str
        AHPS site ID.
    output_mapping_dir - str
        Filepath to HUC-specific CatFIM mapping directory.
    magnitude - str
        Category name for the inundation mapping (e.g., "action", "minor").
    category_key - str
        Key string representing the category and magnitude.

    Returns
    -------
    hand_stage - int
        Computed HAND stage in millimeters.
    datum_adj_wse - float
        Datum-adjusted water surface elevation in feet.
    datum_adj_wse_m - float
        Datum-adjusted water surface elevation in meters.

    Notes
    -----
    - Branch-level TIFFs are mosaicked into a single extent TIFF for the LID and category.
    - Lakes are masked out from the final inundation array.
    - Intermediary branch TIFFs are deleted after merging to save space.
    - Logging is performed throughout for traceability and error handling.
    - If negative HAND stage or missing segments are detected, processing is skipped for those cases.

    # Jan 26: was previously called produce_stage_based_lid_tifs
    '''

    # TODO: Decide if we want to implement this ID to FB CatFIM too?
    huc_lid_cat_id = f"{huc} : {ahps_site} : {magnitude}"

    logging.info(f"{huc_lid_cat_id} - Starting to create tifs")

    # ---------------------
    # Calculate HAND stage

    # Determine datum-offset water surface elevation
    datum_adj_wse = stage_val + datum_adj_ft + lid_altitude  # Calculate the WSE in ft
    datum_adj_wse_m = csf.feet_to_meters(datum_adj_wse)  # Convert ft to m

    # Subtract HAND gage elevation from HAND WSE to get HAND stage.
    hand_stage_m = datum_adj_wse_m - lid_usgs_elev  # HAND stage in m

    logging.info("datum_adj_wse = stage_val + datum_adj_ft + lid_altitude")  # TEMP DEBUG
    logging.info(f"{datum_adj_wse} = {stage_val} + {datum_adj_ft} + {lid_altitude}")  # TEMP DEBUG
    logging.info("hand_stage_m = datum_adj_wse_m - lid_usgs_elev")  # TEMP DEBUG
    logging.info(f"{hand_stage_m} = {datum_adj_wse_m} - {lid_usgs_elev}")  # TEMP DEBUG

    # hand_stage = ( # TODO: Clean up
    #     hand_stage_m if str(huc)[:2] == '19' else round(hand_stage_m * 1000)
    # )  # convert to mm to match HAND if it's NOT Alaska (HUC starts with 19)

    # Keep stage in meters if it's in Alaska (HUC starts with 19)
    if str(huc)[:2] == '19':
        hand_stage = hand_stage_m
        hand_stage_units = 'm'

    # otherwise convert to mm and round to nearest integer
    else:
        hand_stage = round(hand_stage_m * 1000)
        hand_stage_units = 'mm'

    # Round the datum_adj_wse and datum_adj_wse_m values
    datum_adj_wse = round(datum_adj_wse, 2)
    datum_adj_wse_m = round(datum_adj_wse_m, 2)

    logging.info(f"datum_adj_wse : {datum_adj_wse}")  # TEMP DEBUG
    logging.info(f"datum_adj_wse_m : {datum_adj_wse_m}")  # TEMP DEBUG
    logging.info(f"hand_stage : {hand_stage} {hand_stage_units}")  # TEMP DEBUG
    logging.info("")  # TEMP DEBUG

    # If hand_stage is negative, write message and exit out
    if hand_stage < 0:
        msg = f"Negative hand stage ({hand_stage} {hand_stage_units}) detected, no inundation possible"
        logging.warning(f"{huc_lid_cat_id} - {msg}")
        return hand_stage, datum_adj_wse, datum_adj_wse_m

    # ---------------------
    # Get branches from FIM outputs

    branch_dir = os.path.join(fim_run_dir, huc, 'branches')

    logging.info(f"{huc_lid_cat_id} - FIM branch dir is {branch_dir}")

    # Get branch list from FIM output directory
    branches = [x for x in os.listdir(branch_dir) if os.path.isdir(os.path.join(branch_dir, x))]
    branches.sort()

    # ---------------------
    # Iterate through branches to produce the inundated branch tifs for the HAND stage

    # TODO: In the future, could add multi-threading for branch processing.

    # Jan 2026 CatFIM Reorg Note:
    # Previously we had multiprocessing in place for the branch processing.
    # We've taken out the multiprocessing, but in the future we could put back
    # in some multi-threading to speed some things up. We would want to do some
    # benchmark tests before and after multithreading to make sure it's actually
    # speeding things up.

    # For now, we will just run it single-threaded and can implement multi-
    # threading later.

    for branch in branches:

        # Prepare branch-specific file paths
        msg_id_w_branch = f"{huc_lid_cat_id} : {branch}"
        # logging.info(f"{msg_id_w_branch} - Inundating branch")

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
        # if so.. change it here (adjusting for the actual status message in the final library gpkg) # TODO: Test this contingency?

        if not os.path.exists(rem_path):
            msg = "Branch REM doesn't exist (could be bad branch)"
            logging.warnings(f'{msg_id_w_branch} - {msg}')
            continue
        if not os.path.exists(catchments_path):
            msg = "Branch catchments files don't exist (could be bad branch)"
            logging.warnings(f'{msg_id_w_branch} - {msg}')
            continue
        if not os.path.exists(hydrotable_path):
            msg = "Branch hydrotable doesn't exist (could be bad branch)"
            logging.warnings(f'{msg_id_w_branch} - {msg}')
            continue

        # Use hydroTable to determine hydroid_list from site_ms_segments.
        hydrotable_df = pd.read_csv(
            hydrotable_path, low_memory=False, dtype={'HUC': str, 'LakeID': float, 'subdiv_applied': int}
        )

        hydroid_list, lake_hydroid_list, nolake_hydroid_list = [], [], []

        # ---------------------
        # Determine hydroids at which to perform inundation, filter out lakes

        for feature_id in segments:
            try:
                subset_hydrotable_df = hydrotable_df[hydrotable_df['feature_id'] == int(feature_id)]

                # List of HydroID's where the LakeID is greater than 0 (which shows that there's a lake)
                lake_hydroid_list = list(
                    subset_hydrotable_df.loc[subset_hydrotable_df['LakeID'] > 0]['HydroID'].unique()
                )

                # If lakes are detected, add info to the log
                if len(lake_hydroid_list) > 0:
                    logging.info(
                        f"{msg_id_w_branch} : {feature_id} - Removed HydroIDs w/ lakes: {lake_hydroid_list}"
                    )

                # List of HydroID's where there the LakeID is less than 0 (no lake, so we can inundate)
                nolake_hydroid_list = list(
                    subset_hydrotable_df.loc[subset_hydrotable_df['LakeID'] < 0]['HydroID'].unique()
                )

                # Add HydroIDs without lakes to the list to process
                hydroid_list += nolake_hydroid_list

            except IndexError:
                logging.error(  # TODO: Was a warning, changed to error. If it's super commonplace, can change back to warning
                    f"Index Error for {msg_id_w_branch}. FeatureId is {feature_id} : Continuing on."
                )
                pass

        # Create inundation maps with branch and stage data
        # Only sites /categories that got this far are valid and can be inundated
        try:
            # CatFIM Reorg (Jan 2026): Previously we had executor.submit(inundate_sb, etc. ) here.
            # Took out MP for now, but can reimplement in the future if we want to optimize.

            # Define output inundation extent tif path
            file_name = ahps_site + '_' + category_key + '_extent_' + huc + '_' + branch
            output_branch_tif = os.path.join(output_mapping_dir, file_name + '.tif')

            inundate_sb(  # Jan 26: was previously called produce_inundated_branch_tif
                huc,
                ahps_site,  # called lid inside function
                category_key,
                hand_stage,
                rem_path,
                catchments_path,
                hydroid_list,
                output_branch_tif,
            )

        except Exception:
            msg = f'Inundation failed for {category_key}'
            logging.error(f'{msg_id_w_branch} - {msg}')
            logging.error(traceback.format_exc())

    # end of previous MP (removed Jan 2026)
    # end of branch loop

    # ---------------------
    # Mosaic inundation tifs for ahps_site/magnitude

    logging.info(f"{huc_lid_cat_id} - Starting to mosaic inundation")

    # Note: Jan 2026 - Moved mosaic code into mosaic_sb_inundation() to match FB processing
    # mosaic_sb_inundation() in SB is analogous to Mosaic_inundation() function in FB

    output_extent_tif, mosaic_sb_success = mosaic_sb_inundation(
        ahps_site, output_mapping_dir, category_key, huc_lid_cat_id
    )

    # Exit early if the mosaic function returned an error
    if mosaic_sb_success is False:
        # Error already was logged in mosaic_sb_inundation, so no need to log it here
        return hand_stage, datum_adj_wse, datum_adj_wse_m

    # Exit early if no output extent tif was created
    if output_extent_tif is None or not os.path.exists(output_extent_tif):
        logging.error(
            f"{huc_lid_cat_id} - Branch tifs found but mosaic_sb_inundation failed to create output extent tif. Skipping lake masking"
        )
        return hand_stage, datum_adj_wse, datum_adj_wse_m

    else:
        logging.info(f"{huc_lid_cat_id} - Mosaic inundation completed")

    # ---------------------
    # Mask out lakes from inundated tif and re-save tif
    # Jan 2026 - Moved lake masking to outside the mosaic function to match fb processing

    # logging.info(f'{huc_lid_cat_id} - Masking out lakes from {os.path.basename(output_extent_tif)}')

    with rasterio.open(output_extent_tif, 'r+') as output_extent_src:
        output_extent_array = output_extent_src.read(1)
        output_extent_array_masked, mask_status = mask_out_lakes(
            output_extent_array, huc, output_extent_src, fim_run_dir
        )
        output_extent_src.write(output_extent_array_masked, 1)

    if mask_status:
        logging.info(f'{huc_lid_cat_id} - Masking status: {mask_status}')

    return hand_stage, datum_adj_wse, datum_adj_wse_m


def inundate_sb(  # formerly called produce_inundated_branch_tif
    huc,
    lid,
    category_key,
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

    This function reads a REM (Raster Elevation Model) and catchments raster, applies
    a stage threshold, and masks the result to catchments matching the provided hydroid
    list. The output is a raster where inundated cells are marked as 1 and non-inundated
    cells as 0.

    This is a form of inundation which is specific to CatFIM because we only have one
    flow value and the other FIM inundation tools are looking for flow files not single
    values.

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code.
    lid - str
        APHS site ID.
    category_key - str
        Key string representing the category and magnitude.
    hand_stage - float or int
        Stage threshold value for inundation.
    rem_path - str
        Path to the HAND FIM REM raster file.
    catchments_path - str
        Path to the catchments raster file.
    hydroid_list - list of int
        List of hydroid identifiers to include in the mask.
    output_branch_tif - str
        Filepath for the eventual inundated branch tif.

    '''

    try:
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

        # Check if no cells were inundated (branches don't inundate as they are out of the extent area)
        is_all_zero = np.all(masked_reclass_rem_array == 0)

        if is_all_zero == False:
            with rasterio.Env():
                profile = rem_src.profile
                profile.update(dtype=rasterio.uint8)
                profile.update(nodata=0)

                # Replace any existing nodata values with the new one
                # masked_reclass_rem_array[masked_reclass_rem_array == profile["nodata"]] = 0

                with rasterio.open(output_branch_tif, 'w', **profile) as dst:
                    dst.write(masked_reclass_rem_array, 1)

                logging.info(
                    f'{huc} : {lid} : {category_key} - Saving tif to {os.path.basename(output_branch_tif)}'
                )

        # else: # is_all_zero = True:
        # logging.info(f'inundate_sb - not saving tif because masked array was all zero')

    except Exception:
        logging.critical(f"{huc} : {lid} : {category_key} - Error producing inundation maps with stage")
        logging.critical(traceback.format_exc())

    return


def mosaic_sb_inundation(lid, output_mapping_dir, category_key, huc_lid_cat_id):
    '''
    Mosaics all branch inundation tifs for a given lid/magnitude combination into a single extent tif.

    Jan 2026 - turned into a new function, previously was just loose code in run_sb_inundation

    NOTE: This section is analogous to Mosaic_inundation() function in FB CatFIM

    Arguments
    ---------
    lid - str
        AHPS site ID.
    output_mapping_dir - str
        Filepath to HUC-specific CatFIM mapping output directory.
    category_key - str
        Key string representing the category and magnitude (i.e. action_10.0ft)
    huc_lid_cat_id - str
        Key string represnting the HUC, LID, and category (for logging) (i.e. '03070104 : ABBG1 : action').

    Returns
    -------
    output_extent_tif - str
        Filepath of newly mosaiced and saved output extent tif.
    is_success - bool
        Indicator of whether mosaic sb inundation properly finished

    '''
    # Merge all rasters in output_mapping_dir that have the same magnitude/category.
    path_list = []

    # We are looking for the branch files for the category/stage (or any given stage interval)

    lid_dir_list = [x for x in os.listdir(output_mapping_dir) if category_key in x]
    lid_dir_list.sort()  # To force branch 0 first in list, sort

    for f in lid_dir_list:
        path_list.append(os.path.join(output_mapping_dir, f))

    # Exit function if there aren't any tifs to mosaic
    if len(lid_dir_list) == 0:
        logging.error(  # TODO: Should this be an error or warning? Might want to trace further up
            f"{huc_lid_cat_id} - No branch tifs found for category key {category_key}. Skipping mosaicking and lake masking."
        )
        is_success = False
        return None, is_success

    logging.info(f"{huc_lid_cat_id} - Merging {len(lid_dir_list)} branch tifs")

    # Merging all of the branch tifs into one lid_category tif
    zero_branch_grid = path_list[0]
    zero_branch_src = rasterio.open(zero_branch_grid)
    zero_branch_array = zero_branch_src.read(1)
    summed_array = zero_branch_array  # Initialize it as the branch zero array

    output_extent_tif = os.path.join(output_mapping_dir, lid + '_' + category_key + '_extent.tif')
    # logging.info(f"{huc_lid_cat_id}: Merging all branches into output file to be saved as {output_extent_tif}")

    # Loop through remaining items in list and sum them with summed_array
    for remaining_raster in path_list[1:]:

        remaining_raster_src = rasterio.open(remaining_raster)
        remaining_raster_array_original = remaining_raster_src.read(1)

        # TODO: Nov 2024: Check whether we still need to reproject

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

    # Save summed array
    profile = zero_branch_src.profile
    summed_array = summed_array.astype('uint8')

    with rasterio.open(output_extent_tif, 'w', **profile) as dst:
        dst.write(summed_array, 1)
        logging.info(
            f"{huc_lid_cat_id} - Branch rollup extent file saved at {os.path.basename(output_extent_tif)}"
        )

    # Jan 2026 - Moved lake masking to from here to outside the mosaic function to match the fb processing

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

    logging.info(f"{huc_lid_cat_id} - Removing interium inundated branch files")

    branch_tifs = glob.glob(f"{output_mapping_dir}/{lid}_{category_key}_extent_*.tif")
    for tif_file in branch_tifs:
        os.remove(tif_file)

    is_success = True

    return output_extent_tif, is_success


def post_process_huc_mapping(huc, catfim_type, sites_gdf, huc_library_df, output_mapping_dir):
    '''
    Used in both flow-based and stage-based CatFIM.

    Post-processes inundation mapping results for a given HUC.

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code
    catfim_type - str
        'sb' or 'fb'
    sites_gdf - Geopandas GeoDataFrame
        Table of sites and data for the given HUC.
    huc_library_df - Pandas DataFrame
        Table of site/magnitude combinations for the given HUC.
    output_mapping_dir - str
        Filepath to the HUC-specific CatFIM mapping directory.

    Returns
    -------
    sites_gdf - Geopandas GeoDataFrame
        Table of sites and data for the given HUC, updated.
    huc_library_df - Pandas DataFrame
        Table of site/magnitude combinations for the given HUC, updated.

    Notes
    -----
    TODO: Look into optimization options. We have the library.gpkg at this point
    and it knows which recs are intervals versuses not.. is there an optimization option here?

    '''
    logging.info("")
    logging.info(f"{huc} - Post-Process HUC Mapping")

    # inundate_hr = bool(os.getenv('INUNDATE_HR')) # TODO: Clean up if we don't need this
    hr_preference = bool(os.getenv('HR_PREFERENCE'))

    # -----------------------
    # Iterate through tifs

    # Get a list of tifs (files ending with "extent.tif" means it is a rolled up version of HAND inundation)
    hand_tif_list = [x for x in os.listdir(output_mapping_dir) if ('extent.tif') in x]  # HAND tif list
    hr_tif_list = [x for x in os.listdir(output_mapping_dir) if ('extent_hr.tif') in x]  # HEC-RAS tif list

    logging.info(f"Before filtering by preference, found {len(hand_tif_list)} HAND tifs and {len(hr_tif_list)} HEC-RAS tifs to process.")  # TEMP DEBUG

    # If the HEC-RAS preference is true, remove the HAND tifs wherever HEC-RAS tifs are avail
    if hr_preference is True:
        for hr_tif in hr_tif_list:

            # Get corresonding HAND tif filepath
            hand_tif = hr_tif.replace("extent_hr.tif", "extent.tif")

            # If the corresponding HAND tif is there, remove it
            # This should be redundant because we shouldn't have produced this tif in the fist place
            if hand_tif in hand_tif_list:
                logging.warning(f'Found HAND tif where HR tif already existed. Indicates possible error. Tif: {hand_tif}')
                hand_tif_list.remove(hand_tif)

    # Make the full filepaths
    tifs_to_reformat_list = []
    tif_list = hand_tif_list + hr_tif_list
    for tif in tif_list:
        tifs_to_reformat_list.append(os.path.join(output_mapping_dir, tif))

    logging.info(f"Found {len(hand_tif_list)} HAND tifs and {len(hr_tif_list)} HEC-RAS tifs to process.")

    if len(tifs_to_reformat_list) == 0:
        logging.warning(f"{huc} - Post-Process HUC Mapping  - No tifs found at {output_mapping_dir}")
        return sites_gdf, None

    # Iterate through the saved tifs to make a list of inundated multipolygons
    reformatted_geom_list = []

    for tif_to_process in tifs_to_reformat_list:
        if not os.path.exists(tif_to_process):
            # Shouldn't be possible since we just pulled these paths from the folder, but just in case
            logging.warning(
                f"{huc} - Post-Process HUC Mapping - Path found but no file found (potential bug?), unable to post-process {tif_to_process}"
            )
            continue

        # If stage based, the file names looks like this:
        #      masm1_major_extent.tif  (non-interval, whole number)
        #      masm1_major_20.6_extent.tif  (non-interval, float)
        #      masm1_major_20.0fti_extent.tif (interval)
        #
        # If flow based, the file name looks like this:
        #      masm1_action_extent.tif

        try:
            # Get site, magnitude, and interval data from the tif name
            tif_file_name = os.path.basename(tif_to_process)
            file_name_parts = tif_file_name.split("_")
            nws_lid = file_name_parts[0]
            magnitude = file_name_parts[1]

            if tif_file_name.endswith("_hr.tif"):
                model = 'HEC-RAS'
                # Pull the model name from the tif -> tif_file_name = f'{nws_lid}_{magnitude}_{model_name}_extent_hr.tif'

                hr_tif_prefix = nws_lid + '_' + magnitude + '_'  
                hr_tif_suffix = '_extent_hr.tif'
                model_version = tif_file_name.removeprefix(hr_tif_prefix).removesuffix(hr_tif_suffix)

            else:
                model = 'HAND'
                model_version = 'HAND' # TODO: input version?

            # Check whether the tif is an interval (indicated by "fti" in the file name)
            # (carefully, we only check part 3 because "ft" can be part of the site name)
            interval_stage = None
            is_interval = False
            if len(file_name_parts) >= 3 and "fti" in file_name_parts[2]:
                try:
                    stage_val = file_name_parts[2].replace("fti", "")
                    interval_stage = float(stage_val)
                    is_interval = True

                except ValueError as ve:
                    interval_stage = None
                    logging.critical(
                        f"{huc} - Post-Process HUC Mapping - Error differentiating intervals from non-interval values for tif {tif_file_name}"
                    )
                    logging.critical(traceback.format_exc())

                    raise ve  # this shuts down processsing for this tif # TODO: apply elsewhere?

            # Convert the inundation raster tif into a dissolved inundation multipolygon
            # TODO: Is there an optimization available here instead of processing each tif / mag at a time?
            extent_poly_diss = reformat_inundation_maps(
                huc, nws_lid, magnitude, tif_to_process, interval_stage, is_interval, model, model_version
            )

            # Append the inundation multipolygon to the list
            reformatted_geom_list.append(extent_poly_diss)

        except Exception:
            logging.critical(
                f"{huc} - Post-Process HUC Mapping - An ind reformat map error occured for site {nws_lid} - magnitude {magnitude}"
            )
            logging.critical(traceback.format_exc())

    # Make inundated multipolygon list into a dataframe
    reformatted_geom_list_df = pd.concat(reformatted_geom_list, ignore_index=True)

    # Exit if no geoms were created
    # (pretty unlikely, should only happen if something has gone wrong while reformatting inundation maps)
    if len(reformatted_geom_list_df) == 0:
        logging.warning(
            f"{huc} - Post-Process HUC Mapping - TIFFs found but no reformatted geom created at {output_mapping_dir}"
        )
        return sites_gdf, None

    # Handle intervals if CatFIM type is stage-based
    if catfim_type == 'sb':

        # Subset the reformatted inundation maps to get only the intervals
        mapped_intervals_df = reformatted_geom_list_df[reformatted_geom_list_df['is_interval']]
        num_intervals = len(mapped_intervals_df)

        if num_intervals == 0:
            logging.info(f"{huc} - Post-Process HUC Mapping - Stage-based - No intervals mapped")
            # Join the inundated multipolgyon dataframe to the HUC library dataframe (without using interval_stage col)
            huc_library_df = huc_library_df.merge(
                reformatted_geom_list_df, on=['nws_lid', 'magnitude', 'is_interval'], how='left'
            ) # For now we don't need to merge on model type because SB is not set up to run HEC-RAS

        elif num_intervals > 0:
            logging.info(
                f"{huc} - Post-Process HUC Mapping  - Stage-based - Updating HUC library to include {num_intervals} intervals"
            )

            # Get the lid, mag, and interval stage vals from the intervals DF so we can add the new interval records to the HUC library
            huc_library_interval_data_list = []

            for index, row in mapped_intervals_df.iterrows():
                # logging.info(f"nws_lid: {nws_lid}, magnitude: {magnitude}, interval_stage: {interval_stage} ") ## TEMP DEBUG

                nws_lid = row['nws_lid']
                magnitude = row['magnitude']
                interval_stage = row['interval_stage']

                # Find the equivalent row in huc_library_df
                # Shouldn't be more than one line but iloc[0] selects the first entry just in case
                huc_library_df_subset = huc_library_df[
                    (huc_library_df['nws_lid'] == nws_lid) & (huc_library_df['magnitude'] == magnitude)
                ].iloc[0]

                # Add the interval information
                huc_library_df_subset['interval_stage'] = interval_stage
                huc_library_df_subset['is_interval'] = True

                huc_library_interval_data_list.append(huc_library_df_subset)

            # Make the new interval data into a DF and append it to the huc_library_df
            huc_library_interval_df = pd.DataFrame(huc_library_interval_data_list)
            huc_library_df = pd.concat([huc_library_df, huc_library_interval_df], ignore_index=True)

            # Join the inundated multipolgyon dataframe to the HUC library dataframe
            huc_library_df = huc_library_df.merge(
                reformatted_geom_list_df,
                on=['nws_lid', 'magnitude', 'interval_stage', 'is_interval'],
                how='left',
            )

    elif catfim_type == 'fb':

        # if hr_preference is True:
        #     # There should be only one inundation polygon per magnitude/nws_lid combo

        #     # Could iterate through just in case...


        #     # reformatted_geom_list_df will have filled-in model and model_version columns
        #     # Drop the bla

        #     # Join the inundated multipolgyon dataframe to the HUC library dataframe
        #     huc_library_df = huc_library_df.merge(
        #         reformatted_geom_list_df, on=['nws_lid', 'magnitude'], how='left'
        #     ) # TODO: Do we need to update the HUC library with model info? Test...

        # else:
    
        # There could be several inundation polygons per magnitude/nws_lid combos (HAND and multiple HEC-RAS)
        # TODO: test multiple HEC-RAS models at once

        logging.info(
            f"{huc} - Post-Process HUC Mapping - Flow-based - Updating HUC library to include enough rows for model types"
        )

        # Get the lid, mag, and model vals from the HR DF so we can add the new HR records to the HUC library
        huc_library_data_list = []

        logging.info(f"Len of huc_library_df, BEFORE adding extra rows: {len(huc_library_df)}")
        logging.info(f"Len of reformatted_geom_list_df: {len(reformatted_geom_list_df)}")

        for index, row in reformatted_geom_list_df.iterrows():

            nws_lid = row['nws_lid']
            magnitude = row['magnitude']
            model = row['model']
            model_version = row['model_version']

            # Find the equivalent row in huc_library_df
            # Shouldn't be more than one line but iloc[0] selects the first entry just in case
            huc_library_df_subset = huc_library_df[
                (huc_library_df['nws_lid'] == nws_lid) & (huc_library_df['magnitude'] == magnitude)
            ].iloc[0]

            # Add the model information
            huc_library_df_subset['model'] = model
            huc_library_df_subset['model_version'] = model_version

            huc_library_data_list.append(huc_library_df_subset)

        # Make the new library df (with enough rows for all the models and versions in the geom list)
        huc_library_df = pd.DataFrame(huc_library_data_list)

        logging.info(f"Len of huc_library_data_list, AFTER adding extra rows: {len(huc_library_data_list)}")
        logging.info(f"Len of huc_library_df, AFTER adding extra rows: {len(huc_library_df)}")

        # Join the inundated multipolgyon dataframe to the HUC library dataframe
        huc_library_df = huc_library_df.merge(
            reformatted_geom_list_df, on=['nws_lid', 'magnitude', 'model', 'model_version'], how='left'
        )

    # Make the HUC library df into a gdf
    huc_library_gdf = gpd.GeoDataFrame(huc_library_df, geometry='geometry', crs=VIZ_PROJECTION)

    # -----------------------
    # Dissolve based on site and magnitude
    # TODO: I'm not convinced that dissolving will change things in most cases... but maybe worth
    # keeping for now?

    # Previous methods only had dissolving for flow-based, probably because the intervals could make
    # things confusing...

    if catfim_type == "fb":
        logging.info(f"{huc} - Post-Process HUC Mapping - Dissolving CatFIM library for flow-based CatFIM")

        huc_library_undissolved_gdf = huc_library_gdf
        huc_library_gdf = huc_library_gdf.dissolve(by=['nws_lid', 'magnitude', 'model', 'model_version'], as_index=False)
        # TODO: Do we need to update the HUC library with model info? Test...

        # Exit post process HUC early if the library GDF is empty after dissolving (if it was previously not empty)
        if len(huc_library_gdf) == 0 and len(huc_library_undissolved_gdf) > 0:
            logging.critical(
                f"{huc} - Post-Process HUC Mapping - Library was not empty before dissolving but is empty after dissolving... likely a critical code error"
            )
            return sites_gdf, None

    elif catfim_type == "sb":  # prev versions did not dissolve SB... for good reason? or no?
        logging.info(
            f"{huc} - Post-Process HUC Mapping - Not dissolving CatFIM library for stage-based CatFIM"
        )
        # huc_library_gdf = huc_library_gdf.dissolve(by=['nws_lid', 'magnitude', 'interval_stage'], as_index=False)

    return sites_gdf, huc_library_gdf


def reformat_inundation_maps(huc, nws_lid, magnitude, tif_to_process, interval_stage, is_interval, model, model_version):
    '''
    Used in both flow- and stage-based CatFIM.

    Convert the inundation raster tif into a dissolved inundation multipolygon.

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code
    nws_lid - str
        APHS site ID.
    magnitude - str
        Category name for the inundation mapping (e.g., "action", "minor").
    tif_to_process - str
        Filepath for the inundated branch tiff for the site/magnitude/HUC combination.
    interval_stage - float? or None
        If the inundation is interval, this is the stage value. If not, it is None.
    is_interval - bool?
        Indicates whether the inundation is interval (True) or not (False)
    model - STR
        TODO: Fill in
    model_version - STR
        TODO: Fill in
    Returns
    -------
    extent_poly_diss - Multipolygon
        The inundation from the TIFF, turned into a multipolygon object and dissolved if needed.

    '''

    try:
        logging.info(f"{huc} : {nws_lid} : {magnitude} : {model} : {model_version} - Converting inundated tif to multipolygon")

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
            logging.critical(
                f"{huc} : {nws_lid} : {magnitude} : {model} : {model_version} - No values above zero in inundated tif, "
                "so zero inundated shapes were found. See GitHub issue #1491 for details."
                # TODO: Is this GitHub issue still active? make sure error msg is up-to-date
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
        extent_poly_diss['model'] = model
        extent_poly_diss['model_version'] = model_version

        # Project to Web Mercator
        extent_poly_diss = extent_poly_diss.to_crs(VIZ_PROJECTION)

        # Convert the features to multipolygon if needed
        extent_poly_diss["geometry"] = [
            MultiPolygon([feature]) if type(feature) is Polygon else feature
            for feature in extent_poly_diss["geometry"]
        ]

        # Throw an error w/ traceback if the polygon is empty
        if extent_poly_diss.empty:
            logging.error(
                f"{huc} : {nws_lid} : {magnitude} : {model} : {model_version} - Reformat inundation tif to gpkg resulted in an empty multipolygon"
            )
            logging.error(traceback.format_exc())

    except ValueError as ve:
        msg = f"{huc} : {nws_lid} : {magnitude} : {model} : {model_version} - Reformatted inundation map"
        if "Assigning CRS to a GeoDataFrame without a geometry column is not supported" in ve:
            logging.critical(f"{msg} - Warning: details: {ve}")
        else:
            logging.critical(f"{msg} - Exception")
            logging.critical(traceback.format_exc())

    except Exception:
        logging.critical(f"{huc} : {nws_lid} : {magnitude} : {model} : {model_version} - Reformatted inundation map - Exception")
        logging.critical(traceback.format_exc())

    return extent_poly_diss


def __calculate_category_key(category, stage_value, is_interval_stage):
    '''
    Used in stage-based CatFIM.

    Calcuates the category key which becomes part of a file name
    Changes to an int if whole number only. ie.. we don't want 22.00 but 22.0, but keep 22.15
    category_key comes things like this: action, action_24.0ft, or action_24.6ft

    Arguments
    ---------
    category - str
        The flood category (e.g., 'action', 'minor', etc.).
    stage_value - float
        The stage value for the category.
    is_interval_stage - bool
        Whether this is an interval stage (True) or a main stage (False).

    Returns
    -------
    category_key - str
        Category and stage value string for file naming.

    TODO: Could create a more refined system.
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


def __calc_sb_intervals(non_rec_thresholds_df_site, past_major_interval_cap, huc_lid_id):
    '''
    Used in stage-based CatFIM.

    Calculate stage intervals for inundation mapping based on non-recurrent stage values.
    This function generates a list of intervals between stage values, rounding up to the next whole number
    where necessary, and ensures that intervals are unique and in order. For each stage, it determines the
    range of integer depths to be used for inundation calculations, up to the next stage or a specified cap
    for the last stage.

    Arguments
    ---------
    non_rec_thresholds_df_site - pd.DataFrame
        DataFrame containing stage names and their corresponding stage values.
        Must have columns "magnitude_type" and "magnitude_value".
    past_major_interval_cap - int
        The number of intervals to add beyond the last stage value.
    huc_lid_id - str
        Identifier used for logging and tracing.

    Returns
    -------
    interval_recs - list
        A list of lists, where each sublist contains a stage name and an integer interval value,
        e.g., [["action", 21], ["action", 22], ...]. This represents the stage names and depths
        to be used for inundation mapping.

    TODO: Would be good to rethink how we calculate intervals and improve.

    '''
    interval_recs, stage_values_claimed = [], []
    num_stage_value_recs = len(non_rec_thresholds_df_site)

    logging.info("")
    logging.info(f"{huc_lid_id}: Calculating intervals for {num_stage_value_recs} stages")

    # Calculate the intervals for each magnitude in the df
    # Note: Records will be in order by stage value. We calculate intervals one magnitude at a time
    # so we can keep track of the magnitude name associated with the interval.
    for idx in non_rec_thresholds_df_site.index:

        row = non_rec_thresholds_df_site.loc[idx]
        cur_magnitude_name = row["magnitude_type"]  # was cur_stage_name
        cur_stage_val = row["magnitude_value"]

        # Calculate the intervals betwen the current and the next stage value.
        # For the current val, we need to round up because it is possible for stages to be decimals
        # (for example, if action is 2.4, and mod is 4.6, we want intervals at 3 and 4).
        # The highest value of the interval_list is not included
        # TODO: double check/reprase... I think what we mean here is that the max interval isn't included?

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
            next_stage_val = non_rec_thresholds_df_site.iloc[idx + 1]["magnitude_value"]
            max_interval_val = int(next_stage_val)

        else:
            # If the record IS the last record for the site, calculate the maximum interval
            # value as the minimum interval value + the interval cap (default is 5).
            max_interval_val = int(min_interval_val) + int(past_major_interval_cap)

        # Take the minimum and maximum interval values and get a list of the whole nummbers that
        # occur between them (Example: np.arange(1, 5) -> [1, 2, 3, 4]).
        interval_list = np.arange(min_interval_val, max_interval_val)

        # Add intervals to the output list (if they are not already claimed)
        # Previously some duplicate values would slip throguh, the stage_values_claimed
        # functionality mitigates that problem.
        for int_val in interval_list:
            if int_val not in stage_values_claimed:
                interval_recs.append([cur_magnitude_name, int_val])
                stage_values_claimed.append(int_val)

    logging.info(f"{huc_lid_id} interval recs are {interval_recs}")

    return interval_recs


def __load_validate_mapping_data(huc, huc_path, sites_pre_mapping_file_path, library_pre_mapping_file_path):
    '''
    Used for both SB and FB.

    Loads data needed for CatFIM mapping.
    It does not load discharge here as only FB needs that

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code.
    huc_path - str
        Filepath to the HUC-specific CatFIM outputs.
    sites_pre_mapping_file_path - str
        Filepath to the sites file before mapping.
    library_pre_mapping_file_path - str
        Filepath to the library before mapping.

    Returns
    -------
    sites_gdf - Geopandas Geodataframe
        The sites table (before mapping).
    huc_library_df - Pandas DataFrame
        The library table (before mapping).
    huc_segments_df - Pandas DataFrame
        The table of NWM stream segments from the FIM run outputs.
    fim_run_dir - str
        The filepath to the HAND FIM output to be used for inundation.

    Notes
    ------
    New as of CatFIM reorg Jan 2026.

    TODO: do we want to validate additional inputs? could validate any inputs
    (such as branches maybe) when reasonable so we can abort sooner than later?
    '''

    # --------------------------
    # Get FIM run dir from env file
    fim_run_dir = os.getenv("FIM_RUN_DIR")
    logging.info(f"{huc} - Mapping - FIM run directory: {fim_run_dir}")

    if not os.path.isdir(fim_run_dir):
        raise Exception(f"{huc} - Mapping - FIM run dir not found at {fim_run_dir}")

    # --------------------------
    # Load segments file (used by both FB and SB)
    segments_file_path = os.path.join(huc_path, "features_segments.csv")
    if not os.path.isfile(segments_file_path):
        raise Exception(f"{huc} - Mapping - Missing file, expected {segments_file_path}")

    huc_segments_df = pd.read_csv(segments_file_path)

    if len(huc_segments_df) == 0:
        raise Exception(f"{huc} - Mapping - Segments file should not be empty by this point")

    # --------------------------
    # Load up the library csv that has been building up so far
    # This is not a gpkg yet, but a df instead, as it has no geometry yet.
    if not os.path.isfile(library_pre_mapping_file_path):
        raise Exception(f"{huc} - Mapping - Missing file, expected {library_pre_mapping_file_path}")

    huc_library_df = pd.read_csv(library_pre_mapping_file_path)

    if len(huc_library_df) == 0:
        raise Exception(f"{huc} - Mapping - huc_library_df file should not be empty by this point.")

    # --------------------------
    # Load the temp sites_gdf. This is a copy for usage in mapping
    # It can be updated and later, catfim_process_huc.py will pick it up and reload it
    if not os.path.isfile(sites_pre_mapping_file_path):
        raise Exception(f"{huc} - Mapping - Missing file, expected {sites_pre_mapping_file_path}")

    sites_gdf = gpd.read_file(sites_pre_mapping_file_path, engine='fiona')

    if len(sites_gdf) == 0:
        raise Exception(f"{huc} - Mapping - site_gdf should not be empty")

    # --------------------------
    # Note sites where mapped = no (just as an FYI)
    # (sites that are unmapped but mappable will have the value 'not yet' in their mapped column)
    sites_no_mapping = sites_gdf.loc[sites_gdf['mapped'] == 'no']['nws_lid'].to_list()
    sites_no_mapping_status = sites_gdf.loc[sites_gdf['mapped'] == 'no']['status'].to_list()

    if len(sites_no_mapping) > 0:
        logging.info(f'{huc} - Mapping - The following sites will not be mapped:')

        for index, site in enumerate(sites_no_mapping):
            logging.info(f"{huc} - Mapping -   * {site} - {sites_no_mapping_status[index]}")

    return sites_gdf, huc_library_df, huc_segments_df, fim_run_dir


def __save_huc_outputs_post_mapping(
    huc,
    sites_gdf,
    huc_library_gdf,
    sites_post_mapping_file_path,  # was sites_file_path
    library_post_mapping_file_path,
):
    '''
    Saves the HUC library geodataframe after mapping (or logs its absence).

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code
    sites_gdf - Geopandas Geodataframe (or None)
        The sites table (AFTER mapping).
    huc_library_gdf - Geopandas Geodataframe (or None)
        Table of site/magnitude combinations for the given HUC, WITH inundated polygons.
    sites_post_mapping_file_path - str
        Filepath for which to the sites geopackage to after mapping.
    library_post_mapping_file_path - str
        Filepath for which to save the library to geopackage after mapping.

    '''

    logging.info(" ")

    # ---------------------------
    # Save HUC library GDF if it exists, otherwise log its absense

    if huc_library_gdf is None:
        # Skip saving if there's no library gdf to save
        logging.warning(f"{huc} - Mapping - No HUC libary GDF to save")
    else:
        # Save HUC library as GPKG (CSV will be saved later, so we don't have to update both down the line)
        logging.info(f"{huc} - Mapping - Saving HUC library to {library_post_mapping_file_path}")
        huc_library_gdf.to_file(library_post_mapping_file_path, driver='GPKG', engine="fiona", index=False)

    # ---------------------------
    # Save HUC sites GDF if it exists, otherwise log its absense

    if sites_gdf is None:
        # Skip saving if there's no sites gdf to save
        # (this is really unlikely, so if it happens then you should definitely look into it)
        logging.error(
            f"{huc} - Mapping - No HUC sites GDF to save, table should exist even if no sites got mapped"
        )

    else:
        # Save HUC sites as GPKG (CSV will be saved later, so we don't have to update both down the line)
        logging.info(f"{huc} - Mapping - Saving HUC sites GDF to {sites_post_mapping_file_path}")
        sites_gdf.to_file(sites_post_mapping_file_path, driver='GPKG', engine="fiona", index=False)

    return


# This function is only called when the tool is being run by itself from command line
def main(huc, output_folder, huc_path):
    '''
    Run CatFIM mapping from the command line for a HUC.

    Arguments
    ---------
    huc - str
        8-digit hydologic unit code
    output_folder - str
        Filepath to CatFIM outputs.
    huc_path - str
        Filepath to the HUC-specific CatFIM outputs.

    '''
    is_logging_loaded = False

    try:
        overall_start_time = datetime.now(timezone.utc)
        dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")
        print("===================================================")
        print("")

        # ----------------------------
        # Create filepaths

        catfim_type = os.getenv('CATFIM_TYPE')

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

        # ----------------------------
        # Set up logging

        log_file_path = sf.setup_file_logger(output_log_dir, f"map_{huc}_catfim")

        logging.info(f"Mapping command-line wrapper - Start CatFIM mapping for {huc} ; (UTC): {dt_string}")
        print(f"Mapping command-line wrapper - Logs will be saved to {log_file_path}")
        print("")
        is_logging_loaded = True

        # ----------------------------
        # Load CatFIM run arguments from the runtime_args.env file
        csf.load_runtime_args(output_folder)

        # Validate input parameters and return normalized paths
        huc_path, output_folder = csf.validate_inputs(huc, output_folder)

        # ----------------------------
        # Validate inputs and remove previous outputs

        # Confirm that sites pre-mapping and library pre-mapping files exist
        if not os.path.exists(sites_pre_mapping_file_path):
            msg = f"Mapping command-line wrapper - Missing file at sites_pre_mapping_file_path - {sites_pre_mapping_file_path}"
            logging.critical(msg)
            raise FileNotFoundError(msg)

        if not os.path.exists(library_pre_mapping_file_path):
            msg = f"Mapping command-line wrapper - Missing file at library_pre_mapping_file_path - {library_pre_mapping_file_path}"
            logging.critical(msg)
            raise FileNotFoundError(msg)

        # Remove and rebuild the HUC mapping directory
        # We do NOT want to remove the /{huc}/temp folder because that's where pre-mapping files are
        preexisting_mapping_files = len(os.listdir(output_mapping_dir))
        if preexisting_mapping_files > 0:
            logging.info(
                f'Mapping command-line wrapper - Removing {preexisting_mapping_files} pre-existing files from {output_mapping_dir}'
            )
            shutil.rmtree(output_mapping_dir, ignore_errors=True)
            os.mkdir(
                output_mapping_dir, mode=0o777
            )  # recreate the mapping folder with permissions that allow read/write/execute for all

        # Remove pre-existing post-mapping library and sites files
        if os.path.isfile(library_post_mapping_file_path):
            os.remove(library_post_mapping_file_path)
            logging.info('Mapping command-line wrapper - Removed pre-existing library post-mapping file')

        if os.path.isfile(sites_post_mapping_file_path):
            os.remove(sites_post_mapping_file_path)
            logging.info('Mapping command-line wrapper - Removed pre-existing sites post-mapping file')

        # ----------------------------
        # Process CatFIM mapping

        logging.info("")
        logging.info("Mapping command-line wrapper - Starting process_mapping()")
        logging.info("")

        process_mapping(
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

        # ----------------------------
        # Finalize sites and library GDF

        logging.info("")
        logging.info("Mapping command-line wrapper - Starting finalize_sites_mapping_status()")
        logging.info("")

        csf.finalize_sites_mapping_status(
            huc,
            catfim_type,
            sites_post_mapping_file_path,  # Output path
            library_post_mapping_file_path,  # Output path
            sites_post_mapping_file_path,  # Data to finalize
            library_post_mapping_file_path,  # Data to finalize
        )

        # TODO: Wrap up logs?

        duration_msg = sf.calculate_duration_msg(overall_start_time)
        logging.info("")
        logging.info(f"Mapping command-line wrapper - Completed running mapping! - {duration_msg}")
        print("")
        print("===================================================")

    except Exception as ex:
        trace_error = traceback.format_exc()
        err_msg = f"Mapping command-line wrapper - A critical error has occurred during CatFIM mapping. Detail: {trace_error}"

        if is_logging_loaded:
            logging.critical(err_msg)
        else:
            print(err_msg)

        # re-raise the exception, mostly for AWS
        raise ex

    return


if __name__ == '__main__':
    '''
    Runs the HUC-level mapping functionality from the command line.

    In order to run mapping in the command line:
        - Both flow-based and stage-based CatFIM must have gotten through the
          process_threshold_data() function.
        - Stage-based CatFIM must also have gotten through the
          __process_elevations() function.

    Arguments
    ---------
    huc - str
        Hydrologic Unit Code.
    output_folder - str
        CatFIM output folder.
    huc_path - str
        Filepath to the HUC-specific CatFIM outputs.

    Sample
    ------
    python /foss_fim/tools/catfim/generate_categorical_fim_mapping.py -u 12090301 -t /data/catfim/hand_4_8_7_2
    '''

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
    parser.add_argument(
        '-p',
        '--huc-path',
        help='REQUIRED: Filepath to the HUC-specific CatFIM outputs. This is where the mapping tool will look for inputs and save outputs. '
        'ie /data/catfim/hand_4_8_7_2/12090301 or /data/catfim/test/test1/12090301',
        required=True,
    )

    args = vars(parser.parse_args())

    main(**args)
