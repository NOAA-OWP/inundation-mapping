#!/usr/bin/env python3

import argparse
import csv
import glob
import logging
import os
import random
import shutil
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
#from logging.handlers import QueueHandler, QueueListener
from datetime import datetime, timezone
from itertools import repeat
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from dotenv import load_dotenv
from generate_categorical_fim_flows import generate_catfim_flows
from generate_categorical_fim_mapping import manage_catfim_mapping, post_process_cat_fim_for_viz
from rasterio.warp import Resampling, calculate_default_transform, reproject

from tools_shared_functions import (
    filter_nwm_segments_by_stream_order,
    get_datum,
    get_nwm_segs,
    get_thresholds,
    ngvd_to_navd_ft,
    setup_logger,
    merge_log_files,
)
from tools_shared_variables import (
    acceptable_alt_acc_thresh,
    acceptable_alt_meth_code_list,
    acceptable_coord_acc_code_list,
    acceptable_coord_method_code_list,
    acceptable_site_type_list,
)

from utils.shared_variables import VIZ_PROJECTION

gpd.options.io_engine = "pyogrio"

def process_generate_categorical_fim(
    fim_run_dir,
    env_file,
    job_number_huc,
    job_number_inundate,
    stage_based,
    output_folder,
    overwrite,
    search,
    job_number_intervals,
    past_major_interval_cap,
):
    print("================================")
    print("Start generate categorical fim")

    overall_start_time = datetime.now(timezone.utc)
    dt_string = datetime.now(timezone.utc).strftime("%m/%d/%Y %H:%M:%S")
    print(f"started (UTC): {dt_string}")
    print()

    # Check job numbers and raise error if necessary
    total_cpus_requested = job_number_huc * job_number_inundate * job_number_intervals
    total_cpus_available = os.cpu_count() - 2
    if total_cpus_requested > total_cpus_available:
        raise ValueError(
            'The HUC job number, {}, multiplied by the inundate job number, {}, '
            'exceeds your machine\'s available CPU count minus one. '
            'Please lower the job_number_huc or job_number_inundate '
            'values accordingly.'.format(job_number_huc, job_number_inundate)
        )

   
    # we are getting too many folders and files. We want just huc folders.    
    # output_flow_dir_list = os.listdir(fim_run_dir)
    # looking for folders only starting with 0, 1, or 2
    lst_hucs = [x for x in os.listdir(fim_run_dir) if os.path.isdir(os.path.join(fim_run_dir, x))
                and x[0] in ['0', '1', '2']]
    # print(lst_hucs)
        
    num_hucs = len(lst_hucs)        
    if num_hucs == 0:
        raise ValueError(
            f'Output directory {fim_run_dir} is empty. ' 'Verify that you have the correct input folder.'
        )
    print(f"Processing {num_hucs} huc(s)")        
    
    load_dotenv(env_file)
    API_BASE_URL = os.getenv('API_BASE_URL')
    if API_BASE_URL is None:
        raise ValueError(
            'API base url not found. '
            'Ensure inundation_mapping/tools/ has an .env file with the following info: '
            'API_BASE_URL, EVALUATED_SITES_CSV, WBD_LAYER, NWM_FLOWS_MS, '
            'USGS_METADATA_URL, USGS_DOWNLOAD_URL'
        )

    # TODO: Add check for if lid_to_run and lst_hucs parameters conflict

    # Check that fim_inputs.csv exists and raise error if necessary
    #fim_inputs_csv_path = os.path.join(fim_run_dir, 'fim_inputs.csv')
    #if not os.path.exists(fim_inputs_csv_path):
    #    raise ValueError(f'{fim_inputs_csv_path} not found. Verify that you have the correct input files.')

    print()

    # Define default arguments. Modify these if necessary
    fim_version = os.path.split(fim_run_dir)[1]

    # Append option configuration (flow_based or stage_based) to output folder name.
    if stage_based:
        file_handle_appendage, catfim_method = "_stage_based", "STAGE-BASED"
    else:
        file_handle_appendage, catfim_method = "_flow_based", "FLOW-BASED"

    # Define output directories
    output_catfim_dir = output_folder + file_handle_appendage
  
    output_flows_dir = os.path.join(output_catfim_dir, 'flows')
    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')

    # Create output directories (check against maping only as a proxy for all three)
    if os.path.exists(output_mapping_dir) is True:
        if overwrite is False:
            raise Exception(f"The output mapping folder of {output_catfim_dir} already exists."
                " If you want to overwrite it, please add the -o flag. Note: When overwritten, "
                " the three folders of mapping, flows and attributes wil be deleted and rebuilt")
        shutil.rmtree(output_mapping_dir, ignore_errors=True)
        shutil.rmtree(output_flows_dir, ignore_errors=True)
        shutil.rmtree(attributes_dir, ignore_errors=True)

    if not os.path.exists(output_flows_dir):
        os.mkdir(output_flows_dir)
    if not os.path.exists(output_mapping_dir):
        os.mkdir(output_mapping_dir)
    if not os.path.exists(attributes_dir):
        os.mkdir(attributes_dir)

    # setup general logger    
    log_folder = os.path.join(output_catfim_dir, "logs")
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"generate_cat_fim_{file_dt_string}.log"
    log_output_file = os.path.join(log_folder, log_file_name)
    setup_logger(log_output_file, __name__, True)

    # Define upstream and downstream search in miles
    nwm_us_search, nwm_ds_search = search, search

    # Set up specific file logging
    log_dir = os.path.join(output_catfim_dir, 'logs')
    log_file = os.path.join(log_dir, 'errors.log')

    # STAGE-BASED
    if stage_based:
        # Generate Stage-Based CatFIM mapping3
        nws_sites_layer = generate_stage_based_categorical_fim(
            output_catfim_dir,
            fim_run_dir,
            nwm_us_search,
            nwm_ds_search,
            env_file,
            job_number_inundate,
            job_number_huc,            
            lst_hucs,
            job_number_intervals,
            past_major_interval_cap,
            log_output_file,            
        )

        #job_number_tif = job_number_inundate * job_number_intervals
        logging.info("Post-processing TIFs...")
        post_process_cat_fim_for_viz(
            output_catfim_dir,
            job_number_huc,
            job_number_inundate,
            log_file,
            fim_version,
        )

        # Updating mapping status
        logging.info('Updating mapping status...')
        update_mapping_status(output_mapping_dir, output_flows_dir, nws_sites_layer, stage_based)

    # FLOW-BASED
    else:
        logging.info('Creating flow files using the ' + catfim_method + ' technique...')
        start = time.time()
        nws_sites_layer = generate_catfim_flows(
            output_flows_dir,
            nwm_us_search,
            nwm_ds_search,
            env_file,
            stage_based,
            lst_hucs,
            job_number_huc,
        )
        end = time.time()
        elapsed_time = (end - start) / 60

        logging.info(f'Finished creating flow files in {elapsed_time} minutes')

        # Generate CatFIM mapping
        logging.info('Begin mapping')
        start = time.time()
        manage_catfim_mapping(
            fim_run_dir,
            output_flows_dir,
            output_mapping_dir,
            attributes_dir,
            job_number_huc,
            job_number_inundate,
            overwrite,
            depthtif=False,
        )
        end = time.time()
        elapsed_time = (end - start) / 60
        logging.info(f'Finished mapping in {elapsed_time} minutes')

        # Updating mapping status
        logging.info('Updating mapping status')
        update_mapping_status(str(output_mapping_dir), str(output_flows_dir), nws_sites_layer, stage_based)

    # Create CSV versions of the final geopackages.
    logging.info('Creating CSVs. This may take several minutes.')
    reformatted_catfim_method = catfim_method.lower().replace('-', '_')
    create_csvs(output_mapping_dir, reformatted_catfim_method)

    logging.info("================================")
    logging.info("End generate categorical fim")

    end_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    logging.info(f"ended: {dt_string}")

    # calculate duration
    time_duration = end_time - overall_start_time
    logging.info(f"Duration: {str(time_duration).split('.')[0]}")
    logging.info("")


def create_csvs(output_mapping_dir, reformatted_catfim_method):
    '''
    Produces CSV versions of desired geopackage in the output_mapping_dir.

    Parameters
    ----------
    output_mapping_dir : STR
        Path to the output directory of all inundation maps.
    reformatted_catfim_method : STR
        Text to append to CSV to communicate the type of CatFIM.

    Returns
    -------
    None.

    '''

    # Convert any geopackage in the root level of output_mapping_dir to CSV and rename.
    gpkg_list = glob.glob(os.path.join(output_mapping_dir, '*.gpkg'))
    for gpkg in gpkg_list:
        logging.info(f"Creating CSV for {gpkg}")
        gdf = gpd.read_file(gpkg)
        parent_directory = os.path.split(gpkg)[0]
        if 'catfim_library' in gpkg:
            file_name = reformatted_catfim_method + '_catfim.csv'
        if 'nws_lid_sites' in gpkg:
            file_name = reformatted_catfim_method + '_catfim_sites.csv'

        csv_output_path = os.path.join(parent_directory, file_name)
        gdf.to_csv(csv_output_path)


def update_mapping_status(output_mapping_dir, output_flows_dir, nws_sites_layer, stage_based):
    '''
    Updates the status for nws_lids from the flows subdirectory. Status
    is updated for sites where the inundation.py routine was not able to
    produce inundation for the supplied flow files. It is assumed that if
    an error occured in inundation.py that all flow files for a given site
    experienced the error as they all would have the same nwm segments.

    Parameters
    ----------
    output_mapping_dir : STR
        Path to the output directory of all inundation maps.
    output_flows_dir : STR
        Path to the directory containing all flows.

    Returns
    -------
    None.

    '''
    # Find all LIDs with empty mapping output folders
    subdirs = [str(i) for i in Path(output_mapping_dir).rglob('**/*') if i.is_dir()]
    
    logging.info("")
    logging.debug("subdirs are..")
    logging.debug(subdirs)
    logging.info("")
    
    empty_nws_lids = [Path(directory).name for directory in subdirs if not list(Path(directory).iterdir())]
    logging.debug("")
    logging.debug("empty_nws_lids are..")
    logging.debug(empty_nws_lids)
    logging.debug("")

    # Write list of empty nws_lids to DataFrame, these are sites that failed in inundation.py
    mapping_df = pd.DataFrame({'nws_lid': empty_nws_lids})
    
    mapping_df['did_it_map'] = 'no'
    mapping_df['map_status'] = ' and all categories failed to map'

    # Import geopackage output from flows creation
    flows_df = gpd.read_file(nws_sites_layer)

    if len(flows_df) == 0:
        logging.info(f"flows_df is empty. Path is {nws_sites_layer}. Program aborted.")
        sys.exit(1)

    try:
        # Join failed sites to flows df
        flows_df = flows_df.merge(mapping_df, how='left', on='nws_lid')

        # Switch mapped column to no for failed sites and update status
        flows_df.loc[flows_df['did_it_map'] == 'no', 'mapped'] = 'no'
        flows_df.loc[flows_df['did_it_map'] == 'no', 'status'] = flows_df['status'] + flows_df['map_status']

        #    # Perform pass for HUCs where mapping was skipped due to missing data  #TODO check with Brian
        #    if stage_based:
        #        missing_mapping_hucs =
        #    else:
        #        flows_hucs = [i.stem for i in Path(output_flows_dir).iterdir() if i.is_dir()]
        #        mapping_hucs = [i.stem for i in Path(output_mapping_dir).iterdir() if i.is_dir()]
        #        missing_mapping_hucs = list(set(flows_hucs) - set(mapping_hucs))
        #
        #    # Update status for nws_lid in missing hucs and change mapped attribute to 'no'
        #    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'status'] =
        #           flows_df['status'] + ' and all categories failed to map because missing HUC information'
        #    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'mapped'] = 'no'

        # Clean up GeoDataFrame and rename columns for consistency
        flows_df = flows_df.drop(columns=['did_it_map', 'map_status'])
        flows_df = flows_df.rename(columns={'nws_lid': 'ahps_lid'})

        # Write out to file
        flows_df.to_file(nws_sites_layer)
    except Exception as e:
        logging.info(f"{output_mapping_dir} : No LIDs, \n Exception: \n {repr(e)} \n")
        logging.info(traceback.format_exc())


def produce_inundation_map_with_stage_and_feature_ids(
    rem_path, catchments_path, hydroid_list, hand_stage, lid_directory, category, huc, lid, branch, log_output_file, log_file_prefix
):
    # Open rem_path and catchment_path using rasterio.
    
    # Setup Multiproc logging
    random_id = random.randrange(10000, 99999)    
    log_name = f"{log_file_prefix}_{random_id}"
    log_file_name = f"{log_name}.log"
    log_folder = os.path.dirname(log_output_file)
    log_output_file = os.path.join(log_folder, log_file_name)
    setup_logger(log_output_file, log_name)
    
    logging.info("")
    logging.info("+++++++++++++++++++++++")
    logging.info(f"At the start of producing inundation maps for {huc}")
    logging.debug(locals())
    logging.debug("+++++++++++++++++++++++")    
    logging.info("")
    
    
    rem_src = rasterio.open(rem_path)
    catchments_src = rasterio.open(catchments_path)
    rem_array = rem_src.read(1)
    catchments_array = catchments_src.read(1)

    # Use numpy.where operation to reclassify rem_path on the condition that the pixel values
    #   are <= to hand_stage and the catchments value is in the hydroid_list.
    reclass_rem_array = np.where((rem_array <= hand_stage) & (rem_array != rem_src.nodata), 1, 0).astype(
        'uint8'
    )
    hydroid_mask = np.isin(catchments_array, hydroid_list)
    target_catchments_array = np.where(
        (hydroid_mask is True) & (catchments_array != catchments_src.nodata), 1, 0
    ).astype('uint8')
    masked_reclass_rem_array = np.where(
        (reclass_rem_array == 1) & (target_catchments_array == 1), 1, 0
    ).astype('uint8')

    # Save resulting array to new tif with appropriate name. brdc1_record_extent_18060005.tif
    is_all_zero = np.all((masked_reclass_rem_array == 0))
    
    # TODO: How can we get all zeros??
    
    logging.info(f"is_all_zero is {is_all_zero}")

    if not is_all_zero:
        output_tif = os.path.join(
            lid_directory, lid + '_' + category + '_extent_' + huc + '_' + branch + '.tif'
        )
        logging.info(f" +++ Output_Tif is {output_tif}")
        with rasterio.Env():
            profile = rem_src.profile
            profile.update(dtype=rasterio.uint8)
            profile.update(nodata=10)

            with rasterio.open(output_tif, 'w', **profile) as dst:
                dst.write(masked_reclass_rem_array, 1)


def mark_complete(site_directory):
    marker_file = Path(site_directory) / 'complete.txt'
    marker_file.touch()
    return


def iterate_through_huc_stage_based(
    output_catfim_dir,
    huc,
    fim_dir,
    huc_dictionary,
    threshold_url,
    flood_categories,
    all_lists,
    past_major_interval_cap,
    number_of_jobs,
    number_of_interval_jobs,
    attributes_dir,
    nwm_flows_df,
    log_output_file,
    log_file_prefix,
):
    missing_huc_files = []
    all_messages = []
    stage_based_att_dict = {}

    # it needs its own MP logger
    log_folder = os.path.join(output_catfim_dir, "logs")
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    log_file_name = f"{log_file_prefix}_{huc}.log"
    log_output_file = os.path.join(log_folder, log_file_name)
    setup_logger(log_output_file)

    mapping_dir = os.path.join("output_catfim_dir", "mapping")

    #logging.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    #logging.info(f'Iterating through {huc}...')
    # Make output directory for huc.
    mapping_huc_directory = os.path.join(mapping_dir, huc)
    if not os.path.exists(mapping_huc_directory):
        os.mkdir(mapping_huc_directory)

    # Define paths to necessary HAND and HAND-related files.
    usgs_elev_table = os.path.join(fim_dir, huc, 'usgs_elev_table.csv')
    branch_dir = os.path.join(fim_dir, huc, 'branches')

    # Loop through each lid in nws_lids list
    nws_lids = huc_dictionary[huc]
    for lid in nws_lids:
        logging.info("")
        logging.info(f"lid id {lid}")
        
        lid = lid.lower()  # Convert lid to lower case
        # -- If necessary files exist, continue -- #
        if not os.path.exists(usgs_elev_table):
            msg = f'{lid}: usgs_elev_table missing, likely unacceptable gage datum error-- more details to come in future release'
            logging.info(msg)
            continue
        if not os.path.exists(branch_dir):
            logging.info(f'{lid}: branch directory missing')
            continue
        usgs_elev_df = pd.read_csv(usgs_elev_table)

        # Make lid_directory.
        lid_directory = os.path.join(mapping_huc_directory, lid)
        if not os.path.exists(lid_directory):
            os.mkdir(lid_directory)
        else:
            complete_marker = os.path.join(lid_directory, 'complete.txt')
            if os.path.exists(complete_marker):
                logging.info(f"{lid}: already completed in previous run.")
                continue
        # Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
        stages, flows = get_thresholds(
            threshold_url=threshold_url, select_by='nws_lid', selector=lid, threshold='all'
        )

        if stages is None:
            logging.info(f'{lid}: error getting thresholds from WRDS API')
            continue
        # Check if stages are supplied, if not write message and exit.
        if all(stages.get(category, None) is None for category in flood_categories):
            logging.info(f'{lid}:missing threshold stages')
            continue

        try:
            # Drop columns that offend acceptance criteria
            usgs_elev_df['acceptable_codes'] = (
                usgs_elev_df['usgs_data_coord_accuracy_code'].isin(acceptable_coord_acc_code_list)
                & usgs_elev_df['usgs_data_coord_method_code'].isin(acceptable_coord_method_code_list)
                & usgs_elev_df['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list)
                & usgs_elev_df['usgs_data_site_type'].isin(acceptable_site_type_list)
            )

            usgs_elev_df = usgs_elev_df.astype({'usgs_data_alt_accuracy_code': float})
            usgs_elev_df['acceptable_alt_error'] = np.where(
                usgs_elev_df['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh, True, False
            )

            acceptable_usgs_elev_df = usgs_elev_df[
                (usgs_elev_df['acceptable_codes'] == True) & (usgs_elev_df['acceptable_alt_error'] == True)
            ]
        except Exception as e:
            # Not sure any of the sites actually have those USGS-related
            # columns in this particular file, so just assume it's fine to use

            # print("(Various columns related to USGS probably not in this csv)")
            # print(f"Exception: \n {repr(e)} \n")
            logging.info(f"ERROR: {huc} - {lid}:  An error has occurred while working with the usgs_elev table")
            logging.info(traceback.format_exc())            
            acceptable_usgs_elev_df = usgs_elev_df

        # Get the dem_adj_elevation value from usgs_elev_table.csv.
        # Prioritize the value that is not from branch 0.
        try:
            matching_rows = acceptable_usgs_elev_df.loc[
                acceptable_usgs_elev_df['nws_lid'] == lid.upper(), 'dem_adj_elevation'
            ]

            if len(matching_rows) == 2:  # It means there are two level paths, use the one that is not 0
                lid_usgs_elev = acceptable_usgs_elev_df.loc[
                    (acceptable_usgs_elev_df['nws_lid'] == lid.upper())
                    & (acceptable_usgs_elev_df['levpa_id'] != 0),
                    'dem_adj_elevation',
                ].values[0]
            else:
                lid_usgs_elev = acceptable_usgs_elev_df.loc[
                    acceptable_usgs_elev_df['nws_lid'] == lid.upper(), 'dem_adj_elevation'
                ].values[0]
        except IndexError:  # Occurs when LID is missing from table
            logging.info(f"ERROR: {huc} - {lid}:  adjusting dem_adj_elevation")
            logging.info(traceback.format_exc())
            logging.info(f'{lid}: likely unacceptable gage datum error or accuracy code(s); please see acceptance criteria')
            continue
        # Initialize nested dict for lid attributes
        stage_based_att_dict.update({lid: {}})

        # Find lid metadata from master list of metadata dictionaries.
        metadata = next((item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False)
        lid_altitude = metadata['usgs_data']['altitude']

        # Filter out sites that don't have "good" data
        try:
            if not metadata['usgs_data']['coord_accuracy_code'] in acceptable_coord_acc_code_list:
                logging.info(
                    f"\t{lid}: {metadata['usgs_data']['coord_accuracy_code']} "
                    "Not in acceptable coord acc codes"
                )
                continue
            if not metadata['usgs_data']['coord_method_code'] in acceptable_coord_method_code_list:
                logging.info(f"\t{lid}: Not in acceptable coord method codes")
                continue
            if not metadata['usgs_data']['alt_method_code'] in acceptable_alt_meth_code_list:
                logging.info(f"\t{lid}: Not in acceptable alt method codes")
                continue
            if not metadata['usgs_data']['site_type'] in acceptable_site_type_list:
                logging.info(f"\t{lid}: Not in acceptable site type codes")
                continue
            if not float(metadata['usgs_data']['alt_accuracy_code']) <= acceptable_alt_acc_thresh:
                logging.info(f"\t{lid}: Not in acceptable threshold range")
                continue
        except Exception as e:
            logging.info(f"ERROR: {huc} - {lid}:  filtering out 'bad' data in the usgs_data")
            logging.info(traceback.format_exc())
            continue

        ### --- Do Datum Offset --- ###
        # determine source of interpolated threshold flows, this will be the rating curve that will be used.
        rating_curve_source = flows.get('source')
        if rating_curve_source is None:
            logging.info(f'{lid}:No source for rating curve')
            continue
        # Get the datum and adjust to NAVD if necessary.
        nws_datum_info, usgs_datum_info = get_datum(metadata)
        if rating_curve_source == 'USGS Rating Depot':
            datum_data = usgs_datum_info
        elif rating_curve_source == 'NRLDB':
            datum_data = nws_datum_info

        # If datum not supplied, skip to new site
        datum = datum_data.get('datum', None)
        if datum is None:
            logging.info(f'{lid}:datum info unavailable')
            continue
        # ___________________________________________________________________________________________________#
        # SPECIAL CASE: Workaround for "bmbp1" where the only valid datum is from NRLDB (USGS datum is null).
        # Modifying rating curve source will influence the rating curve and
        #   datum retrieved for benchmark determinations.
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
            # Get the datum adjustment to convert NGVD to NAVD. Sites not in contiguous US are previously
            #   removed otherwise the region needs changed.
            try:
                datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data, region='contiguous')
            except Exception as e:
                logging.info(f"ERROR: {lid}: ngvd_to_navd_ft")
                logging.info(traceback.format_exc())
                e = str(e)
                if crs is None:
                   logging.info(f'{lid}: ERROR: NOAA VDatum adjustment error, CRS is missing')
                if 'HTTPSConnectionPool' in e:
                    time.sleep(10)  # Maybe the API needs a break, so wait 10 seconds
                    try:
                        datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data, region='contiguous')
                    except Exception:
                        logging.info(f'{lid}: ERROR: NOAA VDatum adjustment error, possible API issue')
                if 'Invalid projection' in e:
                    logging.info(f'{lid}: ERROR: NOAA VDatum adjustment error, invalid projection: crs={crs}')
                continue

        ### -- Concluded Datum Offset --- ###
        # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
        unfiltered_segments = list(set(get_nwm_segs(metadata)))

        # Filter segments to be of like stream order.
        desired_order = metadata['nwm_feature_data']['stream_order']
        segments = filter_nwm_segments_by_stream_order(unfiltered_segments, desired_order, nwm_flows_df)
        action_stage = stages['action']
        minor_stage = stages['minor']
        moderate_stage = stages['moderate']
        major_stage = stages['major']
        stage_list = [i for i in [action_stage, minor_stage, moderate_stage, major_stage] if i is not None]
        # Create a list of stages, incrementing by 1 ft.
        if stage_list == []:
            logging.info(f'{lid}: WARNING: no stage values available')
            continue
        interval_list = np.arange(
            min(stage_list), max(stage_list) + past_major_interval_cap, 1.0
        )  # Go an extra 10 ft beyond the max stage, arbitrary

        # Check for large discrepancies between the elevation values from WRDS and HAND.
        #   Otherwise this causes bad mapping.
        elevation_diff = lid_usgs_elev - (lid_altitude * 0.3048)
        if abs(elevation_diff) > 10:
            logging.info(f'{lid}: large discrepancy in elevation estimates from gage and HAND')
            continue

            # For each flood category
        logging.info(f"{huc} - {lid}: About to process flood categories")
        for category in flood_categories:
            logging.info(f"{huc} - {lid}: Category is {category}")
            # Pull stage value and confirm it's valid, then process
            stage = stages[category]

            if stage is not None and datum_adj_ft is not None and lid_altitude is not None:
                # Call function to execute mapping of the TIFs.
                (messages, hand_stage, datum_adj_wse, datum_adj_wse_m) = produce_stage_based_catfim_tifs(
                    stage,
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
                    log_folder,
                    "",
                )
                all_messages += messages

                # Extra metadata for alternative CatFIM technique.
                # TODO Revisit because branches complicate things
                stage_based_att_dict[lid].update(
                    {
                        category: {
                            'datum_adj_wse_ft': datum_adj_wse,
                            'datum_adj_wse_m': datum_adj_wse_m,
                            'hand_stage': hand_stage,
                            'datum_adj_ft': datum_adj_ft,
                            'lid_alt_ft': lid_altitude,
                            'lid_alt_m': lid_altitude * 0.3048,
                        }
                    }
                )
            # If missing HUC file data, write message
            if huc in missing_huc_files:
                logging.info(f'{lid}:missing some HUC data')

        # Now that the "official" category maps are made, produce the incremental maps.
        log_file_prefix = "MP_produce_stage_based_catfim_tifs_{huc}"
        with ProcessPoolExecutor(max_workers=number_of_interval_jobs) as executor:
            try:
                for interval_stage in interval_list:
                    # Determine category the stage value belongs with.
                    if action_stage <= interval_stage < minor_stage:
                        category = 'action_' + str(interval_stage).replace('.', 'p') + 'ft'
                    if minor_stage <= interval_stage < moderate_stage:
                        category = 'minor_' + str(interval_stage).replace('.', 'p') + 'ft'
                    if moderate_stage <= interval_stage < major_stage:
                        category = 'moderate_' + str(interval_stage).replace('.', 'p') + 'ft'
                    if interval_stage >= major_stage:
                        category = 'major_' + str(interval_stage).replace('.', 'p') + 'ft'
                    executor.submit(
                        produce_stage_based_catfim_tifs,
                        interval_stage,
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
                        log_output_file,
                        log_file_prefix,
                    )
            except TypeError:  # sometimes the thresholds are Nonetypes
                logging.info("ERROR: type error in ProcessPool somewhere")
                logging.info(traceback.format_exc())
                pass
            except Exception:
                logging.info("ERROR: ProcessPool has an error")
                logging.info(traceback.format_exc())
                # merge MP Logs (Yes)                
                merge_log_files(log_output_file, log_file_prefix )                
                sys.exit(1)
                
        # merge MP Logs (merging MP into an MP (proc_pool in a proc_pool))
        merge_log_files(log_output_file, log_file_prefix )

        # Create a csv with same information as geopackage but with each threshold as new record.
        # Probably a less verbose way.
        csv_df = pd.DataFrame()
        for threshold in flood_categories:
            try:
                line_df = pd.DataFrame(
                    {
                        'nws_lid': [lid],
                        'name': metadata['nws_data']['name'],
                        'WFO': metadata['nws_data']['wfo'],
                        'rfc': metadata['nws_data']['rfc'],
                        'huc': [huc],
                        'state': metadata['nws_data']['state'],
                        'county': metadata['nws_data']['county'],
                        'magnitude': threshold,
                        'q': flows[threshold],
                        'q_uni': flows['units'],
                        'q_src': flows['source'],
                        'stage': stages[threshold],
                        'stage_uni': stages['units'],
                        's_src': stages['source'],
                        'wrds_time': stages['wrds_timestamp'],
                        'nrldb_time': metadata['nrldb_timestamp'],
                        'nwis_time': metadata['nwis_timestamp'],
                        'lat': [float(metadata['nws_preferred']['latitude'])],
                        'lon': [float(metadata['nws_preferred']['longitude'])],
                        'dtm_adj_ft': stage_based_att_dict[lid][threshold]['datum_adj_ft'],
                        'dadj_w_ft': stage_based_att_dict[lid][threshold]['datum_adj_wse_ft'],
                        'dadj_w_m': stage_based_att_dict[lid][threshold]['datum_adj_wse_m'],
                        'lid_alt_ft': stage_based_att_dict[lid][threshold]['lid_alt_ft'],
                        'lid_alt_m': stage_based_att_dict[lid][threshold]['lid_alt_m'],
                    }
                )
                csv_df = pd.concat([csv_df, line_df])

            except Exception:
                logging.info("ERROR: threshold has an error")
                logging.info(traceback.format_exc())
                # sys.exit(1)
                # print(e)

        # Round flow and stage columns to 2 decimal places.
        csv_df = csv_df.round({'q': 2, 'stage': 2})
        # If a site folder exists (ie a flow file was written) save files containing site attributes.
        #output_dir = os.path.join(workspace, huc, lid)
        #if os.path.exists(output_dir):
            # Export DataFrame to csv containing attributes
        csv_df.to_csv(os.path.join(attributes_dir, f'{lid}_attributes.csv'), index=False)
        #else:
        #    logging.info(f'{lid}:missing all calculated flows')

        # If it made it to this point (i.e. no continues), there were no major preventers of mapping
        logging.info(f'{lid}:OK')
        mark_complete(mapping_huc_directory)
    # Write all_messages by HUC to be scraped later.
    # if len(all_messages) > 0:
    #     messages_dir = os.path.join(workspace, 'messages')
    #     if not os.path.exists(messages_dir):
    #         os.mkdir(messages_dir)
    #     huc_messages_csv = os.path.join(messages_dir, huc + '_messages.csv')
    #     with open(huc_messages_csv, 'w') as output_csv:
    #         writer = csv.writer(output_csv)
    #         writer.writerows(all_messages)

    logging.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")


# ??? TODO: Should this have flood categories? what do we load into HV. I thought stage is not about magnitudes
def generate_stage_based_categorical_fim(
    output_catfim_dir,
    fim_run_dir,
    nwm_us_search,
    nwm_ds_search,
    env_file,
    job_number_inundate,
    job_number_huc,    
    lst_hucs,
    number_of_interval_jobs,
    past_major_interval_cap,
    log_output_file
):
    flood_categories = ['action', 'minor', 'moderate', 'major', 'record']

    output_flows_dir = os.path.join(output_catfim_dir, 'flows')
    output_mapping_dir = os.path.join(output_catfim_dir, 'mapping')
    attributes_dir = os.path.join(output_catfim_dir, 'attributes')    

    (huc_dictionary, out_gdf, ___ , threshold_url, all_lists, nwm_flows_df, nwm_flows_alaska_df) = (
        generate_catfim_flows(
            output_catfim_dir,
            nwm_us_search,
            nwm_ds_search,
            env_file,
            job_number_inundate,
            job_number_huc,    
            True,
            lst_hucs,
        )
    )
    # TEMP DEBUG
    logging.info(">>>>>>>>>>>>>>>>>")
    # print(locals())
    #print(">>>>>>>>>>>>>>>>>")    
    # print('huc_dictionary')
    # print(huc_dictionary)
    # print('out_gdf')
    # print(out_gdf)
    # print('metadata_url')
    # print(metadata_url)
    # print('threshold_url')
    # print(threshold_url)
    # print('all_lists')
    # print(all_lists)
    # print('nwm_flows_df')
    # print(nwm_flows_df)
    # print('nwm_flows_alaska_df')
    # print(nwm_flows_alaska_df)

    log_file_prefix = "MP_iterate_through_huc_stage_based"
    with ProcessPoolExecutor(max_workers=job_number_huc) as executor:
        for huc in huc_dictionary:
            if huc in lst_hucs:
                logging.info(f'Generating stage based fim for : {huc}') 

                if huc[:2] == '19':
                    # Alaska
                    executor.submit(
                        iterate_through_huc_stage_based,
                        output_catfim_dir,
                        huc,
                        fim_run_dir,
                        huc_dictionary,
                        threshold_url,
                        flood_categories,
                        all_lists,
                        past_major_interval_cap,
                        job_number_inundate,
                        number_of_interval_jobs,
                        nwm_flows_alaska_df,
                        log_output_file,
                        log_file_prefix,
                    )
                else:
                    # not Alaska
                    executor.submit(
                        iterate_through_huc_stage_based,
                        output_catfim_dir,
                        huc,
                        fim_run_dir,
                        huc_dictionary,
                        threshold_url,
                        flood_categories,
                        all_lists,
                        past_major_interval_cap,
                        job_number_inundate,
                        number_of_interval_jobs,
                        nwm_flows_df,
                        log_output_file,
                        log_file_prefix,
                    )

    # Need to merge MP logs here
    merge_log_files(log_output_file, log_file_prefix )

    logging.info('Wrapping up Stage-Based CatFIM...')
    # csv_files = os.listdir(attributes_dir)
    
    csv_files = [x for x in os.listdir(attributes_dir) if x.endswith('.csv')]
    
    all_csv_df = pd.DataFrame()
    refined_csv_files_list = []
    for csv_file in csv_files:
        full_csv_path = os.path.join(attributes_dir, csv_file)
        # HUC has to be read in as string to preserve leading zeros.
        try:
            temp_df = pd.read_csv(full_csv_path, dtype={'huc': str})
            all_csv_df = pd.concat([all_csv_df, temp_df], ignore_index=True)
            refined_csv_files_list.append(csv_file)
        except Exception:  # Happens if a file is empty (i.e. no mapping)
            logging.info("ERROR: loading csv {full_csv_path}")
            logging.info(traceback.format_exc())
            pass
        
    # Write to file
    all_csv_df.to_csv(os.path.join(output_flows_dir, 'nws_lid_attributes.csv'), index=False)
    logging.info(f".. all_csv_df saved at {all_csv_df}")    

    # This section populates a geopackage of all potential sites and details
    # whether it was mapped or not (mapped field) and if not, why (status field).

    # Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.
    viz_out_gdf = out_gdf.to_crs(VIZ_PROJECTION)  # TODO: Accomodate AK projection?
    viz_out_gdf.rename(
        columns={
            'identifiers_nwm_feature_id': 'nwm_seg',
            'identifiers_nws_lid': 'nws_lid',
            'identifiers_usgs_site_code': 'usgs_gage',
        },
        inplace=True,
    )
    viz_out_gdf['nws_lid'] = viz_out_gdf['nws_lid'].str.lower()

    # Using list of csv_files, populate DataFrame of all nws_lids that had
    # a flow file produced and denote with "mapped" column.
    nws_lids = []
    for csv_file in csv_files:
        nws_lids.append(csv_file.split('_attributes')[0])
    lids_df = pd.DataFrame(nws_lids, columns=['nws_lid'])
    lids_df['mapped'] = 'yes'

    # Identify what lids were mapped by merging with lids_df. Populate
    # 'mapped' column with 'No' if sites did not map.
    viz_out_gdf = viz_out_gdf.merge(lids_df, how='left', on='nws_lid')
    viz_out_gdf['mapped'] = viz_out_gdf['mapped'].fillna('no')

    # Create list from all messages in messages dir.
    #messages_dir = os.path.join(workspace, 'messages')
    # all_messages = []
    # all_message_csvs = os.listdir(messages_dir)
    # for message_csv in all_message_csvs:
    #     full_message_csv_path = os.path.join(messages_dir, message_csv)
    #     with open(full_message_csv_path, newline='') as message_file:
    #         reader = csv.reader(message_file)
    #         for row in reader:
    #             all_messages.append(row)

    # Filter out columns and write out to file
    nws_sites_layer = os.path.join(output_mapping_dir, 'nws_lid_sites.gpkg')
    logging.info(f".. nws_sites_layer name is {nws_sites_layer}")

    # Only write to sites geopackage if it didn't exist yet
    # (and this line shouldn't have been reached if we had an interrupted
    # run previously and are picking back up with a restart)
    # 4.4.0.0, there was no messages column in nws_lid_sites.gkpg
    # if not os.path.exists(nws_sites_layer):
        
    #     logging.info(f"nws_sites_layer does not exist")
       
    #     # FIX:  (DO WE NEED IT?)
        
    #     # Write messages to DataFrame, split into columns, aggregate messages.
    #     if len(all_messages) > 0:

    #         logging.info(f"nws_sites_layer ({nws_sites_layer}) : adding messages")
    #         messages_df = pd.DataFrame(all_messages, columns=['message'])

    #         messages_df = (
    #             messages_df['message']
    #             .str.split(':', n=1, expand=True)
    #             .rename(columns={0: 'nws_lid', 1: 'status'})
    #         )
    #         status_df = messages_df.groupby(['nws_lid'])['status'].apply(', '.join).reset_index()

    #         # Join messages to populate status field to candidate sites. Assign
    #         # status for null fields.
    #         viz_out_gdf = viz_out_gdf.merge(status_df, how='left', on='nws_lid')

    #         #    viz_out_gdf['status'] = viz_out_gdf['status'].fillna('OK')

    #         # Add acceptance criteria to viz_out_gdf before writing
    #         viz_out_gdf['acceptable_coord_acc_code_list'] = str(acceptable_coord_acc_code_list)
    #         viz_out_gdf['acceptable_coord_method_code_list'] = str(acceptable_coord_method_code_list)
    #         viz_out_gdf['acceptable_alt_acc_thresh'] = float(acceptable_alt_acc_thresh)
    #         viz_out_gdf['acceptable_alt_meth_code_list'] = str(acceptable_alt_meth_code_list)
    #         viz_out_gdf['acceptable_site_type_list'] = str(acceptable_site_type_list)

    #         viz_out_gdf.to_file(nws_sites_layer, driver='GPKG')
    #     else:
    #         logging.info(f"nws_sites_layer ({nws_sites_layer}) : has no messages")
            
    logging.info(">>>>>>>>>>>>>>>>>")

    return nws_sites_layer


def produce_stage_based_catfim_tifs(
    stage,
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
    log_output_file,
    log_file_prefix,
):
    messages = []

    if log_file_prefix != "":
        # we are setting up an MP logger,
        # sometimes this function is hit by MP and other times not
        log_folder = os.path.dirname
        if not os.path.exists(log_folder):
            os.mkdir(log_folder)
        random_id = random.randrange(10000, 99999)    
        log_file_name = f"{log_file_prefix}_{random_id}.log"
        log_output_file = os.path.join(log_folder, log_file_name)
        setup_logger(log_output_file)

    logging.info("")
    logging.info(f"{huc} - {lid} - {category}: Starting to create tiffs")

    # Determine datum-offset water surface elevation (from above).
    datum_adj_wse = stage + datum_adj_ft + lid_altitude
    datum_adj_wse_m = datum_adj_wse * 0.3048  # Convert ft to m

    # Subtract HAND gage elevation from HAND WSE to get HAND stage.
    hand_stage = datum_adj_wse_m - lid_usgs_elev

    # Produce extent tif hand_stage. Multiprocess across branches.
    # branches = os.listdir(branch_dir)
    branches = [x for x in os.listdir(branch_dir) if os.path.isdir(os.path.join(branch_dir, x)) and x[0] in ['0', '1', '2']]
    branches.sort()
    
    log_file_prefix = "MP_produce_stage_based_catfim_tifs"    
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        for branch in branches:
            logging.debug(f"{huc} -- {branch} -- {lid} -- {category}: Determining HydroID")        
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
                logging.debug(f"{lid}:rem doesn't exist")
                continue
            if not os.path.exists(catchments_path):
                logging.debug(f"{lid}:catchments files don't exist")
                continue
            if not os.path.exists(hydrotable_path):
                logging.debug(f"{lid}:hydrotable doesn't exist")
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
                    logging.debug(f"Index Error for {huc} -- {branch} -- {category}. FeatureId is {feature_id} : Continuing on.")
                    pass

            # print(f"{huc} -- {branch} -- {category}: Finished determining HydroID")

            # Some branches don't have matching hydroids
            if len(hydroid_list) == 0:
                #logging.debug(f"{lid}:no matching hydroids")
                #logging.info(f" {huc} -- {branch} -- {category} >>>> has no matching hydroids")
                continue
            #logging.info(f" {huc} -- {branch} -- {category} +++++++++++++++++ DOES have matching hydroids")
            #logging.info("")

            # If no segments, write message and exit out
            if not segments:
                logging.info(f'{lid}:missing nwm segments')
                continue

            # Create inundation maps with branch and stage data
            try:
                # print("Generating stage-based FIM for " + huc + " and branch " + branch) # TODO TEMP DEBUG UNCOMMENT THIS MAYBE AFTER DEBUGGING
                logging.info(f"{huc} -- {branch} -- {category} : Generating stage-based FIM")
                executor.submit(
                    produce_inundation_map_with_stage_and_feature_ids,
                    rem_path,
                    catchments_path,
                    hydroid_list,
                    hand_stage,
                    lid_directory,
                    category,
                    huc,
                    lid,
                    branch,
                    log_output_file,
                    log_file_prefix
                )
            except Exception:
                logging.info(f'{huc} -- {branch} -- {lid}: inundation failed at {category}')
                logging.info(traceback.format_exc())

    if log_file_prefix != "":
        merge_log_files(log_output_file, log_file_prefix )

    # -- MOSAIC -- #
    # Merge all rasters in lid_directory that have the same magnitude/category.
    path_list = []
    print(f"Merging files from {lid_directory}")
    lid_dir_list = os.listdir(lid_directory)
    logging.info(f"{huc}: Merging {category}")
    logging.debug("lid_dir_list is ")
    logging.debug(lid_dir_list)
    logging.info("")
    for f in lid_dir_list:
        if category in f:
            path_list.append(os.path.join(lid_directory, f))

    logging.debug("???")
    #logging.debug(f"path_list is (pre sort) is {path_list}")
    #path_list.sort()  # To force branch 0 first in list, sort  it isn't branchs and we don't care the order for mosaiking
    #logging.debug(f"path_list is (post sort) is {path_list}")    

    logging.debug()
    logging.info(f"len of path_list is {len(path_list)}")

    if len(path_list) > 0:
        zero_branch_grid = path_list[0]
        zero_branch_src = rasterio.open(zero_branch_grid)
        zero_branch_array = zero_branch_src.read(1)
        summed_array = zero_branch_array  # Initialize it as the branch zero array

        # Loop through remaining items in list and sum them with summed_array
        for remaining_raster in path_list[1:]:
            remaining_raster_src = rasterio.open(remaining_raster)
            logging.info(f"{huc}: {category}: Reading raster, path is {remaining_raster}")
            remaining_raster_array_original = remaining_raster_src.read(1)

            # Reproject non-branch-zero grids so I can sum them with the branch zero grid
            remaining_raster_array = np.empty(zero_branch_array.shape, dtype=np.int8)
            reproject(
                remaining_raster_array_original,
                destination=remaining_raster_array,
                src_transform=remaining_raster_src.transform,
                src_crs=remaining_raster_src.crs,  # TODO: Accomodate AK projection?
                src_nodata=remaining_raster_src.nodata,
                dst_transform=zero_branch_src.transform,
                dst_crs=zero_branch_src.crs,  # TODO: Accomodate AK projection?
                dst_nodata=-1,
                dst_resolution=zero_branch_src.res,
                resampling=Resampling.nearest,
            )
            # Sum rasters
            summed_array = summed_array + remaining_raster_array

        del zero_branch_array  # Clean up

        # Define path to merged file, in same format as expected by post_process_cat_fim_for_viz function
        output_tif = os.path.join(lid_directory, lid + '_' + category + '_extent.tif')
        profile = zero_branch_src.profile
        summed_array = summed_array.astype('uint8')
        with rasterio.open(output_tif, 'w', **profile) as dst:
            dst.write(summed_array, 1)
            logging.info(f"output_tif of {output_tif} : saved ??")
        del summed_array


    return messages, hand_stage, datum_adj_wse, datum_adj_wse_m


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM')
    parser.add_argument(
        '-f',
        '--fim_run_dir',
        help='Path to directory containing HAND outputs, e.g. /data/previous_fim/fim_4_0_9_2',
        required=True,
    )
    parser.add_argument(
        '-e',
        '--env_file',
        help='Docker mount path to the catfim environment file. ie) data/config/catfim.env',
        required=True,
    )
    parser.add_argument(
        '-jh',
        '--job_number_huc',
        help='OPTIONAL: Number of processes to use for HUC scale operations.'
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count of the'
        ' machine. CatFIM sites generally only have 2-3 branches overlapping a site, so this number can be '
        'kept low (2-4). Defaults to 1.',
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        '-jn',
        '--job_number_inundate',
        help='OPTIONAL: Number of processes to use for inundating'
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count'
        ' of the machine. Defaults to 1.',
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        '-a',
        '--stage_based',
        help='Run stage-based CatFIM instead of flow-based? Add this -a param to make it stage based,'
        ' leave it off for flow based',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-t',
        '--output_folder',
        help='OPTIONAL: Target location, Where the output folder will be. Defaults to /data/catfim/',
        required=False,
        default='/data/catfim/',
    )
    parser.add_argument(
        '-o', '--overwrite', help='OPTIONAL: Overwrite files', required=False, action="store_true"
    )
    parser.add_argument(
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. How far up and downstream do you want to go? Defaults to 5.',
        required=False,
        default='5',
    )
    
    # lid_to_run temp disabled
    # parser.add_argument(
    #     '-l',
    #     '--lid_to_run',
    #     help='OPTIONAL: NWS LID, lowercase, to produce CatFIM for. Currently only accepts one. Defaults to all sites',
    #     required=False,
    #     default='all',
    # )
    
    # lst_hucs temp disabled. All hucs in fim outputs in a directory will used
    # parser.add_argument(
    #     '-lh',
    #     '--lst_hucs',
    #     help='OPTIONAL: Space-delimited list of HUCs to produce CatFIM for. Defaults to all HUCs',
    #     required=False,
    #     default='all',
    # )

    parser.add_argument(
        '-ji',
        '--job_number_intervals',
        help='OPTIONAL: Number of processes to use for inundating multiple intervals in stage-based'
        ' inundation and interval job numbers should multiply to no more than one less than the CPU count '
        'of the machine. Defaults to 1.',
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        '-mc',
        '--past_major_interval_cap',
        help='OPTIONAL: Stage-Based Only. How many feet past major do you want to go for the interval FIMs?'
        ' of the machine. Defaults to 5.',
        required=False,
        default=5.0,
        type=float,
    )

    args = vars(parser.parse_args())
    process_generate_categorical_fim(**args)
