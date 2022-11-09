#!/usr/bin/env python3

import os
import argparse
import traceback
import sys
import time
from pathlib import Path
import geopandas as gpd
import pandas as pd
import rasterio
import glob
from generate_categorical_fim_flows import generate_catfim_flows
from generate_categorical_fim_mapping import manage_catfim_mapping, post_process_cat_fim_for_viz
from tools_shared_functions import get_thresholds, get_nwm_segs, get_datum, ngvd_to_navd_ft
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
import numpy as np
from utils.shared_variables import VIZ_PROJECTION


def process_generate_categorical_fim(fim_version, job_number_huc, job_number_inundate, 
                                 stage_based, output_folder, overwrite):

    # check job numbers
    total_cpus_requested = job_number_huc * job_number_inundate
    total_cpus_available = os.cpu_count() - 1
    if total_cpus_requested > total_cpus_available:
        raise ValueError('The HUC job number, {}, multiplied by the inundate job number, {}, '\
                          'exceeds your machine\'s available CPU count minus one. '\
                          'Please lower the job_number_huc or job_number_inundate '\
                          'values accordingly.'.format(job_number_huc, job_number_inundate) )

    # Define default arguments. Modify these if necessary
    fim_run_dir = Path(f'{fim_version}')
    fim_version_folder = os.path.basename(fim_version)
    
    # Append option configuration (flow_based or stage_based) to output folder name.
    if args['stage_based']:
        fim_version_folder += "_stage_based"
        catfim_method = "STAGE-BASED"
    else:
        fim_version_folder += "_flow_based"
        catfim_method = "FLOW-BASED"
    
    output_catfim_dir_parent = Path(f'/data/catfim_brad_testing/{fim_version_folder}')
    output_flows_dir = Path(f'/data/catfim_brad_testing/{fim_version_folder}/flows')
    output_mapping_dir = Path(f'/data/catfim_brad_testing/{fim_version_folder}/mapping')
    nwm_us_search = '5'
    nwm_ds_search = '5'
    write_depth_tiff = False
    fim_dir = args['fim_version']
    
    # Create log directory
    log_dir = os.path.join(output_catfim_dir_parent, 'logs')
    # Create error log path
    log_file = os.path.join(log_dir, 'errors.log')
    
    fim_version = os.path.split(fim_version)[1]
    
    if args['stage_based']:
        stage_based = True
        # Generate Stage-Based CatFIM mapping
        generate_stage_based_categorical_fim(output_mapping_dir, fim_version, fim_run_dir, nwm_us_search, nwm_ds_search, job_number_inundate)
    
        print("Post-processing TIFs...")
        print(fim_version)
        post_process_cat_fim_for_viz(job_number_inundate, output_mapping_dir, nws_lid_attributes_filename="", log_file=log_file, fim_version=fim_version)
    
        # Updating mapping status
        print('Updating mapping status')
        update_mapping_status(str(output_mapping_dir), str(output_flows_dir))


    ## Run CatFIM scripts in sequence
    # Generate CatFIM flow files
    else:
        fim_dir = ""
        stage_based = False
        print('Creating flow files using the ' + catfim_method + ' technique...')
        start = time.time()
#        generate_catfim_flows(output_flows_dir, nwm_us_search, nwm_ds_search, stage_based, fim_dir)
        end = time.time()
        elapsed_time = (end-start)/60
        print(f'Finished creating flow files in {elapsed_time} minutes')
        # Generate CatFIM mapping
        print('Begin mapping')
        start = time.time()
        manage_catfim_mapping(fim_run_dir, output_flows_dir, output_mapping_dir, 
                          job_number_huc, job_number_inundate, overwrite, depthtif=False)
        end = time.time()
        elapsed_time = (end-start)/60
        print(f'Finished mapping in {elapsed_time} minutes')

        # Updating mapping status
        print('Updating mapping status')
        update_mapping_status(str(output_mapping_dir), str(output_flows_dir))
    
    # Create CSV versions of the final shapefiles.
    print('Creating CSVs')
    reformatted_catfim_method = catfim_method.lower().replace('-', '_')
    create_csvs(output_mapping_dir, reformatted_catfim_method)

    


def create_csvs(output_mapping_dir, reformatted_catfim_method):
    '''
    Produces CSV versions of any shapefile in the output_mapping_dir.

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
    
    # Convert any shapefile in the root level of output_mapping_dir to CSV and rename.
    shapefile_list = glob.glob(os.path.join(output_mapping_dir, '*.shp'))
    for shapefile in shapefile_list:
        gdf = gpd.read_file(shapefile)
        parent_directory = os.path.split(shapefile)[0]
        if 'catfim_library' in shapefile:
            file_name = reformatted_catfim_method + '_catfim.csv'
        if 'nws_lid_sites' in shapefile:
            file_name = reformatted_catfim_method + '_catfim_sites.csv'
        
        csv_output_path = os.path.join(parent_directory, file_name)
        gdf.to_csv(csv_output_path)


def update_mapping_status(output_mapping_dir, output_flows_dir):
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
    empty_nws_lids = [Path(directory).name for directory in subdirs if not list(Path(directory).iterdir())]

    # Write list of empty nws_lids to DataFrame, these are sites that failed in inundation.py
    mapping_df = pd.DataFrame({'nws_lid':empty_nws_lids})
    mapping_df['did_it_map'] = 'no'
    mapping_df['map_status'] = ' and all categories failed to map'

    # Import shapefile output from flows creation
    shapefile = Path(output_flows_dir)/'nws_lid_flows_sites.shp'
    flows_df = gpd.read_file(shapefile)

    # Join failed sites to flows df
    flows_df = flows_df.merge(mapping_df, how = 'left', on = 'nws_lid')

    # Switch mapped column to no for failed sites and update status
    flows_df.loc[flows_df['did_it_map'] == 'no', 'mapped'] = 'no'
    flows_df.loc[flows_df['did_it_map']=='no','status'] = flows_df['status'] + flows_df['map_status']
    
    # Update status for nws_lid in missing hucs and change mapped attribute to 'no'
    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'status'] = flows_df['status'] + ' and all categories failed to map because missing HUC information'
    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'mapped'] = 'no'

    # Clean up GeoDataFrame and rename columns for consistency
    flows_df = flows_df.drop(columns = ['did_it_map','map_status'])
    flows_df = flows_df.rename(columns = {'nws_lid':'ahps_lid'})

    # Write out to file
    nws_lid_path = Path(output_mapping_dir) / 'nws_lid_sites.shp'
    flows_df.to_file(nws_lid_path)


def produce_inundation_map_with_stage_and_feature_ids(rem_path, catchments_path, hydroid_list, hand_stage, lid_directory, category, huc, lid, branch):
    
    # Open rem_path and catchment_path using rasterio.
    rem_src = rasterio.open(rem_path)
    catchments_src = rasterio.open(catchments_path)
    rem_array = rem_src.read(1)
    catchments_array = catchments_src.read(1)
    
    # Use numpy.where operation to reclassify rem_path on the condition that the pixel values are <= to hand_stage and the catchments
    # value is in the hydroid_list.
    reclass_rem_array = np.where((rem_array<=hand_stage) & (rem_array != rem_src.nodata), 1, 0).astype('uint8')
    hydroid_mask = np.isin(catchments_array, hydroid_list)   
    target_catchments_array = np.where((hydroid_mask == True) & (catchments_array != catchments_src.nodata), 1, 0).astype('uint8')
    masked_reclass_rem_array = np.where((reclass_rem_array == 1) & (target_catchments_array == 1), 1, 0).astype('uint8')
        
    # Save resulting array to new tif with appropriate name. brdc1_record_extent_18060005.tif
    is_all_zero = np.all((masked_reclass_rem_array == 0))
    
    if not is_all_zero:
        output_tif = os.path.join(lid_directory, lid + '_' + category + '_extent_' + huc + '_' + branch + '.tif')
        with rasterio.Env():
            profile = rem_src.profile
            profile.update(dtype=rasterio.uint8)
            profile.update(nodata=10)
            
            with rasterio.open(output_tif, 'w', **profile) as dst:
                dst.write(masked_reclass_rem_array, 1)
    
    
def generate_stage_based_categorical_fim(workspace, fim_version, fim_run_dir, nwm_us_search, nwm_ds_search, number_of_jobs):
    
    missing_huc_files = []
    all_messages = []  # TODO
    flood_categories = ['action', 'minor', 'moderate', 'major', 'record']
    stage_based_att_dict = {}

    huc_dictionary, out_gdf, ms_segs, list_of_sites, metadata_url, threshold_url, all_lists = generate_catfim_flows(workspace, nwm_us_search, nwm_ds_search, stage_based, fim_dir)
                    
    for huc in huc_dictionary:  # TODO should multiprocess at HUC level?
        # Make output directory for huc.
        huc_directory = os.path.join(workspace, huc)
        if not os.path.exists(huc_directory):
            os.mkdir(huc_directory)
        
        # Open necessary HAND and HAND-related files.
        usgs_elev_table = os.path.join(fim_dir, huc, 'usgs_elev_table.csv')
        # If usgs_elev_table doesn't exist for a HUC, append HUC to list for logging
        if not os.path.exists(usgs_elev_table):
            if huc not in missing_huc_files:
                missing_huc_files.append(huc)
            with open(os.path.join(workspace, "missing_files.txt"),"a") as f:
                f.write(usgs_elev_table + "\n")
            continue
        branch_dir = os.path.join(fim_dir, huc, 'branches')
        if not os.path.exists(branch_dir):
            with open(os.path.join(workspace, "missing_files.txt"),"a") as f:
                f.write(branch_dir + "\n")
            continue        
        # Read usgs_elev_df
        usgs_elev_df = pd.read_csv(usgs_elev_table)
            
        print(f'Iterating through {huc}')
        #Get list of nws_lids
        nws_lids = huc_dictionary[huc]
        #Loop through each lid in nws_lids list
        for lid in nws_lids:
            print(lid)
            
            #Convert lid to lower case
            lid = lid.lower()
            
            # Make lid_directory.
            lid_directory = os.path.join(huc_directory, lid)
            if not os.path.exists(lid_directory):
                os.mkdir(lid_directory)

            #Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
            stages, flows = get_thresholds(threshold_url = threshold_url, select_by = 'nws_lid', selector = lid, threshold = 'all')
            #Check if stages are supplied, if not write message and exit. 
            if all(stages.get(category, None)==None for category in flood_categories):
                message = f'{lid}:missing threshold stages'
                all_messages.append(message)
                continue
            try:
                lid_usgs_elev = usgs_elev_df.loc[usgs_elev_df['nws_lid'] == lid.upper(), 'dem_adj_elevation'].values[0]  # Assuming DEM datums are consistent across all DEMs
            except IndexError:  # Occurs when LID is missing from table
                continue
            
            # Initialize nested dict for lid attributes
            stage_based_att_dict.update({lid:{}})
                
            #find lid metadata from master list of metadata dictionaries.
            metadata = next((item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False)
            lid_altitude = metadata['usgs_data']['altitude']
       
            ### --- Do Datum Offset --- ###
            #determine source of interpolated threshold flows, this will be the rating curve that will be used.
            rating_curve_source = flows.get('source')
            if rating_curve_source is None:
                continue
                        
            #Workaround for "bmbp1" where the only valid datum is from NRLDB (USGS datum is null). Modifying rating curve source will influence the rating curve and datum retrieved for benchmark determinations.
            if lid == 'bmbp1':
                rating_curve_source = 'NRLDB'
            
            #Get the datum and adjust to NAVD if necessary.
            nws, usgs = get_datum(metadata)
            datum_data = {}
            if rating_curve_source == 'USGS Rating Depot':
                datum_data = usgs
            elif rating_curve_source == 'NRLDB':
                datum_data = nws
                        
            #If datum not supplied, skip to new site
            datum = datum_data.get('datum', None)
            if datum is None:
#                f.write(f'{lid} : skipping because site is missing datum\n')
                continue      
            
            #Custom workaround these sites have faulty crs from WRDS. CRS needed for NGVD29 conversion to NAVD88
            # USGS info indicates NAD83 for site: bgwn7, fatw3, mnvn4, nhpp1, pinn4, rgln4, rssk1, sign4, smfn7, stkn4, wlln7 
            # Assumed to be NAD83 (no info from USGS or NWS data): dlrt2, eagi1, eppt2, jffw3, ldot2, rgdt2
            if lid in ['bgwn7', 'dlrt2','eagi1','eppt2','fatw3','jffw3','ldot2','mnvn4','nhpp1','pinn4','rgdt2','rgln4','rssk1','sign4','smfn7','stkn4','wlln7' ]:
                datum_data.update(crs = 'NAD83')
            
            #Workaround for bmbp1; CRS supplied by NRLDB is mis-assigned (NAD29) and is actually NAD27. This was verified by converting USGS coordinates (in NAD83) for bmbp1 to NAD27 and it matches NRLDB coordinates.
            if lid == 'bmbp1':
                datum_data.update(crs = 'NAD27')
            
            #Custom workaround these sites have poorly defined vcs from WRDS. VCS needed to ensure datum reported in NAVD88. If NGVD29 it is converted to NAVD88.
            #bgwn7, eagi1 vertical datum unknown, assume navd88
            #fatw3 USGS data indicates vcs is NAVD88 (USGS and NWS info agree on datum value).
            #wlln7 USGS data indicates vcs is NGVD29 (USGS and NWS info agree on datum value).
            if lid in ['bgwn7','eagi1','fatw3']:
                datum_data.update(vcs = 'NAVD88')
            elif lid == 'wlln7':
                datum_data.update(vcs = 'NGVD29')
            
            #Adjust datum to NAVD88 if needed
            # Default datum_adj_ft to 0.0
            datum_adj_ft = 0.0
            if datum_data.get('vcs') in ['NGVD29', 'NGVD 1929', 'NGVD,1929', 'NGVD OF 1929', 'NGVD']:
                #Get the datum adjustment to convert NGVD to NAVD. Sites not in contiguous US are previously removed otherwise the region needs changed.
                try:
                    datum_adj_ft = ngvd_to_navd_ft(datum_info = datum_data, region = 'contiguous')
                except Exception as e:
                    all_messages.append(e)
            ### -- Concluded Datum Offset --- ###
            
            # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            segments = get_nwm_segs(metadata)
            site_ms_segs = set(segments).intersection(ms_segs)
            site_ms_segments = list(site_ms_segs)    
            
            # For each flood category
            for category in flood_categories:
                if datum_adj_ft == None:
                    datum_adj_ft = 0.0
                stage = stages[category]
                
                if stage != None and datum_adj_ft != None and lid_altitude != None:
                    # Determine datum-offset water surface elevation (from above).
                    datum_adj_wse = stage + datum_adj_ft + lid_altitude
                    datum_adj_wse_m = datum_adj_wse*0.3048  # Convert ft to m
                    
                    # Subtract HAND gage elevation from HAND WSE to get HAND stage.
                    hand_stage = datum_adj_wse_m - lid_usgs_elev
                    
                    # Produce extent tif hand_stage. Multiprocess across branches.
                    branches = os.listdir(branch_dir)
                    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
                        for branch in branches:
                            # Define paths to necessary files to produce inundation grids.
                            full_branch_path = os.path.join(branch_dir, branch)
                            rem_path = os.path.join(fim_dir, huc, full_branch_path, 'rem_zeroed_masked_' + branch + '.tif')
                            catchments_path = os.path.join(fim_dir, huc, full_branch_path, 'gw_catchments_reaches_filtered_addedAttributes_' + branch + '.tif')
                            hydrotable_path = os.path.join(fim_dir, huc, full_branch_path, 'hydroTable_' + branch + '.csv')
                            
                            if not os.path.exists(rem_path): continue
                            if not os.path.exists(catchments_path): continue
                            if not os.path.exists(hydrotable_path): continue
                            
                            # Use hydroTable to determine hydroid_list from site_ms_segments.
                            hydrotable_df = pd.read_csv(hydrotable_path)
                            hydroid_list = []
                            
                            # Determine hydroids at which to perform inundation
                            for feature_id in site_ms_segments:
                                try:
                                    subset_hydrotable_df = hydrotable_df[hydrotable_df['feature_id'] == int(feature_id)]
                                    hydroid_list += list(subset_hydrotable_df.HydroID.unique())
                                except IndexError:
                                    pass
                            if len(hydroid_list) == 0:
                                continue

                            #if no segments, write message and exit out
                            if not segments:
                                print(f'{lid} no segments')
                                message = f'{lid}:missing nwm segments'
                                all_messages.append(message)
                                continue
                            
                            if not os.path.exists(hydrotable_path):
                                print("hydrotable doesn't exist")
                                continue
                            # Create inundation maps with branch and stage data
                            try:
                                print("Running inundation for " + huc + " and branch " + branch)
                                executor.submit(produce_inundation_map_with_stage_and_feature_ids, rem_path, catchments_path, hydroid_list, hand_stage, lid_directory, category, huc, lid, branch)
                            except Exception as ex:
                                print(f"*** {ex}")
                                traceback.print_exc()
                                sys.exit(1)
                                
                    # Merge all rasters in lid_directory that have the same magnitude/category.
                    path_list = []
                    lid_dir_list = os.listdir(lid_directory)
                    print("Merging " + category)
                    for f in lid_dir_list:
                        if category in f:
                            path_list.append(os.path.join(lid_directory, f))
                    path_list.sort()  # To force branch 0 first in list, sort
                    
                    if len(path_list) > 0:
                        zero_branch_grid = path_list[0]
                        zero_branch_src = rasterio.open(zero_branch_grid)
                        zero_branch_array = zero_branch_src.read(1)
                        summed_array = zero_branch_array  # Initialize it as the branch zero array
                        
                        # Loop through remaining items in list and sum them with summed_array
                        for remaining_raster in path_list[1:]:
                            remaining_raster_src = rasterio.open(remaining_raster)
                            remaining_raster_array_original = remaining_raster_src.read(1)
                        
                            # Reproject non-branch-zero grids so I can sum them with the branch zero grid
                            remaining_raster_array = np.empty(zero_branch_array.shape, dtype=np.int8)
                            reproject(remaining_raster_array_original,
                                  destination = remaining_raster_array,
                                  src_transform = remaining_raster_src.transform,
                                  src_crs = remaining_raster_src.crs,
                                  src_nodata = remaining_raster_src.nodata,
                                  dst_transform = zero_branch_src.transform,
                                  dst_crs = zero_branch_src.crs,
                                  dst_nodata = -1,
                                  dst_resolution = zero_branch_src.res,
                                  resampling = Resampling.nearest)
                            # Sum rasters
                            summed_array = summed_array + remaining_raster_array
                            
                        del zero_branch_array  # Clean up
                        
                        # Define path to merged file, in same format as expected by post_process_cat_fim_for_viz function
                        output_tif = os.path.join(lid_directory, lid + '_' + category + '_extent.tif')  
                        profile = zero_branch_src.profile
                        summed_array = summed_array.astype('uint8')
                        with rasterio.open(output_tif, 'w', **profile) as dst:
                            dst.write(summed_array, 1)
                        del summed_array
                                                    
                    # Extra metadata for alternative CatFIM technique. TODO Revisit because branches complicate things
                    stage_based_att_dict[lid].update({category: {'datum_adj_wse_ft': datum_adj_wse,
                                                                 'datum_adj_wse_m': datum_adj_wse_m,
                                                                 'hand_stage': hand_stage,
                                                                 'datum_adj_ft': datum_adj_ft,
                                                                 'lid_alt_ft': lid_altitude,
                                                                 'lid_alt_m': lid_altitude*0.3048}})
                    
                # If missing HUC file data, write message
                if huc in missing_huc_files:
                    all_messages.append("Missing some HUC data")
                    
            lat = float(metadata['nws_preferred']['latitude'])
            lon = float(metadata['nws_preferred']['longitude'])
            wfo = metadata['nws_data']['wfo']
            rfc = metadata['nws_data']['rfc']
            state = metadata['nws_data']['state']
            county = metadata['nws_data']['county']
            name = metadata['nws_data']['name']
            flow_units = flows['units']
            flow_source = flows['source']
            stage_units = stages['units']
            stage_source = stages['source']
            wrds_timestamp = stages['wrds_timestamp']
            nrldb_timestamp = metadata['nrldb_timestamp']
            nwis_timestamp = metadata['nwis_timestamp']
                        
            #Create a csv with same information as shapefile but with each threshold as new record.
            csv_df = pd.DataFrame()
            for threshold in flood_categories:
                try:
                    datum_adj_ft = stage_based_att_dict[lid][threshold]['datum_adj_ft']
                    datum_adj_wse_ft = stage_based_att_dict[lid][threshold]['datum_adj_wse_ft']
                    datum_adj_wse_m = stage_based_att_dict[lid][threshold]['datum_adj_wse_m']
                    lid_alt_ft = stage_based_att_dict[lid][threshold]['lid_alt_ft']
                    lid_alt_m = stage_based_att_dict[lid][threshold]['lid_alt_m']

                    line_df = pd.DataFrame({'nws_lid': [lid], 'name':name, 'WFO': wfo, 'rfc':rfc, 'huc':[huc], 'state':state, 'county':county, 'magnitude': threshold, 'q':flows[threshold], 'q_uni':flows['units'], 'q_src':flow_source, 'stage':stages[threshold], 'stage_uni':stages['units'], 's_src':stage_source, 'wrds_time':wrds_timestamp, 'nrldb_time':nrldb_timestamp,'nwis_time':nwis_timestamp, 'lat':[lat], 'lon':[lon],
                                        'dtm_adj_ft': datum_adj_ft,
                                        'dadj_w_ft': datum_adj_wse_ft,
                                        'dadj_w_m': datum_adj_wse_m,
                                        'lid_alt_ft': lid_alt_ft,
                                        'lid_alt_m': lid_alt_m})
                    csv_df = csv_df.append(line_df)
                    
                except Exception as e:
                    print(e)
              
            #Round flow and stage columns to 2 decimal places.
            csv_df = csv_df.round({'q':2,'stage':2})

            #If a site folder exists (ie a flow file was written) save files containing site attributes.
            output_dir = workspace / huc / lid
            if output_dir.exists():
                #Export DataFrame to csv containing attributes
                csv_df.to_csv(output_dir / f'{lid}_attributes.csv', index = False)
            else:
                message = f'{lid}:missing all calculated flows'
                all_messages.append(message)
                
    print('Wrapping up Stage-Based CatFIM...')
    #Recursively find all *_attributes csv files and append
    csv_files = list(workspace.rglob('*_attributes.csv'))
    all_csv_df = pd.DataFrame()
    
    for csv in csv_files:
        #Huc has to be read in as string to preserve leading zeros.
        try:
            print("Opening temp_df")
            temp_df = pd.read_csv(csv, dtype={'huc':str})
            all_csv_df = all_csv_df.append(temp_df, ignore_index = True)
        except Exception:  # Happens if a file is empty (i.e. no mapping)
            pass
    #Write to file
    all_csv_df.to_csv(workspace / 'nws_lid_attributes.csv', index = False)
   
    #This section populates a shapefile of all potential sites and details
    #whether it was mapped or not (mapped field) and if not, why (status field).
    
    print("HERE!@")
    print(out_gdf)
    #Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.
    viz_out_gdf = out_gdf.to_crs(VIZ_PROJECTION)
    print(viz_out_gdf.columns)
    viz_out_gdf.rename(columns = {'identifiers_nwm_feature_id': 'nwm_seg', 'identifiers_nws_lid':'nws_lid', 'identifiers_usgs_site_code':'usgs_gage'}, inplace = True)
    print(viz_out_gdf.columns)
    viz_out_gdf['nws_lid'] = viz_out_gdf['nws_lid'].str.lower()
    
    #Using list of csv_files, populate DataFrame of all nws_lids that had
    #a flow file produced and denote with "mapped" column.
    nws_lids = [file.stem.split('_attributes')[0] for file in csv_files]
    lids_df = pd.DataFrame(nws_lids, columns = ['nws_lid'])
    lids_df['mapped'] = 'yes'
    print("Right here now")
    
    
    #Identify what lids were mapped by merging with lids_df. Populate 
    #'mapped' column with 'No' if sites did not map.
    viz_out_gdf = viz_out_gdf.merge(lids_df, how = 'left', on = 'nws_lid')    
    viz_out_gdf['mapped'] = viz_out_gdf['mapped'].fillna('no')
    
    #Filter out columns and write out to file
    viz_out_gdf = viz_out_gdf.filter(['nws_lid','usgs_gage','nwm_seg','HUC8','mapped','geometry'])
    viz_out_gdf.to_file(workspace /'nws_lid_flows_sites.shp')
    

if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Run Categorical FIM')
    
    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Run Categorical FIM')
    parser.add_argument('-f', '--fim_version', help='Path to directory containing outputs of fim_run.sh',
                        required=True)
    parser.add_argument('-jh','--job_number_huc',help='Number of processes to use for HUC scale operations.'\
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count of the'\
        ' machine.', required=False, default=1, type=int)
    parser.add_argument('-jn','--job_number_inundate', help='Number of processes to use for inundating'\
        ' HUC and inundation job numbers should multiply to no more than one less than the CPU count'\
        ' of the machine.', required=False, default=1, type=int)    
    parser.add_argument('-a', '--stage_based', help = 'Run stage-based CatFIM instead of flow-based?'\
        ' NOTE: flow-based CatFIM is the default.', required=False, default=False, action='store_true')
    parser.add_argument('-t', '--output_folder', help = 'Target: Where the output folder will be',
                        required = False, default = '/data/catfim/')
    parser.add_argument('-o','--overwrite', help='Overwrite files', required=False, action="store_true")
    
    args = vars(parser.parse_args())
    process_generate_categorical_fim(**args)

