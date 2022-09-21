#!/usr/bin/env python3

import os
import subprocess
import argparse
import time
from pathlib import Path
import geopandas as gpd
import pandas as pd
import rasterio
import glob
from generate_categorical_fim_flows import generate_catfim_flows
from tools_shared_functions import aggregate_wbd_hucs, mainstem_nwm_segs, get_thresholds, flow_data, get_metadata, get_nwm_segs, get_datum, ngvd_to_navd_ft
import numpy as np
from utils.shared_variables import VIZ_PROJECTION


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

    # Perform pass for HUCs where mapping was skipped due to missing data  #TODO check with Brian
    flows_hucs = [i.stem for i in Path(output_flows_dir).iterdir() if i.is_dir()]
    mapping_hucs = [i.stem for i in Path(output_mapping_dir).iterdir() if i.is_dir()]
    missing_mapping_hucs = list(set(flows_hucs) - set(mapping_hucs))
    
    # Update status for nws_lid in missing hucs and change mapped attribute to 'no'
    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'status'] = flows_df['status'] + ' and all categories failed to map because missing HUC information'
    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'mapped'] = 'no'

    # Clean up GeoDataFrame and rename columns for consistency
    flows_df = flows_df.drop(columns = ['did_it_map','map_status'])
    flows_df = flows_df.rename(columns = {'nws_lid':'ahps_lid'})

    # Write out to file
    nws_lid_path = Path(output_mapping_dir) / 'nws_lid_sites.shp'
    flows_df.to_file(nws_lid_path)


def produce_inundation_map_with_stage_and_feature_ids(rem_path, catchments_path, hydroid_list, hand_stage, lid_directory, category, huc, lid):
    # Open rem_path and catchment_path using rasterio.
    rem_src = rasterio.open(rem_path)
    catchments_src = rasterio.open(catchments_path)
    rem_array = rem_src.read(1)
    catchments_array = catchments_src.read(1)
    
    # Use numpy.where operation to reclassify rem_path on the condition that the pixel values are <= to hand_stage and the catchments
    # value is in the hydroid_list.
    reclass_rem_array = np.where((rem_array<=hand_stage) & (rem_array != rem_src.nodata), 1, 0).astype('uint8')
    
#    output_tif1 = os.path.join(lid_directory, lid + '_' + category + '_rem_reclass_' + huc + '.tif')
#    with rasterio.Env():
#        profile = rem_src.profile
#        profile.update(dtype=rasterio.uint8)
#        profile.update(nodata=10)
#        with rasterio.open(output_tif1, 'w', **profile) as dst:
#            dst.write(reclass_rem_array, 1)
            
    print(hydroid_list)

    min_hydroid = min(hydroid_list)
    max_hydroid = max(hydroid_list)
    target_catchments_array = np.where((catchments_array >= min_hydroid) & (catchments_array <= max_hydroid) & (catchments_array != catchments_src.nodata), 1, 0).astype('uint8')

#    output_tif2 = os.path.join(lid_directory, lid + '_' + category + '_target_cats_' + huc + '.tif')
#    with rasterio.Env():
#        profile = catchments_src.profile
#        profile.update(dtype=rasterio.uint8)
#        profile.update(nodata=10)
#        with rasterio.open(output_tif2, 'w', **profile) as dst:
#            dst.write(target_catchments_array, 1)
    
    masked_reclass_rem_array = np.where((reclass_rem_array == 1) & (target_catchments_array == 1), 1, 0).astype('uint8')
        
    # Save resulting array to new tif with appropriate name. brdc1_record_extent_18060005.tif
    is_all_zero = np.all((masked_reclass_rem_array == 0))
    
    if not is_all_zero:
        print(lid + " at " + category + " in " + huc + " is not all zero")
        output_tif = os.path.join(lid_directory, lid + '_' + category + '_extent_' + huc + '.tif')
        with rasterio.Env():
            profile = rem_src.profile
            profile.update(dtype=rasterio.uint8)
            profile.update(nodata=10)
            
            with rasterio.open(output_tif, 'w', **profile) as dst:
                dst.write(masked_reclass_rem_array, 1)
    
    
def generate_stage_based_categorical_fim(workspace, fim_version, fim_run_dir, nwm_us_search, nwm_ds_search):
    
    stage_based = True
    missing_huc_files = []
    all_messages = []
    flood_categories = ['action', 'minor', 'moderate', 'major', 'record']
    stage_based_att_dict = {}

    huc_dictionary, out_gdf, ms_segs, list_of_sites, metadata_url, threshold_url, all_lists = generate_catfim_flows(workspace, nwm_us_search, nwm_ds_search, stage_based, fim_dir)
    
    for huc in huc_dictionary:
        
        # Make output directory for huc.
        huc_directory = os.path.join(workspace, huc)
        if not os.path.exists(huc_directory):
            os.mkdir(huc_directory)
        
        if stage_based:  # Only need to read in hydroTable if running in alt mode.
            
            catchments_path = os.path.join(fim_dir, huc, 'gw_catchments_reaches_filtered_addedAttributes.tif')
            if not os.path.exists(catchments_path):
                continue
            hydrotable_path = os.path.join(fim_dir, huc, 'hydroTable.csv')
            if not os.path.exists(hydrotable_path):
                continue
            rem_path = os.path.join(fim_dir, huc, 'rem_zeroed_masked.tif')
            if not os.path.exists(rem_path):
                continue
            usgs_elev_table = os.path.join(fim_dir, huc, 'usgs_elev_table.csv')
            if not os.path.exists(usgs_elev_table):
                if huc not in missing_huc_files:
                    missing_huc_files.append(huc)
                with open(os.path.join(workspace, "missing_files.txt"),"a") as f:
                    f.write(usgs_elev_table + "\n")
                continue
            usgs_elev_df = pd.read_csv(usgs_elev_table)
            
        print(f'Iterating through {huc}')
        #Get list of nws_lids
        nws_lids = huc_dictionary[huc]
        #Loop through each lid in list to create flow file
        for lid in nws_lids:
            
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
                hydroid = usgs_elev_df.loc[usgs_elev_df['nws_lid'] == lid.upper(), 'HydroID'].values[0]
            except IndexError:  # Occurs when LID is missing from table
                continue
            
            stage_based_att_dict.update({lid:{}})
                
            #find lid metadata from master list of metadata dictionaries (line 66).
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
            
            #Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            segments = get_nwm_segs(metadata)        
            site_ms_segs = set(segments).intersection(ms_segs)
            site_ms_segments = list(site_ms_segs)    
            
            # Use hydroTable to determine hydroid_list from site_ms_segments.
            hydrotable_df = pd.read_csv(hydrotable_path)
            hydroid_list = []
#            print(hydrotable_df.dtypes)
            
            for feature_id in site_ms_segments:
                try:
                    nwm_crosswalked_hydroid = hydrotable_df.loc[hydrotable_df['feature_id'] == int(feature_id), 'HydroID'].values[0]
                    hydroid_list.append(nwm_crosswalked_hydroid)
                except IndexError:
                    pass
            print("NWM")
            print(site_ms_segments)
            print("Hydroids")
            print(hydroid_list)
            print("")
            print("")
        
            #if no segments, write message and exit out
            if not segments:
                print(f'{lid} no segments')
                message = f'{lid}:missing nwm segments'
                all_messages.append(message)
                continue
            #For each flood category
            for category in flood_categories:
                
                # If running in the alternative CatFIM mode, then determine flows using the
                # HAND synthetic rating curves, looking up the corresponding flows for datum-offset
                # AHPS stage values.
                if datum_adj_ft == None:
                    datum_adj_ft = 0.0
                stage = stages[category]
                
                if stage != None and datum_adj_ft != None and lid_altitude != None:
                    # Determine datum-offset water surface elevation (from above).
                    datum_adj_wse = stage + datum_adj_ft + lid_altitude
                    datum_adj_wse_m = datum_adj_wse*0.3048  # Convert ft to m
                    
                    # Subtract HAND gage elevation from HAND WSE to get HAND stage.
                    hand_stage = datum_adj_wse_m - lid_usgs_elev
                    print(hand_stage)
                    
                    # Produce extent tif hand_stage.
                    produce_inundation_map_with_stage_and_feature_ids(rem_path, catchments_path, hydroid_list, hand_stage, lid_directory, category, huc, lid)
                    
                    # Extra metadata for alternative CatFIM technique.
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
                if stage_based:
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
                else:
                    line_df = pd.DataFrame({'nws_lid': [lid], 'name':name, 'WFO': wfo, 'rfc':rfc, 'huc':[huc], 'state':state, 'county':county, 'magnitude': threshold, 'q':flows[threshold], 'q_uni':flows['units'], 'q_src':flow_source, 'stage':stages[threshold], 'stage_uni':stages['units'], 's_src':stage_source, 'wrds_time':wrds_timestamp, 'nrldb_time':nrldb_timestamp,'nwis_time':nwis_timestamp, 'lat':[lat], 'lon':[lon]})
                    csv_df = csv_df.append(line_df)
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
                
    print('wrapping up...')
    #Recursively find all *_attributes csv files and append
    csv_files = list(workspace.rglob('*_attributes.csv'))
    all_csv_df = pd.DataFrame()
    for csv in csv_files:
        #Huc has to be read in as string to preserve leading zeros.
        print(csv)
        temp_df = pd.read_csv(csv, dtype={'huc':str})
        all_csv_df = all_csv_df.append(temp_df, ignore_index = True)
    #Write to file
    all_csv_df.to_csv(workspace / 'nws_lid_attributes.csv', index = False)
   
    #This section populates a shapefile of all potential sites and details
    #whether it was mapped or not (mapped field) and if not, why (status field).
    
    #Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.
    viz_out_gdf = out_gdf.to_crs(VIZ_PROJECTION)    
    viz_out_gdf.rename(columns = {'identifiers_nwm_feature_id': 'nwm_seg', 'identifiers_nws_lid':'nws_lid', 'identifiers_usgs_site_code':'usgs_gage'}, inplace = True)
    viz_out_gdf['nws_lid'] = viz_out_gdf['nws_lid'].str.lower()
    
    #Using list of csv_files, populate DataFrame of all nws_lids that had
    #a flow file produced and denote with "mapped" column.
    nws_lids = [file.stem.split('_attributes')[0] for file in csv_files]
    lids_df = pd.DataFrame(nws_lids, columns = ['nws_lid'])
    lids_df['mapped'] = 'yes'
    
    #Identify what lids were mapped by merging with lids_df. Populate 
    #'mapped' column with 'No' if sites did not map.
    viz_out_gdf = viz_out_gdf.merge(lids_df, how = 'left', on = 'nws_lid')    
    viz_out_gdf['mapped'] = viz_out_gdf['mapped'].fillna('no')
    
    #Write messages to DataFrame, split into columns, aggregate messages.
    messages_df  = pd.DataFrame(all_messages, columns = ['message'])
    messages_df = messages_df['message'].str.split(':', n = 1, expand = True).rename(columns={0:'nws_lid', 1:'status'})   
    status_df = messages_df.groupby(['nws_lid'])['status'].apply(', '.join).reset_index()
    
    #Join messages to populate status field to candidate sites. Assign 
    #status for null fields.
    viz_out_gdf = viz_out_gdf.merge(status_df, how = 'left', on = 'nws_lid')
    viz_out_gdf['status'] = viz_out_gdf['status'].fillna('all calculated flows available')
    
    #Filter out columns and write out to file
    viz_out_gdf = viz_out_gdf.filter(['nws_lid','usgs_gage','nwm_seg','HUC8','mapped','status','geometry'])
    viz_out_gdf.to_file(workspace /'nws_lid_flows_sites.shp')
    
    #time operation
    all_end = time.time()
    print(f'total time is {round((all_end - all_start)/60),1} minutes')
    

if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Run Categorical FIM')
    parser.add_argument('-f','--fim_version',help='Path to directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-j','--number_of_jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    parser.add_argument('-a', '--stage_based', help = 'Run stage-based CatFIM instead of flow-based? NOTE: flow-based CatFIM is the default.', required=False, default=False, action='store_true')
    args = vars(parser.parse_args())

    # Get arguments
    fim_version = args['fim_version']
    number_of_jobs = args['number_of_jobs']

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
    
    output_flows_dir = Path(f'/data/catfim/{fim_version_folder}/flows')
    output_mapping_dir = Path(f'/data/catfim/{fim_version_folder}/mapping')
    nwm_us_search = '5'
    nwm_ds_search = '5'
    write_depth_tiff = False

    ## Run CatFIM scripts in sequence
    # Generate CatFIM flow files
    start = time.time()
    if args['stage_based']:
        fim_dir = args['fim_version']
        generate_stage_based_categorical_fim(output_mapping_dir, fim_version, fim_run_dir, nwm_us_search, nwm_ds_search)        
    else:
        subprocess.call(['python3','/foss_fim/tools/generate_categorical_fim_flows.py', '-w' , str(output_flows_dir), '-u', nwm_us_search, '-d', nwm_ds_search])
    end = time.time()
    elapsed_time = (end-start)/60
    print(f'Finished creating flow files in {elapsed_time} minutes')

    # Generate CatFIM mapping
    print('Begin mapping')
    start = time.time()
    subprocess.call(['python3','/foss_fim/tools/generate_categorical_fim_mapping.py', '-r' , str(fim_run_dir), '-s', str(output_flows_dir), '-o', str(output_mapping_dir), '-j', str(number_of_jobs)])
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
