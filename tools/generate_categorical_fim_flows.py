#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import numpy as np
import geopandas as gpd
import time
from tools_shared_functions import aggregate_wbd_hucs, mainstem_nwm_segs, get_thresholds, flow_data, get_metadata, get_nwm_segs, get_datum, ngvd_to_navd_ft
import argparse
from dotenv import load_dotenv
import os
import sys
sys.path.append('/foss_fim/src')
from utils.shared_variables import VIZ_PROJECTION

EVALUATED_SITES_CSV = r'/data/inputs/ahps_sites/evaluated_ahps_sites.csv'


def get_env_paths():
    load_dotenv()
    #import variables from .env file
    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    return API_BASE_URL, WBD_LAYER


def generate_catfim_flows(workspace, nwm_us_search, nwm_ds_search, alt_catfim, fim_dir):
    '''
    This will create static flow files for all nws_lids and save to the 
    workspace directory with the following format:
    huc code
        nws_lid_code
            threshold (action/minor/moderate/major if they exist/are defined by WRDS)
                flow file (ahps_{lid code}_huc_{huc 8 code}_flows_{threshold}.csv)
    
    This will use the WRDS API to get the nwm segments as well as the flow 
    values for each threshold at each nws_lid and then create the necessary 
    flow file to use for inundation mapping.

    Parameters
    ----------
    workspace : STR
        Location where output flow files will exist.
    nwm_us_search : STR
        Upstream distance (in miles) for walking up NWM network.
    nwm_ds_search : STR
        Downstream distance (in miles) for walking down NWM network.
    wbd_path : STR
        Location of HUC geospatial data (geopackage).
        
    Returns
    -------
    None.

    '''
        
    all_start = time.time()
    #Define workspace and wbd_path as a pathlib Path. Convert search distances to integer.
    workspace = Path(workspace)
    nwm_us_search = int(nwm_us_search)
    nwm_ds_search = int(nwm_ds_search)
    metadata_url = f'{API_BASE_URL}/metadata'
    threshold_url = f'{API_BASE_URL}/nws_threshold'
    ###################################################################
    
    #Create workspace
    workspace.mkdir(parents=True,exist_ok = True)

    print('Retrieving metadata...')
    #Get metadata for 'CONUS'
    print(metadata_url)
#    conus_list, conus_dataframe = get_metadata(metadata_url, select_by = 'nws_lid', selector = ['all'], must_include = 'nws_data.rfc_forecast_point', upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search )

#    #Get metadata for Islands
#    islands_list, islands_dataframe = get_metadata(metadata_url, select_by = 'state', selector = ['HI','PR'] , must_include = None, upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search)
#    
#    #Append the dataframes and lists
#    all_lists = conus_list + islands_list
#    
    # TEMP CODE
    import pickle
    file_name = r'/data/temp/brad/alternate_catfim_temp_files/all_lists.pkl'
#    open_file = open(file_name, "wb")
#    pickle.dump(all_lists, open_file)
#    open_file.close()
    
    open_file = open(file_name, "rb")
    all_lists = pickle.load(open_file)
    open_file.close()
    
    
#    all_lists = all_lists[:4]  #TODO Remove, only used for testing
    
    print('Determining HUC using WBD layer...')
    #Assign HUCs to all sites using a spatial join of the FIM 3 HUC layer. 
    #Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
    #of all sites used later in script.
    huc_dictionary, out_gdf = aggregate_wbd_hucs(metadata_list = all_lists, wbd_huc8_path = WBD_LAYER)

    import json
#    print("Writing huc dictionary")
#    with open(r'/data/temp/brad/alternate_catfim_temp_files/huc_dictionary.json', "w") as outfile:
#        json.dump(huc_dictionary, outfile)

#    with open(r'/data/temp/brad/alternate_catfim_temp_files/huc_dictionary.json') as json_file:
#        huc_dictionary = json.load(json_file)

    #Get all possible mainstem segments
    print('Getting list of mainstem segments')
    #Import list of evaluated sites
#    list_of_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].to_list()
#    #The entire routine to get mainstems is hardcoded in this function.
#    ms_segs = mainstem_nwm_segs(metadata_url, list_of_sites)
#    print(type(ms_segs))
#    print(ms_segs)
    
#    with open(r'/data/temp/brad/alternate_catfim_temp_files/ms_segs.txt', "w") as f:
#        f.write(str(ms_segs))  # set of numbers & a tuple
        
    import ast
    with open('/data/temp/brad/alternate_catfim_temp_files/ms_segs.txt','r') as f:
       ms_segs = ast.literal_eval(f.read())
      
    
    #Loop through each huc unit, first define message variable and flood categories.
    all_messages = []
    flood_categories = ['action', 'minor', 'moderate', 'major', 'record']
    for huc in huc_dictionary:
        
        if alt_catfim:  # Only need to read in hydroTable if running in alt mode.
            # Get path to relevant synthetic rating curve.
            hydroTable_path = os.path.join(fim_dir, huc, 'hydroTable.csv')
            if not os.path.exists(hydroTable_path):
                continue
            hydroTable = pd.read_csv(
                         hydroTable_path,
                         dtype={'HUC':str,'feature_id':str,
                                 'HydroID':str,'stage':float,
                                 'discharge_cms':float,'LakeID' : int, 
                                 'last_updated':object,'submitter':object,'adjust_ManningN':object,'obs_source':object}
                        )

            catchments_path = os.path.join(fim_dir, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
            if not os.path.exists(catchments_path):
                continue
            catchments_poly = gpd.read_file(catchments_path)

        print(f'Iterating through {huc}')
        #Get list of nws_lids
        nws_lids = huc_dictionary[huc]
        #Loop through each lid in list to create flow file
        for lid in nws_lids:
            #Convert lid to lower case
            lid = lid.lower()
            #Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
            stages, flows = get_thresholds(threshold_url = threshold_url, select_by = 'nws_lid', selector = lid, threshold = 'all')
            #Check if stages are supplied, if not write message and exit. 
            if all(stages.get(category, None)==None for category in flood_categories):
                message = f'{lid}:missing threshold stages'
                all_messages.append(message)
                continue
            #Check if calculated flows are supplied, if not write message and exit.
            if all(flows.get(category, None) == None for category in flood_categories):
                message = f'{lid}:missing calculated flows'
                all_messages.append(message)
                continue

            #find lid metadata from master list of metadata dictionaries (line 66).
            metadata = next((item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False)
            from pprint import pprint
#            pprint(metadata)
       
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
#                print(metadata)
            
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
                    
#                datum88 = round(datum + datum_adj_ft, 2)
#            else:
#                datum88 = datum
            
            ### -- Concluded Datum Offset --- ###
            
            #Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            segments = get_nwm_segs(metadata)        
            site_ms_segs = set(segments).intersection(ms_segs)
            segments = list(site_ms_segs)    
            nwm_feature_id = metadata['identifiers']['nwm_feature_id']
            
            longitude = metadata['usgs_data']['longitude']
            latitude = metadata['usgs_data']['latitude']
            
            df = pd.DataFrame(
                            {'feature_id': [nwm_feature_id],
                             'Latitude': [latitude],
                             'Longitude': [longitude]})
            point_gdf = gpd.GeoDataFrame(df, crs=4269, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))  # TODO dynamic CRS determination
            point_gdf = point_gdf.to_crs(catchments_poly.crs)
#            point_gdf.to_crs(catchments_poly.crs)

            closest_catchment = gpd.sjoin(point_gdf, catchments_poly, how='left', op='within').reset_index(drop=True)
            hydroid = closest_catchment.iloc[0]['HydroID']
            hydroid = str(hydroid)
        
            # Subset by Hydroid
            subset_hydroTable = hydroTable.loc[hydroTable['HydroID'] == hydroid]
            hand_stage_array = subset_hydroTable[["stage"]].to_numpy()
            hand_flow_array = subset_hydroTable[["discharge_cms"]].to_numpy()
            hand_stage_array = hand_stage_array[:, 0]
            hand_flow_array = hand_flow_array[:, 0]

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
                if alt_catfim:
                    stage = stages[category]
                    if len(hand_stage_array) > 0 and stage != None and datum_adj_ft != None:
                        # Determine datum-offset stage (from above).
                        datum_adj_stage = stage + datum_adj_ft
                        datum_adj_stage_m = datum_adj_stage*0.3048  # Convert ft to m
                        
                        # Interpolate flow value for offset stage.
                        interpolated_hand_flow = np.interp(datum_adj_stage_m, hand_stage_array, hand_flow_array)
                        stage = stages[category]

                        #round flow to nearest hundredth
                        flow = round(interpolated_hand_flow,2)
                        #Create the guts of the flow file.
                        flow_info = flow_data(segments,flow,convert_to_cms=False)
                        #Define destination path and create folders
                        output_file = workspace / huc / lid / category / (f'ahps_{lid}_huc_{huc}_flows_{category}.csv') 
                        output_file.parent.mkdir(parents = True, exist_ok = True)
                        #Write flow file to file
                        flow_info.to_csv(output_file, index = False)
                    else:
                        message = f'{lid}:{category} no stage information available'
                        all_messages.append(message)
                    
                else:  # If running in default mode
                    #Get the flow
                    flow = flows[category]
                    #If there is a valid flow value, write a flow file.
                    if flow:
                        #round flow to nearest hundredth
                        flow = round(flow,2)
                        #Create the guts of the flow file.
                        flow_info = flow_data(segments,flow)
                        #Define destination path and create folders
                        output_file = workspace / huc / lid / category / (f'ahps_{lid}_huc_{huc}_flows_{category}.csv') 
                        output_file.parent.mkdir(parents = True, exist_ok = True)
                        #Write flow file to file
                        flow_info.to_csv(output_file, index = False)
                    else:
                        message = f'{lid}:{category} is missing calculated flow'
                        all_messages.append(message)

            #Get various attributes of the site.
            lat = float(metadata['usgs_preferred']['latitude'])
            lon = float(metadata['usgs_preferred']['longitude'])
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
        print()
        print()
    print('wrapping up...')
    #Recursively find all *_attributes csv files and append
    csv_files = list(workspace.rglob('*_attributes.csv'))
    all_csv_df = pd.DataFrame()
    for csv in csv_files:
        #Huc has to be read in as string to preserve leading zeros.
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
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Create forecast files for all nws_lid sites')
    parser.add_argument('-w', '--workspace', help = 'Workspace where all data will be stored.', required = True)
    parser.add_argument('-u', '--nwm_us_search',  help = 'Walk upstream on NWM network this many miles', required = True)
    parser.add_argument('-d', '--nwm_ds_search', help = 'Walk downstream on NWM network this many miles', required = True)
    parser.add_argument('-a', '--alt-catfim', help = 'Run alternative CatFIM that bypasses synthetic rating curves?', required=False, default=False, action='store_true')
    parser.add_argument('-f', '--fim-dir', help='Path to FIM outputs directory. Only use this option if you are running in alt-catfim mode.',required=False,default="")
    args = vars(parser.parse_args())


    #Run get_env_paths and static_flow_lids
    API_BASE_URL, WBD_LAYER = get_env_paths()
    generate_catfim_flows(**args)
