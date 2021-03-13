#!/usr/bin/env python3
from pathlib import Path
import geopandas as gpd
import pandas as pd
import time
from tools_shared_functions import aggregate_wbd_hucs, mainstem_nwm_segs, get_thresholds, flow_data, get_metadata, get_nwm_segs, flow_data
import argparse
from dotenv import load_dotenv
import os
import sys
sys.path.append('/foss_fim/src')
from utils.shared_variables import PREP_PROJECTION,VIZ_PROJECTION

load_dotenv()
#import variables from .env file
API_BASE_URL = os.getenv("API_BASE_URL")
EVALUATED_SITES_CSV = os.getenv("EVALUATED_SITES_CSV")
WBD_LAYER = os.getenv("WBD_LAYER")

def static_flow_lids(workspace, nwm_us_search, nwm_ds_search):
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
    workspace.mkdir(exist_ok = True)

    #Return dictionary of huc (key) and sublist of ahps(value) as well as geodataframe of sites.
    print('Retrieving metadata...')
    #Get metadata for 'CONUS'
    conus_list, conus_dataframe = get_metadata(metadata_url, select_by = 'nws_lid', selector = ['all'], must_include = 'nws_data.rfc_forecast_point', upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search )

    #Get metadata for Islands
    islands_list, islands_dataframe = get_metadata(metadata_url, select_by = 'state', selector = ['HI','PR'] , must_include = None, upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search)
    
    #Append the dataframes and lists
    all_lists = conus_list + islands_list
    all_dataframe = conus_dataframe.append(islands_dataframe)
    
    print('Determining HUC using WBD layer...')
    #Assign FIM HUC to GeoDataFrame and export to shapefile all candidate sites.
    agg_start = time.time()
    huc_dictionary, out_gdf = aggregate_wbd_hucs(metadata_list = all_lists, wbd_huc8_path = WBD_LAYER)
    viz_out_gdf = out_gdf.to_crs(VIZ_PROJECTION)
    viz_out_gdf.to_file(workspace / f'candidate_sites.shp')
    agg_end = time.time()
    print(f'agg time is {(agg_end - agg_start)/60} minutes')
    #Get all possible mainstem segments
    print('Getting list of mainstem segments')
    #Import list of evaluated sites
    list_of_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].to_list()
    #The entire routine to get mainstems is harcoded in this function.
    ms_segs = mainstem_nwm_segs(metadata_url, list_of_sites)
    
    #Loop through each huc unit
    all_messages = []
    for huc in huc_dictionary:
        print(f'Iterating through {huc}')
        #Get list of nws_lids
        nws_lids = huc_dictionary[huc]
        #Loop through each lid in list to create flow file
        for lid in nws_lids:
        #     #In some instances the lid is not assigned a name, skip over these.
        #     if not isinstance(lid,str):
        #         print(f'{lid} is {type(lid)}')
        #         continue
            #Convert lid to lower case
            lid = lid.lower()
            #Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
            stages, flows = get_thresholds(threshold_url = threshold_url, location_ids = lid, physical_element = 'all', threshold = 'all', bypass_source_flag = False)
            #If stages/flows don't exist write message and exit out.
            if not (stages and flows):
                message = f'{lid}: missing all thresholds'
                all_messages.append(message)
                continue

            #find lid metadata from master list of metadata dictionaries (line 66).
            metadata = next((item for item in all_lists if item['identifiers']['nws_lid'] == lid.upper()), False)
        
            #Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            segments = get_nwm_segs(metadata)        
            site_ms_segs = set(segments).intersection(ms_segs)
            segments = list(site_ms_segs)       
            #if no segments, write message and exit out
            if not segments:
                print(f'{lid} no segments')
                message = f'{lid} no segments'
                all_messages.append(message)
                continue
            #For each flood category
            for category in ['action', 'minor', 'moderate', 'major', 'record']:
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
                    message = f'{lid}:{category}: missing calculated flow'
                    all_messages.append(message)
            #This section will produce a point file of the LID location
            #Get various attributes of the site.
            lat = float(metadata['usgs_data']['latitude'])
            lon = float(metadata['usgs_data']['longitude'])
            wfo = metadata['nws_data']['wfo']
            rfc = metadata['nws_data']['rfc']
            state = metadata['nws_data']['state']
            county = metadata['nws_data']['county']
            name = metadata['nws_data']['name']
            q_act = flows['action']
            q_min = flows['minor']
            q_mod = flows['moderate']
            q_maj = flows['major']
            q_rec = flows['record']
            flow_units = flows['units']
            flow_source = flows['source']
            s_act = stages['action']
            s_min = stages['minor']
            s_mod = stages['moderate']
            s_maj = stages['major']
            s_rec = stages['record']
            stage_units = stages['units']
            stage_source = stages['source']
            wrds_timestamp = stages['wrds_timestamp']
            #Create a DataFrame using the collected attributes
            df = pd.DataFrame({'nws_lid': [lid], 'name':name, 'WFO': wfo, 'rfc':rfc, 'huc':[huc], 'state':state, 'county':county, 'q_act':q_act, 'q_min':q_min, 'q_mod':q_mod, 'q_maj':q_maj, 'q_rec':q_rec, 'q_uni':flow_units, 'q_src':flow_source, 'stage_act':s_act, 'stage_min':s_min, 'stage_mod':s_mod, 'stage_maj':s_maj, 'stage_rec':s_rec, 'stage_uni':stage_units, 's_src':stage_source, 'wrds_time':wrds_timestamp, 'lat':[lat], 'lon':[lon]})
            #Round stages and flows to nearest hundredth
            df = df.round({'q_act':2,'q_min':2,'q_mod':2,'q_maj':2,'q_rec':2,'stage_act':2,'stage_min':2,'stage_mod':2,'stage_maj':2,'stage_rec':2})
            
            #Create a geodataframe using usgs lat/lon property from WRDS then reproject to WGS84.
            #Define EPSG codes for possible usgs latlon datum names (NAD83WGS84 assigned NAD83)
            crs_lookup ={'NAD27':'EPSG:4267', 'NAD83':'EPSG:4269', 'NAD83WGS84': 'EPSG:4269'}
            #Get horizontal datum (from dataframe) and assign appropriate EPSG code, assume NAD83 if not assigned.
            h_datum = metadata['usgs_data']['latlon_datum_name']
            src_crs = crs_lookup.get(h_datum, 'EPSG:4269')            
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['lon'], df['lat']), crs =  src_crs) 
            #Reproject to VIZ_PROJECTION
            viz_gdf = gdf.to_crs(VIZ_PROJECTION)
            
            #Create a csv with same info as shapefile
            csv_df = pd.DataFrame()
            for threshold in ['action', 'minor', 'moderate', 'major', 'record']:
                line_df = pd.DataFrame({'nws_lid': [lid], 'name':name, 'WFO': wfo, 'rfc':rfc, 'huc':[huc], 'state':state, 'county':county, 'magnitude': threshold, 'q':flows[threshold], 'q_uni':flows['units'], 'q_src':flow_source, 'stage':stages[threshold], 'stage_uni':stages['units'], 's_src':stage_source, 'wrds_time':wrds_timestamp, 'lat':[lat], 'lon':[lon]})
                csv_df = csv_df.append(line_df)
            #Round flow and stage columns to 2 decimal places.
            csv = csv_df.round({'q':2,'stage':2})

            #If a site folder exists (ie a flow file was written) save files containing site attributes.
            try:
                #Save GeoDataFrame to shapefile format and export csv containing attributes
                output_dir = workspace / huc / lid
                viz_gdf.to_file(output_dir / f'{lid}_location.shp' )
                csv_df.to_csv(output_dir / f'{lid}_attributes.csv', index = False)
            except:
                print(f'{lid} missing all flows')
                message = f'{lid}: missing all calculated flows'
                all_messages.append(message)
    #Write out messages to file
    messages_df  = pd.DataFrame(all_messages, columns = ['message'])
    messages_df.to_csv(workspace / f'all_messages.csv', index = False)

    #Recursively find all location shapefiles
    locations_files = list(workspace.rglob('*_location.shp'))    
    spatial_layers = gpd.GeoDataFrame()
    #Append all shapefile info to a geodataframe
    for location in locations_files:
        location_gdf = gpd.read_file(location)
        spatial_layers = spatial_layers.append(location_gdf)
    #Write appended spatial data to disk.
    output_file = workspace /'all_mapped_ahps.shp'
    spatial_layers.to_file(output_file)
    
    #Recursively find all *_info csv files and append
    csv_files = list(workspace.rglob('*_attributes.csv'))
    all_csv_df = pd.DataFrame()
    for csv in csv_files:
        temp_df = pd.read_csv(csv, dtype={'huc':str})
        all_csv_df = all_csv_df.append(temp_df, ignore_index = True)
    #Write appended _info.csvs to file
    all_info_csv = workspace / 'nws_lid_attributes.csv'
    all_csv_df.to_csv(all_info_csv, index = False)
    all_end = time.time()
    print(f'total time is {(all_end - all_start)/60} minutes')
    

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Create forecast files for all nws_lid sites')
    parser.add_argument('-w', '--workspace', help = 'Workspace where all data will be stored.', required = True)
    parser.add_argument('-u', '--nwm_us_search',  help = 'Walk upstream on NWM network this many miles', required = True)
    parser.add_argument('-d', '--nwm_ds_search', help = 'Walk downstream on NWM network this many miles', required = True)
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    

    #Run create_flow_forecast_file
    static_flow_lids(**args)
