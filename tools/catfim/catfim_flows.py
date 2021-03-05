#!/usr/bin/env python3
from pathlib import Path
import geopandas as gpd
import pandas as pd
from catfim_functions import aggregate_wbd_hucs, mainstem_nwm_segs, get_threshold, flow_data, get_metadata, get_nwm_segs, flow_data
import argparse
from dotenv import load_dotenv
import os

load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL")
EVALUATED_SITES_CSV = os.getenv("SITES_CSV")

def static_flow_lids(workspace, nwm_us_search, nwm_ds_search, wbd_path):
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
    domain : STR
        "conus" or "hipr". When selecting nws_lid sites, the flag "rfc_forecast_pt == True is used" however when this is enabled, no sites in hi/pr are returned. To get sites in HI/PR/VI this flag is turne off and all sites in these areas are returned (which is different than how sites are queried in conus).

    Returns
    -------
    None.

    '''    
    #Define workspace and wbd_path as a pathlib Path. Convert search distances to integer.
    workspace = Path(workspace)
    wbd_path = Path(wbd_path)
    nwm_us_search = int(nwm_us_search)
    nwm_ds_search = int(nwm_ds_search)
    metadata_url = f'{API_BASE_URL}/metadata'
    threshold_url = f'{API_BASE_URL}/threshold'
    ###################################################################
    #Create workspace
    workspace.mkdir(exist_ok = True)

    #Return dictionary of huc (key) and sublist of ahps(value) as well as geodataframe of sites.
    print('Retrieving metadata...')
    #Get metadata for 'CONUS'
    conus_list, conus_dataframe = get_metadata(metadata_url, select_by = 'nws_lid', selector = ['all'], must_include = 'rfc_forecast_point', upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search )

    #Get metadata for Islands --Not working
    islands_list, islands_dataframe = get_metadata(metadata_url, select_by = 'state', selector = ['HI','PR'] , must_include = None, upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search)
    
    #Append the dataframes and lists
    all_lists = conus_list + islands_list
    all_dataframe = conus_dataframe.append(islands_dataframe)
    
    print('Determining HUC using WBD layer...')
    #Assign FIM HUC to GeoDataFrame and export to shapefile all candidate sites.
    huc_dictionary, out_gdf = aggregate_wbd_hucs(metadata_dataframe = all_dataframe, wbd_huc8_path = wbd_path)
    out_gdf.to_file(workspace / f'candidate_sites.shp')
    
    #Get all possible mainstem segments
    print('Getting list of mainstem segments')
    #Import list of evaluated sites
    list_of_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].to_list()
    ms_segs = mainstem_nwm_segs(metadata_url, list_of_sites)
    
    #Loop through each aggregate unit
    all_messages = []
    for huc in huc_dictionary:
        print(f'Iterating through {huc}')
        #Get list of nws_lids
        nws_lids = huc_dictionary[huc]
        #Loop through each lid in list to create flow file
        for lid in nws_lids:
            if not isinstance(lid,str):
                print(f'{lid} is {type(lid)}')
                continue
            #Convert lid to lower case
            lid = lid.lower()
            #Get stages and flows for each threshold
            stages, flows = get_thresholds(threshold_url = threshold_url, location_ids = lid, physical_element = 'all', threshold = 'all', bypass_source_flag = False)
            #If stages/flows don't exist write message and exit out.
            if not (stages and flows):
                message = f'{lid} no thresholds'
                all_messages.append(message)
                continue

            #Instead of calling WRDS API, find corresponding record in the list of dictionaries
            metadata = next((item for item in all_lists if item['nws_lid'] == lid.upper()), False)
        
            #Get NWM Segments by intersecting with known mainstem segments
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
                flow = flows[category]
                #If there is a valid flow value, write a flow file.
                if flow != 'None':
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
                    message = f'{lid}_{category}_no flow'
                    all_messages.append(message)
            #This section will produce a point file of the AHPS location
            #Get various attributes of the site including lat, lon, rfc, state, county, name, flows, and stages for each threshold.
            lat = float(metadata['latitude'])
            lon = float(metadata['longitude'])
            rfc = metadata['rfc']
            state = metadata['state']
            county = metadata['county']
            name = metadata['name']
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
            #Create a GeoDataFrame using the lat/lon information
            df = pd.DataFrame({'nws_lid': [lid], 'name':name, 'rfc':rfc, 'huc':[huc], 'state':state, 'county':county, 'q_act':q_act, 'q_min':q_min, 'q_mod':q_mod, 'q_maj':q_maj, 'q_rec':q_rec, 'q_uni':flow_units, 'q_src':flow_source, 'stage_act':s_act, 'stage_min':s_min, 'stage_mod':s_mod, 'stage_maj':s_maj, 'stage_rec':s_rec, 'stage_uni':stage_units, 's_src':stage_source, 'wrds_time':wrds_timestamp, 'lat':[lat], 'lon':[lon]})
            #Round stages and flows to nearest hundredth
            df = df.round({'q_act':2,'q_min':2,'q_mod':2,'q_maj':2,'q_rec':2,'stage_act':2,'stage_min':2,'stage_mod':2,'stage_maj':2,'stage_rec':2})
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['lon'], df['lat']), crs =  "EPSG:4326") 

            #Create a csv with same info as shapefile
            csv_df = pd.DataFrame()
            for threshold in ['action', 'minor', 'moderate', 'major', 'record']:
                line_df = pd.DataFrame({'nws_lid': [lid], 'name':name, 'rfc':rfc, 'huc':[huc], 'state':state, 'county':county, 'magnitude': threshold, 'q':flows[threshold], 'q_uni':flows['units'], 'stage':stages[threshold], 'stage_uni':stages['units'], 'wrds_time':wrds_timestamp, 'lat':[lat], 'lon':[lon]})
                csv_df = csv_df.append(line_df)
            #Round flow and stage columns to 2 decimal places.
            csv = csv_df.round({'q':2,'stage':2})

            
            try:
                #Save GeoDataFrame to shapefile format and export csv
                output_dir = workspace / huc / lid
                gdf.to_file(output_dir / f'{lid}_location.shp' )
                csv_df.to_csv(output_dir / f'{lid}_info.csv', index = False)
            except:
                print(f'{lid} missing all flows')
                message = f'{lid} missing all flows'
                all_messages.append(message)
    #Write out messages
    messages_df  = pd.DataFrame(all_messages, columns = ['message'])
    messages_df.to_csv(workspace / f'all_messages.csv', index = False)

    #Find all location shapefiles
    locations_files = list(workspace.rglob('*_location.shp'))    
    spatial_layers = gpd.GeoDataFrame()
    #Append all shapefile info to a geodataframe
    for location in locations_files:
        gdf = gpd.read_file(location)
        spatial_layers = spatial_layers.append(gdf)
    #Write appended spatial data to disk.
    output_file = workspace /'all_mapped_ahps.shp'
    spatial_layers.to_file(output_file)
    #Find all *_info csv files
    csv_files = list(workspace.rglob('*_info.csv'))
    all_csv_df = pd.DataFrame()
    for csv in csv_files:
        temp_df = pd.read_csv(csv)
        all_csv_df = all_csv_df.append(temp_df)
    #Write csv to file
    output_csv = workspace / '_info.csv'
    csv_df.to_csv(output_csv, index = False)

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Create forecast files for all nws_lid sites')
    parser.add_argument('-w', '--workspace', help = 'Workspace where all data will be stored.', required = True)
    parser.add_argument('-u', '--nwm_us_search',  help = 'Walk upstream on NWM network this many miles', required = True)
    parser.add_argument('-d', '--nwm_ds_search', help = 'Walk downstream on NWM network this many miles', required = True)
    parser.add_argument('-hu', '--wbd_path', help = 'HUC layer (as geopackage)', required = True)
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    

    #Run create_flow_forecast_file
    static_flow_lids(**args)
