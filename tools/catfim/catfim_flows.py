#!/usr/bin/env python3
from pathlib import Path
import geopandas as gpd
import pandas as pd
from utils.shared_functions import aggregate_wbd_hucs, mainstem_nwm_segs, get_threshold, flow_data, get_metadata, get_nwm_segs, flow_data
import argparse

def static_flow_lids(BASE_URL, workspace, nwm_us_search, nwm_ds_search, wbd_path,domain):
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
    metadata_url = f'{BASE_URL}/metadata'
    threshold_url = f'{BASE_URL}/threshold'
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
    
    #Get all possible mainstem segments, if hi/pr domain = 'hipr', if conus domain = 'conus'
    print('Getting list of mainstem segments')
    ms_segs = mainstems_network(metadata_url, list_of_sites)
    
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
            flow_source = flows.get('source')
            s_act = stages['action']
            s_min = stages['minor']
            s_mod = stages['moderate']
            s_maj = stages['major']
            s_rec = stages['record']
            stage_units = stages['units']
            stage_source = stages.get('source')
            wrds_timestamp = stages.get('wrds_timestamp')
            #Create a GeoDataFrame using the lat/lon information
            df = pd.DataFrame({'nws_lid': [lid], 'name':name, 'rfc':rfc, 'HUC8':[huc], 'state':state, 'county':county, 'Q_act':q_act, 'Q_min':q_min, 'Q_mod':q_mod, 'Q_maj':q_maj, 'Q_rec':q_rec, 'Q_unit':flow_units, 'Q_source':flow_source, 'S_act':s_act, 'S_min':s_min, 'S_mod':s_mod, 'S_maj':s_maj, 'S_rec':s_rec, 'S_unit':stage_units, 'S_source':stage_source, 'WRDS_time':wrds_timestamp, 'lat':[lat], 'lon':[lon]})
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['lon'], df['lat']), crs =  "EPSG:4326") 
            
            try:
                #Save GeoDataFrame to shapefile format
                output_file = workspace / huc / lid / (f'{lid}_location.shp')
                gdf.to_file(output_file)
            except:
                print(f'{lid} missing all flows')
                message = f'{lid} missing all flows'
                all_messages.append(message)
    #Write out messages
    messages_df  = pd.DataFrame(all_messages, columns = ['message'])
    messages_df.to_csv(workspace / f'all_messages.csv', index = False)
    #Append all location shapefiles
    locations_files = list(workspace.rglob('*_location.shp'))
    spatial_layers = gpd.GeoDataFrame()
    for location in locations_files:
        gdf = gpd.read_file(location)
        spatial_layers = spatial_layers.append(gdf)
    output_file = workspace /'all_mapped_ahps.shp'
    spatial_layers.to_file(output_file)
    #Write out spatial layers as a text file    
    csv_df = spatial_layers.drop(columns = 'geometry')
    output_csv = workspace / 'all_mapped_ahps.csv'
    csv_df.to_csv(output_csv, index = False)

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Create forecast files for all nws_lid sites')
    parser.add_argument('-u', '--BASE_URL', help = 'URL to get metadata about site.', required = True)    
    parser.add_argument('-w', '--workspace', help = 'Workspace where all data will be stored.', required = True)
    parser.add_argument('-u', '--nwm_us_search',  help = 'Walk upstream on NWM network this many miles', required = True)
    parser.add_argument('-d', '--nwm_ds_search', help = 'Walk downstream on NWM network this many miles', required = True)
    parser.add_argument('-hu', '--wbd_path', help = 'HUC layer (as geopackage)', required = True)
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    

    #Run create_flow_forecast_file
    static_flow_lids(**args)