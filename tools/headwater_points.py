# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 13:55:14 2021

@author: Trevor.Grout
"""
import pandas as pd
from collections import deque 
import geopandas as gpd
from collections import defaultdict

metadata_url = f'{API_BASE_URL}/metadata/'

#Get all nws_lid points
#Trace downstream from all rfc_forecast_point.
select_by = 'nws_lid'
selector = ['all']
must_include = 'nws_data.rfc_forecast_point'
downstream_trace_distance = 'all'
fcst_list, fcst_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

#Get list of all evaluated sites not in fcst_list
fcst_list_sites = [record.get('identifiers').get('nws_lid').lower() for record in fcst_list]
evaluated_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].str.lower().to_list()
evaluated_sites= set(list_of_sites) - set(lid)

#Trace downstream from all evaluated sites not in fcst_list
select_by = 'nws_lid'
selector = evaluated_sites
must_include = None
downstream_trace_distance = 'all'
eval_list, eval_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

#Trace downstream from all sites in HI/PR.
select_by = 'state'
selector = ['HI','PR']
must_include = None
downstream_trace_distance = 'all'
islands_list, islands_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

#Append all lists
all_lists = fcst_list + eval_list + islands_list

Get 
downstream = defaultdict(list)
target = defaultdict(list)
#For each lid metadata dictionary in list
for lid in all_lists:
    site = lid.get('identifiers').get('nws_lid')
    site_segments = []
    #Get the nwm feature id associated with the location
    location_nwm_seg = lid.get('identifiers').get('nwm_feature_id')
    #get all downstream segments
    downstream_nwm_segs = lid.get('downstream_nwm_features')
    #If valid location_nwm_segs and downstream segments construct two dictionaries    
    if location_nwm_seg and downstream_nwm_segs:
        #Dictionary with target segment and site
        target[int(location_nwm_seg)].append(site)         
       #Dictionary of key (2nd to last element) and value (target segment)
       #2nd to last element used because last element is always 0 (ocean) and the 2nd to last allows for us to get the correct 'tree' (Mississippi, Colorado, etc)
        value = location_nwm_seg
        if len(downstream_nwm_segs) == 1:
            key = location_nwm_seg
        elif len(downstream_nwm_segs)>1:
            key = downstream_nwm_segs[-2]            
        #Dictionary with key of 2nd to last downstream segment and value of site nwm segment 
        downstream[int(key)].append(int(value))
  
#Create dictionary of NWM network (key) from_node, (value) to_node
nwm = '/path/to/nwm/file'
nwm_gdf = gpd.read_file(nwm)
network = nwm_gdf.groupby('ID')['to'].apply(list).to_dict()

#Walk through network and find headwater nws_lid
all_dicts = {}
for tree, targets in downstream.items():    
    #All targets are assigned headwaters
    sub_dict = {i:'headwater' for i in targets}
    #Walk downstream of each target
    for i in targets:
        #Check to see element is not a headwater
        if sub_dict[i] == 'not_headwater':
            continue
        #Get from_node and to_node.
        from_node = i
        [to_node] = network[from_node]
        #Walk downstream from target
        while to_node>0:
            [to_node] = network[to_node]
            #Check if to_node is in targets list
            if to_node in targets:
                sub_dict[to_node] = 'not_headwater'    
    #Append status to master dictionary
    all_dicts.update(sub_dict)

#Dictionaries of nws_lid (key) and headwater status (value) and nws_lid (key) and co-located with same feature_id(value)
final_dict = {}
duplicate_dict = {}
for key,status in all_dicts.items():
    site_list = target[key]       
    for site in site_list:
        final_dict[site] = status
        if len(site_list) > 1:
            duplicate_dict[site] = 'duplicate'
        else:
            duplicate_dict[site] = 'not duplicate'


#Geodataframe from all_lists
trash, gdf = aggregate_wbd_hucs(all_lists, WBD_LAYER, retain_attributes = False)
new_columns = [name.replace('identifiers_','') for name in gdf.columns]
gdf.columns = new_columns
#Join headwater status and co-location attribute
final_dict_pd = pd.DataFrame(list(final_dict.items()), columns = ['nws_lid','headwater'])
duplicate_dict_pd = pd.DataFrame(list(duplicate_dict.items()),columns = ['nws_lid','dups'])
attributes = final_dict_pd.merge(duplicate_dict_pd, on = 'nws_lid')
#Join headwater and co-location attributes
joined = gdf.merge(attributes, on='nws_lid', how = 'left')
#Write out to file
joined.to_file(r':\Temp\sierra_test\headwaters.shp')