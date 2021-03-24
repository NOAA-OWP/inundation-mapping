#!/usr/bin/env python3

import pandas as pd
import geopandas as gpd
from pathlib import Path
from tools_shared_functions import get_metadata, get_datum, ngvd_to_navd_ft, get_rating_curve, aggregate_wbd_hucs
from dotenv import load_dotenv
import os
import argparse

load_dotenv()
#import variables from .env file
API_BASE_URL = os.getenv("API_BASE_URL")
WBD_LAYER = os.getenv("WBD_LAYER")

def get_all_active_usgs_sites():
    '''
    Compile a list of all active usgs gage sites that meet certain criteria. 
    Return a dictionary of huc (key) and list of sites (values) as well as a 
    GeoDataFrame of all sites.

    Returns
    -------
    None.

    '''
    metadata_url = f'{API_BASE_URL}/metadata' 
    #Define arguments to retrieve metadata and then get metadata from WRDS
    select_by = 'usgs_site_code'
    selector = 'all'
    must_include = 'usgs_data.active'
    metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector, must_include = None, upstream_trace_distance = None, downstream_trace_distance = None )

    #Filter out sites based on vertical datum quality and horizontal datum quality
    acceptable_sites = []
    for metadata in metadata_list:
        usgs_data = metadata['usgs_data']
        
        #!!!!Modify the acceptable codes and vertical tolerances
        acceptable_coord_acc_code = ['S']
        acceptable_coord_method_code = ['M']
        acceptable_alt_acc_thresh = 1
        acceptable_alt_meth_code = ['L']
        
        #Test if site meets criteria.
        if (usgs_data['coord_accuracy_code'] in acceptable_coord_acc_code and 
            usgs_data['coord_method_code'] in acceptable_coord_method_code and
            usgs_data['alt_accuracy_code'] < acceptable_alt_acc_thresh and 
            usgs_data['alt_method_code'] in acceptable_alt_meth_code):
            
            #If an acceptable site, append to acceptable_sites list.
            acceptable_sites.append(metadata)
            
    #!!!!Get a geospatial layer for all acceptable sites
    dictionary, gdf = aggregate_wbd_hucs(acceptable_sites, Path(WBD_LAYER), retain_attributes = False)
    
    return dictionary, gdf     
            
            

def usgs_rating_to_elev(list_of_gage_sites, workspace=False):
    '''
    Provided a list of usgs gage sites returns rating curves adjusted to 
    elevation NAVD. Workflow as follows:
        1. Get metadata for all sites supplied by user.
        2. Extract datum information for each site.
        3. If site is not in contiguous US skip (due to issue with datum conversions)
        4. Convert datum if NGVD
        5. Get rating curve for each site individually
        6. Convert rating curve to absolute elevation (NAVD) and store in DataFrame
        7. Append all rating curves to a master DataFrame.

    Parameters
    ----------
    list_of_gage_sites : LIST
        List of all gage site IDs.
    workspace : STR
        Directory, if specified, where output csv is saved. OPTIONAL, Default is False.

    Returns
    -------
    all_rating_curves : Pandas DataFrame
        DataFrame containing USGS rating curves adjusted to elevation for 
        all input sites. Additional metadata also contained in DataFrame

    '''
    #Define URLs for metadata and rating curve
    metadata_url = f'{API_BASE_URL}/metadata'
    rating_curve_url = f'{API_BASE_URL}/rating_curve'
    
    #Define arguments to retrieve metadata and then get metadata from WRDS
    select_by = 'usgs_site_code'
    selector = list_of_gage_sites
    metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector, must_include = None, upstream_trace_distance = None, downstream_trace_distance = None )
    
    #Create DataFrame to store all appended rating curves
    all_rating_curves = pd.DataFrame()
    
    #For each site in metadata_list
    for metadata in metadata_list:
        #Get datum information for site (only need usgs_data)
        nws, usgs = get_datum(metadata)
        #Filter out sites that are not in contiguous US, issue with NGVD to NAVD conversion
        if usgs['state'] in ['Alaska', 'Puerto Rico', 'Virgin Islands', 'Hawaii']:
            continue
        #Adjust datum to NAVD88 if needed
        if usgs['vcs'] == 'NGVD29':
            #Get the datum adjustment to convert NGVD to NAVD. Sites not in contiguous US are previously removed otherwise the region needs changed.
            datum_adj_ft = ngvd_to_navd_ft(datum_info = usgs, region = 'contiguous')
            navd88_datum = round(usgs['datum'] + datum_adj_ft, 2)
        else:
            navd88_datum = usgs['datum']
        
        #Get rating curve (only passing single site, convert to list)
        location_ids = usgs['usgs_site_code']
        curve = get_rating_curve(rating_curve_url, location_ids = [location_ids])
        #Check to make sure a curve was returned
        if not curve.empty:
            #Adjust rating curve with NAVD88 datum
            curve['active'] = usgs['active']
            curve['datum'] = usgs['datum']
            curve['datum_vcs'] = usgs['vcs']
            curve['navd88_datum'] = navd88_datum
            curve['elevation_navd88'] = curve['stage'] + navd88_datum
        
            #Append all rating curves to a dataframe
            all_rating_curves = all_rating_curves.append(curve)
        else:
            print(f'{location_ids} has no rating curve')
            continue
    #If workspace is specified, write dataframe to file.
    if workspace:
        #Write rating curve dataframe to file
        output_csv = Path(workspace)/'usgs_rating_curves.csv'
        all_rating_curves.to_csv(output_csv, index = False)
    
    return all_rating_curves

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Retrieve USGS rating curves adjusted to elevation (NAVD88)')
    parser.add_argument('-l', '--list_of_gage_sites',  help = 'csv containing list of usgs sites supplied as a file', required = True)
    parser.add_argument('-w', '--workspace', help = 'Workspace where all data will be stored.', action = 'store_true')
       
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    
    #Convert csv list to python list
    with open(args['list_of_gage_sites']) as f:
        sites = f.read().splitlines()
    args['list_of_gage_sites'] = sites

    #Run create_flow_forecast_file
    usgs_rating_to_elev(**args)