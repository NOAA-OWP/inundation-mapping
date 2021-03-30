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
    Return a GeoDataFrame of all sites.

    Returns
    -------
    None.

    '''
    #Get metadata for all usgs_site_codes that are active in the U.S.
    metadata_url = f'{API_BASE_URL}/metadata' 
    #Define arguments to retrieve metadata and then get metadata from WRDS
    select_by = 'usgs_site_code'
    selector = ['all']
    must_include = 'usgs_data.active'
    metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = None )

    #Filter out sites based quality of site. These acceptable codes were initially
    #decided upon and may need fine tuning. A link where more information
    #regarding the USGS attributes is provided. 
    
    #https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html
    acceptable_coord_acc_code = ['H','1','5','S','R','B','C','D','E']
    #https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html
    acceptable_coord_method_code = ['C','D','W','X','Y','Z','N','M','L','G','R','F','S']
    #https://help.waterdata.usgs.gov/codes-and-parameters/codes#SI
    acceptable_alt_acc_thresh = 1
    #https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html
    acceptable_alt_meth_code = ['A','D','F','I','J','L','N','R','W','X','Y','Z']
    #https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html
    acceptable_site_type = ['ST']
    
    #Cycle through each site and filter out if site doesn't meet criteria.
    acceptable_sites_metadata = []
    for metadata in metadata_list:
        #Get the usgs info from each site
        usgs_data = metadata['usgs_data']
                
        #Get site quality attributes      
        coord_accuracy_code = usgs_data.get('coord_accuracy_code')        
        coord_method_code =   usgs_data.get('coord_method_code')
        alt_accuracy_code =   usgs_data.get('alt_accuracy_code')
        alt_method_code =     usgs_data.get('alt_method_code')
        site_type =           usgs_data.get('site_type')
        
        #Check to make sure that none of the codes were null, if null values are found, skip to next.
        if not all([coord_accuracy_code, coord_method_code, alt_accuracy_code, alt_method_code, site_type]):
            continue
        
        #Test if site meets criteria.
        if (coord_accuracy_code in acceptable_coord_acc_code and 
            coord_method_code in acceptable_coord_method_code and
            alt_accuracy_code <= acceptable_alt_acc_thresh and 
            alt_method_code in acceptable_alt_meth_code and
            site_type in acceptable_site_type):
            
            #If nws_lid is not populated then add a dummy ID so that 'aggregate_wbd_hucs' works correctly.
            if not metadata.get('identifiers').get('nws_lid'):
                metadata['identifiers']['nws_lid'] = 'Bogus_ID' 
            
            #Append metadata of acceptable site to acceptable_sites list.
            acceptable_sites_metadata.append(metadata)  
        
    #Get a geospatial layer (gdf) for all acceptable sites
    dictionary, gdf = aggregate_wbd_hucs(acceptable_sites_metadata, Path(WBD_LAYER), retain_attributes = False)
    #Get a list of all sites in gdf
    list_of_sites = gdf['identifiers_usgs_site_code'].to_list()
    #Rename gdf fields
    gdf.columns = gdf.columns.str.replace('identifiers_','')

    return gdf, list_of_sites, acceptable_sites_metadata
            
            
def usgs_rating_to_elev(list_of_gage_sites, workspace=False):
    '''
    Returns rating curves, for a set of sites, adjusted to elevation NAVD. 
    Workflow as follows:
        1a. If 'all' option passed, get metadata for all acceptable USGS sites in CONUS.
        1b. If a list of sites passed, get metadata for all sites supplied by user.
        2.  Extract datum information for each site.
        3.  If site is not in contiguous US skip (due to issue with datum conversions)
        4.  Convert datum if NGVD
        5.  Get rating curve for each site individually
        6.  Convert rating curve to absolute elevation (NAVD) and store in DataFrame
        7.  Append all rating curves to a master DataFrame.

    Parameters
    ----------
    list_of_gage_sites : LIST
        List of all gage site IDs. If all acceptable sites in CONUS are desired
        list_of_gage_sites can be passed 'all' and it will use the get_all_active_usgs_sites
        function to filter out sites that meet certain requirements across CONUS.
        
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

    #If 'all' option passed to list of gages sites, it retrieves all acceptable sites within CONUS.
    if list_of_gage_sites == ['all']:
        acceptable_sites_gdf, acceptable_sites_list, metadata_list = get_all_active_usgs_sites()
    #Otherwise, if a list of sites is passed, retrieve sites from WRDS.
    else:        
        #Define arguments to retrieve metadata and then get metadata from WRDS
        select_by = 'usgs_site_code'
        selector = list_of_gage_sites        
        #Since there is a limit to number characters in url, split up selector if too many sites.
        max_sites = 150
        if len(selector)>max_sites:
            chunks = [selector[i:i+max_sites] for i in range(0,len(selector),max_sites)]
            #Get metadata for each chunk
            metadata_list = []
            metadata_df = pd.DataFrame()
            for chunk in chunks:
                chunk_list, chunk_df = get_metadata(metadata_url, select_by, chunk, must_include = None, upstream_trace_distance = None, downstream_trace_distance = None )
                #Append chunk data to metadata_list/df
                metadata_list.extend(chunk_list)
                metadata_df = metadata_df.append(chunk_df)
        else:
            #If selector has less than max sites, then get metadata.
            metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector, must_include = None, upstream_trace_distance = None, downstream_trace_distance = None )
    
    #Create DataFrame to store all appended rating curves
    all_rating_curves = pd.DataFrame()
    missing_rating_curve = []    
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
            missing_rating_curve.append(location_ids)
            print(f'{location_ids} has no rating curve')
            continue
    missed_curves = pd.DataFrame(missing_rating_curve)
    #If workspace is specified, write data to file.
    if workspace:
        #Write rating curve dataframe to file
        output_csv = Path(workspace)/'usgs_rating_curves.csv'
        output_csv.parent.mkdir(parents = True, exist_ok = True)
        all_rating_curves.to_csv(output_csv, index = False)
        missed_curves.to_csv(output_csv.parent/'missed_curves.csv')
        #If 'all' option specified, write out shapefile of acceptable sites.
        if list_of_gage_sites == ['all']:
            output_shapefile = Path(workspace) / 'sites.shp'
            acceptable_sites_gdf.to_file(output_shapefile)
    
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