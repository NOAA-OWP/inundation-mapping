#!/usr/bin/env python3
import time
import pandas as pd
import geopandas as gpd
from pathlib import Path
from tools_shared_functions import get_metadata, get_datum, ngvd_to_navd_ft, get_rating_curve, aggregate_wbd_hucs, get_thresholds, flow_data
from dotenv import load_dotenv
import os
import argparse
import sys
sys.path.append('/foss_fim/src')
from utils.shared_variables import PREP_PROJECTION

'''
This script calls the NOAA Tidal API for datum conversions. Experience shows that
running script outside of business hours seems to be most consistent way
to avoid API errors. Currently configured to get rating curve data within
CONUS. Tidal API call may need to be modified to get datum conversions for 
AK, HI, PR/VI.
'''

#import variables from .env file
load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL")
WBD_LAYER = os.getenv("WBD_LAYER")
EVALUATED_SITES_CSV = os.getenv("EVALUATED_SITES_CSV")
NWM_FLOWS_MS = os.getenv("NWM_FLOWS_MS")

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
            
##############################################################################
#Generate categorical flows for each category across all sites.
##############################################################################
def write_categorical_flow_files(metadata, workspace):
    '''
    Writes flow files of each category for every feature_id in the input metadata.
    Written to supply input flow files of all gage sites for each flood category.

    Parameters
    ----------
    metadata : DICT
        Dictionary of metadata from WRDS (e.g. output from get_all_active_usgs_sites).
    workspace : STR
        Path to workspace where flow files will be saved.

    Returns
    -------
    None.

    '''
    
    threshold_url = f'{API_BASE_URL}/nws_threshold'
    workspace = Path(workspace)
    workspace.mkdir(parents = True, exist_ok = True)
    #For each site in metadata 
    all_data = pd.DataFrame()
    
    for site in metadata:
        #Get the feature_id and usgs_site_code
        feature_id = site.get('identifiers').get('nwm_feature_id')
        usgs_code = site.get('identifiers').get('usgs_site_code')
        nws_lid = site.get('identifiers').get('nws_lid')
        
        #thresholds only provided for valid nws_lid.
        if nws_lid == 'Bogus_ID':
            continue
        
        #if invalid feature_id skip to next site
        if feature_id is None:
            continue
        
        #Get the stages and flows
        stages, flows = get_thresholds(threshold_url, select_by = 'nws_lid', selector = nws_lid, threshold = 'all')
        
        #For each flood category
        for category in ['action','minor','moderate','major']:
            #Get flow
            flow = flows.get(category, None)
            #If flow or feature id are not valid, skip to next site
            if flow is None:
                continue
            #Otherwise, write 'guts' of a flow file and append to a master DataFrame.
            else:
                data = flow_data([feature_id], flow, convert_to_cms = True)
                data['recurr_interval'] = category            
                data['nws_lid'] = nws_lid
                data['location_id'] = usgs_code
                data = data.rename(columns = {'discharge':'discharge_cms'})
                #Append site data to master DataFrame
                all_data = all_data.append(data, ignore_index = True)
    
    #Write CatFIM flows to file
    final_data = all_data[['feature_id','discharge_cms', 'recurr_interval']]
    final_data.to_csv(workspace / f'catfim_flows_cms.csv', index = False)
    return all_data
###############################################################################
           
def usgs_rating_to_elev(list_of_gage_sites, workspace=False, sleep_time = 1.0):
    '''

    Returns rating curves, for a set of sites, adjusted to elevation NAVD.
    Currently configured to get rating curve data within CONUS. Tidal API 
    call may need to be modified to get datum conversions for AK, HI, PR/VI.
    Workflow as follows:
        1a. If 'all' option passed, get metadata for all acceptable USGS sites in CONUS.
        1b. If a list of sites passed, get metadata for all sites supplied by user.
        2.  Extract datum information for each site.
        3.  If site is not in contiguous US skip (due to issue with datum conversions)
        4.  Convert datum if NGVD
        5.  Get rating curve for each site individually
        6.  Convert rating curve to absolute elevation (NAVD) and store in DataFrame
        7.  Append all rating curves to a master DataFrame.

    
    Outputs, if a workspace is specified, are:
        usgs_rating_curves.csv -- A csv containing USGS rating curve as well
        as datum adjustment and rating curve expressed as an elevation (NAVD88).
        ONLY SITES IN CONUS ARE CURRENTLY LISTED IN THIS CSV. To get 
        additional sites, the Tidal API will need to be reconfigured and tested.
        
        log.csv -- A csv containing runtime messages.
        
        (if all option passed) usgs_gages.gpkg -- a point layer containing ALL USGS gage sites that meet
        certain criteria. In the attribute table is a 'curve' column that will indicate if a rating
        curve is provided in "usgs_rating_curves.csv"
       
    Parameters
    ----------
    list_of_gage_sites : LIST
        List of all gage site IDs. If all acceptable sites in CONUS are desired
        list_of_gage_sites can be passed 'all' and it will use the get_all_active_usgs_sites
        function to filter out sites that meet certain requirements across CONUS.
        
    workspace : STR
        Directory, if specified, where output csv is saved. OPTIONAL, Default is False.
    
    sleep_time: FLOAT
        Amount of time to rest between API calls. The Tidal API appears to 
        error out more during business hours. Increasing sleep_time may help.
        

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
    print('getting metadata for all sites')
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
    print('processing metadata')
    all_rating_curves = pd.DataFrame()
    regular_messages = []    
    api_failure_messages=[]
    #For each site in metadata_list
    for metadata in metadata_list:
        
        #Get datum information for site (only need usgs_data)
        nws, usgs = get_datum(metadata)        
        
        #Filter out sites that are not in contiguous US. If this section is removed be sure to test with datum adjustment section (region will need changed)
        if usgs['state'] in ['Alaska', 'Puerto Rico', 'Virgin Islands', 'Hawaii']:
            continue        
        
        #Get rating curve for site
        location_ids = usgs['usgs_site_code']
        curve = get_rating_curve(rating_curve_url, location_ids = [location_ids])
        #If no rating curve was returned, skip site.
        if curve.empty:
            message = f'{location_ids}: has no rating curve'
            regular_messages.append(message)
            continue

        #Adjust datum to NAVD88 if needed. If datum unknown, skip site.
        if usgs['vcs'] == 'NGVD29':
            #To prevent time-out errors
            time.sleep(sleep_time)
            #Get the datum adjustment to convert NGVD to NAVD. Region needs changed if not in CONUS.
            datum_adj_ft = ngvd_to_navd_ft(datum_info = usgs, region = 'contiguous')

            #If datum API failed, print message and skip site.
            if datum_adj_ft is None:
                api_message = f"{location_ids}: datum adjustment failed!!"
                api_failure_messages.append(api_message)
                print(api_message)
                continue

            #If datum adjustment succeeded, calculate datum in NAVD88            
            navd88_datum = round(usgs['datum'] + datum_adj_ft, 2)
            message = f'{location_ids}:succesfully converted NGVD29 to NAVD88'
            regular_messages.append(message)

        elif usgs['vcs'] == 'NAVD88':
            navd88_datum = usgs['datum']
            message = f'{location_ids}: already NAVD88'
            regular_messages.append(message)

        else:
            message = f"{location_ids}: datum unknown"
            regular_messages.append(message)
            continue

        #Populate rating curve with metadata and use navd88 datum to convert stage to elevation.
        curve['active'] = usgs['active']
        curve['datum'] = usgs['datum']
        curve['datum_vcs'] = usgs['vcs']
        curve['navd88_datum'] = navd88_datum
        curve['elevation_navd88'] = curve['stage'] + navd88_datum
        #Append all rating curves to a dataframe
        all_rating_curves = all_rating_curves.append(curve)        

    #Rename columns and add attribute indicating if rating curve exists
    acceptable_sites_gdf.rename(columns = {'nwm_feature_id':'feature_id','usgs_site_code':'location_id'}, inplace = True)
    sites_with_data = pd.DataFrame({'location_id':all_rating_curves['location_id'].unique(),'curve':'yes'})
    acceptable_sites_gdf = acceptable_sites_gdf.merge(sites_with_data, on = 'location_id', how = 'left')
    acceptable_sites_gdf.fillna({'curve':'no'},inplace = True)    
    #Add mainstems attribute to acceptable sites
    print('Attributing mainstems sites')
    #Import mainstems segments used in run_by_unit.sh
    ms_df = gpd.read_file(NWM_FLOWS_MS)
    ms_segs = ms_df.ID.astype(str).to_list()
    #Populate mainstems attribute field
    acceptable_sites_gdf['mainstem'] = 'no'
    acceptable_sites_gdf.loc[acceptable_sites_gdf.eval('feature_id in @ms_segs'),'mainstem'] = 'yes' 
    
    
    #If workspace is specified, write data to file.
    if workspace:
        #Write rating curve dataframe to file
        Path(workspace).mkdir(parents = True, exist_ok = True)
        all_rating_curves.to_csv(Path(workspace) / 'usgs_rating_curves.csv', index = False)
        #Save out messages to file.
        first_line = [f'THERE WERE {len(api_failure_messages)} SITES THAT EXPERIENCED DATUM CONVERSION ISSUES']
        api_failure_messages = first_line + api_failure_messages
        regular_messages = api_failure_messages + regular_messages                
        all_messages = pd.DataFrame({'Messages':regular_messages})
        all_messages.to_csv(Path(workspace) / 'log.csv', index = False)
        #If 'all' option specified, reproject then write out shapefile of acceptable sites.
        if list_of_gage_sites == ['all']:            
            acceptable_sites_gdf = acceptable_sites_gdf.to_crs(PREP_PROJECTION)
            acceptable_sites_gdf.to_file(Path(workspace) / 'usgs_gages.gpkg', layer = 'usgs_gages', driver = 'GPKG')
        
        #Write out flow files for each threshold across all sites
        all_data = write_categorical_flow_files(metadata_list, workspace)
    
    return all_rating_curves

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Retrieve USGS rating curves adjusted to elevation (NAVD88).\nCurrently configured to get rating curves within CONUS.\nRecommend running outside of business hours to reduce API related errors.\nIf error occurs try increasing sleep time (from default of 1).')
    parser.add_argument('-l', '--list_of_gage_sites',  help = '"all" for all active usgs sites, specify individual sites separated by space, or provide a csv of sites (one per line).', nargs = '+', required = True)
    parser.add_argument('-w', '--workspace', help = 'Directory where all outputs will be stored.', default = False, required = False)
    parser.add_argument('-t', '--sleep_timer', help = 'How long to rest between datum API calls', default = 1.0, required = False)
       
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    #Check if csv is supplied       
    if args['list_of_gage_sites'][0].endswith('.csv'):        
        #Convert csv list to python list
        with open(args['list_of_gage_sites']) as f:
            sites = f.read().splitlines()
        args['list_of_gage_sites'] = sites

    l = args['list_of_gage_sites']
    w = args['workspace'] 
    t = float(args['sleep_timer'])           
    
    #Generate USGS rating curves
    usgs_rating_to_elev(list_of_gage_sites = l, workspace=w, sleep_time = t)
    