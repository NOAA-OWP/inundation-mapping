#!/usr/bin/env python3

import requests
import pandas as pd
import geopandas as gpd


def get_metadata(metadata_url, select_by, selector, must_include = None, upstream_trace_distance = None, downstream_trace_distance = None ):
    '''
    Retrieve metadata for a site or list of sites.

    Parameters
    ----------
    metadata_url : STR
        metadata base URL.
    select_by : STR
        Location search option.
    selector : LIST
        Value to match location data against. Supplied as a LIST.
    must_include : STR, optional
        What attributes are required to be valid response. The default is None.
    upstream_trace_distance : INT, optional
        Distance in miles upstream of site to trace NWM network. The default is None.
    downstream_trace_distance : INT, optional
        Distance in miles downstream of site to trace NWM network. The default is None.

    Returns
    -------
    metadata_list : LIST
        Dictionary or list of dictionaries containing metadata at each site.
    metadata_dataframe : Pandas DataFrame
        Dataframe of metadata for each site.

    '''
    
    #Format selector variable in case multiple selectors supplied
    format_selector = '%2C'.join(selector)
    #Define the url
    url = f'{metadata_url}/{select_by}/{format_selector}/'
    #Assign optional parameters to a dictionary
    params = {}
    params['must_include'] = must_include
    params['upstream_trace_distance'] = upstream_trace_distance
    params['downstream_trace_distance'] = downstream_trace_distance
    #Request data from url
    response = requests.get(url, params = params)
    if response.ok:
        #Convert data response to a json
        metadata_json = response.json()
        #Get the count of returned records
        location_count = metadata_json['_metrics']['location_count']
        #Get metadata
        metadata_list = metadata_json['locations']        
        #Add timestamp of WRDS retrieval
        timestamp = response.headers['Date']        
        #for v3, crosswalk info always last dictionary in list
        *metadata_list, crosswalk_info = metadata_list
        #Update each dictionary with timestamp and crosswalk info
        for metadata in metadata_list:
            metadata.update({"wrds_timestamp": timestamp})        
            metadata.update(crosswalk_info)
        #If count is 1
        if location_count == 1:
            metadata_list = metadata_json['locations'][0]
        metadata_dataframe = pd.json_normalize(metadata_list)
        #Replace all periods with underscores in column names
        metadata_dataframe.columns = metadata_dataframe.columns.str.replace('.','_')
    else:
        #if request was not succesful, print error message.
        print(f'Code: {response.status_code}\nMessage: {response.reason}\nURL: {response.url}')
        #Return empty outputs
        metadata_list = []
        metadata_dataframe = pd.DataFrame()
    return metadata_list, metadata_dataframe

########################################################################
#Function to assign HUC code using the WBD spatial layer using a spatial join
########################################################################
def aggregate_wbd_hucs(metadata_list, wbd_huc8_path, retain_attributes = False):    
    '''
    Assigns the proper FIM HUC 08 code to each site in the input DataFrame.
    Converts input DataFrame to a GeoDataFrame using the lat/lon attributes
    with sites containing null lat/lon removed. Reprojects GeoDataFrame
    to same CRS as the HUC 08 layer. Performs a spatial join to assign the
    HUC 08 layer to the GeoDataFrame. Sites that are not assigned a HUC
    code removed as well as sites in Alaska and Canada. 
    
    Parameters
    ----------
    metadata_list: List of Dictionaries
        Output list from get_metadata
    wbd_huc8_path : pathlib Path
        Path to HUC8 wbd layer (assumed to be geopackage format)
    retain_attributes ; Bool OR List
        Flag to define attributes of output GeoDataBase. If True, retain 
        all attributes. If False, the site metadata will be trimmed to a 
        default list. If a list of desired attributes is supplied these 
        will serve as the retained attributes.
    Returns
    -------
    dictionary : DICT
        Dictionary with HUC (key) and corresponding AHPS codes (values).
    all_gdf: GeoDataFrame
        GeoDataFrame of all NWS_LID sites.

    '''
    #Import huc8 layer as geodataframe and retain necessary columns
    huc8 = gpd.read_file(wbd_huc8_path, layer = 'WBDHU8')
    huc8 = huc8[['HUC8','name','states', 'geometry']]
    #Define EPSG codes for possible usgs latlon datum names (NAD83WGS84 assigned NAD83)
    crs_lookup ={'NAD27':'EPSG:4267', 'NAD83':'EPSG:4269', 'NAD83WGS84': 'EPSG:4269'}    
    #Create empty geodataframe and define CRS for potential horizontal datums
    metadata_gdf = gpd.GeoDataFrame()
    #Iterate through each site
    for metadata in metadata_list:
        #Convert metadata to json
        df = pd.json_normalize(metadata)        
        #Columns have periods due to nested dictionaries
        df.columns = df.columns.str.replace('.', '_')
        #Drop any metadata sites that don't have lat/lon populated◘
        df.dropna(subset = ['identifiers_nws_lid','usgs_data_latitude','usgs_data_longitude'], inplace = True)
        #If dataframe still has data
        if not df.empty:
            #Get horizontal datum (use usgs) and assign appropriate EPSG code
            h_datum = df.usgs_data_latlon_datum_name.item()
            src_crs = crs_lookup[h_datum]
            #Convert dataframe to geodataframe using lat/lon (USGS)
            site_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.usgs_data_longitude, df.usgs_data_latitude), crs =  src_crs)
            #Reproject to huc 8 crs
            site_gdf = site_gdf.to_crs(huc8.crs)
            #Append site geodataframe to metadata geodataframe
            metadata_gdf = metadata_gdf.append(site_gdf, ignore_index = True)
    
    #Trim metadata to only have certain fields.
    if not retain_attributes:       
        metadata_gdf = metadata_gdf[['identifiers_nwm_feature_id', 'identifiers_nws_lid', 'geometry']]  
    #If a list of attributes is supplied then use that list.
    elif isinstance(retain_attributes,list):
        metadata_gdf = metadata_gdf[retain_attributes]          

    #Perform a spatial join to get the WBD HUC 8 assigned to each AHPS
    joined_gdf = gpd.sjoin(metadata_gdf, huc8, how = 'inner', op = 'intersects', lsuffix = 'ahps', rsuffix = 'wbd')
    joined_gdf = joined_gdf.drop(columns = 'index_wbd')
    
    #Remove all Alaska HUCS (Not in NWM v2.0 domain)    
    joined_gdf = joined_gdf[~joined_gdf.states.str.contains('AK')]
    
    #Create a dictionary of huc [key] and nws_lid[value]
    dictionary = joined_gdf.groupby('HUC8')['identifiers_nws_lid'].apply(list).to_dict()
    
    return dictionary, joined_gdf

########################################################################
def mainstem_nwm_segs(metadata_url, list_of_sites):
    '''
    Define the mainstems network. Currently a 4 pass approach that probably needs refined.
    Once a final method is decided the code can be shortened. Passes are:
        1) Search downstream of gages designated as upstream. This is done to hopefully reduce the issue of mapping starting at the nws_lid. 91038 segments
        2) Search downstream of all LID that are rfc_forecast_point = True. Additional 48,402 segments
        3) Search downstream of all evaluated sites (sites with detailed FIM maps) Additional 222 segments
        4) Search downstream of all sites in HI/PR (locations have no rfc_forecast_point = True) Additional 408 segments

    Parameters
    ----------
    metadata_url : STR
        URL of API.
    list_of_sites : LIST
        List of evaluated sites.

    Returns
    -------
    ms_nwm_segs_set : SET
        Mainstems network segments as a set.

    ''' 
        
    #Define the downstream trace distance
    downstream_trace_distance = 'all'
    
    #Trace downstream from all 'headwater' usgs gages
    select_by = 'tag'
    selector = ['usgs_gages_ii_ref_headwater']
    must_include = None
    gages_list, gages_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

    #Trace downstream from all rfc_forecast_point.
    select_by = 'nws_lid'
    selector = ['all']
    must_include = 'nws_data.rfc_forecast_point'
    fcst_list, fcst_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )
    
    #Trace downstream from all evaluated ahps sites.
    select_by = 'nws_lid'
    selector = list_of_sites
    must_include = None
    eval_list, eval_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )
    
    #Trace downstream from all sites in HI/PR.
    select_by = 'state'
    selector = ['HI','PR']
    must_include = None
    islands_list, islands_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )
    
    #Combine all lists of metadata dictionaries into a single list.
    combined_lists = gages_list + fcst_list + eval_list + islands_list
    #Define list that will contain all segments listed in metadata.
    all_nwm_segments = []
    #For each lid metadata dictionary in list
    for lid in combined_lists:        
        #get all downstream segments
        downstream_nwm_segs = lid.get('downstream_nwm_features')        
        #Append downstream segments
        if downstream_nwm_segs:
            all_nwm_segments.extend(downstream_nwm_segs)                    
        #Get the nwm feature id associated with the location
        location_nwm_seg = lid.get('identifiers').get('nwm_feature_id')
        if location_nwm_seg:
            #Append nwm segment (conver to list)
            all_nwm_segments.extend([location_nwm_seg])    
    #Remove duplicates by assigning to a set.
    ms_nwm_segs_set = set(all_nwm_segments) 
    
    return ms_nwm_segs_set

##############################################################################
#Function to create list of NWM segments
###############################################################################
def get_nwm_segs(metadata):
    '''
    Using the metadata output from "get_metadata", output the NWM segments.

    Parameters
    ----------
    metadata : DICT
        Dictionary output from "get_metadata" function.

    Returns
    -------
    all_segments : LIST
        List of all NWM segments.

    '''
    
    nwm_feature_id = metadata.get('identifiers').get('nwm_feature_id')
    upstream_nwm_features = metadata.get('upstream_nwm_features')
    downstream_nwm_features = metadata.get('downstream_nwm_features')
    
    all_segments = []    
    #Convert NWM feature id segment to a list (this is always a string or empty)    
    if nwm_feature_id:
        nwm_feature_id = [nwm_feature_id]
        all_segments.extend(nwm_feature_id)
    #Add all upstream segments (always a list or empty)
    if upstream_nwm_features:
        all_segments.extend(upstream_nwm_features)
    #Add all downstream segments (always a list or empty)
    if downstream_nwm_features:
        all_segments.extend(downstream_nwm_features)

    return all_segments

#######################################################################
#Thresholds
#######################################################################
def get_thresholds(threshold_url, location_ids, physical_element = 'all', threshold = 'all', bypass_source_flag = False):
    '''
    Get nws_lid threshold stages and flows (i.e. bankfull, action, minor,
    moderate, major). Returns a dictionary for stages and one for flows.

    Parameters
    ----------
    threshold_url : STR
        WRDS threshold API.
    location_ids : STR
        nws_lid code (only a single code).
    physical_element : STR, optional
        Physical element option. The default is 'all'.
    threshold : STR, optional
        Threshold option. The default is 'all'.
    bypass_source_flag : BOOL, optional
        Special case if calculated values are not available (e.g. no rating
        curve is available) then this allows for just a stage to be returned.
        Used in case a flow is already known from another source, such as
        a model. The default is False.

    Returns
    -------
    stages : DICT
        Dictionary of stages at each threshold.
    flows : DICT
        Dictionary of flows at each threshold.

    '''

    url = f'{threshold_url}/{physical_element}/{threshold}/{location_ids}'
    response = requests.get(url)
    if response.ok:
        thresholds_json = response.json()
        #Get metadata
        thresholds_info = thresholds_json['stream_thresholds']
        #Initialize stages/flows dictionaries
        stages = {}
        flows = {}
        #Check if thresholds information is populated. If site is non-existent thresholds info is blank
        if thresholds_info:
            #Get all rating sources and corresponding indexes in a dictionary
            rating_sources = {i.get('calc_flow_values').get('rating_curve').get('source'): index for index, i in enumerate(thresholds_info)}
            #Get threshold data use USGS Rating Depot (priority) otherwise NRLDB.
            if 'USGS Rating Depot' in rating_sources:
                threshold_data = thresholds_info[rating_sources['USGS Rating Depot']]
            elif 'NRLDB' in rating_sources:
                threshold_data = thresholds_info[rating_sources['NRLDB']]
            #If neither USGS or NRLDB is available 
            else:
                #A flag option for cases where only a stage is needed for USGS scenario where a rating curve source is not available yet stages are available for the site. If flag is enabled, then stages are retrieved from the first record in thresholds_info. Typically the flows will not be populated as no rating curve is available. Flag should only be enabled when flows are already supplied by source (e.g. USGS) and threshold stages are needed.
                if bypass_source_flag:
                    threshold_data = thresholds_info[0]
                else:
                    threshold_data = []        
            #Get stages and flows for each threshold
            if threshold_data:                
                stages = threshold_data['stage_values']
                flows = threshold_data['calc_flow_values']
                #Add source information to stages and flows. Flows source inside a nested dictionary. Remove key once source assigned to flows.
                stages['source'] = threshold_data['metadata']['threshold_source']
                flows['source'] = flows['rating_curve']['source']
                flows.pop('rating_curve', None)
                #Add timestamp WRDS data was retrieved.
                stages['wrds_timestamp'] = response.headers['Date']
                flows['wrds_timestamp'] = response.headers['Date']                      
    return stages, flows        
        
########################################################################
# Function to write flow file
########################################################################
def flow_data(segments, flows, convert_to_cms = True):
    '''
    Given a list of NWM segments and a flow value in cfs, convert flow to 
    cms and return a DataFrame that is set up for export to a flow file.

    Parameters
    ----------
    segments : LIST
        List of NWM segments.
    flows : FLOAT
        Flow in CFS.
    convert_to_cms : BOOL
        Flag to indicate if supplied flows should be converted to metric. 
        Default value is True (assume input flows are CFS).

    Returns
    -------
    flow_data : DataFrame
        Dataframe ready for export to a flow file.

    '''
    if convert_to_cms:
        #Convert cfs to cms
        cfs_to_cms = 0.3048**3
        flows_cms = round(flows * cfs_to_cms,2)
    else: 
        flows_cms = round(flows,2)
    
    flow_data = pd.DataFrame({'feature_id':segments, 'discharge':flows_cms})
    flow_data = flow_data.astype({'feature_id' : int , 'discharge' : float})
    return flow_data 

