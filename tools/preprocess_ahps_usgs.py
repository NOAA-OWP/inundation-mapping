#!/usr/bin/env python3
import numpy as np
from pathlib import Path
import pandas as pd
import rasterio
import requests
from tools_shared_functions import mainstem_nwm_segs, get_metadata, aggregate_wbd_hucs, get_thresholds, get_datum, ngvd_to_navd_ft, get_rating_curve, select_grids, get_nwm_segs, flow_data, process_extent, process_grid, raster_to_feature
import argparse
from dotenv import load_dotenv
import os
import sys
sys.path.append('/foss_fim/src')
import traceback


def get_env_paths():
    load_dotenv()
    #import variables from .env file
    API_BASE_URL = os.getenv("API_BASE_URL")
    EVALUATED_SITES_CSV = os.getenv("EVALUATED_SITES_CSV")
    WBD_LAYER = os.getenv("WBD_LAYER")
    USGS_METADATA_URL = os.getenv("USGS_METADATA_URL")   
    return API_BASE_URL, EVALUATED_SITES_CSV, WBD_LAYER, USGS_METADATA_URL
###############################################################################
#Get USGS Site metadata
###############################################################################
def usgs_site_metadata(code):
    '''
    Retrieves site metadata from USGS API and saves output as dictionary. Information used includes shortname and site number.
    
    Parameters
    ----------
    code : STR
        AHPS code.
    USGS_METADATA_URL : STR
        URL for USGS datasets.

    Returns
    -------
    site_metadata : DICT
        Output metadata for an AHPS site.
    '''
    # Make sure code is lower case
    code = code.lower()
    # Get site metadata from USGS API using ahps code
    site_url = f'{USGS_METADATA_URL}/server/rest/services/FIMMapper/sites/MapServer/0/query?where=AHPS_ID+%3D+%27{code}%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=pjson'
    #Get data from API
    response = requests.get(site_url)
    #If response is valid, then get metadata and save to dictionary
    if response.ok:
        response_json = response.json()
        site_metadata = response_json['features'][0]['attributes']
    return site_metadata

###############################################################################
#Get USGS grid metadata
###############################################################################
def usgs_grid_metadata(code, has_grid_override = False):    
    '''
    Given an ahps code, retrieve the site metadata (using usgs_site_metadata) and then use that information to obtain metadata about available grids. Information includes elevation, stage, and flow for each grid. 

    Parameters
    ----------
    code : STR
        AHPS code.

    Returns
    -------
    appended_dictionary : DICT
        Dictionary of metadata for each available inundation grid including grid id, flows, elevations, grid name for each inundation grid.
    '''
    #Make sure code is in lower case
    code = code.lower()   
    # Get site_metadata
    site_metadata = usgs_site_metadata(code)    
    #From site metadata get the SHORT_NAME, SITE_NO, and 'MULTI_SITE', 'HAS_GRIDS' key values
    short_name = site_metadata['SHORT_NAME']
    site_no = site_metadata['SITE_NO']
    has_grids = site_metadata['HAS_GRIDS']
    #There is at least one site (kilo1) that doesn't have grids but polygons are available which have been converted grids.
    if has_grid_override:
        has_grids = 1
    multi_site = site_metadata['MULTI_SITE']    
    #Grid metadata located at one of three URLs
    if multi_site == 0 and has_grids == 1:
        grids_url = f'{USGS_METADATA_URL}/server/rest/services/FIMMapper/floodExtents/MapServer/0/query?where=USGSID+%3D+%27{site_no}%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=pjson'
    elif multi_site > 0 and multi_site < 3 and has_grids == 1:
        grids_url = f'{USGS_METADATA_URL}/server/rest/services/FIMMapper/floodExtentsMulti/MapServer/0/query?where=USGSID_1+%3D+%27{site_no}%27+OR+USGSID_2+%3D+%27{site_no}%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=pjson'
    elif multi_site == 3 and has_grids == 1:
        grids_url = f'{USGS_METADATA_URL}/server/rest/services/FIMMapper/floodExtentsThreeSites/MapServer/0/query?where=USGSID_1+%3D+%27{site_no}%27+OR+USGSID_2+%3D+%27{site_no}%27+OR+USGSID_3+%3D+%27{site_no}%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=pjson'            
    #Only get metadata on grids if site has grids available
    if has_grids == 1:
        #Get data from API
        response = requests.get(grids_url)
        #If response is valid then combine metadata on all grids into a single dictionary and write out to DataFrame.
        if response.ok:
            response_json =response.json()
            metadata = response_json['features']
            appended_dictionary = {}
            for i in metadata:
                dictionary = i['attributes']
                gridname = short_name + '_' + str(dictionary['GRIDID']).zfill(4)
                appended_dictionary[gridname] = dictionary
    else: 
        appended_dictionary = {}
    return appended_dictionary


########################################################
#Preprocess USGS FIM
#This script will work on USGS FIM datasets. 
#Provide source directory path (source_dir) where all USGS FIM data is located. This data was previously downloaded from USGS urls.
#Provide a destination directory path (destination) where all outputs are located.
#Provide a reference raster path.
########################################################
#source_dir = Path(r'path/to/usgs/downloads')
#destination = Path(r'path/to/preprocessed/usgs/data')
#reference_raster= Path(r'path/to/reference raster') 
def preprocess_usgs(source_dir, destination, reference_raster):
    '''
    Preprocess USGS AHPS datasets.

    Parameters
    ----------
    source_dir : str
        Path to USGS Benchmark Datasets (AHPS)
    destination : str
        Path to output directory of preprocessed datasets.
    reference_raster : str
        Path to reference raster for benchmark binary raster creation.

    Returns
    -------
    None.

    '''
    
    source_dir = Path(source_dir)
    destination = Path(destination)
    reference_raster = Path(reference_raster)
    metadata_url = f'{API_BASE_URL}/metadata' 
    threshold_url = f'{API_BASE_URL}/nws_threshold'
    rating_curve_url = f'{API_BASE_URL}/rating_curve'
    
    #Write log file
    destination.mkdir(parents=True, exist_ok = True)
    log_file = destination / 'log.txt'
    f = open(log_file, 'a+')
    
    #Define distance (in miles) to search for nwm segments
    nwm_ds_search = 10
    nwm_us_search = 10
    #Need a list of AHPS codes. See "ahps_dictionaries" for method of getting this list.
    ahps_codes = [folder.name for folder in source_dir.glob('*') if len(folder.name) == 5]
    
    #Get mainstems NWM segments
    #Workaround for sites in 02030103 and 02030104, many are not rfc_forecast_point = True
    #Import list of evaluated sites
    list_of_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].to_list()
    ms_segs = mainstem_nwm_segs(metadata_url, list_of_sites)
    
    for code in ahps_codes:
        f.write(f'{code} : Processing\n')
        print(f'processing {code}')
        #For a given code, find all inundation grids under that code.
        code = code.lower()
          
        #Get metadata of site and search for NWM segments x miles upstream/x miles downstream
        select_by = 'nws_lid'
        selector = [code]    
        metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector, must_include = None, upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search)
        metadata = metadata_list[0]
        
        #Assign huc to site using FIM huc layer.
        dictionary, out_gdf = aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes = False)
        [huc] = list(dictionary.keys())
    
        #There are 12 sites with special issues such as these don't have any crs coordinates and grid/polygon data don't align or missing grid data but polygons are available.
        #Sites with no grid data but polygon data --> cfmm8, kilo1
        #Sites with no projection assigned to grid and polygon/grid don't align --> stak1, nmso1, nori3, sasi3
        #Sites with reprojection issues using rasterio (manually reprojected with ESRI) --> kcdm7, knym7, mcri2, ptvn6, tmai4
        #Sites with incomplete grids (used polys to convert to grids) --> 'roun6'
        ahps_dir = source_dir / code / 'depth_grids'
        if code in ['cfmm8','kilo1','stak1', 'sasi3', 'nori3', 'nmso1', 'kcdm7', 'knym7', 'mcri2','ptvn6','tmai4', 'roun6']:        
            f.write(f'{code} : Custom workaround related to benchmark data (mismatch crs, no grid data, etc)\n')
            ahps_dir = source_dir / code / 'custom'
    
        #Get thresholds (action/minor/moderate/major flows and stages), if not available exit.
        #For USGS many sites may not have rating curves but the threshold stages are available.
    
        select_by = 'nws_lid'
        selector = code
        stages, flows =get_thresholds(threshold_url, select_by, selector, threshold = 'all')
       
        #Make sure at least one valid threshold is supplied from WRDS.
        threshold_categories = ['action','minor','moderate','major'] 
        if not any([stages[threshold] for threshold in threshold_categories]):
            f.write(f'{code} : Skipping because no threshold stages available\n')
            continue
    
        #We need to adjust stages to elevations using the datum adjustment. This next section finds the datum adjustment.
        #determine primary source for interpolated threshold flows (USGS first then NRLDB). This will dictate what rating curve to pull.
        rating_curve_source = flows['source']
        #Workaround for sites that don't have rating curve but do have flows specified (USGS only). Assign rating_curve_source to 'USGS Rating Depot' manually inspected all of these sites and USGS datum is available and will be used.
        if code in ['bdxt1','ccti3', 'fnnm7', 'mtao1', 'nfsi3', 'omot1' , 'sbrn1', 'vron4', 'watv1']:
            rating_curve_source = 'USGS Rating Depot'         
    
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
            f.write(f'{code}: Skipping because of missing datum\n')
            continue        

        #Custom workaround, some sites have poorly defined crs. CRS requuired for ngvd to navd conversions
        # Assumed NAVD88 (no info from USGS or NWS metadata): kynm7, ksvm7, yesg1
        # Assigned NAVD88 because USGS metadata indicates NAD83: arnm7, grfi2, kctm7, nast1, nhri3, roun6, vllm7
        # Assigned NAVD88 (reported NAVD 1988): cmtl1
        if code in ['arnm7', 'cmtl1','grfi2','kctm7','knym7','ksvm7','nast1','nhri3','roun6','vllm7','yesg1']:
            #Update crs to NAD83 (some are assumed, others have USGS info indicating NAD83 crs)
            datum_data.update(crs = 'NAD83')
        
        #Adjust datum to NAVD88 if needed (Assumes that if vcs not NGVD29 or NGVD 1929 it is in NAVD88)
        if datum_data.get('vcs') in ['NGVD29', 'NGVD 1929']:
            #Get the datum adjustment to convert NGVD to NAVD. Sites not in contiguous US are previously removed otherwise the region needs changed.
            datum_adj_ft = ngvd_to_navd_ft(datum_info = datum_data, region = 'contiguous')
            datum88 = round(datum + datum_adj_ft, 2)
        else:
            datum88 = datum
    
    
        #Set Grid override flag, if set to True then the 'has_grids' property is ignored. Allows for custom workaround.
        #Special exception for kilo1, where it has attribute (has_grids == 0) yet there is grid metadata and polygons were converted to grids.
        if code == 'kilo1':
            grid_override = True
            f.write(f'{code} : Custom workaround related to "has_grids" attribute')
        else: 
            grid_override = False        
        #get grid metadata (metadata includes, elevation/stage/flow and etc for each site). If empty exit.
        grid_metadata = usgs_grid_metadata(code, has_grid_override=grid_override)
        if not grid_metadata:
            f.write(f'{code} : Skipping because no grid metadata available\n')
            continue
         
        #Get paths of all grids that have been downloaded, if no grids available for site then exit.
        grid_paths = [grids for grids in ahps_dir.glob('*.tif*') if grids.suffix in ['.tif', '.tiff']]
        if not grid_paths:
            f.write(f'{code} : Skipping because no benchmark grids available\n')
            continue
        
        # Iterate through grid_metadata and add the path to the dictionary as well as an indicator of whether the path exists.
        for key in grid_metadata:        
            #When USGS grid data was downloaded, grid was saved with the 'key' name. Update the grid_metadata to include the path.
            path = ahps_dir / (key + '.tif')
            grid_metadata[key]['path'] = path                
            #Verify that the path exists (in some instances the grid should be available but it isn't) and add as subkey
            if path.is_file():
                grid_metadata[key]['path_exist'] = True
            else:
                grid_metadata[key]['path_exist'] = False
        
        #Convert grid metadata information to a DataFrame
        df = pd.DataFrame.from_dict(grid_metadata, orient = 'index')
        #Filter out rows where grids do not exist
        df = df.query('path_exist == True')
        #Prior to renaming columns do a check to make sure single site (will add functionality for multi-sites later)
        if not 'QCFS' in df.columns:
            f.write(f'{code} : Skipping because multisite\n')
            continue
        #Rename columns to match NWS AHPS data structure, this only applies to single USGS sites, if a multisite the columns are different from QCFS.
        df.rename(columns = {'QCFS':'flow', 'STAGE':'stage', 'ELEV':'elevation'}, inplace=True)
        #Many USGS maps have elevations to numerous decimal places. Round to nearest tenth. 
        #NWS has maps to nearest tenth, for example HARP1 is both USGS and NWS, the USGS maps are to the hundredth of foot and NWS are to tenth.
        df['elevation'] = round(df['elevation'],1)
        #Assume flow source is supplied, if it is interpolated, this column will be changed later on.
        df['flow_source'] = 'supplied by USGS'
        #Accomodate for vdsg1 (upon inspection WRDS API reports thresholds in elevation instead of stage for this site)
        if code == 'vdsg1':
            df['stage'] = df['elevation']
            f.write(f'{code} : Custom workaround because thresholds are reported as elevations\n')
        
        #Define rating curve as empty dataframe, populate if needed.
        rating_curve = pd.DataFrame()        
        #If flows are missing from the grid metadata, then interpolate flows using NWS or USGS rating curve
        if df['flow'].isnull().all():        
            #get entire rating curve, same source as interpolated threshold flows (USGS Rating Depot first then NRLDB rating curve).
            if rating_curve_source == 'NRLDB':
                site = [code]
            elif rating_curve_source == 'USGS Rating Depot':
                site = [metadata.get('identifiers').get('usgs_site_code')]
            
            rating_curve = get_rating_curve(rating_curve_url, site)
    
            #If rating curve is not present, skip site
            if rating_curve.empty:
                f.write(f'{code} : Skipping because no rating curve\n')
                continue
            #Add elevation fields to rating curve
            #Add field with vertical coordinate system
            vcs = datum_data['vcs']
            if not vcs:
                vcs = 'Unspecified, Assumed NAVD88'
            rating_curve['vcs'] = vcs
    
            #Add field with original datum
            rating_curve['datum'] = datum
            
            #If VCS is NGVD29 add rating curve elevation (in NGVD) as well as the NAVD88 datum
            if vcs in ['NGVD29', 'NGVD 1929']:        
                #Add field with raw elevation conversion (datum + stage)
                rating_curve['elevation_ngvd29'] = rating_curve['stage'] + datum
                #Add field with adjusted NAVD88 datum
                rating_curve['datum_navd88'] = datum88
            #Add field with NAVD88 elevation
            rating_curve['elevation_navd88'] = rating_curve['stage'] + datum88
            # sort inundation grids in ascending order based on stage
            df.sort_values(by = 'elevation', ascending = True, inplace = True)        
            #interpolate based on stage (don't need elevation because we have stage of floodgrid)
            df['flow'] = np.interp(df['elevation'], rating_curve['elevation_navd88'], rating_curve['flow'], left = np.nan, right = np.nan)
            #Overwrite flow source to reflect interpolation from rc
            df['flow_source'] = f'interpolated from {rating_curve_source} rating curve'
            
        #Select the appropriate threshold grid for evaluation. Using the supplied threshold stages and the calculated map stages. 
        grids,grid_flows = select_grids(df, stages, datum88, 1.1)
    
        #Obtain NWM segments that are on ms to apply flows
        segments = get_nwm_segs(metadata)
        site_ms_segs = set(segments).intersection(ms_segs)
        segments = list(site_ms_segs)
        #Preprocess grids and export to file and create flow file.
        try:
            #for each threshold
            for i in threshold_categories:
                #Obtain the flow and grid associated with threshold as well as extent grid which serves as the domain.
                flow = grid_flows[i]
                grid = grids[i]
                extent = grids['extent']
                #Make sure that flow and flow grid are valid
                if not grid in ['No Map', 'No Threshold', 'No Flow']:
                    #Define output directory (to be created later)
                    outputdir = destination / huc / code / i                    
                    
                    #Create Binary Grids, first create domain of analysis, then create binary grid
                    
                    #Domain extent is largest floodmap in the static library WITH holes filled
                    filled_domain_raster = outputdir.parent / f'{code}_filled_orig_domain.tif'
                    #Create a domain raster if it does not exist.
                    if not filled_domain_raster.exists():
                        #Open extent data as rasterio object
                        domain = rasterio.open(extent)
                        domain_profile = domain.profile
                        #Domain should have donut holes removed
                        process_extent(domain, domain_profile, output_raster = filled_domain_raster)
    
                    #Open domain raster as rasterio object
                    filled_domain = rasterio.open(filled_domain_raster)
                    filled_domain_profile = filled_domain.profile
    
                    #Open benchmark data as a rasterio object.
                    benchmark = rasterio.open(grid)
                    benchmark_profile = benchmark.profile                
    
                    #Create the binary benchmark raster                        
                    boolean_benchmark, boolean_profile = process_grid(benchmark, benchmark_profile, filled_domain, filled_domain_profile, reference_raster)    
                                    
                    #Output binary benchmark grid and flow file to destination
                    outputdir.mkdir(parents = True, exist_ok = True)
                    output_raster = outputdir / (f'ahps_{code}_huc_{huc}_extent_{i}.tif')
                    with rasterio.Env():
                        with rasterio.open(output_raster, 'w', **boolean_profile) as dst:
                            dst.write(boolean_benchmark,1)
                    
                    #Close datasets
                    domain.close()
                    filled_domain.close()
                    benchmark.close()
                    
                    #Create the guts of the flow file.
                    flow_info = flow_data(segments,flow) 
                    #Write out the flow file to csv
                    output_flow_file = outputdir / (f'ahps_{code}_huc_{huc}_flows_{i}.csv')
                    flow_info.to_csv(output_flow_file, index = False)
                    
        except Exception as e:
            f.write(f'{code} : Error preprocessing benchmark\n{repr(e)}\n')
            f.write(traceback.format_exc())
            f.write('\n')
            print(traceback.format_exc())
        #Wrapup for ahps sites that were processed.
        ahps_directory = destination / huc / code
        if ahps_directory.exists():
            #Delete original filled domain raster (it is an intermediate file to create benchmark data)
            orig_domain_grid = ahps_directory / f'{code}_filled_orig_domain.tif'
            orig_domain_grid.unlink()            
            #Create domain shapefile from any benchmark grid for site (each benchmark has domain footprint, value = 0).
            filled_extent = list(ahps_directory.rglob('*_extent_*.tif'))[0]
            domain_gpd = raster_to_feature(grid = filled_extent, profile_override = False, footprint_only = True)           
            domain_gpd['nws_lid'] = code
            domain_gpd.to_file(ahps_directory / f'{code}_domain.shp')   
            #Populate attribute information for site
            grids_attributes = pd.DataFrame(data=grids.items(), columns = ['magnitude','path'])
            flows_attributes = pd.DataFrame(data=grid_flows.items(), columns=['magnitude','grid_flow_cfs'])
            threshold_attributes = pd.DataFrame(data=stages.items(), columns = ['magnitude','magnitude_stage'])       
            #merge dataframes
            attributes = grids_attributes.merge(flows_attributes, on = 'magnitude')
            attributes = attributes.merge(threshold_attributes, on = 'magnitude')
            attributes = attributes.merge(df[['path','stage','elevation', 'flow_source']], on = 'path')        
            #Strip out sensitive paths and convert magnitude stage to elevation
            attributes['path'] = attributes['path'].apply(lambda x :Path(x).name)
            attributes['magnitude_elev_navd88']=(datum88 + attributes['magnitude_stage']).astype(float).round(1)
            #Add general site information
            attributes['nws_lid'] = code
            attributes['wfo'] = metadata['nws_data']['wfo']
            attributes['rfc'] = metadata['nws_data']['rfc']
            attributes['state'] = metadata['nws_data']['state']
            attributes['huc'] = huc
            #Rename and Reorder columns
            attributes.rename(columns = {'path':'grid_name', 'flow_source':'grid_flow_source','stage':'grid_stage','elevation':'grid_elev_navd88'}, inplace = True)        
            attributes = attributes[['nws_lid','wfo','rfc','state','huc','magnitude','magnitude_stage','magnitude_elev_navd88','grid_name','grid_stage','grid_elev_navd88', 'grid_flow_cfs','grid_flow_source']]        
            #Save attributes to csv
            attributes.to_csv(ahps_directory / f'{code}_attributes.csv', index = False) 
                
            #if rating_curve generated, write the rating curve to a file
            if not rating_curve.empty:
                rating_curve_output = ahps_directory / (f'{code}_rating_curve.csv')
                rating_curve['lat'] = datum_data['lat']
                rating_curve['lon'] = datum_data['lon']
                rating_curve.to_csv(rating_curve_output, index = False)
                f.write(f'{code} : Rating curve needed to interpolate flow\n')
            
            #Write the interpolated flows to file
            df_output = ahps_directory / (f'{code}_flows.csv')
            df.to_csv(df_output, index = False)
            
        else: 
            f.write(f'{code} : Unable to evaluate site, missing all flows\n')
    
    f.close()
    
    #Combine all attribute files
    attribute_files = list(destination.rglob('*_attributes.csv'))
    all_attributes = pd.DataFrame()
    for i in attribute_files:
        attribute_df = pd.read_csv(i, dtype={'huc':str})
        all_attributes = all_attributes.append(attribute_df)
    if not all_attributes.empty:
        all_attributes.to_csv(destination / 'attributes.csv', index = False)
            
    return 

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Create preprocessed USGS benchmark datasets at AHPS locations.')
    parser.add_argument('-s', '--source_dir', help = 'Workspace where all source data is located.', required = True)
    parser.add_argument('-d', '--destination',  help = 'Directory where outputs are to be stored', required = True)
    parser.add_argument('-r', '--reference_raster', help = 'reference raster used for benchmark raster creation', required = True)
    args = vars(parser.parse_args())
    

    #Run get_env_paths and static_flow_lids
    API_BASE_URL, EVALUATED_SITES_CSV, WBD_LAYER, USGS_METADATA_URL = get_env_paths()
    preprocess_usgs(**args)