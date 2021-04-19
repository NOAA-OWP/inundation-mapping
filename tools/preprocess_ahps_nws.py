#!/usr/bin/env python3
from pathlib import Path
import geopandas as gpd
import pandas as pd
from utils.shared_functions import raster_to_feature, get_rating_curve, get_metadata, get_datum, ngvd_to_navd_ft, find_grids, get_threshold, select_grids, get_nwm_segs, flow_data, process_grid
import numpy as np
import rasterio
from pathlib import Path
import pathlib
import pandas as pd
import rasterio.shutil
import requests
import numpy as np
import rasterio
import geopandas as gpd
from rasterio.warp import calculate_default_transform, reproject, Resampling
import rasterio.crs
from rasterio.merge import merge
from rasterio import features
from shapely.geometry import shape
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns
import re
########################################################
#Preprocess AHPS NWS
#This script will work on NWS AHPS fim data (some assumptions made about the data structure). 
#Provide a source directory path (source_dir) where all NWS AHPS FIM data is located. NWS source data was previously downloaded and extracted. Some data is buried through several layers of subfolders in the source data. In general, the downloaded datasets were unzipped and starting from where the folder name was the AHPS code, this was copied and pasted into a new directory which is the source_dir. 
#Provide a destination directory path (destination) which is where all outputs are located.
#Provide a reference raster path.
########################################################
source_dir = Path(r'path/to/nws/downloads')
destination = Path(r'path/to/preprocessed/nws/data')
reference_raster= Path(r'path/to/reference raster') 
wbd_huc8_path = Path(r'/path/to/wbd/huc8/layer')
metadata_url = f'{API_BASE_URL}/metadata' 
threshold_url = f'{API_BASE_URL}/nws_threshold'
rating_curve_url = f'{API_BASE_URL}/rating_curve'
#Define distance (in miles) to search for nwm segments
nwm_ds_search = 10
nwm_us_search = 10
#The NWS data was downloaded and unzipped. The ahps folder (with 5 digit code as folder name) was cut and pasted into a separate directory. So the ahps_codes iterates through that parent directory to get all of the AHPS codes that have data. 
ahps_codes = [i.name for i in source_dir.glob('*') if i.is_dir() and len(i.name) == 5]
all_df = pd.DataFrame()
#Get mainstems NWM segments
#Workaround for sites in 02030103 and 02030104, many are not rfc_forecast_point = True
list_of_sites = ahps_codes
ms_segs = mainstem_nwm_segs(metadata_url, list_of_sites)

#Find depth grid subfolder
for code in ahps_codes:
    print(code)
    #'mnda2' is in Alaska outside of NWM domain.
    if code in ['mnda2']:
        print(f'skipping {code}')
        continue
       
    #Get metadata of site and search for NWM segments x miles upstream/x miles downstream
    select_by = 'nws_lid'
    selector = [code]    
    metadata_list, metadata_df = get_metadata(metadata_url, select_by, selector, must_include = None, upstream_trace_distance = nwm_us_search, downstream_trace_distance = nwm_ds_search)
    metadata = metadata_list[0]  

    #Assign huc to site using FIM huc layer.
    dictionary, out_gdf = aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes = False)
    [huc] = list(dictionary.keys())
  
    #Get thresholds for action, minor, moderate, major. If no threshold data present, exit.
    #The threshold flows source will dictate what rating curve (and datum) to use as it uses a decision tree (USGS priority then NRLDB)
    #In multiple instances a USGS ID is given but then no USGS rating curve or in some cases no USGS datum is supplied.
    select_by = 'nws_lid'
    selector = code
    stages, flows =get_thresholds(threshold_url, select_by, selector, threshold = 'all')

    #Make sure at least one valid threshold is supplied from WRDS.
    threshold_categories = ['action','minor','moderate','major'] 
    if not any([stages[threshold] for threshold in threshold_categories]):
        print(f'skipping {code} no threshold stages avialable')
        continue
    
    #determine source of interpolated threshold flows, this will be the rating curve that will be used.
    rating_curve_source = flows['source']

    #Custom workaround for bmbp1 to get a datum supplied in metadata. Although a USGS ID is given, no datum information. A nws supplied datum is supplied.
    if code == 'bmpb1':
        rating_curve_source = 'USGS Rating Depot'
        print(f'{code} workaround')

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
        print(f'{code} is missing datum')
        continue        
    
    #Adjust datum to NAVD88 if needed
    if datum_data.get('vcs') == 'NGVD29':
        #Get the datum adjustment to convert NGVD to NAVD. Sites not in contiguous US are previously removed otherwise the region needs changed.
        datum_adj_ft = ngvd_to_navd_ft(datum_info = datum_data, region = 'contiguous')
        datum88 = round(datum + datum_adj_ft, 2)
    else:
        datum88 = datum

    #get entire rating curve, same source as interpolated threshold flows (USGS Rating Depot first then NRLDB rating curve).
    if rating_curve_source == 'NRLDB':
        site = [code]
    elif rating_curve_source == 'USGS Rating Depot':
        site = [metadata.get('identifiers').get('usgs_site_code')]
        
    rating_curve = get_rating_curve(rating_curve_url, site)
    #If rating curve is not present, skip site
    if rating_curve.empty:
        print(f'skipping {code} no rating curve')
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
    if vcs == 'NGVD29':        
        #Add field with raw elevation conversion (datum + stage)
        rating_curve['elevation_ngvd29'] = rating_curve['stage'] + datum
        #Add field with adjusted NAVD88 datum
        rating_curve['datum_navd88'] = datum88

    #Add field with NAVD88 elevation
    rating_curve['elevation_navd88'] = rating_curve['stage'] + datum88

    
    #Search through ahps directory find depth grid folder
    parent_path = source_dir / code    
      
    #Work around for bgwn7 and smit2 where grids were custom created from polygons (bgwn7-no grids, smit2 - no projection and applying projection from polygons had errors)
    if code in ['bgwn7', 'smit2']:
        [grids_dir] = [directory for directory in parent_path.glob('*custom*') if directory.is_dir()]
    else:
        #Find the directory containing depth grids. Assumes only one directory will be returned.
        [grids_dir] = [directory for directory in parent_path.glob('*depth_grid*') if directory.is_dir()]        
        
    #Get grids (all NWS ESRI grids were converted to Geotiff)
    grid_paths = [grids for grids in grids_dir.glob('*.tif*') if grids.suffix in ['.tif', '.tiff']]
    grid_names = [name.stem for name in grid_paths]
    #If grids are present, interpolate a flow for the grid.
    if grid_paths:
        #Construct Dataframe containing grid paths, names, datum, code
        df = pd.DataFrame({'code': code, 'path':grid_paths, 'name': grid_names, 'datum88': datum88})
        #Determine elevation from the grid name. All elevations are assumed to be in NAVD88 based on random inspection of AHPS inundation website layers.
        df['elevation'] = df['name'].str.replace('elev_', '', case = False).str.replace('_','.').astype(float)             
        # Add a stage column using the datum (in NAVD88). Stage is rounded to the nearest 0.1 ft.
        df['stage'] = round(df['elevation'] - df['datum88'],1)                                
        #Sort stage in ascending order
        df.sort_values(by = 'elevation', ascending = True, inplace = True)
        #Interpolate flow from the rating curve using the elevation_navd88 values, if value is above or below the rating curve assign nan.
        df['flow'] = np.interp(df['elevation'], rating_curve['elevation_navd88'], rating_curve['flow'], left = np.nan, right = np.nan)        
        #Assign flow source to reflect interpolation from rc
        df['flow_source'] = f'interpolated from {rating_curve_source} rating curve'
        
        #Optional, append all dataframes
        all_df = all_df.append(df)
    else: 
        print(f'{code} has no grids')
   
    #Select the appropriate threshold grid for evaluation. Using the supplied threshold stages and the calculated map stages. 
    grids,grid_flows = select_grids(df, stages, datum88, 1.1)

    #workaroud for bigi1 and eag1 which have gridnames based on flows (not elevations)
    if code in ['eagi1', 'bigi1']:
        #Elevation is really flows (due to file names), assign this to stage
        df['flow'] = df['elevation']
        df['stage'] = df['elevation']
        #Select grids using flows
        grids, grid_flows = select_grids(df, flows, datum88, 500)
        print(f'{code} workaround')

    #Obtain NWM segments that are on ms to apply flows
    segments = get_nwm_segs(metadata)
    site_ms_segs = set(segments).intersection(ms_segs)
    segments = list(site_ms_segs)

    #Write out boolean benchmark raster and flow file
    try:
        #for each threshold
        for i in ['action', 'minor', 'moderate', 'major']:
            #Obtain the flow and grid associated with threshold.
            flow = grid_flows[i]
            grid = grids[i]
            extent = grids['extent']
            #Make sure that flow and flow grid are valid
            if not grid in ['No Map', 'No Threshold', 'No Flow']:
                #Create output directory
                outputdir = destination / huc / code / i
                outputdir.mkdir(parents = True, exist_ok = True)                                
                #Create the guts of the flow file.
                flow_info = flow_data(segments,flow)
                #Write out the flow file to csv
                output_flow_file = outputdir / (f'ahps_{code}_huc_{huc}_flows_{i}.csv')
                flow_info.to_csv(output_flow_file, index = False)

                #Create Binary Grids, first create domain of analysis, then create binary grid
                
                #Domain extent is largest floodmap in the static library WITH holes filled
                filled_domain_raster = outputdir.parent / f'{code}_extent.tif'

                #Open benchmark data as a rasterio object.
                benchmark = rasterio.open(grid)
                benchmark_profile = benchmark.profile   

                #Open extent data as rasterio object
                domain = rasterio.open(extent)
                domain_profile = domain.profile

                #if grid doesn't have CRS, then assign CRS using a polygon from the ahps inundation library
                if not benchmark.crs:
                    #Obtain crs of the first polygon inundation layer associated with ahps code. Assumes only one polygon* subdirectory and assumes the polygon directory has at least 1 inundation shapefile.
                    [ahps_polygons_directory] = [directory for directory in parent_path.glob('*polygon*') if directory.is_dir()]
                    shapefile_path = list(ahps_polygons_directory.glob('*.shp'))[0]
                    shapefile = gpd.read_file(shapefile_path)
                    #Update benchmark and domain profiles with crs from shapefile. Assumed that benchmark/extent have same crs.
                    benchmark_profile.update(crs = shapefile.crs)  
                    domain_profile.update(crs = shapefile.crs)

                #Create a domain raster if it does not exist.
                if not filled_domain_raster.exists():
                    #Domain should have donut holes removed
                    process_extent(domain, domain_profile, output_raster = filled_domain_raster)


                #Open domain raster as rasterio object
                filled_domain = rasterio.open(filled_domain_raster)
                filled_domain_profile = filled_domain.profile

                #Create the binary benchmark raster                        
                boolean_benchmark, boolean_profile = process_grid(benchmark, benchmark_profile, filled_domain, filled_domain_profile, reference_raster)    
                
                #Output binary benchmark grid and flow file to destination
                output_raster = outputdir / (f'ahps_{code}_huc_{huc}_depth_{i}.tif')
                with rasterio.Env():
                    with rasterio.open(output_raster, 'w', **boolean_profile) as dst:
                        dst.write(boolean_benchmark,1) 
                
                #Close datasets
                domain.close()
                filled_domain.close()
                benchmark.close()
                                       
    except:
        print(f'issue with {code}')                
    #Process extents, only create extent if ahps code subfolder is present in destination directory.
    ahps_directory = destination / huc / code
    if ahps_directory.exists():
        #Delete extent raster
        filled_extent = ahps_directory / f'{code}_extent.tif' 
        if filled_extent.exists:
            filled_extent.unlink()              

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

        #Write the rating curve to a file
        rating_curve_output = ahps_directory / (f'{code}_rating_curve.csv')
        rating_curve['lat'] = datum_data['lat']
        rating_curve['lon'] = datum_data['lon']
        rating_curve.to_csv(rating_curve_output, index = False)
        
        #Write the interpolated flows to file
        df_output = ahps_directory / (f'{code}_interpolated_flows.csv')
        df.to_csv(df_output, index = False)
    
    else: 
        print(f'{code} missing all flows')