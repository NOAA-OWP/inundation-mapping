#!/usr/bin/env python3
import urllib.request 
from pathlib import Path
from dotenv import load_dotenv
import os
import argparse
import requests
from collections import defaultdict
import urllib
import pandas as pd

load_dotenv()
USGS_DOWNLOAD_URL = os.getenv("USGS_DOWNLOAD_URL")
USGS_METADATA_URL = os.getenv("USGS_METADATA_URL")   
EVALUATED_SITES_CSV = os.getenv("EVALUATED_SITES_CSV")
###############################################################################
#Get all usgs grids available for download. This step is required because the grid metadata API returns gridID as an integer and truncates leading zeros found in grid names.
###############################################################################            
def get_all_usgs_gridnames():
    '''
    Retrieve all the available grids for download from USGS. This is necessary as the grid metadata available from USGS API doesn't preserve leading zeros.

    Returns
    -------
    grid_lookup : collections.defaultdict
        Dictionary with shortname as the key and a list of gridnames associated with a given shortname as values.
    '''
    
    #Grid names are split between 4 websites
    sites = ['grids_1', 'grids_2', 'grids_3', 'grids_4']
    #Append all grid names to this variable
    grid_names = []
    #loop through each site and append the grid name to a list.
    for i in sites:
        #Get gridnames
        url = f'{USGS_METADATA_URL}/server/rest/services/FIMMapper/{i}/MapServer?f=pjson'
        response = requests.get(url)
        site_json = response.json()
        info = site_json['layers']
        #Loop through all grid info and extract the grid name.
        for i in info:
            grid_name = i['name']
            grid_names.append(grid_name)
    #Create dictionary with key of shortname and values being list of grids available.    
    grid_lookup = defaultdict(list)
    for i in grid_names:
        #define key (shortname) and value (gridname)
        key = i.split('_')[0]
        value = i   
        grid_lookup[key].append(value)
    return grid_lookup
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
########################################################################
#Get USGS Benchmark Grids
########################################################################
def obtain_usgs_data(workspace):
    '''
    Download GRIDS from USGS FIM studies

    Parameters
    ----------
    workspace : STR
        Output directory where grids are placed.

    Returns
    -------
    None.

    '''
    
    
    #Define workspace where output data is downloaded to
    workspace = Path(workspace)
    #Get all names of grids available for download from USGS website.
    grid_lookup = get_all_usgs_gridnames()
    #List of target ahps codes. In "ahps_dictionary.py" we defined a dictionary (ahps_lookup) that contains all ahps codes and their sources.
    target_ahps_codes = pd.read_csv(EVALUATED_SITES_CSV)
    target_ahps_codes = target_ahps_codes.query('Source in ["Both","USGS"]')['Total_List'].to_list()
    #Loop through all codes in the target_ahps_codes list.
    all_messages = []
    for code in target_ahps_codes:
        #Get metadata information related to ahps code from USGS API.
        code_metadata = usgs_site_metadata(code)
        #From code_metadata get the shortname and site_no associated with the code.
        shortname = code_metadata['SHORT_NAME']
        site_no = code_metadata['SITE_NO']     
        #Define the output location for all grids and create if it doesn't exist.
        dest_dir = workspace / code.lower() / 'depth_grids'
        dest_dir.mkdir(parents = True, exist_ok = True)    
        #Get list of all available grids for download using the grid_lookup dictionary
        gridnames = grid_lookup[shortname]    
        #Loop through all available grids for download, download them, and save to defined location.
        for gridname in gridnames:
            print(f'working on {gridname}')
            gridid = gridname.split('_')[1]
            #Define a filled gridID that has leading zeros out to 4 digits.
            filled_gridid = gridid.zfill(4)       
            #Download gridded data from the USGS s3 website. The files will be copied specified directory and the GRIDID will have 4 digits with leading zeros.
            base_url = f'{USGS_DOWNLOAD_URL}/FIM/tmp1/fimgrids2iwrss/'       
            #Each grid dataset has these file extensions. Download each file
            extensions = ['.tif', '.tfw', '.tif.aux.xml', '.tif.ovr', '.tif.xml']
            #Loop through each extension type and download.
            for gridext in extensions:
                #Define the url associated with each grid
                url = base_url + gridname + gridext       
                #Define the output file path of the grid. The grid filename uses the filled gridID. This resolves issues down the road of USGS grid metadata information storing the gridid as a number and truncating leading zeros from the gridname.
                saved_grid_path = dest_dir / (f'{shortname}_{filled_gridid}{gridext}')
                #Check to see if file has already been downloaded
                if not saved_grid_path.is_file():
                    #If file hasn't been downloaded, download it. If there was an error downloading, make note.
                    try:
                        urllib.request.urlretrieve(url, saved_grid_path)
                        message = f'{gridname} downloaded'
                        all_messages.append(message)
                    except: 
                        message = f'{gridname} error downloading'
                        all_messages.append(message)
                #If file exists make note of it.
                else:
                    message = f'skipping {gridname}, exists on file'
                    all_messages.append(message)
    return

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Download Grid data associated with USGS FIM studies.')
    parser.add_argument('-w', '--workspace', help = 'Workspace where all outputs will be saved.', required = True)
    args = vars(parser.parse_args())
    
    #Download datasets
    obtain_usgs_data(**args)


