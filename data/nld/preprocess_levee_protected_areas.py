#!/usr/bin/env python3
import argparse
import geopandas as gpd
import logging
import os
import pathlib
import sys

from datetime import datetime

sys.path.append('/foss_fim/src')
from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS
import utils.shared_functions as sf
from utils.shared_functions import FIM_Helpers as fh

def preprocess_levee_protected_areas(source_file_name_and_path,
                                     target_output_folder_path = '',
                                     target_output_filename = 'levee_protected_areas.gpkg',
                                     projection_crs_id = DEFAULT_FIM_PROJECTION_CRS,
                                     overwrite = False):
                                     
    '''
    Overview
    ----------
    This takes a geojson file in (assuming it has geometry), converts it to a gkpg,
    and converts it's projection (id) (defaulting to EPSG:5070)
    
    Parameters:
    ----------
        - source_file_name_and_path (str):
            Name and location of the geojson file. ie) /data/input_temp/LeveedArea.geojson
        - target_output_folder_path (str):
            Location where the output gkpg will be saved (defaults to input folder)
        - target_output_filename (str):
            Output file name (defaults to levee_protected_areas.gpkg)
        - projection_crs_id (str):
            CRS ID that it will be reprojected to (defaults to EPSG:5070). 
            Remember.. projections can and often do get changed during clipping.
            
    Returns:
    ----------
        None
    ''' 

    # -------------------
    # Validation and setup variables
    
    # source file
    if (source_file_name_and_path is None) or (source_file_name_and_path == ''):
        raise ValueError("source file name and path can not be empty")
    source_file_name_and_path = source_file_name_and_path.strip()
    
    if (not os.path.exists(source_file_name_and_path)):
        raise FileNotFoundError("levee geojson source file not found.")
    
    source_file_extension = pathlib.Path(source_file_name_and_path).suffix
    if (source_file_extension.lower() != ".geojson"):
        raise ValueError("source file does not appear to be a geojson file")

    # target file and path
    if (target_output_folder_path is not None) and (target_output_folder_path != ''):
        if (not os.path.exists(target_output_folder_path)):
            os.mkdir(target_output_folder_path)
    else:
        target_output_folder_path = os.path.dirname(source_file_name_and_path)
        
    if (target_output_filename is not None) and (target_output_filename != ''):
        target_file_extension = pathlib.Path(target_output_filename).suffix
        if (target_file_extension.lower() != ".gpkg"):
            raise ValueError("target file name does not appear to be a gpkg file")
    else:
        target_output_filename = 'levee_protected_areas.gpkg'

    target_output_file = os.path.join(target_output_folder_path, target_output_filename)
    if (not overwrite) and os.path.exists(target_output_file):
        raise Exception("Target output file of {target_output_file} already exists. "\
                        "Add overwrite flag or change target path or target file name")
       
    # projection crs
    if (projection_crs_id is None) or (projection_crs_id == ''):
        projection_crs_id = DEFAULT_FIM_PROJECTION_CRS

    # -------------------
    # setup logs
    #start_time = datetime.now()
    print("=======================")
    print("Starting preprocessing levee protected area geojson file")
   
    __setup_logger(target_output_folder_path)
    #sf.setup_logger('preprocess_level_protected_areas', target_output_folder_path)
    logging.info(f"Downloading to {target_output_folder_path}")
    
    # -------------------
    # processing
    
    # load geojson into a dataframe
    # Note:: the logs record a weird line from geopandas
    #    Skipping field stewardOrgIds: invalid type 1 but seems to be fine.
    gdf = gpd.read_file(source_file_name_and_path)
    if (len(gdf) == 0) or (gdf.empty):
        raise Exception("Source geojson file appears to be empty or invalid")
   
    # check current CRS
    gdf_crs = str(gdf.crs)
    #print(gdf_crs)
    
    # don't change it if the projection's already match    
    if (gdf_crs.lower() != projection_crs_id.lower()):
        gdf.to_crs(projection_crs_id, inplace=True)           
    
    gdf.to_file(target_output_file, driver="GPKG", index=False)
    
    msg = f"Preprocessing Complete: file created at {target_output_file}"
    print(msg)
    logging.info(msg)
    
    #end_time = datetime.now()
    #logging.info(fh.print_date_time_duration(start_time, end_time))
        
def __setup_logger(output_folder_path):
    
    root_file_name = "preprocess_level_protected_areas"
    start_time = datetime.now()
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"{root_file_name}-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)    
    
    logging.info(f'Started : {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")

        

if __name__ == '__main__':
    
    # Acquire Notes as of Nov 8, 2022
    '''
    Working with gkpg''s in code is 10 to 20 times faster than working directly with geojson files
    so we will convert it. We have to download the geojson file by hand for now as USACE doesn't have an API
    that allows you to download the entire CONUS+. They do have api's that download levee geojson files at a huc
    level but they don't have geometry. You have to call once for the geojson per huc, then go back for the geometries.
    For now, we will stay with downloading it by hand.
    '''
    
    # Instructions for Downloading (as of Nov 8, 2022)
    #  1) go to :https://levees.sec.usace.army.mil/#/levees/search/&viewType=map&resultsType=systems&advanced=true&hideList=false&eventSystem=false
    #  2) Click on the "Download Data" button in the top right corner and wait for a bit. It takes a bit as it is 200mg but it isn't bad.
    #     It downloads one big LeevedArea.geojson with geometry.

    # Sample Usage: python3 /foss_fim/data/nld/preprocess_levee_protected_areas.py -s /data/inputs/LeveedArea.geojson
    
    parser = argparse.ArgumentParser(description='Preprocess the NLD levee protected areas geojson into a gkpg.')

    parser.add_argument('-s','--source-file-name-and-path', help='location of the geojson that will converted.', 
                        required=True)
    
    parser.add_argument('-t','--target-output-folder-path', help='location of where the gkpg file'\
                        ' will be saved (defaults to input folder path).', required=False)

    parser.add_argument('-f', '--target-output-filename', help='output gpkg file name'\
                        ' (defaults to levee_protected_area.gkpg).', required=False)

    parser.add_argument('-c','--projection-crs-id', help='Reproject to CRS ID (default = EPSG:5070).', 
                        required=False)

    parser.add_argument('-o','--overwrite', help='Overwrite if the file already exists.',
                        required=False,  action='store_true')

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    preprocess_levee_protected_areas(**args)
