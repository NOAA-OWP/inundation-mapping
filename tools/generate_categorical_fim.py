#!/usr/bin/env python3
import subprocess
import argparse
import time
from pathlib import Path
import geopandas as gpd
import pandas as pd
from datetime import date

def update_mapping_status(output_mapping_dir, output_flows_dir):
    
    #Find all LIDs with empty mapping output folders
    subdirs = [str(i) for i in Path(output_mapping_dir).rglob('**/*') if i.is_dir()]
    empty_nws_lids = [Path(directory).name for directory in subdirs if not list(Path(directory).iterdir())]
    
    #Write list of empty nws_lids to DataFrame
    mapping_df = pd.DataFrame({'nws_lid':empty_nws_lids})
    mapping_df['did_it_map'] = 'No'
    mapping_df['map_status'] = ' and all categories failed to map'
    
    #Import shapefile output from flows creation 
    shapefile = Path(output_flows_dir)/'nws_lid_flows_sites.shp'
    flows_df = gpd.read_file(shapefile)
    
    #Join failed sites to flows df    
    flows_df = flows_df.merge(mapping_df, how = 'left', on = 'nws_lid')
    
    #Switch mapped column to no for failed sites and update status
    flows_df.loc[flows_df['did_it_map'] == 'No', 'mapped'] = 'No'
    flows_df.loc[flows_df['did_it_map']=='No','status'] = flows_df['status'] + flows_df['map_status']
    
    #Clean up GeoDataFrame and write out to file.
    flows_df = flows_df.drop(columns = ['did_it_map','map_status'])
    #Output nws_lid site
    nws_lid_path = Path(output_mapping_dir.parent) / 'nws_lid_sites.shp'
    flows_df.to_file(nws_lid_path)
    
if __name__ == '__main__':
    
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Run Categorical FIM')
    parser.add_argument('-f','--fim_version',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-j','--number_of_jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    args = vars(parser.parse_args())
    
    #Get arguments
    fim_version = args['fim_version']
    number_of_jobs = args['number_of_jobs']
    
    ####################################################################
    #Define default arguments. Modify these if necessary. 
    today = date.today().strftime('%m%d%Y')
    fim_run_dir = f'/data/previous_fim/{fim_version}/'
    output_flows_dir = f'/data/catfim/{fim_version}/{today}/flows'
    output_mapping_dir = f'/data/catfim/{fim_version}/{today}/mapping'
    nwm_us_search = 10
    nwm_ds_search = 10        
    write_depth_tiff = False
    ####################################################################
    
    ####################################################################
    #Run CatFIM scripts in sequence
    ####################################################################
    #Generate CatFIM flow files.
    print('Creating flow files')
    start = time.time()
    subprocess.call(['python3','generate_categorical_fim_flows.py', 'w' , output_flows_dir, 'u', nwm_us_search, 'd', nwm_ds_search])
    end = time.time()
    elapsed_time = round((end-start)/60,1)
    print(f'Finished creating flow files in {elapsed_time} minutes')
    
    #Generate CatFIM mapping.
    print('Begin mapping')
    start = time.time()
    subprocess.call(['python3','generate_categorical_fim_mapping.py', 'r' , fim_run_dir, 's', output_flows_dir, 'o', output_mapping_dir, 'j', number_of_jobs, 'depthtiff', write_depth_tiff]) 
    end = time.time()
    elapsed_time = round((end-start)/60,1)
    print(f'Finished mapping in {elapsed_time} minutes')
    
    #Updating Mapping Status
    print('Updating mapping status')
    update_mapping_status(output_mapping_dir, output_flows_dir)

   