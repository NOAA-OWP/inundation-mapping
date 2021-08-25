#!/usr/bin/env python3

import os
import subprocess
import argparse
import time
from pathlib import Path
import geopandas as gpd
import pandas as pd


def update_mapping_status(output_mapping_dir, output_flows_dir):
    '''
    Updates the status for nws_lids from the flows subdirectory. Status
    is updated for sites where the inundation.py routine was not able to
    produce inundation for the supplied flow files. It is assumed that if
    an error occured in inundation.py that all flow files for a given site
    experienced the error as they all would have the same nwm segments.

    Parameters
    ----------
    output_mapping_dir : STR
        Path to the output directory of all inundation maps.
    output_flows_dir : STR
        Path to the directory containing all flows.

    Returns
    -------
    None.

    '''
    # Find all LIDs with empty mapping output folders
    subdirs = [str(i) for i in Path(output_mapping_dir).rglob('**/*') if i.is_dir()]
    empty_nws_lids = [Path(directory).name for directory in subdirs if not list(Path(directory).iterdir())]

    # Write list of empty nws_lids to DataFrame, these are sites that failed in inundation.py
    mapping_df = pd.DataFrame({'nws_lid':empty_nws_lids})
    mapping_df['did_it_map'] = 'no'
    mapping_df['map_status'] = ' and all categories failed to map'

    # Import shapefile output from flows creation
    shapefile = Path(output_flows_dir)/'nws_lid_flows_sites.shp'
    flows_df = gpd.read_file(shapefile)

    # Join failed sites to flows df
    flows_df = flows_df.merge(mapping_df, how = 'left', on = 'nws_lid')

    # Switch mapped column to no for failed sites and update status
    flows_df.loc[flows_df['did_it_map'] == 'no', 'mapped'] = 'no'
    flows_df.loc[flows_df['did_it_map']=='no','status'] = flows_df['status'] + flows_df['map_status']

    # Perform pass for HUCs where mapping was skipped due to missing data  #TODO check with Brian
    flows_hucs = [i.stem for i in Path(output_flows_dir).iterdir() if i.is_dir()]
    mapping_hucs = [i.stem for i in Path(output_mapping_dir).iterdir() if i.is_dir()]
    missing_mapping_hucs = list(set(flows_hucs) - set(mapping_hucs))
    
    # Update status for nws_lid in missing hucs and change mapped attribute to 'no'
    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'status'] = flows_df['status'] + ' and all categories failed to map because missing HUC information'
    flows_df.loc[flows_df.eval('HUC8 in @missing_mapping_hucs & mapped == "yes"'), 'mapped'] = 'no'

    # Clean up GeoDataFrame and rename columns for consistency
    flows_df = flows_df.drop(columns = ['did_it_map','map_status'])
    flows_df = flows_df.rename(columns = {'nws_lid':'ahps_lid'})

    # Write out to file
    nws_lid_path = Path(output_mapping_dir) / 'nws_lid_sites.shp'
    flows_df.to_file(nws_lid_path)


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Run Categorical FIM')
    parser.add_argument('-f','--fim_version',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-j','--number_of_jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    args = vars(parser.parse_args())

    # Get arguments
    fim_version = args['fim_version']
    number_of_jobs = args['number_of_jobs']

    # Define default arguments. Modify these if necessary
    fim_run_dir = Path(f'{fim_version}')
    fim_version_folder = os.path.basename(fim_version)
    output_flows_dir = Path(f'/data/catfim/{fim_version_folder}/flows')
    output_mapping_dir = Path(f'/data/catfim/{fim_version_folder}/mapping')
    nwm_us_search = '10'
    nwm_ds_search = '10'
    write_depth_tiff = False

    ## Run CatFIM scripts in sequence
    # Generate CatFIM flow files
    print('Creating flow files')
    start = time.time()
    subprocess.call(['python3','/foss_fim/tools/generate_categorical_fim_flows.py', '-w' , str(output_flows_dir), '-u', nwm_us_search, '-d', nwm_ds_search])
    end = time.time()
    elapsed_time = (end-start)/60
    print(f'Finished creating flow files in {elapsed_time} minutes')

    # Generate CatFIM mapping
    print('Begin mapping')
    start = time.time()
    subprocess.call(['python3','/foss_fim/tools/generate_categorical_fim_mapping.py', '-r' , str(fim_run_dir), '-s', str(output_flows_dir), '-o', str(output_mapping_dir), '-j', str(number_of_jobs)])
    end = time.time()
    elapsed_time = (end-start)/60
    print(f'Finished mapping in {elapsed_time} minutes')

    # Updating mapping status
    print('Updating mapping status')
    update_mapping_status(str(output_mapping_dir), str(output_flows_dir))
