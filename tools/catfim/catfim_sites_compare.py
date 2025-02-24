#!/usr/bin/env python3

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone
import pandas as pd
import re

pd.options.mode.chained_assignment = None  # default='warn'

# import utils.fim_logger as fl
# FLOG = fl.FIM_logger()  # the non mp version

''' 
This tool compares two or more versions of the CatFIM output site CSV.

It will create separate output CSV for flow- and stage-based version comparisons with the following columns:
- site_id
- <version>_site_processed
- <version>_catfim_mapped
- <version>_status

It will auto overwrite output files already existing.

Potential Upgrades TODO: 
- Could add a flag saying to save differences only.
- Could add a flag to save a log file.
- Could add a flag to save a summary of the differences.
- Could implement sorting by version order in the output CSV.
- Could include HUC in output CSV.

'''

# Function that compiles CatFIM sites based on an input path list
def compile_catfim_sites(sorted_path_list):
    '''
    Inputs: 
    - sorted_path_list: a list of string with paths to CatFIM runs (which should already be sorted in to flow-based or stage-based)

    Outputs: 
    - combined_sites_df
    '''

    print(f'Results to compile: {sorted_path_list}')
    
    for path in sorted_path_list:
    
        # Create mapping filepath and check that mapping folder exists
        mapping_path = os.path.join(path, 'mapping')
        if not os.path.exists(mapping_path):
            print(f'WARNING: Filepath does not exist for mapping folder: {mapping_path}')
            continue
    
        # Get the CSV filename and check that it exists
        csv_path = None
        for filename in os.listdir(mapping_path):
            if filename.endswith('sites.csv'):
                csv_path = os.path.join(mapping_path, filename)
    
        if csv_path == None:
            print(f'WARNING: No CSV path found for input path {path}')
            continue
    
        # Read in site CSV
        sites_df = pd.read_csv(csv_path)
    
        # Reconcile site ID column name
        if 'ahps_lid' in sites_df.columns:
            sites_df['site_id'] = sites_df['ahps_lid']
        elif 'nws_lid' in sites_df.columns:
            sites_df['site_id'] = sites_df['nws_lid']
        else:
            print(f'WARNING: Did not find ahps_lid or nws_lid column in {csv_path}')
            continue
        
        # Make a new df with only the needed columns
        sites_df['site_processed'] = 'yes'
        # trimmed_sites_df = sites_df[['site_id', 'HUC8', 'name', 'states', 'mapped', 'status']] # maybe add this additional data later, or have this already in the appended data?
        trimmed_sites_df = sites_df[['site_id', 'site_processed', 'mapped', 'status']] # maybe add this additional data later
    
        # Extract version_id from the path
        match = version_id = re.search(r'(hand|fim)_(\d+_\d+_\d+_\d+)', path)
        if match:
            version_id = match.group(2)
        else:
            print(f'WARNING: Unable to extract version ID from {path}')
            continue
    
        # Rename status 'mapped' and 'status' columns to have the version_id 
        trimmed_sites_df.rename(columns={'mapped': f'{version_id}_catfim_mapped', 'status': f'{version_id}_status', 
                                        'site_processed': f'{version_id}_site_processed'}, inplace=True)
    
        # If combined_sites_df exists already, do an outer join to add this one
        # If it doesn't already exist, set this trimmed_sites_df to be combined_sites_df
        try:
            combined_sites_df
            # If 'combined_sites_df' exists, perform an outer join with 'trimmed_sites_df'
            combined_sites_df = pd.merge(combined_sites_df, trimmed_sites_df, how='outer', on='site_id')
        except NameError:
            # If 'combined_sites_df' doesn't exist, assign 'trimmed_sites_df' to it
            combined_sites_df = trimmed_sites_df
        # End path loop

    # Loop through columns and fill in details for NA columns
    for col in combined_sites_df.columns:
        if 'site_processed' in col:
            # Fill with 'no' where the value is not 'yes'
            combined_sites_df[col] = combined_sites_df[col].apply(lambda x: 'yes' if x == 'yes' else 'no')
            
        elif 'catfim_mapped' in col:
            # Fill with 'no' where the value is not 'yes'
            combined_sites_df[col] = combined_sites_df[col].apply(lambda x: 'yes' if x == 'yes' else 'no')
            
        elif 'status' in col:
            # Get the version ID from the 'status' column
            version_id = col.replace('_status', '')
            
            # Construct the corresponding 'site_processed' column name
            site_processed_col = f'{version_id}_site_processed'
            
            # Check where the 'status' is NaN and 'site_processed' is 'no'
            combined_sites_df[col] = combined_sites_df.apply(
                lambda row: f'Site not processed in {version_id}. See release notes.' 
                if pd.isna(row[col]) and row[site_processed_col] == 'no' 
                else row[col],
                axis=1
            )
    # End column loop
            
    return combined_sites_df

# Main function for comparing CatFIM sites
def main(path_list, output_save_filepath):
    '''
    Inputs
    - path_list:
    - output_save_filepath

    Outputs
    - saves a CSV to the output_save_filepath

    '''

    # Verify that output save path exists
    if not os.path.exists(output_save_filepath):
        sys.exit(f'ERROR: Output save path does not exist: {output_save_filepath}.')
    
    # Start stopwatch
    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    # NOTE: Removed logging file for now, since this is such a simple script. 
    # Can implement later if needed by replacing print with FLOG.lprint
    # # Set up logging system 
    # log_file_name = f"compare_log_file_{overall_start_time.strftime('%Y_%m_%d__%H_%M_%S')}"
    # log_path = os.path.join(output_save_filepath, log_file_name)
    # FLOG.setup(log_path)

    print('================================')
    print(f'Start CatFIM site comparison - (UTC): {dt_string}')
    print('')

    # Separate path list into flow- and stage-based lists
    # Initialize empty lists for stage and flow
    stage_path_list = []
    flow_path_list = []
    
    # Separate space-delimited list into list
    path_list = path_list.split()

    # Loop through the path_list and categorize the paths
    for path in path_list:
        if 'stage' in path:
            stage_path_list.append(path)
        elif 'flow' in path:
            flow_path_list.append(path)
        else:
            print(f'WARNING: Unable to process path that does not contain "stage" or "flow": {path}')
    
    # Run site compilation for stage-based CatFIM
    if len(stage_path_list) != 0:
        print('--------- Compiling stage-based CatFIM sites ---------')
        stage_based_combined_sites_df = compile_catfim_sites(stage_path_list)
        
        # Save stage-based outputs
        out_save_path = os.path.join(output_save_filepath, 'stage_based_site_organizer.csv')
        stage_based_combined_sites_df.to_csv(out_save_path, index=False)
        print(f'Stage-based outputs saved to {out_save_path}')
        print()

    # Run site compilation for flow-based CatFIM
    if len(flow_path_list) != 0:
        print('--------- Compiling flow-based CatFIM sites ---------')
        flow_based_combined_sites_df = compile_catfim_sites(flow_path_list)
        
        # Save flow-based outputs
        out_save_path = os.path.join(output_save_filepath, 'flow_based_site_organizer.csv')
        flow_based_combined_sites_df.to_csv(out_save_path, index=False)
        print(f'Flow-based outputs saved to {out_save_path}')
        print()

    # Wrap up
    overall_end_time = datetime.now(timezone.utc)
    print()
    print('================================')
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    print(f'End sites compare - (UTC): {dt_string}')

    # Calculate duration
    time_duration = overall_end_time - overall_start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")

    return


if __name__ == '__main__':

    '''
    This tool compares multiple versions of the CatFIM output site CSV. 

    It will auto overwrite output files already existing.
    
    Sample usage: 
    python /foss_fim/tools/catfim_sites_compare.py
    -p  '/data/catfim/hand_4_5_11_1_stage_based/ /data/catfim/fim_4_5_2_11_stage_based/ /data/catfim/fim_4_4_0_0_stage_based/ /data/catfim/hand_4_5_11_1_flow_based/ /data/catfim/fim_4_5_2_11_flow_based/ /data/catfim/fim_4_5_2_0_flow_based/'
    -o '/home/emily.deardorff/notebooks/'
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run CatFIM sites comparison.')

    parser.add_argument( # NOTE: Should this be a textfile input?
        '-p', 
        '--path-list',
        help='REQUIRED: Space-delimited list of CatFIM output paths from which to compile sites.',
        required=True,
    )

    parser.add_argument(
        '-o', '--output-save-filepath', 
        help='REQUIRED: Path to where the results files will be saved.', 
        required=True
    )

    args = vars(parser.parse_args())

    try:
        main(**args)

    except Exception:
        print(traceback.format_exc())
