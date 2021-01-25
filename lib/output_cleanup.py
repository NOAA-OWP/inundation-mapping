#!/usr/bin/env python3
import os
import csv
import json
import shutil
import argparse

def output_cleanup(huc_number, output_folder_path, additional_whitelist, is_production, viz_post_processing):
    '''
    Processes all the final output files to cleanup and add post-processing

    Parameters
    ----------
    huc_number : STR
        The HUC
    output_folder_path : STR
        Path to the outputs for the specific huc
    additional_whitelist : STR
        Additional list of files to keep during a production run
    is_production : BOOL
        Determine whether or not to only keep whitelisted production files
    is_viz_post_processing : BOOL
        Determine whether or not to process outputs for Viz
    '''

    # List of files that will be saved during a production run
    production_whitelist = [
        'rem_zeroed_masked.tif',
        'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg',
        'demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg',
        'gw_catchments_reaches_filtered_addedAttributes.tif',
        'hydroTable.csv',
        'src.json'
    ]

    # List of files that will be saved during a viz run
    viz_whitelist = [
        'rem_zeroed_masked.tif',
        'gw_catchments_reaches_filtered_addedAttributes.tif',
        'hydroTable.csv',
        'src.json'
    ]

    # If "production" run, only keep whitelisted files
    if is_production and not is_viz_post_processing:
        whitelist_directory(output_folder_path, production_whitelist, additional_whitelist)

    # If Viz post-processing is enabled, form output files to Viz specifications
    if is_viz_post_processing:
        # Step 1, keep only files that Viz needs
        whitelist_directory(output_folder_path, viz_whitelist, additional_whitelist)

        # Step 2, add feature_id to src.json and rename file
        # Open src.json for writing feature_ids to
        src_data = {}
        with open(os.path.join(output_folder_path, 'src.json')) as jsonf:
            src_data = json.load(jsonf)

        with open(os.path.join(output_folder_path, 'hydroTable.csv')) as csvf:
            csvReader = csv.DictReader(csvf)
            for row in csvReader:
                if row['HydroID'].lstrip('0') in src_data and 'nwm_feature_id' not in src_data[row['HydroID'].lstrip('0')]:
                    src_data[row['HydroID'].lstrip('0')]['nwm_feature_id'] = row['feature_id']

        # Write src_data to JSON file
        with open(os.path.join(output_folder_path, f'rating_curves_{huc_number}.json'), 'w') as jsonf:
            json.dump(src_data, jsonf)

        # Step 3, copy files to desired names
        shutil.copy(os.path.join(output_folder_path, 'rem_zeroed_masked.tif'), os.path.join(output_folder_path, f'hand_grid_{huc_number}.tif'))
        shutil.copy(os.path.join(output_folder_path, 'gw_catchments_reaches_filtered_addedAttributes.tif'), os.path.join(output_folder_path, f'catchments_{huc_number}.tif'))

def whitelist_directory(directory_path, whitelist, additional_whitelist):
    # Add any additional files to the whitelist that the user wanted to keep
    if additional_whitelist:
        whitelist = whitelist + [filename for filename in additional_whitelist.split(',')]

    # Delete any non-whitelisted files
    directory = os.fsencode(directory_path)
    for file in os.listdir(directory_path):
        filename = os.fsdecode(file)
        if filename not in whitelist:
            os.remove(os.path.join(directory_path, filename))


if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Cleanup output files')
    parser.add_argument('huc_number', type=str, help='The HUC')
    parser.add_argument('output_folder_path', type=str, help='Path to the outputs for the specific huc')
    parser.add_argument('-w', '--additional_whitelist', type=str, help='List of additional files to keep in a production run')
    parser.add_argument('-p', '--is_production', help='Keep only white-listed files for production runs', action='store_true')
    parser.add_argument('-v', '--is_viz_post_processing', help='Formats output files to be useful for Viz', action='store_true')

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # Rename variable inputs
    huc_number = args['huc_number']
    output_folder_path = args['output_folder_path']
    additional_whitelist = args['additional_whitelist']
    is_production = args['is_production']
    is_viz_post_processing = args['is_viz_post_processing']

    # Run output_cleanup
    output_cleanup(huc_number, output_folder_path, additional_whitelist, is_production, is_viz_post_processing)
