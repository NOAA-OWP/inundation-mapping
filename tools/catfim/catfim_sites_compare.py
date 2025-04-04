#!/usr/bin/env python3

import argparse
import os
import re
import sys
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd


pd.options.mode.chained_assignment = None  # default='warn'

# import utils.fim_logger as fl
# FLOG = fl.FIM_logger()  # the non mp version

'''
This tool compares two or more versions of the CatFIM output site CSV. (Rewritten as of 3/8/25.)
It produces a compiled CSV of all sites and a CSV for each version comparison.
It will auto overwrite output files already existing.

Inputs:
- space-delimited list of CatFIM output paths to compare (-p)
- output save path (-o)
- optional flag to keep only sites with status changes in the comparison tables (-k)

- Example usage:
    python /foss_fim/tools/catfim_sites_compare.py
    -p  '/data/catfim/hand_4_5_11_1_stage_based/ /data/catfim/fim_4_5_2_11_stage_based/ /data/catfim/fim_4_4_0_0_stage_based/ /data/catfim/hand_4_5_11_1_flow_based/ /data/catfim/fim_4_5_2_11_flow_based/ /data/catfim/fim_4_5_2_0_flow_based/'
    -o '/home/emily.deardorff/notebooks/'
    -k

Outputs:

- Number of outputs depends on how many CatFIM results are provided.
- For example, if 3 versions of flow-based CatFIM are provided, the following outputs will be created:
    - flow_based_compare_all_versions.csv
    - flow_based_<version_1>_vs_<version_2>.csv
    - flow_based_<version_2>_vs_<version_3>.csv

- If both flow- and stage-based CatFIM are provided, a separate "compare_all_versions" CSV will be created for each product.

- Output CSVs:
    - <product_id>_compare_all_versions.csv
        - site_id
        - nws_data_wfo
        - nws_data_rfc
        - HUC8
        - name
        - states
        - <version_1>_site_processed
        - <version_1>_catfim_mapped
        - <version_1>_status
        - <version_2>_site_processed
        - <version_2>_catfim_mapped
        - <version_2>_status
        - <version_3>_site_processed
        - <version_3>_catfim_mapped
        - <version_3>_status

    - <product_id>_<version_1>_vs_<version_2>.csv
        - site_id
        - Change
        - Change Description
        - <version_1>_status
        - <version_2>_status
        - nws_data_wfo
        - nws_data_rfc
        - HUC8
        - name
        - states

    Note: product_id refers to either 'flow_based' or 'stage_based'

Change Descriptions:
- No Change (Has mapped CatFIM in both versions)
- No Change (Doesn’t have mapped CatFIM in either version)
- No Change (Site is excluded in both versions)
- Added (Site where CatFIM was not processed previously but now has mapped CatFIM)
- Added (Site where  CatFIM was not mapped previously but now has mapped CatFIM)
- Removed (Previously had mapped CatFIM, now site isn’t being processed)
- Removed (Previously had mapped CatFIM, now site isn’t being mapped)
- Status Change (Previously unmapped, now excluded from  processing)

Potential Upgrades TODO:
- Could add a flag to save a log file.
- Could add a flag to save a summary of the differences.
- Could implement sorting by version order in the output CSV.

'''


# Function that compiles CatFIM sites based on an input path list
def compile_catfim_sites(sorted_path_list):
    '''
    Inputs:
    - sorted_path_list: a list of string with paths to CatFIM runs (which should already be sorted in to flow-based or stage-based)

    Outputs:
    - combined_sites_df
    - combined_sites_metadata_df
    - version_id_list

    '''

    print(f'Results to compile: {sorted_path_list}')

    # sys.exit()

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

        if csv_path is None:
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

        # Make a sites dataframes with only the needed columns
        sites_df['site_processed'] = 'yes'

        # Status dataframe
        trimmed_sites_df = sites_df[['site_id', 'site_processed', 'mapped', 'status']]

        # Metadata dataframe
        trimmed_site_metadata_df = sites_df[
            ['site_id', 'nws_data_wfo', 'nws_data_rfc', 'HUC8', 'name', 'states']
        ]

        # Pad 7-digit HUCs with a leading zero
        def add_leading_zero(num):
            num_str = str(num)
            if len(num_str) == 7:
                return '0' + num_str
            return num_str

        trimmed_site_metadata_df['HUC8'] = trimmed_site_metadata_df['HUC8'].apply(add_leading_zero)

        # Extract version_id from the path
        match = version_id = re.search(r'(hand|fim)_(\d+_\d+_\d+_\d+)', path)
        if match:
            version_id = match.group(2)
        else:
            print(f'WARNING: Unable to extract version ID from {path}')
            continue

        # Rename status 'mapped' and 'status' columns to have the version_id
        trimmed_sites_df.rename(
            columns={
                'mapped': f'{version_id}_catfim_mapped',
                'status': f'{version_id}_status',
                'site_processed': f'{version_id}_site_processed',
            },
            inplace=True,
        )

        # Add site status df to output status df
        try:
            combined_sites_df
            # If 'combined_sites_df' exists, perform an outer join with 'trimmed_sites_df'
            combined_sites_df = pd.merge(combined_sites_df, trimmed_sites_df, how='outer', on='site_id')
        except NameError:
            # If 'combined_sites_df' doesn't exist, assign 'trimmed_sites_df' to it
            combined_sites_df = trimmed_sites_df

        # Add site metadata df to output metadata df
        try:
            # Check if combined_sites_metadata_df already exists
            combined_sites_metadata_df

            # If it does, filter rows that are not already in combined_sites_metadata_df based on 'site_id'
            new_metadata_rows = trimmed_site_metadata_df[
                ~trimmed_site_metadata_df['site_id'].isin(combined_sites_metadata_df['site_id'])
            ]

            # If the metadata table exists, add information ONLY from any sites that weren't included
            combined_sites_metadata_df = pd.concat(
                [combined_sites_metadata_df, new_metadata_rows], ignore_index=True
            )

        except NameError:
            # If 'combined_sites_metadata_df' doesn't exist, assign 'trimmed_site_metadata_df' to it
            combined_sites_metadata_df = trimmed_site_metadata_df

        # End path loop

    version_id_list = []

    # Loop through columns and fill in details for NA columns
    for col in combined_sites_df.columns:
        if 'site_processed' in col:
            # Fill with 'no' where the value is not 'yes'
            combined_sites_df[col] = combined_sites_df[col].apply(lambda x: 'yes' if x == 'yes' else 'no')

        # elif 'catfim_mapped' in col:  # Removed for now
        #     # Fill with 'no' where the value is not 'yes'
        #     combined_sites_df[col] = combined_sites_df[col].apply(lambda x: 'yes' if x == 'yes' else 'no')

        elif 'status' in col:
            # Get the version ID from the 'status' column
            version_id = col.replace('_status', '')
            version_id_list.append(version_id)

            # Construct the corresponding 'site_processed' column name
            site_processed_col = f'{version_id}_site_processed'

            # Check where the 'status' is NaN and 'site_processed' is 'no'
            combined_sites_df[col] = combined_sites_df.apply(
                lambda row: (
                    f'Site not processed in {version_id}. See release notes.'
                    if pd.isna(row[col]) and row[site_processed_col] == 'no'
                    else row[col]
                ),
                axis=1,
            )
    # End column loop

    # Join the site metadata to the combined_sites_df
    combined_sites_df = pd.merge(combined_sites_metadata_df, combined_sites_df, how='right', on='site_id')

    return combined_sites_df, combined_sites_metadata_df, version_id_list


# Create version comparison dataframe
def make_version_comparison_tables(
    combined_sites_df,
    combined_sites_metadata_df,
    product_id,
    version_id_list,
    out_save_path,
    keep_differences_only,
):

    # Put versions in order
    def version_key(version):
        return list(map(int, version.split('_')))

    sorted_versions = sorted(version_id_list, key=version_key)

    # Iterate through versions (minus the last one) to calculate the Change and Change_Description columns
    for i in range(len(sorted_versions) - 1):

        old_version_id = sorted_versions[i]
        new_version_id = sorted_versions[i + 1]

        comparison_id = f'{product_id}_{old_version_id}_vs_{new_version_id}'
        comparison_table_save_path = os.path.join(out_save_path, f'{comparison_id}.csv')

        # Define column names as variables
        old_site_processed_col = f'{old_version_id}_site_processed'
        new_site_processed_col = f'{new_version_id}_site_processed'

        old_catfim_mapped_col = f'{old_version_id}_catfim_mapped'
        new_catfim_mapped_col = f'{new_version_id}_catfim_mapped'

        old_catfim_status_col = f'{old_version_id}_status'
        new_catfim_status_col = f'{new_version_id}_status'

        # Create a subset table with just the versions to compare
        compare_sites_df = combined_sites_df[
            [
                'site_id',
                old_site_processed_col,
                old_catfim_mapped_col,
                old_catfim_status_col,
                new_site_processed_col,
                new_catfim_mapped_col,
                new_catfim_status_col,
            ]
        ]

        # Initialize new columns with default values
        change_col = 'Change'
        change_description_col = 'Change_Description'
        compare_sites_df[change_col] = 'ERROR'
        compare_sites_df[change_description_col] = (
            'ERROR - Site was unable to be categorized, check status columns manually.'
        )

        # Define conditions
        conditions = [
            (compare_sites_df[old_catfim_mapped_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'no'),
            (compare_sites_df[old_catfim_mapped_col] == 'yes')
            & (compare_sites_df[new_catfim_mapped_col] == 'yes'),
            (compare_sites_df[old_site_processed_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'yes'),
            (compare_sites_df[old_catfim_mapped_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'yes'),
            (compare_sites_df[old_catfim_mapped_col] == 'yes')
            & (compare_sites_df[new_site_processed_col] == 'no'),
            (compare_sites_df[old_catfim_mapped_col] == 'yes')
            & (compare_sites_df[new_catfim_mapped_col] == 'no'),
            (compare_sites_df[old_catfim_mapped_col] == 'no')
            & (compare_sites_df[new_site_processed_col] == 'no'),
            (compare_sites_df[old_site_processed_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'no'),
            (compare_sites_df[old_site_processed_col] == 'no')
            & (compare_sites_df[new_site_processed_col] == 'no'),
        ]

        # Define corresponding choices
        choices_change = [
            'No Change',
            'No Change',
            'Added',
            'Added',
            'Removed',
            'Removed',
            'Status Change',
            'Status Change',
            'No Change',
        ]

        choices_change_description = [
            'No Change (Does not have mapped CatFIM in either version)',
            'No Change (Has mapped CatFIM in both versions)',
            'Added (Site where CatFIM was not processed previously but now has mapped CatFIM)',
            'Added (Site where CatFIM was not mapped previously but now has mapped CatFIM)',
            'Removed (Previously had mapped CatFIM, now site is not being processed)',
            'Removed (Previously had mapped CatFIM, now site is not being mapped)',
            'Status Change (Previously was unmapped, now excluded from processing)',
            'Status Change (Previously excluded, now included but unmapped)',
            'No Change (Site is excluded in both versions)',
        ]

        # Apply conditions
        compare_sites_df[change_col] = pd.Series(
            pd.Categorical(np.select(conditions, choices_change, default='ERROR'))
        )
        compare_sites_df[change_description_col] = pd.Series(
            pd.Categorical(
                np.select(
                    conditions,
                    choices_change_description,
                    default='ERROR - Site was unable to be categorized, check status columns manually.',
                )
            )
        )

        # Reorder columns and exclude the unnecessary ones
        compare_sites_df = compare_sites_df[
            ['site_id', change_col, change_description_col, old_catfim_status_col, new_catfim_status_col]
        ]

        # Join the site metadata to the compare_sites_df
        compare_sites_df = pd.merge(compare_sites_df, combined_sites_metadata_df, how='left', on='site_id')

        if keep_differences_only == True:
            # Remove rows where the value in 'change_col' is 'No Change'
            compare_sites_df = compare_sites_df[compare_sites_df[change_col] != 'No Change']

        # Save outputs
        compare_sites_df.to_csv(comparison_table_save_path, index=False)

        print()
        print(f'Saved comparison table to {comparison_table_save_path}')


# Main function for catfim_site_tracking
def main(path_list, output_save_filepath, keep_differences_only):
    '''
    Inputs
    - path_list (space-delimited list)
    - output_save_filepath (string)
    - keep_differences_only (true or false)

    Outputs
    - saves CSVs to the output_save_filepath

    '''

    # Verify that output save path exists
    # if not os.path.exists(output_save_filepath):
    #     sys.exit(f'ERROR: Output save path does not exist: {output_save_filepath}.')
    if not os.path.exists(output_save_filepath):
        os.makedirs(output_save_filepath, exist_ok=True)

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
    print()

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

    if keep_differences_only == True:
        print('-k flag used, keeping only sites with status changes in the comparison tables')

    # Run site compilation for stage-based CatFIM
    if len(stage_path_list) != 0:
        print()
        print('--------- Compiling stage-based CatFIM sites ---------')
        product_id = 'stage_based'

        stage_based_combined_sites_df, combined_sites_metadata_df, version_id_list = compile_catfim_sites(
            stage_path_list
        )

        # Make and save version comparison tables
        make_version_comparison_tables(
            stage_based_combined_sites_df,
            combined_sites_metadata_df,
            product_id,
            version_id_list,
            output_save_filepath,
            keep_differences_only,
        )

        # Save stage-based outputs
        out_save_path = os.path.join(output_save_filepath, f'{product_id}_compare_all_versions.csv')
        stage_based_combined_sites_df.to_csv(out_save_path, index=False)
        print()
        print(f'Combined stage-based outputs saved to {out_save_path}')

    # Run site compilation for flow-based CatFIM
    if len(flow_path_list) != 0:
        print()
        print('--------- Compiling flow-based CatFIM sites ---------')
        product_id = 'flow_based'

        flow_based_combined_sites_df, combined_sites_metadata_df, version_id_list = compile_catfim_sites(
            flow_path_list
        )

        # Make and save version comparison tables
        make_version_comparison_tables(
            flow_based_combined_sites_df,
            combined_sites_metadata_df,
            product_id,
            version_id_list,
            output_save_filepath,
            keep_differences_only,
        )

        # Save flow-based outputs
        out_save_path = os.path.join(output_save_filepath, f'{product_id}_compare_all_versions.csv')
        flow_based_combined_sites_df.to_csv(out_save_path, index=False)
        print()
        print(f'Combined flow-based outputs saved to {out_save_path}')

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
    python /foss_fim/tools/catfim/catfim_sites_compare.py
    -p  '/data/catfim/hand_4_5_11_1_stage_based/ /data/catfim/fim_4_5_2_11_stage_based/ /data/catfim/fim_4_4_0_0_stage_based/ /data/catfim/hand_4_5_11_1_flow_based/ /data/catfim/fim_4_5_2_11_flow_based/ /data/catfim/fim_4_5_2_0_flow_based/'
    -o '/home/emily.deardorff/notebooks/'
    -k
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run CatFIM sites comparison.')

    parser.add_argument(  # NOTE: Should this be a textfile input?
        '-p',
        '--path-list',
        help='REQUIRED: Space-delimited list of CatFIM output paths from which to compile sites.',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output-save-filepath',
        help='REQUIRED: Path to where the results files will be saved.',
        required=True,
    )

    parser.add_argument(
        '-k',
        '--keep-differences-only',
        help='OPTIONAL: Option to keep only changed sites in the comparison files.',
        required=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    try:
        main(**args)

    except Exception:
        print(traceback.format_exc())
