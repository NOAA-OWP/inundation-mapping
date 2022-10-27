#!/usr/bin/env python3

import os
import glob
import argparse
import pandas as pd

def combine_crosswalk_tables(data_directory, output_directory=None):
    """
    Combines all hydrotables from a run into a single crosswalk table with HUC, BranchID, HydroID, feature_id, and LakeID
    """

    if not output_directory:
        output_directory = data_directory

    file_list = sorted(glob.glob(os.path.join(data_directory, '*', 'branches', '*', 'hydroTable_*.csv')))

    dfs = list()
    for filename in file_list:
        file_df = pd.read_csv(filename, usecols=['HUC', 'HydroID', 'feature_id', 'LakeID'], dtype={'HUC':str})
        file_df.drop_duplicates(inplace=True)
        file_df.rename(columns={'HUC':'huc8'}, inplace=True)
        file_df['BranchID'] = os.path.split(os.path.dirname(filename))[1]

        dfs.append(file_df)

    df = pd.concat(dfs)

    df.to_csv(os.path.join(output_directory, 'crosswalk_table.csv'), index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Combines hydrotables from HUC/branch into a single crosswalk table')
    parser.add_argument('-d', '--data-directory', help='Data directory (name of run)', type=str, required=True)
    parser.add_argument('-o', '--output-directory', help='Directory for outputs to be saved', type=str, required=False)

    args = vars(parser.parse_args())

    combine_crosswalk_tables(**args)