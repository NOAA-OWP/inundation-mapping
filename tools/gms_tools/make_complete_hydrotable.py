#!/usr/bin/env python3

import os
import glob
import argparse
import pandas as pd

def make_complete_hydrotable(data_directory, output_directory=None):
    """
    Compiles all hydrotables from a run into a single hydrotable with HUC, BranchID, HydroID, feature_id, and LakeID
    """

    if not output_directory:
        output_directory = data_directory

    file_list = sorted(glob.glob(os.path.join(data_directory, '*', 'branches', '*', 'hydroTable_*.csv')))

    for n, filename in enumerate(file_list):
        # Get branch ID
        filename_parts = filename.split('/')
        branch_id = filename_parts[6]

        file_df = pd.read_csv(filename, dtype={'HUC':str})
        file_df['BranchID'] = branch_id
        file_df = file_df[['HUC', 'BranchID', 'HydroID', 'feature_id', 'LakeID']]
        file_df.drop_duplicates(inplace=True)

        if n > 0:
            df = pd.concat([df, file_df])
        else:
            df = file_df

    df.to_csv(os.path.join(output_directory, 'hydroTable_complete.csv'), index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Makes complete table from HUC hydrotables')
    parser.add_argument('-d', '--data-directory', help='Data directory (name of run)', required=True)
    parser.add_argument('-o', '--output-directory', help='Directory for outputs to be saved', required=False)

    args = vars(parser.parse_args())

    make_complete_hydrotable(**args)