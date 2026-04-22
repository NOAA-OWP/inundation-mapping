#!/usr/bin/env python3

import argparse
import glob
import os
import pathlib

import pandas as pd


def generate_branch_list_csv(huc_id, output_branch_csv):
    '''
    Processing:
        This create a branch_ids.csv file which is required for various post processing tasks.

        There likely is a better way to do this, but we need to know which branches completed
        successfully. We could look through the branch logs, but I wonder if that would be too loose.
        Iterate through all branches looking for the branch hydrotables fileas
        it won't be there if the branch failed or aborted.

    Params:
        - huc_id
        - output_branch_csv (str): csv file name and path of the list to be created. (likely branch_list.csv)

    Output:
        - create a csv file (assuming the format coming in is a csv
    '''
    # validations
    file_extension = pathlib.Path(output_branch_csv).suffix

    if file_extension != ".csv":
        raise ValueError("The output branch csv file does not have a .csv extension")

    if (len(huc_id) != 8) or (not huc_id.isnumeric()):
        raise ValueError("The huc_id does not appear to be an eight digit number")

    # figure out the huc folder pathing
    huc_folder = os.path.dirname(output_branch_csv)

    pattern = "**/branches/*/hydroTable_*.csv"
    branch_elev_files = glob.glob(os.path.join(huc_folder, pattern), recursive=True)

    branch_dfs_list = []
    for file_name in branch_elev_files:
        branch_id = pathlib.Path(file_name).parent.name
        dir_rec = {'huc_id': huc_id, 'branch_id': branch_id}
        df_branch = pd.DataFrame([dir_rec])
        branch_dfs_list.append(df_branch)

    if len(branch_dfs_list) > 0:
        df_branches = pd.concat(branch_dfs_list, ignore_index=True)
        print(f"There are {len(branch_dfs_list)} successful branches", flush=True)

        # Save the csv even if it is empty
        df_branches.to_csv(output_branch_csv, index=False, header=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create branch list')
    # parser.add_argument('-b', '--branch-id', help='Branch ID', required=True)
    parser.add_argument('-o', '--output-branch-csv', help='Output branch csv list', required=True)
    parser.add_argument('-u', '--huc-id', help='HUC number being aggregated', required=True)
    args = vars(parser.parse_args())

    generate_branch_list_csv(**args)
