#!/usr/bin/env python3

import argparse
import os
import pathlib

import pandas as pd


def generate_branch_list_csv(huc_id, branch_id, output_branch_csv):
    '''
    Processing:
        This create a branch_ids.csv file which is required for various post processing tasks.
        If the csv already, then the new huc, branch id wil be appended.
        If it does not yet exist, a new csv will be created

    Params:
        - huc_id
        - branch_id
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

    if not branch_id.isnumeric():
        raise ValueError("The branch_id does not appear to be a valid number")

    df_csv = None
    new_data = [[huc_id, branch_id]]
    col_names = ["huc_id", "branch_id"]
    df_csv = pd.DataFrame(new_data, columns=col_names)

    if not os.path.exists(output_branch_csv):
        df_csv.to_csv(output_branch_csv, index=False, header=False)
    else:
        df_csv.to_csv(output_branch_csv, mode='a', index=False, header=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create branch list')
    parser.add_argument('-b', '--branch-id', help='Branch ID', required=True)
    parser.add_argument('-o', '--output-branch-csv', help='Output branch csv list', required=True)
    parser.add_argument('-u', '--huc-id', help='HUC number being aggregated', required=True)
    args = vars(parser.parse_args())

    generate_branch_list_csv(**args)
