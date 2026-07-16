#!/usr/bin/env python3
import argparse
import os

import numpy as np
import pandas as pd
from src.utils.shared_functions import search_concat_huc_csvs


# Most if not all HUCs will have an error log in the file name pattern of huc_{huc num}_errors.csv
def merge_huc_error_reports(hand_dir, output_csv_path, branch_accepted_exit_recs_csv_path):
    """
    Scans a directory and subdirectories for CSV files matching a pattern.

    The list of csv df's found in using the file pattern.

    We will load all into a df, THEN seperate which ones are branches and acceptable
    codes and put them into a new csv. This might leave no hucs remaining, which is fine
    and we will just save the empty file with the headers already defined.

    Args:
        directory_path (str): The root HAND folder to start scanning.
        output_csv_path (str): file path of the mergede csv file to be saved.
        branch_accepted_exit_recs_csv_path (str) : file path for the accepted branch recs
    """

    # =========================
    # Validation
    # output_csv_path first
    if output_csv_path == "":
        raise Exception("The error output csv file (-o) can not be empty")
    
    ___, extention = os.path.splitext(output_csv_path)
    if extention != ".csv":
        raise Exception("The error output file name and path (-o) value of"
                        f" {output_csv_path}  does not end in .csv")
 
    if os.path.dirname(output_csv_path) == "":
        raise Exception(
            f"The directory path of (-o) {output_csv_path} does not have a dir path and"
            "  is just a file name. Please add full path of where you want to save the error output file."
        )

    # branch_accepted_exit_recs_csv_path
    if branch_accepted_exit_recs_csv_path == "":
        raise Exception("The output csv file (-b) for the list of accepted branch recs"
                        " can not be empty")
    
    ___, extention = os.path.splitext(branch_accepted_exit_recs_csv_path)
    if extention != ".csv":
        raise Exception("The output file name and path, for accepted branch recs, (-b) has a"
                        f"value of {branch_accepted_exit_recs_csv_path} but it does not end in .csv")

    if os.path.dirname(branch_accepted_exit_recs_csv_path) == "":
        raise Exception(
            f"The directory path of  has no pathing and is just a file name."
            " Please add full path of where you want to save the output file."
            
            f"The directory path for accepted branch recs file, {branch_accepted_exit_recs_csv_path}"
            " (-b) does not have a dir path and is just a file name."
            " Please add full path of where you want to save the output file."
        )
    
    # =========================

    pattern = "huc_*_error_report.csv"
    csv_df_list = search_concat_huc_csvs(hand_dir, pattern, output_csv_path, is_recursive=True)

    num_recs_merged = len(csv_df_list)

    # To keep the df shape matching, lets load all of the recs first into a df
    # then split it to two csv output files.
    # One for branches that have acceptable branch exit codes like 60 - 65.
    # The rest to the error file.
    combined_df = None
    if num_recs_merged > 0:

        print(f"-- {num_recs_merged} error files found and merged.")

        # ignore_index=True re-indexes the rows from 0 to N
        combined_df = pd.concat(csv_df_list, ignore_index=True)
        if "huc_num" in combined_df:
            combined_df = combined_df.sort_values(by="huc_num")

        # combined_df.to_csv(output_csv_path, index=False)

    else:
        print("++++++++++++++++++++++++++++++++++++++")
        print("No records were found to merge, check the huc logs files to ensure all is well.")
        print("++++++++++++++++++++++++++++++++++++++")
        return

    # The branch_id column could be nan, empty or a number.
    # Some recs will be an empty branch as they might have been huc level.
    # Change the column to -9999 so we can filter it out later.

    # Use pd.to_numeric with errors='coerce' to turn empty strings into NaNs
    # then cast to the nullable integer type 'Int64'
    combined_df['branch_id'] = pd.to_numeric(combined_df['branch_id'], errors='coerce').astype('Int64')
    combined_df["branch_id"] = combined_df['branch_id'].fillna(-9999)
    combined_df['branch_is_acceptable_code'] = str(combined_df['branch_id'].between(60, 69)).astype(str)

    # now split the df to two sets.
    acceptable_branches = combined_df[combined_df['branch_is_acceptable_code'] == "True"]
    error_df = combined_df[combined_df['branch_is_acceptable_code'] != "True"]
    
    # drop the test column
    acceptable_branches = acceptable_branches.drop(columns=['branch_is_acceptable_code'])
    error_df = error_df.drop(columns=['branch_is_acceptable_code'])

    # Now we can save the two files, even if they are empty
    error_df.to_csv(output_csv_path, index=False)
    acceptable_branches.to_csv(branch_accepted_exit_recs_csv_path, index=False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Look for all errors csv files in HUC folders')
    parser.add_argument(
        '-n', '--hand-dir', help='REQUIRED: folder path where the HUC folders exists.', required=True
    )
    parser.add_argument(
        '-o', '--error-output-csv-path', help='REQUIRED: full path of the csv report to be saved', required=True
    )

    parser.add_argument(
        '-b', '--branch-accepted-exit-recs-csv-path',
          help='REQUIRED: All branches with exit codes of 60 to 69 will be split'
          ' to a seperate file. Please provide the full path of that csv report.', required=True
    )
    
    args = vars(parser.parse_args())
    merge_huc_error_reports(**args)
