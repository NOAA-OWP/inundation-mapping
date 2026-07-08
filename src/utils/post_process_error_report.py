#!/usr/bin/env python3

import argparse
import re
import sys
import traceback
from pathlib import Path

from src.utils.shared_functions import search_concat_huc_csvs

import pandas as pd

# Most if not all HUCs will have an error log in the file name pattern of huc_{huc num}_errors.csv

def merge_huc_error_reports(hand_dir, output_csv_path):
    """
    Scans a directory and subdirectories for CSV files matching a pattern.
    
    Args:
        directory_path (str): The root HAND folder to start scanning.
        output_csv_path (str): file path of the mergede csv file to be saved.
    """

    pattern = "huc_*_error_report.csv"
    num_recs_merged = search_concat_huc_csvs(hand_dir, pattern, output_csv_path, is_recursive=True)

    print(f"-- {num_recs_merged} found and merged")

    if num_recs_merged == 0:
        print("++++++++++++++++++++++++++++++++++++++")
        print("ERROR ?? : if no records were found to merge, check the huc logs files as an critical error"
              " in each of the huc error scan tools have failed.")
        print("++++++++++++++++++++++++++++++++++++++")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Look for all errors csv files in HUC folders')
    parser.add_argument('-n', '--hand-dir', help='REQUIRED: folder path where the HUC folders exists.', required=True)
    parser.add_argument('-o', '--output-csv-path', help='REQUIRED: full path of the csv report to be saved', required=True)
    args = vars(parser.parse_args())
    merge_huc_error_reports(**args)    

       

