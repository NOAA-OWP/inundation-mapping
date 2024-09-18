#!/usr/bin/env python3

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone


import pandas as pd

import utils.fim_logger as fl

# global RLOG
FLOG = fl.FIM_logger()  # the non mp version

"""
This tool can compare two version of the "sites" csv"s (not the poly libray files at this time).
It is intended to compare a previous "sites" csv to a new one to see what changes have appeared 
with sites.

Rules:
- see if one of the two files has extra sites
- And  yes.. for now, it assumes both of those sets of columns exist in both files
- compares the following columns at this time:  Note: Not geometry at this time
   - ahps_id
   - nws_data_name
   - HUC8
   - mapped
   - status

- It will display both sets of columns for each set, then a column with a TRUE/FALSE, if the 
  each column matches.
- You optionallly can add a flag saying differences only.
- It will auto overwrite output files already existing.

"""

def compare_sites(prev_file, new_file, output_file):
    
    # Validation
    if not os.path.exists(prev_file):
        print(f"The -p (prev file) does not exist. {prev_file} ")
        sys.exit(1)

    if not os.path.exists(new_file):
        print(f"The -n (new file) does not exist. {new_file} ")
        sys.exit(1)

    output_folder = os.path.dirname(output_file)
    if not os.path.exists(output_folder):
        print(f"While the output file does not need to exist, the output folder does.")
        sys.exit(1)

    # Setup variables
    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")
    # Column names to compare    
    col_names = ["ahps_lid", "nws_data_name", "HUC8", "mapped", "status"]

    # setup logging system
    log_file_name = f"compare_log_file_{overall_start_time.strftime('%Y_%m_%d__%H_%M_%S')}"
    log_path = os.path.join(output_folder, log_file_name)
    FLOG.setup(log_path)

    FLOG.lprint("================================")
    FLOG.lprint(f"Start sites compare - (UTC): {dt_string}")
    FLOG.lprint("")
    FLOG.lprint("Input arguments:")
    FLOG.lprint(locals())
    FLOG.lprint("")
    
    # Load both files
    print("loading previous file")
    # If not, load the columns, the rename to match
    prev_df = pd.read_csv(prev_file, usecols=col_names)
    if "nws_lid" in prev_df.columns: 
        prev_df.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)   
    # change huc8 to string and order
    prev_df.HUC8.astype(str)
    prev_df.HUC8 = prev_df.HUC8.astype('str').str.zfill(8)
    prev_df = prev_df.sort_values(by=['ahps_lid'])
    prev_df = prev_df.add_prefix("prev_") # rename all columns to add the suffix of "prev_"
    # prev_df.to_csv(output_file)
    
    print("loading new file")
    # This has a bug. in 4.5.2.11 stage the column was named 'nws_lid'
    # but for future versions, it will be named "ahps_lid"
    # 4.5.2.11 stage
    # new_col_names = ["nws_lid", "nws_data_name", "HUC8", "mapped", "status"]
    
    # 4.5.2.11 flow
    new_col_names = ["ahps_lid", "nws_data_name", "HUC8", "mapped", "status"]
    new_df = pd.read_csv(new_file, usecols=new_col_names)
    # to get it in sync with the prev column names
    if "nws_lid" in new_df.columns:
        new_df.rename(columns={'nws_lid': 'ahps_lid'}, inplace=True)
    new_df.HUC8.astype(str)
    new_df.HUC8 = new_df.HUC8.astype('str').str.zfill(8)
    new_df = new_df.sort_values(by=['ahps_lid'])
    new_df = new_df.add_prefix("new_") # rename all columns to add the suffix of "new_"
    
    # print("Prev")
    # print(prev_df) # prev_df.loc[0]
    # print()
    
    # print("new")
    # print(new_df)
    # print()    
    
    compare_data(prev_df, new_df, output_file, col_names )

    # Wrap up
    overall_end_time = datetime.now(timezone.utc)    
    FLOG.lprint("================================")
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")    
    FLOG.lprint(f"End sites compare - (UTC): {dt_string}")

    # calculate duration
    time_duration = overall_end_time - overall_start_time
    FLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")

    return  # helps show the end of the def       
       
       
def compare_data(prev_df, new_df, output_file, col_names):
    
    # find the matching recs based on aphs_id, and gets all of the prev_dfs recs
    results_df = prev_df.merge(new_df, left_on='prev_ahps_lid', right_on='new_ahps_lid', how='outer')
    
    # strip out line breaks from the two status columns
    results_df[f"prev_status"] = results_df[f"prev_status"].replace("\n", "")
    results_df[f"new_status"] = results_df[f"new_status"].replace("\n", "")
    
    for col in col_names:
        # add compare columns and sets defaults
        results_df[f"is_match_{col}"] = results_df[f"prev_{col}"] == results_df[f"new_{col}"]
    

    # for 4.4.0.0, almost all stage status columns have the phrase "usgs_elev_table missing, "
    # remove that from the front of prev_if applicable (the space on the end is critical)
    results_df[f"prev_adj_status"] = results_df[f"prev_status"].str.replace(
                                         "usgs_elev_table missing, ", "")
    results_df[f"is_match_adj_status"] = results_df[f"prev_adj_status"] == results_df[f"new_status"]
    
    # for col in col_names:
    #     results_df[f"has_{col}"] = False
    
    results_df.to_csv(output_file)
    
    return # helps show the end of the def
    

if __name__ == "__main__":

    """
    This tool can compare two version of the catfim sites csv's (not the poly libray files at this time).
    It is intended to compare a previous sites csv to a new one to see what changes have appeared 
    with sites.
    More details at the top of this file
    
    It will auto overwrite output files already existing.
    """
    
    """
    Sample
    python /foss_fim/tools/catfim_sites_compare.py 
    -p /data/catfim/fim_4_4_0_0_stage_based/mapping/stage_based_catfim_sites.csv
    -n /data/catfim/fim_4_5_2_11_stage_based/mapping/stage_based_catfim_sites.csv
    -o /data/catfim/fim_4_5_2_11_stage_based/4_4_0_0__4_5_2_11_sites_comparison.csv
    """
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run CatFIM sites comparison")

    parser.add_argument(
        "-p",
        "--prev-file",
        help="Path to the prev (or any one) of the CatFIM ahps sites csv's",
        required=True,
    )
    
    parser.add_argument(
        "-n",
        "--new-file",
        help="Path to the new (or any one) of the 'sites' csv",
        required=True,
    )

    parser.add_argument(
        "-o",
        "--output-file",
        help="Path to where the results file will be saved",
        required=True,
    )
   
    args = vars(parser.parse_args())

    try:

        # call main program
        compare_sites(**args)

    except Exception:
        FLOG.critical(traceback.format_exc())
