import argparse
import errno
import math
import os
import re

import pandas as pd


def calculate_total_time(df, column_name):
    """
    Calculate the total time in minutes and seconds.

    Args:
        df (pandas.DataFrame): The DataFrame containing the processing time columns.
        column_name (str): The name of columns in the DataFrame.
    """
    total_second_time = (
        df[column_name].apply(lambda x: int(x.split(':')[0]) * 60 + int(x.split(':')[1])).sum()
    )
    minutes_time = total_second_time // 60
    seconds_time = total_second_time % 60
    if seconds_time < 10:
        summary_time = f"{minutes_time}:0{seconds_time}"
    else:
        summary_time = f"{minutes_time}:{seconds_time}"
    # Calculate total time for HUC Duration%
    percent_time = minutes_time + math.floor((seconds_time / 60) * 100) / 100
    return summary_time, percent_time


def duration_system(hydrofabric_dir, output_csv_file):
    """
    Thhis function read processing_time text files for each huc and
    writes the results to an output csv file.

    Args:
        hydrofabric_dir (str): Path to hydrofabric directory.
        output_csv_file (str): The path to the output csv file.
    """

    dir_path = hydrofabric_dir
    # Check that hydrofabric_dir exists
    if not os.path.exists(dir_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dir_path)

    # Get the list of all hucs in the directory
    entries = [d for d in os.listdir(dir_path) if re.match(r'^\d{8}$', d)]
    hucs = []
    for entry in entries:
        # create the full path of the entry
        full_path = os.path.join(dir_path, entry)
        # check if the netry is a directory
        if os.path.isdir(full_path):
            hucs.append(entry)

    all_rows = []
    for huc in hucs:
        txt_file = f'processing_time_{huc}.txt'
        txt_path = os.path.join(dir_path, huc, txt_file)
        # Check if the text file exist
        if os.path.exists(txt_path):
            # Read the txt file
            with open(txt_path, 'r') as file:
                txt_content = file.readline().strip().split(',')
                all_rows.append(txt_content)
            # Remove all text files
            os.remove(txt_path)
        else:
            print(f"Warning: Missing {txt_file} for HUC {huc}")

    column_names = [
        "HUC8",
        "HUC Duration",
        "HUC Duration%",
        "Branches",
        "Branch0 Duration",
        "Branch0 Duration%",
        "Branches Duration",
        "Branches Duration%",
    ]
    df = pd.DataFrame(all_rows, columns=column_names)
    num_hucs = len(df['HUC8'])
    num_branches = df['Branches'].astype(int).sum()

    summary_time1, percent_time1 = calculate_total_time(df, 'HUC Duration')
    summary_time2, percent_time2 = calculate_total_time(df, 'Branch0 Duration')
    summary_time3, percent_time3 = calculate_total_time(df, 'Branches Duration')

    Summary_row = pd.DataFrame(
        [
            [
                num_hucs,
                summary_time1,
                percent_time1,
                num_branches,
                summary_time2,
                percent_time2,
                summary_time3,
                percent_time3,
            ]
        ],
        columns=column_names,
    )
    final_df = pd.concat([df, Summary_row], ignore_index=True)

    final_df.to_csv(output_csv_file, index=False)


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Concatenate processing_time text files and save them as a CSV."
    )
    parser.add_argument(
        "-fim",
        "--hydrofabric_dir",
        help="Directory path to FIM hydrofabric by processing unit.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o", "--output_csv_file", help="Path to the output csv file.", required=True, type=str
    )
    # Extract to dictionary and run
    duration_system(**vars(parser.parse_args()))
