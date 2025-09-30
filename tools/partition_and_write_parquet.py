#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm


def load_dataframe_by_variable_huc_level(input_file: Path, huc: str, column_name: str):

    all_data = pd.read_parquet(input_file)
    valid_values = all_data.dropna(subset=['huc12'])

    if column_name == "huc8":
        prefix = huc[:8]
        return_df = valid_values[valid_values['huc12'].str.startswith(prefix)]

    elif column_name == "huc10":
        prefix = huc[:10]
        return_df = valid_values[valid_values['huc12'].str.startswith(prefix)]

    elif column_name == "huc12":
        return_df = pd.read_parquet(input_file, filters=[(column_name, "=", huc)])

    else:
        print(f"\n\t The passed argument: '{column_name}' is not a valid huc identifier.")
        print("\t Please only use huc8 or huc10 as arguments. \n")
        exit(1)

    return return_df


def create_dataframe_by_variable_huc_level(df: pd.DataFrame, huc: str, column_name: str):
    '''
    Filter existing dataframe to only contain specified huc values.

    Inputs:
        df: Pandas Dataframe
        huc: huc to filter using str.startswith on the exisint huc12 colum.
        column_name: Used to identify how many characters to slice off of huc12 values.
    Outputs:
        New filtered dataframe by specified huc.
    '''

    valid_values = df.dropna(subset=['huc12'])

    print(f"\n Amount of rows in input dataframe:  {len(df)}")
    print(f" Amount of huc12 rows dropped with NA values: {len(df) - len(valid_values)} \n")

    # Filter by huc8 if we huc's value (-v) was passed (writing known huc value data)
    if column_name == "huc8":
        prefix = huc[:8]
        df = valid_values[valid_values['huc12'].str.startswith(prefix)]
    # Filter by huc10 if we huc's value (-v) was passed (writing known huc value data)
    elif column_name == "huc10":
        prefix = huc[:10]
        df = valid_values[valid_values['huc12'].str.startswith(prefix)]
    else:
        print(f"\n\t The passed argument: '{column_name}' is not a valid huc identifier.")
        print("\t Please only use huc8 or huc10 as arguments. \n")
        exit(1)

    return df


def create_new_huc_column(df: pd.DataFrame, column_name: str):
    '''
    Either create a new column (huc8 or huc10)

    Inputs:
        df: Pandas Dataframe
        column_name: Used to identify how many characters to slice off of huc12 values.
    Outputs:
        New dataframe with added huc8 or huc10 column added.
    '''

    valid_values = df.dropna(subset=['huc12'])

    print(f"\n Amount of rows in input dataframe:  {len(df)}")
    print(f" Amount of huc12 rows dropped with NA values: {len(df) - len(valid_values)} \n")

    # Create a huc8 column to get unique values (writing all data)
    if column_name == "huc8":
        df = valid_values.copy()
        df.loc[:, 'huc8'] = df['huc12'].str[:8]
    # Create a huc10 column to get unique values (writing all data)
    elif column_name == "huc10":
        df = valid_values.copy()
        df.loc[:, 'huc10'] = df['huc12'].str[:10]
    else:
        print(f"\n\t The passed argument: '{column_name}' is not a valid huc identifier.")
        print("\t Please only use huc8 or huc10 as arguments. \n")
        exit(1)

    return df


def partition_and_write_parquet(
    input_file: Path, output_dir: Path, column_name: str, overwrite: bool = False, hucs: list = None
) -> Path:
    """
    Write individual parquet files based on huc values in a column name (huc12).
    Check if a specific huc or hucs exists in a Parquet file column and extract matching rows.

    If no "hucs" argument provided, write all the data within "input_file", splitting by unique values
    in the "column_name".

    Inputs:
        input_file (str): Path to input Parquet file
        ouput_dir (str): Path to output Parquet file
        column_name (str): Name of column to check
        hucs (str / optional): Value to search for

    Outputs:
        New parquet file with all matching rows in the <output_dir>/<hucs> directory.
    """

    # Create output_dir if it doesn't exist
    if os.path.isdir(output_dir) is False:
        os.mkdir(output_dir)

    try:

        # Filter for one "huc" or value from specified column if hucs argument was passed.
        if hucs is not None and len(hucs) == 1:

            matching_data = load_dataframe_by_variable_huc_level(input_file, hucs[0], column_name)

            huc_output_dir = os.path.join(output_dir, hucs[0])
            output_file = os.path.join(huc_output_dir, "ripple1d_rating_curve.parquet")

            if not matching_data.empty:
                if os.path.isdir(huc_output_dir) is False:
                    os.mkdir(huc_output_dir)
                    print(f"Created directory: {huc_output_dir}, .parquet files will be written there.")
                elif os.path.isdir(huc_output_dir) is True and overwrite is False:
                    print(
                        f"\n\t Output Directory: {huc_output_dir} exists, pass the -o flag to overwrite existing files. \n"
                    )
                    exit(1)

                # Write to new Parquet file
                print(f" Writing all data where {column_name} = {hucs[0]} to {output_file}")
                matching_data.to_parquet(output_file, index=False)
                print(f"Wrote {len(matching_data)} rows to {output_file}")
            else:
                print(f"HUC: {hucs[0]} was not found in the '{column_name}' column")
                print("No file was written.")

        # If more than one huc is passed to create a couple of ripple1d_rating_curve.parquet files, not all rating curve data.
        elif hucs is not None and len(hucs) > 1:

            all_data = pd.read_parquet(input_file)

            for single_huc in hucs:
                huc_output_dir = os.path.join(output_dir, single_huc)
                output_file = os.path.join(huc_output_dir, "ripple1d_rating_curve.parquet")

                if column_name != "huc12":
                    filtered_df = create_dataframe_by_variable_huc_level(all_data, single_huc, column_name)
                else:
                    filtered_df = all_data[all_data[column_name] == single_huc]

                if not filtered_df.empty:
                    if os.path.isdir(huc_output_dir) is False:
                        os.mkdir(huc_output_dir)
                        print(f"Created directory: {output_dir}, .parquet files will be written there.")
                    elif os.path.isdir(huc_output_dir) is True and overwrite is False:
                        print(
                            f"\n\t Output Directory: {huc_output_dir} exists, pass the -o flag to overwrite existing files. \n"
                        )
                        exit(1)

                    # Write to new Parquet file
                    print(f" Writing all data where {column_name} = {single_huc} to {output_file}")
                    filtered_df.to_parquet(output_file, index=False)
                    print(f"Wrote {len(filtered_df)} rows to {output_file}")

                else:
                    print(f"HUC: {single_huc} was not found in the column {column_name}")
                    print("No file was written.")

        # Otherwise, write a file for each unique item (huc) at the specified huc level (huc8, huc10, or huc12)
        else:
            # Read the entire Parquet file
            all_data_df = pd.read_parquet(input_file)

            if column_name != "huc12":
                # Create a huc8 or huc10 column by passing None as the huc argument to create_new_huc_column
                final_df = create_new_huc_column(all_data_df, column_name)
            else:
                final_df = all_data_df.dropna(subset=['huc12'])

            # Set variables to track completion
            total_written_rows = 0
            total_huc_directories = 0

            for unique_value in tqdm(final_df[column_name].unique()):

                # Filter the DataFrame for the unique value
                subset = final_df[final_df[column_name] == unique_value]
                huc_output_dir = os.path.join(output_dir, unique_value)
                output_file = os.path.join(huc_output_dir, "ripple1d_rating_curve.parquet")

                if not subset.empty:
                    if os.path.isdir(huc_output_dir) is False:
                        os.mkdir(huc_output_dir)
                        # print(f"Created directory: {output_dir} - .parquet files will be written there.")

                    elif os.path.isdir(huc_output_dir) is True and overwrite is False:
                        print(
                            f"\n\t Output Directory: {huc_output_dir} exists, pass the -o flag to overwrite existing files. \n"
                        )
                        exit(1)

                    # Write the subset to a new Parquet file
                    subset.to_parquet(output_file, index=False)
                    # print(f"Wrote {len(subset)} rows to {output_file}")
                    # Increment completion variables
                    total_written_rows += len(subset)
                    total_huc_directories += 1

                else:
                    print(f"No files for {unique_value} were written.")

            print(f"\n\t{total_written_rows} rows out of {len(final_df)} written.")
            print(
                f"\n\t{total_huc_directories} huc directories out of {len(final_df[column_name].unique())} written. \n"
            )

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        exit(1)


if __name__ == '__main__':
    '''
    To get/update one huc's Rating Curve .parquet file:
    python3 partition_and_write_parquet.py -i /data/inputs/rating_curve/ripple1d/merged_rc_points.parquet \
        -d /data/inputs/rating_curve/ripple1d/huc12_test \
        -c huc12 \
        -v 120200020409

    To get/update a couple of huc's Rating Curve .parquet file:
    python3 partition_and_write_parquet.py -i /data/inputs/rating_curve/ripple1d/merged_rc_points.parquet \
        -d /data/inputs/rating_curve/ripple1d/huc12_test \
        -c "huc10" \
        -v 1202000204 1806001007

    To generate all new Rating Curve data for each huc level:
    python3 partition_and_write_parquet.py -i /data/inputs/rating_curve/ripple1d/merged_rc_points.parquet \
        -d /data/inputs/rating_curve/ripple1d/huc12_new \
        -c huc12
    '''

    parser = argparse.ArgumentParser(
        description='Filters and writes a new .parquet file for a column name a value.'
    )
    parser.add_argument('-i', '--input_file', help='Input parquet file.', required=True)
    parser.add_argument(
        '-d',
        '--output_dir',
        help='Parent directory to add <huc>/ripple1d_rating_curve.parquet file.',
        required=True,
    )
    parser.add_argument('-c', '--column_name', help='Column name to filter', required=True, type=str)
    parser.add_argument('-v', '--hucs', help='HUC value to filter', nargs='+', required=False)
    parser.add_argument(
        '-o', '--overwrite', help='Overwrite existing parquet files', required=False, action='store_true'
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    partition_and_write_parquet(**args)
