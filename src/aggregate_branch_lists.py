#!/usr/bin/env python3

import argparse
import sys
from glob import glob
from os.path import join

import pandas as pd


def aggregate_branch_lists(output_dir, file_name, output_file_name):
    file_names = glob(join(output_dir, '*', file_name))

    if len(file_names) == 0:
        print("Error: No Branches available to aggregate. Program terminated.", flush=True)
        sys.exit(1)

    df_combined = pd.concat([pd.read_csv(f, header=None, dtype='str') for f in file_names], ignore_index=True)

    df_combined.to_csv(output_file_name, index=False, header=False)


if __name__ == '__main__':
    # This tool takes in a single file name and searchs all directories
    # recusively for the same file name and merges them.

    parser = argparse.ArgumentParser(description='Aggregate')
    parser.add_argument('-d', '--output_dir', help='output run data directory', required=True)
    parser.add_argument('-f', '--file_name', help='file name to match', required=True)
    parser.add_argument('-o', '--output_file_name', help='output file name', required=True)

    args = vars(parser.parse_args())

    aggregate_branch_lists(**args)
