#!/usr/bin/env python
"""
Get parent or child HUCs for a huc list file.
"""
import argparse

import pandas as pd
from hucs import Huc, HucList


def main(
    input_huc_list_file: str | None = None,
    input_huc_list: str | None = None,
    output_huc_list_file: str | None = None,
    huc_level: int | None = None,
    n_jobs: int = 1,
    quiet: bool = False,
):

    # input_huc_list and input_huc_list_file are mutually exclusive
    if input_huc_list:
        huc_list = HucList(input_huc_list)
    else:
        huc_list = HucList.from_huc_list_file(input_huc_list_file)

    # Get parent or child HUCs and create a new HucList object
    output_huc_list__ = huc_list.get_any_hucs(huc_level, verbose=(not quiet), n_jobs=n_jobs)

    # drop NAs
    output_huc_list__.dropna(inplace=True)

    # Convert the output to a list of HUCs
    output_huc_list = HucList(output_huc_list__)

    # Output the new HucList object to a file or print it as line-separated HUCs
    if output_huc_list_file:
        output_huc_list.to_huc_list_file(output_huc_list_file)
    else:
        # line-separated HUCs
        for huc in output_huc_list.hucs:
            print(huc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)

    # input_huc_list_file and input_huc_list are mutually exclusive
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-i", "--input_huc_list_file", type=str, help="Input huc list file path.")
    group.add_argument("-l", "--input_huc_list", type=str, help="Input huc list", nargs="+")

    parser.add_argument(
        "-o", "--output_huc_list_file", required=False, type=str, help="Output huc list file path."
    )
    parser.add_argument(
        "-e", "--huc_level", required=True, type=int, help="HUC level to get parent or child HUCs for."
    )
    parser.add_argument(
        "-j",
        "--n_jobs",
        type=int,
        default=1,
        help="Number of parallel jobs. One job runs the whole process serially.",
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress output.")

    args = vars(parser.parse_args())
    main(**args)
