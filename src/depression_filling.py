#!/usr/bin/env python3

import argparse
import os
import sys

import whitebox


def fill_depressions(
    dem: str, output: str, fix_flats: bool = True, flat_increment: float = None, max_depth: float = None
):
    """Fill depressions in a DEM using WhiteboxTools.
    Parameters:
        dem (str): Path to the input DEM file.
        output (str): Path to the output filled DEM file.
        fix_flats (bool): Whether to fix flat areas. Default is True.
        flat_increment (float or None): Increment for flat areas. Default is None.
        max_depth (float or None): Maximum depth for filling depressions. Default is None.
    """

    # Initialize WhiteboxTools
    # Set wbt envs
    wbt = whitebox.WhiteboxTools()
    wbt.set_verbose_mode(False)
    wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))

    # Run the fill_depressions tool
    if not os.path.exists(dem):
        print(f"Error: Input DEM file '{dem}' does not exist.")
        sys.exit(1)

    try:
        wbt.fill_depressions(
            dem=dem, output=output, fix_flats=fix_flats, flat_increment=flat_increment, max_depth=max_depth
        )
        print(f"Depressions filled successfully. Output saved to '{output}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill depressions in a DEM using WhiteboxTools.")
    parser.add_argument("-i", "--dem", type=str, required=True, help="Input DEM file path")
    parser.add_argument("-o", "--output", type=str, required=True, help="Output filled DEM file path")
    parser.add_argument("--fix_flats", type=bool, default=True, help="Whether to fix flat areas")
    parser.add_argument("--flat_increment", type=float, default=None, help="Increment for flat areas")
    parser.add_argument("--max_depth", type=float, default=None, help="Maximum depth for filling depressions")
    args = parser.parse_args()

    fill_depressions(**vars(args))
