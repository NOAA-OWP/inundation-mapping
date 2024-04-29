#!/usr/bin/env python3

import argparse
import math
import os
import sys

from utils.fim_enums import FIM_exit_codes
from utils.shared_variables import UNIT_ERRORS_MIN_NUMBER_THRESHOLD, UNIT_ERRORS_MIN_PERCENT_THRESHOLD


"""
    Calculates the number of units/hucs that errored during processing.
    Based on a percentage of original number of hucs to be processed,
    this could respond with an abort processing code. It will also
    only throw that code if a minimum number of error exist.
    There should always be at least one (non_zero_exit_codes.log)

    Note: The percentage number as a whole number and the min number of
    errors are stored in the utils/shared_variables.py (kinda like constants)

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders. (ie output_run_data_dir)
    number_of_input_hucs : int
        Number of hucs originally submitted for processing.

    Returns
    ----------
    return_code of 0 (success) or 62 (from fim_enums)
"""


def check_unit_errors(fim_dir, number_of_input_hucs):
    return_code = 0  # default success return code.

    if not os.path.isdir(fim_dir):
        raise Exception(f"The fim output directory of {fim_dir} does not exist")

    unit_errors_dir = os.path.join(fim_dir, "unit_errors")

    if not os.path.isdir(unit_errors_dir):
        raise Exception(
            "The unit errors directory inside the fim output" f" directory of {fim_dir} does not exist"
        )

    error_file_count = 0
    for path in os.listdir(unit_errors_dir):
        if os.path.isfile(os.path.join(unit_errors_dir, path)) and ("non_zero_exit_codes.log" not in path):
            error_file_count += 1

    # We will only error out if it is more than the min number of error files.
    # This is done because sometimes during dev, you are expecting a bunch of errors
    # and sometimes, the number of errors is too small to worry about.

    if error_file_count > UNIT_ERRORS_MIN_NUMBER_THRESHOLD:
        percentage_of_errors = error_file_count / number_of_input_hucs * 100

        if percentage_of_errors >= UNIT_ERRORS_MIN_PERCENT_THRESHOLD:
            errMsg = (
                "Too many unit errors exist to continue," f" code:{FIM_exit_codes.EXCESS_UNIT_ERRORS.value}"
            )
            raise Exception(errMsg)

    return return_code


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='Check number of unit errors to determine if continue')
    parser.add_argument(
        '-f', '--fim_dir', help='root output folder for the process (output + name)', required=True
    )
    parser.add_argument(
        '-n', '--number_of_input_hucs', help='Original number of hucs to process', type=int, required=True
    )

    # extract to dictionary
    args = vars(parser.parse_args())

    # call function
    check_unit_errors(**args)
