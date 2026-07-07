#!/usr/bin/env python3

import argparse
import os
import pathlib
from glob import glob

def __read_acceptable_file_list(full_huc_list):
    filename_patterns = glob(full_huc_list)

    accepted_hucs_set = set()
    for filename in filename_patterns:
        with open(filename, 'r') as huc_list_file:
            file_lines = huc_list_file.readlines()
            f_list = [fl.rstrip() for fl in file_lines]
            accepted_hucs_set.update(f_list)

    return accepted_hucs_set


def __read_input_hucs(hucs):
    huc_list = set()
    if os.path.isfile(hucs[0]):
        source_file_extension = pathlib.Path(hucs[0]).suffix

        if source_file_extension.lower() != ".lst":
            raise Exception("Incoming file must be in .lst format if submitting a file name and path.")

        with open(hucs[0], 'r') as hucs_file:
            file_lines = hucs_file.readlines()
            f_list = [__clean_huc_value(fl) for fl in file_lines]
            huc_list.update(f_list)
    else:
        if len(hucs) > 0:
            for huc in hucs:
                huc_list.add(__clean_huc_value(huc))
        else:
            huc_list.add(__clean_huc_value(hucs[0]))

    huc_list = sorted(huc_list)

    return huc_list


def __clean_huc_value(huc):
    # Strips the newline character plus
    # single or double quotes (which sometimes happens)
    huc = huc.strip().replace("\"", "")
    huc = huc.replace("\'", "")
    return huc


def __check_for_membership(hucs, accepted_hucs_set, full_huc_list):
    for huc in hucs:
        if (type(huc) is str) and (not huc.isnumeric()):
            msg = f"Huc value of {huc} does not appear to be a number. "
            msg += "It could be an incorrect value but also could be that the huc list "
            msg += "(if you used one) is incorrect or is not unix encoded."
            raise KeyError(msg)

        if huc not in accepted_hucs_set:
            msg = f"HUC {huc} not found in the acceptable HUC list at {full_huc_list}."
            " Edit HUC inputs or acquire datasets & try again."
            raise KeyError(msg)

# Might be a file path (full_huc_list) or a list of hucs (ie 12090301 05030104)
def check_hucs(hucs, full_huc_list, huc_list_output_file):
    accepted_hucs = __read_acceptable_file_list(full_huc_list)
    list_hucs = __read_input_hucs(hucs)
    __check_for_membership(list_hucs, accepted_hucs, full_huc_list)

    with open(huc_list_output_file, "w") as f:
        for item in list_hucs:
            f.write(f"{item}\n")

    # we need to return the number of hucs being used.
    # it is not easy to return a value to bash, except with standard out.
    # so we will just to a print line back (Note: This means there can be no other
    # print commands in this file, even for debugging, as bash will pick up the
    # very first "print"

    # if you want to print, you can use flush. ie) print(f"number of hucs is {len(list_hucs)}", flush=True)
    # by returning a print line, bash will pick it up as standard output and assign it
    # to a variable and manage it.
    print(len(list_hucs))


if __name__ == '__main__':

    # This script helps ensure that all hucs passed in to pipeline or pre-processing are valid HUCs
    # and are in the full_huc_list.lst file as valid and approved HUCS.

    # It is ok if this throws exceptions

    # parse arguments
    parser = argparse.ArgumentParser(description='Checks input hucs for availability within inputs')
    parser.add_argument(
        '-u',
        '--hucs',
        help='Line-delimited file or list of HUCs to check availibility for',
        required=True,
        nargs='+',
    )
    parser.add_argument('-i', '--full-huc-list', help='Full HUC list file', required=True)
    parser.add_argument('-o', '--huc-list-output-file', help='The parsed and validated HUC list', required=True)

    # extract to dictionary
    args = vars(parser.parse_args())

    # call function
    check_hucs(**args)
