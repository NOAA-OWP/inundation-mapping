#!/usr/bin/env python3

import argparse
import os

from pathlib import Path
from utils.shared_functions import FIM_Helpers as fh


def remove_deny_list_files(src_dir, deny_list, branch_id, verbose=False):
    '''
    Overview
    ----------
    Delete a set of files in a given directory (and/or subdirectories) based
    on values in the deny list.

    Notes:
        - Strange, but.. if you want to skip deleting file, have the value for
          the 'deny_list' param to be the value of "none" (this is a by product
          of using bash as part of our system)
        - In the deny list, any line starting with a # will be skipped. Any line
          value which contains the value of {}, will be replaced with submitted
          branch id. If the line does not have a {} in it, it will be searched
          and removed for an exact file match.
        - Technically, we don't validate that the branch id is a number, and will
          work with any value.

    Parameters
    ----------

    - src_dir : str
        Folder path where the files are to be deleted (recursive).
        Will error if does not exist.

    - deny_list : str
        If not the value of "none" (any case), the file must exist and it contains
        the list of files to be deleted. Will error if does not exist.

    - branch_id : str
        Needs to have a value and will be subsituted into any {} value.
        Will error if does not exist.

    Returns
    ----------
        None
    '''

    # Yes.. this is a little strange.
    # if the user submitts the deny list name of "none" (any case)
    # we skip this
    if deny_list.upper() == 'NONE':
        print("file clean via the deny list skipped")
        return

    if not os.path.isdir(src_dir):
        raise ValueError(f"Sorry, the directory {src_dir} does not exist")

    if branch_id.strip() == "":
        raise ValueError(f"Sorry, branch id value must exist")

    # Note: some of the deny_file_names might be a comment line
    # this will validate file exists
    deny_file_names = fh.load_list_file(deny_list.strip())

    fh.vprint(f"source folder is {src_dir}", verbose)
    fh.vprint(f"deny_list is {deny_list}", verbose)

    file_removed_count = 0

    for deny_file_name in deny_file_names:
        # Only keep lines that do no start with a #
        # aka.. we are only deleting files that do not start a line with #
        deny_file_name = deny_file_name.strip()
        if deny_file_name.startswith("#"):
            continue

        # the file name may / may not have a {} in it . If it does
        # has a {} it in, we will replace it with the branch ID.
        # if the file name does not have a {} in it, that file
        # will be deleted.
        # We will search all directories recursively.
        deny_file_name = deny_file_name.replace("{}", branch_id)

        found_files = Path(src_dir).rglob(f"{deny_file_name}")

        for found_file in found_files:
            fh.vprint(f"found file: {found_file}", verbose, False)

            if os.path.exists(found_file):
                os.remove(found_file)

            file_removed_count += 1

    fh.vprint(f"Removed {file_removed_count} files", verbose, True)


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(
        description='Clean up outputs given file with line delimineted files'
    )
    parser.add_argument('-d', '--src_dir', help='Directory to find files', required=True)
    parser.add_argument(
        '-l', '--deny_list', help='Path to deny list file. Must be line delimited', required=True
    )
    parser.add_argument('-b', '--branch_id', help='Branch id value', required=True)
    parser.add_argument(
        '-v', '--verbose', help='Verbose', required=False, default=False, action='store_true'
    )

    # extract to dictionary
    args = vars(parser.parse_args())

    remove_deny_list_files(**args)
