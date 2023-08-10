#!/usr/bin/env python3

import os
import argparse
import re


def create_huc_list(input_root_search_folder, output_path, overwrite=False):
    '''
    Summary: This scans an input directory, such as test_cases, and looks for each
         unique huc value through those folders. This gives us a single list of all hucs
         that can be used for alpha or sierra tests.
         This looks for first level folders using the following convention:
            - *_test_cases  (ie usgs_test_cases)
                Then subfolders using the following convention:
                - {8 numbers}_{3 to 7 characters}. ie) 01080005_usgs
    Input:
        - input_root_search_folder: A fully qualified root path (relative to Docker pathing) to the folder that all hucs
            are in.
        - output_path: a path and file name (preferred as .lst or .txt) where the list of line delimited hucs
            will be copied. The file and path do not need to pre-exist.
        - overwrite: If the file exists and the overwrite flag is false, an error will be issued. Else, it will
            be fully overwritten.
    Output:
        - a line delimited list of all huc numbers that have test cases available.
    '''

    if not os.path.exists(input_root_search_folder):
        raise NotADirectoryError(
            f"Sorry. Search_folder of {input_root_search_folder} does not exist"
        )

    if os.path.exists(output_path) and not overwrite:
        raise Exception(
            f"Sorry. The file {output_path} already exists. Use 'overwrite' argument if desired."
        )

    hucs = set()

    for test_case in [
        test_case
        for test_case in os.listdir(input_root_search_folder)
        if re.match('.*_test_cases', test_case)
    ]:
        for test_id in [
            test_id
            for test_id in os.listdir(os.path.join(input_root_search_folder, test_case))
            if re.match('\d{8}_\w{3,7}', test_id)
        ]:
            hucs.add(test_id[:8])

    sorted_hucs = sorted(hucs)
    # print(sorted_hucs)
    print(f"{str(len(sorted_hucs))} hucs found")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save to disk
    with open(output_path, 'w') as output_file:
        # output_file.writelines(sorted_hucs)
        for huc in sorted_hucs:
            output_file.write(huc)
            output_file.write('\n')

    print(f"huc list saved at {output_path}")


if __name__ == '__main__':
    # Sample Usage: python /foss_fim/tools/find_test_case_hucs.py -d /data/test_cases/ -f /some_directory/huc_list_for_tests_22020420.lst -o

    parser = argparse.ArgumentParser(description='Finds all unique hucs that have test case data.')
    parser.add_argument(
        '-d',
        '--input_root_search_folder',
        help='Root folder to be scanned for unique hucs that have test case',
        required=True,
    )
    parser.add_argument(
        '-f',
        '--output_path',
        help='Folder path and file name to be saved (.txt or .lst suggested).',
        required=True,
    )
    parser.add_argument(
        '-o',
        '--overwrite',
        help='Overwrite the file if already existing? (default false)',
        action='store_true',
    )

    args = vars(parser.parse_args())

    create_huc_list(**args)
