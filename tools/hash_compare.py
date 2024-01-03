#!/usr/bin/env python3
import argparse
import hashlib
import os
import re

import geopandas
from geopandas.testing import assert_geodataframe_equal


def main(arg1, arg2, image_only, log_file, gpkg):
    """
    This tool compares either directories or single files. It will create and compare a hashing for
    each file to validate that the files are exactly identical in contents.

    Note: There is an option to compare .gpkg files. This option increases runtimes significantly.
    Please be advised that this feature has not been thoroughly tested to ensure robustness.

    When arg1 and arg2 are single files, the files names need not match but the extensions must match.
    """

    if log_file is not None:
        # Default location of the log file will be the current working directory (where script is called)
        log = os.path.join(os.getcwd(), log_file)
        # Initialize list to send discrepancies to a log file
        print(f" \n \t Will write logging to: {log} \n")

    if os.path.isdir(arg1):
        compare_fim_runs(arg1, arg2, image_only, log_file, gpkg)

    else:
        if os.path.splitext(arg1)[1] == '.gpkg' and os.path.splitext(arg2)[1] == '.gpkg':
            compare_gpkg(arg1, arg2, verbose=True)
        else:
            # Calling hashfile() function to obtain hashes
            # of the files, and saving the result
            # in a variable
            f1_hash = hashfile(arg1)
            f2_hash = hashfile(arg2)

            # Doing primitive string comparison to
            # check whether the two hashes match or not
            if f1_hash == f2_hash:
                print("Both files are the same.")
                print(f"Hash: {f1_hash}")

            else:
                print("Files are different!")
                print(f"Hash of File 1: {f1_hash}")
                print(f"Hash of File 2: {f2_hash}")


def compare_fim_runs(fim_run_dir, previous_fim_run_dir, image_only, log_file, gpkg, list_of_failed_files=[]):
    for huc in [h for h in os.listdir(fim_run_dir) if re.match(r'\d{8}', h)]:
        print(huc)

        branch_dir = os.path.join(fim_run_dir, huc, 'branches')
        branches = [os.path.join(branch_dir, b) for b in os.listdir(branch_dir)]

        for branch in branches:
            print(f'  {os.path.basename(os.path.abspath(branch))}', end='\r')
            passing_branch = True

            for file in os.listdir(branch):
                if image_only and (os.path.splitext(file)[1] != '.tif'):
                    continue

                elif os.path.splitext(file)[1] == '.gpkg' and gpkg:
                    f1 = os.path.join(branch, file)
                    f2 = os.path.join(branch.replace(fim_run_dir, previous_fim_run_dir), file)
                    compare_gpkg(f1, f2, list_of_failed_files, verbose=False)
                    continue

                # Skip Geopackages by defualt
                elif os.path.splitext(file)[1] == '.gpkg':
                    continue

                f1_hash = hashfile(os.path.join(branch, file))
                f2_hash = hashfile(os.path.join(branch.replace(fim_run_dir, previous_fim_run_dir), file))

                if f1_hash == f2_hash:
                    continue
                else:
                    if passing_branch:
                        print(f'  {os.path.basename(os.path.abspath(branch))}')
                        print(f'    -{file} failed hash compare')
                        passing_branch = False
                        if log_file is not None:
                            list_of_failed_files.append(f'{file}')

            if passing_branch:
                print(f'  {os.path.basename(os.path.abspath(branch))}...passed')

    if log_file is not None:
        write_log(list_of_failed_files, log_file)


def hashfile(file):
    # A arbitrary (but fixed) buffer size (change accordingly)
    # 65536 = 65536 bytes = 64 kilobytes
    # BUF_SIZE = 65536
    # 1 MB (size of L2 cache for dev1)
    BUF_SIZE = 1048576

    # Initializing the sha256() method
    sha256 = hashlib.sha256()

    # Open file provided
    with open(file, 'rb') as f:
        while True:
            # reading data = BUF_SIZE from the file and saving it in a variable
            data = f.read(BUF_SIZE)

            if not data:
                break

            # Passing that data to that sh256 hash function (updating the function with that data)
            sha256.update(data)

    # sha256.hexdigest() hashes all the input data passed to the sha256() via sha256.update()
    # Acts as a finalize method, after which all the input data gets hashed hexdigest()
    # hashes the data, and returns the output in hexadecimal format
    return sha256.hexdigest()


def write_log(list_of_failed_files, file):
    log_file = os.path.join(os.getcwd(), file)

    if len(list_of_failed_files) == 0:
        with open(log_file, 'w+') as f:
            f.write("No files failed hash comparison. \n")
    else:
        with open(log_file, 'w+') as f:
            f.write("The following files failed hash comparison: \n")
            for failed_compare_file in list_of_failed_files:
                f.write(f"{failed_compare_file} \n")


def compare_gpkg(file1, file2, list_of_failed_files=[], verbose=False):
    f1_gdf = geopandas.read_file(file1)
    f2_gdf = geopandas.read_file(file2)

    try:
        assert_geodataframe_equal(f1_gdf, f2_gdf)
        # We only care about failures when FIM output directories are compared,
        # however if only two .gpkg files are compared, print success.
        if verbose:
            print("\n Both files are the same. \n")

    except AssertionError as e:
        print(f"\n {str(e)} \n")
        print("  The following files failed assert_geodataframe_equal: ")
        print(f"    {file1.rsplit('/', 1)[-1]} ")
        list_of_failed_files.append(f1_gdf)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This script will generate a cryptographic hash for either two files, or all files '
        'within two directories.  If two directories are provided, it will iterate over both folders named '
        '"branches", and generate a hash for each file, and compare them. This tool is very valuable when '
        'we want to compare FIM two output directories which should not have changed. '
        '.gpkg files will be read into GeoDataFrames and compared (if -gpkg is specified).',
        usage='''
            hash_compare.py /efs-drives/fim-dev-efs/fim-data/outputs/dev-f-and-s-no-post/05030104/branches/0/
                demDerived_reaches_split_filtered_addedAttributes_crosswalked_0.gpkg
                /efs-drives/fim-dev-efs/fim-data/outputs/dev-no-post/05030104/branches/0/
                demDerived_reaches_split_filtered_addedAttributes_crosswalked_0.gpkg
            hash_compare.py /efs-drives/fim-dev-efs/fim-data/outputs/dev-f-and-s-no-post
                /efs-drives/fim-dev-efs/fim-data/outputs/dev-format-and-style-no-post
                -gpkg
                -l log.txt
        ''',
    )
    parser.add_argument(
        'file1', help='File or directory to compare generated hash values. (.gpkg files not supported)'
    )
    parser.add_argument(
        'file2', help='File or directory to compare generated hash values. (.gpkg files not supported)'
    )
    parser.add_argument('-i', '--image', help='', action='store_true')
    parser.add_argument(
        '-l', '--log_file', help='Optional argument to write stdout to a log file', default=None
    )
    parser.add_argument(
        '-gpkg',
        '--compare_gpkg',
        help='Optional argument to compare .gpkg files. This will significantly increase runtime.',
        action='store_true',
        default=False,
    )

    args = vars(parser.parse_args())
    file1 = args['file1']
    file2 = args['file2']
    image_only = args['image']
    log_file = args['log_file']
    gpkg = args['compare_gpkg']

    main(file1, file2, image_only, log_file, gpkg)
