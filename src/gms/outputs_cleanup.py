#!/usr/bin/env python3

import os
import argparse

def remove_deny_list_files(directory,deny_list,branches=False,verbose=False):

    deny_list_file_object = open(deny_list)

    file_removed_count = 0

    for line in deny_list_file_object:
        line = line.partition('#')[0]
        filename = line.rstrip()

        if len(filename) == 0:
            continue
        
        filename = os.path.join(directory,filename)

        # for use with branches. append branch id
        if branches:
            branch_id = directory.split('/')[-1]
            filename = filename.format(branch_id)
        
        if os.path.exists(filename):
            os.remove(filename)
            file_removed_count +=1

    if verbose:
        print(f'Removed {file_removed_count} files')


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Clean up outputs given file with line delimineted files')
    parser.add_argument('-d','--directory', help='Directory to find files', required=True)
    parser.add_argument('-l','--deny-list', help='Path to deny list file. Must be line delimited', required=True)
    parser.add_argument('-b','--branches', help='For use with branch files', required=False,default=False, action='store_true')
    parser.add_argument('-v','--verbose', help='Verbose', required=False,default=False, action='store_true')

    # extract to dictionary
    args = vars(parser.parse_args())

    remove_deny_list_files(**args)

