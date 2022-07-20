#!/usr/bin/env python3

import os
import argparse
from glob import glob

def __read_included_files(parent_dir_path):

    filename_patterns = glob(os.path.join(parent_dir_path,'included_huc*.lst'))
    
    accepted_hucs_set = set()
    for filename in filename_patterns:

        with open(filename,'r') as huc_list_file:
            file_lines = huc_list_file.readlines()
            f_list = [fl.rstrip() for fl in file_lines]
            accepted_hucs_set.update(f_list)

    return(accepted_hucs_set)


def __read_input_hucs(hucs):

    huc_list = set()
    if os.path.isfile(hucs[0]):
        with open(hucs[0],'r') as hucs_file:
            file_lines = hucs_file.readlines()
            # Strips the newline character
            f_list = [fl.rstrip() for fl in file_lines]
            huc_list.update(f_list)
    else:
        if (len(hucs) > 0):
            for huc in hucs:
                huc_list.add(huc.strip())
        else:
            huc_list.add(hucs[0].strip())

    return(huc_list)


def __check_for_membership(hucs,accepted_hucs_set):

    for huc in hucs:
        if huc not in accepted_hucs_set:
            raise KeyError("HUC {} not found in available inputs. Edit HUC inputs or acquire datasets and try again".format(huc))


def check_hucs(hucs):

    accepted_hucs = __read_included_files(os.path.join(os.environ['inputDataDir'],'huc_lists'))
    list_hucs = __read_input_hucs(hucs)
    __check_for_membership(list_hucs, accepted_hucs)
    
    # we need to return the number of hucs being used.
    # it is not easy to return a value to bash, except with standard out.
    # so we will just to a print line back (Note: This means there can be no other
    # print commands in this file, even for debugging, as bash will pick up the 
    # very first "print"
    print(len(list_hucs))


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Checks input hucs for availability within inputs')
    parser.add_argument('-u','--hucs',help='Line-delimited file or list of HUCs to check availibility for',required=True, nargs='+')

    # extract to dictionary
    args = vars(parser.parse_args())

    # call function
    check_hucs(**args)
