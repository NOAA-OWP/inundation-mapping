#!/usr/bin/env python3

import os
import argparse
from glob import glob

def __read_included_files(parent_dir_path):

    filename_patterns = glob(os.path.join(parent_dir_path,'included_huc*.lst'))

    accepted_hucs_set = set()
    for filename in filename_patterns:

        f = open(filename,'r')
        fList = f.readlines()
        f.close()

        fList = [fl.rstrip() for fl in fList]

        accepted_hucs_set.update(fList)

    return(accepted_hucs_set)


def __read_input_hucs(hucs):

    hucs = [h.split() for h in hucs][0]
    if os.path.isfile(hucs[0]):
        with open(hucs[0],'r') as hucs_file:
            hucs = hucs_file.readlines()
            hucs = [h.split() for h in hucs][0]

    return(hucs)


def __check_for_membership(hucs,accepted_hucs_set):

    for huc in hucs:
        if huc not in accepted_hucs_set:
            raise KeyError("HUC {} not found in available inputs. Edit HUC inputs or acquire datasets and try again".format(huc))


def check_hucs(hucs):

    accepted_hucs = __read_included_files(os.path.join(os.environ['inputDataDir'],'huc_lists'))
    hucs = __read_input_hucs(hucs)
    __check_for_membership(hucs,accepted_hucs)

if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Checks input hucs for availability within inputs')
    parser.add_argument('-u','--hucs',help='Line-delimited file or list of HUCs to check availibility for',required=True,nargs='+')

    # extract to dictionary
    args = vars(parser.parse_args())

    # call function
    check_hucs(**args)
