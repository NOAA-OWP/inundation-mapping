#!/usr/bin/env python3

import pandas as pd
import argparse

def Generate_branch_list(hydroTable,branch_list):

    # load
    hydroTable = pd.read_csv(hydroTable)

    # remove lakes
    hydroTable = hydroTable.loc[hydroTable.loc[:,"LakeID"] == -999,:]

    # drop columns and duplicates
    hydroTable = hydroTable['HydroID'].drop_duplicates()

    # write
    hydroTable.to_csv(branch_list, sep=" ", index=False, header = False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create branch list')
    parser.add_argument('-t','--hydroTable', help='Hydro-Table', required=True)
    parser.add_argument('-c','--branch-list', help='Hydro-Table', required=True)
    
    args = vars(parser.parse_args())

    Generate_branch_list(**args)
