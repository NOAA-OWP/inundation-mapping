#!/usr/bin/env python3

import argparse 
import pandas as pd
from os import environ
from os.path import join
from glob import glob


def aggregate_inputs_for_gms(huc_list, output_dir, output_file_name):

    # bash will send huclist in as a colletion and not a string
    if isinstance(huc_list, list):
        huc_list_file = huc_list[0]
    else:
        huc_list_file = huc_list
    print(huc_list_file)
    try:
        huc_list = pd.read_csv(huc_list_file,header=None,dtype=str).loc[:,0].tolist()
    except FileNotFoundError:
        pass

    hucs = set(huc_list)

    # get branch lists
    branch_id_files = glob(join(output_dir,'*','branch_id.lst'))

    all_huc_numbers,all_bids = [],[]
    for bid_file in branch_id_files:
        huc_number = bid_file.split('/')[-2]
        
        if huc_number in hucs:
            bids = pd.read_csv(bid_file,header=None).loc[:,0].tolist()
            huc_number_list = [huc_number] * len(bids)

            all_bids += bids
            all_huc_numbers += huc_number_list
        
    output = pd.DataFrame({ 
                            'huc': all_huc_numbers,
                            'branch' : all_bids
                          })
    
    output.to_csv(join(output_dir, output_file_name),index=False,header=False)



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregate GMS Inputs')
    parser.add_argument('-d','--output_dir', help='output run data directory', required=True)
    parser.add_argument('-f','--output_file_name', help='output file name', required=True)
    parser.add_argument('-l','--huc_list', help='huc list', required=True,nargs='+')

    args = vars(parser.parse_args())

    aggregate_inputs_for_gms(**args)
