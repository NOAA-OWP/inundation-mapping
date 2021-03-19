#!/usr/bin/env python3

import pandas as pd
from stream_branches import StreamNetwork
import argparse

def Generate_branch_list(hydroTable,stream_network_dissolved,branch_id_attribute,branch_list):

    # load
    hydroTable = pd.read_csv(hydroTable)

    # remove lakes
    hydroTable = hydroTable.loc[hydroTable.loc[:,"LakeID"] == -999,:]

    # drop columns and duplicates
    hydroTable = hydroTable['HydroID'].drop_duplicates()

    # load stream network
    stream_network_dissolved = StreamNetwork.from_file(stream_network_dissolved,branch_id_attribute=branch_id_attribute)
    stream_network_dissolved = pd.DataFrame(stream_network_dissolved.loc[:,branch_id_attribute])

    # write
    stream_network_dissolved.to_csv(branch_list,sep= " ",index=False,header=False)
    #hydroTable.to_csv(branch_list, sep=" ", index=False, header = False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create branch list')
    parser.add_argument('-t','--hydroTable', help='Hydro-Table', required=True)
    parser.add_argument('-d','--stream-network-dissolved', help='Hydro-Table', required=True)
    parser.add_argument('-b','--branch-id-attribute', help='Hydro-Table', required=True)
    parser.add_argument('-c','--branch-list', help='Hydro-Table', required=True)
    
    args = vars(parser.parse_args())

    Generate_branch_list(**args)
