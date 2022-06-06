#!/usr/bin/env python3

import pandas as pd
from stream_branches import StreamNetwork
import argparse

def Generate_branch_list(stream_network_dissolved, branch_id_attribute, output_branch_list, branch_zero):

    # load stream network

    stream_network_dissolved = StreamNetwork.from_file( stream_network_dissolved,
                                                        branch_id_attribute=branch_id_attribute )

    # reduce to branch id attribute and convert to pandas df
    stream_network_dissolved = stream_network_dissolved.loc[:,branch_id_attribute]

    # write
    stream_network_dissolved.to_csv(output_branch_list,sep= " ",index=False,header=False)

    # Add branch zero ID to branch list
    if branch_zero:
        with open(output_branch_list,'a') as branch_lst:
            branch_lst.write(f'{branch_zero}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create branch list')
    parser.add_argument('-d','--stream-network-dissolved', help='Dissolved stream network', required=True)
    parser.add_argument('-b','--branch-id-attribute', help='Branch ID attribute to use in dissolved stream network', required=True)
    parser.add_argument('-o','--output-branch-list', help='Output branch list', required=True)
    parser.add_argument('-z','--branch-zero', help='Branch Zero ID', required=False)
    
    args = vars(parser.parse_args())

    Generate_branch_list(**args)
