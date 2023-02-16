#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import sys

#sys.path.append('/foss_fim/src')
from stream_branches import StreamNetwork

def generate_branch_list(stream_network_dissolved, branch_id_attribute,
                         output_branch_list_file):

    '''
    Processing:
        This create a branch_ids.lst file which is required at the very start of processing
        hucs.  This becomes the list that run_unit_wb.sh needs to iterate over branches
        
        Note: The .csv twin to this is appended to each time a branch completes, 
        resulting in a list that only contains successfully processed branches.
    Params:
        - stream_network_dissolved (str): the gkpg that contains the list of disolved branch ids
        - branch_id_attribute (str): the id of the field in the gkpg that has the branch ids.
            (ie. like levpa_id (from params_template.env) )
        - output_branch_list_file (str): file name and path of the list to be created.
    Output:
        - create a file (likely a .lst file) with branch ids (not including branch zero)
    '''
    
    if os.path.exists(stream_network_dissolved):
        # load stream network
        stream_network_dissolved = StreamNetwork.from_file( stream_network_dissolved,
                                                            branch_id_attribute=branch_id_attribute )
        # reduce to branch id attribute and convert to pandas df
        stream_network_dissolved = stream_network_dissolved.loc[:,branch_id_attribute]
        
        # write out the list version (just branch numbers)
        stream_network_dissolved.to_csv(output_branch_list_file, sep= " ", index=False, header=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create branch list')
    parser.add_argument('-d','--stream-network-dissolved', help='Dissolved stream network', required=True)
    parser.add_argument('-b','--branch-id-attribute', help='Branch ID attribute to use in dissolved stream network', required=True)
    parser.add_argument('-o','--output-branch-list-file', help='Output branch list', required=True)    
    
    args = vars(parser.parse_args())

    generate_branch_list(**args)
