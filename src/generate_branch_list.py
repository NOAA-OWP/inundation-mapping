#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import sys

sys.path.append('/foss_fim/src/gms/')
from stream_branches import StreamNetwork

def Generate_branch_list(stream_network_dissolved, branch_id_attribute,
                         output_branch_list, output_branch_csv, huc_id):

    # we need two copies, one that is a single column list for the branch iterator (parallel)
    # and one for later tools that need the huc number as well. (aggregate hucs)

    if os.path.exists(stream_network_dissolved):
        # load stream network
        stream_network_dissolved = StreamNetwork.from_file( stream_network_dissolved,
                                                            branch_id_attribute=branch_id_attribute )
        # reduce to branch id attribute and convert to pandas df
        stream_network_dissolved = stream_network_dissolved.loc[:,branch_id_attribute]
        
        # write out the list version (just branch numbers)
        stream_network_dissolved.to_csv(output_branch_list, sep= " ", index=False, header=False)

        # we only add branch zero to the csv, not the list
        branch_zero_row = pd.Series("0")
        bz_stream_network_dissolved = stream_network_dissolved.append(branch_zero_row)
        
        # Create the dataframe version
        df_stream_network_dissolved = bz_stream_network_dissolved.to_frame()

        # add the extra column (first column)
        df_stream_network_dissolved.insert(0, 'huc_id', huc_id, True)
        
        #stream_network_dissolved.to_csv(output_branch_list,sep= " ",index=False,header=False)
        df_stream_network_dissolved.to_csv(output_branch_csv, index=False, header=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create branch list')
    parser.add_argument('-d','--stream-network-dissolved', help='Dissolved stream network', required=True)
    parser.add_argument('-b','--branch-id-attribute', help='Branch ID attribute to use in dissolved stream network', required=True)
    parser.add_argument('-oc','--output-branch-csv', help='Output branch list', required=True)
    parser.add_argument('-ol','--output-branch-list', help='Output branch list', required=True)    
    parser.add_argument('-u','--huc-id', help='HUC number being aggregated', required=True)
    
    args = vars(parser.parse_args())

    Generate_branch_list(**args)
