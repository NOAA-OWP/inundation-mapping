#!/usr/bin/env python3

import pandas as pd
import geopandas as gpd
from stream_branches import StreamNetwork
from os.path import splitext
import argparse

def Subset_catch_list(catch_list,stream_network,branch_id_attribute,branch_id_list=None,out_catch_list=None,verbose=False):

    if verbose:
        print("Loading files ....")
    
    # loading files
    catch_list = pd.read_csv(catch_list,sep=" ",header=None,skiprows=1)
    catch_list = catch_list.rename(columns={0:"HydroID",1:"slopes",2:"lengthKM",3:"areasqkm"})
    stream_network = StreamNetwork.from_file(stream_network,branch_id_attribute=branch_id_attribute)
    stream_network = StreamNetwork(stream_network.astype({'HydroID':int}),branch_id_attribute=branch_id_attribute)

    if verbose:
        print("Merging HydroIDs ... ")
    catch_list = catch_list.merge(stream_network.loc[:,["HydroID",branch_id_attribute]],on='HydroID',how='inner')

    unique_branch_ids = catch_list.loc[:,branch_id_attribute].sort_values().unique()
    base_file_path,extension = splitext(out_catch_list)

    if branch_id_list:
        # write unique branch ids to file
        if verbose: 
            print("Writing branch id list ...")
    
        unique_branch_ids.tofile(branch_id_list,sep="\n")

    if verbose:
        print("Writing catch list subsets ...")
    for bid in unique_branch_ids:

        # subsetting to branch id and getting number of hydroids
        branch_catch_list = catch_list.loc[ catch_list.loc[:,branch_id_attribute] == bid, : ]
        num_of_hydroIDs = len(branch_catch_list)

        # dropping branch id attribute
        branch_catch_list = branch_catch_list.drop(columns=branch_id_attribute)

        # out file name management
        out_branch_catch_list = "{}_{}{}".format(base_file_path,bid,extension)

        # write number of hydroids
        with open(out_branch_catch_list,'w') as f:
            f.write("{}\n".format(num_of_hydroIDs))

        # write out catch list in append mode
        branch_catch_list.to_csv(out_branch_catch_list, mode = 'a',header=False,sep=" ",index=False)

    return(catch_list)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Subsets catch list to branch scale')
    parser.add_argument('-c','--catch-list', help='Input catchment list', required=True)
    parser.add_argument('-s','--stream-network', help='Stream Network with HydroIDs and Branch IDs', required=True)
    parser.add_argument('-b','--branch-id-attribute', help='Name of the branch attribute desired', required=True)
    parser.add_argument('-l','--branch-id-list', help='Output the branch id list file desired', required=False,default=None)
    parser.add_argument('-o','--out-catch-list', help='Output catchment list', required=False,default=None)
    parser.add_argument('-v','--verbose', help='Verbose output', required=False,default=False,action='store_true')
    
    args = vars(parser.parse_args())

    Subset_catch_list(**args)
