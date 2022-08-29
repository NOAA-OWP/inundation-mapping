#!/usr/bin/env python3

from stream_branches import StreamNetwork
import argparse
from utils.shared_functions import get_fossid_from_huc8
import geopandas as gpd


def Derive_level_paths(in_stream_network, out_stream_network,branch_id_attribute,
                       out_stream_network_dissolved=None,huc_id=None,
                       headwaters_outfile=None,catchments=None,
                       catchments_outfile=None,
                       branch_inlets_outfile=None,
                       toNode_attribute='To_Node',fromNode_attribute='From_Node',
                       reach_id_attribute='HydroID',verbose=False
                       ):

    # getting foss_id of huc8
    #foss_id = get_fossid_from_huc8(huc8_id=huc_id,foss_id_attribute='fossid',
                                   #hucs_layerName='WBDHU8')
    
    if verbose:
        print("Deriving level paths ...")

    # load file
    if verbose:
        print("Loading stream network ...")

    stream_network = StreamNetwork.from_file(in_stream_network)

    inlets_attribute = 'inlet_id'
    outlets_attribute = 'outlet_id'
    outlet_linestring_index = -1

    # converts multi-linestrings to linestrings
    #print(stream_network.crs);exit(0)
    stream_network = stream_network.multilinestrings_to_linestrings()

    # derive nodes
    stream_network = stream_network.derive_nodes(toNode_attribute=toNode_attribute,
                                                  fromNode_attribute=fromNode_attribute,
                                                  reach_id_attribute=reach_id_attribute,
                                                  outlet_linestring_index=outlet_linestring_index,
                                                  node_prefix=None,
                                                  verbose=verbose)
    
    # derive outlets and inlets
    stream_network = stream_network.derive_outlets(toNode_attribute,
                                                   fromNode_attribute,
                                                   outlets_attribute=outlets_attribute,
                                                   verbose=verbose
                                                  )
    stream_network = stream_network.derive_inlets(toNode_attribute,
                                                  fromNode_attribute,
                                                  inlets_attribute=inlets_attribute,
                                                  verbose=verbose
                                                 )

    # derive up and downstream networks
    upstreams, downstreams = stream_network.make_up_and_downstream_dictionaries(
                                                                         reach_id_attribute=reach_id_attribute,
                                                                         toNode_attribute=toNode_attribute,
                                                                         fromNode_attribute=fromNode_attribute,
                                                                         verbose=True
                                                                         )

    # derive arbolate sum
    stream_network = stream_network.get_arbolate_sum(arbolate_sum_attribute='arbolate_sum',
                                                     inlets_attribute=inlets_attribute,
                                                     reach_id_attribute=reach_id_attribute,
                                                     upstreams=upstreams,
                                                     downstreams=downstreams,
                                                     length_conversion_factor_to_km = 0.001,
                                                     verbose=verbose
                                                    )

    # derive stream branches
    stream_network = stream_network.derive_stream_branches( toNode_attribute=toNode_attribute,
                                                            fromNode_attribute=fromNode_attribute,
                                                            upstreams=upstreams,
                                                            branch_id_attribute=branch_id_attribute,
                                                            reach_id_attribute=reach_id_attribute,
                                                            comparison_attributes='arbolate_sum',
                                                            comparison_function=max,
                                                            verbose=verbose
                                                           )
    
    # filter out streams with out catchments
    if (catchments is not None) & (catchments_outfile is not None):

        catchments = gpd.read_file(catchments)

        stream_network = stream_network.remove_branches_without_catchments( 
                                                                catchments,
                                                                reach_id_attribute=reach_id_attribute,
                                                                branch_id_attribute=branch_id_attribute,
                                                                reach_id_attribute_in_catchments=reach_id_attribute,
                                                                verbose=verbose
                                                                              )

        # subset which columns to merge
        stream_network_to_merge = stream_network.filter(
                                                        items = [reach_id_attribute,inlets_attribute,
                                                                 outlets_attribute,branch_id_attribute]
                                                       )

        catchments = catchments.merge(stream_network_to_merge,how='inner',
                                      left_on=reach_id_attribute,
                                      right_on=reach_id_attribute
                                     )

        catchments.reset_index(drop=True,inplace=True)

        catchments.to_file(catchments_outfile,index=False,driver='GPKG')

    # derive headwaters
    if headwaters_outfile is not None:
        headwaters = stream_network.derive_headwater_points_with_inlets(
                                                        fromNode_attribute=fromNode_attribute,
                                                        inlets_attribute=inlets_attribute,
                                                        outlet_linestring_index=outlet_linestring_index
                                                       )
        # headwaters write
        headwaters.to_file(headwaters_outfile,index=False,driver='GPKG')

    
    if out_stream_network is not None:
        if verbose:
            print("Writing stream branches ...")
        stream_network.write(out_stream_network,index=True)
    
    if out_stream_network_dissolved is not None:
    
        # dissolve by levelpath
        stream_network = stream_network.dissolve_by_branch(branch_id_attribute=branch_id_attribute,
                                                           attribute_excluded=None, #'order_',
                                                           values_excluded=None, #[1,2],
                                                           out_vector_files=out_stream_network_dissolved,
                                                           verbose=verbose)

        branch_inlets = stream_network.derive_inlet_points_by_feature( feature_attribute=branch_id_attribute,
                                                                       outlet_linestring_index=outlet_linestring_index
                                                                     )
   
        if branch_inlets_outfile is not None:
            branch_inlets.to_file(branch_inlets_outfile,index=False,driver='GPKG')

    return(stream_network)
    


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create stream network level paths')
    parser.add_argument('-i','--in-stream-network', help='Input stream network', required=True)
    parser.add_argument('-b','--branch-id-attribute', help='Name of the branch attribute desired', required=True)
    parser.add_argument('-u','--huc-id', help='Current HUC ID', required=False,default=None)
    parser.add_argument('-r','--reach-id-attribute', help='Reach ID attribute to use in source file', required=False,default='HydroID')
    parser.add_argument('-c','--catchments', help='NWM catchments to append level path data to', required=False, default=None)
    parser.add_argument('-t','--catchments-outfile', help='NWM catchments outfile with appended level path data', required=False, default=None)
    parser.add_argument('-n','--branch_inlets_outfile', help='Output level paths inlets', required=False,default=None)
    parser.add_argument('-o','--out-stream-network', help='Output stream network', required=False,default=None)
    parser.add_argument('-e','--headwaters-outfile', help='Output stream network headwater points', required=False,default=None)
    parser.add_argument('-d','--out-stream-network-dissolved', help='Dissolved output stream network', required=False,default=None)
    parser.add_argument('-v','--verbose', help='Verbose output', required=False,default=False,action='store_true')
    
    args = vars(parser.parse_args())

    Derive_level_paths(**args)

