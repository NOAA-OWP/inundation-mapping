#!/usr/bin/env python3

from stream_branches import StreamNetwork
import argparse


def Derive_level_paths(in_stream_network,out_stream_network,
                       toNode_attribute='To_Node',fromNode_attribute='From_Node',
                       ):
    # load file
    stream_network = StreamNetwork.from_file(in_stream_network)

    # derive outlets and inlets
    stream_network = stream_network.derive_outlets(toNode_attribute,fromNode_attribute)
    stream_network = stream_network.derive_inlets(toNode_attribute,fromNode_attribute)

    # derive arbolate sum
    stream_network = stream_network.get_arbolate_sum(arbolate_sum_attribute='arbolate_sum',
                                                     inlets_attribute='inlet_id',
                                                     reach_id_attribute='HydroID',
                                                     toNode_attribute=toNode_attribute,
                                                     fromNode_attribute=fromNode_attribute,
                                                     length_conversion_factor_to_km = 0.001,
                                                    )

    # derive stream branches
    stream_network = stream_network.derive_stream_branches( toNode_attribute=toNode_attribute,
                                                            fromNode_attribute=fromNode_attribute,
                                                            branch_id_attribute='levpa_id',
                                                            reach_id_attribute='HydroID',
                                                            comparison_attributes='order_',
                                                            comparison_function=max)
    
    # dissolve by levelpath
    #stream_network = stream_network.dissolve_by_branch(branch_id_attribute='levpa_id',
    #                                                   attribute_excluded='order_',
    #                                                   values_excluded=[1,2],
    #                                                   out_vector_file_template=None)
    
    if out_stream_network is not None:
        stream_network.write(out_stream_network)

    return(stream_network)
    


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create stream network level paths')
    parser.add_argument('-i','--in-stream-network', help='Input stream network', required=True)
    parser.add_argument('-o','--out-stream-network', help='Output stream network', required=True)
    
    args = vars(parser.parse_args())

    Derive_level_paths(**args)

