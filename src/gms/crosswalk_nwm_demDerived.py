#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
import argparse
from utils.shared_functions import getDriver
from utils.shared_variables import FIM_ID
from gms import stream_branches as sb
from shapely.geometry import MultiLineString


def Crosswalk_nwm_demDerived(nwm_streams,demDerived,wbd=None,crosswalk=None,node_prefix=None,outfile=None,verbose=False):
    
    nwm_streams = Add_traversal_to_NWM(nwm_streams,node_prefix=node_prefix,
                                       outfile='/data/temp/continuity/nwm_streams_level_path.gpkg',
                                       verbose=verbose)
    
    # clip nwm_streams
    if wbd is not None:
        nwm_streams = nwm_streams.clip(wbd,keep_geom_type=True,verbose=verbose)

    # load demDerived
    if isinstance(demDerived,sb.StreamNetwork):
        pass
    elif isinstance(demDerived,str):
        demDerived = sb.StreamNetwork.from_file(demDerived)
    else:
        raise TypeError("demDerived pass file path string or GeoDataFrame object")

    demDerived = demDerived.conflate_branches(target_stream_network=nwm_streams,branch_id_attribute_left='levpa_id',
                                                branch_id_attribute_right='levpa_id',
                                                left_order_attribute='order_', right_order_attribute='order_',
                                                crosswalk_attribute='nwm_levpa_id',
                                                verbose=verbose)

    if outfile is not None:
        demDerived.write(outfile,index=False,verbose=verbose)



def Add_traversal_to_NWM(nwm_streams,node_prefix=None,outfile=None,verbose=False):
    
    if isinstance(nwm_streams,sb.StreamNetwork):
        pass
    elif isinstance(nwm_streams,str):
        nwm_streams = sb.StreamNetwork.from_file(nwm_streams)
    else:
        raise TypeError("nwm_streams_file pass file path string or GeoDataFrame object")

    # remove multilinestrings if any
    anyMultiLineStrings = np.any(np.array([isinstance(g, MultiLineString) for g in nwm_streams.geometry]))
    if anyMultiLineStrings:
        nwm_streams = nwm_streams.dissolve_by_branch(branch_id_attribute='ID', attribute_excluded=None,
                                                     values_excluded=None, verbose=verbose)


    # create stream node ids
    nwm_streams = nwm_streams.derive_nodes(toNode_attribute='To_Node',fromNode_attribute='From_Node',
                                           reach_id_attribute='ID',
                                           outlet_linestring_index=-1,node_prefix=node_prefix,
                                           max_node_digits=8,verbose=verbose)

    # inlets and outlets
    nwm_streams = nwm_streams.derive_outlets(toNode_attribute='To_Node',fromNode_attribute='From_Node',
                                             outlets_attribute='outlet_id',verbose=verbose)
    nwm_streams = nwm_streams.derive_inlets(toNode_attribute='To_Node',fromNode_attribute='From_Node',
                                            inlets_attribute='inlet_id',verbose=verbose)

    # upstream and downstream dictionaries
    upstreams,downstreams = nwm_streams.make_up_and_downstream_dictionaries(reach_id_attribute='ID',
                                                                            toNode_attribute='To_Node',
                                                                            fromNode_attribute='From_Node',
                                                                            verbose=verbose)

    # derive arbolate sum
    nwm_streams = nwm_streams.get_arbolate_sum(arbolate_sum_attribute='arbolate_sum',inlets_attribute='inlet_id',
                         reach_id_attribute='ID',length_conversion_factor_to_km = 0.001,
                         upstreams=upstreams, downstreams=downstreams,
                         toNode_attribute='To_Node',
                         fromNode_attribute='From_Node',
                         verbose=verbose
                        )

    # derive levelpaths
    nwm_streams = nwm_streams.derive_stream_branches(toNode_attribute='To_Node',
                               fromNode_attribute='From_Node',
                               upstreams=upstreams,
                               outlet_attribute='outlet_id',
                               branch_id_attribute='levpa_id',
                               reach_id_attribute='ID',
                               comparison_attributes='order_',
                               comparison_function=max,
                               max_branch_id_digits=6,
                               verbose=verbose)

    nwm_streams = nwm_streams.dissolve_by_branch(branch_id_attribute='levpa_id', attribute_excluded=None,
                                                 values_excluded=None, verbose=verbose)
    
    nwm_streams.reset_index(drop=True,inplace=True)

    if outfile is not None:
        nwm_streams.write(outfile,index=False,verbose=verbose)

    return(nwm_streams)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Crosswalking')
    parser.add_argument('-n','--nwm-streams', help='NWM Streams', required=True)
    parser.add_argument('-d','--demDerived', help='demDerived Streams', required=True)
    parser.add_argument('-w','--wbd', help='WBD File', required=False,default=None)
    parser.add_argument('-c','--crosswalk', help='Crosswalk File', required=False,default=None)
    parser.add_argument('-p','--node-prefix', help='Node Prefix', required=False,default=None)
    parser.add_argument('-o','--outfile', help='Streams Outfile', required=False, default=None)
    parser.add_argument('-v','--verbose', help='Verbose', required=False,default=False,action='store_true')

    kwargs = vars(parser.parse_args())

    Crosswalk_nwm_demDerived(**kwargs)
