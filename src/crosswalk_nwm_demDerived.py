#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import MultiLineString

import stream_branches as sb
from utils.shared_functions import getDriver
from utils.shared_variables import FIM_ID


gpd.options.io_engine = "pyogrio"


def Crosswalk_nwm_demDerived(
    streams,
    demDerived,
    wbd=None,
    node_prefix=None,
    sampling_size=None,
    crosswalk_outfile=None,
    demDerived_outfile=None,
    nwm_outfile=None,
    verbose=False,
):
    # load nwm streams
    if isinstance(streams, sb.StreamNetwork):
        pass
    elif isinstance(streams, str):
        streams = sb.StreamNetwork.from_file(streams)
    else:
        raise TypeError("For streams pass file path string or GeoDataFrame object")

    # load demDerived
    if isinstance(demDerived, sb.StreamNetwork):
        pass
    elif isinstance(demDerived, str):
        demDerived = sb.StreamNetwork.from_file(demDerived)
    else:
        raise TypeError("demDerived pass file path string or GeoDataFrame object")

    # clip streams
    if wbd is not None:
        streams = streams.clip(wbd, keep_geom_type=True, verbose=verbose)

    # build traversal to nwm
    streams = Add_traversal_to_NWM(streams, node_prefix=node_prefix, outfile=nwm_outfile, verbose=verbose)

    # create points for nwm and demDerived networks
    nwm_points = streams.explode_to_points(sampling_size=sampling_size, verbose=verbose)
    demDerived_points = demDerived.explode_to_points(sampling_size=sampling_size, verbose=verbose)

    # conflate points
    crosswalk_table = sb.StreamNetwork.conflate_points(
        demDerived_points,
        nwm_points,
        source_reach_id_attribute='HydroID',
        target_reach_id_attribute='ID',
        verbose=verbose,
    )

    # merge crosswalk table
    crosswalk_table = crosswalk_table.rename(columns={'ID': 'feature_id'})
    demDerived = demDerived.drop(columns='feature_id', errors='raise')
    demDerived['HydroID'] = demDerived['HydroID'].astype(int)
    demDerived = demDerived.merge(crosswalk_table, how='left', left_on='HydroID', right_index=True)

    if demDerived_outfile is not None:
        demDerived.write(demDerived_outfile, index=False, verbose=verbose)

    if crosswalk_outfile is not None:
        crosswalk_table.to_csv(crosswalk_outfile, index=True)

    # print(demDerived, crosswalk_table)
    return (demDerived, crosswalk_table)


def Add_traversal_to_NWM(streams, node_prefix=None, outfile=None, verbose=False):
    if isinstance(streams, sb.StreamNetwork):
        pass
    elif isinstance(streams, str):
        streams = sb.StreamNetwork.from_file(streams)
    else:
        raise TypeError("streams_file pass file path string or GeoDataFrame object")

    # remove multilinestrings if any
    anyMultiLineStrings = np.any(np.array([isinstance(g, MultiLineString) for g in streams.geometry]))
    if anyMultiLineStrings:
        streams = streams.dissolve_by_branch(
            branch_id_attribute='ID', attribute_excluded=None, values_excluded=None, verbose=verbose
        )

    # create stream node ids
    streams = streams.derive_nodes(
        toNode_attribute='To_Node',
        fromNode_attribute='From_Node',
        reach_id_attribute='ID',
        outlet_linestring_index=-1,
        node_prefix=node_prefix,
        max_node_digits=8,
        verbose=verbose,
    )
    # inlets and outlets
    streams = streams.derive_outlets(
        toNode_attribute='To_Node',
        fromNode_attribute='From_Node',
        outlets_attribute='outlet_id',
        verbose=verbose,
    )
    streams = streams.derive_inlets(
        toNode_attribute='To_Node',
        fromNode_attribute='From_Node',
        inlets_attribute='inlet_id',
        verbose=verbose,
    )

    # upstream and downstream dictionaries
    upstreams, downstreams = streams.make_up_and_downstream_dictionaries(
        reach_id_attribute='ID', toNode_attribute='To_Node', fromNode_attribute='From_Node', verbose=verbose
    )

    # derive arbolate sum
    streams = streams.get_arbolate_sum(
        arbolate_sum_attribute='arbolate_sum',
        inlets_attribute='inlet_id',
        reach_id_attribute='ID',
        length_conversion_factor_to_km=0.001,
        upstreams=upstreams,
        downstreams=downstreams,
        toNode_attribute='To_Node',
        fromNode_attribute='From_Node',
        verbose=verbose,
    )

    # derive levelpaths
    streams = streams.derive_stream_branches(
        toNode_attribute='To_Node',
        fromNode_attribute='From_Node',
        upstreams=upstreams,
        outlet_attribute='outlet_id',
        branch_id_attribute='levpa_id',
        reach_id_attribute='ID',
        comparison_attributes='order_',
        comparison_function=max,
        max_branch_id_digits=6,
        verbose=verbose,
    )

    # streams = streams.dissolve_by_branch(
    #     branch_id_attribute='levpa_id', attribute_excluded=None, values_excluded=None, verbose=verbose
    # )

    streams = streams.reset_index(drop=True)

    if outfile is not None:
        streams.write(outfile, index=False, verbose=verbose)

    return streams


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crosswalking')
    parser.add_argument('-n', '--nwm-streams', help='NWM Streams', required=True)
    parser.add_argument('-d', '--demDerived', help='demDerived Streams', required=True)
    parser.add_argument('-w', '--wbd', help='WBD File', required=False, default=None)
    parser.add_argument('-p', '--node-prefix', help='Node Prefix', required=False, default=None)
    parser.add_argument(
        '-a', '--sampling-size', help='Sample size for Points', required=False, default=None, type=int
    )
    parser.add_argument('-c', '--crosswalk-outfile', help='Crosswalk Out File', required=False, default=None)
    parser.add_argument(
        '-e', '--demDerived-outfile', help='demDerived Out File', required=False, default=None
    )
    parser.add_argument('-m', '--nwm-outfile', help='NWM Streams Out File', required=False, default=None)
    parser.add_argument('-v', '--verbose', help='Verbose', required=False, default=False, action='store_true')

    kwargs = vars(parser.parse_args())

    Crosswalk_nwm_demDerived(**kwargs)
