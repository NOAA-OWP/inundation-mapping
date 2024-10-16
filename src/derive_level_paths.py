#!/usr/bin/env python3

import argparse
import os
import sys

import geopandas as gpd

from stream_branches import StreamNetwork
from utils.fim_enums import FIM_exit_codes
from utils.shared_variables import HIGH_STREAM_DENSITY_HUCS, MEDIUM_HIGH_STREAM_DENSITY_HUCS


gpd.options.io_engine = "pyogrio"


def Derive_level_paths(
    in_stream_network,
    wbd,
    buffer_wbd_streams,
    out_stream_network,
    branch_id_attribute,
    huc_id,
    out_stream_network_dissolved=None,
    out_stream_network_dissolved_extended=None,
    headwaters_outfile=None,
    catchments=None,
    waterbodies=None,
    catchments_outfile=None,
    branch_inlets_outfile=None,
    toNode_attribute="To_Node",
    fromNode_attribute="From_Node",
    reach_id_attribute="HydroID",
    verbose=False,
):
    if verbose:
        print("Deriving level paths ...")

    # load file
    if verbose:
        print("Loading stream network ...")

    if os.path.exists(in_stream_network):
        stream_network = StreamNetwork.from_file(filename=in_stream_network)
    else:
        print("Sorry, no branches exist and processing can not continue. This could be an empty file.")
        sys.exit(FIM_exit_codes.UNIT_NO_BRANCHES.value)  # will send a 60 back

    # if there are no reaches at this point
    if len(stream_network) == 0:
        # This is technically not an error but we need to have it logged so the user know what
        # happened to it and we need the huc to not be included in future processing.
        # We need it to be not included in the fim_input.csv at the end of the unit processing.
        # Throw an exception with valid text.
        # This will show up in the non-zero exit codes and explain why an error.
        # Later, we can look at creating custom sys exit codes
        # raise UserWarning("Sorry, no branches exist and processing can not continue.
        # This could be an empty file.")
        print("Sorry, no streams exist and processing can not continue. This could be an empty file.")
        sys.exit(FIM_exit_codes.UNIT_NO_BRANCHES.value)  # will send a 60 back

    if huc_id in HIGH_STREAM_DENSITY_HUCS:
        print('HUC is in high density HUC list... removing additional stream segments.')
        stream_network = stream_network.exclude_attribute_values(
            branch_id_attribute="order_", values_excluded=[1, 2, 3, 4]
        )
    elif huc_id in MEDIUM_HIGH_STREAM_DENSITY_HUCS:
        print('HUC is in medium-high density HUC list... removing additional stream segments.')
        stream_network = stream_network.exclude_attribute_values(
            branch_id_attribute="order_", values_excluded=[1, 2, 3]
        )
    else:
        # values_exluded of 1 and 2 mean that we are dropping stream orders 1 and 2. We are leaving those
        # for branch zero.
        stream_network = stream_network.exclude_attribute_values(
            branch_id_attribute="order_", values_excluded=[1, 2]
        )

    # if there are no reaches at this point (due to filtering)
    if len(stream_network) == 0:
        print(
            "No branches exist but branch zero processing will continue (Exit 63)."
            "This could be due to stream order filtering."
        )
        # sys.exit(FIM_exit_codes.NO_BRANCH_LEVELPATHS_EXIST.value)  # will send a 63 back
        return

    inlets_attribute = 'inlet_id'
    outlets_attribute = 'outlet_id'
    outlet_linestring_index = -1

    # converts multi-linestrings to linestrings
    stream_network = stream_network.multilinestrings_to_linestrings()

    # derive nodes
    stream_network = stream_network.derive_nodes(
        toNode_attribute=toNode_attribute,
        fromNode_attribute=fromNode_attribute,
        reach_id_attribute=reach_id_attribute,
        outlet_linestring_index=outlet_linestring_index,
        node_prefix=None,
        verbose=verbose,
    )

    # derive outlets and inlets
    stream_network = stream_network.derive_outlets(
        toNode_attribute, fromNode_attribute, outlets_attribute=outlets_attribute, verbose=verbose
    )

    stream_network = stream_network.derive_inlets(
        toNode_attribute, fromNode_attribute, inlets_attribute=inlets_attribute, verbose=verbose
    )  # derive up and downstream networks
    upstreams, downstreams = stream_network.make_up_and_downstream_dictionaries(
        reach_id_attribute=reach_id_attribute,
        toNode_attribute=toNode_attribute,
        fromNode_attribute=fromNode_attribute,
        verbose=False,
    )

    # derive arbolate sum
    stream_network = stream_network.get_arbolate_sum(
        arbolate_sum_attribute="arbolate_sum",
        inlets_attribute=inlets_attribute,
        reach_id_attribute=reach_id_attribute,
        upstreams=upstreams,
        downstreams=downstreams,
        length_conversion_factor_to_km=0.001,
        verbose=verbose,
    )

    # derive stream branches
    stream_network = stream_network.derive_stream_branches(
        toNode_attribute=toNode_attribute,
        fromNode_attribute=fromNode_attribute,
        upstreams=upstreams,
        branch_id_attribute=branch_id_attribute,
        reach_id_attribute=reach_id_attribute,
        comparison_attributes=["arbolate_sum", "order_"],
        comparison_function=max,
        verbose=verbose,
    )

    # filter out streams without catchments
    if (catchments is not None) & (catchments_outfile is not None):
        catchments = gpd.read_file(catchments)

        stream_network = stream_network.remove_branches_without_catchments(
            catchments,
            reach_id_attribute=reach_id_attribute,
            branch_id_attribute=branch_id_attribute,
            reach_id_attribute_in_catchments=reach_id_attribute,
            verbose=verbose,
        )

        # subset which columns to merge
        stream_network_to_merge = stream_network.filter(
            items=[reach_id_attribute, inlets_attribute, outlets_attribute, branch_id_attribute]
        )

        catchments = catchments.merge(
            stream_network_to_merge, how="inner", left_on=reach_id_attribute, right_on=reach_id_attribute
        )

        catchments = catchments.reset_index(drop=True)

        catchments.to_file(catchments_outfile, index=False, driver="GPKG", engine='fiona')

    # derive headwaters
    if headwaters_outfile is not None:
        headwaters = stream_network.derive_headwater_points_with_inlets(
            inlets_attribute=inlets_attribute, outlet_linestring_index=outlet_linestring_index
        )
        # headwaters write
        headwaters.to_file(headwaters_outfile, index=False, driver="GPKG", engine='fiona')

    if out_stream_network is not None:
        if verbose:
            print("Writing stream branches ...")
        stream_network.write(out_stream_network, index=True)

    if out_stream_network_dissolved is not None:
        stream_network = stream_network.trim_branches_in_waterbodies(
            wbd=wbd, branch_id_attribute=branch_id_attribute, verbose=verbose
        )

        # dissolve by levelpath
        stream_network = stream_network.dissolve_by_branch(
            wbd=wbd,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=None,  # 'order_',
            values_excluded=None,  # [1,2],
            out_vector_files=out_stream_network_dissolved,
            out_extended_vector_files=out_stream_network_dissolved_extended,
            verbose=verbose,
        )

        stream_network = stream_network.remove_branches_in_waterbodies(
            waterbodies=waterbodies,
            out_vector_files=out_stream_network_dissolved,
            branch_id_attribute=branch_id_attribute,
            verbose=False,
        )
        stream_network = stream_network.select_branches_intersecting_huc(
            wbd=wbd,
            buffer_wbd_streams=buffer_wbd_streams,
            out_vector_files=out_stream_network_dissolved,
            verbose=False,
        )
    if stream_network.empty:
        print("Sorry, no streams exist and processing can not continue. This could be an empty file.")
        # sys.exit(FIM_exit_codes.UNIT_NO_BRANCHES.value)  # will send a 60 back
        return
    # else:
    #     return stream_network

    if branch_inlets_outfile is not None:
        branch_inlets = stream_network.derive_inlet_points_by_feature(
            branch_id_attribute=branch_id_attribute, outlet_linestring_index=outlet_linestring_index
        )

        if not branch_inlets.empty:
            branch_inlets.to_file(branch_inlets_outfile, index=False, driver="GPKG", engine='fiona')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create stream network level paths")
    parser.add_argument("-i", "--in-stream-network", help="Input stream network", required=True)
    parser.add_argument(
        "-s", "--buffer-wbd-streams", help="Input wbd buffer for stream network", required=True
    )
    parser.add_argument(
        "-wbd", "--wbd", help="Input watershed boundary (HUC) dataset", required=True, default=None
    )
    parser.add_argument(
        "-b", "--branch-id-attribute", help="Name of the branch attribute desired", required=True
    )
    parser.add_argument("-u", "--huc-id", help="Current HUC ID", required=False, default=None)
    parser.add_argument(
        "-r",
        "--reach-id-attribute",
        help="Reach ID attribute to use in source file",
        required=False,
        default="HydroID",
    )
    parser.add_argument(
        "-c", "--catchments", help="NWM catchments to append level path data to", required=False, default=None
    )
    parser.add_argument(
        "-t",
        "--catchments-outfile",
        help="NWM catchments outfile with appended level path data",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-w", "--waterbodies", help="NWM waterbodies to eliminate branches from", required=False, default=None
    )
    parser.add_argument(
        "-n", "--branch_inlets_outfile", help="Output level paths inlets", required=False, default=None
    )
    parser.add_argument(
        "-o", "--out-stream-network", help="Output stream network", required=False, default=None
    )
    parser.add_argument(
        "-e",
        "--headwaters-outfile",
        help="Output stream network headwater points",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-d",
        "--out-stream-network-dissolved",
        help="Dissolved output stream network",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-de",
        "--out-stream-network-dissolved-extended",
        help="Dissolved output stream network extended",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-v", "--verbose", help="Verbose output", required=False, default=False, action="store_true"
    )

    args = vars(parser.parse_args())

    Derive_level_paths(**args)
