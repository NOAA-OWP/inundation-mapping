#!/usr/bin/env python3

import argparse
import sys

import geopandas as gpd
import numpy as np

from utils.fim_enums import FIM_exit_codes
from utils.shared_variables import FIM_ID


gpd.options.io_engine = "pyogrio"


def filter_catchments_and_add_attributes(
    input_catchments_filename,
    input_flows_filename,
    output_catchments_filename,
    output_flows_filename,
    wbd_filename,
    huc_code,
):
    input_catchments = gpd.read_file(input_catchments_filename)
    wbd = gpd.read_file(wbd_filename)
    input_flows = gpd.read_file(input_flows_filename)

    # filter segments within huc boundary
    select_flows = tuple(map(str, map(int, wbd[wbd.HUC8.str.contains(huc_code)][FIM_ID])))

    del wbd

    if input_flows.HydroID.dtype != 'str':
        input_flows.HydroID = input_flows.HydroID.astype(str)
    output_flows = input_flows[input_flows.HydroID.str.startswith(select_flows)].copy()

    del input_flows

    gdf_out = output_flows.copy()

    # Finding streams that drain out of the watershed (to a lake)
    gdf_out['NextDownID'] = gdf_out['NextDownID'].astype(int)
    gdf_out['HydroID'] = gdf_out['HydroID'].astype(int)
    streams_to_lake = gdf_out[gdf_out['NextDownID'] == -1]

    # Finding streams that do NOT have upstream branch
    nextDownId_set = set(gdf_out['NextDownID'])
    streams_no_upstream = streams_to_lake[~streams_to_lake['HydroID'].isin(nextDownId_set)]

    # Finding those that they are super tiny
    streams_no_upstream_tiny = streams_no_upstream[streams_no_upstream['LengthKm'] < 0.02]

    # Get the index of streams_no_upstream_tiny
    indices_to_remove = streams_no_upstream_tiny.index

    # Remove streams that are in streams_no_upstream_tiny using index difference
    output_flows_filtered = gdf_out.loc[gdf_out.index.difference(indices_to_remove)]

    # Remove streams smaller than one meter
    output_flows_filtered = output_flows_filtered[output_flows_filtered['LengthKm'] > 0.001]

    if output_flows_filtered.HydroID.dtype != 'int':
        output_flows_filtered.HydroID = output_flows_filtered.HydroID.astype(int)

    if len(output_flows_filtered) > 0:
        # merges input flows attributes and filters hydroids
        if input_catchments.HydroID.dtype != 'int':
            input_catchments.HydroID = input_catchments.HydroID.astype(int)
        output_catchments = input_catchments.merge(
            output_flows_filtered.drop(['geometry'], axis=1), on='HydroID'
        )

        # filter out smaller duplicate features
        duplicateFeatures = np.where(np.bincount(output_catchments['HydroID']) > 1)[0]

        for dp in duplicateFeatures:
            indices_of_duplicate = np.where(output_catchments['HydroID'] == dp)[0]
            areas = output_catchments.iloc[indices_of_duplicate, :].geometry.area
            indices_of_smaller_duplicates = indices_of_duplicate[np.where(areas != np.amax(areas))[0]]
            output_catchments = output_catchments.drop(output_catchments.index[indices_of_smaller_duplicates])

        # add geometry column
        output_catchments['areasqkm'] = output_catchments.geometry.area / (1000**2)

        if not output_catchments.empty:
            try:
                output_catchments.to_file(
                    output_catchments_filename, driver="GPKG", index=False, engine='fiona'
                )
                output_flows_filtered.to_file(
                    output_flows_filename, driver="GPKG", index=False, engine='fiona'
                )
            except ValueError:
                # this is not an exception, but a custom exit code that can be trapped
                print("There are no flowlines in the HUC after stream order filtering.")
                sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # will send a 61 back
        else:
            # this is not an exception, but a custom exit code that can be trapped
            print("There are no flowlines in the HUC after stream order filtering.")
            sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # will send a 61 back

    else:
        # this is not an exception, but a custom exit code that can be trapped
        print("There are no flowlines in the HUC after stream order filtering.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # will send a 61 back

    del input_catchments


if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(description='filter_catchments_and_add_attributes.py')
    parser.add_argument('-i', '--input-catchments-filename', help='input-catchments-filename', required=True)
    parser.add_argument('-f', '--input-flows-filename', help='input-flows-filename', required=True)
    parser.add_argument(
        '-c', '--output-catchments-filename', help='output-catchments-filename', required=True
    )
    parser.add_argument('-o', '--output-flows-filename', help='output-flows-filename', required=True)
    parser.add_argument('-w', '--wbd-filename', help='wbd-filename', required=True)
    parser.add_argument('-u', '--huc-code', help='huc-code', required=True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    filter_catchments_and_add_attributes(**args)
