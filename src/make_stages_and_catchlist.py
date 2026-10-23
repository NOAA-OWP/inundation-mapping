#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd


gpd.options.io_engine = "pyogrio"


def make_stages_and_catchlist(
    flows_filename,
    catchments_filename,
    stages_filename,
    catchlist_filename,
    stages_min,
    stages_interval,
    stages_max,
):
    flows = gpd.read_parquet(flows_filename)
    catchments = gpd.read_parquet(catchments_filename)

    # Reconcile flows and catchments hydroids
    flows = flows.merge(catchments[['HydroID']], on='HydroID', how='inner')
    catchments = catchments.merge(flows[['HydroID']], on='HydroID', how='inner')

    # print(f"type of flows is {type(flows)}")

    stages_max = stages_max + stages_interval
    stages = np.round(np.arange(stages_min, stages_max, stages_interval), 4)
    with open(stages_filename, 'w') as f:
            # f.write("Stage\n")
            for stage in stages:
                f.write("{}\n".format(stage))    

    try:
        # areasqkm = catchments['areasqkm'].tolist()
        mergedflows_catchments = flows.merge(
            catchments[['HydroID', 'areasqkm']],
            on='HydroID',
            how='left',  # or 'inner' depending on what you want
        )
    except KeyError:
        print(f"KeyError detected while building catchments for {catchlist_filename}")
        areasqkm = (catchments['geometry'].area / 10**6).tolist()
        hydroIDs = catchments['HydroID'].tolist()
        areasqkm_df = pd.DataFrame({'HydroID': hydroIDs, 'areasqkm': areasqkm})
        mergedflows_catchments = flows.merge(areasqkm_df[['HydroID', 'areasqkm']], on='HydroID', how='left')

    del flows
    del catchments

    catchments_df = pd.DataFrame(mergedflows_catchments[['HydroID', 'S0', 'LengthKm', 'areasqkm']])
    catchments_df.to_csv(catchlist_filename, header=False, index=False)


if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(description='make_stages_and_catchlist.py')
    parser.add_argument('-f', '--flows-filename', help='flows-filename', required=True)
    parser.add_argument('-c', '--catchments-filename', help='catchments-filename', required=True)
    parser.add_argument('-s', '--stages-filename', help='stages-filename', required=True)
    parser.add_argument('-a', '--catchlist-filename', help='catchlist-filename', required=True)
    parser.add_argument('-m', '--stages-min', help='stages-min', required=True, type=float)
    parser.add_argument('-i', '--stages-interval', help='stages-interval', required=True, type=float)
    parser.add_argument('-t', '--stages-max', help='stages-max', required=True, type=float)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    make_stages_and_catchlist(**args)
