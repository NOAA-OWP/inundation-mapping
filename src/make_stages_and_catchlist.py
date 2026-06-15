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

    stages_max = stages_max + stages_interval
    stages = np.round(np.arange(stages_min, stages_max, stages_interval), 4)

    try:
        # areasqkm = catchments['areasqkm'].tolist()
        mergedflows_catchments = flows.merge(
            catchments[['HydroID', 'areasqkm']],
            on='HydroID',
            how='left',  # or 'inner' depending on what you want
        )
    except KeyError:
        areasqkm = (catchments['geometry'].area / 10**6).tolist()
        hydroIDs = catchments['HydroID'].tolist()
        areasqkm_df = pd.DataFrame({'HydroID': hydroIDs, 'areasqkm': areasqkm})
        mergedflows_catchments = flows.merge(areasqkm_df[['HydroID', 'areasqkm']], on='HydroID', how='left')

    hydroIDs = mergedflows_catchments['HydroID'].tolist()
    len_of_hydroIDs = len(hydroIDs)
    slopes = mergedflows_catchments['S0'].tolist()
    lengthkm = mergedflows_catchments['LengthKm'].tolist()
    areasqkm = mergedflows_catchments['areasqkm'].tolist()

    # hydroIDs = flows['HydroID'].tolist()
    # len_of_hydroIDs = len(hydroIDs)
    # slopes = flows['S0'].tolist()
    # lengthkm = flows['LengthKm'].tolist()

    del flows
    del catchments

    with open(stages_filename, 'w') as f:
        f.write("Stage\n")
        for stage in stages:
            f.write("{}\n".format(stage))

    with open(catchlist_filename, 'w') as f:
        f.write("{}\n".format(len_of_hydroIDs))
        for h, s, l, a in zip(hydroIDs, slopes, lengthkm, areasqkm):
            f.write("{} {} {} {}\n".format(h, s, l, a))


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
