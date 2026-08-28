#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd


gpd.options.io_engine = "pyogrio"


def make_stages_and_catchlist_in_memory(
    catchments: gpd.GeoDataFrame,
    flows: gpd.GeoDataFrame,
    stages_min: float = 0.0,
    stages_interval: float = 0.10,
    stages_max: float = 20.0,
) -> tuple[pd.DataFrame, np.ndarray]:
    """
    In-memory adaptation of dev make_stages_and_catchlist.
    Guarantees that the row order of 'flows' (starting with 15440004) is preserved 100%.
    """
    # 1. Calculate stages matching dev calculation
    stages_max_calc = stages_max + stages_interval
    stages = np.round(np.arange(stages_min, stages_max_calc, stages_interval), 4)

    # 2. Work on copies and enforce consistent integer HydroIDs
    flows = flows.copy()
    catchments = catchments.copy()

    flows['HydroID'] = flows['HydroID'].astype(int)
    catchments['HydroID'] = catchments['HydroID'].astype(int)

    # 3. Extract areasqkm with exact dev fallback
    try:
        catch_areas = catchments[['HydroID', 'areasqkm']].drop_duplicates(subset=['HydroID'])
    except KeyError:
        areas = (catchments['geometry'].area / 10**6).tolist()
        hids = catchments['HydroID'].tolist()
        catch_areas = pd.DataFrame({'HydroID': hids, 'areasqkm': areas}).drop_duplicates(subset=['HydroID'])

    # 4. Filter catchments to only HydroIDs existing in flows (reconcile)
    valid_hydroids = set(flows['HydroID'])
    catch_areas = catch_areas[catch_areas['HydroID'].isin(valid_hydroids)]

    # 5. Merge areas onto flows. 'left' merge guarantees 'flows' sequence is strictly maintained.
    mergedflows_catchments = flows.merge(catch_areas, on='HydroID', how='left')

    # 6. Extract final lists directly from the merged DataFrame
    hydroIDs = mergedflows_catchments['HydroID'].tolist()
    slopes = mergedflows_catchments['S0'].tolist()
    lengthkm = mergedflows_catchments['LengthKm'].tolist()
    areasqkm = mergedflows_catchments['areasqkm'].tolist()

    catchlist_df = pd.DataFrame(
        {'HydroID': hydroIDs, 'S0': slopes, 'LengthKm': lengthkm, 'areasqkm': areasqkm}
    )

    return catchlist_df, stages


def write_stages_file(stages: np.ndarray, stages_filename: str) -> None:
    """Writes stage list to text file matching dev formatting."""
    with open(stages_filename, 'w') as f:
        f.write("Stage\n")
        for stage in stages:
            f.write("{}\n".format(stage))


def write_catchlist_file(catchlist_df: pd.DataFrame, catchlist_filename: str) -> None:
    """Writes catchlist to text file matching dev formatting."""
    len_of_hydroIDs = len(catchlist_df)
    hydroIDs = catchlist_df['HydroID'].tolist()
    slopes = catchlist_df['S0'].tolist()
    lengthkm = catchlist_df['LengthKm'].tolist()
    areasqkm = catchlist_df['areasqkm'].tolist()

    with open(catchlist_filename, 'w') as f:
        f.write("{}\n".format(len_of_hydroIDs))
        for h, s, l, a in zip(hydroIDs, slopes, lengthkm, areasqkm):
            f.write("{} {} {} {}\n".format(h, s, l, a))


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

    catchlist_df, stages = make_stages_and_catchlist_in_memory(
        catchments=catchments,
        flows=flows,
        stages_min=stages_min,
        stages_interval=stages_interval,
        stages_max=stages_max,
    )

    write_stages_file(stages, stages_filename)
    write_catchlist_file(catchlist_df, catchlist_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='make_stages_and_catchlist.py')
    parser.add_argument('-f', '--flows-filename', help='flows-filename', required=True)
    parser.add_argument('-c', '--catchments-filename', help='catchments-filename', required=True)
    parser.add_argument('-s', '--stages-filename', help='stages-filename', required=True)
    parser.add_argument('-a', '--catchlist-filename', help='catchlist-filename', required=True)
    parser.add_argument('-m', '--stages-min', help='stages-min', required=True, type=float)
    parser.add_argument('-i', '--stages-interval', help='stages-interval', required=True, type=float)
    parser.add_argument('-t', '--stages-max', help='stages-max', required=True, type=float)

    args = vars(parser.parse_args())
    make_stages_and_catchlist(**args)
