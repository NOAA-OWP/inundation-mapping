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
    """Extracts catchlist attributes in strict input flows sequence."""
    stages_max_calc = stages_max + stages_interval
    stages = np.round(np.arange(stages_min, stages_max_calc, stages_interval), 4)

    f_df = flows.copy()
    c_df = catchments.copy()

    # Track exact row sequence of input flows
    f_df["_seq_id"] = np.arange(len(f_df))

    if "areasqkm" not in c_df.columns:
        c_df["areasqkm"] = c_df["geometry"].area / 10**6

    # Merge catchments attributes onto flows while preserving strictly tracked sequence
    merged = (
        f_df.merge(c_df[["HydroID", "areasqkm"]], on="HydroID", how="left")
        .sort_values("_seq_id")
        .reset_index(drop=True)
    )

    catchlist_df = pd.DataFrame(
        {
            "HydroID": merged["HydroID"].astype(int).values,
            "S0": merged["S0"].astype(float).values,
            "LengthKm": merged["LengthKm"].astype(float).values,
            "areasqkm": merged["areasqkm"].astype(float).values,
        }
    )

    return catchlist_df, stages


def write_stages_file(stages: np.ndarray, stages_filename: str) -> None:
    """Writes stage list to text file matching dev formatting."""
    with open(stages_filename, "w") as f:
        f.write("Stage\n")
        for stage in stages:
            f.write("{}\n".format(stage))


def write_catchlist_file(catchlist_df: pd.DataFrame, catchlist_filename: str) -> None:
    """Writes catchlist to text file matching dev formatting."""
    len_of_hydroIDs = len(catchlist_df)
    hydroIDs = catchlist_df["HydroID"].tolist()
    slopes = catchlist_df["S0"].tolist()
    lengthkm = catchlist_df["LengthKm"].tolist()
    areasqkm = catchlist_df["areasqkm"].tolist()

    with open(catchlist_filename, "w") as f:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="make_stages_and_catchlist.py")
    parser.add_argument("-f", "--flows-filename", help="flows-filename", required=True)
    parser.add_argument("-c", "--catchments-filename", help="catchments-filename", required=True)
    parser.add_argument("-s", "--stages-filename", help="stages-filename", required=True)
    parser.add_argument("-a", "--catchlist-filename", help="catchlist-filename", required=True)
    parser.add_argument("-m", "--stages-min", help="stages-min", required=True, type=float)
    parser.add_argument("-i", "--stages-interval", help="stages-interval", required=True, type=float)
    parser.add_argument("-t", "--stages-max", help="stages-max", required=True, type=float)

    args = vars(parser.parse_args())
    make_stages_and_catchlist(**args)
