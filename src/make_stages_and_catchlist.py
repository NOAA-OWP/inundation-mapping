#!/usr/bin/env python3

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


def make_stages_and_catchlist_in_memory(
    catchments_gdf: gpd.GeoDataFrame,
    flows_gdf: gpd.GeoDataFrame,
    stage_min_meters: float = 0.0,
    stage_interval_meters: float = 0.1,
    stage_max_meters: float = 20.0,
) -> tuple[pd.DataFrame, np.ndarray]:
    """Generates the 4-column catchlist DataFrame and 1D stage sequence in RAM.

    Returns
    -------
    tuple[pd.DataFrame, np.ndarray]
        - catchlist_df: DataFrame with columns ['HydroID', 'S0', 'LengthKm', 'Areasqkm']
        - stage_list: Array of rounded stage heights.
    """
    # 1. Merge catchment area with flowline attributes (S0, LengthKm) via HydroID
    merged = catchments_gdf[["HydroID", "Areasqkm", "geometry"]].merge(
        flows_gdf[["HydroID", "S0", "LengthKm"]], on="HydroID", how="inner"
    )

    # Clean and sort
    merged = merged.drop_duplicates(subset=["HydroID"]).sort_values("HydroID")

    # Reorder columns explicitly to match TauDEM catchhydrogeo expectations
    catchlist_df = merged[["HydroID", "S0", "LengthKm", "Areasqkm"]].copy()

    # Ensure integer type for HydroID
    catchlist_df["HydroID"] = catchlist_df["HydroID"].astype(np.int64)

    # 2. Generate stage sequence
    num_steps = int(np.round((stage_max_meters - stage_min_meters) / stage_interval_meters)) + 1
    stage_list = np.round(np.linspace(stage_min_meters, stage_max_meters, num_steps), 2)

    return catchlist_df, stage_list


def write_catchlist_file(catchlist_df: pd.DataFrame, output_path: str) -> None:
    """Writes the catch_list file with the required line-count header."""
    num_catchments = len(catchlist_df)

    with open(output_path, "w", encoding="utf-8") as f:
        # Line 1: Total number of catchments
        f.write(f"{num_catchments}\n")
        # Lines 2+: HydroID S0 LengthKm Areasqkm
        for _, row in catchlist_df.iterrows():
            f.write(f"{int(row['HydroID'])} {row['S0']} {row['LengthKm']} {row['Areasqkm']}\n")


def make_stages_and_catchlist(
    input_flows_path: str,
    input_catchments_path: str,
    stage_file_path: str,
    catch_list_path: str,
    stage_min_meters: float,
    stage_interval_meters: float,
    stage_max_meters: float,
) -> None:
    """File I/O wrapper for stages and catchlist generation."""
    catchments_gdf = gpd.read_file(input_catchments_path, layer="catchments")
    flows_gdf = gpd.read_file(input_flows_path)

    catchlist_df, stage_list = make_stages_and_catchlist_in_memory(
        catchments_gdf=catchments_gdf,
        flows_gdf=flows_gdf,
        stage_min_meters=stage_min_meters,
        stage_interval_meters=stage_interval_meters,
        stage_max_meters=stage_max_meters,
    )

    write_catchlist_file(catchlist_df, catch_list_path)
    np.savetxt(stage_file_path, stage_list, fmt="%.2f")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make stage and catchlist files in-memory.")
    parser.add_argument("-f", "--input-flows", required=True, help="Input flows GPKG")
    parser.add_argument("-c", "--input-catchments", required=True, help="Input catchments GPKG")
    parser.add_argument("-s", "--stage-file", required=True, help="Output stage text file")
    parser.add_argument("-a", "--catch-list", required=True, help="Output catch list text file")
    parser.add_argument("-m", "--stage-min", type=float, default=0.0, help="Minimum stage")
    parser.add_argument("-i", "--stage-interval", type=float, default=0.1, help="Stage interval")
    parser.add_argument("-t", "--stage-max", type=float, default=20.0, help="Maximum stage")

    args = parser.parse_args()
    make_stages_and_catchlist(
        input_flows_path=args.input_flows,
        input_catchments_path=args.input_catchments,
        stage_file_path=args.stage_file,
        catch_list_path=args.catch_list,
        stage_min_meters=args.stage_min,
        stage_interval_meters=args.stage_interval,
        stage_max_meters=args.stage_max,
    )
