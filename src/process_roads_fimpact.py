#!/usr/bin/env python3

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from rasterstats import zonal_stats

from utils.io import write_geodataframe
from utils.spatial import sjoin


def min_hand_excluding_zero(values):
    """Convert to unmasked array and drop 0 and masked/nodata.

    return np.nan if on NoData Hand to be able to filter them later.
    """
    data = np.ma.filled(values.astype(float), np.nan)  # Convert masked to nan
    valid = data[(data != 0) & (~np.isnan(data))]
    return float(np.min(valid)) if valid.size > 0 else np.nan


def flow_lookup_in_memory(stages: tuple, hydro_id: int, hydrotable_df: pd.DataFrame) -> tuple:
    """In-memory discharge interpolation from hydroTable dataframe."""
    sub_df = hydrotable_df[hydrotable_df["HydroID"] == hydro_id]
    if sub_df.empty:
        return (np.nan, np.nan)

    # Standardize stage and discharge column names
    stage_col = "stage" if "stage" in sub_df.columns else "Stage"
    q_col = "discharge_cms" if "discharge_cms" in sub_df.columns else "Discharge (m3s-1)"

    sub_df = sub_df.sort_values(by=stage_col)

    stages_arr = sub_df[stage_col].to_numpy()
    q_arr = sub_df[q_col].to_numpy()

    return_flows = np.interp(stages, stages_arr, q_arr)
    return tuple(return_flows)


def process_roads_fimpact_in_memory(
    rem_raster_path: str,
    roads_gdf: gpd.GeoDataFrame,
    catchments_gdf: gpd.GeoDataFrame,
    hydrotable_df: pd.DataFrame,
    buffer_m: float = 1.5,
    threatened_percent: float = 0.75,
    output_gpkg_path: str = None,
) -> gpd.GeoDataFrame:
    """Calculates road inundation thresholds (HAND/REM depth & discharge) entirely in RAM."""
    if roads_gdf.empty or catchments_gdf.empty:
        print("Empty roads or catchments input passed to process_roads_fimpact_in_memory.")
        return gpd.GeoDataFrame()

    branch_id = Path(output_gpkg_path).parent.name if output_gpkg_path else "0"

    roads_df = roads_gdf.copy()
    if "catchment_id" in roads_df.columns:
        roads_df = roads_df.drop(columns=["catchment_id"])

    catchments_df = catchments_gdf[["HydroID", "feature_id", "order_", "geometry"]].copy()
    catchments_df["feature_id"] = catchments_df["feature_id"].astype(int).astype(str)
    catchments_df["HydroID"] = catchments_df["HydroID"].astype(int).astype(str)

    # Split roads based on HAND catchments
    roads_gdf_splitted = gpd.overlay(roads_df, catchments_df, how="intersection")
    roads_gdf_splitted = roads_gdf_splitted.explode(index_parts=True).reset_index(drop=True)

    if roads_gdf_splitted.empty:
        print(f"no splitted roads for {branch_id}")
        return gpd.GeoDataFrame()

    roads_gdf_splitted["branch"] = branch_id

    # Execute raster zonal statistics in RAM
    with rio.open(rem_raster_path) as rem_src:
        transform = rem_src.transform
        nodata_val = rem_src.nodata

        stats = zonal_stats(
            roads_gdf_splitted["geometry"],
            rem_raster_path,
            affine=transform,
            nodata=nodata_val,
            all_touched=True,
            stats=[],
            add_stats={"min_ex0": min_hand_excluding_zero},
        )

    roads_gdf_splitted.loc[:, "threshold_hand"] = [x.get("min_ex0") for x in stats]
    roads_gdf_splitted = roads_gdf_splitted.dropna(subset=["threshold_hand"]).copy()

    if roads_gdf_splitted.empty:
        return gpd.GeoDataFrame()

    # Deduplicate exploded segments keeping minimum threshold hand
    group_cols = [c for c in ["osmid_catchid", "HydroID"] if c in roads_gdf_splitted.columns]
    if group_cols:
        min_idx = roads_gdf_splitted.groupby(group_cols)["threshold_hand"].idxmin()
        roads_gdf_splitted = roads_gdf_splitted.loc[min_idx]

    # Compute threatened stage (75% threshold)
    roads_gdf_splitted["threshold_hand_75"] = roads_gdf_splitted["threshold_hand"] * threatened_percent

    # Lookup discharges from hydrotable dataframe
    if not hydrotable_df.empty and "HydroID" in roads_gdf_splitted.columns:
        flows = roads_gdf_splitted.apply(
            lambda row: flow_lookup_in_memory(
                (row["threshold_hand"], row["threshold_hand_75"]), row["HydroID"], hydrotable_df
            ),
            axis=1,
            result_type="expand",
        )
        roads_gdf_splitted[["threshold_discharge", "threshold_discharge75"]] = flows

        # Unit conversions
        roads_gdf_splitted["threshold_hand_ft"] = roads_gdf_splitted["threshold_hand"] * 3.28084
        roads_gdf_splitted["threshold_hand_75_ft"] = roads_gdf_splitted["threshold_hand_75"] * 3.28084
        roads_gdf_splitted["threshold_discharge_cfs"] = roads_gdf_splitted["threshold_discharge"] * 35.3147
        roads_gdf_splitted["threshold_discharge_75_cfs"] = (
            roads_gdf_splitted["threshold_discharge75"] * 35.3147
        )

    # String column formatting
    cols_to_str = ["osmid", "huc8", "HydroID", "feature_id", "order_", "branch"]
    for col in cols_to_str:
        if col in roads_gdf_splitted.columns:
            roads_gdf_splitted[col] = roads_gdf_splitted[col].astype(str)

    # Persist output if requested
    if output_gpkg_path and not roads_gdf_splitted.empty:
        if str(output_gpkg_path).endswith(".csv"):
            df_out = roads_gdf_splitted.drop(columns=["geometry"], errors="ignore")
            df_out.to_csv(output_gpkg_path, index=False)
        else:
            write_geodataframe(roads_gdf_splitted, output_gpkg_path, index=False)

    return roads_gdf_splitted


def process_roads_fimpact(
    rem_raster_path: str,
    roads_gpkg: str,
    catchments_gpkg: str,
    hydrotable_csv: str,
    output_gpkg: str,
    buffer_m: float = 1.5,
):
    """File I/O wrapper supporting both Parquet and GPKG catchment inputs."""
    roads_gdf = gpd.read_file(roads_gpkg) if Path(roads_gpkg).is_file() else gpd.GeoDataFrame()

    if Path(catchments_gpkg).is_file():
        if str(catchments_gpkg).endswith(".parquet"):
            catchments_gdf = gpd.read_parquet(
                catchments_gpkg, columns=["HydroID", "feature_id", "order_", "geometry"]
            )
        else:
            catchments_gdf = gpd.read_file(catchments_gpkg)
    else:
        catchments_gdf = gpd.GeoDataFrame()

    hydrotable_df = pd.read_csv(hydrotable_csv) if Path(hydrotable_csv).is_file() else pd.DataFrame()

    process_roads_fimpact_in_memory(
        rem_raster_path=rem_raster_path,
        roads_gdf=roads_gdf,
        catchments_gdf=catchments_gdf,
        hydrotable_df=hydrotable_df,
        buffer_m=buffer_m,
        output_gpkg_path=output_gpkg,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Road Flood Impact Analysis")
    parser.add_argument("-g", "--rem-raster", required=True, help="REM raster path")
    parser.add_argument("-r", "--roads-gpkg", required=True, help="Roads GPKG path")
    parser.add_argument("-p", "--catchments-gpkg", required=True, help="Catchments GPKG or Parquet path")
    parser.add_argument("-t", "--hydrotable-csv", required=True, help="HydroTable CSV path")
    parser.add_argument("-o", "--output-gpkg", required=True, help="Output impact path (GPKG or CSV)")
    parser.add_argument("-b", "--buffer-m", default=1.5, type=float, help="Buffer meters")

    args = vars(parser.parse_args())
    process_roads_fimpact(
        rem_raster_path=args["rem_raster"],
        roads_gpkg=args["roads_gpkg"],
        catchments_gpkg=args["catchments_gpkg"],
        hydrotable_csv=args["hydrotable_csv"],
        output_gpkg=args["output_gpkg"],
        buffer_m=args["buffer_m"],
    )
