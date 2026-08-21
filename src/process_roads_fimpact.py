#!/usr/bin/env python3

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from rasterstats import zonal_stats

from utils.spatial import sjoin


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
    """Calculates road inundation thresholds (HAND/REM depth & discharge) entirely in RAM.

    Parameters
    ----------
    rem_raster_path : str
        Path to the branch REM raster file.
    roads_gdf : gpd.GeoDataFrame
        Clipped road network vector layer in memory.
    catchments_gdf : gpd.GeoDataFrame
        Branch catchments vector layer in memory.
    hydrotable_df : pd.DataFrame
        Rating curve hydroTable dataframe in memory.
    buffer_m : float
        Buffer distance for road geometries during raster zonal statistics.
    threatened_percent : float
        Fraction of threshold HAND depth to define threatened stage.
    output_gpkg_path : str, optional
        Optional path to persist the resulting road impact layer to disk.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame of road segments enriched with flood thresholds and flow properties.
    """
    if roads_gdf.empty or catchments_gdf.empty:
        print("Empty roads or catchments input passed to process_roads_fimpact_in_memory.")
        return gpd.GeoDataFrame()

    roads_df = roads_gdf.copy()
    roads_df["centroid_geometry"] = roads_df.geometry.centroid

    # Buffer roads for zonal statistics
    buffered_geom = roads_df.geometry.buffer(buffer_m, resolution=buffer_m)

    # Execute raster zonal statistics in RAM
    with rio.open(rem_raster_path) as rem_src:
        transform = rem_src.transform
        nodata_val = rem_src.nodata

        stats = zonal_stats(
            buffered_geom,
            rem_raster_path,
            affine=transform,
            stats="median",
            nodata=nodata_val,
            all_touched=True,
        )

    roads_df["threshold_hand"] = pd.to_numeric([x.get("median") for x in stats], errors="coerce")

    # Filter out unimpacted roads
    roads_df = roads_df.loc[roads_df["threshold_hand"] > 0].copy()

    if roads_df.empty:
        return gpd.GeoDataFrame()

    # Switch geometry back to centroids for spatial join to catchments
    roads_df["geometry"] = roads_df["centroid_geometry"]
    roads_df = roads_df.drop(columns=["centroid_geometry"], errors="ignore")

    # Clean prior join keys and perform spatial join
    roads_df = roads_df.drop(columns=["index_right"], errors="ignore")
    catchments_clean = catchments_gdf.drop(columns=["index_right"], errors="ignore")

    roads_df = sjoin(roads_df, catchments_clean[["HydroID", "feature_id", "order_", "geometry"]], how="inner")
    roads_df = roads_df.drop(columns=["index_right"], errors="ignore")

    # Compute threatened stage (75% threshold)
    roads_df["threshold_hand_75"] = roads_df["threshold_hand"] * threatened_percent

    # Lookup discharges from hydrotable dataframe
    if not hydrotable_df.empty and "HydroID" in roads_df.columns:
        flows = roads_df.apply(
            lambda row: flow_lookup_in_memory(
                (row["threshold_hand"], row["threshold_hand_75"]), row["HydroID"], hydrotable_df
            ),
            axis=1,
            result_type="expand",
        )
        roads_df[["threshold_discharge", "threshold_discharge75"]] = flows

        # Unit conversions
        roads_df["threshold_hand_ft"] = roads_df["threshold_hand"] * 3.28084
        roads_df["threshold_hand_75_ft"] = roads_df["threshold_hand_75"] * 3.28084
        roads_df["threshold_discharge_cfs"] = roads_df["threshold_discharge"] * 35.3147
        roads_df["threshold_discharge_75_cfs"] = roads_df["threshold_discharge75"] * 35.3147

    # Persist output if requested
    if output_gpkg_path and not roads_df.empty:
        roads_df.to_file(output_gpkg_path, driver="GPKG", index=False)

    return roads_df


def process_roads_fimpact(
    rem_raster_path: str,
    roads_gpkg: str,
    catchments_gpkg: str,
    hydrotable_csv: str,
    output_gpkg: str,
    buffer_m: float = 1.5,
):
    """File I/O CLI wrapper for backward compatibility."""
    roads_gdf = gpd.read_file(roads_gpkg) if Path(roads_gpkg).is_file() else gpd.GeoDataFrame()
    catchments_gdf = (
        gpd.read_file(catchments_gpkg, layer="catchments")
        if Path(catchments_gpkg).is_file()
        else gpd.read_file(catchments_gpkg)
    )
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
    parser.add_argument("-p", "--catchments-gpkg", required=True, help="Catchments GPKG path")
    parser.add_argument("-t", "--hydrotable-csv", required=True, help="HydroTable CSV path")
    parser.add_argument("-o", "--output-gpkg", required=True, help="Output impact GPKG path")
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
