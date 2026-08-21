#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats


def min_hand_excluding_zero(values):
    """Convert to unmasked array and drop 0 and masked/nodata.

    return np.nan if on NoData Hand to be able to filter them later.
    """
    data = np.ma.filled(values.astype(float), np.nan)  # Convert masked to nan
    valid = data[(data != 0) & (~np.isnan(data))]
    return float(np.min(valid)) if valid.size > 0 else np.nan


def process_buildings_fimpact_in_memory(
    hand_grid_raster: str,
    buildings_gdf: gpd.GeoDataFrame,
    catchments_gdf: gpd.GeoDataFrame,
    output_path: str = None,
) -> pd.DataFrame:
    """Processes Building impacts within a HUC region using the exact original FIM logic in RAM."""
    if buildings_gdf.empty or catchments_gdf.empty:
        return pd.DataFrame()

    # Get branch_id from output path or default context
    branch_id = Path(output_path).parent.name if output_path else "0"

    # Read hand grid
    with rasterio.open(hand_grid_raster, "r") as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    bldgs_gdf = buildings_gdf.copy()
    catchments_df = catchments_gdf[["HydroID", "feature_id", "geometry"]].copy()

    # Standardize ID types
    catchments_df["feature_id"] = catchments_df["feature_id"].astype(int).astype(str)
    catchments_df["HydroID"] = catchments_df["HydroID"].astype(int).astype(str)

    # Split buildings by HAND catchment boundaries so each piece has the correct HydroID
    bldgs_gdf = gpd.overlay(bldgs_gdf, catchments_df, how="intersection")
    bldgs_gdf = bldgs_gdf.explode(index_parts=True).reset_index(drop=True)

    if not bldgs_gdf.empty:
        bldgs_gdf["branch"] = str(branch_id)

        # Call zonal_stats with the custom stat
        stats = zonal_stats(
            bldgs_gdf["geometry"],
            hand_grid_array,
            affine=hand_grid_profile["transform"],
            nodata=hand_grid_profile["nodata"],
            all_touched=True,
            stats=[],  # No built-in stats needed
            add_stats={"min_ex0": min_hand_excluding_zero},
        )

        bldgs_gdf.loc[:, "threshold_hand"] = [x.get("min_ex0") for x in stats]

        # Remove NaN threshold hands (e.g., leveed areas)
        bldgs_gdf = bldgs_gdf.dropna(subset=["threshold_hand"])

        if bldgs_gdf.empty:
            return pd.DataFrame()

        # Drop geometry column
        bldgs_gdf = bldgs_gdf.drop(columns="geometry")

        # Keep minimum HAND threshold per UUID/HydroID after geometric splitting
        group_cols = [c for c in ["UUID", "HydroID"] if c in bldgs_gdf.columns]
        if group_cols:
            min_idx = bldgs_gdf.groupby(group_cols)["threshold_hand"].idxmin()
            bldgs_gdf = bldgs_gdf.loc[min_idx]

        # String formatting for CSV output file
        cols_to_str = ["huc8", "HydroID", "feature_id", "branch"]
        for c in cols_to_str:
            if c in bldgs_gdf.columns:
                bldgs_gdf[c] = bldgs_gdf[c].astype(str)

        output_df = pd.DataFrame(bldgs_gdf)

        if output_path:
            output_df.to_csv(output_path, index=False)

        return output_df

    else:
        print(f"no split buildings for {branch_id}")
        return pd.DataFrame()


def process_buildings_fimpact(
    hand_grid_raster: str, buildings_polygons: str, catchments_path: str, output_path: str
) -> None:
    """CLI / File-based wrapper."""
    buildings_gdf = (
        gpd.read_file(buildings_polygons) if os.path.exists(buildings_polygons) else gpd.GeoDataFrame()
    )

    if os.path.exists(catchments_path):
        catchments_gdf = (
            gpd.read_file(catchments_path, layer="catchments")
            if "catchments" in gpd.list_layers(catchments_path).name.values
            else gpd.read_file(catchments_path)
        )
    else:
        catchments_gdf = gpd.GeoDataFrame()

    process_buildings_fimpact_in_memory(
        hand_grid_raster=hand_grid_raster,
        buildings_gdf=buildings_gdf,
        catchments_gdf=catchments_gdf,
        output_path=output_path,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process buildings FIMpact")

    parser.add_argument(
        "-g", "--hand_grid_raster", help="REQUIRED: Path for HAND grid raster file", required=True
    )

    parser.add_argument(
        "-r",
        "--buildings_polygons",
        help="REQUIRED: Path to a GPKG file containing the buildings polygons ",
        required=True,
    )

    parser.add_argument(
        "-c",
        "--catchments_path",
        help="REQUIRED: Path and file name of the HAND catchments geopackage",
        required=True,
    )

    parser.add_argument(
        "-o", "--output_path", help="REQUIRED: Path where the output csv file will be saved", required=True
    )

    args = vars(parser.parse_args())

    process_buildings_fimpact(**args)
