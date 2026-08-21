#!/usr/bin/env python3

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd


def evaluate_crosswalk_in_memory(
    dem_reaches_gdf: gpd.GeoDataFrame,
    nwm_streams_gdf: gpd.GeoDataFrame,
    headwaters_gdf: gpd.GeoDataFrame = None,
    huc_unit: str = None,
    branch_id: str = None,
    output_csv_path: str = None,
) -> pd.DataFrame:
    """Evaluates crosswalk mapping between DEM-derived reaches and NWM streams in RAM.

    Parameters
    ----------
    dem_reaches_gdf : gpd.GeoDataFrame
        Crosswalked DEM-derived streams vector layer in memory.
    nwm_streams_gdf : gpd.GeoDataFrame
        NWM stream network vector layer in memory.
    headwaters_gdf : gpd.GeoDataFrame, optional
        NWM headwater points vector layer in memory.
    huc_unit : str, optional
        HUC unit identifier.
    branch_id : str, optional
        Branch identifier.
    output_csv_path : str, optional
        Optional path to write the crosswalk evaluation metrics CSV to disk.

    Returns
    -------
    pd.DataFrame
        DataFrame containing crosswalk match percentages and reach mapping statistics.
    """
    if dem_reaches_gdf.empty or nwm_streams_gdf.empty:
        print("Empty streams passed to evaluate_crosswalk_in_memory.")
        return pd.DataFrame()

    reaches = dem_reaches_gdf.copy()

    # Standardize column headers
    feature_col = "feature_id" if "feature_id" in reaches.columns else "featureID"

    total_reaches = len(reaches)
    matched_reaches = reaches[reaches[feature_col].notnull() & (reaches[feature_col] != 0)]
    num_matched = len(matched_reaches)

    match_rate = (num_matched / total_reaches * 100.0) if total_reaches > 0 else 0.0

    eval_data = [
        {
            "HUC": str(huc_unit) if huc_unit else "",
            "branch": str(branch_id) if branch_id else "",
            "total_dem_reaches": total_reaches,
            "matched_reaches": num_matched,
            "crosswalk_match_rate_pct": round(match_rate, 2),
        }
    ]

    eval_df = pd.DataFrame(eval_data)

    if output_csv_path:
        eval_df.to_csv(output_csv_path, index=False)

    return eval_df


def evaluate_crosswalk(
    dem_reaches_path: str,
    nwm_streams_path: str,
    output_csv: str,
    headwaters_path: str = None,
    huc_unit: str = None,
    branch_id: str = None,
):
    """File I/O CLI wrapper for backward compatibility."""
    reaches_gdf = gpd.read_file(dem_reaches_path) if Path(dem_reaches_path).is_file() else gpd.GeoDataFrame()
    nwm_gdf = gpd.read_file(nwm_streams_path) if Path(nwm_streams_path).is_file() else gpd.GeoDataFrame()
    hw_gdf = gpd.read_file(headwaters_path) if headwaters_path and Path(headwaters_path).is_file() else None

    evaluate_crosswalk_in_memory(
        dem_reaches_gdf=reaches_gdf,
        nwm_streams_gdf=nwm_gdf,
        headwaters_gdf=hw_gdf,
        huc_unit=huc_unit,
        branch_id=branch_id,
        output_csv_path=output_csv,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate Crosswalk Mapping Accuracy")
    parser.add_argument("-a", "--dem-reaches", required=True, help="DEM reaches GPKG path")
    parser.add_argument("-b", "--nwm-streams", required=True, help="NWM streams GPKG path")
    parser.add_argument("-c", "--output-csv", required=True, help="Output evaluation CSV path")
    parser.add_argument("-d", "--headwaters", required=False, help="Headwaters GPKG path")
    parser.add_argument("-u", "--huc-unit", required=False, help="HUC unit ID")
    parser.add_argument("-z", "--branch-id", required=False, help="Branch ID")

    args = vars(parser.parse_args())
    evaluate_crosswalk(
        dem_reaches_path=args["dem_reaches"],
        nwm_streams_path=args["nwm_streams"],
        output_csv=args["output_csv"],
        headwaters_path=args["headwaters"],
        huc_unit=args["huc_unit"],
        branch_id=args["branch_id"],
    )
