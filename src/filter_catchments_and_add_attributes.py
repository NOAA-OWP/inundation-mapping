#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np

from utils.fim_enums import FIM_exit_codes
from utils.shared_variables import FIM_ID


def filter_catchments_and_add_attributes_in_memory(
    catchments_gdf: gpd.GeoDataFrame, flows_gdf: gpd.GeoDataFrame, wbd_gdf: gpd.GeoDataFrame, huc_code: str
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Filters catchments and flowlines and joins all network attributes entirely in RAM."""
    input_catchments = catchments_gdf.copy()
    input_flows = flows_gdf.copy()

    # Ensure string types for startswith prefix checks
    input_flows["HydroID_str"] = input_flows["HydroID"].astype(str)
    input_catchments["HydroID_str"] = input_catchments["HydroID"].astype(str)

    # 1. Determine valid select_flows FIM_IDs matching the target HUC
    wbd_matched = wbd_gdf[wbd_gdf["HUC8"].astype(str).str.contains(huc_code)]
    select_flows = tuple(map(str, map(int, wbd_matched[FIM_ID].dropna().unique())))

    if not select_flows:
        print(f"No matching FIM_ID found in WBD for HUC {huc_code}.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    # 2. Filter flows using FIM_ID prefix match
    output_flows = input_flows[input_flows["HydroID_str"].str.startswith(select_flows)].copy()

    if output_flows.empty:
        print("No relevant streams within HUC boundaries after prefix filter.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    # 3. Filter catchments to match the filtered flow HydroIDs
    output_catchments = input_catchments[
        input_catchments["HydroID_str"].isin(output_flows["HydroID_str"])
    ].copy()

    # Clean up temporary string column
    output_flows = output_flows.drop(columns=["HydroID_str"]).reset_index(drop=True)
    output_catchments = output_catchments.drop(columns=["HydroID_str"]).reset_index(drop=True)

    # 4. MERGE ALL STREAM ATTRIBUTES ONTO CATCHMENTS (NextDownID, LakeID, etc.)
    # Select key attribute columns present in flows to merge onto catchments
    flow_attrs = [
        col
        for col in ["HydroID", "NextDownID", "LakeID", "From_Node", "To_Node", "S0"]
        if col in output_flows.columns
    ]

    # Drop attributes from catchments if they already exist to avoid suffix collision (_x, _y)
    cols_to_drop = [col for col in flow_attrs if col in output_catchments.columns and col != "HydroID"]
    if cols_to_drop:
        output_catchments = output_catchments.drop(columns=cols_to_drop)

    # Left join network attributes from flows onto catchments via HydroID
    output_catchments = output_catchments.merge(output_flows[flow_attrs], on="HydroID", how="left")

    # Ensure clean defaults for missing values
    if "LakeID" in output_catchments.columns:
        output_catchments["LakeID"] = output_catchments["LakeID"].fillna(-999).astype(np.int64)
    if "NextDownID" in output_catchments.columns:
        output_catchments["NextDownID"] = output_catchments["NextDownID"].fillna("-1").astype(str)

    # 5. Calculate geometry attributes in-memory (Areas in sq km)
    output_catchments["Areasqkm"] = (output_catchments.geometry.area / 1e6).astype(np.float32)

    return output_catchments, output_flows


def filter_catchments_and_add_attributes(
    input_catchments_path: str,
    input_flows_path: str,
    output_catchments_path: str,
    output_flows_path: str,
    wbd_path: str,
    huc_code: str,
) -> None:
    """File I/O wrapper around the in-memory catchment filtering engine."""
    print("Loading datasets into RAM...")
    catchments_gdf = gpd.read_file(input_catchments_path, layer="catchments")
    flows_gdf = gpd.read_file(input_flows_path)
    wbd_gdf = gpd.read_file(wbd_path)

    filt_catchments, filt_flows = filter_catchments_and_add_attributes_in_memory(
        catchments_gdf=catchments_gdf, flows_gdf=flows_gdf, wbd_gdf=wbd_gdf, huc_code=huc_code
    )

    print("Writing filtered output layers...")
    out_c_path = Path(output_catchments_path)
    out_f_path = Path(output_flows_path)

    if out_c_path.exists():
        out_c_path.unlink()
    if out_f_path.exists():
        out_f_path.unlink()

    filt_catchments.to_file(out_c_path, layer="catchments", driver="GPKG", index=False)
    filt_flows.to_file(out_f_path, driver="GPKG", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter catchments and add attributes in-memory.")
    parser.add_argument("-i", "--input-catchments", required=True, help="Input catchments GPKG")
    parser.add_argument("-f", "--input-flows", required=True, help="Input flows GPKG")
    parser.add_argument("-c", "--output-catchments", required=True, help="Output catchments GPKG")
    parser.add_argument("-o", "--output-flows", required=True, help="Output flows GPKG")
    parser.add_argument("-w", "--wbd", required=True, help="WBD HUC boundary file")
    parser.add_argument("-u", "--huc-code", required=True, help="HUC unit code")

    args = parser.parse_args()
    filter_catchments_and_add_attributes(
        input_catchments_path=args.input_catchments,
        input_flows_path=args.input_flows,
        output_catchments_path=args.output_catchments,
        output_flows_path=args.output_flows,
        wbd_path=args.wbd,
        huc_code=args.huc_code,
    )
