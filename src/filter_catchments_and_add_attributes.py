#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np

from utils.fim_enums import FIM_exit_codes
from utils.io import write_geodataframe
from utils.shared_variables import FIM_ID


def filter_catchments_and_add_attributes_in_memory(
    catchments_gdf: gpd.GeoDataFrame, flows_gdf: gpd.GeoDataFrame, wbd_gdf: gpd.GeoDataFrame, huc_code: str
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Filters catchments and flowlines and joins all network attributes entirely in RAM,
    incorporating dev stream cleaning and deduplication logic.
    """
    input_catchments = catchments_gdf.copy()
    input_flows = flows_gdf.copy()

    # Ensure HydroID string formatting for prefix matching
    if input_flows["HydroID"].dtype != "str":
        input_flows["HydroID"] = input_flows["HydroID"].astype(str)

    # 1. Filter segments within target HUC boundary using WBD
    wbd_matched = wbd_gdf[wbd_gdf["HUC8"].astype(str).str.contains(huc_code)]
    select_flows = tuple(map(str, map(int, wbd_matched[FIM_ID].dropna().unique())))

    if not select_flows:
        print(f"No matching FIM_ID found in WBD for HUC {huc_code}.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    output_flows = input_flows[input_flows["HydroID"].str.startswith(select_flows)].copy()

    if output_flows.empty:
        print("No relevant streams within HUC boundaries after prefix filter.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    # 2. Filter out tiny isolated stream artifacts (from dev)
    gdf_out = output_flows.copy()
    gdf_out["NextDownID"] = gdf_out["NextDownID"].astype(int)
    gdf_out["HydroID"] = gdf_out["HydroID"].astype(int)

    # Streams draining out of watershed / to a lake
    streams_to_lake = gdf_out[gdf_out["NextDownID"] == -1]

    # Streams with no upstream branch
    nextDownId_set = set(gdf_out["NextDownID"])
    streams_no_upstream = streams_to_lake[~streams_to_lake["HydroID"].isin(nextDownId_set)]

    # Identify super tiny streams (< 20m) with no upstream connectivity
    streams_no_upstream_tiny = streams_no_upstream[streams_no_upstream["LengthKm"] < 0.02]
    indices_to_remove = streams_no_upstream_tiny.index

    # Remove tiny disconnected streams
    output_flows_filtered = gdf_out.loc[gdf_out.index.difference(indices_to_remove)].copy()

    # Filter out streams smaller than 1 meter
    output_flows_filtered = output_flows_filtered[output_flows_filtered["LengthKm"] > 0.001]

    if output_flows_filtered.empty:
        print("There are no flowlines in the HUC after stream order and length filtering.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    output_flows_filtered["HydroID"] = output_flows_filtered["HydroID"].astype(int)

    # 3. Filter and merge attributes onto catchments
    if input_catchments["HydroID"].dtype != "int":
        input_catchments["HydroID"] = input_catchments["HydroID"].astype(int)

    # Left join network attributes from filtered flows onto catchments
    output_catchments = input_catchments.merge(
        output_flows_filtered.drop(columns=["geometry"], errors="ignore"), on="HydroID", how="inner"
    )

    if output_catchments.empty:
        print("There are no catchments remaining after flowline merge.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    # 4. Filter out smaller duplicate catchment features (from dev)
    hydroid_counts = np.bincount(output_catchments["HydroID"].values)
    duplicate_ids = np.where(hydroid_counts > 1)[0]

    if len(duplicate_ids) > 0:
        drop_indices = []
        for dp in duplicate_ids:
            idx_dup = np.where(output_catchments["HydroID"].values == dp)[0]
            areas = output_catchments.iloc[idx_dup].geometry.area.values
            # Keep the largest duplicate feature, mark smaller duplicates for deletion
            smaller_dup_indices = idx_dup[areas != np.amax(areas)]
            drop_indices.extend(smaller_dup_indices)

        if drop_indices:
            output_catchments = output_catchments.drop(output_catchments.index[drop_indices]).reset_index(
                drop=True
            )

    # 5. Calculate catchment area in sq km
    output_catchments["areasqkm"] = (output_catchments.geometry.area / 1e6).astype(np.float32)

    return output_catchments, output_flows_filtered


def filter_catchments_and_add_attributes(
    input_catchments_filename: str,
    input_flows_filename: str,
    output_catchments_filename: str,
    output_flows_filename: str,
    wbd_filename: str,
    huc_code: str,
) -> None:
    """File I/O wrapper supporting dev Parquet/Fiona inputs and writing filtered outputs."""
    # Read inputs using Fiona/Parquet as configured in dev for multiprocessing safety
    if str(input_catchments_filename).endswith(".parquet"):
        input_catchments = gpd.read_parquet(input_catchments_filename)
    else:
        input_catchments = gpd.read_file(input_catchments_filename, layer="catchments")

    if str(input_flows_filename).endswith(".parquet"):
        input_flows = gpd.read_parquet(input_flows_filename)
    else:
        input_flows = gpd.read_file(input_flows_filename)

    wbd = gpd.read_file(wbd_filename, engine="fiona")

    filt_catchments, filt_flows = filter_catchments_and_add_attributes_in_memory(
        catchments_gdf=input_catchments, flows_gdf=input_flows, wbd_gdf=wbd, huc_code=huc_code
    )

    try:
        write_geodataframe(filt_catchments, output_catchments_filename, index=False)
        write_geodataframe(filt_flows, output_flows_filename, index=False)
    except ValueError:
        print("Error writing filtered output layers.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter catchments and add attributes.")
    parser.add_argument("-i", "--input-catchments-filename", required=True, help="Input catchments path")
    parser.add_argument("-f", "--input-flows-filename", required=True, help="Input flows path")
    parser.add_argument("-c", "--output-catchments-filename", required=True, help="Output catchments path")
    parser.add_argument("-o", "--output-flows-filename", required=True, help="Output flows path")
    parser.add_argument("-w", "--wbd-filename", required=True, help="WBD HUC boundary file path")
    parser.add_argument("-u", "--huc-code", required=True, help="HUC unit code")

    args = parser.parse_args()

    filter_catchments_and_add_attributes(
        input_catchments_filename=args.input_catchments_filename,
        input_flows_filename=args.input_flows_filename,
        output_catchments_filename=args.output_catchments_filename,
        output_flows_filename=args.output_flows_filename,
        wbd_filename=args.wbd_filename,
        huc_code=args.huc_code,
    )
