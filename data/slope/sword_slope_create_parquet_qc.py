import argparse
import pathlib
import os
import pandas as pd
import geopandas as gpd


"""
River Network Slope QA/QC and Gap-Filling Script
------------------------------------------------
This script processes river segment slope data to identify and correct unrealistic values
based on user-defined thresholds. It is designed for hydrologic or hydraulic datasets
(e.g., NextGen HydroFabric) where each segment has a slope and upstream/downstream
connectivity defined by `id` and `toid` attributes.

Key Workflow:
1. Read an input CSV containing slope data (with `id`, `toid`, and `slope_iris_sword` columns).
2. Identify slope values outside acceptable thresholds (`lower_threshold`, `upper_threshold`).
3. Attempt to fill missing or invalid slope values using valid upstream and downstream
   segment slopes, based on a stream network defined in a separate GeoPackage (GPKG).
   - Only fill "gaps" where both an upstream and downstream valid slope are found.
   - Extend gap-filling through up to `max_extra_segments` downstream segments,
     limited by a cumulative length of `max_gap_length_m` meters.
4. Segments that cannot be filled are logged and dropped.
5. Outputs include:
   - A corrected Parquet file
   - A log file summarizing replacements and dropped segments
   - A CSV of dropped segment IDs

Usage Example:
    python slope_clean.py \
        -i data/temp/ryan/ciroh_sword_slope/NextGenHF_IRIS_v1.0.csv \
        -n data/inputs/nwm_hydrofabric/nwm_flows.gpkg \
        -o data/temp/ryan/ciroh_sword_slope/processed_filled \
        -k
"""

# Parameters
lower_threshold = 9.999e-7
upper_threshold = 0.5
max_extra_segments = 30  # max number of intermediate (missing) segments allowed
max_gap_length_m = 10000  # stop filling if gap > 10 km


def is_valid_slope(value):
    if value is None or pd.isna(value):
        return False
    return lower_threshold <= value <= upper_threshold


def process_network(csv_file, nwm_streams_gpkg_file, output_dir, gpkg_output):
    # Creat output files (using the input csv file name)
    base_name = pathlib.Path(csv_file).stem
    log_name = f"log_{base_name}.txt"
    log_file = output_dir + os.sep + log_name
    parquet_name = f"{base_name}.parquet"
    parquet_file = output_dir + os.sep + parquet_name
    dropped_name = "dropped_segments.csv"
    dropped_csv_file = output_dir + os.sep + dropped_name
    if gpkg_output:
        out_gpkg_name = f"{base_name}_processed.gpkg"
        out_gpkg_file = output_dir + os.sep + out_gpkg_name

    # Load CSV (original subset)
    df = pd.read_csv(csv_file)

    # normalize id/toid to integer (nullable Int64)
    df["id"] = df["id"].astype("Int64")
    df["toid"] = df["toid"].astype("Int64")

    # keep original slope for audit
    df["slope_original"] = df["slope_iris_sword"]

    # status column (will update)
    df["status"] = df["slope_iris_sword"].apply(
        lambda v: "valid_original" if is_valid_slope(v) else "invalid_original"
    )

    # dictionary of original rows keyed by id for export of dropped rows
    orig_rows_by_id = df.set_index("id").to_dict(orient="index")

    # present ids (initially rows in csv)
    present_ids = set(df["id"].dropna().astype(int).tolist())

    # Load full network nwm_flows GPKG
    print("Load full network nwm_flowline GPKG...")
    gdf = gpd.read_file(nwm_streams_gpkg_file)
    network_df = gdf[["ID", "to", "Length"]].copy()
    network_df["id"] = network_df["ID"].astype("Int64")
    network_df["toid"] = network_df["to"].astype("Int64")
    network_df["Length"] = pd.to_numeric(network_df["Length"], errors="coerce")

    # dict: id -> toid (downstream)
    print("Building network traversal lookups...")
    downstream_lookup = network_df.set_index("id")["toid"].to_dict()
    length_lookup = network_df.set_index("id")["Length"].to_dict()
    # dict: id -> list(upstream ids) across full network (for potential traversal if needed)
    # network_upstream_lookup = network_df.groupby("toid")["id"].apply(list).to_dict()

    # Slope lookup: start with values from IRIS-SWORD CSV
    # slope_lookup will be expanded with added segments during processing
    slope_lookup = {int(k): v for k, v in df.set_index("id")["slope_iris_sword"].dropna().to_dict().items()}

    # track which segments were added (missing in csv but added from gpkg and filled)
    added_nodes_info = {}  # node_id -> dict(info)

    # bookkeeping sets and counts
    filled_ids = set()  # ids we have filled/created
    replaced_count = 0
    added_filled_count = 0
    dropped_ids = []
    log_lines = []

    # Iterate upstream segments that are present in the csv and valid.
    # Starting set of upstream segments = present ids with valid slopes
    print("Performing fill processes...")
    present_valid_upstream = [
        nid for nid, row in df.set_index("id").iterrows() if is_valid_slope(row["slope_iris_sword"])
    ]

    # To avoid double-filling, process upstream candidates in order
    for upstream in present_valid_upstream:
        # skip if upstream already used to fill (avoid duplicate processing)
        if upstream in filled_ids:
            continue

        # ensure upstream slope is read from slope_lookup or df
        upstream_slope = slope_lookup.get(int(upstream), None)
        if not is_valid_slope(upstream_slope):
            # guard: shouldn't happen since we filtered present_valid_upstream
            continue

        # walk downstream collecting path of intermediate segments
        current = int(upstream)
        path = []  # each step: append next_id (could be present or missing in csv)
        steps = 0
        accumulated_length = 0.0
        while steps < (max_extra_segments + 1):  # +1 so we can find downstream present endpoint within limit
            next_seg = downstream_lookup.get(current)
            if next_seg is None or pd.isna(next_seg):
                break
            next_seg = int(next_seg)
            path.append(next_seg)
            seg_length = length_lookup.get(next_seg, 0.0) or 0.0
            accumulated_length += seg_length

            # if exceeded gap length, stop
            if accumulated_length > max_gap_length_m:
                log_lines.append(
                    f"SKIPPED: gap downstream from {upstream} exceeded {max_gap_length_m} m "
                    f"(accumulated {accumulated_length:.1f} m)"
                )
                break

            # if next_seg is present in original csv and has a valid slope -> we have endpoint
            if (next_seg in present_ids) and is_valid_slope(
                slope_lookup.get(
                    next_seg,
                    (
                        df.loc[df["id"] == next_seg, "slope_iris_sword"].values[0]
                        if next_seg in df["id"].values
                        else None
                    ),
                )
            ):
                # intermediate nodes are path excluding the final next_seg (endpoint)
                intermediate = path[:-1]
                if 1 <= len(intermediate) <= max_extra_segments:
                    # endpoints upstream and downstream:
                    downstream_endpoint = next_seg
                    downstream_slope = slope_lookup.get(downstream_endpoint, None)
                    # ensure downstream endpoint slope is valid
                    if not is_valid_slope(downstream_slope):
                        break

                    # compute fill value = mean of upstream & downstream endpoint slopes
                    fill_value = float(upstream_slope + downstream_slope) / 2.0

                    # Fill every intermediate node (present-but-invalid or missing)
                    for node in intermediate:
                        if node in present_ids:
                            # existing row in df, replace only if invalid
                            row_idx = df.index[df["id"] == node]
                            if len(row_idx) > 0:
                                ridx = row_idx[0]
                                old = df.at[ridx, "slope_iris_sword"]
                                if not is_valid_slope(old):
                                    df.at[ridx, "slope_iris_sword"] = fill_value
                                    df.at[ridx, "status"] = "replaced"
                                    slope_lookup[node] = fill_value
                                    replaced_count += 1
                                    log_lines.append(
                                        f"REPLACED: node {node} (original {old}) with {fill_value:.8f} "
                                        f"using upstream {upstream} (slope {upstream_slope:.8f}) and downstream {downstream_endpoint} (slope {downstream_slope:.8f}); intermediate={intermediate}"
                                    )
                                else:
                                    # already valid; nothing to do
                                    pass
                        else:
                            # missing in csv -> add & fill
                            slope_lookup[node] = fill_value
                            present_ids.add(node)
                            filled_ids.add(node)
                            added_filled_count += 1
                            added_nodes_info[node] = {
                                "added_from": "gpkg",
                                "filled_with": fill_value,
                                "upstream_endpoint": upstream,
                                "downstream_endpoint": downstream_endpoint,
                                "intermediate_chain": list(intermediate),
                            }
                            log_lines.append(
                                f"ADDED_AND_FILLED: node {node} filled with {fill_value:.8f} "
                                f"using upstream {upstream} (slope {upstream_slope:.8f}) and downstream {downstream_endpoint} (slope {downstream_slope:.8f}); intermediate={intermediate}"
                            )

                    # Mark intermediate nodes as filled so they are not reprocessed
                    for node in intermediate:
                        filled_ids.add(node)

                # whether we filled or not, we've reached a present endpoint - stop this downstream walk
                break

            # otherwise, continue downstream
            current = next_seg
            steps += 1

    # After attempting fills, any remaining IDs present in original CSV that still have invalid slopes are dropped
    for nid, row in df.set_index("id").iterrows():
        nid_i = int(nid)
        if not is_valid_slope(row["slope_iris_sword"]):
            dropped_ids.append(nid_i)
            log_lines.append(
                f"DROPPED: original id {nid_i} with slope {row['slope_iris_sword']} (no both-endpoints fill found)"
            )

    # remove dropped rows from df
    if dropped_ids:
        df = df[~df["id"].isin(dropped_ids)]

    # Append added nodes (those added from gpkg and filled) into the df
    if added_nodes_info:
        new_rows = []
        for node_id, info in added_nodes_info.items():
            toid = downstream_lookup.get(node_id, pd.NA)
            new_rows.append(
                {
                    "id": pd.Int64Dtype().type(node_id),
                    "toid": pd.Int64Dtype().type(int(toid)) if pd.notna(toid) else pd.NA,
                    "reach_id": pd.NA,
                    "slope_iris_sword": info["filled_with"],
                    "slope_source_iris_sword": "filled_from_endpoints",
                    "slope_original": pd.NA,
                    "status": "added_filled",
                }
            )
        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # Final counts and summary
    summary_lines = [
        "",
        "SUMMARY:",
        f"Original valid rows retained: {len(present_valid_upstream)} (initial candidates count)",
        f"Replaced (existing invalid -> filled): {replaced_count}",
        f"Added & filled (missing nodes added from gpkg and filled): {added_filled_count}",
        f"Dropped original invalid rows: {len(dropped_ids)}",
        f"Final total rows in output: {len(df)}",
    ]
    log_lines.extend(summary_lines)

    # Write log
    with open(log_file, "w") as f:
        f.write("\n".join(log_lines))

    # Save Parquet output
    df.to_parquet(parquet_file, engine="pyarrow", index=False)

    # Optional GeoPackage output (keep only flowlines with valid slopes)
    if gpkg_output:
        valid_ids = set(df["id"])
        valid_df = df[["id", "slope_iris_sword", "status", "slope_original"]].rename(columns={"id": "ID"})
        merged_gdf = gdf[gdf["ID"].isin(valid_ids)].merge(valid_df, on="ID", how="left")
        merged_gdf = merged_gdf[~merged_gdf["slope_iris_sword"].isna()]
        merged_gdf.to_file(out_gpkg_file, driver="GPKG")
        print(f"GeoPackage written to: {out_gpkg_file}")

    # Export dropped segments CSV for review (original rows)
    if dropped_ids:
        dropped_export_rows = []
        for did in dropped_ids:
            if did in orig_rows_by_id:
                row_info = orig_rows_by_id[did].copy()
                row_info["id"] = did
                dropped_export_rows.append(row_info)
        if dropped_export_rows:
            pd.DataFrame(dropped_export_rows).to_csv(dropped_csv_file, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process river slopes with thresholds and gap filling.")
    parser.add_argument("-i", "--input", required=True, help="Input CSV file")
    parser.add_argument("-n", "--network", required=True, help="NWM streams GPKG file")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument(
        "-k", "--gpkg_output", required=False, action='store_true', help="Optional output GeoPackage file"
    )

    args = parser.parse_args()

    process_network(
        csv_file=args.input,
        nwm_streams_gpkg_file=args.network,
        output_dir=args.output,
        gpkg_output=args.gpkg_output,
    )
