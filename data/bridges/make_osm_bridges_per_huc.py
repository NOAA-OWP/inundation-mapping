"""
Geofabrik-based replacement for the per-HUC portion of pull_osm_bridges_legacy.py (Overpass API,
now deprecated). For each HUC8: reads state bridge parquets that spatially overlap the
HUC, clips bridges to the HUC boundary, applies the dissolve-touching-lines step from
pull_osm_bridges_legacy.py, and writes huc_{HUC8}_osm_bridges.parquet.

Upstream: data/osm/pull_osm.py must have already written per-state bridge parquets to
  <osm_base>/states_parquet/bridges/<state>.parquet

Because one HUC can span multiple states, this script reads all state parquets
whose bounding box overlaps the HUC and concatenates the results before
dissolving — the same spatial-join pattern used by make_buildings_parts_per_huc.py.
The bbox/clip helper functions are shared with data/roads/make_osm_roads_per_huc.py and live in
data/osm/geofabrik_clip_utils.py rather than either domain folder.

The dissolve-touching-lines step is intentionally deferred to this script (not
done in pull_osm.py) because it must run in a projected CRS, which is only
known after assigning each feature to its HUC region.

Sample usage:
    python data/bridges/make_osm_bridges_per_huc.py \
        -b data/inputs/osm/20260702/states_parquet/bridges \
        -p data/inputs/pre_clip_huc8/20250218 \
        -o data/inputs/osm/bridges/20260702

    # process specific HUCs only
    python data/bridges/make_osm_bridges_per_huc.py \
        -b data/inputs/osm/20260702/states_parquet/bridges \
        -p data/inputs/pre_clip_huc8/20250218 \
        -o data/inputs/osm/bridges/20260702 \
        -lh '01010002 12090301'
"""

from __future__ import annotations

import argparse
import os
import re
import traceback
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from networkx import Graph, connected_components
from shapely.geometry import LineString

from data.osm.geofabrik_clip_utils import (
    clip_state_parquets_to_huc,
    compute_state_parquet_bounds_4326,
    find_overlapping_parquets,
)
from src.utils.io import write_geodataframe
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')


def find_touching_groups(gdf: gpd.GeoDataFrame) -> list:
    """
    Return connected components of touching geometries as a list of index sets,
    matching find_touching_groups() in pull_osm_bridges_legacy.py.
    """
    # Create a graph
    graph = Graph()

    # Add nodes for each geometry
    graph.add_nodes_from(gdf.index)

    # Create spatial index for efficient querying
    spatial_index = gdf.sindex

    # For each geometry, find touching geometries and add edges to the graph
    for idx, geometry in gdf.iterrows():
        possible_matches_index = list(spatial_index.intersection(geometry['geometry'].bounds))
        possible_matches = gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(geometry['geometry'])]

        for match_idx in precise_matches.index:
            if match_idx != idx:
                graph.add_edge(idx, match_idx)

    # Find connected components
    groups = list(connected_components(graph))
    return groups


def dissolve_touching_lines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Buffer-then-dissolve touching bridge line geometries in the projected CRS,
    then convert buffered polygons back to LineStrings — matching
    pull_osm_bridges_legacy.py lines 208-236.

    Must be called after the GeoDataFrame has been projected (parquets from
    pull_osm.py are already in the target projected CRS).
    """
    buffered = gdf.copy()
    buffered["geometry"] = buffered["geometry"].buffer(0.0001)

    # Find groups of touching geometries
    groups = find_touching_groups(buffered)

    # Dissolve each group separately
    warnings.filterwarnings("ignore")
    dissolved_groups = []
    for group in groups:
        grp_gdf = buffered.loc[list(group)]
        if not grp_gdf.empty:
            d = grp_gdf.dissolve()
            dissolved_groups.append(d.explode(index_parts=False))

    # Combine dissolved groups and reconstruct GeoDataFrame
    if dissolved_groups:
        result = gpd.GeoDataFrame(pd.concat(dissolved_groups, ignore_index=True), crs=buffered.crs)
    else:
        result = buffered.copy()

    # The buffer turns line geometries into thin polygons; recover LineStrings.
    result["geometry"] = result["geometry"].apply(
        lambda g: LineString(g.exterior.coords) if g.geom_type == "Polygon" else g
    )
    return result.copy()


def single_huc_job(
    huc8: str,
    huc_boundary_path: str,
    bridge_parquet_paths: List[Path],
    output_dir: str,
    file_logger,
    screen_queue,
    task_id,
):
    try:
        output_path = Path(output_dir) / f"huc_{huc8}_osm_bridges.parquet"
        if output_path.exists():
            output_path.unlink()

        huc_gdf = gpd.read_file(huc_boundary_path)

        bridges_gdf = clip_state_parquets_to_huc(bridge_parquet_paths, huc_gdf, dedupe_col="osmid")

        if bridges_gdf is None:
            msg = f"[{task_id}] No bridges within HUC {huc8}"
            file_logger.warning(msg)
            screen_queue.put(msg)
            return 1, [True]

        # Add HUC columns matching pull_osm_bridges_legacy.py lines 103-109.
        bridges_gdf["huc8"] = huc8
        bridges_gdf["huc10"] = ""

        final_gdf = dissolve_touching_lines(bridges_gdf)

        if "osmid" in final_gdf.columns:
            final_gdf["osmid"] = final_gdf["osmid"].astype(str)

        write_geodataframe(final_gdf, output_path, index=False)

        msg = f"[{task_id}] Wrote {len(final_gdf)} bridge features -> {output_path.name}"
        file_logger.info(msg)
        screen_queue.put(msg)
        return 1, [True]

    except Exception:
        file_logger.error(f"[{task_id}] Failed for HUC {huc8}")
        file_logger.error(traceback.format_exc())

        # Rename bad output to _bad.parquet so it can be filtered out later,
        # matching pull_osm_bridges_legacy.py error handling.
        try:
            bad_path = Path(output_dir) / f"huc_{huc8}_osm_bridges.parquet"
            if bad_path.exists():
                bad_path.rename(bad_path.with_stem(bad_path.stem + "_bad"))
        except Exception:
            pass

        return 0, [False]


def make_osm_bridges(
    bridges_parquet_dir: str, preclip_dir: str, output_dir: str, number_jobs: int = 4, lst_hucs: str = ""
) -> None:
    start_time = datetime.now(timezone.utc)
    bridges_dir = Path(bridges_parquet_dir)
    preclip_path = Path(preclip_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    log_path = out_path / f"make_osm_bridges_{start_time.strftime('%Y%m%d-%H%M')}.log"
    file_logger = setup_mp_file_logger(str(log_path), "make_osm_bridges")

    print("==================================")
    print("Starting OSM bridges per-HUC")
    file_logger.info("Starting OSM bridges per-HUC")
    file_logger.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")

    parquet_files = sorted(bridges_dir.glob("*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No bridge parquet files found in {bridges_dir}")

    # Load each state parquet once to get its bounding box in EPSG:4326.
    print(f"Computing bounds for {len(parquet_files)} state bridge parquets...")
    parquet_bounds_4326 = compute_state_parquet_bounds_4326(parquet_files)
    file_logger.info(f"Loaded bounds for {len(parquet_bounds_4326)} state parquets")

    huc_dirs = sorted(p for p in preclip_path.iterdir() if p.is_dir() and re.match(r"^\d{8}$", p.name))
    huc_numbers = [d.name for d in huc_dirs]

    if lst_hucs.strip():
        selected = set(lst_hucs.strip().split())
        huc_numbers = [h for h in huc_numbers if h in selected]

    file_logger.info(f"HUCs to process: {len(huc_numbers)}")
    print(f"HUCs to process: {len(huc_numbers)}")

    tasks_args_list = []
    for huc8 in huc_numbers:
        huc_dir = preclip_path / huc8
        wbd_path = huc_dir / "wbd.gpkg"
        if not wbd_path.exists():
            file_logger.warning(f"Missing wbd.gpkg for {huc8}, skipping")
            continue

        overlapping = find_overlapping_parquets(wbd_path, parquet_bounds_4326)
        if not overlapping:
            file_logger.warning(f"No state parquets overlap HUC {huc8}, skipping")
            continue

        tasks_args_list.append(
            {
                "huc8": huc8,
                "huc_boundary_path": str(wbd_path),
                "bridge_parquet_paths": overlapping,
                "output_dir": str(out_path),
            }
        )

    print(f"Processing {len(tasks_args_list)} HUC tasks")
    file_logger.info(f"Processing {len(tasks_args_list)} HUC tasks")

    mp_results = run_with_mp(
        task_function=single_huc_job,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=number_jobs,
        task_id_key="huc8",
        show_progress=True,
    )

    failed = [k for k, v in mp_results.items() if not v[0]]
    if not failed:
        file_logger.info("✅ All HUC tasks succeeded")
        print("✅ All HUC tasks succeeded")
    else:
        file_logger.error(f"❌ {len(failed)} failed: {failed}")
        print(f"❌ {len(failed)} failed: {failed}")

    end_time = datetime.now(timezone.utc)
    dur = str(end_time - start_time).split(".")[0]
    file_logger.info(f"Done. Duration: {dur}")
    print(f"Done. Duration: {dur}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create per-HUC8 bridge GeoParquet files from state-level OSM bridge parquets."
    )
    parser.add_argument(
        "-b",
        "--bridges_parquet_dir",
        required=True,
        help="REQUIRED: folder containing per-state bridge .parquet files (output of pull_osm.py)",
    )
    parser.add_argument(
        "-p",
        "--preclip_dir",
        required=True,
        help="REQUIRED: preclipping directory containing HUC8 subdirectories with wbd.gpkg files",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        required=True,
        help="REQUIRED: folder to write per-HUC huc_*_osm_bridges.parquet files",
    )
    parser.add_argument(
        "-j",
        "--number_jobs",
        required=False,
        default=4,
        type=int,
        help="OPTIONAL: number of parallel workers (default 4)",
    )
    parser.add_argument(
        "-lh",
        "--lst_hucs",
        required=False,
        default="",
        help="OPTIONAL: space-delimited HUC8 numbers to process (default: all)",
    )

    args = vars(parser.parse_args())
    make_osm_bridges(**args)
