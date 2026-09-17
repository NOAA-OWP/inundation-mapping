"""
Geofabrik-based replacement for the per-HUC portion of pull_osm_roads_legacy.py (Overpass API,
now deprecated). For each HUC8: reads state road parquets that spatially overlap the HUC,
clips roads to the HUC boundary, splits by NWM catchments, and writes
roads_{HUC8}.parquet — matching the output data of pull_osm_roads_legacy.py, written as GeoParquet.

Upstream: data/osm/pull_osm.py must have already written per-state road parquets to
  <osm_base>/states_parquet/roads/<state>.parquet

Because one HUC can span multiple states, this script reads all state parquets
whose bounding box overlaps the HUC, clips each to the HUC boundary, and
concatenates the results — the same spatial-join pattern used by
make_buildings_parts_per_huc.py. The bbox/clip helper functions are shared with
data/bridges/make_osm_bridges_per_huc.py and live in data/osm/geofabrik_clip_utils.py rather
than either domain folder.

Sample usage:
    python data/roads/make_osm_roads_per_huc.py \
        -r data/inputs/osm/20260702/states_parquet/roads \
        -p data/inputs/pre_clip_huc8/20250218 \
        -o data/inputs/osm/roads/20260702

    # process specific HUCs only
    python data/roads/make_osm_roads_per_huc.py \
        -r data/inputs/osm/20260702/states_parquet/roads \
        -p data/inputs/pre_clip_huc8/20250218 \
        -o data/inputs/osm/roads/20260702 \
        -lh '01010002 12090301'
"""

from __future__ import annotations

import argparse
import os
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import geopandas as gpd
from dotenv import load_dotenv

from data.osm.geofabrik_clip_utils import (
    clip_state_parquets_to_huc,
    compute_state_parquet_bounds_4326,
    find_overlapping_parquets,
)
from src.utils.io import write_geodataframe
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')


def split_roads(gdf_roads: gpd.GeoDataFrame, catchment_path: str, file_logger, screen_queue, task_id):
    """
    Overlay road segments on NWM catchment boundaries, adding catchment_id and
    osmid_catchid columns — matching split_roads() in pull_osm_roads_legacy.py.
    """
    huc_number = os.path.basename(os.path.dirname(catchment_path)) if catchment_path else ""

    if not catchment_path or not os.path.exists(catchment_path):
        gdf_out = gdf_roads.copy()
        gdf_out["osmid_catchid"] = gdf_out["osmid"].astype(str) + "_000"
        file_logger.info(f"no catchment file for {task_id}")
        screen_queue.put(f"no catchment file for {task_id}")
        return gdf_out

    # Alaska catchments are too numerous; assign dummy ID to avoid memory issues.
    if huc_number.startswith("19"):
        gdf_out = gdf_roads.copy()
        gdf_out["osmid_catchid"] = gdf_out["osmid"].astype(str) + "_000"
        file_logger.info(f"skip splitting roads for Alaska HUC {task_id}")
        screen_queue.put(f"skip splitting roads for Alaska HUC {task_id}")
        return gdf_out

    catchments = gpd.read_file(catchment_path)
    splitted = gpd.overlay(gdf_roads, catchments[["ID", "geometry"]], how="intersection")

    if not splitted.empty:
        splitted.rename(columns={"ID": "catchment_id"}, inplace=True)
        splitted["osmid_catchid"] = (
            splitted["osmid"].astype(str) + "_" + splitted["catchment_id"].astype(str)
        )
    else:
        splitted = gdf_roads.copy()
        splitted["osmid_catchid"] = splitted["osmid"].astype(str) + "_000"
        file_logger.info(f"no intersecting catchments for {task_id}")
        screen_queue.put(f"no intersecting catchments for {task_id}")

    return splitted


def single_huc_job(
    huc8: str,
    huc_boundary_path: str,
    split_boundary_path: str,
    road_parquet_paths: List[Path],
    output_dir: str,
    file_logger,
    screen_queue,
    task_id,
):
    try:
        output_path = Path(output_dir) / f"roads_{huc8}.parquet"
        huc_gdf = gpd.read_file(huc_boundary_path)

        roads_gdf = clip_state_parquets_to_huc(road_parquet_paths, huc_gdf, dedupe_col="osmid")

        if roads_gdf is None:
            msg = f"[{task_id}] No roads within HUC {huc8}"
            file_logger.warning(msg)
            screen_queue.put(msg)
            return 1, [True]

        roads_gdf["huc8"] = huc8

        splitted = split_roads(roads_gdf, split_boundary_path, file_logger, screen_queue, task_id)
        write_geodataframe(splitted, output_path, index=False)

        msg = f"[{task_id}] Wrote {len(splitted)} road segments -> {output_path.name}"
        file_logger.info(msg)
        screen_queue.put(msg)
        return 1, [True]

    except Exception:
        file_logger.error(f"[{task_id}] Failed for HUC {huc8}")
        file_logger.error(traceback.format_exc())
        return 0, [False]


def make_osm_roads(
    roads_parquet_dir: str,
    preclip_dir: str,
    output_dir: str,
    number_jobs: int = 4,
    lst_hucs: str = "",
) -> None:
    start_time = datetime.now(timezone.utc)
    roads_dir = Path(roads_parquet_dir)
    preclip_path = Path(preclip_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    log_path = out_path / f"make_osm_roads_{start_time.strftime('%Y%m%d-%H%M')}.log"
    file_logger = setup_mp_file_logger(str(log_path), "make_osm_roads")

    print("==================================")
    print("Starting OSM roads per-HUC")
    file_logger.info("Starting OSM roads per-HUC")
    file_logger.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")

    parquet_files = sorted(roads_dir.glob("*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No road parquet files found in {roads_dir}")

    # Load each state parquet once to get its bounding box in EPSG:4326. These
    # bboxes are used below to determine which state parquets overlap each HUC,
    # so that per-HUC jobs only read the parquets they actually need.
    print(f"Computing bounds for {len(parquet_files)} state road parquets...")
    parquet_bounds_4326 = compute_state_parquet_bounds_4326(parquet_files)
    file_logger.info(f"Loaded bounds for {len(parquet_bounds_4326)} state parquets")

    huc_dirs = sorted(
        p for p in preclip_path.iterdir()
        if p.is_dir() and re.match(r"^\d{8}$", p.name)
    )
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

        # Guam and Samoa have no NWM catchments.
        if huc8 in ("22010000", "22030001"):
            split_path = ""
        else:
            split_path = str(huc_dir / "nwm_catchments_proj_subset.gpkg")

        tasks_args_list.append({
            "huc8": huc8,
            "huc_boundary_path": str(wbd_path),
            "split_boundary_path": split_path,
            "road_parquet_paths": overlapping,
            "output_dir": str(out_path),
        })

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
        description="Create per-HUC8 road GeoParquet files from state-level OSM road parquets."
    )
    parser.add_argument(
        "-r", "--roads_parquet_dir",
        required=True,
        help="REQUIRED: folder containing per-state road .parquet files (output of pull_osm.py)",
    )
    parser.add_argument(
        "-p", "--preclip_dir",
        required=True,
        help="REQUIRED: preclipping directory containing HUC8 subdirectories with wbd.gpkg files",
    )
    parser.add_argument(
        "-o", "--output_dir",
        required=True,
        help="REQUIRED: folder to write per-HUC roads_*.parquet files",
    )
    parser.add_argument(
        "-j", "--number_jobs",
        required=False,
        default=4,
        type=int,
        help="OPTIONAL: number of parallel workers (default 4)",
    )
    parser.add_argument(
        "-lh", "--lst_hucs",
        required=False,
        default="",
        help="OPTIONAL: space-delimited HUC8 numbers to process (default: all)",
    )

    args = vars(parser.parse_args())
    make_osm_roads(**args)
