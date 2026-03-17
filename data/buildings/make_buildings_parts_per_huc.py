from __future__ import annotations

import argparse
import os
import re
import shutil
import time
import traceback
from itertools import zip_longest
from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from shapely.geometry import box

from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


# Your required building attributes (geometry always will be included)
BUILDING_COLUMNS = ["UUID", "HEIGHT", "OCC_CLS", "SOURCE", "VAL_METHOD"]

srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')

DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
GUAM_CRS = os.getenv('GUAM_CRS')
AMERICAN_SAMOA_CRS = os.getenv('AMERICAN_SAMOA_CRS')


def make_building_parts_per_huc(
    states_buildings_dir: Path,
    current_preclip_directory: Path,
    out_dir: Path,
    states: Optional[List[str]] = None,
    number_jobs: int = 8,
    row_group_chunk_size: int = 3,
):
    start_time = time.perf_counter()
    selected_states = {s.upper() for s in (states or [])}
    if row_group_chunk_size < 1:
        raise ValueError("row_group_chunk_size must be >= 1.")

    out_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = os.path.join(out_dir, "create_building_parts.log")

    file_logger = setup_mp_file_logger(log_file_path, logger_name="building_parts")
    file_logger.info('Started creating building parts...')

    for p in out_dir.glob("huc8_*"):
        if p.is_dir():
            shutil.rmtree(p)

    print("Loading HUC8 buffered polygons from preclipping directory...")
    hucs_by_crs = load_hucs_by_crs(
        current_preclip_directory=current_preclip_directory, file_logger=file_logger, number_jobs=number_jobs
    )
    print("Loaded HUC8 sets by CRS:", {k: len(v) for k, v in hucs_by_crs.items()})

    print('start reading states parquest files...')
    building_files = sorted(p for p in states_buildings_dir.glob("*.parquet") if p.is_file())

    if not building_files:
        raise RuntimeError(f"No building parquet files found in {states_buildings_dir}")

    tasks_args_list = build_mixed_row_group_tasks(
        building_files=building_files,
        selected_states=selected_states,
        hucs_by_crs=hucs_by_crs,
        out_dir=out_dir,
        row_group_chunk_size=row_group_chunk_size,
    )

    if not tasks_args_list:
        if selected_states:
            raise RuntimeError(
                f"No building parquet files matched requested states {sorted(selected_states)} in {states_buildings_dir}"
            )
        raise RuntimeError(f"No row-group tasks were created from parquet files in {states_buildings_dir}")

    print(f"Created {len(tasks_args_list)} mixed row-group tasks (chunk size={row_group_chunk_size}).")

    mp_results = run_with_mp(
        task_function=process_row_group_chunk,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=number_jobs,
        task_id_key="task_id",
        show_progress=True,
    )

    print('multiprocessing tasks finished!')
    failed_keys = [tid for tid, payload in mp_results.items() if not payload[0]]

    if not failed_keys:
        file_logger.info("✅ All multiprocessing tasks Succeeded")
        print("✅ All multiprocessing tasks Succeeded")
    else:
        file_logger.info(f"❌ {len(failed_keys)} failed:")
        print(f"❌ {len(failed_keys)} failed:")
        for tid in failed_keys:
            file_logger.info(f"  - {tid}")
            print(f"  - {tid}")

    print("✅ Done.")
    print("Outputs:", out_dir.resolve())
    total_seconds = int(round(time.perf_counter() - start_time))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    elapsed_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    file_logger.info(f"Total runtime: {elapsed_time}")
    print(f"Total runtime: {elapsed_time}")


def get_crs_of_state(state: str) -> str:
    state = state.upper()
    if state == "AK":
        return ALASKA_CRS
    elif state == "GU":
        return GUAM_CRS
    elif state == "AS":
        return AMERICAN_SAMOA_CRS
    else:
        return DEFAULT_FIM_PROJECTION_CRS


def load_single_huc_for_crs(huc8: str, huc_dir: Path, file_logger, screen_queue, task_id):
    try:
        gpkg = huc_dir / "wbd_buffered.gpkg"
        if not gpkg.exists():
            raise RuntimeError(f"Missing wbd_buffered.gpkg file for HUC8_{huc8}.")

        if huc8.startswith("19"):
            crs_key = ALASKA_CRS
        elif huc8.startswith("22010000"):
            crs_key = GUAM_CRS
        elif huc8.startswith("22030001"):
            crs_key = AMERICAN_SAMOA_CRS
        else:
            crs_key = DEFAULT_FIM_PROJECTION_CRS

        h = gpd.read_file(gpkg)
        geom = h.geometry.union_all()
        return 1, [{"huc8": huc8, "crs_key": crs_key, "geometry": geom}]
    except Exception as e:
        file_logger.error(f"❌ Exception in {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return -1, [None]


def load_hucs_by_crs(
    current_preclip_directory: Path, file_logger, number_jobs: int
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Loads all HUC8 buffered polygons into memory, grouped by CRS (as strings),
    but assigns bucket/CRS purely from HUC8 directory name patterns (no CRS inspection).

    Bucketing rules (as requested):
      - Alaska: huc8 startswith "19"        -> ALASKA_CRS
      - Guam:   huc8 startswith "22010000"  -> GUAM_CRS
      - Samoa:  huc8 startswith "22030001"  -> AMERICAN_SAMOA_CRS
      - Else:   CONUS                       -> DEFAULT_FIM_PROJECTION_CRS

    Expects:
      huc_root/<HUC8>/wbd_buffered.gpkg
    """
    if not current_preclip_directory.exists():
        raise RuntimeError(f"Preclip directory does not exist: {current_preclip_directory}")

    huc_dirs = sorted(
        p for p in current_preclip_directory.iterdir() if p.is_dir() and re.compile(r"^\d{8}$").match(p.name)
    )

    if not huc_dirs:
        raise RuntimeError(f"No HUC8 directories found under {current_preclip_directory}")

    # Buckets keyed by CRS string
    crs_to_hucs_dict: Dict[str, List[dict]] = {
        DEFAULT_FIM_PROJECTION_CRS: [],  # conus
        ALASKA_CRS: [],
        GUAM_CRS: [],
        AMERICAN_SAMOA_CRS: [],
    }

    huc_tasks = [{"huc8": d.name, "huc_dir": d} for d in huc_dirs]
    huc_load_number_jobs = max(1, number_jobs // 2)  # to reduce MP overhead because each task is light
    huc_results = run_with_mp(
        task_function=load_single_huc_for_crs,
        tasks_args_list=huc_tasks,
        file_logger=file_logger,
        max_workers=huc_load_number_jobs,
        task_id_key="huc8",
        show_progress=True,
    )

    for huc8, payload in huc_results.items():
        if not payload:
            raise RuntimeError(f"No payload returned for HUC8_{huc8}.")
        row = payload[0]
        if not row:
            raise RuntimeError(f"Invalid payload returned for HUC8_{huc8}.")
        crs_to_hucs_dict[row["crs_key"]].append({"huc8": row["huc8"], "geometry": row["geometry"]})

    # Convert each bucket to a GeoDataFrame and build its spatial index
    hucs_by_crs: Dict[str, gpd.GeoDataFrame] = {}
    for crs_key, hucs_items in crs_to_hucs_dict.items():
        if not hucs_items:
            continue
        gdf = gpd.GeoDataFrame(hucs_items, geometry="geometry", crs=crs_key)
        _ = (
            gdf.sindex
        )  # force GeoPandas to pre-build an STRtree spatial index, to avoid repeated construction
        hucs_by_crs[crs_key] = gdf

    return hucs_by_crs


def arrow_rowgroup_to_gdf(table, crs: str) -> gpd.GeoDataFrame:
    df = table.to_pandas()
    if "geometry" not in df:
        raise RuntimeError("Expected 'geometry' column in GeoParquet row group, but it was not found.")
    return gpd.GeoDataFrame(
        df.drop(columns="geometry"), geometry=gpd.GeoSeries.from_wkb(df["geometry"]), crs=crs
    )


def build_mixed_row_group_tasks(
    building_files: List[Path],
    selected_states: set[str],
    hucs_by_crs: Dict[str, gpd.GeoDataFrame],
    out_dir: Path,
    row_group_chunk_size: int,
) -> List[dict]:
    per_state_row_groups: List[List[dict]] = []
    for bp in building_files:
        state = bp.stem.split("_")[0].upper()
        if selected_states and state not in selected_states:
            continue
        pf = pq.ParquetFile(str(bp))
        rows = [{"state": state, "buildings_parquet": bp, "row_group": rg} for rg in range(pf.num_row_groups)]
        if rows:
            per_state_row_groups.append(rows)

    interleaved_items: List[dict] = []
    for row in zip_longest(*per_state_row_groups, fillvalue=None):
        for item in row:
            if item is not None:
                interleaved_items.append(item)

    tasks_args_list: List[dict] = []
    for i in range(0, len(interleaved_items), row_group_chunk_size):
        chunk = interleaved_items[i : i + row_group_chunk_size]
        tasks_args_list.append(
            {
                "task_id": f"chunk_{i // row_group_chunk_size:05d}",
                "items": chunk,
                "hucs_by_crs": hucs_by_crs,
                "out_dir": out_dir,
            }
        )
    return tasks_args_list


def process_row_group_chunk(
    items: List[dict],
    hucs_by_crs: Dict[str, gpd.GeoDataFrame],
    out_dir: Path,
    file_logger,
    screen_queue,
    task_id,
) -> None:
    try:
        pf_cache = {}
        if "geometry" not in BUILDING_COLUMNS:
            BUILDING_COLUMNS.append("geometry")

        for item in items:
            state = item["state"]
            buildings_parquet = item["buildings_parquet"]
            rg = item["row_group"]
            state_crs = get_crs_of_state(state)
            hucs = hucs_by_crs[state_crs]

            bp_key = str(buildings_parquet)
            if bp_key not in pf_cache:
                pf_cache[bp_key] = pq.ParquetFile(bp_key)
            pf = pf_cache[bp_key]

            screen_queue.put(f"[{task_id}] {state} | {buildings_parquet.name} | row_group={rg}")
            table = pf.read_row_group(rg, columns=BUILDING_COLUMNS)
            bg = arrow_rowgroup_to_gdf(table, crs=state_crs)
            if bg.empty:
                continue

            # Cheap bbox gate: limit intersected hucs for this chunk
            minx, miny, maxx, maxy = bg.total_bounds
            intersected_idx = hucs.sindex.query(box(minx, miny, maxx, maxy), predicate="intersects")
            if len(intersected_idx) == 0:  # if this row-group does not intersect any hucs
                screen_queue.put(f'No HUCs intersected row_group: {rg} of {state}')
                file_logger.info(f'No HUCs intersected row_group: {rg} of {state}')
                continue

            intersected_hucs = hucs.iloc[intersected_idx][["huc8", "geometry"]]

            # now do the accurate sjoin only for the intersected hucs
            joined = gpd.sjoin(
                bg, intersected_hucs, how="inner", predicate="intersects"
            )  # keep buildings touching a HUC boundary (use within if needed)
            if joined.empty:
                continue

            # Clean join artifacts
            joined = joined.drop(columns=["index_right", "geometry_right"], errors="ignore")

            # Write parts per HUC8
            for huc8, sub in joined.groupby("huc8"):
                sub = sub.copy()
                sub["huc8"] = sub["huc8"].astype("string")  # pandas StringDtype (not categorical)

                part_dir = out_dir / f"huc8_{huc8}"
                part_dir.mkdir(parents=True, exist_ok=True)
                part_path = part_dir / f"{state}_rg{rg:05d}.parquet"

                sub.to_parquet(
                    part_path,
                    index=False,
                    compression="zstd",
                    use_dictionary=False,  # important to avoid schema-merge conflicts
                )

        return 1, [True]

    except Exception as e:
        file_logger.error(f"❌ Exception in {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return 0, [False]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create per-HUC8 parquet parts from state-level building parquet files."
    )
    parser.add_argument(
        "-b",
        "--states_buildings_dir",
        help="REQUIRED: folder path containing per-state parquet building files.",
        required=True,
    )
    parser.add_argument(
        "-p",
        "--current_preclip_directory",
        help="REQUIRED: folder path to the most recent preclipping directory.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--out_dir",
        help="REQUIRED: folder path for output buildings_by_huc8 parquet parts.",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--state",
        help="OPTIONAL: space-delimited list of states/territories in quotes (e.g., 'TX CA').",
        required=False,
        default="",
    )
    parser.add_argument(
        "-j",
        "--number_jobs",
        help="OPTIONAL: Number of multiprocessing workers. Default is 8.",
        required=False,
        default=8,
        type=int,
    )
    parser.add_argument(
        "--row_group_chunk_size",
        help="OPTIONAL: Number of mixed row-groups per multiprocessing task. Default is 3.",
        required=False,
        default=3,
        type=int,
    )

    args = parser.parse_args()
    make_building_parts_per_huc(
        states_buildings_dir=Path(args.states_buildings_dir),
        current_preclip_directory=Path(args.current_preclip_directory),
        out_dir=Path(args.out_dir),
        states=args.state.split() if args.state else None,
        number_jobs=args.number_jobs,
        row_group_chunk_size=args.row_group_chunk_size,
    )
