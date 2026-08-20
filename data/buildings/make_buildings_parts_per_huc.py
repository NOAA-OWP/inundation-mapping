from __future__ import annotations

import argparse
import os
import re
import shutil
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from shapely.geometry import box

from src.utils.io import write_geodataframe
from src.utils.shared_functions import get_huc_vars, run_with_mp, setup_mp_file_logger


# Workflow overview:
# HUC8 boundaries are loaded once, grouped by regional CRS, and spatially indexed.
# For each state GeoParquet file, row-group bounding boxes are obtained from
# Parquet metadata without reading the building records. Those bounding boxes
# are used to preselect candidate HUC8s, after which each row group is read once
# and an exact spatial join is performed only against those candidate HUCs.
# The resulting buildings are then written as independent per-HUC Parquet parts.
# This metadata-first filtering avoids unnecessary row-group reads and limits
# expensive spatial operations to geographically relevant HUCs.

# Design note:
# Each Parquet row group is treated as one multiprocessing task. This keeps the
# row group as the natural unit of I/O: its building data is read once, then
# spatially joined against all candidate HUC8s and split into per-HUC outputs
# from memory. Compared with HUC-based tasks, this avoids rereading the same
# building data for multiple HUCs.


BUILDING_COLUMNS = ["UUID", "HEIGHT", "OCC_CLS", "SOURCE", "VAL_METHOD", "geometry"]

srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')


def make_building_parts_per_huc(
    states_buildings_dir: Path,
    current_preclip_directory: Path,
    out_dir: Path,
    states: Optional[List[str]] = None,
    number_jobs: int = 8,
):
    start_time = time.perf_counter()
    selected_states = {s.upper() for s in (states or [])}

    out_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = os.path.join(out_dir, "create_building_parts.log")

    file_logger = setup_mp_file_logger(log_file_path, logger_name="building_parts")
    file_logger.info('Started creating building parts...')

    for p in out_dir.glob("huc8_*"):
        if p.is_dir():
            shutil.rmtree(p)

    print("Loading HUC8 boundary polygons from preclipping directory...")
    hucs_by_crs = load_hucs_by_crs(
        current_preclip_directory=current_preclip_directory, file_logger=file_logger, number_jobs=number_jobs
    )
    print("Loaded HUC8 sets by CRS:", {k: len(v) for k, v in hucs_by_crs.items()})

    print('start reading states parquest files...')
    building_files = sorted(p for p in states_buildings_dir.glob("*.parquet") if p.is_file())

    if not building_files:
        raise RuntimeError(f"No building parquet files found in {states_buildings_dir}")

    tasks_args_list = build_row_group_tasks(
        building_files=building_files,
        selected_states=selected_states,
        hucs_by_crs=hucs_by_crs,
        out_dir=out_dir,
    )

    if not tasks_args_list:
        if selected_states:
            raise RuntimeError(
                f"No building parquet files matched requested states {sorted(selected_states)} in {states_buildings_dir}"
            )
        raise RuntimeError(f"No row-group tasks were created from parquet files in {states_buildings_dir}")

    print(f"Created {len(tasks_args_list)} row-group tasks.")

    mp_results = run_with_mp(
        task_function=process_row_group,
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
        return os.getenv('ALASKA_CRS')
    elif state == "GU":
        return os.getenv('GUAM_CRS')
    elif state == "AS":
        return os.getenv('AMERICAN_SAMOA_CRS')
    else:
        return os.getenv('DEFAULT_FIM_PROJECTION_CRS')


def load_single_huc_for_crs(huc8: str, huc_dir: Path, file_logger, screen_queue, task_id):
    try:
        gpkg = huc_dir / "wbd.gpkg"
        if not gpkg.exists():
            raise RuntimeError(f"Missing wbd.gpkg file for HUC8_{huc8}.")

        crs_key = get_huc_vars(huc8)['crs']

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
    Loads all HUC8 boundary polygons into memory, grouped by CRS (as strings),
    but assigns bucket/CRS purely from HUC8 directory name patterns (no CRS inspection).

    Bucketing rules (as requested):
      - Alaska: huc8 startswith "19"        -> ALASKA_CRS
      - Guam:   huc8 startswith "22010000"  -> GUAM_CRS
      - Samoa:  huc8 startswith "22030001"  -> AMERICAN_SAMOA_CRS
      - Else:   CONUS                       -> DEFAULT_FIM_PROJECTION_CRS

    Expects:
      huc_root/<HUC8>/wbd.gpkg
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
        get_huc_vars('00000000')['crs']: [],  # conus
        get_huc_vars('19000000')['crs']: [],  # alaska
        get_huc_vars('22010000')['crs']: [],  # guam
        get_huc_vars('22030001')['crs']: [],  # american samoa
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
    gdf = gpd.GeoDataFrame(
        df.drop(columns="geometry"), geometry=gpd.GeoSeries.from_wkb(df["geometry"]), crs=crs
    )
    return gdf


def get_row_group_bbox_column_index(pf: pq.ParquetFile) -> Dict[str, int]:
    """
    finds which Parquet columns correspond to the GeoParquet bounding-box fields
    and returns a dict mapping each bbox field to column index...
    typical result={"bbox.xmin":4, "bbox.ymin":5,...}
    """

    BBOX_STAT_PATHS = ("bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax")
    if pf.metadata.num_row_groups == 0:
        return {}

    typical_row_group_meta = pf.metadata.row_group(0)
    dict_result = {}
    for i in range(typical_row_group_meta.num_columns):
        column = typical_row_group_meta.column(i)
        column_path = column.path_in_schema
        if column_path in BBOX_STAT_PATHS:
            dict_result[column_path] = i

    return dict_result


def get_row_group_bbox(pf: pq.ParquetFile, rg: int, bbox_col_index: Dict[str, int]) -> Optional[tuple]:
    """Read a row group's spatial extent from GeoParquet bbox column stats, without reading its data."""
    row_group = pf.metadata.row_group(rg)
    stats = {}
    for path, idx in bbox_col_index.items():
        col_stats = row_group.column(idx).statistics
        if col_stats is None or not col_stats.has_min_max:
            return None
        stats[path] = col_stats

    # Each bbox field has separate min and max statistics across all geometries in the row group.
    # e.g., bbox.xmin.min is the smallest xmin among all geometries vs bbox.xmin.max is
    # the largest xmin value among all geometries in that row group.
    return (stats["bbox.xmin"].min, stats["bbox.ymin"].min, stats["bbox.xmax"].max, stats["bbox.ymax"].max)


def build_row_group_tasks(
    building_files: List[Path],
    selected_states: set[str],
    hucs_by_crs: Dict[str, gpd.GeoDataFrame],
    out_dir: Path,
) -> List[dict]:
    tasks_args_list: List[dict] = []
    for bp in building_files:
        state = bp.stem.split("_")[0].upper()
        if selected_states and state not in selected_states:
            continue
        pf = pq.ParquetFile(str(bp))

        # GeoParquet's nested "bbox" struct column is flattened into separate leaf
        # columns (bbox.xmin/ymin/xmax/ymax), and pyarrow's row-group stats API is
        # indexed positionally, not by name, so resolve each path -> index once per file.
        bbox_col_index = get_row_group_bbox_column_index(pf)
        if len(bbox_col_index) != 4:
            raise RuntimeError(
                f"Expected all 4 bbox statistic columns in {bp.name}, found {list(bbox_col_index)}"
            )

        hucs = hucs_by_crs.get(get_crs_of_state(state))
        if hucs is None:
            raise RuntimeError(f"No HUC boundaries found for CRS of state {state}")

        for rg in range(pf.num_row_groups):
            rg_bbox = get_row_group_bbox(pf, rg, bbox_col_index)
            if rg_bbox is None:
                raise RuntimeError(f"Missing bbox statistics for {bp.name}, row group {rg}")

            xmin, ymin, xmax, ymax = rg_bbox
            intersected_idx = hucs.sindex.query(box(xmin, ymin, xmax, ymax), predicate="intersects")
            if len(intersected_idx) == 0:
                # Row group's bbox touches no HUC
                continue

            # Candidate HUCs resolved here, once, from metadata alone. One task per row
            # group -- the atomic multiprocessing work unit.
            tasks_args_list.append(
                {
                    "item": {
                        "state": state,
                        "buildings_parquet": bp,
                        "row_group": rg,
                        "intersected_hucs": hucs.iloc[intersected_idx][["huc8", "geometry"]],
                    },
                    "out_dir": out_dir,
                    "task_id": f"{state}_rg{rg:05d}",
                }
            )

    return tasks_args_list


def process_row_group(item: dict, out_dir: Path, file_logger, screen_queue, task_id):
    try:
        state = item["state"]
        buildings_parquet = item["buildings_parquet"]
        rg = item["row_group"]
        intersected_hucs = item["intersected_hucs"]
        state_crs = get_crs_of_state(state)

        pf = pq.ParquetFile(str(buildings_parquet))

        screen_queue.put(f"[{task_id}] {state} | {buildings_parquet.name} | row_group={rg}")
        table = pf.read_row_group(rg, columns=BUILDING_COLUMNS)
        bg = arrow_rowgroup_to_gdf(table, crs=state_crs)
        if bg.empty:
            return 1, [True]

        # Candidate HUCs were already narrowed from parquet bbox metadata in
        # build_row_group_tasks, so go straight to the accurate sjoin.
        joined = gpd.sjoin(
            bg, intersected_hucs, how="inner", predicate="intersects"
        )  # keep buildings touching a HUC boundary (use within if needed)
        if joined.empty:
            return 1, [True]

        # Clean join artifacts
        joined = joined.drop(columns=["index_right", "geometry_right"], errors="ignore")

        # Write parts per HUC8
        for huc8, sub in joined.groupby("huc8"):
            sub = sub.copy()
            sub["huc8"] = sub["huc8"].astype("string")  # pandas StringDtype (not categorical)

            part_dir = out_dir / f"huc8_{huc8}"
            part_dir.mkdir(parents=True, exist_ok=True)
            part_path = part_dir / f"{state}_rg{rg:05d}.parquet"

            write_geodataframe(
                sub, part_path, index=False, use_dictionary=False  # important to avoid schema-merge conflicts
            )

        return 1, [True]

    except Exception as e:
        file_logger.error(f"❌ Exception in {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return 0, [False]


if __name__ == "__main__":
    # Example:
    # python foss_fim/data/buildings/make_buildings_parts_per_huc.py \
    #     -b data/inputs/fema/buildings/20260306/states_parquet/ \
    #     -p data/inputs/pre_clip_huc8/20260424/ \
    #     -o data/inputs/fema/buildings/20260306/ \
    #     -s 'IL'
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
        help="REQUIRED: parent folder path. Outputs are written to a huc_parts subfolder.",
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

    args = parser.parse_args()
    make_building_parts_per_huc(
        states_buildings_dir=Path(args.states_buildings_dir),
        current_preclip_directory=Path(args.current_preclip_directory),
        out_dir=Path(args.out_dir) / "huc_parts",
        states=args.state.split() if args.state else None,
        number_jobs=args.number_jobs,
    )
