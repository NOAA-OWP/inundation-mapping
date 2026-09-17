"""
Download per-state OSM road and bridge data from Geofabrik and convert to
per-state GeoParquet files, using osmium-tool for tag-based extraction.

This replaces the per-HUC Overpass API calls in pull_osm_roads_legacy.py and
pull_osm_bridges_legacy.py, which are unreliable from AWS-hosted environments
(Overpass blocks AWS/Azure IP ranges as anti-abuse policy).

Pipeline:
    For each state:
        1. Download <state>-latest.osm.pbf from Geofabrik  (deleted after step 3)
        2. osmium tags-filter + export → roads GeoDataFrame  → roads parquet
        3. osmium tags-filter + export → bridges GeoDataFrame → bridges parquet

Downstream: data/roads/make_osm_roads_per_huc.py and data/bridges/make_osm_bridges_per_huc.py (modelled
on make_buildings_parts_per_huc.py) read these parquet files and spatial-join them into
per-HUC outputs. Their shared bbox/clip helper functions live in this folder's
geofabrik_clip_utils.py rather than either domain folder.

Requires: osmium-tool (apt package) installed in the environment.

Sample usage:
    # all states
    python data/osm/pull_osm.py -o data/inputs/osm/20260702

    # specific states only
    python data/osm/pull_osm.py -o data/inputs/osm/20260702 -s 'delaware michigan'
"""

import argparse
import logging
import os
import shutil
import subprocess
import traceback
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from dotenv import load_dotenv

from src.utils.io import write_geodataframe
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
GUAM_CRS = os.getenv('GUAM_CRS')
AMERICAN_SAMOA_CRS = os.getenv('AMERICAN_SAMOA_CRS')


GEOFABRIK_BASE_URL = "https://download.geofabrik.de/north-america/us"

# Territory slugs whose Geofabrik files live outside north-america/us/.
# Geofabrik does not publish standalone "guam" or "american-samoa" extracts — those slugs
# 302-redirect to the bare homepage instead of a file. Guam is covered by Geofabrik's
# Micronesia extract (which also includes Palau, the Marshall Islands, and the Northern
# Mariana Islands); American Samoa is covered by Geofabrik's Samoa extract (which also
# includes the independent nation of Samoa). The extra out-of-scope territory data is
# harmless here since downstream processing clips to HUC boundaries.
# Verify these URLs if Geofabrik reorganises their layout.
GEOFABRIK_URL_OVERRIDES = {
    "guam": "https://download.geofabrik.de/australia-oceania/micronesia-latest.osm.pbf",
    "american-samoa": "https://download.geofabrik.de/australia-oceania/samoa-latest.osm.pbf",
}

# All supported state/territory slugs (Geofabrik naming convention).
ALL_STATES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "district-of-columbia", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada", "new-hampshire",
    "new-jersey", "new-mexico", "new-york", "north-carolina", "north-dakota",
    "ohio", "oklahoma", "oregon", "pennsylvania", "rhode-island",
    "south-carolina", "south-dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west-virginia", "wisconsin", "wyoming",
    "guam", "american-samoa",
]

# Highway tag set matching pull_osm_roads_legacy.py's current (unmerged dev-add-residental-roads)
# 5-category set, plus residential:
#   - motorway through tertiary: all ways regardless of name.
#   - residential: named ways only (filtered in _write_roads_parquet). Unnamed residential ways
#     (cul-de-sacs, dead-ends, private drives, minor unnamed connectors) are too numerous and
#     rarely relevant at the HUC scale; named ones are the actual through-streets worth including
#     in FIMpact.
#   - "unclassified" is intentionally excluded entirely (not just unnamed ones) — despite the
#     name, it's not "unknown"; in OSM it sits one step above residential, covering minor public
#     connector roads between settlements too small to be tertiary.
ROAD_HIGHWAY_VALUES = [
    "motorway",
    "trunk",
    "primary",
    "secondary",
    "tertiary",
    "residential",
]

# Abandoned/demolished/proposed bridge types to exclude, matching pull_osm_bridges_legacy.py.
UNWANTED_BRIDGE_TYPES = {
    "highway-razed", "highway-proposed", "highway-abandoned",
    "highway-destroyed", "highway-dismantled", "highway-demolished",
    "railway-razed", "railway-proposed", "railway-abandoned",
    "railway-destroyed", "railway-dismantled", "railway-demolished",
}


def _crs_for_state(state: str) -> str:
    if state == "alaska":
        return ALASKA_CRS
    if state == "guam":
        return GUAM_CRS
    if state == "american-samoa":
        return AMERICAN_SAMOA_CRS
    return DEFAULT_FIM_PROJECTION_CRS


def _pbf_url(state: str) -> str:
    if state in GEOFABRIK_URL_OVERRIDES:
        return GEOFABRIK_URL_OVERRIDES[state]
    return f"{GEOFABRIK_BASE_URL}/{state}-latest.osm.pbf"


def _download_pbf(state: str, pbf_dir: Path, file_logger, screen_queue) -> Path:
    dest = pbf_dir / f"{state}-latest.osm.pbf"
    url = _pbf_url(state)
    msg = f"Downloading {url}"
    file_logger.info(msg)
    screen_queue.put(msg)

    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    size_mb = dest.stat().st_size / 1_048_576
    msg = f"Downloaded {dest.name} ({size_mb:.1f} MB)"
    file_logger.info(msg)
    screen_queue.put(msg)
    return dest


def _run_osmium(cmd: list, file_logger) -> None:
    """
    Run an osmium subprocess with output captured (not inherited from the parent
    terminal) and stderr logged on failure.

    Without capture_output, osmium's own progress bar (shown by default whenever
    stdout/stderr are a TTY) writes directly to the shared terminal from every
    worker process, corrupting tqdm's in-place redraw of the "Processing tasks" bar.
    --no-progress additionally suppresses that bar outright.
    """
    file_logger.info(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        file_logger.error(f"Command failed: {' '.join(cmd)}\n{e.stderr}")
        raise


def _osmium_filter_export(pbf_path: Path, filter_expr: str, geojson_path: Path, file_logger, screen_queue):
    """
    Run osmium tags-filter then osmium export, writing GeoJSON to geojson_path.

    GeoJSON (not Parquet/GPKG) is required here: `osmium export -f` only supports
    geojson/geojsonseq/pg/spaten/text — it doesn't wrap GDAL/OGR, so it can't write
    Parquet or GPKG directly.
    GeoJSON is read by geopandas right after this call and converted to Parquet there.
    """
    filtered_pbf = geojson_path.with_suffix(".filtered.osm.pbf")

    filter_cmd = [
        "osmium", "tags-filter", str(pbf_path),
        filter_expr,
        "-o", str(filtered_pbf), "--overwrite", "--no-progress",
    ]
    _run_osmium(filter_cmd, file_logger)

    # -a id: osmium export's --attributes defaults to "none" (confirmed via `osmium export --help`
    # and by inspecting the actual output), so without this flag we would not have any id.
    # osmium names this output field "@id" (confirmed by testing), matching KEEP_COLS/rename below.
    export_cmd = [
        "osmium", "export", str(filtered_pbf),
        "-o", str(geojson_path), "-f", "geojson", "-a", "id", "--overwrite", "--no-progress",
    ]
    _run_osmium(export_cmd, file_logger)

    filtered_pbf.unlink(missing_ok=True)


def _write_roads_parquet(
    state: str,
    pbf_path: Path,
    roads_dir: Path,
    file_logger,
    screen_queue,
):
    geojson_path = pbf_path.parent / f"{state}_roads.geojson"
    # we get all desired roads and will later exclude bridges segments from them -- see below.
    highway_filter = "w/highway=" + ",".join(ROAD_HIGHWAY_VALUES)
    _osmium_filter_export(pbf_path, highway_filter, geojson_path, file_logger, screen_queue)

    gdf = gpd.read_file(geojson_path)
    geojson_path.unlink(missing_ok=True)

    gdf = gdf[gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])]

    # Allowlist column drop — avoids GeoPackage/pyogrio write failures from
    # arbitrary OSM tag columns (fixme, note, etc.).
    KEEP_COLS = {"@id", "highway", "name", "bridge"}
    gdf = gdf.drop(
        columns=[c for c in gdf.columns if c not in KEEP_COLS and c != gdf.geometry.name],
        errors="ignore",
    )
    if "@id" in gdf.columns:
        gdf = gdf.rename(columns={"@id": "osmid"})
        gdf["osmid"] = gdf["osmid"].astype(str)

    # Exclude bridge segments from roads.
    # Done here in pandas rather than in the osmium tags-filter step above: unlike Overpass QL's
    # single-clause ["highway"~"..."][!"bridge"] (used by pull_osm_roads_legacy.py), osmium tags-filter's
    # expressions are OR'd for inclusion only — there's no one-shot "has tag A AND lacks tag B".
    # Replicating that AND-NOT at the osmium level would need a second chained tags-filter pass
    # (select by highway, then --invert-match to strip bridge=*). Since we already have to load
    # this GeoJSON into a GeoDataFrame anyway, dropping bridge rows here is a free one-liner 
    # instead of an extra osmium subprocess call.
    if "bridge" in gdf.columns:
        gdf = gdf[gdf["bridge"].isna()].drop(columns=["bridge"])

    # Named-only for residential: keep all motorway/trunk/primary/secondary/tertiary ways
    # regardless of name, but for residential keep only those with a non-empty name tag
    # (unnamed ones are the numerous, rarely-relevant cul-de-sacs/private drives/minor
    # connectors described in the ROAD_HIGHWAY_VALUES comment above).
    if "highway" in gdf.columns:
        is_named_only_class = gdf["highway"].isin(["residential"])
        if is_named_only_class.any():
            has_name = (
                gdf["name"].notna() & (gdf["name"] != "")
                if "name" in gdf.columns
                else pd.Series(False, index=gdf.index)
            )
            gdf = gdf[~is_named_only_class | has_name]

    target_crs = _crs_for_state(state)
    gdf = gdf.to_crs(target_crs).copy()

    out_path = roads_dir / f"{state}_roads.parquet"
    write_geodataframe(gdf, out_path, index=False)

    msg = f"[roads] {state}: wrote {len(gdf)} features -> {out_path.name}"
    file_logger.info(msg)
    screen_queue.put(msg)


def _write_bridges_parquet(state: str, pbf_path: Path, bridges_dir: Path, file_logger, screen_queue):
    geojson_path = pbf_path.parent / f"{state}_bridges.geojson"
    _osmium_filter_export(pbf_path, "w/bridge", geojson_path, file_logger, screen_queue)

    gdf = gpd.read_file(geojson_path)
    geojson_path.unlink(missing_ok=True)

    gdf = gdf[gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()

    if gdf.empty:
        msg = f"[bridges] {state}: no linear bridge geometries found, skipping"
        file_logger.warning(msg)
        screen_queue.put(msg)
        return

    if "@id" in gdf.columns:
        gdf = gdf.rename(columns={"@id": "osmid"})

    # Ensure highway and railway columns exist before building bridge_type,
    # matching pull_osm_bridges_legacy.py lines 113-118.
    if "highway" not in gdf.columns:
        gdf["highway"] = None
    if "railway" not in gdf.columns:
        gdf["railway"] = None

    # Build bridge_type from highway or railway tag, matching pull_osm_bridges_legacy.py
    gdf["bridge_type"] = gdf.apply(
        lambda row: (
            f"highway-{row['highway']}" if pd.notna(row["highway"]) else f"railway-{row['railway']}"
        ),
        axis=1,
    )

    # Remove abandoned/demolished/proposed bridges, matching pull_osm_bridges_legacy.py lines 131-146.
    gdf = gdf[~gdf["bridge_type"].isin(UNWANTED_BRIDGE_TYPES)]

    if gdf.empty:
        msg = f"[bridges] {state}: all features were unwanted bridge types, skipping"
        file_logger.warning(msg)
        screen_queue.put(msg)
        return

    # Allowlist column drop.
    KEEP_COLS = {"osmid", "name", "bridge_type"}
    gdf = gdf.drop(
        columns=[c for c in gdf.columns if c not in KEEP_COLS and c != gdf.geometry.name],
        errors="ignore",
    )

    if "osmid" in gdf.columns:
        gdf["osmid"] = gdf["osmid"].astype(str)

    target_crs = _crs_for_state(state)
    gdf = gdf.to_crs(target_crs).copy()

    out_path = bridges_dir / f"{state}_bridges.parquet"
    write_geodataframe(gdf, out_path, index=False)

    msg = f"[bridges] {state}: wrote {len(gdf)} features -> {out_path.name}"
    file_logger.info(msg)
    screen_queue.put(msg)


def per_state_job(
    state,
    pbf_dir,
    roads_dir,
    bridges_dir,
    keep_pbf,
    file_logger,
    screen_queue,
    task_id,
):
    try:
        roads_out = roads_dir / f"{state}_roads.parquet"
        bridges_out = bridges_dir / f"{state}_bridges.parquet"

        if roads_out.exists() and bridges_out.exists():
            msg = f"[{task_id}] Both parquet files already exist, skipping download."
            file_logger.info(msg)
            screen_queue.put(msg)
            return 1, [True]

        pbf_path = _download_pbf(state, pbf_dir, file_logger, screen_queue)

        _write_roads_parquet(state, pbf_path, roads_dir, file_logger, screen_queue)
        _write_bridges_parquet(state, pbf_path, bridges_dir, file_logger, screen_queue)

        if not keep_pbf:
            pbf_path.unlink(missing_ok=True)

        return 1, [True]

    except Exception as e:
        file_logger.error(f"[{task_id}] Failed: {e}")
        file_logger.error(traceback.format_exc())
        return 0, [False]


def pull_osm(
    output_dir: str,
    states: str = "",
    keep_pbf: bool = False,
) -> None:
    start_time = datetime.now(timezone.utc)
    out = Path(output_dir)

    pbf_dir = out / "states_pbf"
    roads_dir = out / "states_parquet" / "roads"
    bridges_dir = out / "states_parquet" / "bridges"

    for d in (pbf_dir, roads_dir, bridges_dir):
        d.mkdir(parents=True, exist_ok=True)

    log_path = out / f"pull_osm_{start_time.strftime('%Y%m%d-%H%M')}.log"
    file_logger = setup_mp_file_logger(str(log_path), "pull_osm")

    file_logger.info("==================================")
    file_logger.info("Starting OSM download (Geofabrik)")
    file_logger.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    print("==================================")
    print("Starting OSM download (Geofabrik)")
    print(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")

    selected = [s.strip() for s in states.split()] if states.strip() else ALL_STATES
    invalid = [s for s in selected if s not in ALL_STATES]
    if invalid:
        raise ValueError(f"Unknown state slug(s): {invalid}. Must be Geofabrik slugs, e.g. 'delaware'.")

    file_logger.info(f"States to process: {len(selected)}")
    print(f"States to process: {len(selected)}")

    tasks_args_list = [
        {
            "state": state,
            "pbf_dir": pbf_dir,
            "roads_dir": roads_dir,
            "bridges_dir": bridges_dir,
            "keep_pbf": keep_pbf,
        }
        for state in selected
    ]

    mp_results = run_with_mp(
        task_function=per_state_job,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        # Hardcoded, not a CLI flag. Kept low (not higher, despite .pbf downloads being
        # small/fast enough to parallelize further) because each worker holds a full
        # state's road/bridge GeoDataFrame in memory at once; too many concurrent large
        # states can OOM-kill a worker, which surfaces as an unrecoverable
        # BrokenProcessPool rather than a per-state failure caught by per_state_job.
        max_workers=2,
        task_id_key="state",
        show_progress=True,
        # gpd.read_file()'s GDAL/OGR GeoJSON parsing retains memory across calls within the
        # same worker process (confirmed: neither dropping the GeoDataFrame nor glibc's
        # malloc_trim reclaims it — it isn't fragmentation, the library just keeps it live).
        # RSS climbs state after state in a reused worker until the kernel OOM-kills it.
        # Recycling the worker after every state is the only thing that actually frees it.
        max_tasks_per_child=1,
    )

    failed = [k for k, v in mp_results.items() if not v[0]]
    if not failed:
        file_logger.info("✅ All states succeeded")
        print("✅ All states succeeded")
    else:
        file_logger.error(f"❌ {len(failed)} state(s) failed: {failed}")
        print(f"❌ {len(failed)} state(s) failed: {failed}")

    if not keep_pbf:
        shutil.rmtree(pbf_dir, ignore_errors=True)

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    msg = f"Done. Duration: {str(duration).split('.')[0]}"
    file_logger.info(msg)
    print(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download per-state OSM extracts from Geofabrik and write road "
        "and bridge GeoParquet files."
    )
    parser.add_argument(
        "-o", "--output_dir",
        help="REQUIRED: root output folder. Creates states_parquet/roads/ and "
        "states_parquet/bridges/ subfolders.",
        required=True,
    )
    parser.add_argument(
        "-s", "--states",
        help="OPTIONAL: space-delimited Geofabrik state slugs in quotes, "
        "e.g. 'delaware michigan'. Defaults to all states.",
        required=False,
        default="",
    )
    parser.add_argument(
        "-k", "--keep_pbf",
        help="OPTIONAL: add this flag to keep the downloaded .osm.pbf files "
        "(takes no value). If omitted, PBFs are removed",
        required=False,
        action="store_true",
    )
    args = vars(parser.parse_args())
    pull_osm(**args)
