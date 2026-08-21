#!/usr/bin/env python3
"""
Complete Hybrid In-Memory delineate_hydros_and_produce_HAND.py
---------------------------------------------------------------
Executes all HAND/REM production steps in RAM. Intermediate datasets are
flushed to disk strictly at TauDEM C++ process boundaries, and memory objects
are aggressively deleted with `del` and `gc.collect()`.
"""

import argparse
import gc
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path


# --- Setup Python Path for Local Imports (Pre-Import Resolution) ---
SRC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

TOOLS_DIR = (PROJECT_ROOT / "tools").resolve()
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

# Standard GIS & Array Libraries
import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import rasterio as rio  # noqa: E402
import shapely  # noqa: E402
from convert_to_int16 import convert_raster_file_to_int16_in_memory  # noqa: E402
from evaluate_crosswalk import evaluate_crosswalk_in_memory  # noqa: E402
from osgeo import gdal, ogr  # noqa: E402

# Direct In-Memory Module Imports (NQA comments suppress E402 where sys.path modification is required)
from accumulate_headwaters import accumulate_headwaters_in_memory  # noqa: E402
from add_crosswalk import add_crosswalk_in_memory  # noqa: E402
from adjust_thalweg_lateral import adjust_thalweg_lateral_in_memory  # noqa: E402
from filter_catchments_and_add_attributes import filter_catchments_and_add_attributes_in_memory  # noqa: E402
from heal_bridges_osm import heal_bridges_osm_in_memory  # noqa: E402
from make_rem import create_rem_in_memory  # noqa: E402
from make_stages_and_catchlist import make_stages_and_catchlist_in_memory, write_catchlist_file  # noqa: E402
from mask_dem import mask_dem_in_memory  # noqa: E402
from mitigate_branch_outlet_backpool import mitigate_branch_outlet_backpool_in_memory  # noqa: E402
from process_buildings_fimpact import process_buildings_fimpact_in_memory  # noqa: E402
from process_roads_fimpact import process_roads_fimpact_in_memory  # noqa: E402
from reachID_grid_to_vector_points import reachID_grid_to_vector_points_in_memory  # noqa: E402
from split_flows import split_flows_in_memory  # noqa: E402
from unique_pixel_and_allocation import unique_pixel_allocation_in_memory  # noqa: E402
from utils.polygonize_raster import polygonize_in_memory  # noqa: E402
from utils.rasterize_vector import rasterize_vector  # noqa: E402


if hasattr(shapely, "geos_version"):
    print(f"--> GEOS C-library active: {shapely.geos_version_string}")

gdal.UseExceptions()


# --- OPTIMIZATION: Global GDAL Threading & RAM Caching ---
gdal.SetCacheMax(4096 * 1024 * 1024)  # Use 4GB GDAL Block Cache
os.environ["GDAL_NUM_THREADS"] = "ALL_CPUS"
os.environ["VSI_CACHE"] = "TRUE"

# Multi-threaded compression flags for flushing rasters to disk
TIFF_WRITE_OPTIONS = [
    "COMPRESS=LZW",
    "TILED=YES",
    "BLOCKXSIZE=512",
    "BLOCKYSIZE=512",
    "NUM_THREADS=ALL_CPUS",
    "BIGTIFF=IF_NEEDED",
]

_last_step_time = None


def log_step(message: str) -> None:
    """Logs pipeline step messages with a timestamp and elapsed time."""
    global _last_step_time
    now = time.perf_counter()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if _last_step_time is not None:
        elapsed = now - _last_step_time
        mins, secs = divmod(elapsed, 60)
        time_str = f" [Took: {int(mins)}m {secs:.2f}s]" if mins > 0 else f" [Took: {secs:.2f}s]"
    else:
        time_str = ""

    _last_step_time = now
    print(f"[{timestamp}]{time_str} {message}", flush=True)


def load_config_from_env_files() -> dict:
    config = dict(os.environ)
    config.setdefault("projectDir", str(PROJECT_ROOT))
    config.setdefault("srcDir", str(SRC_DIR))
    config.setdefault("toolsDir", str(PROJECT_ROOT / "tools"))

    env_files = [PROJECT_ROOT / "config" / "params_template.env", SRC_DIR / "bash_variables.env"]

    for env_file in env_files:
        if env_file.is_file():
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    m = re.match(r'^(?:export\s+)?([A-Za-z0-9_]+)\s*=\s*(.*)$', line)
                    if m:
                        key = m.group(1).strip()
                        val = m.group(2).rsplit("#", 1)[0].strip().strip('"').strip("'")
                        config[key] = val

    pattern = re.compile(r'\$\{?([A-Za-z0-9_]+)\}?')
    for _ in range(5):
        updated = False
        for k, v in list(config.items()):
            matches = pattern.findall(str(v))
            for var_name in matches:
                if var_name in config:
                    new_v = v.replace(f"${{{var_name}}}", config[var_name]).replace(
                        f"${var_name}", config[var_name]
                    )
                    if new_v != v:
                        config[k] = new_v
                        v = new_v
                        updated = True
        if not updated:
            break

    return config


def persist_dataset(src_ds: gdal.Dataset, dst_path: str, srs_wkt: str = None, force: bool = True):
    """Flushes in-memory rasters to disk using multi-threaded CPU compression."""
    if not force and os.path.exists(dst_path):
        return

    driver = gdal.GetDriverByName("GTiff")
    dst_ds = driver.CreateCopy(str(dst_path), src_ds, options=TIFF_WRITE_OPTIONS)
    if srs_wkt:
        dst_ds.SetProjection(srs_wkt)
    dst_ds.FlushCache()
    dst_ds = None


def run_python_script(script_path: Path, args: list):
    """Executes subprocess scripts with verbose error output."""
    if not script_path.is_file():
        raise FileNotFoundError(f"Required script missing: {script_path}")
    cmd = [sys.executable, str(script_path)] + [str(a) for a in args]
    print(f"--> [Subprocess Exec] {' '.join(str(c) for c in cmd)}")
    import subprocess

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        print(f"\n==================== SUBPROCESS ERROR ({p.returncode}) ====================")
        print(f"Command: {' '.join(str(c) for c in cmd)}")
        if p.stdout:
            print(f"\n--- STDOUT ---\n{p.stdout.strip()}")
        if p.stderr:
            print(f"\n--- STDERR ---\n{p.stderr.strip()}")
        print("===================================================================\n")
        raise RuntimeError(f"Script error ({p.returncode}) in {script_path.name}")


def gdal_multiply_in_memory(ds_a: gdal.Dataset, ds_b: gdal.Dataset, nodata_val: float = 0) -> gdal.Dataset:
    arr_a = ds_a.GetRasterBand(1).ReadAsArray().astype(np.int32, copy=False)
    arr_b = ds_b.GetRasterBand(1).ReadAsArray().astype(np.int32, copy=False)

    calc_res = np.multiply(arr_a, arr_b)

    driver = gdal.GetDriverByName("MEM")
    out_ds = driver.Create("", ds_a.RasterXSize, ds_a.RasterYSize, 1, gdal.GDT_Int32)
    out_ds.SetGeoTransform(ds_a.GetGeoTransform())
    out_ds.SetProjection(ds_a.GetProjectionRef())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(int(nodata_val))
    band.WriteArray(calc_res)
    out_ds.FlushCache()

    return out_ds


def gdal_rem_zero_mask_in_memory(
    rem_ds: gdal.Dataset, catch_ds: gdal.Dataset, nodata_val: float
) -> gdal.Dataset:
    arr_a = rem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32, copy=False)
    arr_b = catch_ds.GetRasterBand(1).ReadAsArray().astype(np.float32, copy=False)

    ndv_float = float(nodata_val)
    mask = (arr_a >= 0) & (arr_b > 0)
    calc_res = np.where(mask, arr_a, ndv_float).astype(np.float32, copy=False)

    driver = gdal.GetDriverByName("MEM")
    out_ds = driver.Create("", rem_ds.RasterXSize, rem_ds.RasterYSize, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(rem_ds.GetGeoTransform())
    out_ds.SetProjection(rem_ds.GetProjectionRef())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(ndv_float)
    band.WriteArray(calc_res)
    out_ds.FlushCache()

    return out_ds


def gdal_heal_hand_in_memory(
    rem_ds: gdal.Dataset, dem_ds: gdal.Dataset, thalweg_cond_ds: gdal.Dataset, nodata_val: float
) -> gdal.Dataset:
    arr_r = rem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32, copy=False)
    arr_d = dem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32, copy=False)
    arr_t = thalweg_cond_ds.GetRasterBand(1).ReadAsArray().astype(np.float32, copy=False)

    ndv_float = float(nodata_val)
    valid = arr_r != ndv_float
    calc_res = np.copy(arr_r)
    calc_res[valid] = arr_r[valid] + (arr_d[valid] - arr_t[valid])

    driver = gdal.GetDriverByName("MEM")
    out_ds = driver.Create("", rem_ds.RasterXSize, rem_ds.RasterYSize, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(rem_ds.GetGeoTransform())
    out_ds.SetProjection(rem_ds.GetProjectionRef())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(ndv_float)
    band.WriteArray(calc_res)
    out_ds.FlushCache()

    return out_ds


def delineate_and_produce_hand(
    level: str,
    huc_number: str,
    temp_huc_dir: str,
    temp_branch_dir: str,
    current_branch_id: str,
    branch_zero_id: str,
):
    cfg = load_config_from_env_files()

    tempHucDataDir = Path(temp_huc_dir)
    tempCurrentBranchDataDir = Path(temp_branch_dir)
    tempCurrentBranchDataDir.mkdir(parents=True, exist_ok=True)

    srcDir = Path(cfg.get("srcDir", str(SRC_DIR)))
    taudemDir = cfg.get("taudemDir", "/dependencies/taudem/bin")
    taudemDir2 = cfg.get("taudemDir2", "/dependencies/taudem_accelerated_flowDirections/taudem/build/bin")

    ndv = float(cfg.get("ndv", "-9999"))
    ncores_fd = cfg.get("ncores_fd", "1")
    ncores_gw = cfg.get("ncores_gw", "1")
    mask_leveed_area_toggle = cfg.get("mask_leveed_area_toggle", "False")
    branch_id_attribute = cfg.get("branch_id_attribute", "levpa_id")
    levee_id_attribute = cfg.get("levee_id_attribute", "feature_id")
    thalweg_lateral_elev_threshold = cfg.get("thalweg_lateral_elev_threshold", "2")
    max_split_distance_meters = cfg.get("max_split_distance_meters", "1500")
    slope_min = cfg.get("slope_min", "0.00001")
    lakes_buffer_dist_meters = cfg.get("lakes_buffer_dist_meters", "150")
    stage_min_meters = cfg.get("stage_min_meters", "0")
    stage_interval_meters = cfg.get("stage_interval_meters", "0.1")
    stage_max_meters = cfg.get("stage_max_meters", "20")
    is_healed_hand = cfg.get("healed_hand_hydrocondition", "false").lower() == "true"
    manning_n = cfg.get("manning_n", "0.05")
    min_catchment_area = cfg.get("min_catchment_area", "0")
    min_stream_length = cfg.get("min_stream_length", "0")
    iris_sword_slope = cfg.get("iris_sword_slope", "0.0001")
    hfab_ransac_slope = cfg.get("hfab_ransac_slope", "0.0001")
    evaluate_crosswalk = cfg.get("evaluateCrosswalk", "0")

    huc2Identifier = int(huc_number[:2]) if huc_number and len(huc_number) >= 2 else 0

    if level == "branch":
        b_arg = tempCurrentBranchDataDir / f"nwm_subset_streams_levelPaths_{current_branch_id}.gpkg"
        z_arg = tempCurrentBranchDataDir / f"nwm_catchments_proj_subset_levelPaths_{current_branch_id}.gpkg"
    else:
        b_arg = tempHucDataDir / "nwm_subset_streams.gpkg"
        z_arg = tempHucDataDir / "nwm_catchments_proj_subset.gpkg"

    wbd8_clp_file = tempHucDataDir / "wbd8_clp.gpkg"

    # --- OPTIMIZATION (Fixes #3 & #4): LOAD HUC VECTOR SUBSETS ONCE INTO RAM ---
    wbd8_gdf = gpd.read_file(wbd8_clp_file, engine="pyogrio")
    nwm_streams_gdf = gpd.read_file(b_arg, engine="pyogrio")

    osm_bridges_path = tempHucDataDir / "osm_bridges_subset.gpkg"
    osm_bridges_gdf = (
        gpd.read_file(osm_bridges_path, engine="pyogrio") if osm_bridges_path.is_file() else None
    )

    buildings_subset_path = tempHucDataDir / "buildings_subset.gpkg"
    buildings_gdf = (
        gpd.read_file(buildings_subset_path, engine="pyogrio") if buildings_subset_path.is_file() else None
    )

    # --- 1. MASK LEVEE-PROTECTED AREAS FROM DEM (In-Memory Python) ---
    ds_dem = gdal.Open(str(tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif"))
    srs_wkt = ds_dem.GetProjectionRef()
    levee_subset = tempHucDataDir / "LeveeProtectedAreas_subset.gpkg"

    if mask_leveed_area_toggle == "True" and levee_subset.is_file():
        log_step(f"--> [Step 1] Mask levee-protected areas (In-Memory) {huc_number} {current_branch_id}")
        ds_dem = mask_dem_in_memory(
            dem_ds=ds_dem,
            nld_gpkg_path=str(levee_subset),
            catchments_gpkg_path=str(z_arg),
            branch_id_attr=branch_id_attribute,
            current_branch_id=current_branch_id,
            branch_zero_id=branch_zero_id,
            levee_id_attr=levee_id_attribute,
        )

    # --- 2. D8 FLOW ACCUMULATIONS (In-Memory pyflwdir) ---
    log_step(f"--> [Step 2] D8 Flow Accumulations (In-Memory pyflwdir) {huc_number} {current_branch_id}")
    ds_flowdir = gdal.Open(
        str(tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{current_branch_id}.tif")
    )

    hw_file = tempCurrentBranchDataDir / f"headwaters_{current_branch_id}.tif"
    ds_headwaters = gdal.Open(str(hw_file)) if hw_file.is_file() else None

    ds_flowaccum, ds_streams = accumulate_headwaters_in_memory(
        flow_dir_ds=ds_flowdir, headwaters_ds=ds_headwaters, threshold=1
    )
    if ds_headwaters:
        ds_headwaters = None

    # --- 3. PREPROCESSING FOR LATERAL THALWEG (In-Memory Python) ---
    log_step(
        f"--> [Step 3] Preprocessing for lateral thalweg adjustment (In-Memory) {huc_number} {current_branch_id}"
    )
    ds_stream_ids, ds_allo, ds_dist = unique_pixel_allocation_in_memory(stream_pixels_ds=ds_streams)

    # --- 4. ADJUST THALWEG LATERAL MINIMUM (In-Memory Python) ---
    log_step(
        f"--> [Step 4] Performing lateral thalweg adjustment (In-Memory) {huc_number} {current_branch_id}"
    )
    ds_dem_adj = adjust_thalweg_lateral_in_memory(
        dem_ds=ds_dem,
        stream_pixels_ds=ds_streams,
        allocation_ds=ds_allo,
        distance_ds=ds_dist,
        distance_threshold=50.0,
        elev_threshold=float(thalweg_lateral_elev_threshold),
    )

    # --- 5. MASK BURNED DEM FOR STREAMS ONLY (In-Memory GDAL) ---
    log_step(f"--> [Step 5] Mask Burned DEM for Thalweg Only (In-Memory) {huc_number} {current_branch_id}")
    ds_flows = gdal_multiply_in_memory(ds_flowdir, ds_streams, nodata_val=0)

    # TAUDEM C++ BOUNDARY FLUSH
    persist_dataset(ds_dem, tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif", srs_wkt)
    persist_dataset(
        ds_dem_adj, tempCurrentBranchDataDir / f"dem_lateral_thalweg_adj_{current_branch_id}.tif", srs_wkt
    )
    persist_dataset(
        ds_flows,
        tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_flows_{current_branch_id}.tif",
        srs_wkt,
    )
    persist_dataset(
        ds_streams, tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif", srs_wkt
    )
    persist_dataset(
        ds_stream_ids,
        tempCurrentBranchDataDir / f"demDerived_streamPixels_ids_{current_branch_id}.tif",
        srs_wkt,
    )
    persist_dataset(
        ds_allo,
        tempCurrentBranchDataDir / f"demDerived_streamPixels_ids_{current_branch_id}_allo.tif",
        srs_wkt,
    )
    persist_dataset(
        ds_dist,
        tempCurrentBranchDataDir / f"demDerived_streamPixels_ids_{current_branch_id}_dist.tif",
        srs_wkt,
    )
    persist_dataset(
        ds_flowaccum,
        tempCurrentBranchDataDir / f"flowaccum_d8_burned_filled_{current_branch_id}.tif",
        srs_wkt,
    )

    # FREE STEP 1-5 RASTER MEMORY
    ds_flowdir = None
    ds_dem = None
    ds_dem_adj = None
    ds_flows = None
    ds_stream_ids = None
    ds_allo = None
    ds_dist = None
    ds_flowaccum = None
    gc.collect()

    # --- 6. FLOW CONDITION STREAMS (TauDEM C++ Subprocess) ---
    log_step(f"--> [Step 6] Flow Condition Thalweg {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "run_taudem_subprocess.py",
        [
            "flowdircond",
            "-t",
            taudemDir,
            "-p",
            tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_flows_{current_branch_id}.tif",
            "-z",
            tempCurrentBranchDataDir / f"dem_lateral_thalweg_adj_{current_branch_id}.tif",
            "-zfdc",
            tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif",
        ],
    )

    # --- 7. D8 SLOPES (TauDEM C++ Subprocess) ---
    log_step(f"--> [Step 7] D8 Slopes from DEM {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "run_taudem_subprocess.py",
        [
            "d8flowdir",
            "-n",
            ncores_fd,
            "-t",
            taudemDir2,
            "-fel",
            tempCurrentBranchDataDir / f"dem_lateral_thalweg_adj_{current_branch_id}.tif",
            "-sd8",
            tempCurrentBranchDataDir / f"slopes_d8_dem_meters_{current_branch_id}.tif",
        ],
    )

    # --- 8. STREAMNET FOR REACHES (TauDEM C++ Subprocess) ---
    log_step(f"--> [Step 8] Stream Net for Reaches {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "run_taudem_subprocess.py",
        [
            "streamnet",
            "-t",
            taudemDir,
            "-p",
            tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{current_branch_id}.tif",
            "-fel",
            tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif",
            "-ad8",
            tempCurrentBranchDataDir / f"flowaccum_d8_burned_filled_{current_branch_id}.tif",
            "-src",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif",
            "-ord",
            tempCurrentBranchDataDir / f"streamOrder_{current_branch_id}.tif",
            "-tree",
            tempCurrentBranchDataDir / f"treeFile_{current_branch_id}.txt",
            "-coord",
            tempCurrentBranchDataDir / f"coordFile_{current_branch_id}.txt",
            "-w",
            tempCurrentBranchDataDir / f"sn_catchments_reaches_{current_branch_id}.tif",
            "-net",
            tempCurrentBranchDataDir / f"demDerived_reaches_{current_branch_id}.shp",
        ],
    )

    # --- Step 9: In-Memory Stream Splitting ---
    log_step(f"--> [Step 9] Splitting flows in-memory for branch {current_branch_id}")

    flows_path = tempCurrentBranchDataDir / f"demDerived_reaches_{current_branch_id}.shp"
    dem_path = tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif"
    split_flows_path = tempCurrentBranchDataDir / f"demDerived_reaches_split_{current_branch_id}.gpkg"
    split_points_path = tempCurrentBranchDataDir / f"demDerived_reaches_split_points_{current_branch_id}.gpkg"
    lakes_file = tempHucDataDir / "nwm_lakes_proj_subset.gpkg"

    flows_gdf = gpd.read_file(flows_path, engine="pyogrio")
    lakes_gdf = gpd.read_file(lakes_file, engine="pyogrio") if lakes_file.is_file() else None

    # --- OPTIMIZATION (Fix #1): KEEP BOTH SPLIT GDFS IN RAM FOR STEP 13 ---
    with rio.open(dem_path) as dem_dataset:
        split_flows_gdf, split_points_gdf = split_flows_in_memory(
            flows_gdf=flows_gdf,
            dem_dataset=dem_dataset,
            wbd8_gdf=wbd8_gdf,
            nwm_streams_gdf=nwm_streams_gdf,
            lakes_gdf=lakes_gdf,
            max_length=float(max_split_distance_meters),
            slope_min=float(slope_min),
            lakes_buffer_input=float(lakes_buffer_dist_meters),
        )

    split_flows_gdf.to_file(split_flows_path, driver="GPKG", engine="pyogrio", index=False)
    split_points_gdf.to_file(split_points_path, driver="GPKG", engine="pyogrio", index=False)

    del flows_gdf, lakes_gdf
    gc.collect()

    # --- 10. GAGE WATERSHED FOR REACHES (TauDEM C++ Subprocess) ---
    log_step(f"--> [Step 10] Gage Watershed for Reaches {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "run_taudem_subprocess.py",
        [
            "gagewatershed",
            "-n",
            ncores_gw,
            "-t",
            taudemDir,
            "-p",
            tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{current_branch_id}.tif",
            "-gw",
            tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif",
            "-o",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_points_{current_branch_id}.gpkg",
            "-id",
            tempCurrentBranchDataDir / f"idFile_{current_branch_id}.txt",
        ],
    )

    # --- Step 11: VECTORIZE FEATURE ID CENTROIDS (In-Memory) ---
    log_step(f"--> [Step 11] Vectorize Pixel Centroids in-memory {huc_number} {current_branch_id}")

    stream_pixels_tif = tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif"
    flows_points_gpkg = tempCurrentBranchDataDir / f"flows_points_pixels_{current_branch_id}.gpkg"

    with rio.open(stream_pixels_tif) as src_stream_pixels:
        flows_pts_gdf = reachID_grid_to_vector_points_in_memory(
            raster_dataset=src_stream_pixels, id_field_name="featureID"
        )

    flows_pts_gdf.to_file(flows_points_gpkg, driver="GPKG", engine="pyogrio", index=False)
    del flows_pts_gdf
    gc.collect()

    # --- 12. GAGE WATERSHED FOR PIXELS (TauDEM C++ Subprocess) ---
    log_step(f"--> [Step 12] Gage Watershed for Pixels {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "run_taudem_subprocess.py",
        [
            "gagewatershed",
            "-n",
            ncores_gw,
            "-t",
            taudemDir,
            "-p",
            tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{current_branch_id}.tif",
            "-gw",
            tempCurrentBranchDataDir / f"gw_catchments_pixels_{current_branch_id}.tif",
            "-o",
            tempCurrentBranchDataDir / f"flows_points_pixels_{current_branch_id}.gpkg",
            "-id",
            tempCurrentBranchDataDir / f"idFile_{current_branch_id}.txt",
        ],
    )

    # --- Step 13: CATCH AND MITIGATE BRANCH OUTLET BACKPOOL ERROR (Pure In-Memory) ---
    log_step(
        f"--> [Step 13] Catching and mitigating branch outlet backpool issue {huc_number} {current_branch_id}"
    )

    cp_tif_path = tempCurrentBranchDataDir / f"gw_catchments_pixels_{current_branch_id}.tif"
    cr_tif_path = tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif"
    dem_tif_path = tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif"

    with rio.open(cp_tif_path) as cp_ds, rio.open(cr_tif_path) as cr_ds, rio.open(dem_tif_path) as dem_ds:
        split_flows_gdf, split_points_gdf, masked_cr_arr, masked_cp_arr = (
            mitigate_branch_outlet_backpool_in_memory(
                catchment_pixels_ds=cp_ds,
                catchment_reaches_ds=cr_ds,
                split_flows_gdf=split_flows_gdf,
                split_points_gdf=split_points_gdf,
                nwm_streams_gdf=nwm_streams_gdf,
                dem_dataset=dem_ds,
                branch_dir=str(tempCurrentBranchDataDir),
                slope_min=float(slope_min),
                calculate_stats=True,
                dry_run=False,
            )
        )

        for path, arr, ds in [(cr_tif_path, masked_cr_arr, cr_ds), (cp_tif_path, masked_cp_arr, cp_ds)]:
            profile = ds.profile.copy()
            profile.update(BIGTIFF="YES")
            with rio.open(path, "w", **profile) as dst:
                dst.write(arr, 1)

    split_flows_gdf.to_file(split_flows_path, driver="GPKG", engine="pyogrio", index=False)
    split_points_gdf.to_file(split_points_path, driver="GPKG", engine="pyogrio", index=False)

    del split_points_gdf, masked_cr_arr, masked_cp_arr
    gc.collect()

    # --- 14. D8 REM ---
    log_step(f"--> [Step 14] D8 REM {huc_number} {current_branch_id}")
    ds_dem_cond = gdal.Open(str(tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif"))
    ds_gw_pixels = gdal.Open(str(tempCurrentBranchDataDir / f"gw_catchments_pixels_{current_branch_id}.tif"))

    ds_rem = create_rem_in_memory(
        dem_ds=ds_dem_cond, pixel_watersheds_ds=ds_gw_pixels, thalweg_ds=ds_streams, nodata_val=ndv
    )

    rem_tif_path = tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"
    persist_dataset(ds_rem, rem_tif_path, srs_wkt=srs_wkt, force=True)

    ds_gw_pixels = None
    if ds_streams:
        ds_streams = None

    # --- 15. ZERO & MASK REM TO CATCHMENTS (In-Memory GDAL) ---
    log_step(
        f"--> [Step 15] Bring negative values in REM to zero and mask (In-Memory) {huc_number} {current_branch_id}"
    )
    ds_gw_reach = gdal.Open(str(tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif"))
    ds_rem_zero = gdal_rem_zero_mask_in_memory(ds_rem, ds_gw_reach, nodata_val=ndv)
    persist_dataset(
        ds_rem_zero, tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif", srs_wkt
    )

    ds_rem = None
    ds_gw_reach = None
    gc.collect()

    # --- 16. RASTERIZE LANDSEA POLYGON ---
    landsea_subset = tempHucDataDir / "LandSea_subset.gpkg"
    landsea_tif = tempCurrentBranchDataDir / f"LandSea_subset_{current_branch_id}.tif"
    if landsea_subset.is_file():
        log_step(f"--> [Step 16] Rasterize ocean/Glake polygon {huc_number} {current_branch_id}")
        rasterize_vector(
            vector_path_or_gdf=str(landsea_subset),
            template_raster_path=str(tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"),
            output_raster_path=str(landsea_tif),
            burn_value=ndv,
            init_value=1,
        )

    # --- 17. POLYGONIZE REACH WATERSHEDS (In-Memory) ---
    log_step(f"--> [Step 17] Polygonize Reach Watersheds in-memory {huc_number} {current_branch_id}")

    cr_tif_path = tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif"
    catchments_gpkg = tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.gpkg"

    # Polygonize directly to GeoDataFrame in RAM
    catch_gdf = polygonize_in_memory(
        input_raster=str(cr_tif_path), field_name="HydroID", connectivity=8, output_file=str(catchments_gpkg)
    )

    # --- Step 18: PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 (In-Memory) ---
    log_step(f"--> [Step 18] Process catchments and model streams in-memory {huc_number} {current_branch_id}")

    out_catchments_gpkg = (
        tempCurrentBranchDataDir / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.gpkg"
    )
    out_flows_gpkg = tempCurrentBranchDataDir / f"demDerived_reaches_split_filtered_{current_branch_id}.gpkg"

    # Pass catch_gdf directly from Step 17 without reading from disk
    filt_catch_gdf, filt_flows_gdf = filter_catchments_and_add_attributes_in_memory(
        catchments_gdf=catch_gdf, flows_gdf=split_flows_gdf, wbd_gdf=wbd8_gdf, huc_code=huc_number
    )

    filt_catch_gdf.to_file(
        out_catchments_gpkg, layer="catchments", driver="GPKG", engine="pyogrio", index=False
    )
    filt_flows_gdf.to_file(out_flows_gpkg, driver="GPKG", engine="pyogrio", index=False)

    del catch_gdf, split_flows_gdf
    gc.collect()

    # --- 19. RASTERIZE NEW CATCHMENTS AGAIN ---
    log_step(f"--> [Step 19] Rasterize filtered catchments {huc_number} {current_branch_id}")
    rasterize_vector(
        vector_path_or_gdf=filt_catch_gdf,
        template_raster_path=str(tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"),
        output_raster_path=str(
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.tif"
        ),
        attribute="HydroID",
        init_value=0,
    )

    # --- 20. MASK SLOPE TO CATCHMENTS ---
    log_step(f"--> [Step 20] Mask slopes to catchments {huc_number} {current_branch_id}")
    ds_slopes = gdal.Open(str(tempCurrentBranchDataDir / f"slopes_d8_dem_meters_{current_branch_id}.tif"))
    ds_catch_filt = gdal.Open(
        str(
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.tif"
        )
    )
    ds_slopes_masked = gdal_multiply_in_memory(ds_slopes, ds_catch_filt, nodata_val=ndv)
    persist_dataset(
        ds_slopes_masked,
        tempCurrentBranchDataDir / f"slopes_d8_dem_meters_masked_{current_branch_id}.tif",
        srs_wkt,
    )

    ds_slopes = None
    ds_catch_filt = None
    ds_slopes_masked = None
    gc.collect()

    # --- Step 21: MAKE CATCHMENT AND STAGE FILES (In-Memory) ---
    log_step(
        f"--> [Step 21] Generate Catchment List and Stage List Files in-memory {huc_number} {current_branch_id}"
    )

    catch_list_txt = tempCurrentBranchDataDir / f"catch_list_{current_branch_id}.txt"
    stage_txt = tempCurrentBranchDataDir / f"stage_{current_branch_id}.txt"

    catchlist_df, stage_list = make_stages_and_catchlist_in_memory(
        catchments_gdf=filt_catch_gdf,
        flows_gdf=filt_flows_gdf,
        stage_min_meters=float(stage_min_meters),
        stage_interval_meters=float(stage_interval_meters),
        stage_max_meters=float(stage_max_meters),
    )

    write_catchlist_file(catchlist_df, str(catch_list_txt))
    np.savetxt(stage_txt, stage_list, fmt="%.2f")

    del catchlist_df, stage_list
    gc.collect()

    # --- 22. MASK REM RASTER TO REMOVE OCEAN AREAS ---
    if landsea_tif.is_file():
        log_step(
            f"--> [Step 22] Additional masking to REM raster to remove ocean/Glake areas {huc_number} {current_branch_id}"
        )
        ds_landsea = gdal.Open(str(landsea_tif))
        ds_rem_zero = gdal_multiply_in_memory(ds_rem_zero, ds_landsea, nodata_val=ndv)
        persist_dataset(
            ds_rem_zero, tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif", srs_wkt
        )
        ds_landsea = None

    # --- 23. HEAL HAND (NON-BRANCH ZERO) ---
    if is_healed_hand and current_branch_id != branch_zero_id:
        log_step(
            f"--> [Step 23] Healed HAND to Remove Hydro-conditioning Artifacts {huc_number} {current_branch_id}"
        )
        ds_dem_orig = gdal.Open(str(tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif"))
        ds_rem_zero = gdal_heal_hand_in_memory(ds_rem_zero, ds_dem_orig, ds_dem_cond, nodata_val=ndv)
        persist_dataset(
            ds_rem_zero, tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif", srs_wkt
        )
        ds_dem_orig = None

    ds_dem_cond = None
    ds_rem_zero = None
    gc.collect()

    # --- 24. HYDRAULIC PROPERTIES (TauDEM C++ Subprocess) ---
    log_step(f"--> [Step 24] Sample reach averaged parameters {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "run_taudem_subprocess.py",
        [
            "catchhydrogeo",
            "-t",
            taudemDir,
            "-hand",
            tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            "-catch",
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.tif",
            "-catchlist",
            tempCurrentBranchDataDir / f"catch_list_{current_branch_id}.txt",
            "-slp",
            tempCurrentBranchDataDir / f"slopes_d8_dem_meters_masked_{current_branch_id}.tif",
            "-H",
            tempCurrentBranchDataDir / f"stage_{current_branch_id}.txt",
            "-table",
            tempCurrentBranchDataDir / f"src_base_{current_branch_id}.csv",
        ],
    )

    # --- Step 25: FINALIZE CATCHMENTS AND MODEL STREAMS (In-Memory) ---
    log_step(
        f"--> [Step 25] Finalize catchments and model streams in-memory {huc_number} {current_branch_id}"
    )

    src_base_csv = tempCurrentBranchDataDir / f"src_base_{current_branch_id}.csv"
    small_segs_csv = tempCurrentBranchDataDir / f"small_stream_segments_{current_branch_id}.csv"

    out_catch_path = (
        tempCurrentBranchDataDir
        / f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg"
    )
    out_flows_path = (
        tempCurrentBranchDataDir
        / f"demDerived_reaches_split_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg"
    )
    out_src_full_path = tempCurrentBranchDataDir / f"src_full_crosswalked_{current_branch_id}.csv"
    out_src_json_path = tempCurrentBranchDataDir / f"src_{current_branch_id}.json"
    out_cross_path = tempCurrentBranchDataDir / f"crosswalk_table_{current_branch_id}.csv"
    out_hydro_path = tempCurrentBranchDataDir / f"hydroTable_{current_branch_id}.csv"

    src_base_df = pd.read_csv(src_base_csv, dtype=object)

    iris_df = pd.read_parquet(iris_sword_slope).rename(
        columns={"slope_iris_sword": "SLOPE_IRIS_SWORD", "id": "feature_id"}
    )
    hfab_slopes_df = pd.read_parquet(hfab_ransac_slope)[["id", "slope_m_per_m"]].rename(
        columns={"id": "feature_id", "slope_m_per_m": "SLOPE_HFAB"}
    )

    # --- OPTIMIZATION (Fix #2): RETURN RAM DATAFRAMES DIRECTLY ---
    (
        cross_catch_gdf,
        cross_flows_gdf,
        src_full_df,
        src_crosswalk_df,
        hydro_table_df,
        src_json_dict,
        sml_segs_df,
    ) = add_crosswalk_in_memory(
        input_catchments=filt_catch_gdf,
        input_flows=filt_flows_gdf,
        input_src_base=src_base_df,
        input_huc=wbd8_gdf,
        input_nwmflows=nwm_streams_gdf,
        iris_df=iris_df,
        hfab_slopes_df=hfab_slopes_df,
        mannings_n=float(manning_n),
        min_catchment_area=float(min_catchment_area),
        min_stream_length=float(min_stream_length),
        huc_id=huc_number,
    )

    if len(sml_segs_df) > 0:
        sml_segs_df.to_csv(small_segs_csv, index=False)

    cross_catch_gdf.to_file(out_catch_path, layer="catchments", driver="GPKG", engine="pyogrio", index=False)
    cross_flows_gdf.to_file(out_flows_path, driver="GPKG", engine="pyogrio", index=False)
    src_full_df.to_csv(out_src_full_path, index=False)
    src_crosswalk_df.to_csv(out_cross_path, index=False)
    hydro_table_df.to_csv(out_hydro_path, index=False)

    with open(out_src_json_path, "w", encoding="utf-8") as f:
        json.dump(src_json_dict, f, sort_keys=True, indent=2)

    # --- 26. HEAL HAND (BRANCH ZERO) ---
    if is_healed_hand and current_branch_id == branch_zero_id:
        log_step(
            f"--> [Step 26] Healed HAND to Remove Hydro-conditioning Artifacts {huc_number} {current_branch_id}"
        )
        ds_dem_orig = gdal.Open(str(tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif"))
        ds_dem_cond = gdal.Open(str(tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif"))
        ds_rem_zero = gdal.Open(str(tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif"))

        ds_rem_zero_healed = gdal_heal_hand_in_memory(ds_rem_zero, ds_dem_orig, ds_dem_cond, nodata_val=ndv)
        persist_dataset(
            ds_rem_zero_healed,
            tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            srs_wkt,
        )
        ds_dem_orig = None
        ds_dem_cond = None
        ds_rem_zero = None
        ds_rem_zero_healed = None
        gc.collect()

    # --- 27. HEAL HAND BRIDGES (In-Memory) ---
    if osm_bridges_path.is_file():
        log_step(f"--> [Step 27] Burn in bridges in-memory {huc_number} {current_branch_id}")

        rem_tif = tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif"
        diff_tif = tempCurrentBranchDataDir / f"bridge_elev_diff_meters_{current_branch_id}.tif"
        centroids_gpkg = tempCurrentBranchDataDir / f"osm_bridge_centroids_{current_branch_id}.gpkg"

        # Pass cross_catch_gdf directly from Step 25
        updated_rem_arr, rem_prof = heal_bridges_osm_in_memory(
            source_hand_raster=str(rem_tif),
            bridge_elev_diff_raster=str(diff_tif),
            bridge_vector_file=str(osm_bridges_path),
            non_lidar_buffer=10.0,
            lidar_buffer=1.5,
            catchments_gdf=cross_catch_gdf,
            bridge_centroids_path=str(centroids_gpkg),
        )

        if updated_rem_arr is not None:
            with rio.open(rem_tif, "w", **rem_prof) as dst:
                dst.write(updated_rem_arr, 1)

        del updated_rem_arr
        gc.collect()

    # --- Step 28: PROCESS ROAD FLOOD IMPACTS (In-Memory) ---
    # --- OPTIMIZATION (Fix #3): PASS PRE-LOADED osm_bridges_gdf FROM RAM ---
    if osm_bridges_gdf is not None and not osm_bridges_gdf.empty:
        log_step(
            f"--> [Step 28] Process road flood impact in-memory for HUC {huc_number} {current_branch_id}"
        )

        rem_tif = tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif"
        road_impact_gpkg = tempCurrentBranchDataDir / f"osm_roads_impact_{current_branch_id}.gpkg"

        process_roads_fimpact_in_memory(
            rem_raster_path=str(rem_tif),
            roads_gdf=osm_bridges_gdf,
            catchments_gdf=cross_catch_gdf,
            hydrotable_df=hydro_table_df,
            buffer_m=1.5,
            output_gpkg_path=str(road_impact_gpkg),
        )

    # --- 29. PROCESS BUILDINGS FIMpact (In-Memory) ---
    # --- OPTIMIZATION (Fix #4): PASS PRE-LOADED buildings_gdf FROM RAM ---
    if buildings_gdf is not None and not buildings_gdf.empty:
        log_step(f"--> [Step 29] Process buildings FIMpact in-memory {huc_number} {current_branch_id}")

        rem_tif = tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif"
        buildings_csv = tempCurrentBranchDataDir / f"buildings_fimpact_{current_branch_id}.csv"

        process_buildings_fimpact_in_memory(
            hand_grid_raster=str(rem_tif),
            buildings_gdf=buildings_gdf,
            catchments_gdf=cross_catch_gdf,
            output_path=str(buildings_csv),
        )

    # --- 30. EVALUATE CROSSWALK (In-Memory) ---
    if current_branch_id == branch_zero_id and evaluate_crosswalk == "1":
        log_step(f"--> [Step 30] Evaluate crosswalk in-memory {huc_number} {current_branch_id}")

        eval_csv_path = tempHucDataDir / f"crosswalk_evaluation_{current_branch_id}.csv"
        hw_gpkg_path = tempHucDataDir / "nwm_headwater_points_subset.gpkg"

        hw_gdf = gpd.read_file(hw_gpkg_path, engine="pyogrio") if hw_gpkg_path.is_file() else None

        evaluate_crosswalk_in_memory(
            dem_reaches_gdf=cross_flows_gdf,
            nwm_streams_gdf=nwm_streams_gdf,
            headwaters_gdf=hw_gdf,
            huc_unit=huc_number,
            branch_id=current_branch_id,
            output_csv_path=str(eval_csv_path),
        )

        if hw_gdf is not None:
            del hw_gdf
            gc.collect()

    # --- 31. CONVERSION TO INT16 (In-Memory) ---
    if huc2Identifier == 19:
        log_step("--> [Step 31] Skipping Int16 Conversion for Alaska HUC")
    else:
        log_step(
            f"--> [Step 31] Convert GW Catchments and REM to Int16 in-memory {huc_number} {current_branch_id}"
        )

        rem_zero_tif = tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif"
        gw_catch_tif = (
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.tif"
        )

        for tif_file in [rem_zero_tif, gw_catch_tif]:
            if tif_file.is_file():
                convert_raster_file_to_int16_in_memory(str(tif_file))

    print(f"=== [SUCCESS] Completed delineate_hydros_and_produce_HAND for HUC {huc_number} ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full 31-step Hybrid In-Memory Delineate hydros workflow.")
    parser.add_argument("-l", "--level", type=str, required=True, help="Processing level (unit or branch)")
    parser.add_argument("-u", "--huc-number", type=str, required=True, help="HUC number string")
    parser.add_argument("-d", "--temp-huc-dir", type=str, required=True, help="Path to HUC temp directory")
    parser.add_argument(
        "-b", "--temp-branch-dir", type=str, required=True, help="Path to branch temp directory"
    )
    parser.add_argument("-cb", "--current-branch-id", type=str, required=True, help="Current branch ID")
    parser.add_argument("-b0", "--branch-zero-id", type=str, required=True, help="Branch zero ID")

    args = parser.parse_args()
    delineate_and_produce_hand(
        level=args.level,
        huc_number=args.huc_number,
        temp_huc_dir=args.temp_huc_dir,
        temp_branch_dir=args.temp_branch_dir,
        current_branch_id=args.current_branch_id,
        branch_zero_id=args.branch_zero_id,
    )
