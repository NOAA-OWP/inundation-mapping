#!/usr/bin/env python3
"""
Complete Hybrid In-Memory delineate_hydros_and_produce_HAND.py
---------------------------------------------------------------
Executes all HAND/REM production steps. Python sub-scripts are imported directly
and executed in RAM. Intermediate datasets are flushed to disk strictly at TauDEM
C++ process boundaries.
"""

import argparse
import os
import re
import sys
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr

# Direct Python In-Memory Imports
from accumulate_headwaters import accumulate_headwaters_in_memory
from adjust_thalweg_lateral import adjust_thalweg_lateral_in_memory
from make_rem import create_rem_in_memory
from mask_dem import mask_dem_in_memory
from unique_pixel_and_allocation import unique_pixel_allocation_in_memory


gdal.UseExceptions()

SRC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SRC_DIR.parent


# ------------------------------------------------------------------------------
# 1. Config Loader & Memory/Disk Handlers
# ------------------------------------------------------------------------------
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


def persist_dataset(ds: gdal.Dataset, dst_path: Path, srs_wkt: str = None, force: bool = True):
    """Flushes an in-memory dataset to disk with explicit SRS preservation."""
    if force and ds is not None:
        dst_path = Path(dst_path)
        dst_path.parent.mkdir(parents=True, exist_ok=True)

        if srs_wkt and not ds.GetProjectionRef():
            ds.SetProjection(srs_wkt)

        driver = gdal.GetDriverByName("GTiff")
        driver.CreateCopy(str(dst_path), ds, strict=0, options=["COMPRESS=LZW", "TILED=YES"])


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
    arr_a = ds_a.GetRasterBand(1).ReadAsArray()
    arr_b = ds_b.GetRasterBand(1).ReadAsArray()

    calc_res = (arr_a * arr_b).astype(np.int32)

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
    arr_a = rem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    arr_b = catch_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)

    ndv_float = float(nodata_val)
    mask = (arr_a >= 0) & (arr_b > 0)
    calc_res = np.where(mask, arr_a, ndv_float).astype(np.float32)

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
    arr_r = rem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    arr_d = dem_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
    arr_t = thalweg_cond_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)

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


def gdal_rasterize_vector(
    src_vector: str,
    template_raster: str,
    dst_raster: str,
    attribute: str = None,
    burn_value: float = None,
    init_value: float = 0,
):
    tmpl_ds = gdal.Open(template_raster)
    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(
        dst_raster,
        tmpl_ds.RasterXSize,
        tmpl_ds.RasterYSize,
        1,
        gdal.GDT_Int32 if attribute else gdal.GDT_Float32,
        options=["COMPRESS=LZW", "TILED=YES"],
    )
    out_ds.SetGeoTransform(tmpl_ds.GetGeoTransform())
    out_ds.SetProjection(tmpl_ds.GetProjectionRef())

    band = out_ds.GetRasterBand(1)
    band.Fill(init_value)

    vec_ds = ogr.Open(src_vector)
    layer = vec_ds.GetLayer()

    opts = []
    if attribute:
        opts.append(f"ATTRIBUTE={attribute}")
    if burn_value is not None:
        opts.append(f"BURN={burn_value}")

    gdal.RasterizeLayer(out_ds, [1], layer, options=opts)
    out_ds.FlushCache()


def gdal_polygonize_raster(in_raster: Path, out_gpkg: Path, layer_name: str, field_name: str):
    src_ds = gdal.Open(str(in_raster))
    src_band = src_ds.GetRasterBand(1)

    drv = ogr.GetDriverByName("GPKG")
    if out_gpkg.exists():
        drv.DeleteDataSource(str(out_gpkg))

    out_ds = drv.CreateDataSource(str(out_gpkg))
    srs = ogr.osr.SpatialReference()
    srs.ImportFromWkt(src_ds.GetProjectionRef())

    out_layer = out_ds.CreateLayer(layer_name, srs=srs, geom_type=ogr.wkbPolygon)
    fld = ogr.FieldDefn(field_name, ogr.OFTInteger)
    out_layer.CreateField(fld)

    gdal.Polygonize(src_band, None, out_layer, 0, ["8CONNECTED=8"], callback=None)

    src_ds = None
    out_ds = None


# ------------------------------------------------------------------------------
# 2. Main Workflow Execution
# ------------------------------------------------------------------------------
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
    toolsDir = Path(cfg.get("toolsDir", str(PROJECT_ROOT / "tools")))
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

    # Path resolution directly matching original FIM logic
    if level == "branch":
        b_arg = tempCurrentBranchDataDir / f"nwm_subset_streams_levelPaths_{current_branch_id}.gpkg"
        z_arg = tempCurrentBranchDataDir / f"nwm_catchments_proj_subset_levelPaths_{current_branch_id}.gpkg"
    else:
        b_arg = tempHucDataDir / "nwm_subset_streams.gpkg"
        z_arg = tempHucDataDir / "nwm_catchments_proj_subset.gpkg"

    # --- 1. MASK LEVEE-PROTECTED AREAS FROM DEM (In-Memory Python) ---
    ds_dem = gdal.Open(str(tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif"))
    srs_wkt = ds_dem.GetProjectionRef()
    levee_subset = tempHucDataDir / "LeveeProtectedAreas_subset.gpkg"

    if mask_leveed_area_toggle == "True" and levee_subset.is_file():
        print(f"--> [Step 1] Mask levee-protected areas (In-Memory) {huc_number} {current_branch_id}")
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
    print(f"--> [Step 2] D8 Flow Accumulations (In-Memory pyflwdir) {huc_number} {current_branch_id}")
    ds_flowdir = gdal.Open(
        str(tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{current_branch_id}.tif")
    )

    hw_file = tempCurrentBranchDataDir / f"headwaters_{current_branch_id}.tif"
    ds_headwaters = gdal.Open(str(hw_file)) if hw_file.is_file() else None

    ds_flowaccum, ds_streams = accumulate_headwaters_in_memory(
        flow_dir_ds=ds_flowdir, headwaters_ds=ds_headwaters, threshold=1
    )

    # --- 3. PREPROCESSING FOR LATERAL THALWEG (In-Memory Python) ---
    print(
        f"--> [Step 3] Preprocessing for lateral thalweg adjustment (In-Memory) {huc_number} {current_branch_id}"
    )
    ds_stream_ids, ds_allo, ds_dist = unique_pixel_allocation_in_memory(stream_pixels_ds=ds_streams)

    # --- 4. ADJUST THALWEG LATERAL MINIMUM (In-Memory Python) ---
    print(f"--> [Step 4] Performing lateral thalweg adjustment (In-Memory) {huc_number} {current_branch_id}")
    ds_dem_adj = adjust_thalweg_lateral_in_memory(
        dem_ds=ds_dem,
        stream_pixels_ds=ds_streams,
        allocation_ds=ds_allo,
        distance_ds=ds_dist,
        distance_threshold=50.0,
        elev_threshold=float(thalweg_lateral_elev_threshold),
    )

    # --- 5. MASK BURNED DEM FOR STREAMS ONLY (In-Memory GDAL) ---
    print(f"--> [Step 5] Mask Burned DEM for Thalweg Only (In-Memory) {huc_number} {current_branch_id}")
    ds_flows = gdal_multiply_in_memory(ds_flowdir, ds_streams, nodata_val=0)

    # =========================================================================
    # TAUDEM C++ BOUNDARY FLUSH: Flushes RAM datasets with explicit SRS
    # =========================================================================
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

    # --- 6. FLOW CONDITION STREAMS (TauDEM C++ Subprocess) ---
    print(f"--> [Step 6] Flow Condition Thalweg {huc_number} {current_branch_id}")
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
    print(f"--> [Step 7] D8 Slopes from DEM {huc_number} {current_branch_id}")
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
    print(f"--> [Step 8] Stream Net for Reaches {huc_number} {current_branch_id}")
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

    # --- 9. SPLIT DERIVED REACHES ---
    print(f"--> [Step 9] Split Derived Reaches {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "split_flows.py",
        [
            "-f",
            tempCurrentBranchDataDir / f"demDerived_reaches_{current_branch_id}.shp",
            "-d",
            tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif",
            "-s",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_{current_branch_id}.gpkg",
            "-p",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_points_{current_branch_id}.gpkg",
            "-w",
            tempHucDataDir / "wbd8_clp.gpkg",
            "-l",
            tempHucDataDir / "nwm_lakes_proj_subset.gpkg",
            "-n",
            b_arg,
            "-m",
            max_split_distance_meters,
            "-t",
            slope_min,
            "-b",
            lakes_buffer_dist_meters,
        ],
    )

    # --- 10. GAGE WATERSHED FOR REACHES (TauDEM C++ Subprocess) ---
    print(f"--> [Step 10] Gage Watershed for Reaches {huc_number} {current_branch_id}")
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

    # --- 11. VECTORIZE FEATURE ID CENTROIDS ---
    print(f"--> [Step 11] Vectorize Pixel Centroids {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "reachID_grid_to_vector_points.py",
        [
            "-r",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif",
            "-i",
            "featureID",
            "-p",
            tempCurrentBranchDataDir / f"flows_points_pixels_{current_branch_id}.gpkg",
        ],
    )

    # --- 12. GAGE WATERSHED FOR PIXELS (TauDEM C++ Subprocess) ---
    print(f"--> [Step 12] Gage Watershed for Pixels {huc_number} {current_branch_id}")
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

    # --- 13. CATCH AND MITIGATE BRANCH OUTLET BACKPOOL ERROR ---
    print(
        f"--> [Step 13] Catching and mitigating branch outlet backpool issue {huc_number} {current_branch_id}"
    )
    run_python_script(
        srcDir / "mitigate_branch_outlet_backpool.py",
        [
            "-b",
            tempCurrentBranchDataDir,
            "-cp",
            tempCurrentBranchDataDir / f"gw_catchments_pixels_{current_branch_id}.tif",
            "-cpp",
            tempCurrentBranchDataDir / f"gw_catchments_pixels_{current_branch_id}.gpkg",
            "-cr",
            tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif",
            "-s",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_{current_branch_id}.gpkg",
            "-p",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_points_{current_branch_id}.gpkg",
            "-n",
            b_arg,
            "-d",
            tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif",
            "-t",
            slope_min,
            "--calculate-stats",
        ],
    )

    # --- 14. D8 REM ---
    print(f"--> [Step 14] D8 REM {huc_number} {current_branch_id}")
    ds_dem_cond = gdal.Open(str(tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif"))
    ds_gw_pixels = gdal.Open(str(tempCurrentBranchDataDir / f"gw_catchments_pixels_{current_branch_id}.tif"))

    ds_rem = create_rem_in_memory(
        dem_ds=ds_dem_cond, pixel_watersheds_ds=ds_gw_pixels, thalweg_ds=ds_streams, nodata_val=ndv
    )

    # PERSIST REM TO DISK FOR DOWNSTREAM TEMPLATE RASTERIZATION
    rem_tif_path = tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"
    persist_dataset(ds_rem, rem_tif_path, srs_wkt=srs_wkt, force=True)

    # --- 15. ZERO & MASK REM TO CATCHMENTS (In-Memory GDAL) ---
    print(
        f"--> [Step 15] Bring negative values in REM to zero and mask (In-Memory) {huc_number} {current_branch_id}"
    )
    ds_gw_reach = gdal.Open(str(tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif"))
    ds_rem_zero = gdal_rem_zero_mask_in_memory(ds_rem, ds_gw_reach, nodata_val=ndv)
    persist_dataset(
        ds_rem_zero, tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif", srs_wkt
    )

    # --- 16. RASTERIZE LANDSEA POLYGON ---
    landsea_subset = tempHucDataDir / "LandSea_subset.gpkg"
    landsea_tif = tempCurrentBranchDataDir / f"LandSea_subset_{current_branch_id}.tif"
    if landsea_subset.is_file():
        print(f"--> [Step 16] Rasterize ocean/Glake polygon {huc_number} {current_branch_id}")
        gdal_rasterize_vector(
            src_vector=str(landsea_subset),
            template_raster=str(tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"),
            dst_raster=str(landsea_tif),
            burn_value=ndv,
            init_value=1,
        )

    # --- 17. POLYGONIZE REACH WATERSHEDS ---
    print(f"--> [Step 17] Polygonize Reach Watersheds {huc_number} {current_branch_id}")
    gdal_polygonize_raster(
        in_raster=tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif",
        out_gpkg=tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.gpkg",
        layer_name="catchments",
        field_name="HydroID",
    )

    # --- 18. PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ---
    print(f"--> [Step 18] Process catchments and model streams {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "filter_catchments_and_add_attributes.py",
        [
            "-i",
            tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.gpkg",
            "-f",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_{current_branch_id}.gpkg",
            "-c",
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.gpkg",
            "-o",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_filtered_{current_branch_id}.gpkg",
            "-w",
            tempHucDataDir / "wbd8_clp.gpkg",
            "-u",
            huc_number,
        ],
    )

    # --- 19. RASTERIZE NEW CATCHMENTS AGAIN ---
    print(f"--> [Step 19] Rasterize filtered catchments {huc_number} {current_branch_id}")
    gdal_rasterize_vector(
        src_vector=str(
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.gpkg"
        ),
        template_raster=str(tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"),
        dst_raster=str(
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.tif"
        ),
        attribute="HydroID",
        init_value=0,
    )

    # --- 20. MASK SLOPE TO CATCHMENTS ---
    print(f"--> [Step 20] Mask slopes to catchments {huc_number} {current_branch_id}")
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

    # --- 21. MAKE CATCHMENT AND STAGE FILES ---
    print(f"--> [Step 21] Generate Catchment List and Stage List Files {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "make_stages_and_catchlist.py",
        [
            "-f",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_filtered_{current_branch_id}.gpkg",
            "-c",
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.gpkg",
            "-s",
            tempCurrentBranchDataDir / f"stage_{current_branch_id}.txt",
            "-a",
            tempCurrentBranchDataDir / f"catch_list_{current_branch_id}.txt",
            "-m",
            stage_min_meters,
            "-i",
            stage_interval_meters,
            "-t",
            stage_max_meters,
        ],
    )

    # --- 22. MASK REM RASTER TO REMOVE OCEAN AREAS ---
    if landsea_tif.is_file():
        print(
            f"--> [Step 22] Additional masking to REM raster to remove ocean/Glake areas {huc_number} {current_branch_id}"
        )
        ds_landsea = gdal.Open(str(landsea_tif))
        ds_rem_zero = gdal_multiply_in_memory(ds_rem_zero, ds_landsea, nodata_val=ndv)
        persist_dataset(
            ds_rem_zero, tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif", srs_wkt
        )

    # --- 23. HEAL HAND (NON-BRANCH ZERO) ---
    if is_healed_hand and current_branch_id != branch_zero_id:
        print(
            f"--> [Step 23] Healed HAND to Remove Hydro-conditioning Artifacts {huc_number} {current_branch_id}"
        )
        ds_dem_orig = gdal.Open(str(tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif"))
        ds_rem_zero = gdal_heal_hand_in_memory(ds_rem_zero, ds_dem_orig, ds_dem_cond, nodata_val=ndv)
        persist_dataset(
            ds_rem_zero, tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif", srs_wkt
        )

    # --- 24. HYDRAULIC PROPERTIES (TauDEM C++ Subprocess) ---
    print(f"--> [Step 24] Sample reach averaged parameters {huc_number} {current_branch_id}")
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

    # --- 25. FINALIZE CATCHMENTS AND MODEL STREAMS ---
    print(f"--> [Step 25] Finalize catchments and model streams {huc_number} {current_branch_id}")
    run_python_script(
        srcDir / "add_crosswalk.py",
        [
            "-d",
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.gpkg",
            "-a",
            tempCurrentBranchDataDir / f"demDerived_reaches_split_filtered_{current_branch_id}.gpkg",
            "-s",
            tempCurrentBranchDataDir / f"src_base_{current_branch_id}.csv",
            "-l",
            tempCurrentBranchDataDir
            / f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg",
            "-f",
            tempCurrentBranchDataDir
            / f"demDerived_reaches_split_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg",
            "-r",
            tempCurrentBranchDataDir / f"src_full_crosswalked_{current_branch_id}.csv",
            "-j",
            tempCurrentBranchDataDir / f"src_{current_branch_id}.json",
            "-x",
            tempCurrentBranchDataDir / f"crosswalk_table_{current_branch_id}.csv",
            "-t",
            tempCurrentBranchDataDir / f"hydroTable_{current_branch_id}.csv",
            "-w",
            tempHucDataDir / "wbd8_clp.gpkg",
            "-b",
            b_arg,
            "-u",
            huc_number,
            "-m",
            manning_n,
            "-k",
            tempCurrentBranchDataDir / f"small_segments_{current_branch_id}.csv",
            "-e",
            min_catchment_area,
            "-g",
            min_stream_length,
            "-i",
            iris_sword_slope,
            "-p",
            hfab_ransac_slope,
        ],
    )

    # --- 26. HEAL HAND (BRANCH ZERO) ---
    if is_healed_hand and current_branch_id == branch_zero_id:
        print(
            f"--> [Step 26] Healed HAND to Remove Hydro-conditioning Artifacts {huc_number} {current_branch_id}"
        )
        ds_dem_orig = gdal.Open(str(tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif"))
        ds_rem_zero = gdal_heal_hand_in_memory(ds_rem_zero, ds_dem_orig, ds_dem_cond, nodata_val=ndv)
        persist_dataset(
            ds_rem_zero, tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif", srs_wkt
        )

    # --- 27. HEAL HAND BRIDGES ---
    osm_bridges = tempHucDataDir / "osm_bridges_subset.gpkg"
    if osm_bridges.is_file():
        print(f"--> [Step 27] Burn in bridges {huc_number} {current_branch_id}")
        run_python_script(
            srcDir / "heal_bridges_osm.py",
            [
                "-g",
                tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
                "-d",
                tempCurrentBranchDataDir / f"bridge_elev_diff_meters_{current_branch_id}.tif",
                "-s",
                osm_bridges,
                "-b1",
                "10",
                "-b2",
                "1.5",
                "-p",
                tempCurrentBranchDataDir
                / f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg",
                "-c",
                tempCurrentBranchDataDir / f"osm_bridge_centroids_{current_branch_id}.gpkg",
            ],
        )

    # --- 28. PROCESS ROADS FIMpact ---
    osm_roads = tempHucDataDir / "osm_roads_subset.gpkg"
    if osm_roads.is_file():
        print(f"--> [Step 28] Process roads FIMpact {huc_number} {current_branch_id}")
        run_python_script(
            srcDir / "process_roads_fimpact.py",
            [
                "-g",
                tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
                "-r",
                osm_roads,
                "-c",
                tempCurrentBranchDataDir
                / f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg",
                "-o",
                tempCurrentBranchDataDir / f"osm_roads_fimpact_{current_branch_id}.csv",
            ],
        )

    # --- 29. PROCESS BUILDINGS FIMpact ---
    buildings_subset = tempHucDataDir / "buildings_subset.gpkg"
    if buildings_subset.is_file():
        print(f"--> [Step 29] Process buildings FIMpact {huc_number} {current_branch_id}")
        run_python_script(
            srcDir / "process_buildings_fimpact.py",
            [
                "-g",
                tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
                "-r",
                buildings_subset,
                "-c",
                tempCurrentBranchDataDir
                / f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg",
                "-o",
                tempCurrentBranchDataDir / f"buildings_fimpact_{current_branch_id}.csv",
            ],
        )

    # --- 30. EVALUATE CROSSWALK ---
    if current_branch_id == branch_zero_id and evaluate_crosswalk == "1":
        print(f"--> [Step 30] Evaluate crosswalk {huc_number} {current_branch_id}")
        run_python_script(
            toolsDir / "evaluate_crosswalk.py",
            [
                "-a",
                tempCurrentBranchDataDir
                / f"demDerived_reaches_split_filtered_addedAttributes_crosswalked_{current_branch_id}.gpkg",
                "-b",
                b_arg,
                "-c",
                tempHucDataDir / f"crosswalk_evaluation_{current_branch_id}.csv",
                "-d",
                tempHucDataDir / "nwm_headwater_points_subset.gpkg",
                "-u",
                huc_number,
                "-z",
                current_branch_id,
            ],
        )

    # --- 31. CONVERSION TO INT16 ---
    if huc2Identifier == 19:
        print("--> [Step 31] Skipping Int16 Conversion for Alaska HUC")
    else:
        print(f"--> [Step 31] Convert GW Catchments and REM to Int16 {huc_number} {current_branch_id}")
        run_python_script(toolsDir / "convert_to_int16.py", ["-b", tempCurrentBranchDataDir])

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
