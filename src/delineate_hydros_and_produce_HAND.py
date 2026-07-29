#!/usr/bin/env python3
"""
Exact 1:1 Python Modernization of delineate_hydros_and_produce_HAND.sh
----------------------------------------------------------------------
Executes every single subroutine, condition, argument, and script call from the
bash script. Replaces shell gdal_calc, gdal_rasterize, and gdal_polygonize calls
with modernized, high-performance GDAL Python C-API & NumPy operations.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr

from utils.gdal_cli_utils import export_vsimem_to_disk, gdal_rasterize_vector


gdal.UseExceptions()

SRC_DIR = Path(__file__).resolve().parent


# ------------------------------------------------------------------------------
# 1. Environment & Utility Helpers
# ------------------------------------------------------------------------------
def get_env(key: str, default: str = "") -> str:
    """Gets an environment variable or returns default."""
    return os.getenv(key, default)


def run_cmd(cmd: list, env: dict = None):
    """Executes a command-line program with strict error checking (-e behavior)."""
    print(f"--> [Exec] {' '.join(str(c) for c in cmd)}")
    res = subprocess.run(cmd, env=env or os.environ.copy(), capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(
            f"Command failed with code {res.returncode}:\n"
            f"Command: {' '.join(str(c) for c in cmd)}\n"
            f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"
        )
    return res.stdout


def run_python_script(script_path: Path, args: list):
    """Executes an internal Python script using sys.executable."""
    if not script_path.is_file():
        raise FileNotFoundError(f"Required script missing: {script_path}")
    cmd = [sys.executable, str(script_path)] + [str(a) for a in args]
    run_cmd(cmd)


# ------------------------------------------------------------------------------
# 2. Modernized GDAL Python Operations
# ------------------------------------------------------------------------------
def gdal_calc_multiply(in_raster_a: Path, in_raster_b: Path, out_raster: Path, nodata_val=0):
    """Modernized in-memory GDAL/NumPy replacement for gdal_calc A*B (Int32)."""
    ds_a = gdal.Open(str(in_raster_a))
    ds_b = gdal.Open(str(in_raster_b))

    arr_a = ds_a.GetRasterBand(1).ReadAsArray()
    arr_b = ds_b.GetRasterBand(1).ReadAsArray()

    calc_res = (arr_a * arr_b).astype(np.int32)

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(
        str(out_raster),
        ds_a.RasterXSize,
        ds_a.RasterYSize,
        1,
        gdal.GDT_Int32,
        options=["COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES"],
    )
    out_ds.SetGeoTransform(ds_a.GetGeoTransform())
    out_ds.SetProjection(ds_a.GetProjection())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(int(nodata_val))
    band.WriteArray(calc_res)
    out_ds.FlushCache()

    ds_a = None
    ds_b = None
    out_ds = None


def gdal_calc_rem_zero_mask(rem_path: Path, gw_reach_path: Path, out_path: Path, ndv_val):
    """
    Modernized GDAL/NumPy replacement for:
    gdal_calc.py --calc="(A*(A>=0)*(B>0))" --NoDataValue=$ndv
    """
    ds_a = gdal.Open(str(rem_path))
    ds_b = gdal.Open(str(gw_reach_path))

    arr_a = ds_a.GetRasterBand(1).ReadAsArray().astype(np.float32)
    arr_b = ds_b.GetRasterBand(1).ReadAsArray().astype(np.float32)

    try:
        ndv_float = float(ndv_val)
    except (ValueError, TypeError):
        ndv_float = -9999.0

    mask = (arr_a >= 0) & (arr_b > 0)
    calc_res = np.where(mask, arr_a, ndv_float).astype(np.float32)

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(
        str(out_path),
        ds_a.RasterXSize,
        ds_a.RasterYSize,
        1,
        gdal.GDT_Float32,
        options=["COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES"],
    )
    out_ds.SetGeoTransform(ds_a.GetGeoTransform())
    out_ds.SetProjection(ds_a.GetProjection())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(ndv_float)
    band.WriteArray(calc_res)
    out_ds.FlushCache()

    ds_a = None
    ds_b = None
    out_ds = None


def gdal_calc_heal_hand(rem_path: Path, dem_path: Path, thalweg_cond_path: Path, out_path: Path, ndv_val):
    """
    Modernized GDAL/NumPy replacement for Healed HAND:
    gdal_calc.py -R rem -D dem -T thalweg --calc="R+(D-T)" --NoDataValue=$ndv
    """
    ds_r = gdal.Open(str(rem_path))
    ds_d = gdal.Open(str(dem_path))
    ds_t = gdal.Open(str(thalweg_cond_path))

    arr_r = ds_r.GetRasterBand(1).ReadAsArray().astype(np.float32)
    arr_d = ds_d.GetRasterBand(1).ReadAsArray().astype(np.float32)
    arr_t = ds_t.GetRasterBand(1).ReadAsArray().astype(np.float32)

    try:
        ndv_float = float(ndv_val)
    except (ValueError, TypeError):
        ndv_float = -9999.0

    valid = arr_r != ndv_float
    calc_res = np.copy(arr_r)
    calc_res[valid] = arr_r[valid] + (arr_d[valid] - arr_t[valid])

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(
        str(out_path),
        ds_r.RasterXSize,
        ds_r.RasterYSize,
        1,
        gdal.GDT_Float32,
        options=["COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES"],
    )
    out_ds.SetGeoTransform(ds_r.GetGeoTransform())
    out_ds.SetProjection(ds_r.GetProjection())

    band = out_ds.GetRasterBand(1)
    band.SetNoDataValue(ndv_float)
    band.WriteArray(calc_res)
    out_ds.FlushCache()

    ds_r = None
    ds_d = None
    ds_t = None
    out_ds = None


def gdal_polygonize_raster(in_raster: Path, out_gpkg: Path, layer_name: str, field_name: str):
    """Modernized GDAL Polygonize replacement for gdal_polygonize.py -q -8 -f GPKG."""
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
# 3. Main Workflow Orchestration
# ------------------------------------------------------------------------------
def delineate_and_produce_hand(level: str = "unit", huc_number: str = None, temp_huc_dir: Path = None):
    # Resolve Environment Variables exactly as set in bash_variables.env & params.env
    srcDir = Path(get_env("srcDir", str(SRC_DIR)))
    toolsDir = Path(get_env("toolsDir", str(SRC_DIR.parent / "tools")))
    taudemDir = get_env("taudemDir", "/dependencies/taudem/bin")
    taudemDir2 = get_env("taudemDir2", "/dependencies/taudem_accelerated_flowDirections/taudem/build/bin")

    hucNumber = huc_number or get_env("hucNumber")
    current_branch_id = get_env("current_branch_id", "0")
    branch_zero_id = get_env("branch_zero_id", "0")

    tempHucDataDir = temp_huc_dir or Path(get_env("tempHucDataDir"))
    tempCurrentBranchDataDir = Path(get_env("tempCurrentBranchDataDir", str(tempHucDataDir)))

    huc2Identifier = int(hucNumber[:2]) if hucNumber and len(hucNumber) >= 2 else 0

    # Read config parameters
    ncores_fd = get_env("ncores_fd", "1")
    ncores_gw = get_env("ncores_gw", "1")
    # burn_depth = get_env("burn_depth", "10.0")
    mask_leveed_area_toggle = get_env("mask_leveed_area_toggle", "False")
    branch_id_attribute = get_env("branch_id_attribute", "levpa_id")
    levee_id_attribute = get_env("levee_id_attribute", "feature_id")
    thalweg_lateral_elev_threshold = get_env("thalweg_lateral_elev_threshold", "2")
    max_split_distance_meters = get_env("max_split_distance_meters", "1500")
    slope_min = get_env("slope_min", "0.00001")
    lakes_buffer_dist_meters = get_env("lakes_buffer_dist_meters", "150")
    ndv = get_env("ndv", "-9999")
    stage_min_meters = get_env("stage_min_meters", "0")
    stage_interval_meters = get_env("stage_interval_meters", "0.1")
    stage_max_meters = get_env("stage_max_meters", "20")
    healed_hand_hydrocondition = get_env("healed_hand_hydrocondition", "false").lower() == "true"
    manning_n = get_env("manning_n", "0.05")
    min_catchment_area = get_env("min_catchment_area", "0")
    min_stream_length = get_env("min_stream_length", "0")
    iris_sword_slope = get_env("iris_sword_slope", "0.0001")
    hfab_ransac_slope = get_env("hfab_ransac_slope", "0.0001")
    evaluateCrosswalk = get_env("evaluateCrosswalk", "0")

    # Determine b_arg and z_arg
    if level == "branch":
        b_arg = tempCurrentBranchDataDir / f"nwm_subset_streams_levelPaths_{current_branch_id}.gpkg"
        z_arg = tempCurrentBranchDataDir / f"nwm_catchments_proj_subset_levelPaths_{current_branch_id}.gpkg"
    else:
        b_arg = tempHucDataDir / "nwm_subset_streams.gpkg"
        z_arg = tempHucDataDir / "nwm_catchments_proj_subset.gpkg"

    # --- 1. MASK LEVEE-PROTECTED AREAS FROM DEM ---
    levee_subset = tempHucDataDir / "LeveeProtectedAreas_subset.gpkg"
    if mask_leveed_area_toggle == "True" and levee_subset.is_file():
        print(f"--> Mask levee-protected areas from DEM {hucNumber} {current_branch_id}")
        run_python_script(
            srcDir / "mask_dem.py",
            [
                "-dem",
                tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif",
                "-nld",
                levee_subset,
                "-catchments",
                z_arg,
                "-out",
                tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif",
                "-b",
                branch_id_attribute,
                "-i",
                current_branch_id,
                "-b0",
                branch_zero_id,
                "-csv",
                tempHucDataDir / "levee_levelpaths.csv",
                "-l",
                levee_id_attribute,
            ],
        )

    # --- 2. D8 FLOW ACCUMULATIONS ---
    print(f"--> D8 Flow Accumulations {hucNumber} {current_branch_id}")
    run_python_script(
        srcDir / "accumulate_headwaters.py",
        [
            "-fd",
            tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{current_branch_id}.tif",
            "-fa",
            tempCurrentBranchDataDir / f"flowaccum_d8_burned_filled_{current_branch_id}.tif",
            "-wg",
            tempCurrentBranchDataDir / f"headwaters_{current_branch_id}.tif",
            "-stream",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif",
            "-thresh",
            "1",
        ],
    )

    # --- 3. PREPROCESSING FOR LATERAL THALWEG ADJUSTMENT ---
    print(f"--> Preprocessing for lateral thalweg adjustment {hucNumber} {current_branch_id}")
    run_python_script(
        srcDir / "unique_pixel_and_allocation.py",
        [
            "-s",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif",
            "-o",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_ids_{current_branch_id}.tif",
        ],
    )

    # --- 4. ADJUST THALWEG MINIMUM USING LATERAL ZONAL MINIMUM ---
    print(f"--> Performing lateral thalweg adjustment {hucNumber} {current_branch_id}")
    run_python_script(
        srcDir / "adjust_thalweg_lateral.py",
        [
            "-e",
            tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif",
            "-s",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif",
            "-a",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_ids_{current_branch_id}_allo.tif",
            "-d",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_ids_{current_branch_id}_dist.tif",
            "-t",
            "50",
            "-o",
            tempCurrentBranchDataDir / f"dem_lateral_thalweg_adj_{current_branch_id}.tif",
            "-th",
            thalweg_lateral_elev_threshold,
        ],
    )

    # --- 5. MASK BURNED DEM FOR STREAMS ONLY (In-Memory GDAL) ---
    print(f"--> Mask Burned DEM for Thalweg Only {hucNumber} {current_branch_id}")
    gdal_calc_multiply(
        in_raster_a=tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{current_branch_id}.tif",
        in_raster_b=tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif",
        out_raster=tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_flows_{current_branch_id}.tif",
        nodata_val=0,
    )

    # --- 6. FLOW CONDITION STREAMS ---
    print(f"--> Flow Condition Thalweg {hucNumber} {current_branch_id}")
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

    # --- 7. D8 SLOPES ---
    print(f"--> D8 Slopes from DEM {hucNumber} {current_branch_id}")
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

    # --- 8. STREAMNET FOR REACHES ---
    print(f"--> Stream Net for Reaches {hucNumber} {current_branch_id}")
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
    print(f"--> Split Derived Reaches {hucNumber} {current_branch_id}")
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

    # --- 10. GAGE WATERSHED FOR REACHES ---
    print(f"--> Gage Watershed for Reaches {hucNumber} {current_branch_id}")
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
    print(f"--> Vectorize Pixel Centroids {hucNumber} {current_branch_id}")
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

    # --- 12. GAGE WATERSHED FOR PIXELS ---
    print(f"--> Gage Watershed for Pixels {hucNumber} {current_branch_id}")
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
    print(f"--> Catching and mitigating branch outlet backpool issue {hucNumber} {current_branch_id}")
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
    print(f"--> D8 REM {hucNumber} {current_branch_id}")
    run_python_script(
        srcDir / "make_rem.py",
        [
            "-d",
            tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif",
            "-w",
            tempCurrentBranchDataDir / f"gw_catchments_pixels_{current_branch_id}.tif",
            "-o",
            tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif",
            "-t",
            tempCurrentBranchDataDir / f"demDerived_streamPixels_{current_branch_id}.tif",
        ],
    )

    # --- 15. BRING DISTANCE DOWN TO ZERO & MASK TO CATCHMENTS (In-Memory GDAL) ---
    print(f"--> Bring negative values in REM to zero and mask to catchments {hucNumber} {current_branch_id}")
    gdal_calc_rem_zero_mask(
        rem_path=tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif",
        gw_reach_path=tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif",
        out_path=tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
        ndv_val=ndv,
    )

    # --- 16. RASTERIZE LANDSEA POLYGON (IF APPLICABLE) ---
    landsea_subset = tempHucDataDir / "LandSea_subset.gpkg"
    landsea_tif = tempCurrentBranchDataDir / f"LandSea_subset_{current_branch_id}.tif"
    if landsea_subset.is_file():
        print(f"--> Rasterize filtered/dissolved ocean/Glake polygon {hucNumber} {current_branch_id}")
        # ref_ds = gdal.Open(str(tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"))
        # gt = ref_ds.GetGeoTransform()
        # ncols = ref_ds.RasterXSize
        # nrows = ref_ds.RasterYSize
        # xmin = gt[0]
        # ymax = gt[3]
        # xmax = xmin + gt[1] * ncols
        # ymin = ymax + gt[5] * nrows
        # ref_ds = None

        gdal_rasterize_vector(
            src_vector=str(landsea_subset),
            template_raster=str(tempCurrentBranchDataDir / f"rem_{current_branch_id}.tif"),
            dst_raster=str(landsea_tif),
            burn_value=int(ndv) if ndv != "" else -9999,
            init_value=1,
        )

    # --- 17. POLYGONIZE REACH WATERSHEDS (GDAL C-API) ---
    print(f"--> Polygonize Reach Watersheds {hucNumber} {current_branch_id}")
    gdal_polygonize_raster(
        in_raster=tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.tif",
        out_gpkg=tempCurrentBranchDataDir / f"gw_catchments_reaches_{current_branch_id}.gpkg",
        layer_name="catchments",
        field_name="HydroID",
    )

    # --- 18. PROCESS CATCHMENTS AND MODEL STREAMS STEP 1 ---
    print(f"--> Process catchments and model streams {hucNumber} {current_branch_id}")
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
            hucNumber,
        ],
    )

    # --- 19. RASTERIZE NEW CATCHMENTS AGAIN (GDAL C-API) ---
    print(f"--> Rasterize filtered catchments {hucNumber} {current_branch_id}")
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

    # --- 20. MASK SLOPE TO CATCHMENTS (In-Memory GDAL) ---
    print(f"--> Mask slopes to catchments {hucNumber} {current_branch_id}")
    gdal_calc_multiply(
        in_raster_a=tempCurrentBranchDataDir / f"slopes_d8_dem_meters_{current_branch_id}.tif",
        in_raster_b=tempCurrentBranchDataDir
        / f"gw_catchments_reaches_filtered_addedAttributes_{current_branch_id}.tif",
        out_raster=tempCurrentBranchDataDir / f"slopes_d8_dem_meters_masked_{current_branch_id}.tif",
        nodata_val=ndv,
    )

    # --- 21. MAKE CATCHMENT AND STAGE FILES ---
    print(f"--> Generate Catchment List and Stage List Files {hucNumber} {current_branch_id}")
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
            f"--> Additional masking to REM raster to remove ocean/Glake areas {hucNumber} {current_branch_id}"
        )
        gdal_calc_multiply(
            in_raster_a=tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            in_raster_b=landsea_tif,
            out_raster=tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            nodata_val=ndv,
        )

    # --- 23. HEAL HAND (NON-BRANCH ZERO) ---
    if healed_hand_hydrocondition and current_branch_id != branch_zero_id:
        print(f"--> Healed HAND to Remove Hydro-conditioning Artifacts {hucNumber} {current_branch_id}")
        gdal_calc_heal_hand(
            rem_path=tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            dem_path=tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif",
            thalweg_cond_path=tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif",
            out_path=tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            ndv_val=ndv,
        )

    # --- 24. HYDRAULIC PROPERTIES (TauDEM catchhydrogeo) ---
    print(f"--> Sample reach averaged parameters {hucNumber} {current_branch_id}")
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
    print(f"--> Finalize catchments and model streams {hucNumber} {current_branch_id}")
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
            hucNumber,
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
    if healed_hand_hydrocondition and current_branch_id == branch_zero_id:
        print(f"--> Healed HAND to Remove Hydro-conditioning Artifacts {hucNumber} {current_branch_id}")
        gdal_calc_heal_hand(
            rem_path=tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            dem_path=tempCurrentBranchDataDir / f"dem_meters_{current_branch_id}.tif",
            thalweg_cond_path=tempCurrentBranchDataDir / f"dem_thalwegCond_{current_branch_id}.tif",
            out_path=tempCurrentBranchDataDir / f"rem_zeroed_masked_{current_branch_id}.tif",
            ndv_val=ndv,
        )

    # --- 27. HEAL HAND BRIDGES ---
    osm_bridges = tempHucDataDir / "osm_bridges_subset.gpkg"
    if osm_bridges.is_file():
        print(f"--> Burn in bridges {hucNumber} {current_branch_id}")
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
    else:
        print(f"--> No applicable bridge data for {hucNumber}")

    # --- 28. PROCESS ROADS FIMpact ---
    osm_roads = tempHucDataDir / "osm_roads_subset.gpkg"
    if osm_roads.is_file():
        print(f"--> Process roads FIMpact {hucNumber} {current_branch_id}")
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
    else:
        print(f"--> No osm roads data for {hucNumber}")

    # --- 29. PROCESS BUILDINGS FIMpact ---
    buildings_subset = tempHucDataDir / "buildings_subset.gpkg"
    if buildings_subset.is_file():
        print(f"--> Process buildings FIMpact {hucNumber} {current_branch_id}")
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
    else:
        print(f"--> No buildings data for {hucNumber}")

    # --- 30. EVALUATE CROSSWALK ---
    if current_branch_id == branch_zero_id and evaluateCrosswalk == "1":
        print(f"--> Evaluate crosswalk {hucNumber} {current_branch_id}")
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
                hucNumber,
                "-z",
                current_branch_id,
            ],
        )

    # --- 31. CONVERSION TO INT16 ---
    if huc2Identifier == 19:
        print("--> Skipping Int16 Conversion for Alaska HUC")
    else:
        print(f"--> Convert GW Catchments and REM to Int16 {hucNumber} {current_branch_id}")
        run_python_script(toolsDir / "convert_to_int16.py", ["-b", tempCurrentBranchDataDir])

    print(f"=== [SUCCESS] Completed delineate_hydros_and_produce_HAND for HUC {hucNumber} ===")


def main():
    delineate_and_produce_hand()


if __name__ == "__main__":
    main()
