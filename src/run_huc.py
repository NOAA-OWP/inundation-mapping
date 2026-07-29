#!/usr/bin/env python3
"""
Exact 1:1 Python Modernization of run_huc.sh
---------------------------------------------
Handles HUC-level pre-clipped data staging, level path derivation, AGREE DEM
stream burning, pit removal, branch zero HAND & REM creation, and parallel
branch execution using native GDAL C-API bindings.
"""

import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from osgeo import gdal

import delineate_hydros_and_produce_HAND


gdal.UseExceptions()

SRC_DIR = Path(__file__).resolve().parent


def get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def load_environment_params():
    """Parses bash_variables.env, params_template.env, params.env, and runtime_args.env into os.environ cleanly."""
    project_root = SRC_DIR.parent
    run_name = get_env("runName")

    candidate_files = [
        SRC_DIR / "bash_variables.env",
        project_root / "config" / "params_template.env",
        project_root / "config" / "params.env",
        Path("/fim_temp") / run_name / "runtime_args.env",
        Path("/outputs") / run_name / "runtime_args.env",
        Path("/fim_temp") / run_name / "params.env",
        Path("/outputs") / run_name / "params.env",
    ]

    parsed_vars = {
        "dataDir": os.environ.get("dataDir", "/data"),
        "inputsDir": os.environ.get("inputsDir", "/data/inputs"),
        "outputsDir": os.environ.get("outputsDir", "/outputs"),
        "projectDir": os.environ.get("projectDir", "/foss_fim"),
        "srcDir": os.environ.get("srcDir", "/foss_fim/src"),
        "toolsDir": os.environ.get("toolsDir", "/foss_fim/tools"),
        "workDir": os.environ.get("workDir", "/fim_temp"),
        "taudemDir": os.environ.get("taudemDir", "/dependencies/taudem/bin"),
        "taudemDir2": os.environ.get(
            "taudemDir2", "/dependencies/taudem_accelerated_flowDirections/taudem/build/bin"
        ),
    }

    for k, v in parsed_vars.items():
        os.environ[k] = v

    for env_file in candidate_files:
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
                        parsed_vars[key] = val

    for _ in range(3):
        for k, v in list(parsed_vars.items()):
            expanded = os.path.expandvars(v)
            for var_k, var_v in parsed_vars.items():
                expanded = expanded.replace(f"${{{var_k}}}", var_v).replace(f"${var_k}", var_v)
            parsed_vars[k] = expanded
            os.environ[k] = expanded


def run_python_script(script_path: Path, args: list, allow_codes: list = None) -> int:
    """Executes a repository Python script cleanly via sys.executable."""
    if not script_path.is_file():
        raise FileNotFoundError(f"Required script missing: {script_path}")

    allow_codes = allow_codes or [0]
    cmd = [sys.executable, str(script_path)] + [str(a) for a in args]
    print(f"--> [Exec] {' '.join(cmd)}")

    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.stdout:
        print(res.stdout.strip())
    if res.stderr and res.returncode not in allow_codes:
        print(res.stderr.strip(), file=sys.stderr)

    if res.returncode not in allow_codes:
        raise RuntimeError(
            f"Script execution failed ({res.returncode}): {' '.join(cmd)}\n"
            f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"
        )
    return res.returncode


def get_crs_for_huc(huc_number: str) -> str:
    tools_dir = Path(get_env("toolsDir"))
    script = tools_dir / "get_crs_for_huc.py"
    if script.is_file():
        try:
            res = subprocess.run(
                [sys.executable, str(script), "-u", str(huc_number)],
                capture_output=True,
                text=True,
                check=True,
            )
            return res.stdout.strip()
        except Exception:
            pass
    return "EPSG:5070"


def get_raster_info_native(raster_path: Path) -> dict:
    ds = gdal.Open(str(raster_path))
    gt = ds.GetGeoTransform()
    ncols = ds.RasterXSize
    nrows = ds.RasterYSize
    band = ds.GetRasterBand(1)
    ndv = band.GetNoDataValue()
    if ndv is None:
        ndv = -9999

    xmin = gt[0]
    ymax = gt[3]
    cellsize_resx = gt[1]
    cellsize_resy = abs(gt[5])
    xmax = xmin + cellsize_resx * ncols
    ymin = ymax - cellsize_resy * nrows

    ds = None
    return {
        "ncols": ncols,
        "nrows": nrows,
        "ndv": ndv,
        "xmin": xmin,
        "ymin": ymin,
        "xmax": xmax,
        "ymax": ymax,
        "cellsize_resx": cellsize_resx,
        "cellsize_resy": cellsize_resy,
    }


def warp_dem_to_cutline(src_path: str, dst_path: str, cutline_path: str, target_srs: str, res_meters: float):
    """Native GDAL C-API replacement for gdalwarp cutline clipping."""
    options = gdal.WarpOptions(
        format="GTiff",
        outputType=gdal.GDT_Float32,
        resampleAlg=gdal.GRA_NearestNeighbour,
        cutlineDSName=str(cutline_path),
        cropToCutline=True,
        dstSRS=target_srs,
        xRes=float(res_meters),
        yRes=float(res_meters),
        targetAlignedPixels=True,
        creationOptions=["BLOCKXSIZE=512", "BLOCKYSIZE=512", "TILED=YES", "COMPRESS=LZW", "BIGTIFF=YES"],
    )
    gdal.Warp(str(dst_path), str(src_path), options=options)


def create_combined_dem_vrt(orig_dem: Path, pit_fill_dem: Path, output_tif: Path):
    vrt_path = f"/vsimem/combined_{output_tif.stem}.vrt"
    vrt_options = gdal.BuildVRTOptions(resampleAlg="nearest")
    vrt_ds = gdal.BuildVRT(vrt_path, [str(orig_dem), str(pit_fill_dem)], options=vrt_options)

    translate_options = gdal.TranslateOptions(
        format="GTiff",
        outputType=gdal.GDT_Float32,
        creationOptions=["BLOCKXSIZE=512", "BLOCKYSIZE=512", "TILED=YES", "COMPRESS=LZW", "BIGTIFF=YES"],
    )
    gdal.Translate(str(output_tif), vrt_ds, options=translate_options)
    vrt_ds = None
    gdal.Unlink(vrt_path)


def rasterize_vector_native(
    src_vector: str,
    template_raster: str,
    dst_raster: str,
    attribute: str = None,
    burn_value: float = None,
    init_value: float = 0,
    use_3d: bool = False,
):
    """Native GDAL C-API vector rasterization replacement."""
    ds_tmpl = gdal.Open(str(template_raster))
    gt = ds_tmpl.GetGeoTransform()
    ncols = ds_tmpl.RasterXSize
    nrows = ds_tmpl.RasterYSize
    proj = ds_tmpl.GetProjection()
    ds_tmpl = None

    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + gt[1] * ncols
    ymin = ymax - abs(gt[5]) * nrows

    extra_options = []
    if use_3d:
        extra_options.append("-3d")

    options_kwargs = {
        "format": "GTiff",
        "outputType": gdal.GDT_Float32,
        "outputBounds": [xmin, ymin, xmax, ymax],
        "xRes": abs(gt[1]),
        "yRes": abs(gt[5]),
        "outputSRS": proj,
        "initValues": [init_value],
        "options": extra_options,
        "creationOptions": ["BLOCKXSIZE=512", "BLOCKYSIZE=512", "TILED=YES", "COMPRESS=LZW", "BIGTIFF=YES"],
    }

    if attribute and not use_3d:
        options_kwargs["attribute"] = attribute

    if burn_value is not None:
        options_kwargs["burnValues"] = [burn_value]

    opts = gdal.RasterizeOptions(**options_kwargs)
    gdal.Rasterize(str(dst_raster), str(src_vector), options=opts)


def run_huc_processing(run_name: str = None, huc_number: str = None, temp_huc_dir: Path = None):
    huc_start_time = time.time()

    if not run_name and len(sys.argv) > 1:
        run_name = sys.argv[1]
    if not huc_number and len(sys.argv) > 2:
        huc_number = sys.argv[2]

    runName = run_name or get_env("runName")
    hucNumber = huc_number or get_env("hucNumber")

    if not hucNumber:
        raise ValueError("Environment variable 'hucNumber' or argument huc_number must be provided.")

    os.environ["runName"] = runName
    os.environ["hucNumber"] = hucNumber

    # Auto-load config parameters strictly into os.environ
    load_environment_params()

    srcDir = Path(get_env("srcDir"))
    pre_clip_huc_dir = Path(get_env("pre_clip_huc_dir"))
    ras2fim_input_dir = Path(get_env("ras2fim_input_dir"))

    tempHucDataDir = temp_huc_dir or Path(get_env("tempHucDataDir"))
    tempBranchDataDir = tempHucDataDir / "branches"
    tempHucDataDir.mkdir(parents=True, exist_ok=True)
    tempBranchDataDir.mkdir(parents=True, exist_ok=True)
    (tempHucDataDir / "logs").mkdir(parents=True, exist_ok=True)

    os.environ["tempHucDataDir"] = str(tempHucDataDir)
    os.environ["tempBranchDataDir"] = str(tempBranchDataDir)

    branch_zero_id = get_env("branch_zero_id", "0")
    branch_id_attribute = get_env("branch_id_attribute", "levpa_id")
    branch_buffer_distance_meters = get_env("branch_buffer_distance_meters", "1000")
    res = get_env("res", "10")
    agree_DEM_buffer = get_env("agree_DEM_buffer", "100")
    jobBranchLimit = int(get_env("jobBranchLimit", "4"))
    deny_branch_zero_list = get_env("deny_branch_zero_list", "")
    deny_unit_list = get_env("deny_unit_list", "")
    ncores_fd = get_env("ncores_fd", "1")
    taudemDir2 = get_env("taudemDir2")

    branch_list_csv_file = tempHucDataDir / "branch_ids.csv"
    branch_list_lst_file = tempHucDataDir / "branch_ids_for_huc_processing.lst"

    # Region DEM Selection
    huc2Identifier = int(hucNumber[:2]) if len(hucNumber) >= 2 else 0
    if huc2Identifier == 19:
        huc_input_DEM_domain = get_env("input_DEM_domain_Alaska")
        input_DEM = get_env("input_DEM_Alaska")
        input_pit_fill = get_env("input_DEM_pit_fills_Alaska")
        input_bridge_elev_diff = get_env("input_bridge_elev_diff_alaska")
    elif hucNumber == "22010000":
        huc_input_DEM_domain = get_env("input_DEM_domain_Guam")
        input_DEM = get_env("input_DEM_Guam")
        input_pit_fill = get_env("input_DEM_pit_fills_Guam")
        input_bridge_elev_diff = get_env("input_bridge_elev_diff_guam")
    elif hucNumber == "22030001":
        huc_input_DEM_domain = get_env("input_DEM_domain_AmericanSamoa")
        input_DEM = get_env("input_DEM_AmericanSamoa")
        input_pit_fill = get_env("input_DEM_pit_fills_AmericanSamoa")
        input_bridge_elev_diff = get_env("input_bridge_elev_diff_americansamoa")
    else:
        huc_input_DEM_domain = get_env("input_DEM_domain")
        input_DEM = get_env("input_DEM")
        input_pit_fill = get_env("input_DEM_pit_fills")
        input_bridge_elev_diff = get_env("input_bridge_elev_diff")

    huc_CRS = get_crs_for_huc(hucNumber)
    print(f"--> Using CRS: {huc_CRS}")

    # Stage pre-clipped files from pre_clip_huc_dir
    source_preclip = pre_clip_huc_dir / hucNumber
    print(f"--> Stage pre-clip directory: {source_preclip}")
    shutil.copytree(source_preclip, tempHucDataDir, dirs_exist_ok=True)

    # Promote dem_meters_<hucNumber>.tif to dem_meters.tif if present
    huc_clipped_dem = tempHucDataDir / f"dem_meters_{hucNumber}.tif"
    dem_meters = tempHucDataDir / "dem_meters.tif"
    if huc_clipped_dem.is_file() and not dem_meters.is_file():
        print(f"--> Promoting {huc_clipped_dem.name} to {dem_meters.name}")
        shutil.copy(huc_clipped_dem, dem_meters)

    # Copy input dependencies
    if huc_input_DEM_domain and Path(huc_input_DEM_domain).is_file():
        shutil.copy(huc_input_DEM_domain, tempHucDataDir)

    nws_lid = get_env("nws_lid")
    if nws_lid and Path(nws_lid).is_file():
        shutil.copy(nws_lid, tempHucDataDir / "nws_lid.gpkg")

    usgs_gages_file = get_env("usgs_gages_file")
    if usgs_gages_file and Path(usgs_gages_file).is_file():
        shutil.copy(usgs_gages_file, tempHucDataDir / "usgs_gages.gpkg")

    # Check RAS2FIM rating curve files
    ras_huc_dir = ras2fim_input_dir / hucNumber
    ras_gpkg_name = get_env("ras_rating_curve_gpkg_filename", "reformat_ras_rating_curve_points.gpkg")
    ras_csv_name = get_env("ras_rating_curve_csv_filename", "reformat_ras_rating_curve_table.csv")
    if ras_huc_dir.is_dir():
        if (ras_huc_dir / ras_gpkg_name).is_file():
            shutil.copy(ras_huc_dir / ras_gpkg_name, tempHucDataDir)
        if (ras_huc_dir / ras_csv_name).is_file():
            shutil.copy(ras_huc_dir / ras_csv_name, tempHucDataDir)

    # --- STEP 2: DERIVE LEVEL PATHS ---
    print(f"--> Generating Level Paths for {hucNumber}")
    lp_code = run_python_script(
        srcDir / "derive_level_paths.py",
        [
            "-i",
            tempHucDataDir / "nwm_subset_streams.gpkg",
            "-s",
            tempHucDataDir / "wbd_buffered_streams.gpkg",
            "-b",
            branch_id_attribute,
            "-r",
            "ID",
            "-o",
            tempHucDataDir / "nwm_subset_streams_levelPaths.gpkg",
            "-d",
            tempHucDataDir / "nwm_subset_streams_levelPaths_dissolved.gpkg",
            "-de",
            tempHucDataDir / "nwm_subset_streams_levelPaths_extended.gpkg",
            "-e",
            tempHucDataDir / "nwm_headwaters.gpkg",
            "-c",
            tempHucDataDir / "nwm_catchments_proj_subset.gpkg",
            "-t",
            tempHucDataDir / "nwm_catchments_proj_subset_levelPaths.gpkg",
            "-n",
            tempHucDataDir / "nwm_subset_streams_levelPaths_dissolved_headwaters.gpkg",
            "-w",
            tempHucDataDir / "nwm_lakes_proj_subset.gpkg",
            "-wbd",
            tempHucDataDir / "wbd.gpkg",
            "-u",
            hucNumber,
        ],
        allow_codes=[0, 60],
    )

    if lp_code == 60:
        print(
            f"--> Acceptable Exit Status: 60 - No level paths exist for HUC {hucNumber}. Processing branch zero only."
        )
        levelpaths_exist = False
    else:
        levelpaths_exist = (tempHucDataDir / "nwm_subset_streams_levelPaths_dissolved.gpkg").is_file()

    # Associate Level Paths with Levees
    nld_subset = tempHucDataDir / "nld_subset_levees.gpkg"
    if nld_subset.is_file() and levelpaths_exist:
        print(f"--> Associate level paths with levees for {hucNumber}")
        run_python_script(
            srcDir / "associate_levelpaths_with_levees.py",
            [
                "-nld",
                nld_subset,
                "-s",
                tempHucDataDir / "nwm_subset_streams_levelPaths_dissolved.gpkg",
                "-lpa",
                tempHucDataDir / "LeveeProtectedAreas_subset.gpkg",
                "-out",
                tempHucDataDir / "levee_levelpaths.csv",
                "-w",
                get_env("levee_buffer", "1000"),
                "-b",
                branch_id_attribute,
                "-l",
                get_env("levee_id_attribute", "feature_id"),
            ],
        )

    # Stream Branch Polygons
    if levelpaths_exist:
        print(f"--> Generating Stream Branch Polygons for {hucNumber}")
        run_python_script(
            srcDir / "buffer_stream_branches.py",
            [
                "-s",
                tempHucDataDir / "nwm_subset_streams_levelPaths_dissolved.gpkg",
                "-i",
                branch_id_attribute,
                "-d",
                branch_buffer_distance_meters,
                "-b",
                tempHucDataDir / "branch_polygons.gpkg",
                "-w",
                tempHucDataDir / "wbd_buffered.gpkg",
            ],
        )

        print(f"--> Create list file of branch ids for processing for {hucNumber}")
        run_python_script(
            srcDir / "generate_branch_list.py",
            [
                "-d",
                tempHucDataDir / "nwm_subset_streams_levelPaths_dissolved.gpkg",
                "-b",
                branch_id_attribute,
                "-o",
                branch_list_lst_file,
            ],
        )

    # --- BRANCH ZERO CREATION ---
    print(f"--> Creating branch zero for {hucNumber}")
    tempCurrentBranchDataDir = tempBranchDataDir / str(branch_zero_id)
    tempCurrentBranchDataDir.mkdir(parents=True, exist_ok=True)

    os.environ["tempCurrentBranchDataDir"] = str(tempCurrentBranchDataDir)
    os.environ["current_branch_id"] = str(branch_zero_id)

    # Rasters
    dem_orig = tempHucDataDir / "dem_meters_orig.tif"
    dem_pit_fill = tempHucDataDir / "dem_meters_pit_fill.tif"
    bridge_elev_diff = tempHucDataDir / "bridge_elev_diff_meters.tif"

    cutline_gpkg = tempHucDataDir / "wbd_buffered.gpkg"
    if not cutline_gpkg.is_file():
        cutline_gpkg = tempHucDataDir / "wbd.gpkg"

    # Warp input_DEM if dem_meters.tif does not exist
    if not dem_meters.is_file():
        warp_dem_to_cutline(input_DEM, str(dem_orig), str(cutline_gpkg), huc_CRS, res)

        if input_pit_fill and Path(input_pit_fill).is_file():
            warp_dem_to_cutline(input_pit_fill, str(dem_pit_fill), str(cutline_gpkg), huc_CRS, res)

        if input_bridge_elev_diff and Path(input_bridge_elev_diff).is_file():
            warp_dem_to_cutline(
                input_bridge_elev_diff, str(bridge_elev_diff), str(cutline_gpkg), huc_CRS, res
            )

        if dem_orig.is_file() and dem_pit_fill.is_file():
            create_combined_dem_vrt(dem_orig, dem_pit_fill, dem_meters)
        elif dem_orig.is_file():
            shutil.copy(dem_orig, dem_meters)

    # Export Raster Metadata to Environment BEFORE calling HAND
    raster_info = get_raster_info_native(dem_meters)
    ncols, nrows, ndv = raster_info["ncols"], raster_info["nrows"], str(raster_info["ndv"])
    xmin, ymin, xmax, ymax = (
        raster_info["xmin"],
        raster_info["ymin"],
        raster_info["xmax"],
        raster_info["ymax"],
    )

    os.environ["ncols"] = str(ncols)
    os.environ["nrows"] = str(nrows)
    os.environ["ndv"] = str(ndv)
    os.environ["xmin"] = str(xmin)
    os.environ["ymin"] = str(ymin)
    os.environ["xmax"] = str(xmax)
    os.environ["ymax"] = str(ymax)

    # Rasterize NLD multilines if available using 3D Z-coordinates
    nld_3d_gpkg = tempHucDataDir / "3d_nld_subset_levees_burned.gpkg"
    nld_rasterized = tempCurrentBranchDataDir / f"nld_rasterized_elev_{branch_zero_id}.tif"
    if nld_3d_gpkg.is_file():
        rasterize_vector_native(
            src_vector=str(nld_3d_gpkg),
            template_raster=str(dem_meters),
            dst_raster=str(nld_rasterized),
            use_3d=True,
        )
        print(f"--> Burn nld levees into dem for {hucNumber} {branch_zero_id}")
        run_python_script(
            srcDir / "burn_in_levees.py", ["-dem", dem_meters, "-nld", nld_rasterized, "-out", dem_meters]
        )

    # Rasterize Reach Boolean (Branch 0)
    flows_grid_b0 = tempCurrentBranchDataDir / f"flows_grid_boolean_{branch_zero_id}.tif"
    rasterize_vector_native(
        src_vector=str(tempHucDataDir / "nwm_subset_streams.gpkg"),
        template_raster=str(dem_meters),
        dst_raster=str(flows_grid_b0),
        burn_value=1,
        init_value=0,
    )

    if levelpaths_exist:
        rasterize_vector_native(
            src_vector=str(tempHucDataDir / "nwm_subset_streams_levelPaths_extended.gpkg"),
            template_raster=str(dem_meters),
            dst_raster=str(tempHucDataDir / "flows_grid_boolean.tif"),
            burn_value=1,
            init_value=0,
        )

    # Rasterize Headwaters
    headwaters_b0 = tempCurrentBranchDataDir / f"headwaters_{branch_zero_id}.tif"
    if (tempHucDataDir / "nwm_headwater_points_subset.gpkg").is_file():
        rasterize_vector_native(
            src_vector=str(tempHucDataDir / "nwm_headwater_points_subset.gpkg"),
            template_raster=str(dem_meters),
            dst_raster=str(headwaters_b0),
            burn_value=1,
            init_value=0,
        )

    # DEM Reconditioning (AGREE DEM)
    burned_b0 = tempCurrentBranchDataDir / f"dem_burned_{branch_zero_id}.tif"
    print(f"--> Creating AGREE DEM using {agree_DEM_buffer}m buffer for {hucNumber} {branch_zero_id}")
    run_python_script(
        srcDir / "agreedem.py",
        [
            "-r",
            flows_grid_b0,
            "-d",
            dem_meters,
            "-w",
            tempCurrentBranchDataDir,
            "-o",
            burned_b0,
            "-b",
            agree_DEM_buffer,
            "-sm",
            "10",
            "-sh",
            "1000",
        ],
    )

    # Pit Remove Burned DEM
    burned_filled_b0 = tempCurrentBranchDataDir / f"dem_burned_filled_{branch_zero_id}.tif"
    rd_bin = shutil.which("rd_depression_filling") or "/usr/local/bin/rd_depression_filling"
    subprocess.run([rd_bin, str(burned_b0), str(burned_filled_b0)], check=True)

    # D8 Flow Directions
    p_b0 = tempCurrentBranchDataDir / f"flowdir_d8_burned_filled_{branch_zero_id}.tif"
    run_python_script(
        srcDir / "run_taudem_subprocess.py",
        ["d8flowdir", "-n", ncores_fd, "-t", taudemDir2, "-fel", burned_filled_b0, "-p", p_b0],
    )

    # Copy DEM and Bridge Diff to Branch Zero
    shutil.copy(dem_meters, tempCurrentBranchDataDir / f"dem_meters_{branch_zero_id}.tif")
    if bridge_elev_diff.is_file():
        shutil.copy(
            bridge_elev_diff, tempCurrentBranchDataDir / f"bridge_elev_diff_meters_{branch_zero_id}.tif"
        )

    # PRODUCE BRANCH ZERO HAND
    print(f"--> Executing delineate_hydros_and_produce_HAND for Branch Zero ({hucNumber})")
    delineate_hydros_and_produce_HAND.delineate_and_produce_hand(
        level="unit", huc_number=hucNumber, temp_huc_dir=tempHucDataDir
    )

    # PRODUCE BRANCH ZERO REM using make_rem.py
    print(f"--> Generating REM for Branch Zero ({hucNumber}) using make_rem.py")
    run_python_script(srcDir / "make_rem.py", ["-d", tempCurrentBranchDataDir, "-b", branch_zero_id])

    # USGS Gages & Crosswalk
    if (tempHucDataDir / "nwm_subset_streams_levelPaths.gpkg").is_file():
        run_python_script(
            srcDir / "usgs_gage_unit_setup.py",
            [
                "-gages",
                tempHucDataDir / "usgs_gages.gpkg",
                "-nwm",
                tempHucDataDir / "nwm_subset_streams_levelPaths.gpkg",
                "-ras",
                tempHucDataDir / ras_gpkg_name,
                "-o",
                tempHucDataDir / "usgs_subset_gages.gpkg",
                "-huc",
                hucNumber,
                "-ahps",
                tempHucDataDir / "nws_lid.gpkg",
                "-bzero_id",
                branch_zero_id,
                "-huc_CRS",
                huc_CRS,
            ],
        )

    gages_b0 = tempHucDataDir / f"usgs_subset_gages_{branch_zero_id}.gpkg"
    if gages_b0.is_file():
        run_python_script(
            srcDir / "usgs_gage_crosswalk.py",
            [
                "-gages",
                gages_b0,
                "-flows",
                tempCurrentBranchDataDir / f"demDerived_reaches_split_filtered_{branch_zero_id}.gpkg",
                "-cat",
                tempCurrentBranchDataDir
                / f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_zero_id}.gpkg",
                "-dem",
                tempCurrentBranchDataDir / f"dem_meters_{branch_zero_id}.tif",
                "-dem_adj",
                tempCurrentBranchDataDir / f"dem_thalwegCond_{branch_zero_id}.tif",
                "-out",
                tempCurrentBranchDataDir,
                "-b",
                branch_zero_id,
                "-huc_CRS",
                huc_CRS,
            ],
        )

    # Cleanup Branch Zero Outputs
    if (srcDir / "outputs_cleanup.py").is_file():
        run_python_script(
            srcDir / "outputs_cleanup.py",
            ["-d", tempCurrentBranchDataDir, "-l", deny_branch_zero_list, "-b", branch_zero_id],
        )

    # --- BRANCH PROCESSING (Parallel) ---
    print(f"---- Start of branch processing for {hucNumber} using {jobBranchLimit} workers")

    if branch_list_lst_file.is_file() and levelpaths_exist:
        with open(branch_list_lst_file, "r") as f:
            branches_to_process = [line.strip() for line in f if line.strip()]

        print(f"--> Processing {len(branches_to_process)} branches in parallel...")
        process_branch_script = srcDir / "process_branch.py"

        def _execute_branch(b_id):
            cmd = [sys.executable, str(process_branch_script), runName, hucNumber, str(b_id)]
            res = subprocess.run(cmd, capture_output=True, text=True)
            return (b_id, res.returncode == 0, res.stdout, res.stderr)

        with ProcessPoolExecutor(max_workers=jobBranchLimit) as executor:
            futures = [executor.submit(_execute_branch, b) for b in branches_to_process]
            for future in as_completed(futures):
                b_id, success, out_text, err_text = future.result()
                if success:
                    print(f"--> Branch {b_id} completed successfully.")
                else:
                    print(f"***** Branch {b_id} FAILED *****\nSTDOUT:\n{out_text}\nSTDERR:\n{err_text}")
    else:
        print("--> No level paths exist for this HUC. Completed Branch Zero processing.")

    # Remove files from deny list
    if deny_unit_list and (srcDir / "outputs_cleanup.py").is_file():
        run_python_script(
            srcDir / "outputs_cleanup.py", ["-d", tempHucDataDir, "-l", deny_unit_list, "-b", hucNumber]
        )

    # Calibration Adjustment
    calib_script = srcDir / "calibrate_rating_curves.sh"
    if calib_script.is_file():
        print(f"--> Executing calibrate_rating_curves for {hucNumber}")
        subprocess.run(["bash", str(calib_script), "False", str(jobBranchLimit), str(hucNumber)], check=True)

    # Generate successful branch list CSV
    run_python_script(srcDir / "generate_branch_list_csv.py", ["-o", branch_list_csv_file, "-u", hucNumber])

    total_duration = time.time() - huc_start_time
    print(f"---- HUC processing for {hucNumber} complete in {total_duration:.2f}s")


def main():
    run_huc_processing()


if __name__ == "__main__":
    main()
