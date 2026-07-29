#!/usr/bin/env python3
"""
Python modernization of clip_vectors_and_rasters using GDAL bindings.
Prepares WBD, DEM, stream vectors, and derives topological level paths.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

from osgeo import gdal, ogr

from utils.gdal_cli_utils import gdal_raster_warp, gdal_vector_convert, gdal_vector_filter
from utils.shared_functions import get_huc_vars


# Direct import of level path derivation module
try:
    import derive_level_paths
except ImportError:
    from src import derive_level_paths

gdal.UseExceptions()


def clip_vectors_and_rasters(huc_number: str, temp_huc_dir: Path) -> None:
    print(f"=== [GDAL CLI Modernized] Preparing inputs for HUC {huc_number}")

    # 1. Resolve pre_clip_huc_dir
    pre_clip_huc_dir_str = (
        os.getenv("pre_clip_huc_dir") or os.getenv("pre_clip_huc8_dir") or os.getenv("pre_clip_dir")
    )

    inputs_dir = Path(os.getenv("inputsDir", "/data/inputs"))

    pre_clipped_dirs = []
    if pre_clip_huc_dir_str:
        pre_clipped_dirs.extend([Path(pre_clip_huc_dir_str) / huc_number, Path(pre_clip_huc_dir_str)])

    pre_clipped_dirs.extend(
        [inputs_dir / "pre_clip_huc8" / huc_number, inputs_dir / "pre_clip_huc_dir" / huc_number]
    )

    staged_wbd, staged_dem, staged_streams = None, None, None
    for p_dir in pre_clipped_dirs:
        if not p_dir.is_dir():
            continue
        if not staged_wbd:
            for w in [p_dir / "wbd.gpkg", p_dir / f"wbd_{huc_number}.gpkg"]:
                if w.is_file():
                    staged_wbd = w
                    break
        if not staged_dem:
            for d in [p_dir / f"dem_meters_{huc_number}.tif", p_dir / "dem_meters.tif"]:
                if d.is_file():
                    staged_dem = d
                    break
        if not staged_streams:
            for s in [p_dir / "nwm_subset_streams.gpkg", p_dir / f"nwm_subset_streams_{huc_number}.gpkg"]:
                if s.is_file():
                    staged_streams = s
                    break

    national_wbd_path = inputs_dir / "wbd" / "WBD_National.gpkg"
    input_dem_path = Path(os.getenv("input_DEM", inputs_dir / "3dep" / "3dep_10m_conus.vrt"))
    nwm_streams_path = inputs_dir / "nwm_hydrofabric" / "nwm_flows.gpkg"

    target_wbd = temp_huc_dir / f"wbd_{huc_number}.gpkg"
    target_dem = temp_huc_dir / f"dem_meters_{huc_number}.tif"
    target_streams = temp_huc_dir / "nwm_subset_streams.gpkg"

    # --- Step 1: WBD Boundary ---
    if not target_wbd.is_file():
        if staged_wbd and staged_wbd.is_file():
            print(f"--> Using pre-clipped WBD: {staged_wbd} -> {target_wbd}")
            shutil.copy(staged_wbd, target_wbd)
        elif national_wbd_path and national_wbd_path.is_file():
            print(f"--> Filtering HUC {huc_number} boundary from {national_wbd_path}...")
            wbd_layer = f"WBDHU{len(huc_number)}" if len(huc_number) in [2, 4, 6, 8, 10, 12] else "WBDHU8"
            gdal_vector_filter(
                src=str(national_wbd_path),
                dst=str(target_wbd),
                where=f"HUC{len(huc_number)} = '{huc_number}' OR huc{len(huc_number)} = '{huc_number}'",
                layer=wbd_layer,
            )

    # --- Step 2: DEM ---
    if not target_dem.is_file():
        if staged_dem and staged_dem.is_file():
            print(f"--> Using pre-clipped DEM: {staged_dem} -> {target_dem}")
            shutil.copy(staged_dem, target_dem)
        elif input_dem_path and input_dem_path.is_file():
            print("--> Cropping regional input_DEM using 'gdal raster warp'...")
            gdal_raster_warp(
                src=str(input_dem_path), dst=str(target_dem), cutline=str(target_wbd), crop_to_cutline=True
            )

    # --- Step 3: Stream Network Vector ---
    if not target_streams.is_file():
        if staged_streams and staged_streams.is_file():
            print(f"--> Using pre-clipped streams: {staged_streams} -> {target_streams}")
            shutil.copy(staged_streams, target_streams)
        elif nwm_streams_path and nwm_streams_path.is_file():
            print("--> Clipping stream network vector...")
            gdal_vector_convert(src=str(nwm_streams_path), dst=str(target_streams), clipsrc=str(target_wbd))

    # --- Step 4: Derive Level Paths directly in Python ---
    vec_ds = ogr.Open(str(target_streams))
    if vec_ds:
        layer = vec_ds.GetLayer()
        defn = layer.GetLayerDefn()
        cols = [defn.GetFieldDefn(i).GetName().lower() for i in range(defn.GetFieldCount())]
        vec_ds = None

        if "levpa_id" not in cols:
            print("--> [Python Call] Deriving level paths via derive_level_paths module...")

            # Execute Python module entry point directly
            if hasattr(derive_level_paths, "derive_level_paths"):
                derive_level_paths.derive_level_paths(
                    streams_path=str(target_streams),
                    wbd_path=str(target_wbd),
                    huc_number=huc_number,
                    output_dir=str(temp_huc_dir),
                )
            elif hasattr(derive_level_paths, "main"):
                # Pass CLI arguments via sys.argv if main() expects argument parsing
                sys_args_backup = sys.argv
                sys.argv = [
                    "derive_level_paths.py",
                    "-s",
                    str(target_streams),
                    "-w",
                    str(target_wbd),
                    "-u",
                    huc_number,
                    "-o",
                    str(temp_huc_dir),
                ]
                try:
                    derive_level_paths.main()
                finally:
                    sys.argv = sys_args_backup

    print(f"=== Successfully prepared all inputs for HUC {huc_number}")
