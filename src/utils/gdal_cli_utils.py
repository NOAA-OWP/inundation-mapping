#!/usr/bin/env python3
"""
Utility module for modern GDAL operations and Virtual File System (/vsimem/) tasks.
"""

import subprocess
from pathlib import Path

from osgeo import gdal


gdal.UseExceptions()


def run_gdal_cli(subcmd_args: list[str]) -> str:
    """Executes modern GDAL CLI subcommands."""
    full_cmd = ["gdal"] + subcmd_args
    res = subprocess.run(full_cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"GDAL CLI command failed: {' '.join(full_cmd)}\nError: {res.stderr}")
    return res.stdout


def gdal_raster_warp(
    src: str, dst: str, cutline: str = None, crop_to_cutline: bool = True, extra_args: list = None
):
    """Executes raster warping/cropping using gdal.Warp."""
    warp_options = {"format": "GTiff", "xRes": 10.0, "yRes": 10.0, "resampleAlg": "bilinear"}

    if cutline:
        warp_options["cutlineDSName"] = str(cutline)
        warp_options["cropToCutline"] = crop_to_cutline

    opt = gdal.WarpOptions(**warp_options)

    try:
        # Pass dest and src positional arguments directly
        ds = gdal.Warp(str(dst), str(src), options=opt)
        if ds is None:
            raise RuntimeError("gdal.Warp returned None")
        ds = None
    except Exception:
        cmd = ["gdalwarp", "-of", "GTiff", "-tr", "10", "10", "-r", "bilinear"]
        if cutline:
            cmd.extend(["-cutline", str(cutline)])
            if crop_to_cutline:
                cmd.append("-crop_to_cutline")
        cmd.extend([str(src), str(dst)])

        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"gdalwarp failed: {res.stderr}")

    return True


def gdal_rasterize_vector(src_vector: str, template_raster: str, dst_raster: str, burn_value: int = 1):
    """
    Rasterizes a vector layer (e.g. stream network GPKG) onto a grid matching
    a template raster's extent and cell size using gdal.Rasterize.
    """
    # Open template raster to extract extent, projection, and geotransform
    ref_ds = gdal.Open(str(template_raster))
    if not ref_ds:
        raise FileNotFoundError(f"Template raster missing at {template_raster}")

    gt = ref_ds.GetGeoTransform()
    x_res = abs(gt[1])
    y_res = abs(gt[5])
    bounds = [gt[0], gt[3] + gt[5] * ref_ds.RasterYSize, gt[0] + gt[1] * ref_ds.RasterXSize, gt[3]]
    ref_ds = None

    rast_options = gdal.RasterizeOptions(
        format="GTiff",
        outputType=gdal.GDT_Int32,
        initValues=0,
        burnValues=[burn_value],
        xRes=x_res,
        yRes=y_res,
        outputBounds=bounds,
    )

    ds = gdal.Rasterize(str(dst_raster), str(src_vector), options=rast_options)
    if ds is None:
        raise RuntimeError(f"gdal.Rasterize failed for vector {src_vector}")
    ds = None
    return True


def gdal_vector_convert(src: str, dst: str, clipsrc: str = None, extra_args: list = None):
    """Executes vector spatial clipping/conversion."""
    options = ["-f", "GPKG"]

    if clipsrc:
        clip_ds = gdal.OpenEx(str(clipsrc))
        if clip_ds and clip_ds.GetLayerCount() > 0:
            clip_layer_name = clip_ds.GetLayer(0).GetName()
            options.extend(["-clipsrc", str(clipsrc), clip_layer_name])
            clip_ds = None
        else:
            options.extend(["-clipsrc", str(clipsrc)])

    if extra_args:
        options.extend(extra_args)

    try:
        opt = gdal.VectorTranslateOptions(options=options)
        ds = gdal.VectorTranslate(destNameOrDestDS=str(dst), srcDS=str(src), options=opt)
        if ds is None:
            raise RuntimeError("gdal.VectorTranslate returned None")
        ds = None
    except Exception:
        cmd = ["ogr2ogr", "-f", "GPKG"]
        if clipsrc:
            cmd.extend(["-clipsrc", str(clipsrc)])
        if extra_args:
            cmd.extend(extra_args)
        cmd.extend([str(dst), str(src)])

        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"ogr2ogr clipping failed: {res.stderr}")

    return True


def gdal_vector_filter(src: str, dst: str, where: str = None, layer: str = None, extra_args: list = None):
    """
    Executes attribute filtering via gdal.VectorTranslate / ogr2ogr.
    Targets a specific vector layer (e.g., 'WBDHU8').
    """
    options = ["-f", "GPKG"]
    if where:
        options.extend(["-where", where])
    if extra_args:
        options.extend(extra_args)

    layers_list = [layer] if layer else None

    try:
        opt = gdal.VectorTranslateOptions(options=options, layers=layers_list)
        ds = gdal.VectorTranslate(destNameOrDestDS=str(dst), srcDS=str(src), options=opt)
        if ds is None:
            raise RuntimeError("gdal.VectorTranslate returned None")
        ds = None
    except Exception:
        cmd = ["ogr2ogr", "-f", "GPKG"]
        if where:
            cmd.extend(["-where", where])
        if extra_args:
            cmd.extend(extra_args)
        cmd.extend([str(dst), str(src)])
        if layer:
            cmd.append(layer)

        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            if Path(dst).exists():
                Path(dst).unlink(missing_ok=True)
            raise RuntimeError(f"ogr2ogr filtering failed: {res.stderr}")

    check_ds = gdal.OpenEx(str(dst))
    if check_ds is None or check_ds.GetLayerCount() == 0:
        if Path(dst).exists():
            Path(dst).unlink(missing_ok=True)
        raise RuntimeError(f"Created GeoPackage at {dst} is invalid or contains 0 layers/features.")
    check_ds = None

    return True


def export_vsimem_to_disk(mem_path: str, dst_path: str) -> None:
    """Copies an in-memory dataset (/vsimem/) out to disk."""
    src_ds = gdal.Open(mem_path)
    if not src_ds:
        raise FileNotFoundError(f"In-memory raster not found at {mem_path}")
    gdal.GetDriverByName("GTiff").CreateCopy(dst_path, src_ds)
    src_ds = None


def write_vsimem_raster_to_disk(mem_path: str, dst_path: str) -> None:
    """Alias for export_vsimem_to_disk."""
    export_vsimem_to_disk(mem_path, dst_path)
