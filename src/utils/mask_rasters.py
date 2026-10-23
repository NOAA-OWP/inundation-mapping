#!/usr/bin/env python3
"""
Fast raster masking tool using spatial indexing pre-filtering.
"""

import argparse
import os
import sys

import geopandas as gpd
import rasterio
from rasterio.features import shapes
from rasterio.mask import mask
from shapely.geometry import box, shape

from utils.fim_enums import FIM_exit_codes


def get_mask_geometries_indexed(mask_file, target_bounds=None, target_crs=None):
    """
    Reads a mask file (vector or raster) and pre-filters geometries
    using a spatial index against target bounds.
    """
    is_raster = False
    try:
        with rasterio.open(mask_file) as src:
            is_raster = True
            mask_crs = src.crs
            mask_arr = src.read(1)
            nodata = src.nodata

            valid_mask = (mask_arr != nodata) if nodata is not None else (mask_arr > 0)

            geometries = [
                shape(geom) for geom, val in shapes(mask_arr, mask=valid_mask, transform=src.transform) if val
            ]
    except Exception:
        is_raster = False

    if not is_raster:
        print(f"Reading vector mask: {mask_file}")
        gdf = gpd.read_file(mask_file)

        # 1. Hilbert Sort for spatial cache locality (fixed for GeoSeries)
        if not gdf.empty:
            hilbert_order = gdf.geometry.hilbert_distance().argsort()
            gdf = gdf.iloc[hilbert_order].reset_index(drop=True)

        # 2. Spatial Index Pre-filtering against target raster bounding box
        if target_bounds and target_crs:
            if gdf.crs != target_crs:
                gdf = gdf.to_crs(target_crs)

            raster_bbox = box(*target_bounds)

            # Use STRtree spatial index to query intersecting bounding boxes
            possible_matches_idx = list(gdf.sindex.query(raster_bbox, predicate="intersects"))
            gdf = gdf.iloc[possible_matches_idx]

            print(f"Spatial index filtered geometries down to {len(gdf)}")

        geometries = list(gdf.geometry.values)
        mask_crs = gdf.crs

    return geometries, mask_crs


def mask_rasters_optimized(mask_path, raster_inputs, output_paths, exit_on_out_of_bounds=True):
    """
    Masks rasters using spatial index filtering.
    If a raster lies completely outside the mask boundaries, it logs an error
    and exits with non-zero status to kill the branch processing.
    """
    if len(raster_inputs) != len(output_paths):
        raise ValueError("Inputs and outputs count mismatch.")

    for in_raster, out_raster in zip(raster_inputs, output_paths):
        print(f"\nProcessing: {in_raster} -> {out_raster}")

        with rasterio.open(in_raster) as src:
            nodata_val = src.nodata if src.nodata is not None else -9999

            # Query vector mask using target raster's bounding box and CRS
            mask_geoms, mask_crs = get_mask_geometries_indexed(
                mask_file=mask_path, target_bounds=src.bounds, target_crs=src.crs
            )

            # Handle out-of-bounds branch
            if not mask_geoms:
                print(f"{in_raster} lies completely outside the mask boundary.")
                if exit_on_out_of_bounds:
                    print("Terminating branch processing with exit code 1.")
                    sys.exit(FIM_exit_codes.BRANCH_IS_COMPLETELY_OUTSIDE_US.value)
                continue

            mask_gdf = gpd.GeoDataFrame(geometry=mask_geoms, crs=mask_crs)
            if mask_gdf.crs != src.crs:
                mask_gdf = mask_gdf.to_crs(src.crs)

            geometries = mask_gdf.geometry.values

            out_image, out_transform = mask(src, geometries, crop=True, nodata=nodata_val)
            out_meta = src.meta.copy()

            out_meta.update(
                {
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    "transform": out_transform,
                    "nodata": nodata_val,
                    "tiled": True,
                    "blockxsize": 256,
                    "blockysize": 256,
                }
            )

            out_dir = os.path.dirname(out_raster)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

            with rasterio.open(out_raster, "w", **out_meta) as dest:
                dest.write(out_image)

            print(f"Successfully saved masked raster: {out_raster}")


def main(args=None):
    parser = argparse.ArgumentParser(description="Fast raster masking using spatial indexing pre-filtering.")
    parser.add_argument("-m", "--mask", required=True, help="Input mask file (vector or raster).")
    parser.add_argument("-i", "--inputs", nargs="+", required=True, help="Input raster file(s).")
    parser.add_argument("-o", "--outputs", nargs="+", required=True, help="Output raster file(s).")

    parsed_args = parser.parse_args(args)
    mask_rasters_optimized(parsed_args.mask, parsed_args.inputs, parsed_args.outputs)


if __name__ == "__main__":
    main()
