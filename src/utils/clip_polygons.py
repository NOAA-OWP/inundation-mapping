#!/usr/bin/env python3
"""
Fast clipping of GeoParquet catchments using a pre-dissolved boundary layer.
"""

import argparse
import os

import geopandas as gpd


def clip_parquet_catchments(catchments_parquet, boundary_path, output_parquet):
    print(f"Reading catchment GeoParquet: {catchments_parquet}")
    catchments = gpd.read_parquet(catchments_parquet)

    if catchments.empty:
        print("Warning: Catchment parquet is empty. Skipping.")
        return

    print(f"Reading pre-dissolved boundary: {boundary_path}")
    if boundary_path.endswith((".parquet", ".geoparquet")):
        boundary = gpd.read_parquet(boundary_path)
    else:
        boundary = gpd.read_file(boundary_path)

    # Reproject boundary to match catchments CRS if needed
    if boundary.crs != catchments.crs:
        print(f"Reprojecting boundary from {boundary.crs} to {catchments.crs}...")
        boundary = boundary.to_crs(catchments.crs)

    # Clean geometry before clipping
    catchments["geometry"] = catchments.geometry.make_valid()
    boundary["geometry"] = boundary.geometry.make_valid()

    print("Clipping catchments to national boundary...")
    clipped = gpd.clip(catchments, boundary)

    # Remove empty or invalid results
    clipped = clipped[~clipped.is_empty & clipped.is_valid]

    out_dir = os.path.dirname(output_parquet)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    print(f"Writing {len(clipped)} clipped catchments to: {output_parquet}")
    clipped.to_parquet(output_parquet)
    print("Clipping completed successfully.")


def main(args=None):
    parser = argparse.ArgumentParser(description="Clip GeoParquet catchments using a pre-dissolved boundary.")
    parser.add_argument(
        "-c", "--catchments", required=True, help="Input catchment GeoParquet file (.parquet)."
    )
    parser.add_argument(
        "-b", "--boundary", required=True, help="Pre-dissolved boundary file (.parquet or .gpkg)."
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Output clipped GeoParquet file path (.parquet)."
    )

    parsed = parser.parse_args(args)
    clip_parquet_catchments(parsed.catchments, parsed.boundary, parsed.output)


if __name__ == "__main__":
    main()
