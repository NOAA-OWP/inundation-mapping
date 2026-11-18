#!/usr/bin/env python3
"""
Precomputes a list of HUC8s intersecting the U.S. international/outer boundary.

Usage:
python3 /foss_fim/data/wbd/filter_international_huc8s.py \
    -w /data/inputs/wbd/WBD_National_HUC8.gpkg \
    -b /data/inputs/USGS_State_Boundaries/s_11au16.shp \
    -o /data/inputs/huc_lists/international_huc8s_20261023.lst
"""

import argparse
import json

import geopandas as gpd


def generate_international_border_hucs(wbd_path, boundary_path, output_file):
    print(f"Loading boundary layer: {boundary_path}")
    boundaries = gpd.read_file(boundary_path)

    print("Dissolving state polygons into a unified national boundary...")
    # Dissolve state features into a single multipart national boundary geometry
    national_boundary = boundaries.dissolve()

    # Convert polygon fill into a boundary line representation (exterior edges)
    # This ensures we query HUCs intersecting the perimeter, not inland interiors
    national_perimeter = national_boundary.boundary

    print(f"Loading WBD HUC8 layer: {wbd_path}")
    wbd = gpd.read_file(wbd_path)

    # Ensure matching CRS before spatial operations
    if wbd.crs != national_boundary.crs:
        print(f"Reprojecting national boundary to match WBD CRS ({wbd.crs})...")
        national_perimeter = national_perimeter.to_crs(wbd.crs)

    print("Identifying HUC8s intersecting the outer national perimeter...")
    # Spatial join using the perimeter boundary line
    border_hucs = gpd.sjoin(wbd, gpd.GeoDataFrame(geometry=national_perimeter), predicate="intersects")

    # Extract clean, sorted 8-digit HUC strings (preserving leading zeros)
    border_huc_list = sorted([str(huc).zfill(8) for huc in border_hucs["HUC8"].unique()])

    print(f"\nFound {len(border_huc_list)} international border HUC8s.")

    with open(output_file, "w") as f:
        for huc in border_huc_list:
            f.write(f"{huc}\n")

    print(f"Saved border HUC8 list to text file: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Extract HUC8 IDs intersecting outer international borders.")
    parser.add_argument("-w", "--wbd", required=True, help="Path to WBD National GDB/GPKG layer.")
    parser.add_argument(
        "-b",
        "--boundary",
        required=True,
        help="Path to state/national boundary shapefile (e.g. s_11au16.shp).",
    )
    parser.add_argument("-o", "--output_file", required=True, help="Output filename.")

    args = parser.parse_args()
    generate_international_border_hucs(args.wbd, args.boundary, args.output_file)


if __name__ == "__main__":
    main()
