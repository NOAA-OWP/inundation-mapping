#!/usr/bin/env python3
"""
Pre-dissolves state boundaries into a single national polygon asset.

Usage:
python3 /foss_fim/data/usgs/dissolve_state_boundaries.py \
    -i /data/inputs/USGS_State_Boundaries/s_11au16.shp \
    -o /data/inputs/USGS_State_Boundaries/s_11au16_dissolved.shp
"""

import argparse

import geopandas as gpd

from utils.io import write_geodataframe


def pre_dissolve_boundary(input_shapefile, output_path):
    print(f"Reading state boundaries from: {input_shapefile}")
    gdf = gpd.read_file(input_shapefile)

    print("Dissolving state polygons into a unified national mask...")
    dissolved = gdf.dissolve()

    # Fix potential self-intersections or sliver topology issues
    dissolved["geometry"] = dissolved.geometry.make_valid()

    # Keep only geometry to keep the asset minimal
    dissolved = dissolved[["geometry"]]

    write_geodataframe(dissolved, output_path)

    print(f"Saved pre-dissolved boundary to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Pre-dissolve state boundary shapefile.")
    parser.add_argument(
        "-i", "--input", required=True, help="Input state boundary shapefile (e.g., s_11au16.shp)."
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output path (e.g., national_boundary_dissolved.parquet or .gpkg).",
    )
    args = parser.parse_args()

    pre_dissolve_boundary(args.input, args.output)


if __name__ == "__main__":
    main()
