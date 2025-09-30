#!/usr/bin/env python
"""
Clips 3DEP tile indices to WBD and each other
"""
from __future__ import annotations

import argparse
import os

import geopandas as gpd
from shapely.geometry import Polygon


def clip_3m_to_1m(
    one_meter_gdf: gpd.GeoDataFrame, three_meter_gdf: gpd.GeoDataFrame, verbose: bool = False
) -> gpd.GeoDataFrame:
    """
    Clips 3m tile index to 1m tile index and saves to output path.
    """
    if verbose:
        print("Clipping 3m tile index to 1m tile index...")

    # if geometry in three_meter_gdf doesn't intersect with one_meter_gdf, drop it
    if verbose:
        print("   One meter unary union...")
    one_meter_unary_union = one_meter_gdf.union_all()
    if verbose:
        print("   Removing 3m tiles that are covered by 1m tiles...")
    not_covered_by_one_meter_bool = ~three_meter_gdf.geometry.covered_by(one_meter_unary_union)
    three_meter_gdf = three_meter_gdf[not_covered_by_one_meter_bool].reset_index(drop=True)

    return three_meter_gdf


def get_union_all(gdf: gpd.GeoDataFrame) -> Polygon:
    """
    Get the union of all geometries in a GeoDataFrame.
    """
    # get union of all geometries
    return gdf.union_all()


def clip_tiles_to_wbd(
    tile_index_gdf: gpd.GeoDataFrame, wbd_union_all: Polygon, verbose: bool = False
) -> gpd.GeoDataFrame:
    """
    Clips tile index to WBD and saves to output path.
    """

    if verbose:
        print("Clipping tile index to WBD...")

    # if geometry in tile_index_gdf doesn't intersect with wbd_gdf, drop it
    if verbose:
        print("   Intersecting tile index with WBD...")
    intersecting_tile_index_bool = tile_index_gdf.geometry.intersects(wbd_union_all)
    tile_index_gdf = tile_index_gdf[intersecting_tile_index_bool].reset_index(drop=True)

    return tile_index_gdf


def generate_outputs_paths(one_meter_path: str, three_meter_path: str, suffix: str) -> tuple[str, str, str]:
    """
    Generate output paths for the clipped tile indices and output directory.
    """

    # get output_dir from one_meter_path
    output_dir = os.path.dirname(one_meter_path)

    # now make new basenames for the clipped files with suffix
    one_meter_basename = os.path.basename(one_meter_path)
    three_meter_basename = os.path.basename(three_meter_path)
    one_meter_basename = os.path.splitext(one_meter_basename)[0] + '_' + suffix + ".gpkg"
    three_meter_basename = os.path.splitext(three_meter_basename)[0] + '_' + suffix + ".gpkg"
    one_meter_path = os.path.join(output_dir, one_meter_basename)
    three_meter_path = os.path.join(output_dir, three_meter_basename)

    return one_meter_path, three_meter_path, output_dir


def main(
    one_meter_path: str,
    three_meter_path: str,
    wbd_path: str,
    suffix: str = "_clipped",
    verbose: bool = False,
    overwrite: bool = False,
) -> None:
    """
    Clips 3DEP tile indices to WBD and each other.
    """
    if verbose:
        print("Clipping 3DEP tile indices to WBD and each other...")

    # ensure paths are absolute
    one_meter_path = os.path.abspath(one_meter_path)
    three_meter_path = os.path.abspath(three_meter_path)
    wbd_path = os.path.abspath(wbd_path)

    # generate output paths
    one_meter_output_path, three_meter_output_path, output_dir = generate_outputs_paths(
        one_meter_path, three_meter_path, suffix=suffix
    )

    # check if output tiles already exist
    if os.path.exists(one_meter_output_path):
        if overwrite:
            os.remove(one_meter_output_path)
        else:
            raise FileExistsError(
                f"Output tile index {one_meter_output_path} already exists. Use -o to overwrite."
            )
    if os.path.exists(three_meter_output_path):
        if overwrite:
            os.remove(three_meter_output_path)
        else:
            raise FileExistsError(
                f"Output tile index {three_meter_output_path} already exists. Use -o to overwrite."
            )

    if verbose:
        print("Loading tile indices and WBD ...")

    # load tile indices
    one_meter_gdf = gpd.read_file(one_meter_path)
    three_meter_gdf = gpd.read_file(three_meter_path)

    # load WBD
    wbd_gdf = gpd.read_file(wbd_path)

    # check if all CRS's are the same
    assert (
        one_meter_gdf.crs == three_meter_gdf.crs == wbd_gdf.crs
    ), "CRS's of tile indices and WBD must be the same"

    # clip 3m tile index to 1m tile index
    three_meter_gdf = clip_3m_to_1m(one_meter_gdf, three_meter_gdf, verbose=verbose)

    # get union of all geometries in WBD
    if verbose:
        print("Getting union of all geometries in WBD...")
    wbd_union_all = get_union_all(wbd_gdf)

    # clip 1m tile index to WBD
    one_meter_gdf = clip_tiles_to_wbd(one_meter_gdf, wbd_union_all, verbose=verbose)

    # clip 3m tile index to WBD
    three_meter_gdf = clip_tiles_to_wbd(three_meter_gdf, wbd_union_all, verbose=verbose)

    # save clipped tile indices
    if verbose:
        print(f"Saving clipped tile indices to {one_meter_output_path} and {three_meter_output_path}...")

    if os.path.exists(one_meter_output_path):
        os.remove(one_meter_output_path)
    if os.path.exists(three_meter_output_path):
        os.remove(three_meter_output_path)

    one_meter_gdf.to_file(one_meter_output_path, driver="GPKG", index=False)
    three_meter_gdf.to_file(three_meter_output_path, driver="GPKG", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clip 3DEP tile indices to WBD and each other.")
    parser.add_argument("one_meter_path", type=str, help="Path to 1m tile index")
    parser.add_argument("three_meter_path", type=str, help="Path to 3m tile index")
    parser.add_argument("wbd_path", type=str, help="Path to WBD")
    parser.add_argument("-s", "--suffix", type=str, default="clipped", help="Suffix for clipped tile indices")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-o", "--overwrite", action="store_true", help="Overwrite existing files")

    args = parser.parse_args()

    main(args.one_meter_path, args.three_meter_path, args.wbd_path, suffix=args.suffix, verbose=args.verbose)
