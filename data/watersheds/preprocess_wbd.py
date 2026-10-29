#!/usr/bin/env python3
"""
Subset and project Watershed Boundary Dataset (WBD) layers.
Can be imported as a module or executed directly via CLI.
"""

import argparse
import os
import sys

import geopandas as gpd
from rasterio.crs import CRS


def preprocess_wbd(region, target_crs_number, inputs_dir, hucs=None, source_wbd_file=None):
    """
    Subsets WBD boundaries by region HUCs and reprojects to target CRS.

    Parameters:
    -----------
    region : str
        Region identifier (e.g., 'Fairbanks', 'Juneau').
    target_crs_number : str or int
        Target EPSG code (e.g., '3338').
    inputs_dir : str
        Base inputs directory containing the wbd folder.
    hucs : list of str, optional
        Target HUC8 IDs to subset. If None, exports full dataset.
    source_wbd_file : str, optional
        Explicit path to source WBD gpkg. Defaults to {inputs_dir}/wbd/WBD_Alaska_3338.gpkg.

    Returns:
    --------
    output_wbd_path : str
        Path to the generated regional GeoPackage.
    """
    wbd_dir = os.path.join(inputs_dir, "wbd")
    os.makedirs(wbd_dir, exist_ok=True)

    if source_wbd_file is None:
        source_wbd_file = os.path.join(wbd_dir, "WBD_Alaska_3338.gpkg")

    if not os.path.exists(source_wbd_file):
        sys.exit(f"ERROR: WBD source file {source_wbd_file} does not exist.")

    target_crs = CRS.from_epsg(int(target_crs_number))
    wbd_gdf = gpd.read_file(source_wbd_file, columns=["HUC8"])

    # Filter to regional HUCs if provided
    if hucs:
        wbd_gdf = wbd_gdf[wbd_gdf["HUC8"].astype(str).isin([str(h) for h in hucs])].copy()
        if wbd_gdf.empty:
            sys.exit(f"ERROR: None of HUCs {hucs} were found in {source_wbd_file}.")

    # Reproject if necessary
    if wbd_gdf.crs != target_crs:
        wbd_gdf = wbd_gdf.to_crs(epsg=target_crs_number)

    # Use project naming convention: WBD_{region}_{target_crs_number}.gpkg
    output_wbd_path = os.path.join(wbd_dir, f"WBD_{region}_{target_crs_number}.gpkg")
    wbd_gdf.to_file(output_wbd_path, layer="WBDHU8", driver="GPKG")
    print(f"[{region}] Successfully saved WBD to {output_wbd_path}")

    return output_wbd_path


def main():
    parser = argparse.ArgumentParser(description="Subset and project WBD boundaries for a specified region.")
    parser.add_argument(
        "-r", "--region", type=str, required=True, help="Region identifier (e.g., 'Fairbanks', 'Juneau')"
    )
    parser.add_argument(
        "-i",
        "--inputs_dir",
        type=str,
        required=True,
        help="Base inputs directory containing the wbd folder (e.g., /data/inputs)",
    )
    parser.add_argument(
        "-c",
        "--target_crs_number",
        type=str,
        required=False,
        default="3338",
        help="Target EPSG code (default: 3338)",
    )
    parser.add_argument(
        "-u",
        "--hucs",
        nargs="+",
        required=False,
        default=None,
        help="Optional space-separated list of HUC8 IDs to subset (e.g., -u 19080306 19080307)",
    )
    parser.add_argument(
        "-s",
        "--source_wbd_file",
        type=str,
        required=False,
        default=None,
        help="Optional custom path to statewide/source WBD GPKG file",
    )

    args = parser.parse_args()

    preprocess_wbd(
        region=args.region,
        target_crs_number=args.target_crs_number,
        inputs_dir=args.inputs_dir,
        hucs=args.hucs,
        source_wbd_file=args.source_wbd_file,
    )


if __name__ == "__main__":
    main()
