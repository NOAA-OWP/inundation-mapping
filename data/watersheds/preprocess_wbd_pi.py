import argparse
import os
import sys

import geopandas as gpd

from src.utils.io import write_geodataframe


def preprocess_wbd_pi(nhdplus_path: str, inputs_dir: str, target_crs_number: int, target_name: str) -> None:
    """Extract, reproject, dissolve, and export WBD subwatershed data for Pacific Islands."""
    wbd = os.path.join(nhdplus_path, "WBDSnapshot", "WBD", "WBD_Subwatershed.shp")
    if not os.path.exists(wbd):
        sys.exit(f"WBD file {wbd} does not exist. Exiting...")

    WBD = gpd.read_file(wbd, columns=["HUC_8"])
    WBD = WBD.rename(columns={"HUC_8": "HUC8"})
    WBD = WBD.dissolve(by="HUC8")
    WBD = WBD.to_crs(epsg=target_crs_number)

    output_dir = os.path.join(inputs_dir, "wbd")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f"WBD_{target_name}.gpkg")
    write_geodataframe(WBD, output_path, layer="WBDHU8", driver="GPKG")


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess WBD for Guam and American Samoa.")
    parser.add_argument(
        "--nhdplus-path",
        type=str,
        required=True,
        help="Path to the directory containing NHDPlus data snapshot.",
    )
    parser.add_argument(
        "--inputs-dir",
        type=str,
        required=True,
        help="Target root output directory for processed geospatial inputs.",
    )
    parser.add_argument(
        "--target-crs-number",
        type=int,
        required=True,
        help="Target EPSG code for reprojection (e.g., 32655 for Guam, 32702 for American Samoa).",
    )
    parser.add_argument(
        "--target-name", type=str, required=True, help="Target region short code/identifier (e.g., GU, AS)."
    )

    args = parser.parse_args()

    preprocess_wbd_pi(
        nhdplus_path=args.nhdplus_path,
        inputs_dir=args.inputs_dir,
        target_crs_number=args.target_crs_number,
        target_name=args.target_name,
    )


if __name__ == "__main__":
    main()
