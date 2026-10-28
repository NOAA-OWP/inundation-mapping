#!/usr/bin/env python3

"""
Workflow orchestrator for Guam and American Samoa preprocessing.
Coordinates NED elevation, WBD boundaries, hydrography vectors, and NFHL downloads.
"""

import argparse
import os
import sys


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import py7zr

from data.dem.preprocess_nhdplus_ned import preprocess_nhdplus_ned
from data.hazards.get_fema_nfhl import fetch_nfhl_by_hucs
from data.hydrography.preprocess_nhdplus_pi import preprocess_nhdplus_pi
from data.watersheds.preprocess_wbd import preprocess_wbd


REGION_CONFIG = {
    "Guam": {
        "region_code": "22GU",
        "region_number": "22a",
        "huc": "22010000",
        "target_crs_number": "6637",
        "ned_subfolder": "Ned22a",
    },
    "AmericanSamoa": {
        "region_code": "22AS",
        "region_number": "22c",
        "huc": "22030001",
        "target_crs_number": "32702",
        "ned_subfolder": "ned22c",
    },
}


def download_and_extract_7z(urls, download_dir):
    """Downloads and extracts 7z archives if not already present."""
    os.makedirs(download_dir, exist_ok=True)
    for url in urls:
        archive_name = os.path.basename(url)
        archive_path = os.path.join(download_dir, archive_name)

        if not os.path.exists(archive_path):
            print(f"Downloading {url} ...")
            cmd = f"wget -q --show-progress -O {archive_path} {url}"
            ret = os.system(cmd)
            if ret != 0 or not os.path.exists(archive_path):
                sys.exit(f"Failed to download archive: {url}")
        else:
            print(f"Archive '{archive_name}' already exists. Skipping download.")

        print(f"Extracting '{archive_name}' into '{download_dir}'...")
        try:
            with py7zr.SevenZipFile(archive_path, "r") as archive:
                archive.extractall(path=download_dir)
            print(f"Successfully extracted {archive_name}")
        except Exception as e:
            sys.exit(f"Failed to extract {archive_path}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess NHDPlus DEM, WBD boundaries, hydrography, and NFHL for Guam and American Samoa."
    )
    parser.add_argument(
        "-r",
        "--regions",
        nargs="+",
        choices=list(REGION_CONFIG.keys()),
        default=list(REGION_CONFIG.keys()),
        help="Region(s) to process. Defaults to both Guam and AmericanSamoa.",
    )
    parser.add_argument("-i", "--inputs_dir", type=str, required=True, help="Base inputs directory")
    parser.add_argument("-d", "--date", type=str, required=True, help="Run date (e.g., 20260619)")
    parser.add_argument("-b", "--vrt_basename", type=str, required=True, help="Basename of VRT")
    parser.add_argument("-o", "--vrt_output_dir", type=str, default=None, help="VRT output directory")
    parser.add_argument("-s", "--vrt_suffix", type=str, default="", help="Suffix for VRT file")
    parser.add_argument("--skip-dem", action="store_true", help="Skip NED DEM processing")
    parser.add_argument("--skip-wbd", action="store_true", help="Skip WBD processing")
    parser.add_argument("--skip-hydro", action="store_true", help="Skip vector hydrography processing")
    parser.add_argument("--skip-nfhl", action="store_true", help="Skip FEMA NFHL download")

    args = parser.parse_args()

    for region in args.regions:
        cfg = REGION_CONFIG[region]
        region_code = cfg["region_code"]
        region_number = cfg["region_number"]
        huc = cfg["huc"]
        target_crs_number = cfg["target_crs_number"]
        target_name = f"{region}_{target_crs_number}"

        nhdplus_root_raw = os.path.join(args.inputs_dir, "nhdplus")
        target_folder = os.path.join(nhdplus_root_raw, target_name)
        nhdplus_path = os.path.join(nhdplus_root_raw, "NHDPlusPI", f"NHDPlus{region_code}")
        dem_file = os.path.join(nhdplus_path, "NEDSnapshot", cfg["ned_subfolder"], "elev_cm")
        nhd_path = os.path.join(nhdplus_path, "NHDSnapshot", "Hydrography")
        target_dem_folder = os.path.join(args.inputs_dir, "dems", "3dep_dems", f"10m_{region}", args.date)
        vrt_out_dir = args.vrt_output_dir or os.path.join(args.inputs_dir, "dems", "vrt")

        os.makedirs(target_folder, exist_ok=True)
        os.makedirs(vrt_out_dir, exist_ok=True)

        nhdplus_urls = [
            f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_{region_number}_NEDSnapshot_01.7z",
            f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_NHDPlusAttributes_02.7z",
            f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_NHDPlusCatchment_01.7z",
            f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_NHDSnapshot_01.7z",
            f"https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusPI/NHDPlus{region_code}/NHDPlusV21_PI_{region_code}_WBDSnapshot_02.7z",
        ]

        print(
            f"\n{'='*70}\n--> Processing Region: {region} (HUC: {huc}, CRS: EPSG:{target_crs_number})\n{'='*70}\n"
        )
        download_and_extract_7z(nhdplus_urls, nhdplus_root_raw)

        # 1. NED Elevation Preprocessing
        if not args.skip_dem:
            print(f"[{region}] (1/4) Processing NED DEM...")
            preprocess_nhdplus_ned(
                dem_file=dem_file,
                target_dem_folder=target_dem_folder,
                target_folder=target_folder,
                huc=huc,
                region=region,
                target_crs_number=target_crs_number,
                vrt_basename=args.vrt_basename,
                vrt_output_dir=vrt_out_dir,
                vrt_suffix=args.vrt_suffix,
            )
        else:
            print(f"[{region}] (1/4) Skipping DEM processing (--skip-dem passed).")

        # 2. Watershed Boundaries (WBD) Preprocessing
        if not args.skip_wbd:
            print(f"[{region}] (2/4) Subsetting & projecting WBD boundaries...")
            raw_wbd_shp = os.path.join(nhdplus_path, "WBDSnapshot", "WBD", "WBD_Subwatershed.shp")
            preprocess_wbd(
                region=region,
                target_crs_number=target_crs_number,
                inputs_dir=args.inputs_dir,
                hucs=[huc],
                source_wbd_file=raw_wbd_shp,
            )
        else:
            print(f"[{region}] (2/4) Skipping WBD preprocessing (--skip-wbd passed).")

        # 3. NHDPlus Vector Preprocessing
        if not args.skip_hydro:
            print(f"[{region}] (3/4) Running NHDPlus hydrography vector preprocessing...")
            preprocess_nhdplus_pi(
                nhd_path=nhd_path,
                nhdplus_path=nhdplus_path,
                target_folder=target_folder,
                target_crs_number=target_crs_number,
                region=region,
            )
        else:
            print(f"[{region}] (3/4) Skipping hydrography preprocessing (--skip-hydro passed).")

        # 4. FEMA NFHL Hazard Data
        if not args.skip_nfhl:
            print(f"[{region}] (4/4) Fetching FEMA NFHL hazard data...")
            fetch_nfhl_by_hucs(region=region, hucs=[huc], inputs_dir=args.inputs_dir, num_processes=14)
        else:
            print(f"[{region}] (4/4) Skipping FEMA NFHL download (--skip-nfhl passed).")

        print(f"\n{region} preprocessing finished successfully.")


if __name__ == "__main__":
    main()
