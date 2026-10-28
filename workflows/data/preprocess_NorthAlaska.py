#!/usr/bin/env python3

"""
Workflow orchestrator for North Alaska preprocessing.
Processes Fairbanks and Juneau regions across four discrete stages:
  1. DGGS IFSAR DEM extraction, mosaicing, and clipping
  2. Watershed Boundary Dataset (WBD) regional subsetting and projection
  3. NHDPlus hydrography vector ingestion, filtering, and headwater derivation
  4. FEMA NFHL hazard layer acquisition

  Example usage:

  python3 /foss_fim/workflows/data/preprocess_NorthAlaska.py \
      -i /data/inputs \
      -c 3338 \
      -s /data/inputs/GEOGLOWS/ak_tests_BETA_AK_reference_fabric_fairbanks_juneau.gpkg \
      -e /data/inputs/dems/ifsar_dtm/10m_NorthAlaska \
      -d 20261027
"""

import argparse
import os
import sys


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from data.elevation.preprocess_dggs_ifsar import preprocess_dggs_ifsar
from data.hazards.get_fema_nfhl import fetch_nfhl_by_hucs
from data.hydrography.preprocess_nhdplus_NorthAlaska import preprocess_nhdplus
from data.watersheds.preprocess_wbd import preprocess_wbd


NORTH_ALASKA_REGIONS = {"Fairbanks": ["19080306", "19080307"], "Juneau": ["19010301"]}


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess North Alaska DGGS IFSAR DEM, WBD boundaries, NHDPlus hydrography, and FEMA NFHL."
    )
    parser.add_argument(
        "-i",
        "--inputs_dir",
        type=str,
        required=True,
        help="Base directory containing inputs (e.g., /data/inputs)",
    )
    parser.add_argument(
        "-c",
        "--target_crs_number",
        type=str,
        required=False,
        default="3338",
        help="Target EPSG projection number (default: 3338)",
    )
    parser.add_argument(
        "-s",
        "--reference_fabric_file",
        type=str,
        required=True,
        help="Path to shared reference hydrofabric GPKG file",
    )
    parser.add_argument(
        "-e",
        "--out_dem_folder",
        type=str,
        required=True,
        help="Staging base directory for unzipping and intermediary DEM files",
    )
    parser.add_argument(
        "-d", "--date", type=str, required=True, help="Run identifier date string (e.g., 20261026)"
    )
    parser.add_argument(
        "--regions",
        nargs="+",
        choices=list(NORTH_ALASKA_REGIONS.keys()),
        default=list(NORTH_ALASKA_REGIONS.keys()),
        help="Regions to run. Defaults to running both Fairbanks and Juneau.",
    )
    parser.add_argument(
        "--skip-dem", action="store_true", default=False, help="Skip the DEM ingestion and processing step."
    )
    parser.add_argument(
        "--skip-wbd", action="store_true", default=False, help="Skip WBD subsetting and reprojection."
    )
    parser.add_argument(
        "--skip-hydro", action="store_true", default=False, help="Skip NHDPlus hydrography preprocessing."
    )
    parser.add_argument(
        "--skip-nfhl",
        action="store_true",
        default=False,
        help="Skip downloading FEMA NFHL flood hazard data.",
    )

    args = parser.parse_args()

    if not os.path.exists(args.reference_fabric_file) and not args.skip_hydro:
        sys.exit(f"[ERROR] Shared reference fabric file not found: {args.reference_fabric_file}")

    print(f"Starting North Alaska preprocessing for regions: {args.regions}")

    for region in args.regions:
        hucs = NORTH_ALASKA_REGIONS[region]

        region_dem_zip = os.path.join(
            args.inputs_dir, "dems", "ifsar_dtm", region, args.date, "custom_download.zip"
        )
        region_staging_dir = os.path.join(args.out_dem_folder, region)

        print(f"\n{'='*70}")
        print(f"--> Processing Region: {region}")
        print(f"    Target HUCs: {hucs}")
        print(f"    Target CRS: EPSG:{args.target_crs_number}")
        print(f"    Staging Dir: {region_staging_dir}")
        print(f"{'='*70}\n")

        # -------------------------------------------------------------------------
        # 1. DGGS IFSAR Elevation Preprocessing
        # -------------------------------------------------------------------------
        if not args.skip_dem:
            if not os.path.exists(region_dem_zip):
                print(f"[ERROR] DEM archive missing for {region} at: {region_dem_zip}", file=sys.stderr)
                sys.exit(1)

            print(f"[{region}] (1/4) Running DGGS IFSAR DEM preprocessing...")
            preprocess_dggs_ifsar(
                input_dem_zip_file=region_dem_zip,
                out_dem_folder=region_staging_dir,
                region=region,
                date=args.date,
                hucs=hucs,
                inputs_dir=args.inputs_dir,
                target_crs_number=args.target_crs_number,
            )
        else:
            print(f"[{region}] (1/4) Skipping DEM preprocessing (--skip-dem passed).")

        # -------------------------------------------------------------------------
        # 2. Watershed Boundaries (WBD) Preprocessing
        # -------------------------------------------------------------------------
        if not args.skip_wbd:
            print(f"[{region}] (2/4) Subsetting & projecting WBD boundaries...")
            preprocess_wbd(
                region=region, target_crs_number=args.target_crs_number, inputs_dir=args.inputs_dir, hucs=hucs
            )
        else:
            print(f"[{region}] (2/4) Skipping WBD preprocessing (--skip-wbd passed).")

        # -------------------------------------------------------------------------
        # 3. NHDPlus Hydrography Vector Preprocessing
        # -------------------------------------------------------------------------
        if not args.skip_hydro:
            print(f"[{region}] (3/4) Running NHDPlus hydrography preprocessing...")
            preprocess_nhdplus(
                region=region,
                hucs=hucs,
                target_crs_number=args.target_crs_number,
                inputs_dir=args.inputs_dir,
                reference_fabric_file=args.reference_fabric_file,
                date=args.date,
            )
        else:
            print(f"[{region}] (3/4) Skipping hydrography preprocessing (--skip-hydro passed).")

        # -------------------------------------------------------------------------
        # 4. FEMA NFHL Hazard Data Acquisition
        # -------------------------------------------------------------------------
        if not args.skip_nfhl:
            print(f"[{region}] (4/4) Fetching FEMA NFHL hazard data...")
            fetch_nfhl_by_hucs(region=region, hucs=hucs, inputs_dir=args.inputs_dir)
        else:
            print(f"[{region}] (4/4) Skipping FEMA NFHL download (--skip-nfhl passed).")

    print("\nNorth Alaska preprocessing completed successfully for all specified regions.")


if __name__ == "__main__":
    main()
