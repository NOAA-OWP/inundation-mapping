import argparse
import glob
import os
import re
import sys
from datetime import datetime, timezone

import geopandas as gpd

from src.utils.shared_functions import setup_mp_file_logger


def make_modified_bridges(osm_bridge_dir, lidar_raster_dir, ml_raster_dir, output_dir):
    start_time = datetime.now(timezone.utc)
    os.makedirs(output_dir, exist_ok=True)

    log_file_path = os.path.join(output_dir, 'make_modified_bridges.log')
    file_logger = setup_mp_file_logger(log_file_path, logger_name='make_modified_bridges')
    file_logger.info(f"Started at (UTC): {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    file_logger.info(f"OSM bridge dir:    {osm_bridge_dir}")
    file_logger.info(f"Lidar raster dir:  {lidar_raster_dir}")
    file_logger.info(f"ML raster dir:     {ml_raster_dir}")
    file_logger.info(f"Output dir:        {output_dir}")

    bridge_files = sorted(glob.glob(os.path.join(osm_bridge_dir, 'huc_*_osm_bridges.gpkg')))
    if not bridge_files:
        sys.exit(f"No huc_*_osm_bridges.gpkg files found in {osm_bridge_dir}")

    file_logger.info(f"Found {len(bridge_files)} HUC bridge files to process")
    print(f"Found {len(bridge_files)} HUC bridge files to process")

    for bridge_file in bridge_files:
        basename = os.path.basename(bridge_file)
        match = re.match(r'huc_(\d{8})_osm_bridges\.gpkg$', basename)
        if not match:
            file_logger.warning(f"Skipping unrecognised filename: {basename}")
            continue
        HUC = match.group(1)

        lidar_tifs = glob.glob(os.path.join(lidar_raster_dir, HUC, 'lidar_osm_rasters', '*.tif'))
        ml_tifs = glob.glob(os.path.join(ml_raster_dir, HUC, 'ml_osm_rasters', '*.tif'))
        lidar_osmids = {os.path.splitext(os.path.basename(p))[0] for p in lidar_tifs}
        ml_osmids = {os.path.splitext(os.path.basename(p))[0] for p in ml_tifs}

        gdf = gpd.read_file(bridge_file)
        gdf = gdf[['osmid', 'name', 'bridge_type', 'huc8', 'geometry']].copy()
        gdf['osmid'] = gdf['osmid'].astype(str)
        gdf['has_lidar_tif'] = gdf['osmid'].apply(lambda x: 'Y' if x in lidar_osmids else 'N')
        gdf['has_ml_tif'] = gdf['osmid'].apply(lambda x: 'Y' if x in ml_osmids else 'N')

        out_path = os.path.join(output_dir, f'huc_{HUC}_osm_bridges_modified.gpkg')
        gdf.to_file(out_path)

        n_lidar = (gdf['has_lidar_tif'] == 'Y').sum()
        n_ml = (gdf['has_ml_tif'] == 'Y').sum()
        msg = f"HUC {HUC}: {len(gdf)} bridges — {n_lidar} with lidar TIF, {n_ml} with ML TIF → {out_path}"
        file_logger.info(msg)
        print(msg)

    end_time = datetime.now(timezone.utc)
    file_logger.info(f"TOTAL RUN TIME: {end_time - start_time}")
    print(f"TOTAL RUN TIME: {end_time - start_time}")
    print("Done!")
    file_logger.info("Done!")


if __name__ == "__main__":
    '''
    INPUT DATA PIPELINE — run scripts in this order:
        Step 1  : pull_osm_bridges.py          — pull OSM bridge lines per HUC
        Step 2a : make_rasters_using_lidar.py  — generate lidar TIFs (independent of 2b, can run in parallel)
        Step 2b : make_rasters_using_ml.py     — generate ML-classified lidar TIFs (independent of 2a, can run in parallel)
        Step 2c : make_modified_bridges.py     — (this script) add TIF flags, write huc_*_osm_bridges_modified.gpkg (run once)
        Step 3  : make_dem_dif_for_bridges.py  — build diff rasters per region (run 4× for CONUS/AK/GU/AS)

    This script runs once for all regions; output is region-agnostic.

    Sample usage:
        python make_modified_bridges.py \
          -i data/inputs/osm/bridges/bridge_lines/20260315/ \
          -l data/inputs/osm/bridges/lidar_data/20260315/ \
          -m data/inputs/NGWPC/ML_bridges/20260315/rasters/ \
          -o data/inputs/osm/bridges/modified_osm_bridges/20260315/
    '''

    parser = argparse.ArgumentParser(
        description=(
            'Add has_lidar_tif and has_ml_tif flag columns to HUC-level OSM bridge GeoPackages. '
            'Reads raw bridge GPKGs and checks TIF availability in the lidar and ML raster directories. '
            'Outputs one huc_*_osm_bridges_modified.gpkg per HUC. Run once before make_dem_dif_for_bridges.py.'
        )
    )

    parser.add_argument(
        '-i',
        '--osm_bridge_dir',
        help='REQUIRED: Folder containing raw huc_*_osm_bridges.gpkg files (output of pull_osm_bridges.py).',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--lidar_raster_dir',
        help='REQUIRED: Root directory containing per-HUC lidar_osm_rasters/ subdirectories (output of make_rasters_using_lidar.py).',
        required=True,
    )

    parser.add_argument(
        '-m',
        '--ml_raster_dir',
        help='REQUIRED: Root directory containing per-HUC ml_osm_rasters/ subdirectories (output of make_rasters_using_ml.py).',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output_dir',
        help='REQUIRED: Folder where huc_*_osm_bridges_modified.gpkg files will be written.',
        required=True,
    )

    args = vars(parser.parse_args())
    make_modified_bridges(**args)
