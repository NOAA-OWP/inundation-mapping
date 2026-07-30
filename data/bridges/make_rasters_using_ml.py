import argparse
import logging
import os
import re
import sys
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from make_rasters_using_lidar import (
    gpkg_to_las,
    handle_noises,
    las_to_gpkg,
    make_local_tifs,
    summarize_classification_counts,
)

from src.utils.shared_functions import get_huc_vars, run_with_mp, setup_mp_file_logger


_srcDir = os.getenv('srcDir')
if _srcDir:
    load_dotenv(f'{_srcDir}/bash_variables.env')

HUC_PATTERN = re.compile(r'^\d{8}$')
OSMID_PATTERN = re.compile(r'^bridge_(\d+)_')


def make_raster_for_osmid(
    osmid, laz_paths, output_dir, raster_resolution, bridges_crs, file_logger, screen_queue, task_id
):
    try:
        all_gdfs = []
        for laz_path in laz_paths:
            gdf = las_to_gpkg(osmid, laz_path, bridges_crs, ML_derived=True)
            if not gdf.empty:
                all_gdfs.append(gdf)

        if not all_gdfs:
            file_logger.info("No points available for osmid: %s" % osmid)
            return 1, [True, None, None]

        points_gdf = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True), crs=bridges_crs)
        classification_counts = summarize_classification_counts(points_gdf, osmid)
        modified_points_gdf, elevation_filter_summary = handle_noises(points_gdf, osmid)

        if modified_points_gdf is None:
            file_logger.info("No approved for osmid: %s" % str(osmid))
            screen_queue.put("No approved for osmid: %s" % str(osmid))
            return 1, [
                True,
                classification_counts,
                elevation_filter_summary,
            ]  # {'classification_counts': classification_counts, 'elevation_filter_summary': elevation_filter_summary}

        modified_las_path = os.path.join(output_dir, '%s_modified.las' % osmid)
        las_obj = gpkg_to_las(modified_points_gdf)
        las_obj.write(modified_las_path)

        tif_path = os.path.join(output_dir, '%s.tif' % osmid)
        make_local_tifs(modified_las_path, raster_resolution, bridges_crs, tif_path)
        os.remove(modified_las_path)

        return 1, [True, classification_counts, elevation_filter_summary]

        # return {'classification_counts': classification_counts, 'elevation_filter_summary': elevation_filter_summary}

    except Exception as e:
        file_logger.error(f"❌ Exception in {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return 0, [False, None, None]


def discover_tasks(laz_base_dir):
    """
    Walk laz_base_dir for 8-digit HUC subdirs, parse LAZ filenames to extract osmids,
    and group files by osmid within each HUC.
    Returns list of (osmid, [laz_paths], huc_name).
    """
    tasks = []
    base = Path(laz_base_dir)

    if not base.exists():
        sys.exit(f"Error: LAZ base directory does not exist: {laz_base_dir}")

    for huc_dir in sorted(base.iterdir()):
        if not huc_dir.is_dir() or not HUC_PATTERN.match(huc_dir.name):
            continue

        # Creates a dictionary where each OSMID can map to a list of .laz files--each osmd can have multiple laz files
        osmid_map = defaultdict(list)
        for laz_file in sorted(huc_dir.glob('*.laz')):
            # osmid_match is a re.Match object if the filename fits that pattern, else None.
            osmid_match = OSMID_PATTERN.match(laz_file.name)
            if osmid_match:
                # group(1) is the first parenthesized group, i.e. the digits captured by (\d+) -- the osmid itself.
                osmid = osmid_match.group(1)
                osmid_map[osmid].append(str(laz_file))

        for osmid, paths in osmid_map.items():
            tasks.append((osmid, paths, huc_dir.name))

    return tasks


def process_ml_laz_to_rasters(laz_base_dir, output_raster_dir, raster_resolution, job_number, lst_hucs=''):
    start_time = datetime.now(timezone.utc)

    os.makedirs(output_raster_dir, exist_ok=True)

    log_file_path = os.path.join(output_raster_dir, "bridge_rasters_from_ml.log")
    file_logger = setup_mp_file_logger(log_file_path, logger_name="bridge_rasters_from_ml")
    print('started the process')

    file_logger.info(f"Starting ML bridge LAZ-to-raster conversion at {start_time}")
    file_logger.info(f"LAZ input dir:     {laz_base_dir}")
    file_logger.info(f"Raster output dir: {output_raster_dir}")

    # Scan HUC directories and group LAZ files by extracted OSMID.
    # this returns list of (osmid, [laz_paths], huc_name)
    tasks = discover_tasks(laz_base_dir)
    if not tasks:
        sys.exit(f"No valid LAZ files found under {laz_base_dir}")

    if lst_hucs:
        selected_hucs = set(lst_hucs.strip().split())
        tasks = [t for t in tasks if t[2] in selected_hucs]
        if not tasks:
            sys.exit(f"No LAZ files found for the requested HUCs: {selected_hucs}")

    huc_count = len({t[2] for t in tasks})
    file_logger.info(f"Working on {len(tasks)} osmid across {huc_count} HUC")
    print(f"Working on {len(tasks)} osmid across {huc_count} HUC")

    for huc in {t[2] for t in tasks}:
        os.makedirs(os.path.join(output_raster_dir, huc), exist_ok=True)

    ### start of parallel
    osmid_to_huc = {osmid: huc for osmid, _, huc in tasks}
    tasks_args_list = []
    for osmid, laz_paths, huc in tasks:
        bridges_crs = get_huc_vars(huc)['crs']
        output_dir = os.path.join(output_raster_dir, huc)
        tasks_args_list.append(
            {
                'osmid': osmid,
                'laz_paths': laz_paths,
                'output_dir': output_dir,
                'raster_resolution': raster_resolution,
                'bridges_crs': bridges_crs,
            }
        )

    # Run multiprocessing
    mp_results = run_with_mp(
        task_function=make_raster_for_osmid,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=job_number,
        task_id_key="osmid",
        show_progress=True,
    )

    failed_keys = [tid for tid, payload in mp_results.items() if not payload[0]]

    if not failed_keys:
        file_logger.info("✅ All multiprocessing tasks Succeeded")
        print("✅ All multiprocessing tasks Succeeded")
    else:
        file_logger.info(f"❌ {len(failed_keys)} failed:")
        print(f"❌ {len(failed_keys)} failed:")
        for tid in failed_keys:
            file_logger.info(f"  - {tid}")
            print(f"  - {tid}")

    classification_dfs = [
        payload[1].assign(huc=osmid_to_huc[task_id])
        for task_id, payload in mp_results.items()
        if payload[0] and payload[1] is not None
    ]
    elevation_dfs = [
        payload[2].assign(huc=osmid_to_huc[task_id])
        for task_id, payload in mp_results.items()
        if payload[0] and payload[2] is not None
    ]

    if classification_dfs:
        pd.concat(classification_dfs, ignore_index=True).to_csv(
            os.path.join(output_raster_dir, 'classifications_summary.csv'), index=False
        )
    if elevation_dfs:
        pd.concat(elevation_dfs, ignore_index=True).to_csv(
            os.path.join(output_raster_dir, 'bridge_elevation_filter_summary.csv'), index=False
        )

    end_time = datetime.now(timezone.utc)
    file_logger.info('TOTAL RUN TIME: ' + str(end_time - start_time))
    print('TOTAL RUN TIME: ' + str(end_time - start_time))
    print("Done!")
    file_logger.info("Done!")


if __name__ == "__main__":
    '''
    INPUT DATA PIPELINE — run scripts in this order:
        Step 1  : pull_osm_bridges.py          — pull OSM bridge lines per HUC
        Step 2a : make_rasters_using_lidar.py  — generate lidar TIFs (independent of 2b, can run in parallel)
        Step 2b : make_rasters_using_ml.py     — (this script) generate ML-classified lidar TIFs (independent of 2a, can run in parallel)
        Step 2c : make_modified_bridges.py     — add TIF flags, write huc_*_osm_bridges_modified.gpkg (run once)
        Step 3  : make_dem_dif_for_bridges.py  — build diff rasters per region (run 4× for CONUS/AK/GU/AS)

    Sample usage:
        python make_rasters_using_ml.py
          -l data/inputs/NGWPC/ML_bridges/20260514/laz_files
          -o data/inputs/NGWPC/ML_bridges/20260514/rasters
          -r 3
          -j 10

    TIFs are written to {output_raster_dir}/{HUC}/ml_osm_rasters/{osmid}.tif
    CRS is resolved automatically per HUC from $srcDir/bash_variables.env
    '''

    parser = argparse.ArgumentParser(
        description=(
            'Convert ML bridge-masked LAZ files to raster TIFs. '
            'Reads 8-digit HUC subdirectories from the LAZ base dir, groups files by osmid, '
            'concatenates points for the same osmid, applies bridge-point noise handling, '
            'and writes one TIF per osmid into the matching HUC subdirectory of the raster base dir.'
        )
    )

    parser.add_argument(
        '-l',
        '--laz_base_dir',
        help='REQUIRED: Root directory containing 8-digit HUC subdirectories with LAZ files.',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output_raster_dir',
        help='REQUIRED: Root directory where output HUC subdirectories and TIF files will be saved.',
        required=True,
    )

    parser.add_argument(
        '-r',
        '--raster_resolution',
        help='OPTIONAL: Output raster resolution in meters. Default=3.0',
        required=False,
        default=3.0,
        type=float,
    )

    parser.add_argument(
        '-j',
        '--job_number',
        help='OPTIONAL: Number of parallel worker processes. Default=10',
        required=False,
        default=10,
        type=int,
    )

    parser.add_argument(
        '-lh',
        '--lst_hucs',
        help='OPTIONAL: Space-delimited list of 8-digit HUCs to process. Defaults to all HUC directories found under laz_base_dir.',
        required=False,
        default='',
    )

    args = vars(parser.parse_args())
    process_ml_laz_to_rasters(**args)
