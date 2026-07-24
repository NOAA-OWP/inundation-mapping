import argparse
import glob
import logging
import os
import shlex
import sys

# import time
import traceback
from datetime import datetime, timezone

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio

# import rioxarray
import xarray as xr
from rasterio.features import rasterize
from rasterio.merge import merge
from shapely.geometry import Point

from data.create_vrt_file import create_vrt_file
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


"""Build bridge DEM-difference rasters from HUC8 bridge GeoPackages and lidar rasters."""


def rasters_to_point(tif_paths, file_logger, screen_queue, task_id):
    gdf_list = []
    for tif_file in tif_paths:
        with rasterio.open(tif_file) as src:
            raster_data = src.read(1)
            transform = src.transform
            nodata = src.nodata

            # Get the indices of all valid (non-nodata) pixels
            valid_mask = raster_data != nodata
            rows, cols = np.where(valid_mask)

            # Get raster values for the valid pixels
            values = raster_data[rows, cols]

            # Calculate the geographic coordinates for all valid pixels
            xs, ys = rasterio.transform.xy(transform, rows, cols, offset='center')

            # Create point geometries
            points = [Point(x, y) for x, y in zip(xs, ys)]

        gdf = gpd.GeoDataFrame(
            {'lidar_elev': values, 'osmid': os.path.basename(tif_file)[0:-4]}, geometry=points, crs=src.crs
        )
        gdf_list.append(gdf)

    combined_gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True))
    file_logger.info(f"{len(combined_gdf)} points for {task_id}")
    screen_queue.put(f"{len(combined_gdf)} points for {task_id}")
    return combined_gdf


def make_one_diff(
    dem_file,
    huc_bridge_file,
    lidar_raster_dir,
    ml_raster_dir,
    HUC,
    output_diff_path,
    file_logger,
    screen_queue,
    task_id,
):
    try:
        screen_queue.put(f"Start processing {task_id}")
        OSM_bridge_lines_gdf = None

        # Collect lidar and ML TIF paths for this HUC
        HUC_lidar_tif_paths = sorted(
            glob.glob(os.path.join(lidar_raster_dir, HUC, 'lidar_osm_rasters', '*.tif'))
        )
        HUC_ml_tif_paths = sorted(glob.glob(os.path.join(ml_raster_dir, HUC, '*.tif')))

        # Build combined TIF map: lidar takes priority; ML fills in where lidar is absent
        lidar_tif_map = {os.path.splitext(os.path.basename(p))[0]: p for p in HUC_lidar_tif_paths}
        ml_tif_map = {os.path.splitext(os.path.basename(p))[0]: p for p in HUC_ml_tif_paths}
        combined_tif_map = {**ml_tif_map, **lidar_tif_map}
        combined_tif_paths = sorted(combined_tif_map.values())

        if not os.path.exists(huc_bridge_file):
            file_logger.info(f"No HUC8 bridge gpkg found for {HUC} at {huc_bridge_file}")
            screen_queue.put(f"No HUC8 bridge gpkg found for {HUC} at {huc_bridge_file}")
        else:
            OSM_bridge_lines_gdf = gpd.read_file(huc_bridge_file)
            OSM_bridge_lines_gdf = OSM_bridge_lines_gdf[['osmid', 'geometry']].copy()
            OSM_bridge_lines_gdf['osmid'] = OSM_bridge_lines_gdf['osmid'].astype(str)

        if combined_tif_paths:
            file_logger.info(
                'working on HUC8 %s with %d combined rasters (%d lidar, %d ML): '
                % (str(HUC), len(combined_tif_paths), len(HUC_lidar_tif_paths), len(HUC_ml_tif_paths))
            )
            HUC_lidar_points_gdf = rasters_to_point(combined_tif_paths, file_logger, screen_queue, task_id)

            temp_buffer = OSM_bridge_lines_gdf[OSM_bridge_lines_gdf['osmid'].isin(combined_tif_map.keys())]

            # keep only the points within 2 meters of the bridge lines
            temp_buffer.loc[:, 'geometry'] = temp_buffer['geometry'].buffer(2)
            HUC_lidar_points_gdf = gpd.sjoin(HUC_lidar_points_gdf, temp_buffer, predicate='within')

            coords = [(geom.x, geom.y) for geom in HUC_lidar_points_gdf.geometry]
            with rasterio.open(dem_file) as src:
                raster = src.read(1)
                raster_meta = src.meta.copy()
                transform = src.transform
                nodata = src.nodata
                sampled_values = [value[0] for value in src.sample(coords)]

            HUC_lidar_points_gdf['ori_dem_elev'] = sampled_values
            HUC_lidar_points_gdf['elev_diff'] = (
                HUC_lidar_points_gdf['lidar_elev'] - HUC_lidar_points_gdf['ori_dem_elev']
            )

            shapes = (
                (geom, value)
                for geom, value in zip(HUC_lidar_points_gdf.geometry, HUC_lidar_points_gdf['elev_diff'])
            )

            updated_raster = rasterize(
                shapes=shapes,
                out_shape=raster.shape,
                transform=transform,
                fill=0,
                merge_alg=rasterio.enums.MergeAlg.replace,
                dtype=raster.dtype,
            )

            updated_raster[raster == nodata] = nodata

            raster_meta.update({'dtype': updated_raster.dtype, 'compress': 'lzw'})
            with rasterio.open(output_diff_path, 'w', **raster_meta) as dst:
                dst.write(updated_raster, 1)

        else:
            screen_queue.put('Making a diff raster file only with values of zero for HUC8:' + str(HUC))
            file_logger.info('Making a diff raster file only with values of zero for HUC8:' + str(HUC))

            with rasterio.open(dem_file) as src:
                raster = src.read(1)
                nodata = src.nodata
                raster_meta = src.meta.copy()

            updated_raster = np.where(raster == nodata, nodata, 0.0)

            raster_meta.update({'compress': 'lzw'})
            with rasterio.open(output_diff_path, 'w', **raster_meta) as dst:
                dst.write(updated_raster, 1)

        screen_queue.put(f"End of processing {task_id}")
        return 1, [True]

    except Exception as e:
        file_logger.error(f"❌ Exception in HUC {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return 0, [False]


def make_dif_rasters(
    dem_dir,
    lidar_raster_dir,
    ml_raster_dir,
    modified_bridge_dir,
    dem_diff_output_dir,
    number_jobs,
    cli_args=None,
):
    start_time = datetime.now(timezone.utc)
    if not os.path.exists(dem_diff_output_dir):
        os.makedirs(dem_diff_output_dir)

    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_path = os.path.join(dem_diff_output_dir, f"DEM_diff_rasters-{file_dt_string}.log")
    file_logger = setup_mp_file_logger(log_file_path, "DEM_diff_raster")
    if cli_args:
        file_logger.info(f"CLI invocation: {cli_args}")

    try:
        if not os.path.isdir(modified_bridge_dir):
            raise ValueError(f"Argument -i modified_bridge_dir of {modified_bridge_dir} does not exist.")
        if not os.path.isdir(lidar_raster_dir):
            raise ValueError(f"Argument -l lidar_raster_dir of {lidar_raster_dir} does not exist.")
        if not os.path.isdir(ml_raster_dir):
            raise ValueError(f"Argument -m ml_raster_dir of {ml_raster_dir} does not exist.")

        dem_files = list(glob.glob(os.path.join(dem_dir, '*.tif')))
        if len(dem_files) == 0:
            raise ValueError("No DEM files were found. Please recheck the DEM folder pathing")
        dem_files.sort()

        available_dif_files = list(glob.glob(os.path.join(dem_diff_output_dir, '*.tif')))
        base_names_no_ext = [
            os.path.splitext(os.path.basename(path))[0].split('_')[1] for path in available_dif_files
        ]

        tasks_args_list = []
        for dem_file in dem_files:
            # prepare path for output diff file
            base_name, extension = os.path.splitext(os.path.basename(dem_file))
            output_diff_file_name = f"{base_name}_diff{extension}"
            output_diff_path = os.path.join(dem_diff_output_dir, output_diff_file_name)
            HUC = base_name.split('_')[1]

            if HUC not in base_names_no_ext:
                tasks_args_list.append(
                    {
                        'dem_file': dem_file,
                        'huc_bridge_file': os.path.join(
                            modified_bridge_dir, f"huc_{HUC}_osm_bridges_modified.gpkg"
                        ),
                        'lidar_raster_dir': lidar_raster_dir,
                        'ml_raster_dir': ml_raster_dir,
                        'HUC': HUC,
                        'output_diff_path': output_diff_path,
                    }
                )

        # now start the multiprocessing
        # Shut down if an error is found.
        mp_results = run_with_mp(
            task_function=make_one_diff,
            tasks_args_list=tasks_args_list,
            file_logger=file_logger,
            max_workers=number_jobs,
            task_id_key="HUC",  # must be one of the task arg keys. used for status report
            show_progress=True,
        )

        print('multiprocessing tasks finished!')
        # only report if all succeeded or the failed ones
        failed_keys = [k for k, v in mp_results.items() if not v[0]]

        if not failed_keys:
            file_logger.info("✅ All multiprocessing tasks Succeeded")
            print("✅ All multiprocessing tasks Succeeded")
        else:
            file_logger.info(f"❌ {len(failed_keys)} failed:")
            print(f"❌ {len(failed_keys)} failed:")
            for k in failed_keys:
                file_logger.info(f"  - {k}")
                print(f"  - {k}")

        # now make a vrt file from all generated diff raster files
        print("==================")
        print('Making a vrt files from all diff raster files.')
        file_logger.info('Making a vrt files from all diff raster files')
        create_vrt_file(dem_diff_output_dir, 'bridge_elev_diff.vrt')

        # Record run time
        end_time = datetime.now(timezone.utc)
        tot_run_time = end_time - start_time
        print('TOTAL RUN TIME: ' + str(tot_run_time))
        file_logger.info('TOTAL RUN TIME: ' + str(tot_run_time))

    except Exception as ex:
        msg = f"*** An error occurred while making dem diffs : Details: {ex}"
        print(msg)
        file_logger.critical(msg)
        print(traceback.format_exc())
        file_logger.critical(traceback.format_exc())


if __name__ == "__main__":

    '''
    INPUT DATA PIPELINE — run scripts in this order:
        Step 1  : pull_osm_bridges.py          — pull OSM bridge lines per HUC
        Step 2a : make_rasters_using_lidar.py  — generate lidar TIFs (independent of 2b, can run in parallel)
        Step 2b : make_rasters_using_ml.py     — generate ML-classified lidar TIFs (independent of 2a, can run in parallel)
        Step 2c : make_modified_bridges.py     — add TIF flags, write huc_*_osm_bridges_modified.gpkg (run once)
        Step 3  : make_dem_dif_for_bridges.py  — (this script) build diff rasters per region (run 4× for CONUS/AK/GU/AS)

    This script must be run separately for each region (CONUS, AK, GU, AS).
    For each run only -d and -o need to be updated; -l, -m, -i remain the same across all regions.
    Note: Guam and AS may have no lidar or ML data, producing diff rasters with only zero values.

    Example for CONUS and Alaska:

    python /foss_fim/data/bridges/make_dem_dif_for_bridges.py \
     -d data/inputs/dems/3dep_dems/10m_5070/20260128/ \
     -l data/inputs/osm/bridges/lidar_data/20260315/ \
     -m data/inputs/NGWPC/ML_bridges/20260315/rasters/ \
     -i data/inputs/osm/bridges/modified_osm_bridges/20260315/ \
     -o data/inputs/osm/bridges/DEM_Diffs/20260315/conus/ \
     -j 10

    python /foss_fim/data/bridges/make_dem_dif_for_bridges.py \
     -d data/inputs/dems/3dep_dems/10m_South_Alaska/20260128/ \
     -l data/inputs/osm/bridges/lidar_data/20260315/ \
     -m data/inputs/NGWPC/ML_bridges/20260315/rasters/ \
     -i data/inputs/osm/bridges/modified_osm_bridges/20260315/ \
     -o data/inputs/osm/bridges/DEM_Diffs/20260315/alaska/ \
     -j 10

    Re-run triggers:
      - New OSM bridge data → re-run steps 2a, 2b, 2c, then this script
      - New DEMs only       → re-run this script only (re-use existing lidar/ML TIFs and modified GPKGs)
    '''

    parser = argparse.ArgumentParser(description='Make bridge dem difference rasters')

    parser.add_argument(
        '-d', '--dem_dir', help='REQUIRED: folder path where 3DEP dems are loated.', required=True
    )

    parser.add_argument(
        '-l',
        '--lidar_raster_dir',
        help='REQUIRED: folder path to the lidar-processing output root containing per-HUC lidar rasters.',
        required=True,
    )

    parser.add_argument(
        '-m',
        '--ml_raster_dir',
        help='REQUIRED: Root directory containing per-HUC subdirectories containing ML TIF files.',
        required=True,
    )

    parser.add_argument(
        '-i',
        '--modified_bridge_dir',
        help='REQUIRED: Folder containing huc_*_osm_bridges_modified.gpkg files (output of make_modified_bridges.py).',
        required=True,
    )

    parser.add_argument(
        '-o', '--dem_diff_output_dir', help='REQUIRED: folder path for output diff rasters.', required=True
    )

    parser.add_argument(
        '-j',
        '--number-jobs',
        help='OPTIONAL: Number of (jobs) cores/processes for making dem diffs, default is 10. ',
        required=False,
        default=10,
        type=int,
    )

    args = vars(parser.parse_args())
    args['cli_args'] = shlex.join(sys.argv)
    make_dif_rasters(**args)
