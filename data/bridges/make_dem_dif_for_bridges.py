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


def identify_bridges_with_lidar(OSM_bridge_lines_gdf, tif_ids):
    # identify osmids with lidar-tif or not

    OSM_bridge_lines_gdf['has_lidar_tif'] = OSM_bridge_lines_gdf['osmid'].apply(
        lambda x: 'Y' if str(x) in tif_ids else 'N'
    )
    return OSM_bridge_lines_gdf


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
    tif_ids,
    lidar_tif_dir,
    HUC,
    output_diff_path,
    output_bridge_path,
    file_logger,
    screen_queue,
    task_id,
):

    try:
        screen_queue.put(f"Start processing {task_id}")
        if not os.path.exists(huc_bridge_file):
            file_logger.info(f"No HUC8 bridge gpkg found for {HUC} at {huc_bridge_file}")
            screen_queue.put(f"No HUC8 bridge gpkg found for {HUC} at {huc_bridge_file}")
            return 1, [True]

        OSM_bridge_lines_gdf = gpd.read_file(huc_bridge_file)
        cols_to_keep = ['osmid', 'bridge_type', 'huc8', 'geometry']
        OSM_bridge_lines_gdf = OSM_bridge_lines_gdf[cols_to_keep]
        OSM_bridge_lines_gdf['osmid'] = OSM_bridge_lines_gdf['osmid'].astype(str)
        OSM_bridge_lines_gdf['huc8'] = OSM_bridge_lines_gdf['huc8'].astype(str)
        OSM_bridge_lines_gdf = identify_bridges_with_lidar(OSM_bridge_lines_gdf, tif_ids)
        OSM_bridge_lines_gdf.to_file(output_bridge_path)

        HUC_lidar_tif_osmids = OSM_bridge_lines_gdf[OSM_bridge_lines_gdf['has_lidar_tif'] == 'Y'][
            'osmid'
        ].values.tolist()
        HUC_lidar_tif_paths = [os.path.join(lidar_tif_dir, f"{osmid}.tif") for osmid in HUC_lidar_tif_osmids]

        if HUC_lidar_tif_paths:
            file_logger.info(
                'working on HUC8 %s with %d osm rasters: ' % (str(HUC), len(HUC_lidar_tif_paths))
            )
            HUC_lidar_points_gdf = rasters_to_point(HUC_lidar_tif_paths, file_logger, screen_queue, task_id)

            temp_buffer = OSM_bridge_lines_gdf[OSM_bridge_lines_gdf['osmid'].isin(HUC_lidar_tif_osmids)]

            # make a buffer file because we want to keep only the points within 2 meter of the bridge lines
            temp_buffer.loc[:, 'geometry'] = temp_buffer['geometry'].buffer(2)
            HUC_lidar_points_gdf = gpd.sjoin(HUC_lidar_points_gdf, temp_buffer, predicate='within')

            # Sample raster values at each point location
            coords = [(geom.x, geom.y) for geom in HUC_lidar_points_gdf.geometry]  # Extract point coordinates
            with rasterio.open(dem_file) as src:
                raster = src.read(1)
                raster_meta = src.meta.copy()
                transform = src.transform
                nodata = src.nodata
                sampled_values = [value[0] for value in src.sample(coords)]  # Sample raster values

            # Step 5: Add the sampled values to the GeoDataFrame
            HUC_lidar_points_gdf['ori_dem_elev'] = sampled_values
            HUC_lidar_points_gdf['elev_diff'] = (
                HUC_lidar_points_gdf['lidar_elev'] - HUC_lidar_points_gdf['ori_dem_elev']
            )

            # Replace 'value' with the column in your GeoPackage that contains the point values
            shapes = (
                (geom, value)
                for geom, value in zip(HUC_lidar_points_gdf.geometry, HUC_lidar_points_gdf['elev_diff'])
            )

            # Step 4: Rasterize the points
            updated_raster = rasterize(
                shapes=shapes,
                out_shape=raster.shape,  # Match the shape of the original raster
                transform=transform,  # Use the original raster's affine transform
                fill=0,  # Preserve 0 value for areas without points
                merge_alg=rasterio.enums.MergeAlg.replace,  # Replace raster values with point values
                dtype=raster.dtype,
            )

            # Apply the original raster's NoData mask
            updated_raster[raster == nodata] = nodata

            # Step 5: Save the updated raster
            raster_meta.update({'dtype': updated_raster.dtype, 'compress': 'lzw'})  # Update metadata
            with rasterio.open(output_diff_path, 'w', **raster_meta) as dst:
                dst.write(updated_raster, 1)

        else:
            screen_queue.put('Making a diff raster file only with values of zero for HUC8:' + str(HUC))
            file_logger.info('Making a diff raster file only with values of zero for HUC8:' + str(HUC))

            with rasterio.open(dem_file) as src:
                raster = src.read(1)
                nodata = src.nodata
                raster_meta = src.meta.copy()

            # Set all raster values except nodata to zero
            updated_raster = np.where(raster == nodata, nodata, 0.0)

            # Step 5: Save the updated raster
            raster_meta.update({'compress': 'lzw'})  # Update metadata to compress
            with rasterio.open(output_diff_path, 'w', **raster_meta) as dst:
                dst.write(updated_raster, 1)
        screen_queue.put(f"End of processing {task_id}")
        return 1, [True]

    except Exception as e:
        file_logger.error(f"❌ Exception in HUC {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return 0, [False]


def make_dif_rasters(dem_dir, lidar_tif_dir, OSM_bridge_dir, output_dir, number_jobs, cli_args=None):
    start_time = datetime.now(timezone.utc)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_path = os.path.join(output_dir, f"DEM_diff_rasters-{file_dt_string}.log")
    file_logger = setup_mp_file_logger(log_file_path, "DEM_diff_raster")
    if cli_args:
        file_logger.info(f"CLI invocation: {cli_args}")

    try:
        if not os.path.isdir(OSM_bridge_dir):
            raise ValueError(f"Argument -i OSM_bridge_dir of {OSM_bridge_dir} does not exist.")

        # capture all available lidar-based raster files
        tif_ids = set(
            os.path.splitext(os.path.basename(f))[0] for f in os.listdir(lidar_tif_dir) if f.endswith('.tif')
        )
        if len(tif_ids) == 0:
            raise ValueError("There are no bridges with lidar data, check data and tif folder pathing.")

        dem_files = list(glob.glob(os.path.join(dem_dir, '*.tif')))
        if len(dem_files) == 0:
            raise ValueError("No DEM files were found. Please recheck the DEM folder pathing")
        dem_files.sort()

        available_dif_files = list(glob.glob(os.path.join(output_dir, '*.tif')))
        base_names_no_ext = [
            os.path.splitext(os.path.basename(path))[0].split('_')[1] for path in available_dif_files
        ]

        tasks_args_list = []
        for dem_file in dem_files:
            # prepare path for output diff file
            base_name, extension = os.path.splitext(os.path.basename(dem_file))
            output_diff_file_name = f"{base_name}_diff{extension}"
            output_diff_path = os.path.join(output_dir, output_diff_file_name)
            HUC = base_name.split('_')[1]
            output_bridge_path = os.path.join(output_dir, f"huc_{HUC}_osm_bridges_modified.gpkg")

            if HUC not in base_names_no_ext:
                tasks_args_list.append(
                    {
                        'dem_file': dem_file,
                        'huc_bridge_file': os.path.join(OSM_bridge_dir, f"huc_{HUC}_osm_bridges.gpkg"),
                        'tif_ids': tif_ids,
                        'lidar_tif_dir': lidar_tif_dir,
                        'HUC': HUC,
                        'output_diff_path': output_diff_path,
                        'output_bridge_path': output_bridge_path,
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
        failed_keys = [k for k, v in mp_results.items() if not v]

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
        create_vrt_file(output_dir, 'bridge_elev_diff.vrt')

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

    # NOTE that the result of this script is used as preclipped data,
    #   because pre-clipped osm data must have "has_lidar_tif" field showing existence of lidar or not
    #   This code needs to be run twice: once for conus and once for Alaska. Guam and AS are not
    # needed because there is no lidar-based elevation raster for them :

    # python /foss_fim/data/bridges/make_dem_dif_for_bridges.py \
    #  -d data/inputs/dems/3dep_dems/10m_5070/20260128/ \
    #  -l data/inputs/osm/bridges/lidar_data/20260315/conus_lidar_rasters/lidar_osm_rasters/ \
    #  -b data/inputs/osm/bridges/bridge_lines/20260315/ \
    #  -o data/inputs/osm/bridges/DEM_Diffs/20260315/conus/ \
    #  -j 10

    #  python /foss_fim/data/bridges/make_dem_dif_for_bridges.py \
    #  -d data/inputs/dems/3dep_dems/10m_South_Alaska/20260128/ \
    #  -l data/inputs/osm/bridges/lidar_data/20260315/alaska_lidar_rasters/lidar_osm_rasters/ \
    #  -b data/inputs/osm/bridges/bridge_lines/20260315/ \
    #  -o data/inputs/osm/bridges/DEM_Diffs/20260315/alaska/ \
    #  -j 10
    # runs in under 1 min

    # This tool needs to be run 4 times, once for Conus, AK, GU and AS
    # Note: TBD: Guam and AS might not have any lidar data.

    ###############################
    #
    # If new OSM bridge data is pulled, it will trigger new bridge lidar date, which would trigger
    #   running this tool.
    #
    # Independently, if new DEMs are pulled, then we need to re-run this tool. Assuming we still
    #    have the most recent Bridge Lidar, which may/may not need to be re-run. It is only needed if
    #    new OSM data is run.
    #
    #  After running this tool, you will get a new "modified" bridge files.
    #    ie) conus_osm_bridges_modified.gpkg,  ak_osm_bridges_modified, etc
    #
    #  You will also get new DEM Diff VRT's each time you run it. ie) bridge_elev_diff.vrt
    #
    #  Bash Variables will need to be updated for all four of these files and copied to all 5 enviros.
    #
    ###############################

    parser = argparse.ArgumentParser(description='Make bridge dem difference rasters')

    parser.add_argument(
        '-d', '--dem_dir', help='REQUIRED: folder path where 3DEP dems are loated.', required=True
    )

    parser.add_argument(
        '-l',
        '--lidar_tif_dir',
        help='REQUIRED: folder path where lidar-gerenared bridge elevation rasters corresponding to the 3DEP dems are located.',
        required=True,
    )

    parser.add_argument(
        '-b',
        '--OSM_bridge_dir',
        help='REQUIRED: Folder containing all HUC-level gpkg bridge line files for all regions.',
        required=True,
    )

    parser.add_argument(
        '-o', '--output_dir', help='REQUIRED: folder path for output diff rasters.', required=True
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
