import argparse
import glob
import logging
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from datetime import datetime, timezone
from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rioxarray
import xarray as xr
from rasterio.features import rasterize
from rasterio.merge import merge
from shapely.geometry import Point

import utils.shared_functions as sf
from data.create_vrt_file import create_vrt_file
from utils.shared_functions import FIM_Helpers as fh


def identify_bridges_with_lidar(OSM_bridge_lines_gdf, lidar_tif_dir):
    # identify osmids with lidar-tif or not
    tif_ids = set(
        os.path.splitext(os.path.basename(f))[0] for f in os.listdir(lidar_tif_dir) if f.endswith('.tif')
    )
    OSM_bridge_lines_gdf['has_lidar_tif'] = OSM_bridge_lines_gdf['osmid'].apply(
        lambda x: 'Y' if str(x) in tif_ids else 'N'
    )
    return OSM_bridge_lines_gdf


def rasters_to_point(tif_paths):
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
    return combined_gdf


def make_one_diff(dem_file, OSM_bridge_lines_gdf, lidar_tif_dir, HUC,HUC_choice, output_diff_path):
    
    try:

        HUC6_lidar_tif_osmids = OSM_bridge_lines_gdf[
            (OSM_bridge_lines_gdf['HUC%d'%HUC_choice] == HUC) & (OSM_bridge_lines_gdf['has_lidar_tif'] == 'Y')
        ]['osmid'].values.tolist()
        HUC6_lidar_tif_paths = [
            os.path.join(lidar_tif_dir, f"{osmid}.tif") for osmid in HUC6_lidar_tif_osmids
        ]

        if HUC6_lidar_tif_paths:
            logging.info('working on HUC%d %s with %d osm rasters: '%(HUC_choice,str(HUC),len(HUC6_lidar_tif_paths)) )
            HUC6_lidar_points_gdf = rasters_to_point(HUC6_lidar_tif_paths)

            temp_buffer = OSM_bridge_lines_gdf[OSM_bridge_lines_gdf['osmid'].isin(HUC6_lidar_tif_osmids)]

            # make a buffer file because we want to keep only the points within 2 meter of the bridge lines
            temp_buffer.loc[:, 'geometry'] = temp_buffer['geometry'].buffer(2)
            HUC6_lidar_points_gdf = gpd.sjoin(HUC6_lidar_points_gdf, temp_buffer, predicate='within')

            # Sample raster values at each point location
            coords = [
                (geom.x, geom.y) for geom in HUC6_lidar_points_gdf.geometry
            ]  # Extract point coordinates
            with rasterio.open(dem_file) as src:
                raster = src.read(1)
                raster_meta = src.meta.copy()
                transform = src.transform
                nodata = src.nodata
                sampled_values = [value[0] for value in src.sample(coords)]  # Sample raster values

            # Step 5: Add the sampled values to the GeoDataFrame
            HUC6_lidar_points_gdf['ori_dem_elev'] = sampled_values
            HUC6_lidar_points_gdf['elev_diff'] = (
                HUC6_lidar_points_gdf['lidar_elev'] - HUC6_lidar_points_gdf['ori_dem_elev']
            )

            # Replace 'value' with the column in your GeoPackage that contains the point values
            shapes = (
                (geom, value)
                for geom, value in zip(HUC6_lidar_points_gdf.geometry, HUC6_lidar_points_gdf['elev_diff'])
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
            print('Making a diff raster file only with values of zero for HUC%d:'%HUC_choice + str(HUC))
            logging.info('Making a diff raster file only with values of zero for HUC%d:'%HUC_choice + str(HUC))

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

    except Exception:
        print('something is wrong for HUC: %s' % str(HUC))
        logging.info('something is wrong for HUC6: ' + str(HUC))


def make_dif_rasters(OSM_bridge_file, dem_dir, lidar_tif_dir, output_dir):
    # start time and setup logs
    start_time = datetime.now(timezone.utc)
    fh.print_start_header('Making HUC6 elev difference rasters', start_time)

    __setup_logger(output_dir)
    logging.info(f"Saving results in {output_dir}")

    try:
        print('reading osm bridge lines...')
        OSM_bridge_lines_gdf = gpd.read_file(OSM_bridge_file)

        print('adding HUC6 number and info about existence of lidar raster or not...')
        OSM_bridge_lines_gdf['HUC6'] = OSM_bridge_lines_gdf['HUC8'].str[:6]
        OSM_bridge_lines_gdf = identify_bridges_with_lidar(OSM_bridge_lines_gdf, lidar_tif_dir)

        dem_files = list(glob.glob(os.path.join(dem_dir, '*.tif')))

        # TODO delete below two lines
        available_dif_files = list(glob.glob(os.path.join(output_dir, '*.tif')))
        base_names_no_ext = [
            os.path.splitext(os.path.basename(path))[0].split('_')[1] for path in available_dif_files
        ]

        number_of_jobs = 6
        with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
            executor_dict = {}

            for dem_file in dem_files:
                # prepare path for output diff file
                base_name, extension = os.path.splitext(os.path.basename(dem_file))
                output_diff_file_name = f"{base_name}_diff{extension}"
                output_diff_path = os.path.join(output_dir, output_diff_file_name)
                HUC = base_name.split('_')[1]

                HUC_choice=len(HUC) #this is usually 8 or 6

                if HUC not in base_names_no_ext:

                    make_one_diff_args = {
                        'dem_file': dem_file,
                        'OSM_bridge_lines_gdf': OSM_bridge_lines_gdf,
                        'lidar_tif_dir': lidar_tif_dir,
                        'HUC': HUC,
                        'HUC_choice':HUC_choice,
                        'output_diff_path': output_diff_path,
                    }

                    try:
                        future = executor.submit(make_one_diff, **make_one_diff_args)
                        executor_dict[future] = dem_file
                    except Exception as ex:
                        summary = traceback.StackSummary.extract(traceback.walk_stack(None))
                        print(f"*** {ex}")
                        print(''.join(summary.format()))
                        logging.critical(f"*** {ex}")
                        logging.critical(''.join(summary.format()))
                        sys.exit(1)

            # Send the executor to the progress bar and wait for all tasks to finish
            sf.progress_bar_handler(executor_dict, "Making HUC6 Diff Raster files")

        # save with new info (with existence of lidar data or not)
        print('saving the osm bridge lines with info for existence of lidar rasters or not.')
        logging.info('saving the osm bridge lines with info for existence of lidar rasters or not')
        base, ext = os.path.splitext(os.path.basename(OSM_bridge_file))
        OSM_bridge_lines_gdf.to_file(os.path.join(output_dir, f"{base}_modified{ext}"))

        # now make a vrt file from all generated diff raster files
        print('Making a vrt files from all diff raster files.')
        logging.info('Making a vrt files from all diff raster files')
        create_vrt_file(output_dir, 'bridge_elev_diff.vrt')

        # Record run time
        end_time = datetime.now(timezone.utc)
        tot_run_time = end_time - start_time
        fh.print_end_header('Making HUC6 dem diff rasters complete', start_time, end_time)
        logging.info('TOTAL RUN TIME: ' + str(tot_run_time))
        logging.info(fh.print_date_time_duration(start_time, end_time))

    except Exception as ex:
        summary = traceback.StackSummary.extract(traceback.walk_stack(None))
        print(f"*** {ex}")
        print(''.join(summary.format()))
        logging.critical(f"*** {ex}")
        logging.critical(''.join(summary.format()))
        sys.exit(1)


def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"DEM_diff_rasters-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    logging.info(f'Started (UTC): {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


if __name__ == "__main__":

    # this code needs to be run twice: once for conus and once for Alaska :

    #    python foss_fim/data/bridges/make_dem_dif_for_bridges.py
    #    -i data/inputs/osm/bridges/250102/conus_osm_bridges.gpkg
    #    -d /data/inputs/dems/3dep_dems/10m_5070/20240916/
    #    -l data/inputs/osm/bridges/250102/CanBeDeleted_conus_lidar_osm_rasters/
    #    -o data/inputs/osm/bridges/250102/huc6_dem_diff/conus/

    #    python foss_fim/data/bridges/make_dem_dif_for_bridges.py
    #    -i data/inputs/osm/bridges/250102/alaska_osm_bridges.gpkg
    #    -d /data/inputs/dems/3dep_dems/10m_South_Alaska/20240916/
    #    -l data/inputs/osm/bridges/250102/CanBeDeleted_alaska_lidar_osm_rasters/
    #    -o data/inputs/osm/bridges/250102/huc6_dem_diff/alaska/

    parser = argparse.ArgumentParser(description='Make bridge dem difference rasters')

    parser.add_argument(
        '-i', '--OSM_bridge_file', help='REQUIRED: A gpkg that contains the bridges lines', required=True
    )

    parser.add_argument(
        '-d', '--dem_dir', help='REQUIRED: folder path where 3DEP dems are loated.', required=True
    )

    parser.add_argument(
        '-l',
        '--lidar_tif_dir',
        help='REQUIRED: folder path where lidar-gerenared bridge elevation rasters are located.',
        required=True,
    )

    parser.add_argument(
        '-o', '--output_dir', help='REQUIRED: folder path for output diff rasters.', required=True
    )

    args = vars(parser.parse_args())
    make_dif_rasters(**args)
