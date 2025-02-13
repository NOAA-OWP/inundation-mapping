import argparse
import glob
import json
import logging
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

import geopandas as gpd
import laspy
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pdal
import xarray as xr
from scipy.spatial import KDTree
from shapely.geometry import MultiPoint, Point
from tqdm import tqdm


def progress_bar_handler(executor_dict, desc):
    for future in tqdm(as_completed(executor_dict), total=len(executor_dict), desc=desc):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))


def download_lidar_points(osmid, poly_geo, lidar_url, output_dir, bridges_crs):
    try:
        poly_wkt = poly_geo.wkt
        las_file_path = os.path.join(output_dir, 'point_files', '%s.las' % str(osmid))

        # based on pdal documentation, The polygon wkt can be followed by a slash (‘/’) and a spatial reference specification to apply to the polygon.
        my_pipe = {
            "pipeline": [
                {
                    "polygon": str(poly_wkt) + '/%s' % bridges_crs,
                    "filename": lidar_url,
                    "type": "readers.ept",
                    "tag": "readdata",
                },
                {
                    "type": "filters.returns",
                    "groups": "last,only",  # need both last and only because 'last' applies only when there are multiple returns and does not include cases with a single return.
                },
                {  # make sure to reproject to desired crs. Otherwise the points will be in EPSG 3857
                    "in_srs": 'EPSG:3857',
                    "out_srs": '%s' % bridges_crs,
                    "type": "filters.reprojection",
                    "tag": "reprojected",
                },
                {"filename": las_file_path, "tag": "writerslas", "type": "writers.las"},
            ]
        }

        # Create a PDAL pipeline object
        pipeline = pdal.Pipeline(json.dumps(my_pipe))

        # Execute the pipeline
        pipeline.execute()

    except Exception as e:
        error_message = f"Error processing {osmid}: {str(e)}"
        print(error_message)
        logging.error(error_message)
        traceback.print_exc()


def las_to_gpkg(osmid, las_path, bridges_crs):
    las = laspy.read(las_path)

    # make x,y coordinates
    x_y = np.vstack((np.array(las.x), np.array(las.y))).transpose()

    # convert the coordinates to a list of shapely Points
    las_points = list(MultiPoint(x_y).geoms)

    # put the points in a GeoDataFrame for a more standard syntax through Geopandas
    points_gdf = gpd.GeoDataFrame(geometry=las_points, crs=bridges_crs)

    # add other required data into gdf...here only elevation
    z_values = np.array(las.z)
    points_gdf['z'] = z_values

    return_values = np.array(las.return_number)
    points_gdf['return'] = return_values

    class_values = np.array(las.classification)
    points_gdf['classification'] = class_values

    number_of_returns = np.array(las.number_of_returns)
    points_gdf['number_of_returns'] = number_of_returns

    # note that pdal uses below scenarios of 'return number' and 'number_of_return' as described in documentauon below to identify, first, and last returns ...
    # There is no other way to do this classifcation task. Note that x,y of different returns of a specifc pulse can be in different locations. so you should not
    # expect to have multiple points at a exact x, y with different return numbers.
    #  Also, 'point_source_id' are usually not reliable.  So, the only way to assign return numbers is by comparing return number and 'number of returns' as pdal is doing:
    # https://pdal.io/en/latest/stages/filters.returns.html

    classification_counts = points_gdf.groupby('classification').size().reset_index()
    classification_counts.columns = ['class_code', 'count']

    classification_counts['count_Percent'] = 100 * classification_counts['count'] / len(points_gdf)
    classification_counts['osmid'] = osmid

    return points_gdf, classification_counts


def handle_noises(points_gdf):
    # Replace non-bridge point values with the average of the nearest two bridge points (with classification codes 13 or 17_).

    points_gdf.loc[:, 'x'] = points_gdf.geometry.x
    points_gdf.loc[:, 'y'] = points_gdf.geometry.y

    # save the original z
    points_gdf.loc[:, 'origi_z'] = points_gdf['z'].values

    noise_bool = ~points_gdf['classification'].isin([17, 13])
    points_gdf.loc[:, 'noise'] = np.where(noise_bool, 'y', 'n')

    non_noises = points_gdf[points_gdf['noise'] == 'n']
    noises = points_gdf[(points_gdf['noise'] == 'y')]
    if len(non_noises) / len(points_gdf) < 0.05:  # if there is few class 13 and 17
        return None

    # # Create KDTree using x and y coordinates of non-noise points across all classes
    tree = KDTree(non_noises[['x', 'y']])

    # Find the 2 nearest neighbors for each outlier in class 1 based on x, y coordinates
    _, indices = tree.query(noises[['x', 'y']].values, k=2)

    # Get the z values of the nearest 2 neighbors
    nearest_z_values = non_noises.iloc[indices.flatten()]['z'].values.reshape(indices.shape)
    noises.loc[:, 'z'] = np.mean(nearest_z_values, axis=1)

    modified_points_gdf = pd.concat([noises, non_noises])

    return modified_points_gdf


def make_local_tifs(modified_las_path, raster_resolution, bridges_crs, tif_path):
    my_pipe = {
        "pipeline": [
            {"type": "readers.las", "filename": modified_las_path, "spatialreference": bridges_crs},
            {
                "type": "writers.gdal",
                "filename": tif_path,
                "dimension": "Z",
                "output_type": "idw",  # or try  "mean",
                "resolution": raster_resolution,
                "nodata": -999,
                "data_type": "float32",
            },
        ]
    }

    # Create a PDAL pipeline object
    pipeline = pdal.Pipeline(json.dumps(my_pipe))

    # Execute the pipeline
    pipeline.execute()


def gpkg_to_las(points_gdf):

    x = points_gdf.geometry.x
    y = points_gdf.geometry.y
    z = points_gdf['z'].values

    header = laspy.LasHeader()
    las_obj = laspy.LasData(header)
    las_obj.x = x
    las_obj.y = y
    las_obj.z = z
    return las_obj


def make_lidar_footprints(bridges_crs):
    str_hobu_footprints = (
        r"https://raw.githubusercontent.com/hobu/usgs-lidar/master/boundaries/boundaries.topojson"
    )
    entwine_footprints_gdf = gpd.read_file(str_hobu_footprints)
    entwine_footprints_gdf.set_crs(
        "epsg:4326", inplace=True
    )  # it is geographic (lat-long degrees) commonly used for GPS for accurate locations

    entwine_footprints_gdf.to_crs(bridges_crs, inplace=True)
    return entwine_footprints_gdf


def make_rasters_in_parallel(osmid, points_path, output_dir, raster_resolution, bridges_crs):
    try:

        # #make a gpkg file from points
        points_gdf, classification_counts = las_to_gpkg(osmid, points_path, bridges_crs)

        if not points_gdf.empty:
            modified_points_gdf = handle_noises(points_gdf)
            if modified_points_gdf is None:
                return

            # make a las file for subsequent pdal pipeline
            modified_las_path = os.path.join(output_dir, 'point_files', '%s_modified.las' % osmid)
            las_obj = gpkg_to_las(modified_points_gdf)
            las_obj.write(modified_las_path)

            # make tif files
            tif_output = os.path.join(output_dir, 'lidar_osm_rasters', '%s.tif' % osmid)
            make_local_tifs(modified_las_path, raster_resolution, bridges_crs, tif_output)
            os.remove(modified_las_path)

        else:
            logging.info("No points available for osmid: %s" % str(osmid))
            print("No points available for osmid: %s" % str(osmid))

        return classification_counts

    except Exception as e:
        error_message = f"Error processing {osmid}: {str(e)}"
        print(error_message)
        logging.error(error_message)
        traceback.print_exc()


def process_bridges_lidar_data(OSM_bridge_file, buffer_width, raster_resolution, output_dir):
    # start time and setup logs
    start_time = datetime.now(timezone.utc)

    # check existence of output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    else:
        non_log_files = [f for f in os.listdir(output_dir) if not f.endswith(".log")]
        if non_log_files:  # if output directory has any file exepting previous log files, stop the code.
            sys.exit(
                f" Error: {output_dir} contains some files. Either remove them or provide an empty directory. Program terminated."
            )

    __setup_logger(output_dir)
    logging.info(f"Making elevation raster files for osm bridges {start_time}")

    logging.info(f"Saving results in {output_dir}")

    # check input file veracity
    if not OSM_bridge_file.endswith(".gpkg"):
        logging.critical(f" Error: {OSM_bridge_file} is not a .gpkg file. Program terminated.")
        sys.exit(f" Error: {OSM_bridge_file} is not a .gpkg file. Program terminated.")

    try:

        # Create subfolders 'point_points' and 'lidar_osm_rasters' inside the moutput folder
        point_dir = os.path.join(output_dir, 'point_files')
        tif_files_dir = os.path.join(output_dir, 'lidar_osm_rasters')

        os.makedirs(point_dir, exist_ok=True)
        os.makedirs(tif_files_dir, exist_ok=True)

        text = 'read osm bridge lines and make a polygon footprint'
        print(text)
        logging.info(text)
        OSM_bridge_lines_gdf = gpd.read_file(OSM_bridge_file)

        # osm file must contain osmid field
        if 'osmid' not in OSM_bridge_lines_gdf.columns:
            logging.critical(f"Error: {OSM_bridge_file} is missing osmid column. Program terminated.")
            sys.exit(f"Error: {OSM_bridge_file} is missing osmid column. Program terminated.")

        OSM_polygons_gdf = OSM_bridge_lines_gdf.copy()
        OSM_polygons_gdf['geometry'] = OSM_polygons_gdf['geometry'].buffer(buffer_width)

        if 'name' in OSM_polygons_gdf.columns:
            OSM_polygons_gdf.rename(columns={'name': 'bridge_name'}, inplace=True)
        bridges_crs = str(OSM_polygons_gdf.crs)  # parallel processing arguments do not like crs objects

        # produce footprints of lidar dataset over conus
        text = 'generating footprints of available CONUS lidar datasets'
        print(text)
        logging.info(text)
        entwine_footprints_gdf = make_lidar_footprints(bridges_crs)

        # intersect with lidar urls
        text = 'Identify USGS/Entwine lidar URLs for intersecting with each bridge polygon'
        print(text)
        logging.info(text)
        OSM_polygons_gdf = gpd.overlay(OSM_polygons_gdf, entwine_footprints_gdf, how='intersection')

        OSM_polygons_gdf.to_file(os.path.join(output_dir, 'buffered_bridges.gpkg'))

        # filter if there are multiple urls for a bridge, keep the url with highest count
        OSM_polygons_gdf = OSM_polygons_gdf.loc[OSM_polygons_gdf.groupby('osmid')['count'].idxmax()]
        OSM_polygons_gdf = OSM_polygons_gdf.reset_index(drop=True)

        text = 'download last-return lidar points (with epsg:3857) within each bridge polygon from the identified URLs'
        print(text)
        logging.info(text)

        executor_dict = {}

        print(f"There are {len(OSM_polygons_gdf)} files to download")

        with ProcessPoolExecutor(max_workers=15) as executor:
            for i, row in OSM_polygons_gdf.iterrows():
                osmid, poly_geo, lidar_url = row.osmid, row.geometry, row.url

                download_lidar_args = {
                    'osmid': osmid,
                    'poly_geo': poly_geo,
                    'lidar_url': lidar_url,
                    'output_dir': output_dir,
                    'bridges_crs': bridges_crs,
                }

                try:
                    future = executor.submit(download_lidar_points, **download_lidar_args)
                    executor_dict[future] = osmid  # Store task association
                except Exception as ex:
                    summary = traceback.StackSummary.extract(traceback.walk_stack(None))
                    print(f"*** {ex}")
                    print(''.join(summary.format()))
                    logging.critical(f"*** {ex}")
                    logging.critical(''.join(summary.format()))
                    sys.exit(1)

            # Progress bar handler
            progress_bar_handler(executor_dict, "Downloading Lidar Points")

        text = 'Generate raster files after filtering the points for bridge classification codes'
        print(text)
        logging.info(text)
        downloaded_points_files = glob.glob(os.path.join(output_dir, 'point_files', '*.las'))

        executor_dict = {}

        with ProcessPoolExecutor(max_workers=10) as executor:
            for points_path in downloaded_points_files:
                osmid = os.path.basename(points_path).split('.las')[0]

                make_rasters_args = {
                    'osmid': osmid,
                    'points_path': points_path,
                    'output_dir': output_dir,
                    'raster_resolution': raster_resolution,
                    'bridges_crs': bridges_crs,
                }

                try:
                    future = executor.submit(make_rasters_in_parallel, **make_rasters_args)
                    executor_dict[future] = points_path  # Store task association
                except Exception as ex:
                    summary = traceback.StackSummary.extract(traceback.walk_stack(None))
                    print(f"*** {ex}")
                    print(''.join(summary.format()))
                    logging.critical(f"*** {ex}")
                    logging.critical(''.join(summary.format()))
                    sys.exit(1)

            # Progress bar handler
            progress_bar_handler(executor_dict, "Processing Rasters")

        # Collect results
        list_of_classification_results = [future.result() for future in executor_dict]

        # Combine results into a DataFrame
        bridges_classifications_df = pd.concat(list_of_classification_results, ignore_index=True)

        bridges_classifications_df.to_csv(
            os.path.join(output_dir, 'classifications_summary.csv'), index=False
        )

        # Record run time
        end_time = datetime.now(timezone.utc)
        tot_run_time = end_time - start_time
        logging.info('TOTAL RUN TIME: ' + str(tot_run_time))

    except Exception as ex:
        error_message = traceback.format_exc()
        print(f"Critical Error: {ex}")
        print(error_message)
        logging.critical(f"Critical Error: {ex}")
        logging.critical(error_message)
        sys.exit(1)


def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"osm_lidar_rasters-{file_dt_string}.log"

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

    # Sample usage:
    # python create_osm_raster_using_lidar.py
    # -i osm_all_bridges.gpkg
    # -b 1.5
    # -r 3
    # -o /results/02050206

    parser = argparse.ArgumentParser(
        description='Download lidar points for buffered OSM bridges and make tif files'
    )

    parser.add_argument(
        '-i', '--OSM_bridge_file', help='REQUIRED: A gpkg that contains the bridges lines', required=True
    )

    parser.add_argument(
        '-b',
        '--buffer_width',
        help='OPTIONAL: Buffer to apply to OSM bridge lines to select lidar points within the buffered area. Default value is 1.5m (on each side)',
        required=False,
        default=1.5,
        type=float,
    )

    parser.add_argument(
        '-r',
        '--raster_resolution',
        help='OPTIONAL: Resolution of bridge raster files generated from lidar. Default value is 3m',
        required=False,
        default=3.0,
        type=float,
    )

    parser.add_argument(
        '-o', '--output_dir', help='REQUIRED: folder path where results will be saved to.', required=True
    )

    args = vars(parser.parse_args())

    process_bridges_lidar_data(**args)
