import argparse
import glob
import json
import logging
import os
import shlex
import subprocess
import sys
import tempfile
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

import geopandas as gpd
import laspy
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from scipy.spatial import KDTree
from shapely.geometry import MultiPoint, Point
from tqdm import tqdm


PDAL_CLI_PATH = os.environ.get("PDAL_CLI_PATH", "pdal")
PDAL_ENV_ROOT = os.environ.get("PDAL_ENV_ROOT")
PDAL_PIPELINE_TIMEOUT_SECONDS = 20 * 60


def progress_bar_handler(executor_dict, desc):
    for future in tqdm(as_completed(executor_dict), total=len(executor_dict), desc=desc):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))


def run_pdal_pipeline(my_pipe):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        json.dump(my_pipe, temp_file)
        temp_path = temp_file.name

    try:
        pdal_env = os.environ.copy()
        if PDAL_ENV_ROOT:
            pdal_env["PROJ_LIB"] = os.path.join(PDAL_ENV_ROOT, "share", "proj")
            pdal_env["PROJ_DATA"] = os.path.join(PDAL_ENV_ROOT, "share", "proj")
            pdal_env["GDAL_DATA"] = os.path.join(PDAL_ENV_ROOT, "share", "gdal")

        result = subprocess.run(
            [PDAL_CLI_PATH, "pipeline", temp_path],
            check=False,
            capture_output=True,
            text=True,
            env=pdal_env,
            timeout=PDAL_PIPELINE_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            pipeline_summary = {
                "stages": [stage.get("type") for stage in my_pipe.get("pipeline", [])],
                "filename": next(
                    (
                        stage.get("filename")
                        for stage in my_pipe.get("pipeline", [])
                        if stage.get("type") == "readers.ept"
                    ),
                    None,
                ),
                "output": next(
                    (
                        stage.get("filename")
                        for stage in my_pipe.get("pipeline", [])
                        if str(stage.get("type", "")).startswith("writers.")
                    ),
                    None,
                ),
            }
            raise RuntimeError(
                f"PDAL pipeline failed with exit code {result.returncode}. "
                f"stdout={result.stdout.strip()!r} stderr={result.stderr.strip()!r} "
                f"pipeline_file={temp_path!r} pipeline_summary={json.dumps(pipeline_summary)}"
            )
    finally:
        os.remove(temp_path)


def download_lidar_points(osmid, poly_geo, lidar_url, output_dir, bridges_crs, url_name):
    try:
        poly_wkt = poly_geo.wkt
        las_file_path = os.path.join(output_dir, 'point_files', '%s_%s.las' % (str(osmid), url_name))

        # based on pdal documentation, The polygon wkt can be followed by a slash ("/") and a spatial reference specification to apply to the polygon.
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

        run_pdal_pipeline(my_pipe)

    except Exception as e:
        error_message = f"Error processing osmid={osmid}, url_name={url_name}: {str(e)}"
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

    return points_gdf


def summarize_classification_counts(points_gdf, osmid):
    classification_counts = points_gdf.groupby('classification').size().reset_index()
    classification_counts.columns = ['class_code', 'count']
    classification_counts['count_Percent'] = 100 * classification_counts['count'] / len(points_gdf)
    classification_counts['osmid'] = osmid
    return classification_counts


def handle_noises(points_gdf, osmid, threshold_m=10, bridge_classes=(13, 17)):
    points_gdf.loc[:, 'x'] = points_gdf.geometry.x
    points_gdf.loc[:, 'y'] = points_gdf.geometry.y

    # save the original z
    points_gdf.loc[:, 'origi_z'] = points_gdf['z'].values

    bridge_points = points_gdf[points_gdf['classification'].isin(bridge_classes)].copy()

    if bridge_points.empty:
        summary = pd.DataFrame(
            [
                {
                    'osmid': osmid,
                    'median_bridge_z': np.nan,
                    'min_bridge_z': np.nan,
                    'max_bridge_z': np.nan,
                    'pct_bridge_points_below_threshold_removed': 0.0,
                    'pct_bridge_points_above_threshold_removed': 0.0,
                }
            ]
        )
        return None, summary

    median_bridge_z = bridge_points['origi_z'].median()
    min_bridge_z = bridge_points['origi_z'].min()
    max_bridge_z = bridge_points['origi_z'].max()
    total_bridge_points = len(bridge_points)
    lower_threshold = median_bridge_z - threshold_m
    upper_threshold = median_bridge_z + threshold_m

    below_threshold_mask = bridge_points['origi_z'] < lower_threshold
    above_threshold_mask = bridge_points['origi_z'] > upper_threshold
    is_approved_bridge = (
        points_gdf['classification'].isin(bridge_classes)
        & (points_gdf['origi_z'] >= lower_threshold)
        & (points_gdf['origi_z'] <= upper_threshold)
    )

    summary = pd.DataFrame(
        [
            {
                'osmid': osmid,
                'median_bridge_z': median_bridge_z,
                'min_bridge_z': min_bridge_z,
                'max_bridge_z': max_bridge_z,
                'pct_bridge_points_below_threshold_removed': 100
                * float(below_threshold_mask.sum())
                / total_bridge_points,
                'pct_bridge_points_above_threshold_removed': 100
                * float(above_threshold_mask.sum())
                / total_bridge_points,
            }
        ]
    )

    points_gdf.loc[:, 'noise'] = np.where(is_approved_bridge, 'n', 'y')

    non_noises = points_gdf[points_gdf['noise'] == 'n']
    noises = points_gdf[(points_gdf['noise'] == 'y')]
    if len(non_noises) / len(points_gdf) < 0.05:  # if there is few class 13 and 17
        return None, summary

    # # Create KDTree using x and y coordinates of non-noise points across all classes
    tree = KDTree(non_noises[['x', 'y']])

    # Find the 2 nearest neighbors for each outlier in class 1 based on x, y coordinates
    _, indices = tree.query(noises[['x', 'y']].values, k=2)

    # Get the z values of the nearest 2 neighbors
    nearest_z_values = non_noises.iloc[indices.flatten()]['z'].values.reshape(indices.shape)
    noises.loc[:, 'z'] = np.mean(nearest_z_values, axis=1)

    modified_points_gdf = pd.concat([noises, non_noises])

    return modified_points_gdf, summary


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

    run_pdal_pipeline(my_pipe)


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


def make_rasters_in_parallel(
    osmid, points_paths, output_dir, raster_resolution, bridges_crs, remove_las_files=True
):
    try:
        all_points_gdfs = []
        for points_path in points_paths:
            try:
                points_gdf = las_to_gpkg(osmid, points_path, bridges_crs)
            finally:
                if remove_las_files:
                    try:
                        os.remove(points_path)
                    except FileNotFoundError:
                        pass

            if not points_gdf.empty:
                points_gdf = points_gdf.copy()
                points_gdf['source_las'] = os.path.basename(points_path)
                all_points_gdfs.append(points_gdf)

        if not all_points_gdfs:
            logging.info("No points available for osmid: %s" % str(osmid))
            return

        points_gdf = gpd.GeoDataFrame(pd.concat(all_points_gdfs, ignore_index=True), crs=bridges_crs)
        classification_counts = summarize_classification_counts(points_gdf, osmid)
        modified_points_gdf, elevation_filter_summary = handle_noises(points_gdf, osmid)

        if not points_gdf.empty:
            if modified_points_gdf is None:
                logging.info("No approved for osmid: %s" % str(osmid))
                return {
                    'classification_counts': classification_counts,
                    'elevation_filter_summary': elevation_filter_summary,
                }

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

        return {
            'classification_counts': classification_counts,
            'elevation_filter_summary': elevation_filter_summary,
        }

    except Exception as e:
        error_message = f"Error processing {osmid}: {str(e)}"
        print(error_message)
        logging.error(error_message)
        traceback.print_exc()


def process_bridges_lidar_data(
    OSM_bridge_file,
    buffer_width,
    raster_resolution,
    output_dir,
    job_number_lidar,
    job_number_raster,
    remove_las_files=True,
    cli_args=None,
):
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
    if cli_args:
        logging.info(f"CLI invocation: {cli_args}")

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

        # make sure osmid is string
        OSM_bridge_lines_gdf["osmid"] = OSM_bridge_lines_gdf["osmid"].astype(str)

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
        entwine_footprints_gdf.to_file(os.path.join(output_dir, 'entwine_footprints.gpkg'))

        # intersect with lidar urls
        text = 'Identify USGS/Entwine lidar URLs for intersecting with each bridge polygon'
        print(text)
        logging.info(text)
        OSM_polygons_gdf = gpd.overlay(OSM_polygons_gdf, entwine_footprints_gdf, how='intersection')

        OSM_polygons_gdf.to_file(os.path.join(output_dir, 'buffered_bridges.gpkg'))

        text = f'download last-return lidar points within each bridge polygon from the identified URLs using {job_number_lidar} processors'
        print(text)
        logging.info(text)

        executor_dict = {}

        print(f"There are {len(OSM_polygons_gdf)} files to download")

        with ProcessPoolExecutor(max_workers=job_number_lidar) as executor:
            for i, row in OSM_polygons_gdf.iterrows():
                osmid, poly_geo, lidar_url, url_name = row.osmid, row.geometry, row.url, row['name']

                download_lidar_args = {
                    'osmid': osmid,
                    'poly_geo': poly_geo,
                    'lidar_url': lidar_url,
                    'output_dir': output_dir,
                    'bridges_crs': bridges_crs,
                    'url_name': url_name,
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

        text = f'Generate raster files after filtering the points for bridge classification codes using {job_number_raster} processors'
        print(text)
        logging.info(text)
        downloaded_points_files = glob.glob(os.path.join(output_dir, 'point_files', '*.las'))
        osmid_to_points_paths = defaultdict(list)

        for points_path in downloaded_points_files:
            points_file_name = os.path.basename(points_path)
            osmid = points_file_name.split('_', 1)[0]
            osmid_to_points_paths[osmid].append(points_path)

        executor_dict = {}

        with ProcessPoolExecutor(max_workers=job_number_raster) as executor:
            for osmid, points_paths in osmid_to_points_paths.items():

                make_rasters_args = {
                    'osmid': osmid,
                    'points_paths': sorted(points_paths),
                    'output_dir': output_dir,
                    'raster_resolution': raster_resolution,
                    'bridges_crs': bridges_crs,
                    'remove_las_files': remove_las_files,
                }

                try:
                    future = executor.submit(make_rasters_in_parallel, **make_rasters_args)
                    executor_dict[future] = osmid  # Store task association
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
        raster_results = [future.result() for future in executor_dict]

        # filter None which are for the cases when no raster is created
        raster_results = [result for result in raster_results if result is not None]

        list_of_classification_results = [result['classification_counts'] for result in raster_results]
        elevation_filter_summaries = [result['elevation_filter_summary'] for result in raster_results]

        if list_of_classification_results:
            # Combine results into a DataFrame
            bridges_classifications_df = pd.concat(list_of_classification_results, ignore_index=True)

            bridges_classifications_df.to_csv(
                os.path.join(output_dir, 'classifications_summary.csv'), index=False
            )
        else:
            logging.info('No Raster file was created.')
            print('No Raster file was created.')

        if elevation_filter_summaries:
            elevation_filter_summary_df = pd.concat(elevation_filter_summaries, ignore_index=True)
            elevation_filter_summary_df.to_csv(
                os.path.join(output_dir, 'bridge_elevation_filter_summary.csv'), index=False
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

    # This tool must be run in a conda enviro. See the 'setup_conda_for_make_rasters.txt'
    #    for more details. Other steps can be run in our standard docker enviros.
    #
    # Sample usage:  (using a the conda enviro specifically for this step)
    # python make_rasters_using_lidar.py
    #  -i /data/inputs/osm/bridges/bridge_lines/20250207/conus_osm_bridges.gpkg
    #  -o /data/inputs/osm/bridges/lidar_data/20250323/conus_osm_lidar_rasters/
    #  -jl 30
    #  -jr 30
    #  -b 1.5
    #  -r 3

    # This needs to be run twice, once for AK and once for CONUS.

    ###############################
    #
    # If new OSM bridge data is pulled, this tool needs to be run to pull new bridge lidar data.
    #
    # With running this tool, you will also need to run 'make_dem_dif_for_briges.py`
    #
    ###############################

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

    parser.add_argument(
        '-jl',
        '--job_number_lidar',
        help='OPTIONAL: Number of (jobs) cores/processes for downloading lidar data. Default=15. ',
        required=False,
        default=15,
        type=int,
    )

    parser.add_argument(
        '-jr',
        '--job_number_raster',
        help='OPTIONAL: Number of (jobs) cores/processes for making osm rasters from lidar data. Default=10.  ',
        required=False,
        default=10,
        type=int,
    )

    parser.add_argument(
        '--keep_las_files',
        help='OPTIONAL: If included, downloaded LAS files will be kept after reading.',
        required=False,
        action='store_false',
        dest='remove_las_files',
    )
    parser.set_defaults(remove_las_files=True)

    args = vars(parser.parse_args())
    args['cli_args'] = shlex.join(sys.argv)

    process_bridges_lidar_data(**args)
