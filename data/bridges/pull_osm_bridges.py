import argparse
import datetime as dt
import logging
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import osmnx as ox
import pandas as pd
import pyproj
from shapely import LineString
from shapely.geometry import shape


CRS = "epsg:5070"


# Save all OSM bridge features by HUC8 to a specified folder location.
# Bridges will have point geometry converted to linestrings if needed
#
def pull_osm_features_by_huc(huc_bridge_file, huc_num, huc_geom):
    """
    Returns: The huc number but only if it failed, so we can make a master list of failed HUCs.
      The errors will be logged as it goes.
    """
    tags = {"bridge": True}
    try:
        if os.path.exists(huc_bridge_file):
            logging.info(f" **{huc_bridge_file} already exists.. skipped")
            # return huc_bridge_file
            return ""
        logging.info(f" ** Saving off {huc_num}")

        gdf = ox.features_from_polygon(shape(huc_geom), tags)

        if gdf is None or len(gdf) == 0:
            logging.info(f"osmnx pull for {huc_num} came back with no records")
            return huc_num

        cols_to_drop = []
        for col in gdf.columns:
            if any(isinstance(val, list) for val in gdf[col]):
                cols_to_drop.append(col)

        # This a common and know duplicate column name (and others)
        bad_column_names = ["atv", "fixme", "FIXME"]
        for bad_cn in bad_column_names:
            if bad_cn in gdf.columns:
                cols_to_drop.append(bad_cn)

        if len(cols_to_drop) > 0:
            gdf = gdf.drop(columns=cols_to_drop, axis=1)

        # drops records with poly or multipolygons.
        gdf = gdf[gdf.geom_type != 'Polygon']
        gdf = gdf[gdf.geom_type != 'MultiPolygon']

        # fix geometry for saving if there are any Points
        for i, row in gdf.iterrows():
            if 'POINT' in str(row[1]):
                new_geom = LineString([row[1], row[1]])
                gdf.at[i, 'geometry'] = new_geom

        gdf = gdf.to_crs(CRS)
        gdf.to_file(huc_bridge_file, driver="GPKG")

        # returns the HUC but only if it failed so we can keep a list of failed HUCs
        return ""

    except Exception:
        logging.error(f"\t--- Couldn't write {huc_num}")
        logging.error(traceback.format_exc())

        try:
            # rename and we can filter it out later. Even it fails sometimes
            if os.path.exists(huc_bridge_file):
                os.rename(huc_bridge_file, f"bad_{huc_bridge_file}")
        except Exception as ex:
            print(f"Unable to rename {huc_bridge_file} for huc {huc_num} to add 'bad_' in front")
            print(ex)

        return huc_num
        # Continue on


#
# Combine all HUC-based OSM bridges from specified folder
#
def combine_huc_features(output_dir):

    all_bridges_gdf = gpd.GeoDataFrame()

    # Should skip file starting with "bad_"
    bridge_file_names = Path(output_dir).glob("huc_*_osm_bridges.gpkg")

    osm_bridge_file = os.path.join(output_dir, "osm_all_bridges.gpkg")
    osm_bridge_midpoints_file = os.path.join(output_dir, "osm_all_bridges_midpoints.gpkg")

    is_first_valid_gdf_loaded = False

    # Lots of the geopackages are invalid. We try to catch them above, but we neeed this as a safety
    for i, bridge_file in enumerate(bridge_file_names):
        logging.info(f"merging bridge {i}:{os.path.basename(bridge_file)}")
        try:
            bridge_gdf = gpd.read_file(bridge_file)
            if is_first_valid_gdf_loaded is False:
                all_bridges_gdf = bridge_gdf
                is_first_valid_gdf_loaded = True
            else:
                # Not fond of copying directly back to a gpkg beign merged
                temp_gdf = pd.concat(all_bridges_gdf, bridge_gdf)
                all_bridges_gdf = temp_gdf

        except Exception:
            logging.error(f"Error: bridge file of {bridge_file} failed to merge")
            logging.error(traceback.format_exc())

    # To many gpkgs had errors and were blowing up on loading it as one large concat
    # gdf = pd.concat([gpd.read_file(shp) for shp in bridge_file_names], ignore_index=True)

    if len(all_bridges_gdf) == 0:
        logging.error("The merged roll up of bridge data has no records.")
        return

    # only save out a subset of columns, because many hucs have different column names
    # and data, so you could end up with thousands of columns if you keep them all!
    gdf = all_bridges_gdf.to_crs(CRS)
    gdf[['osmid', 'geometry']].to_file(osm_bridge_file, driver="GPKG")

    # save out midpoints (centroids) of bridge lines
    gdf['geometry'] = gdf.centroid
    gdf[['osmid', 'geometry']].to_file(osm_bridge_midpoints_file, driver="GPKG")

    return


def process_osm_bridges(wbd_file, output_folder, number_of_jobs):
    start_time = dt.datetime.now(dt.timezone.utc)
    __setup_logger(output_folder)

    print("==================================")
    logging.info("Starting load of OSM bridge data")
    logging.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    logging.info("")

    # --------------------------
    # Validation
    if os.path.exists(wbd_file) is False:
        raise Exception(f"The wbd file of {wbd_file} does not exist")

    split_wbd_file_name = os.path.splitext(wbd_file)
    if len(split_wbd_file_name) != 2:
        raise Exception(f"The wbd file of {wbd_file} does not appear to valid file name")

    if str(split_wbd_file_name[1]).lower() != ".gpkg":
        raise Exception(f"The wbd file of {wbd_file} does not appear to valid a gpkg")

    # -------------------
    # Validation
    total_cpus_available = os.cpu_count() - 2
    if number_of_jobs > total_cpus_available:
        raise ValueError(
            f'The number of jobs provided: {number_of_jobs} ,'
            ' exceeds your machine\'s available CPU count minus two.'
            ' Please lower the number of jobs value accordingly.'
        )

    # --------------------------
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    logging.info("*** Reading in and reprojecting the WBD HUC8 file")
    logging.info("*** Depending on WBD size, this can take a bit. 5 to 40 mins is not uncommon.")
    huc8s = gpd.read_file(wbd_file, layer="WBDHU8")

    if len(huc8s) == 0:
        raise Exception("wbd_file has no records")

    logging.info(f"wbd rec count is {len(huc8s)}")
    section_time = dt.datetime.now(dt.timezone.utc)
    logging.info(f"WBD Loaded: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")

    # osm seems to like 4326
    logging.info("")
    logging.info("Reprojecting to 4326 (osm seems to like that one)")
    huc8s = huc8s.to_crs(pyproj.CRS.from_string("epsg:4326"))
    section_time = dt.datetime.now(dt.timezone.utc)
    logging.info(f"Reprojection done: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")

    failed_HUCs_list = []
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        futures = {}
        for row in huc8s.iterrows():
            huc = row[1]
            huc_bridge_file = os.path.join(output_folder, f"huc_{huc['HUC8']}_osm_bridges.gpkg")
            args = {"huc_num": huc['HUC8'], "huc_bridge_file": huc_bridge_file, "huc_geom": huc['geometry']}
            future = executor.submit(pull_osm_features_by_huc, **args)
            futures[future] = future

        for future in as_completed(futures):
            if future is not None:
                if not future.exception():
                    failed_huc = future.result()
                    if failed_huc != "":
                        failed_HUCs_list.append(failed_huc)
                else:
                    raise future.exception()

    logging.info("")
    logging.info("Pulling hucs now complete")

    logging.info("")

    logging.info("Beginning combining features")
    # all huc8 processing must be completed before this function call
    combine_huc_features(output_folder)

    if len(failed_HUCs_list) > 0:
        logging.info("\n+++++++++++++++++++")
        logging.info("HUCs that failed to download from OSM correctly are:")
        for huc in failed_HUCs_list:
            logging.info(f" --- {huc}")
        logging.info("  See logs for more details on each HUC fail")
        logging.info("+++++++++++++++++++")

    # Get time metrics
    end_time = dt.datetime.now(dt.timezone.utc)
    logging.info("")
    logging.info("==================================")
    logging.info("OSM bridge data load complete")
    logging.info(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")

    time_delta = end_time - start_time
    total_seconds = int(time_delta.total_seconds())

    ___, rem_seconds = divmod(total_seconds, 60 * 60 * 24)
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

    logging.info(f"Duration: {time_fmt}")
    print()
    return


def __setup_logger(outputs_dir):
    '''
    Set up logging to file. Since log file includes the date, it will be overwritten if this
    script is run more than once on the same day.
    '''
    start_time = dt.datetime.now(dt.timezone.utc)
    file_dt_string = start_time.strftime("%y%m%d-%H%M")

    script_file_name = os.path.basename(__file__).split('.')[0]
    file_name = f"{script_file_name}-{file_dt_string}.log"

    log_file_path = os.path.join(outputs_dir, file_name)

    if not os.path.exists(outputs_dir):
        os.mkdir(outputs_dir)

    # set up logging to file
    logging.basicConfig(
        filename=log_file_path, level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S'
    )

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    # logger = logging.getLogger(__name__)


if __name__ == "__main__":
    '''
    Sample usage (min params):
        python3 data/bridges/pull_osm_bridges.py
            -w /data/inputs/wbd/WBD_National_HUC8.gpkg
            -p /data/inputs/osm/bridges/
            -j 6
    Notes:
        - This tool is meant to pull down all the Open Street Map bridge data for CONUS as a
        precursor to the bridge healing pre-processing (so, a pre-pre-processing step).
        It should be run only as often as the user thinks OSM has had any important updates.
        - As written, the code will skip any HUC that there's already a file for.
        - Each HUC8's worth of OSM bridge features is saved out individually, then merged together
        into one. The HUC8 files can be deleted if desired, as an added final cleanup step.
    '''

    parser = argparse.ArgumentParser(description='Acquires and saves Open Street Map bridge features')

    parser.add_argument(
        '-w',
        '--wbd_file',
        help='REQUIRED: location the gpkg file that will'
        ' contain all the HUC8 clip regions in one layer. Must contain field \'HUC8\'.',
        required=True,
    )

    parser.add_argument(
        '-p',
        '--output_folder',
        help='REQUIRED: folder path location where individual HUC8 geopackages'
        ' will be saved to after being downloaded from OSM.'
        ' File names are hardcoded to format hucxxxxxxxx_osm_bridges.gpkg,'
        ' with xxxxxxxx as the HUC8 value',
        required=True,
    )

    parser.add_argument(
        '-j',
        '--number_of_jobs',
        help='OPTIONAL: Number of (jobs) cores/processes to used.',
        required=False,
        default=1,
        type=int,
    )

    args = vars(parser.parse_args())

    try:
        process_osm_bridges(**args)

    except Exception:
        logging.error(traceback.format_exc())
        end_time = dt.datetime.now(dt.timezone.utc)
        logging.info(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
