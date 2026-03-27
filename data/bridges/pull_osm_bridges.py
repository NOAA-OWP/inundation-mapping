import argparse
import datetime as dt
import glob
import os
import shlex
import sys
import traceback
import warnings
from pathlib import Path

import geopandas as gpd
import osmnx as ox
import pandas as pd
import pyproj
import requests
from dotenv import load_dotenv
from networkx import Graph, connected_components
from osmnx._errors import InsufficientResponseError
from shapely.geometry import LineString, shape

from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


# Set the timeout to 500 seconds (5 minutes), which is possible depending on network speed
# and network volume (number of jobs)
ox.settings.request_timeout = 500


# ox.settings.requests_timeout = 1200  # Set timeout to 20 minutes
srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
GUAM_CRS = os.getenv('GUAM_CRS')
AMERICAN_SAMOA_CRS = os.getenv('AMERICAN_SAMOA_CRS')

# TODO: HUC 02060006 remains a known special case. Prior attempts, including splitting to HUC10s,
# have not produced usable bridge output through the standard osmnx.features_from_polygon(...) workflow.
# Revisit with an alternative acquisition method.


# Dissolve touching lines
def find_touching_groups(gdf, file_logger, screen_queue, task_id):
    # Create a graph
    graph = Graph()

    # Add nodes for each geometry
    graph.add_nodes_from(gdf.index)

    # Create spatial index for efficient querying
    spatial_index = gdf.sindex

    # For each geometry, find touching geometries and add edges to the graph
    for idx, geometry in gdf.iterrows():
        possible_matches_index = list(spatial_index.intersection(geometry['geometry'].bounds))
        possible_matches = gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(geometry['geometry'])]

        for match_idx in precise_matches.index:
            if match_idx != idx:
                graph.add_edge(idx, match_idx)

    # Find connected components
    groups = list(connected_components(graph))
    return groups


def pull_osm_features_by_huc(huc_bridge_file, huc_num, huc_geom, file_logger, screen_queue, task_id):
    try:

        if os.path.exists(huc_bridge_file):
            # remove it
            os.remove(huc_bridge_file)

        file_logger.info(f" ** Creating gpkg for {huc_num}")
        screen_queue.put(f" ** Creating gpkg for {huc_num}")

        try:
            gdf = ox.features_from_polygon(shape(huc_geom), {"bridge": True})
        except InsufficientResponseError:
            file_logger.warning(f"osmnx pull for {huc_num} came back with no matching bridge features")
            screen_queue.put(f"osmnx pull for {huc_num} came back with no matching bridge features")
            return 1, [True]
        except requests.exceptions.ReadTimeout:
            file_logger.warning(f"osmnx pull for {huc_num} timed out while waiting on Overpass")
            screen_queue.put(f"osmnx pull for {huc_num} timed out while waiting on Overpass")
            return 1, [True]

        if gdf is None or len(gdf) == 0:
            file_logger.warning(f"osmnx pull for {huc_num} came back with no records")
            screen_queue.put(f"osmnx pull for {huc_num} came back with no records")
            return 1, [True]

        # OSMnx returns a MultiIndex for feature downloads. For this bridge pull, the index levels are
        # typically "element" and "id", where "id" is the original OSM feature identifier.
        # Drop the "element" level and copy the remaining index into an explicit "osmid" column so the
        # source OSM ID is preserved after index resets and when writing to file.
        gdf = gdf.droplevel('element')

        # Preserve the OSM feature ID as a regular column instead of relying on the GeoDataFrame index.
        gdf["osmid"] = gdf.index

        # we will always have a huc8 value but may not have a huc10, depending on what was submitted
        gdf['huc10'] = ""
        if len(huc_num) == 8:
            gdf['huc8'] = huc_num
        if len(huc_num) == 10:
            gdf['huc8'] = huc_num[:8]
            gdf['huc10'] = huc_num

        # Create bridge_type column
        # Check if 'highway' column exists
        if 'highway' not in gdf.columns:
            gdf['highway'] = None

        # Check if 'railway' column exists
        if 'railway' not in gdf.columns:
            gdf['railway'] = None

        # Create the bridge_type column by combining above information
        gdf['bridge_type'] = gdf.apply(
            lambda row: (
                f"highway-{row['highway']}" if pd.notna(row['highway']) else f"railway-{row['railway']}"
            ),
            axis=1,
        )
        gdf.reset_index(inplace=True)

        # Remove abandoned bridges

        unwanted_bridge_types = [
            'highway-razed',
            'highway-proposed',
            'highway-abandoned',
            'highway-destroyed',
            'highway-dismantled',
            'highway-demolished',
            'railway-razed',
            'railway-proposed',
            'railway-abandoned',
            'railway-destroyed',
            'railway-dismantled',
            'railway-demolished',
        ]

        gdf = gdf[~gdf['bridge_type'].isin(unwanted_bridge_types)]

        cols_to_drop = []
        for col in gdf.columns:
            if any(isinstance(val, list) for val in gdf[col]):
                cols_to_drop.append(col)

        # This a common and know duplicate column name (and others) (yes.. id can be a dup).
        # Each returning dataset from OSM can and almost always does have different schemas - crazy
        bad_column_names = [
            "id",
            "fid",
            "ID",
            "fixme",
            "FIXME",
            "NYSDOT_ref",
            "REF",
            "fixme:maxspeed",
            "LAYER",
            "OBJECTID",
            "objectid",
            "unsigned_ref",
            "Fut_Ref",
            "Ref",
            "FIXME:ref",
        ]
        for bad_cn in bad_column_names:
            if bad_cn in gdf.columns:
                cols_to_drop.append(bad_cn)

        # OSM "fixme*" tags are editorial metadata and can arrive with mixed casing
        # (for example, "fixme:bicycle" and "FIXME:bicycle"), which can cause
        # duplicate-column failures when writing to GeoPackage/SQLite.
        for col in gdf.columns:
            if str(col).lower().startswith("fixme"):
                cols_to_drop.append(col)

        if len(cols_to_drop) > 0:
            gdf = gdf.drop(columns=list(set(cols_to_drop)), axis=1)

        # Keep only linear bridge geometries and rebuild as a GeoDataFrame so geometry ops remain available.
        linear_geom_types = {'LineString', 'MultiLineString'}
        gdf1 = gpd.GeoDataFrame(
            gdf.loc[gdf.geometry.geom_type.isin(linear_geom_types)].copy(), geometry='geometry', crs=gdf.crs
        )

        if gdf1.empty:
            file_logger.warning(
                f"osmnx pull for {huc_num} returned no linear bridge geometries after filtering"
            )
            screen_queue.put(f"osmnx pull for {huc_num} returned no linear bridge geometries after filtering")
            return 1, [True]

        if str(huc_num).startswith('19'):
            gdf1 = gdf1.to_crs(ALASKA_CRS)
        elif str(huc_num) == '22010000':
            gdf1 = gdf1.to_crs(GUAM_CRS)
        elif str(huc_num) == '22030001':
            gdf1 = gdf1.to_crs(AMERICAN_SAMOA_CRS)
        else:
            gdf1 = gdf1.to_crs(DEFAULT_FIM_PROJECTION_CRS)

        # Perform dissolve touching lines
        buffered = gdf1.copy()
        buffered['geometry'] = buffered['geometry'].buffer(0.0001)
        # Find groups of touching geometries
        touching_groups = find_touching_groups(buffered, file_logger, screen_queue, task_id)

        # Dissolve each group separately
        warnings.filterwarnings('ignore')
        dissolved_groups = []
        for group in touching_groups:
            group_gdf = buffered.loc[list(group)]
            if not group_gdf.empty:
                dissolved_group = group_gdf.dissolve()
                single_part_group = dissolved_group.explode(index_parts=False)
                dissolved_groups.append(single_part_group)

        # Combine dissolved groups and reconstruct GeoDataFrame
        if dissolved_groups:
            dissolved_gdf = pd.concat(dissolved_groups, ignore_index=True)
            final_gdf = gpd.GeoDataFrame(dissolved_gdf, crs=buffered.crs)
        else:
            final_gdf = buffered.copy()

        # Polygon to linestring
        final_gdf['geometry'] = final_gdf['geometry'].apply(
            lambda geom: LineString(geom.exterior.coords) if geom.geom_type == 'Polygon' else geom
        )
        # Reconstruct the GeoDataFrame to remove fragmentation
        final_gdf = final_gdf.copy()

        final_gdf.to_file(huc_bridge_file, driver="GPKG", index=True, engine='fiona')

        return 1, [True]

    except Exception:
        screen_queue.put("---------------")
        file_logger.critical(f"**** ERROR: Couldn't write {huc_num}")
        screen_queue.put(f"**** ERROR: Couldn't write {huc_num}")
        file_logger.critical(traceback.format_exc())

        try:
            # rename and we can filter it out later. Even it fails sometimes
            if os.path.exists(huc_bridge_file):
                # change it's file name have "_bad" added before the extension
                new_name = huc_bridge_file.replace(".gpkg", "_bad.gpkg")
                os.rename(huc_bridge_file, new_name)
        except Exception:
            screen_queue.put("---------------")
            file_logger.critical(
                f"Unable to delete {huc_bridge_file} for huc {huc_num} to add '_bad' in file name"
            )
            screen_queue.put(
                f"Unable to delete {huc_bridge_file} for huc {huc_num} to add '_bad' in file name"
            )

        return 0, [False]


#
# Combine all HUC-based OSM bridges from specified folder
#
def combine_huc_features(output_dir, file_logger):

    # only save out a subset of columns, because many hucs have different column names
    # and data, so you could end up with thousands of columns if you keep them all!

    # Note... files in error have been renamed to {xxxx}_bad.gpkg and will be skipped. To debug later

    # It is ok that we have dup osmid's at this as each point as one bridge can cross a huc boundary
    # so each huc can add the same osmid. When it gets to the final... it will be clipped and both
    # halves of the same bridge (in each huc) will have the same osmid. So.. don't change.

    # huc8 will always have the huc8 value but huc10 might be empty
    file_logger.info("Combining intermediate files...")
    print("Combining intermediate files...")
    cols_to_keep = ['osmid', 'name', 'bridge_type', 'huc8', 'huc10', 'geometry']

    # Bucket files by region
    buckets = {"alaska": [], "guam": [], "samoa": [], "conus": []}
    for f in Path(output_dir).glob("huc_*_osm_bridges.gpkg"):
        n = f.name
        if n.startswith("huc_19"):
            buckets["alaska"].append(f)
        elif n.startswith("huc_22010000"):
            buckets["guam"].append(f)
        elif n.startswith("huc_22030001"):
            buckets["samoa"].append(f)
        else:
            buckets["conus"].append(f)

    # Process each bucket
    for region, flist in buckets.items():
        if not flist:
            continue
        gdf = pd.concat((gpd.read_file(f) for f in flist), ignore_index=True)[cols_to_keep]
        if "osmid" in gdf.columns:
            gdf["osmid"] = gdf["osmid"].astype(str)
        out_path = os.path.join(output_dir, f"{region}_osm_bridges.gpkg")
        file_logger.info(f"Writing {region.title()} bridge lines: {out_path}")
        print(f"Writing {region.title()} bridge lines: {out_path}")
        gdf.to_file(out_path, driver="GPKG", engine="fiona")


def make_logger(output_folder):
    start_time = dt.datetime.now(dt.timezone.utc)

    file_dt_string = start_time.strftime("%y%m%d-%H%M")

    script_file_name = os.path.basename(__file__).split('.')[0]
    file_name = f"{script_file_name}-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder, file_name)

    os.makedirs(output_folder, exist_ok=True)

    file_logger = setup_mp_file_logger(log_file_path, logger_name="pull_osm_bridges")
    return file_logger


def single_huc_job(huc_num, huc_boundary_path, output_folder, file_logger, screen_queue, task_id):
    file_logger.info(f"started the process for {task_id}")

    try:
        huc_gpd = gpd.read_file(huc_boundary_path)
        huc_gpd_projected = huc_gpd.to_crs(pyproj.CRS.from_string("EPSG:4326"))
        huc_geom = huc_gpd_projected.iloc[0]["geometry"]
        huc_bridge_file = os.path.join(output_folder, f"huc_{huc_num}_osm_bridges.gpkg")
        return pull_osm_features_by_huc(
            huc_bridge_file, huc_num, huc_geom, file_logger, screen_queue, task_id
        )

    except Exception:
        screen_queue.put("---------------")
        file_logger.critical(f"**** ERROR: Couldn't prepare {huc_num}")
        screen_queue.put(f"**** ERROR: Couldn't prepare {huc_num}")
        file_logger.critical(traceback.format_exc())
        return 0, [False]


def process_osm_bridges(preclip_dir, output_folder, number_of_jobs, lst_hucs, file_logger, cli_args=None):
    start_time = dt.datetime.now(dt.timezone.utc)

    print("==================================")
    file_logger.info("Starting load of OSM bridge data")
    if cli_args:
        file_logger.info(f"CLI invocation: {cli_args}")
    print("Starting load of OSM bridge data")
    file_logger.info(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    print(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")

    if not os.path.exists(preclip_dir):
        raise ValueError("preclip directory not found")

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
    else:
        # remove the intermediates from past run as it can interfere with next runs as the tools combines
        # all files in that directory starting with huc_*_osm_bridges.gpkg
        file_logger.info(
            ".. Removing intermediate files from previous runs, if any,"
            " as it can interfere with future runs."
        )
        for file in Path(output_folder).glob("huc_*_osm_bridges.gpkg"):
            os.remove(file)

    # Build the HUC domain from preclip folders, matching pull_osm_roads.py.
    huc_numbers = [
        str(huc)
        for huc in os.listdir(preclip_dir)
        if os.path.isdir(os.path.join(preclip_dir, huc)) and len(huc) == 8
    ]

    if lst_hucs != '':
        lst_hucs = lst_hucs.strip()
        selected_hucs = lst_hucs.split(" ")
        huc_numbers = [huc for huc in huc_numbers if huc in selected_hucs]

    huc_numbers.sort()

    file_logger.info(f"Number of hucs to process {len(huc_numbers)}")
    print(f"Number of hucs to process {len(huc_numbers)}")

    tasks_args_list = []
    for huc_num in huc_numbers:
        tasks_args_list.append(
            {
                "huc_num": huc_num,
                "huc_boundary_path": os.path.join(preclip_dir, huc_num, "wbd.gpkg"),
                "output_folder": output_folder,
            }
        )

    # Run multiprocessing
    mp_results = run_with_mp(
        task_function=single_huc_job,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=number_of_jobs,
        task_id_key="huc_num",  # to label logs by HUC ID
        show_progress=True,
    )

    # report a summary of MP tasks status
    failed_tasks = [tid for tid, payload in mp_results.items() if not payload[0]]

    if not failed_tasks:
        file_logger.info("✅ All multiprocessing tasks Succeeded")
        print("✅ All multiprocessing tasks Succeeded")
    else:
        file_logger.info(f"❌ {len(failed_tasks)} failed:")
        print(f"❌ {len(failed_tasks)} failed:")
        for tid in failed_tasks:
            file_logger.info(f"  - {tid}")
            print(f"  - {tid}")

    file_logger.info("")
    section_time = dt.datetime.now(dt.timezone.utc)
    file_logger.info(f"Combining huc feature files started: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")
    print(f"Combining huc feature files started: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")

    # all huc8 processing must be completed before this function call
    combine_huc_features(output_folder, file_logger)

    # Get time metrics
    end_time = dt.datetime.now(dt.timezone.utc)
    file_logger.info("")
    file_logger.info("==================================")
    file_logger.info("OSM bridge data load complete")
    print("OSM bridge data load complete")
    file_logger.info(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")

    time_delta = end_time - start_time
    total_seconds = int(time_delta.total_seconds())

    ___, rem_seconds = divmod(total_seconds, 60 * 60 * 24)
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

    file_logger.info(f"Duration: {time_fmt}")
    print(f"Duration: {time_fmt}")


if __name__ == "__main__":

    '''

    Sample usage:

        (For )
        python3 /foss_fim/data/bridges/pull_osm_bridges.py
            -p /data/inputs/pre_clip_huc8/20250218
            -o /data/inputs/osm/bridges/bridge_lines/20250207/
            -j 10
            -lh '01010002 12090301'

        ** The -lh flg is an optional list of HUC8 if you want to process just those hucs
           if you want all HUC8s in the preclip directory, leave this arg off


    Code Usage
    This tool follows the same HUC discovery method used by pull_osm_roads.py.
    It reads HUC folders from the preclip directory, so one run can process all regions
    present in that folder layout, including CONUS, Alaska, Guam, and American Samoa.

        - One HUC8 was too big for osmnx to handle adn kept timing out. So, we split that HUC8
          to HUC10's and feed it through this tool again.
          02060000  -- WBD_National_HUC10_for_osm_pull_02060006.gpkg

        - This tool is meant to pull down all the Open Street Map bridge data for CONUS as a
        precursor to the bridge healing pre-processing (so, a pre-pre-processing step).
        It should be run only as often as the user thinks OSM has had any important updates.
        - As written, the code will skip any HUC that there's already a file for if you save
          an existing folder.

        - Each HUC8/10's worth of OSM bridge features is saved out individually, then merged together
        into one.

    '''

    ###############################
    #
    # When you run, this tool, a few additional tools will need to be run.
    #
    # First run, `make_rasters_using_lidar.py` (get new bridge lidar). Then you will need to run
    #    `make_dem_dif_for_bridges.py`.
    #
    # Independently, with pulling new osm data, we need to run new pre-clips. pre-clips do not
    #    need new lidar or dem diffs.
    #
    ###############################

    parser = argparse.ArgumentParser(description='Acquires and saves Open Street Map bridge features')

    parser.add_argument(
        '-p',
        '--preclip_dir',
        help='REQUIRED: folder path where the preclip HUC folders are located.',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output-folder',
        help='REQUIRED: folder path location where individual HUC8 geopackages'
        ' will be saved to after being downloaded from OSM.'
        ' File names are hardcoded to format hucxxxxxxxx_osm_bridges.gpkg,'
        ' with xxxxxxxx as the HUC8 value',
        required=True,
    )

    parser.add_argument(
        '-j',
        '--number-of-jobs',
        help='OPTIONAL: Number of (jobs) cores/processes to used.',
        required=False,
        default=4,
        type=int,
    )

    parser.add_argument(
        '-lh',
        '--lst-hucs',
        help='OPTIONAL: Space-delimited list of HUCs to which can be used to filter osm bridge processing.'
        ' Defaults to all HUC8s in the preclip directory.',
        required=False,
        default='',
    )

    args = vars(parser.parse_args())
    args["cli_args"] = shlex.join(sys.argv)

    file_logger = make_logger(args["output_folder"])

    try:
        process_osm_bridges(**args, file_logger=file_logger)

    except Exception:
        file_logger.critical(traceback.format_exc())
        end_time = dt.datetime.now(dt.timezone.utc)
        file_logger.critical(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
