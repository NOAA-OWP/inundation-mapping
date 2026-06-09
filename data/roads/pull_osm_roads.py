"""
Download road segments from OpenStreetMap (OSM) for FIM analysis.

This script queries the Overpass API to download major road segments (motorway, trunk,
primary, secondary, tertiary) for each HUC boundary. Road segments are then split by
NWM catchment boundaries for use in flood impact (FIMpact) calculations.

Important: Bridge segments (tagged with bridge=*) are explicitly EXCLUDED from the road
downloads to prevent unrealistic flood depth calculations. Bridge segments are handled
separately via pull_osm_bridges.py and the bridge-healing workflow.
"""

import argparse
import http.client
import os
import random
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import overpy
import pandas as pd
import pyproj
from dotenv import load_dotenv
from overpy.exception import OverpassGatewayTimeout, OverpassTooManyRequests
from shapely.geometry import LineString

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
GUAM_CRS = os.getenv('GUAM_CRS')
AMERICAN_SAMOA_CRS = os.getenv('AMERICAN_SAMOA_CRS')


#TODO field "huc8" of output of fimpact tool misses leading zeros. 


def report_road_download_status(huc_numbers, output_dir):
    files = list(Path(output_dir).glob("roads_*.gpkg"))
    pattern = re.compile(r"roads_(\d{8})\.gpkg")

    downloaded_hucs = [pattern.match(f.name).group(1) for f in files if pattern.match(f.name)]

    missing_hucs = sorted(set(huc_numbers) - set(downloaded_hucs))

    if missing_hucs:
        print("❌ Road data was not downloaded for the following HUCs:")
        for huc in missing_hucs:
            print(huc)
    else:
        print("✅ All requested HUCs have their road data downloaded.")


def combine_hucs(output_dir):
    # identify Alaska vs CONUS vs Guam vs Samoa HUCs
    files = list(Path(output_dir).glob("roads_*.gpkg"))

    regions = {
        "alaska": [f for f in files if f.name.startswith("roads_19")],
        "guam": [f for f in files if f.name.startswith("roads_22010000")],
        "samoa": [f for f in files if f.name.startswith("roads_22030001")],
        "conus": [
            f
            for f in files
            if not (
                f.name.startswith("roads_19")
                or f.name.startswith("roads_22010000")
                or f.name.startswith("roads_22030001")
            )
        ],
    }

    for region, flist in regions.items():
        if not flist:
            continue
        gdf = pd.concat([gpd.read_file(f) for f in flist], ignore_index=True)
        gdf["osmid"] = gdf["osmid"].astype(str)
        gdf.to_file(os.path.join(output_dir, f"{region}_osm_roads.gpkg"), driver="GPKG", engine="fiona")
        print(f"Compiled {region.title()} road lines!")
        [f.unlink() for f in flist]


def split_bbox(minx, miny, maxx, maxy, num_splits=4):
    """
    Split a bounding box into num_splits  by num_splits smaller boxes.

    Args:
        minx, miny, maxx, maxy: Coordinates of the original bounding box.
        num_splits (int): Number of divisions along each axis (e.g., 4 → 4x4 grid = 16 boxes)

    Returns:
        List of (sub_minx, sub_miny, sub_maxx, sub_maxy) tuples for each tile.
    """
    x_step = (maxx - minx) / num_splits
    y_step = (maxy - miny) / num_splits
    boxes = []

    for i in range(num_splits):  # Horizontal divisions
        for j in range(num_splits):  # Vertical divisions
            sub_minx = minx + i * x_step
            sub_maxx = sub_minx + x_step
            sub_miny = miny + j * y_step
            sub_maxy = sub_miny + y_step
            boxes.append((sub_minx, sub_miny, sub_maxx, sub_maxy))

    return boxes


def pull_roads(HUC_no, huc_geom, file_logger, screen_queue, task_id):
    """
    Pull road segments from OpenStreetMap for a given HUC boundary.

    Note: Bridge segments (tagged with bridge=*) are explicitly excluded to prevent
    unrealistic flood depth calculations in FIMpact analyses. Bridges are handled
    separately via pull_osm_bridges.py and the bridge-healing workflow.
    """
    road_data = pd.DataFrame()

    minx, miny, maxx, maxy = huc_geom.bounds
    bbox_query = f"({miny},{minx},{maxy},{maxx})"

    # Exclude bridge segments to prevent unrealistic flood depth calculations
    # Bridge segments are pulled separately via pull_osm_bridges.py and handled differently
    #
    # Residential roads are included via a second query statement, but limited to named
    # streets only. Unnamed residential roads (cul-de-sacs, dead-ends, private drives) are
    # too numerous and rarely relevant at the HUC scale; named ones are the actual
    # through-streets worth including in FIMpact. "unclassified" is also added: despite the
    # name it is NOT unknown — in OSM it sits one step above residential and covers minor
    # public connector roads between settlements too small to be tertiary. Overpass
    # deduplicates the union automatically so no road appears twice.
    query_template = """
    [out:json];
    (
    way["highway"~"^motorway$|^trunk$|^primary$|^secondary$|^tertiary$|^unclassified$"][!"bridge"]{bbox};
    way["highway"="residential"]["name"][!"bridge"]{bbox};
    );
    out body;
    >;
    out skel qt;
    """

    api = overpy.Overpass()
    max_attempts = 5
    result = None
    incomplete_read_occurred = False

    for attempt in range(1, max_attempts + 1):
        try:
            result = api.query(query_template.format(bbox=bbox_query))
            break  # success
        except (
            overpy.exception.OverpassTooManyRequests,
            overpy.exception.OverpassGatewayTimeout,
            overpy.exception.OverpassUnknownHTTPStatusCode,
        ) as e:
            wait_time = 5 * attempt + random.uniform(0, 2)
            screen_queue.put(f"[{task_id}] Too many requests, retrying in {wait_time}s (attempt {attempt})")
            file_logger.warning(
                f"[{task_id}] Server busy ({e.__class__.__name__}) — retrying in {wait_time:.1f}s (attempt {attempt})"
            )
            time.sleep(wait_time)
        except (http.client.IncompleteRead, overpy.exception.OverpassRuntimeError) as e:
            incomplete_read_occurred = True
            screen_queue.put(f"[{task_id}] IncompleteRead error — retrying: {str(e)}")
            file_logger.warning(f"[{task_id}] IncompleteRead error — retrying: {str(e)}")
            if (
                attempt >= 2
            ):  # no need to retry more than twice , because it is most likely due to large size. directly go to split part
                file_logger.warning(
                    f"[{task_id}] Stopping retries due to repeated IncompleteRead or OverpassRuntimeError "
                )
                break
        except Exception as e:
            file_logger.error(f"[{task_id}] Unexpected error during Overpass query: {str(e)}")
            raise

    if result:
        road_data = pd.DataFrame(
            [
                {
                    "osmid": way.id,
                    "geometry": LineString([(float(n.lon), float(n.lat)) for n in way.nodes]),
                    "highway": way.tags.get("highway", "unknown"),
                    "name": way.tags.get("name", "unnamed"),
                }
                for way in result.ways
                if len(way.nodes) > 1
            ]
        )

    # If we exhausted attempts and last exception was IncompleteRead → split and try again
    if result is None and incomplete_read_occurred:
        file_logger.warning(
            f"[{task_id}] Splitting HUC {HUC_no} into sub-regions due to repeated IncompleteRead errors."
        )
        screen_queue.put(
            f"[{task_id}] Splitting HUC {HUC_no} into sub-regions due to repeated IncompleteRead errors."
        )
        num_splits = 4
        sub_boxes = split_bbox(minx, miny, maxx, maxy, num_splits)
        all_dfs = []
        for idx, (sub_minx, sub_miny, sub_maxx, sub_maxy) in enumerate(sub_boxes):
            sub_bbox = f"({sub_miny},{sub_minx},{sub_maxy},{sub_maxx})"
            sub_query = query_template.format(bbox=sub_bbox)
            for attempt in range(1, max_attempts + 1):
                try:
                    sub_result = api.query(sub_query)
                    df = pd.DataFrame(
                        [
                            {
                                "osmid": way.id,
                                "geometry": LineString([(float(n.lon), float(n.lat)) for n in way.nodes]),
                                "highway": way.tags.get("highway", "unknown"),
                                "name": way.tags.get("name", "unnamed"),
                            }
                            for way in sub_result.ways
                            if len(way.nodes) > 1
                        ]
                    )
                    all_dfs.append(df)
                    break
                except Exception as e:
                    file_logger.warning(f"[{task_id}] Sub-query {idx+1}/10 failed on attempt {attempt}: {e}")
                    time.sleep(3)
        if len(all_dfs) == num_splits**2:
            road_data = pd.concat(all_dfs, ignore_index=True)

    if not road_data.empty:
        gdf_roads = gpd.GeoDataFrame(road_data, crs="EPSG:4326")
        gdf_roads["huc8"] = HUC_no

        if str(HUC_no).startswith('19'):
            gdf_roads = gdf_roads.to_crs(ALASKA_CRS)

        elif str(HUC_no) == '22010000':
            gdf_roads = gdf_roads.to_crs(GUAM_CRS)

        elif str(HUC_no) == '22030001':
            gdf_roads = gdf_roads.to_crs(AMERICAN_SAMOA_CRS)

        else:
            gdf_roads = gdf_roads.to_crs(DEFAULT_FIM_PROJECTION_CRS)

        return gdf_roads
    else:
        screen_queue.put(f"[{task_id}] No roads retrived )")
        file_logger.warning(f"[{task_id}] No roads retrived )")
        return None


def split_roads(gdf_roads, catchment_path, file_logger, screen_queue, task_id):
    huc_number = os.path.basename(os.path.dirname(catchment_path))

    # raise Exception(f"testing exception for {task_id}")

    if not os.path.exists(catchment_path):
        # return the original roads and assign a dummy catchment id of 000
        file_logger.info(f"no catchment file for {task_id}")
        screen_queue.put(f"no catchment file for {task_id}")
        gdf_roads_splitted = gdf_roads.copy()
        gdf_roads_splitted["osmid_catchid"] = gdf_roads_splitted["osmid"].astype(str) + "_000"

    # skip splitting for Alaska because there are too many catchments
    elif huc_number.startswith('19'):
        # return the original roads and assign a dummy catchment id of 000
        file_logger.info(f"skip splitting roads for Alaska HUC {task_id}")
        screen_queue.put(f"skip splitting roads for Alaska HUC {task_id}")
        gdf_roads_splitted = gdf_roads.copy()
        gdf_roads_splitted["osmid_catchid"] = gdf_roads_splitted["osmid"].astype(str) + "_000"
    else:
        file_logger.info(f"spliting roads for {task_id}")
        screen_queue.put(f"spliting roads for {task_id}")
        catchments_df = gpd.read_file(catchment_path)
        gdf_roads_splitted = gpd.overlay(gdf_roads, catchments_df[["ID", "geometry"]], how="intersection")

        if not gdf_roads_splitted.empty:
            gdf_roads_splitted.rename(columns={"ID": "catchment_id"}, inplace=True)
            gdf_roads_splitted["osmid_catchid"] = (
                gdf_roads_splitted["osmid"].astype(str) + "_" + gdf_roads_splitted["catchment_id"].astype(str)
            )
        else:  # if no catchment available at roads, then return the original roads and assign a dummy catchment id of 000
            file_logger.info(f"no intersecting catchments for {task_id}")
            screen_queue.put(f"no intersecting catchments for {task_id}")
            gdf_roads_splitted = gdf_roads.copy()
            gdf_roads_splitted["osmid_catchid"] = gdf_roads_splitted["osmid"].astype(str) + "_000"

    return gdf_roads_splitted


def single_huc_job(
    HUC_no, huc_boundary_path, split_boundary_path, output_dir, file_logger, screen_queue, task_id
):
    # this is basically the task function that is passed into mp run
    file_logger.info(f"started the process for {task_id}")
    try:
        huc_gpd = gpd.read_file(huc_boundary_path)
        huc_gpd_projected = huc_gpd.to_crs(pyproj.CRS.from_string("EPSG:4326"))
        huc_geom = huc_gpd_projected.iloc[0]["geometry"]

        osm_roads = pull_roads(HUC_no, huc_geom, file_logger, screen_queue, task_id)
        if osm_roads is not None:
            # clip them before splitting
            osm_roads_clipped = gpd.clip(osm_roads, huc_gpd, keep_geom_type=True)

            # it is possible that all retrieved roads are out of actual huc boundary (but within its rectangular box)
            if not osm_roads_clipped.empty:
                splitted_osm_roads = split_roads(
                    osm_roads_clipped, split_boundary_path, file_logger, screen_queue, task_id
                )

                output_path = os.path.join(output_dir, f"roads_{HUC_no}.gpkg")
                splitted_osm_roads.to_file(output_path)
            else:
                file_logger.warning(f"No roads within actual boundary of HUC for {task_id}")
                screen_queue.put(f"No roads within actual boundary of HUC for {task_id}")
        return 1, [True]

    except Exception as e:
        file_logger.error(f"❌ Exception in HUC {HUC_no}: {str(e)}")
        file_logger.error(traceback.format_exc())  # Optional: log full traceback
        return 0, [False]


def pull_osm_roads(preclip_dir, output_dir, number_jobs, lst_hucs):

    if number_jobs > 3:
        print("Overpy does not seem to like more than 3 jobs. Adjusting job number down to 3.")
        number_jobs = 3

    if not os.path.exists(preclip_dir):
        raise ValueError("preclip directory not found")

    start_time = datetime.now(timezone.utc)

    # make output directory
    os.makedirs(output_dir, exist_ok=True)

    # Create the logger
    file_dt_string = start_time.strftime("%Y%m%d-%H%M")
    log_file_path = os.path.join(output_dir, f"pull_osm_roads_{file_dt_string}.log")
    file_logger = setup_mp_file_logger(log_file_path, "pull_osm_roads")

    print("==================================")
    print("Starting load of OSM Road data")
    msg = f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}"
    file_logger.info(msg)
    print(msg)

    # === Build job list ===
    # get list of HUC8s
    huc_numbers = [
        str(huc)
        for huc in os.listdir(preclip_dir)
        if os.path.isdir(os.path.join(preclip_dir, huc)) and len(huc) == 8
    ]

    # If filtering hucs coming in, use it, if not continue with all.
    if lst_hucs != '':
        lst_hucs = lst_hucs.strip()
        selected_hucs = lst_hucs.split(" ")
        huc_numbers = [huc for huc in huc_numbers if huc in selected_hucs]
        # print(f"new filtered list is {huc_numbers}")

    msg = f"Total number of hucs to be processed after filtering, if applicable, is {len(huc_numbers)}"
    file_logger.info(msg)
    print(msg)
    huc_numbers.sort()

    tasks_args_list = []
    for HUC_no in huc_numbers:
        if HUC_no in (
            '22010000',
            '22030001',
        ):  # for guam and samoa it is possible that does not exist and generally not needed
            split_boundary_path = ""
        else:
            split_boundary_path = os.path.join(preclip_dir, HUC_no, "nwm_catchments_proj_subset.gpkg")

        tasks_args_list.append(
            {
                "HUC_no": HUC_no,
                "huc_boundary_path": os.path.join(preclip_dir, HUC_no, 'wbd.gpkg'),
                "split_boundary_path": split_boundary_path,
                "output_dir": output_dir,
            }
        )

    # === Run jobs in parallel ===
    mp_results = run_with_mp(
        task_function=single_huc_job,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=number_jobs,  # Overpass API does not really like more than 3 request at a time
        task_id_key="HUC_no",  # used for task id---must be one of the keys from args dict
    )

    print('multiprocessing tasks finished!')
    # only report if all succeeded or the failed ones
    failed_keys = [k for k, v in mp_results.items() if not v]

    if not failed_keys:
        file_logger.info("✅ All multiprocessing tasks succeeded")
        print("✅ All multiprocessing tasks succeeded")
    else:
        file_logger.info(f"❌ {len(failed_keys)} failed:")
        print(f"❌ {len(failed_keys)} failed:")
        for k in failed_keys:
            file_logger.info(f"  - {k}")
            print(f"  - {k}")

    # also report which HUCs did not have any roads doanloaded for them (regardless of the reason)
    report_road_download_status(huc_numbers, output_dir)

    # now combine all downloaded hucs into dedicated files for CONUS,  Alaska, Guam, and Samoa roads
    combine_hucs(output_dir)

    # Record run time
    end_time = datetime.now(timezone.utc)
    msg = f"Pulling OSM roads complete. {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    file_logger.info(msg)
    print(msg)
    dur = fh.print_date_time_duration(start_time, end_time, print_dur_msg=False)
    file_logger.info(dur)
    print(dur)


if __name__ == "__main__":

    # Only need to run this code once, since it looks for all hucs (conus, alaska, Guam, Am Somoa) from preclip folder

    # sample usage:
    # python foss_fim/data/roads/pull_osm_roads.py
    #     -p data/inputs/pre_clip_huc8/20250218
    #     -o outputs/roads/test/20250910  (the FIM Dev convention is to make a folder with the date at the end)

    # When using the -lh argument, make sure the hucs are in quotes, spaces between HUC numbers.
    #    ie) '21010005 19020302'

    # +++++++++++++++++++++++++
    # Note: Overpass API has a system at their servers that manages the number of calls coming in from
    # all locations, not just this script. At busier times, that threshold can be lower.
    # If you submit more than 3 jobs, the code will adjust it down to 3.
    # +++++++++++++++++++++++++

    parser = argparse.ArgumentParser(description='Download OSM roads for all HUCs')

    parser.add_argument(
        '-p',
        '--preclip_dir',
        help='REQUIRED: folder path where the most recent preclip data are located.',
        required=True,
    )

    parser.add_argument('-o', '--output_dir', help='REQUIRED: folder path for output.', required=True)

    parser.add_argument(
        '-j',
        '--number_jobs',
        help='OPTIONAL: Number of parallel API calls for downloading HUC roads. Default = 3, max = 3; values > 3 are reset to 3',
        required=False,
        default=3,
        type=int,
    )

    parser.add_argument(
        '-lh',
        '--lst-hucs',
        help='OPTIONAL: Space-delimited list of HUCs to which can be used to filter osm road processing.'
        ' This can be used for quick dev tests.'
        ' Defaults to all HUC8s in the WBD input file.',
        required=False,
        default='',
    )

    args = vars(parser.parse_args())
    pull_osm_roads(**args)
