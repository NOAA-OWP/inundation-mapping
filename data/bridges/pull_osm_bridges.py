import argparse
import datetime as dt
import logging
import os
import sys
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import osmnx as ox
import pandas as pd
import pyproj
from dotenv import load_dotenv
from networkx import Graph, connected_components
from shapely.geometry import LineString, shape


# ox.settings.requests_timeout = 600  # Set timeout to 10 minutes (300 seconds)
srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')


# Save all OSM bridge features by HUC8 to a specified folder location.
# Bridges will have point geometry converted to linestrings if needed

"""
Feb 4, 2025: There are a good handful of HUCs that return no data.
Known HUCS are:
02060006,
04160001, 12110102, 13020206, 13020210, 15010006, 16020303, 16020302, 16060003, 16060004, 16060005,
16060006, 16060009, 16060010, 16060011, 16060013, 16060014, 17050109, 18090201,
19020203, 19020800, 20030000

NOTE: 02060006 is a weird one and times out even after 10 mins. Most are back in seconds.
When the run is done, compare the bottom of the logs for the phrase
'''HUCs that failed to download from OSM correctly are:'' and compare those hucs to the list above.

"""


# Dissolve touching lines
def find_touching_groups(gdf):
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


def pull_osm_features_by_huc(huc_bridge_file, huc_num, huc_geom):
    """
    Returns: The huc number but only if it failed, so we can make a master list of failed HUCs.
      The errors will be logged as it goes.
    """

    try:

        if os.path.exists(huc_bridge_file):
            # remove it
            os.remove(huc_bridge_file)

        logging.info(f" ** Creating gkpg for {huc_num}")

        gdf = ox.features_from_polygon(shape(huc_geom), {"bridge": True})

        if gdf is None or len(gdf) == 0:
            logging.info(f"osmnx pull for {huc_num} came back with no records")
            return huc_num

        # Note: Jan 31, 2025: Despite osmnx saying that it sends back a multi-index and the osmid, the field
        # return is just named "id". The multi-index column names are "element" and "id"
        # We just drop the "element" index level, then make a copy of of the id (index) column as the osmid.

        gdf = gdf.droplevel('element')
        gdf["osmid"] = gdf.index

        # Create bridge_type column
        # Check if 'highway' column exists
        if 'highway' not in gdf.columns:
            gdf['highway'] = None

        # Check if 'railway' column exists
        if 'railway' not in gdf.columns:
            gdf['railway'] = None

        # Create the bridge_type column by combining above information
        gdf['HUC8'] = huc_num
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

        # the "bridge" field is only the True / False
        # gd = gdf[gdf['bridge'] != 'abandoned']

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
            "unsigned_ref",
            "Fut_Ref",
            "Ref",
            "FIXME:ref",
        ]
        for bad_cn in bad_column_names:
            if bad_cn in gdf.columns:
                cols_to_drop.append(bad_cn)

        if len(cols_to_drop) > 0:
            gdf = gdf.drop(columns=cols_to_drop, axis=1)

        gdf1 = gdf[gdf.geometry.apply(lambda x: x.geom_type == 'LineString')]

        if str(huc_num).startswith('19'):
            gdf1 = gdf1.to_crs(ALASKA_CRS)
        else:
            gdf1 = gdf1.to_crs(DEFAULT_FIM_PROJECTION_CRS)

        # Perform dissolve touching lines
        buffered = gdf1.copy()
        buffered['geometry'] = buffered['geometry'].buffer(0.0001)
        # Find groups of touching geometries
        touching_groups = find_touching_groups(buffered)

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

        # returns the HUC but only if it failed so we can keep a list of failed HUCs
        return ""

    except Exception:
        print("---------------")
        logging.critical(f"**** ERROR: Couldn't write {huc_num}")
        logging.critical(traceback.format_exc())

        try:
            # rename and we can filter it out later. Even it fails sometimes
            if os.path.exists(huc_bridge_file):
                # change it's file name have "_bad" added before the extension
                new_name = huc_bridge_file.replace(".gpkg", "_bad.gpkg")
                os.rename(huc_bridge_file, new_name)
        except Exception as ex:
            print("---------------")
            logging.critical(
                f"Unable to delete {huc_bridge_file} for huc {huc_num} to add '_bad' in file name"
            )
            print(ex)

        return huc_num
        # Continue on


#
# Combine all HUC-based OSM bridges from specified folder
#
def combine_huc_features(output_dir):

    # make two separate files for alaska and non-alaska (conus)
    # only save out a subset of columns, because many hucs have different column names
    # and data, so you could end up with thousands of columns if you keep them all!

    # Note... files in error have been renamed to {xxxx}_bad.gpkg and will be skipped. To debug later

    # It is ok that we have dup osmid's at this as each point as one bridge can cross a huc boundary
    # so each huc can add the same osmid. When it gets to the final... it will be clipped and both
    # halves of the same bridge (in each huc) will have the same osmid. So.. don't change.

    alaska_bridge_file_names = list(Path(output_dir).glob("huc_19*_osm_bridges.gpkg"))
    if alaska_bridge_file_names:
        alaska_all_bridges_gdf_raw = pd.concat(
            [gpd.read_file(gpkg) for gpkg in alaska_bridge_file_names], ignore_index=True
        )
        alaska_all_bridges_gdf = alaska_all_bridges_gdf_raw[
            ['osmid', 'name', 'bridge_type', 'HUC8', 'geometry']
        ]

        alaska_all_bridges_gdf.reset_index(inplace=True)
        alaska_osm_bridge_file = os.path.join(output_dir, "alaska_osm_bridges.gpkg")

        logging.info(f"Writing Alaska bridge lines: {alaska_osm_bridge_file}")
        alaska_all_bridges_gdf.to_file(alaska_osm_bridge_file, driver="GPKG", index=False)

    conus_bridge_file_names = list(Path(output_dir).glob("huc_*_osm_bridges.gpkg"))
    conus_bridge_file_names = [file for file in conus_bridge_file_names if not file.name.startswith("huc_19")]
    if conus_bridge_file_names:
        conus_all_bridges_gdf_raw = pd.concat(
            [gpd.read_file(gpkg) for gpkg in conus_bridge_file_names], ignore_index=True
        )

        conus_all_bridges_gdf = conus_all_bridges_gdf_raw[
            ['osmid', 'name', 'bridge_type', 'lanes', 'HUC8', 'geometry']
        ]

        conus_all_bridges_gdf.reset_index(inplace=True)
        conus_osm_bridge_file = os.path.join(output_dir, "conus_osm_bridges.gpkg")

        logging.info(f"Writing CONUS bridge lines: {conus_osm_bridge_file}")
        conus_all_bridges_gdf.to_file(conus_osm_bridge_file, driver="GPKG", index=False, engine='fiona')

    return


def process_osm_bridges(wbd_file, output_folder, number_of_jobs, lst_hucs):
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

    huc8s_all = gpd.read_file(wbd_file)

    if len(huc8s_all) == 0:
        raise Exception("wbd_file has no records")

    logging.info(f"WBD rec count is {len(huc8s_all)} (pre-filtering if applicable)")
    section_time = dt.datetime.now(dt.timezone.utc)
    logging.info(f"WBD Loaded: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")

    # If filtering hucs coming in, use it, if not ocntinue
    if lst_hucs == '':  # process all
        hucs8s = huc8s_all
    else:
        lst_hucs = lst_hucs.strip()
        lst_hucs = lst_hucs.split()
        hucs8s = huc8s_all[huc8s_all['HUC8'].isin(lst_hucs)]

    logging.info(f"Number of hucs to process {len(hucs8s)}")

    # osm seems to like 4326
    logging.info("")
    logging.info("Reprojecting to 4326 (osm seems to like that one)")
    huc8s = hucs8s.to_crs(pyproj.CRS.from_string("epsg:4326"))
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

    section_time = dt.datetime.now(dt.timezone.utc)
    logging.info(f"Combining feature files started: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")

    # all huc8 processing must be completed before this function call
    combine_huc_features(output_folder)

    # Clean up individual HUC8 files
    logging.info('Deleting individual HUC8 files as a final cleanup step')
    # huc_files = Path(output_folder).glob('huc_*_osm_bridges.gpkg')  # Keep the "bad" ones
    # for huc_file in huc_files:
    #     try:
    #         os.remove(huc_file)
    #     except Exception as e:
    #         logging.info(f"Error deleting {huc_file}: {str(e)}")

    if len(failed_HUCs_list) > 0:
        logging.info("\n+++++++++++++++++++")
        logging.info("HUCs that failed to download from OSM correctly are:")
        huc_error_msg = "... "
        for huc in failed_HUCs_list:
            huc_error_msg += f", {huc} "
        logging.info(huc_error_msg)
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
    logging.captureWarnings(True)

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
        python3 /foss_fim/data/bridges/pull_osm_bridges.py
            -w /data/inputs/wbd/WBD_National_HUC8_EPSG_5070_HAND_domain.gpkg
            -p /data/inputs/osm/bridges/20250129/
            -j 4
            -lh '01010002 12090301'

        ** The -lh flg is an optional list of HUC8 if you want to process just those hucs
           if you want all HUC8s in the WBD you submit, leave this arg off

    Notes:
        - Note: Jan 2025: use the -w flag as the WBD_National_HUC8_EPSG_5070_HAND_domain.gpkg.
          It is the full HUC8 WBD layer, but removed all of the 22x, some of the 20x and 21x,
          removed North Alaska, keeping just the South Alaska we need, plus some stray unneeded HUCs.
          It has not been fully cleaned against our included_huc8_withAlaska.lst as this gpkg
          has some extras but that is ok for now untili we clean it more.
          Why the cleaned .gpkg file? we use it in ohter places and it keeps the
          size and processing time down.

        - This tool is meant to pull down all the Open Street Map bridge data for CONUS as a
        precursor to the bridge healing pre-processing (so, a pre-pre-processing step).
        It should be run only as often as the user thinks OSM has had any important updates.
        - As written, the code will skip any HUC that there's already a file for if you save
          an existing folder.

        - Each HUC8's worth of OSM bridge features is saved out individually, then merged together
        into one.

    New Feature: Jan 31, 2025:
        Scenerio:
        You run a full WBD and let's say 3 HUCs failed for whatever reasons, let's say two failed for
        timeouts.

        Now, the successfully processed HUCs gpkgs stay in the folder. We no longer remove them. The ones
        that failed first time, we renamed to have the word "bad.gkpg". That convention means the "bad" ones
        fall out and are not included in the final HUC rollup gpkg.

        Now, with the ability to an new input arg for just specific HUCs to be processed, you can
        re-run this tool with no changes but use the "-lh" flag to run just those specific HUCs
        you want to retry ie) the failed ones that are eligible for re-run.

        I will re-run those hucs, but then fully recalc the final outputs gpkgs, so now you have a
        correct final gpkg with the originally successful plus the new re-submitted ones.

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

    parser.add_argument(
        '-lh',
        '--lst_hucs',
        help='OPTIONAL: Space-delimited list of HUCs to which can be used to filter osm bridge processing.'
        ' Defaults to all HUC8s in the WBD input file.',
        required=False,
        default='',
    )

    args = vars(parser.parse_args())

    try:
        process_osm_bridges(**args)

    except Exception:
        logging.critical(traceback.format_exc())
        end_time = dt.datetime.now(dt.timezone.utc)
        logging.critical(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
