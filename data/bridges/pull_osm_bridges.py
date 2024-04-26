import argparse
import datetime as dt
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from pathlib import Path

import fiona
import geopandas as gpd
import osmnx as ox
import pandas as pd
import pyproj
from shapely import LineString
from shapely.geometry import shape


CRS = "epsg:5070"


#
# Save all OSM bridge features by HUC8 to a specified folder location.
# Bridges will have point geometry converted to linestrings if needed
#
def pull_osm_features_by_huc(huc_bridge_file, huc_num, huc_geom):
    tags = {"bridge": True}
    try:
        print("++++++++++++++++++++++++")
        if os.path.exists(huc_bridge_file):
            print(f" **{huc_bridge_file} already exists.. skipped")
            # return huc_bridge_file
            return
        print(f" ** Saving off {huc_num}")

        gdf = ox.features_from_polygon(shape(huc_geom), tags)

        if gdf is None or len(gdf) == 0:
            print(f"osmnx pull for {huc_num} came back with no records")
            return

        cols_to_drop = []
        for col in gdf.columns:
            if any(isinstance(val, list) for val in gdf[col]):
                cols_to_drop.append(col)

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
        gdf.to_file(huc_bridge_file)

    except Exception:
        print(f"\t--- Couldn't write {huc_num}")
        print(traceback.format_exc())
        # Continue on

    return


#
# Combine all HUC-based OSM bridges from specified folder
#
def combine_huc_features(location, output_lines_location, output_midpts_location):
    shapefile_names = Path(location).glob("huc_*_osm_bridges.gpkg")

    gdf = pd.concat([gpd.read_file(shp) for shp in shapefile_names], ignore_index=True)

    # only save out a subset of columns, because many hucs have different column names
    # and data, so you could end up with thousands of columns if you keep them all!
    gdf = gdf.to_crs(CRS)

    gdf[['osmid', 'geometry']].to_file(output_lines_location)

    # save out midpoints (centroids) of bridge lines
    gdf['geometry'] = gdf.centroid
    gdf[['osmid', 'geometry']].to_file(output_midpts_location)

    return


def process_osm_bridges(wbd_file, location, output_lines_location, output_midpts_location, number_of_jobs):
    start_time = dt.datetime.now(dt.timezone.utc)

    print("==================================")
    print("Starting load of OSM bridge data")
    print(f"Start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
    print()

    # --------------------------
    # Validation
    if os.path.exists(wbd_file) is False:
        raise Exception(f"The wbd file of {wbd_file} does not exist")

    split_wbd_file_name = os.path.splitext(wbd_file)
    if len(split_wbd_file_name) != 2:
        raise Exception(f"The wbd file of {wbd_file} does not appear to valid file name")

    if str(split_wbd_file_name[1]).lower() != ".gpkg":
        raise Exception(f"The wbd file of {wbd_file} does not appear to valid a gpkg")

    # ------------------
    split_output_lines_location = os.path.splitext(output_lines_location)
    if len(split_output_lines_location) != 2:
        raise Exception(
            "The file name submitted for (-l) output lines location"
            f" value of {output_lines_location} does not appear to be a valid file name"
        )

    if str(split_output_lines_location[1]).lower() != ".gpkg":
        raise Exception(
            "The outputs come out as gkpgs. The file name submitted for (-l) output lines"
            f"  location value of {output_lines_location} does not appear to be a gpkg file name"
        )

    # ------------------
    split_output_midpts_location = os.path.splitext(output_midpts_location)
    if len(split_output_midpts_location) != 2:
        raise Exception(
            "The file name submitted for (-m) output midpoint location"
            f" value of {output_midpts_location} does not appear to be a valid file name"
        )

    if str(split_output_midpts_location[1]).lower() != ".gpkg":
        raise Exception(
            "The outputs come out as gkpgs. The file name submitted for (-m) output"
            f" value of {output_midpts_location} does not appear to be a gpkg file name"
        )

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
    if not os.path.exists(location):
        os.mkdir(location)

    print("*** Reading in HUC8 file")
    print("*** Depending on WBD size, this can take a bit. 5 to 15 mins is not uncommon.")
    huc8s = gpd.read_file(wbd_file, layer="WBDHU8")

    if len(huc8s) == 0:
        raise Exception("wbd_file has no records")

    print(f"wbd rec count is {len(huc8s)}")
    section_time = dt.datetime.now(dt.timezone.utc)
    print(f"WBD Loaded: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")

    # osm seems to like 4326
    print()
    print("Reprojecting to 4326 (osm seems to like that one)")
    huc8s = huc8s.to_crs(pyproj.CRS.from_string("epsg:4326"))
    section_time = dt.datetime.now(dt.timezone.utc)
    print(f"Reprojection done: {section_time.strftime('%m/%d/%Y %H:%M:%S')}")

    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        futures = {}
        for row in huc8s.iterrows():
            huc = row[1]
            huc_bridge_file = os.path.join(location, f"huc_{huc['HUC8']}_osm_bridges.gpkg")
            args = {"huc_num": huc['HUC8'], "huc_bridge_file": huc_bridge_file, "huc_geom": huc['geometry']}
            future = executor.submit(pull_osm_features_by_huc, **args)
            futures[future] = future

    print()
    print("Pulling hucs now complete")
    print("Beginning combining features")
    # all huc8 processing must be completed before this function call
    combine_huc_features(location, output_lines_location, output_midpts_location)

    # Get time metrics
    end_time = dt.datetime.now(dt.timezone.utc)
    print("")
    print("==================================")
    print("OSM bridge data load complete")
    print(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")

    time_delta = end_time - start_time
    total_seconds = int(time_delta.total_seconds())

    ___, rem_seconds = divmod(total_seconds, 60 * 60 * 24)
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

    print(f"Duration: {time_fmt}")

    print()

    return


if __name__ == "__main__":
    '''
    Sample usage (min params):
        python3 data/bridges/pull_osm_bridges.py
            -w /data/inputs/wbd/WBD_National_HUC8.gpkg
            -p /data/inputs/osm/bridges/
            -l /data/inputs/osm/bridges/osm_bridges.gpkg
            -m /data/inputs/osm/bridges/osm_bridges_midpts.gpkg
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
        '--location',
        help='REQUIRED: folder path location where individual HUC8 geopackages'
        ' will be saved to after being downloaded from OSM.'
        ' File names are hardcoded to format hucxxxxxxxx_osm_bridges.gpkg,'
        ' with xxxxxxxx as the HUC8 value',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--output_lines_location',
        help='REQUIRED: path to gpkg where OSM bridge line features'
        ' will be saved to after being downloaded from OSM by HUC and combined.'
        ' Will overwrite existing files with the same path.',
        required=True,
    )

    parser.add_argument(
        '-m',
        '--output_midpts_location',
        help='REQUIRED: path to gpkg where OSM bridge midpoints'
        ' will be saved to after being downloaded from OSM by HUC and combined.'
        ' Will overwrite existing files with the same path.',
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

    process_osm_bridges(**args)
