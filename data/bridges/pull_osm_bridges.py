import argparse
import os
from pathlib import Path

import geopandas as gpd
import osmnx as ox
import pandas as pd
from shapely import LineString
from shapely.geometry import shape


#
# Save all OSM bridge features by HUC8 to a specified folder location.
# Bridges will have point geometry converted to linestrings if needed
#
def pull_osm_features_by_huc(huc_bridge_file, huc_num, huc_geom):
    tags = {"bridge": True}
    try:
        if os.path.exists(huc_bridge_file):
            print(f" **{huc_bridge_file} already exists.. returning")
            return
        print(f" ** Saving off {huc_num}")
        gdf = ox.features_from_polygon(shape(huc_geom), tags)

        # fix contents of attributes to make it possible to save
        gdf['nodes'] = [str(x) for x in gdf['nodes']]

        # fix geometry for saving if there are any Points
        for i, row in gdf.iterrows():
            if 'POINT' in str(row[1]):
                new_geom = LineString([row[1], row[1]])
                gdf.at[i, 'geometry'] = new_geom

        gdf.to_file(huc_bridge_file)
    except Exception as e:
        print(f"\t--- Couldn't write {huc_num}")
        print(f"\t{e}")

    return


#
# Combine all HUC-based OSM bridges from specified folder
#
def combine_huc_features(location, output_lines_location, output_midpts_location):
    shapefile_names = Path(location).glob("huc*_osm_bridges.shp")
    shapefiles = [shp for shp in shapefile_names]  # need this to get the first item's crs
    gdf = gpd.GeoDataFrame(
        pd.concat([gpd.read_file(shp) for shp in shapefiles], ignore_index=True),
        crs=gpd.read_file(shapefiles[0]).crs,
    )

    # only save out a subset of columns, because many hucs have different column names
    # and data, so you could end up with thousands of columns if you keep them all!
    gdf = gdf.to_crs("epsg:5070")
    gdf[['osmid', 'geometry']].to_file(output_lines_location)

    # save out midpoints (centroids) of bridge lines
    gdf['geometry'] = gdf.centroid
    gdf[['osmid', 'geometry']].to_file(output_midpts_location)

    return


def process_osm_bridges(wbd_file, location, output_lines_location, output_midpts_location):
    if not os.path.exists(location):
        os.mkdir(location)

    print("*** Reading in HUC8 file")
    huc8s = gpd.read_file(wbd_file, driver="FileGDB", layer=0)

    # the content of the following loop can be multiprocessed
    for row in huc8s.iterrows():
        huc = row[1]
        huc_bridge_file = os.path.join(location, f"huc{huc['HUC8']}_osm_bridges.shp")

        pull_osm_features_by_huc(huc_bridge_file, huc['HUC8'], huc['geometry'])

    # all huc8 processing must be completed before this function call
    combine_huc_features(location, output_lines_location, output_midpts_location)

    return


if __name__ == "__main__":
    '''
    Sample usage (min params):
        python3 /foss_fim/data/bridges/pull_osm_bridges.py
            -w /data/inputs/wbd/wbdhu8_a_us_oct2018.gdb
            -p /data/inputs/osm/
            -l /data/inputs/osm/osm_bridges.shp
            -m /data/inputs/osm/osm_bridges_midpts.shp

    Notes:
        - This tool is meant to pull down all the Open Street Map bridge data for CONUS as a
        precursor to the bridge healing pre-processing (so, a pre-pre-processing step).
        It should be run only as often as the user thinks OSM has had any important updates.
        - As written, the code will skip any HUC that there's already a file for.
        - Each HUC8's worth of OSM bridge features is saved out individually, then merged together
        into one. The HUC8 files can be deleted if desired, as an added final cleanup step.
        - The HUCs of interest wbd file might fail if it's a gkpg, based on the use of the FileGDB driver
        specified in the code, but I believe shapefiles are fine.
    '''

    parser = argparse.ArgumentParser(description='Acquires and saves Open Street Map bridge features')

    parser.add_argument(
        '-w',
        '--wbd_file',
        help='REQUIRED: location the shapefile, gpkg or gdb files that will'
        ' contain all the HUC8 clip regions in one layer.',
        required=True,
    )

    parser.add_argument(
        '-p',
        '--location',
        help='REQUIRED: folder path location where individual HUC8 shapefiles'
        ' will be saved to after being downloaded from OSM.'
        ' File names are hardcoded to format hucxxxxxxxx_osm_bridges.shp,'
        ' with xxxxxxxx as the HUC8 value',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--output_lines_location',
        help='REQUIRED: path to shapefile or gpkg where OSM bridge line features'
        ' will be saved to after being downloaded from OSM by HUC and combined.'
        ' Will overwrite existing files with the same path.',
        required=True,
    )

    parser.add_argument(
        '-m',
        '--output_midpts_location',
        help='REQUIRED: path to shapefile or gpkg where OSM bridge midpoints'
        ' will be saved to after being downloaded from OSM by HUC and combined.'
        ' Will overwrite existing files with the same path.',
        required=True,
    )

    args = vars(parser.parse_args())

    process_osm_bridges(**args)
