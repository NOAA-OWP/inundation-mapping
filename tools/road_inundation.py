#!/usr/bin/env python3

import argparse
import errno
import os
import re
from timeit import default_timer as timer

import geopandas as gpd
import pandas as pd


def road_risk_status(
    fim_run_dir: str, flow_file: str, output_file_path: str, limit_hucs: list = []
) -> gpd.GeoDataFrame:
    """
    This function detect which roads are inundated by a specified flow file. The function requires a flow file (expected to follow
    the schema used by 'inundation_mosaic_wrapper') with data organized by 'feature_id' and 'discharge' in cms. The output includes a geopackage
    containing inundated roads based on forcasted discharge compared to threshold discharge (for inundation).

    Args:
        fim_run_dir (str):    Path to FIM outputs were written by fim_pipeline.
        flow_file (str):      Path to flow file to be used for inundation.
        output_file_path (str):             Path to output geopackage file.
        limit_hucs (list):    Optional. If specified, only the roads in these HUCs will be processed.

    Example usage:
    python /foss_fim/tools/road_inundation.py \
        -y /data/previous_fim/fim_4_5_2_0 \
        -f /data/ble_huc_12090301_flows_100yr.csv \
        -o /home/user/Documents/bridges/inundated_roads.gpkg \
        -u 12090301 02020005
    """

    # Check that fim run directory exists
    if not os.path.exists(fim_run_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fim_run_dir)

    # Get the list of all hucs in the directory
    entries = [d for d in os.listdir(fim_run_dir) if re.match(r'^\d{8}$', d)]
    hucs = []
    for entry in entries:
        full_path = os.path.join(fim_run_dir, entry)

        if os.path.isdir(full_path):
            hucs.append(entry)

    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    # Read the flow_file. make sure feaure id is str for consistency
    flow_file_data = pd.read_csv(flow_file, dtype={'feature_id': str})

    # Initialize an empty list to hold GeoDataFrames
    fimpact_gdfs_list = []

    # Filter HUCs if specified
    if limit_hucs:
        hucs = [h for h in limit_hucs if h in hucs]

    # Iterate through hucs
    for huc in hucs:
        print(f'Processing HUC: {huc}')
        # Construct the file path
        fimpact_path = os.path.join(fim_run_dir, huc, 'osm_roads_fimpact.csv')
        roads_path = os.path.join(fim_run_dir, huc, 'osm_roads_subset.gpkg')

        # Check if the file exists
        if not os.path.exists(fimpact_path) or not os.path.exists(roads_path):
            print(f"No FIMpact data in HUC {huc}. Skipping...")
            continue

        # Open the roads fimpact, making sure the ids are read as str
        cols_to_str = ['osmid', 'huc8', 'catchment_id', 'HydroID', 'feature_id', 'branch']
        dtype_dict = {col: str for col in cols_to_str}

        fimpact_df = pd.read_csv(fimpact_path, dtype=dtype_dict)

        # open roads geometry
        roads_gdf = gpd.read_file(roads_path)[['osmid_catchid', 'geometry']]

        # merge to get geometry of roads and add it to fimpact
        # No need to worry for huc numbers since for each huc iteration, we should not have any duplicated road with multiple hucs
        # and it is ok (good) to have multiple records for a osmid_catchid if they are from different hucs runs. This is what we want
        fimpact_gdf = fimpact_df.merge(roads_gdf, on='osmid_catchid', how='left')

        fimpact_gdf = gpd.GeoDataFrame(fimpact_gdf, geometry='geometry', crs=roads_gdf.crs)

        # Reproject to EPSG:4326
        fimpact_gdf = fimpact_gdf.to_crs('epsg:4326')
        fimpact_gdfs_list.append(fimpact_gdf)

    # Concatenate all GeoDataFrame into a single GeoDataFrame
    fimpact_gdfs = gpd.GeoDataFrame(pd.concat(fimpact_gdfs_list, ignore_index=True))

    # Find the common feature_id between flow_file and bridge_points
    fimpact_gdfs_merged = fimpact_gdfs.merge(flow_file_data, on='feature_id')

    # define inundation status
    def assign_inundation_status(row):
        if row['discharge'] > row['threshold_discharge']:
            return 'inundated'
        else:
            return 'not_inundated'

    # Apply inundation status to each row
    fimpact_gdfs_merged['inundation_status'] = fimpact_gdfs_merged.apply(assign_inundation_status, axis=1)

    # change the name of the given flow
    fimpact_gdfs_merged.rename(columns={'discharge': 'evaluated_discharge'}, inplace=True)

    # make a new file for roads that at least have one inundated status
    inundated_roads_gdf = fimpact_gdfs_merged[fimpact_gdfs_merged['inundation_status'] == 'inundated']

    inundated_roads_gdf = inundated_roads_gdf.drop_duplicates(subset='geometry')

    inundated_roads_gdf.to_file(output_file_path, driver="GPKG", engine='fiona')


if __name__ == "__main__":
    # sample usage
    # python foss_fim/tools/road_inundation.py
    # -y outputs/roads/test2_05030104
    # -o outputs/roads/test2_05030104/roads_inundation.gpkg
    # -f outputs/20240506T1338Z_ana_past_14day_max_high_flow_magnitude.csv

    # Parse arguments
    parser = argparse.ArgumentParser(description="Detect which road are inundated by a specified flow file.")
    parser.add_argument(
        "-y", "--fim_run_dir", help="Directory path to FIM run directory.", required=True, type=str
    )
    parser.add_argument(
        "-f",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o", "--output_file_path", help="Path to geopackage output.", required=True, type=str
    )
    parser.add_argument(
        "-u",
        "--limit_hucs",
        help="Optional. If specified, only the roads in these HUCs will be processed.",
        required=False,
        type=str,
        nargs="+",
    )

    start = timer()

    road_risk_status(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
