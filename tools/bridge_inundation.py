#!/usr/bin/env python3

import argparse
import errno
import os
import re
from timeit import default_timer as timer

import geopandas as gpd
import pandas as pd


def bridge_risk_status(
    hydrofabric_dir: str, flow_file: str, output_dir: str, limit_hucs: list = []
) -> gpd.GeoDataFrame:
    """
    This function detect which bridge points are affected by a specified flow file. The function requires a flow file (expected to follow
    the schema used by 'inundation_mosaic_wrapper') with data organized by 'feature_id' and 'discharge' in cms. The output includes a geopackage
    containing bridge points labeled as "threatened", "at risk", or "not at risk" based on forcasted discharge compared to preset discharge
    ("threshold_discharge" or "threshold_discharge75").

    Args:
        hydrofabric_dir (str):    Path to hydrofabric directory where FIM outputs were written by
                                    fim_pipeline.
        flow_file (str):      Path to flow file to be used for inundation.
                                    feature_ids in flow_file should be present in supplied HUC.
        output (str):             Path to output geopackage.
        limit_hucs (list):    Optional. If specified, only the bridges in these HUCs will be processed.

    Example usage:
    python /foss_fim/tools/bridge_inundation.py \
        -y /data/previous_fim/fim_4_5_2_0 \
        -f /data/ble_huc_12090301_flows_100yr.csv \
        -o /home/user/Documents/bridges/inundated_bridge_pnts.gpkg \
        -u 12090301 02020005
    """

    dir_path = hydrofabric_dir
    # Check that hydrofabric_dir exists
    if not os.path.exists(dir_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dir_path)

    # Get the list of all hucs in the directory
    entries = [d for d in os.listdir(dir_path) if re.match(r'^\d{8}$', d)]
    hucs = []
    for entry in entries:
        # create the full path of the entry
        full_path = os.path.join(dir_path, entry)
        # check if the netry is a directory
        if os.path.isdir(full_path):
            hucs.append(entry)

    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    # Read the flow_file
    dtype_dict = {'feature_id': str}
    flow_file_data = pd.read_csv(flow_file, dtype=dtype_dict)

    # Initialize an empty list to hold GeoDataFrames
    gdfs = []

    # Filter HUCs if specified
    if limit_hucs:
        hucs = [h for h in limit_hucs if h in hucs]

    # Iterate through hucs
    for huc in hucs:
        print(f'Processing HUC: {huc}')
        # Construct the file path
        gpkg_path = os.path.join(dir_path, huc, 'osm_bridge_centroids.gpkg')
        # Check if the file exists
        if not os.path.exists(gpkg_path):
            print(f"No GeoPackage file found in {gpkg_path}. Skipping...")
            continue
        # Open the bridge point GeoPackage for each huc
        bri_po = gpd.read_file(gpkg_path)

        # Save the origignal crs in a new column
        bri_po['original_crs'] = bri_po.crs.to_string()

        # Reproject to EPSG:4326
        bri_po = bri_po.to_crs('epsg:4326')
        gdfs.append(bri_po)

    # Concatenate all GeoDataFrame into a single GeoDataFrame
    bridge_points = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

    if bridge_points.feature_id.dtype != 'int':
        bridge_points.feature_id = bridge_points.feature_id.astype(int)
    if flow_file_data.feature_id.dtype != 'int':
        flow_file_data.feature_id = flow_file_data.feature_id.astype(int)

    # Find the common feature_id between flow_file and bridge_points
    merged_bri = bridge_points.merge(flow_file_data, on='feature_id', how='inner')

    # Assign risk status for each point
    def risk_class(row):
        if row['discharge'] > row['threshold_discharge']:
            return 'threatened'
        elif row['threshold_discharge75'] <= row['discharge'] < row['threshold_discharge']:
            return 'at_risk'
        else:
            return 'not_at_risk'

    # Apply risk_class function to each row
    merged_bri['risk_status'] = merged_bri.apply(risk_class, axis=1)

    # change the name of the given flow
    merged_bri.rename(columns={'discharge': 'evaluated_discharge'}, inplace=True)

    # Drop not_at_risk status from points with the same geometry
    mapping_dic = {'not_at_risk': 0, 'at_risk': 1, 'threatened': 2}
    merged_bri['risk'] = merged_bri['risk_status'].map(mapping_dic)
    merged_bri.reset_index(drop=True, inplace=True)
    merged_data_max = merged_bri.groupby('geometry')['risk'].idxmax()
    bridge_out = merged_bri.loc[merged_data_max]
    bridge_out.reset_index(drop=True, inplace=True)
    bridge_out.drop('risk', axis=1, inplace=True)
    bridge_out.to_file(output_dir, index=False, driver="GPKG", engine='fiona')

    return bridge_out


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Detect which bridge points are affected by a specified flow file."
    )
    parser.add_argument(
        "-y",
        "--hydrofabric_dir",
        help="Directory path to FIM hydrofabric by processing unit.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-f",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument("-o", "--output_dir", help="Path to geopackage output.", required=True, type=str)
    parser.add_argument(
        "-u",
        "--limit_hucs",
        help="Optional. If specified, only the bridges in these HUCs will be processed.",
        required=False,
        type=str,
        nargs="+",
    )

    start = timer()

    # Extract to dictionary and run
    bridge_risk_status(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
