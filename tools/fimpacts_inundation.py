#!/usr/bin/env python3

import argparse
import errno
import os
import re
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd


def stage_lookup(flows, discharge_array, stage_array):
    return np.interp(flows, discharge_array, stage_array)


def get_evaluated_stage(fimpact_df, fim_run_dir, huc):
    # Ensure the column exists before assignment
    fimpact_df['evaluated_stage'] = np.nan

    # Loop over each unique branch
    for branch_id in fimpact_df['branch'].unique():
        # Load hydrotable once for the branch
        hydrotable_filename = os.path.join(
            fim_run_dir, str(huc), 'branches', str(branch_id), f'hydroTable_{branch_id}.csv'
        )
        hydrotable_df = pd.read_csv(
            hydrotable_filename,
            dtype={'HydroID': str, 'stage': float, 'discharge_cms': float},
            usecols=['HydroID', 'discharge_cms', 'stage'],
        )

        # Subset fimpact_df for this branch
        branch_df = fimpact_df[fimpact_df['branch'] == branch_id]

        # Interpolate for each row in this subset
        for _, row in branch_df.iterrows():
            single_hydro = hydrotable_df[hydrotable_df.HydroID == row.HydroID]
            evaluated_stage = stage_lookup(
                row.evaluated_discharge, single_hydro['discharge_cms'], single_hydro['stage']
            )
            fimpact_df.at[row.name, 'evaluated_stage'] = evaluated_stage


def inundation_status(
    fim_run_dir: str,
    flow_file: str,
    output_file_path: str,
    limit_hucs: list = [],
    feature_type: str = 'roads',
) -> gpd.GeoDataFrame:
    """
    This function detects which roads/buildings are inundated by a specified flow file and calculates flood depth.
    The function requires a flow file (expected to follow the schema used by 'inundation_mosaic_wrapper')
    with data organized by 'feature_id' and 'discharge' in cms. The output includes a geopackage
    containing inundated roads with their maximum flood depth.

    Args:
        fim_run_dir (str):    Path to FIM outputs were written by fim_pipeline.
        flow_file (str):      Path to csv flow file to be used for inundation.
        output_file_path (str):             Path to output geopackage file.
        limit_hucs (list):    Optional. If specified, only those HUCs will be processed.
        feature_type (str):   Optional. One of ['roads', 'buildings'].
    """

    supported_feature_types = {
        'roads': {
            'fimpact_filename': 'osm_roads_fimpact.csv',
            'subset_filename': 'osm_roads_subset.gpkg',
            'join_id': 'osmid_catchid',
            'id_columns': ['osmid', 'huc8', 'HydroID', 'feature_id', 'branch'],
        },
        'buildings': {
            'fimpact_filename': 'buildings_fimpact.csv',
            'subset_filename': 'buildings_subset.parquet',
            'join_id': 'UUID',
            'id_columns': ['UUID', 'huc8', 'HydroID', 'feature_id', 'branch'],
        },
    }

    if feature_type not in supported_feature_types:
        raise ValueError(
            f"Unsupported feature_type '{feature_type}'. Choose from: {list(supported_feature_types.keys())}"
        )

    feature_config = supported_feature_types[feature_type]

    # Check that fim run directory exists
    if not os.path.exists(fim_run_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fim_run_dir)

    # Check if file has .gpkg extension
    if not output_file_path.lower().endswith('.gpkg'):
        raise ValueError("Output file must have a .gpkg extension.")

    # confirm existence of output directory
    output_dir = os.path.dirname(output_file_path)

    # Create the directory if it doesn't exist
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

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
        fimpact_path = os.path.join(fim_run_dir, huc, feature_config['fimpact_filename'])
        feature_path = os.path.join(fim_run_dir, huc, feature_config['subset_filename'])

        # Check if the file exists
        if not os.path.exists(fimpact_path) or not os.path.exists(feature_path):
            print(f"No {feature_type} FIMpact data in HUC {huc}. Skipping...")
            continue

        # Open the fimpact file, making sure key ids are read as str
        cols_to_str = feature_config['id_columns']
        dtype_dict = {col: str for col in cols_to_str}

        fimpact_df = pd.read_csv(fimpact_path, dtype=dtype_dict)

        # read hydroTable and find the stage corresponding to given discharge
        # subtract that from threshold_hand...this is called flood_depth
        # so when we report the inundated roads, we also report the case with maximum flood depth
        # add given discharge
        fimpact_df = fimpact_df.merge(flow_file_data, on='feature_id')

        # change the name of the given flow to evaluated discharge
        fimpact_df.rename(columns={'discharge': 'evaluated_discharge'}, inplace=True)

        # selected the inundated records
        fimpact_df = fimpact_df[fimpact_df['evaluated_discharge'] > fimpact_df['threshold_discharge']]

        # add evaluated stage. For performance, read hydrotable of each branch once for all records in that branch
        print(f'  Computing evaluated stage for {len(fimpact_df)} {feature_type} records...')
        get_evaluated_stage(fimpact_df, fim_run_dir, huc)

        fimpact_df['flood_depth'] = fimpact_df['evaluated_stage'] - fimpact_df['threshold_hand']

        # for now, remove any record with negative flood depth. these may happen due to non-monotonic src especially in branch zero.
        # also ignore any flood depth less than 0.03048m (0.1 ft) to match with FIM spatial maps ( see inundation.py:364-367:)
        fimpact_df = fimpact_df[fimpact_df['flood_depth'] >= 0.03048]
        fimpact_df['flood_depth_ft'] = fimpact_df['flood_depth'] * 3.28084

        # open feature geometry
        feature_id = feature_config['join_id']
        if feature_path.lower().endswith('.parquet'):
            feature_gdf = gpd.read_parquet(feature_path)[[feature_id, 'geometry']]
        else:
            feature_gdf = gpd.read_file(feature_path)[[feature_id, 'geometry']]

        # merge to get geometry and add it to fimpact
        fimpact_gdf = fimpact_df.merge(feature_gdf, on=feature_id, how='left')

        fimpact_gdf = gpd.GeoDataFrame(fimpact_gdf, geometry='geometry', crs=feature_gdf.crs)

        # Reproject to EPSG:4326
        fimpact_gdf = fimpact_gdf.to_crs('epsg:4326')

        fimpact_gdfs_list.append(fimpact_gdf)

    if not fimpact_gdfs_list:
        raise ValueError(f'No inundated {feature_type} records found for the requested HUCs/run.')

    # Concatenate all GeoDataFrame into a single GeoDataFrame
    fimpact_gdfs = gpd.GeoDataFrame(pd.concat(fimpact_gdfs_list, ignore_index=True))

    fimpact_gdfs = fimpact_gdfs.loc[fimpact_gdfs.groupby([feature_config['join_id']])['flood_depth'].idxmax()]

    fimpact_gdfs.to_file(output_file_path, driver="GPKG", engine='fiona')


if __name__ == "__main__":
    # sample usage
    # python foss_fim/tools/road_inundation.py
    # -y outputs/roads/test2_05030104
    # -o outputs/roads/test2_05030104/roads_inundation.gpkg
    # -f data/inputs/rating_curve/nwm_recur_flows/nwm3_17C_recurr_50_0_cms.csv

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Detect which roads/buildings are inundated by a specified flow file."
    )
    parser.add_argument(
        "-y", "--fim_run_dir", help="Directory path to FIM run directory.", required=True, type=str
    )
    parser.add_argument(
        "-f",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns must be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o", "--output_file_path", help="Path to geopackage output.", required=True, type=str
    )
    parser.add_argument(
        "-u",
        "--limit_hucs",
        help="Optional. If specified, only these HUCs will be processed.",
        required=False,
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "-t",
        "--feature_type",
        help="Feature type to process. Options: roads, buildings.",
        required=True,
        type=str,
        choices=['roads', 'buildings'],
        default='roads',
    )

    start = timer()

    inundation_status(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
