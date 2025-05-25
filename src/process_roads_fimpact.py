import argparse
import glob
import os
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from rasterio import features
from rasterio.warp import Resampling, reproject
from rasterstats import zonal_stats


def process_roads_fimpact(
    hand_grid_raster: str, osm_road_vector: str, catchments_path: str, output_path: str
) -> gpd.GeoDataFrame:
    """
    Processes road impacts within a HUC region using the FIMpact framework.

    Parameters:
    - source_hand_raster (str): REQUIRED. Path to the source HAND raster file
    - osm_road_vector (str): REQUIRED. Path to a GeoPackage (GPKG) file containing the splitted road centerline vectors.
    - catchments (srr): REQUIRED. Path to HAND catchment
    - output_path (str): REQUIRED. Path where the output GeoPackage (GPKG) file will be saved.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame containing processed road impact data.
    """
    # get branch id from output file passed from fim pipeline
    branch_id = Path(output_path).parent.name

    catchments_df = gpd.read_file(catchments_path, columns=['HydroID', 'feature_id', 'order_', 'geometry'])

    # read hand grid
    with rasterio.open(hand_grid_raster, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    # read roads data
    roads_gdf = gpd.read_file(osm_road_vector)

    # split the roads based on HAND catchments
    roads_gdf_splitted = gpd.overlay(roads_gdf, catchments_df, how="intersection")

    if not roads_gdf_splitted.empty:
        # Get median of hand values for each bridge since using min would catch random hand-cells with zero value and the entire road is reported as inundated always
        # when we got lidar data for roads, then we can use min which provides more conservative/safe results
        selected_stat = "percentile_25"  # "median"
        stats = zonal_stats(
            roads_gdf_splitted['geometry'],
            hand_grid_array,
            affine=hand_grid_profile['transform'],
            stats=selected_stat,
            nodata=hand_grid_profile["nodata"],
            all_touched=True,
        )

        roads_gdf_splitted.loc[:, 'threshold_hand'] = [x.get(selected_stat) for x in stats]

        roads_gdf_splitted['branch'] = branch_id
        roads_gdf_splitted.to_file(os.path.splitext(output_path)[0] + ".gpkg")
        roads_gdf_splitted.drop(columns=['geometry'], inplace=True)

        roads_gdf_splitted.to_csv(output_path, index=False)
    else:
        print(f'no splitted roads for {branch_id}')

    del catchments_df


if __name__ == "__main__":
    # note that we do not apply any buffer for roads since especially it can catch min HAND from neighboring catchment.
    '''
    Sample usage :
        python foss_fim/src/process_roads_fimpact.py
        -g outputs/roads/02050206/branches/0/rem_zeroed_masked_0.tif
        -c outputs/roads/02050206/branches/0/gw_catchments_reaches_filtered_addedAttributes_crosswalked_0.gpkg
        -r outputs/roads/02050206/osm_roads_subset.gpkg
        -o outputs/roads/02050206/branches/0/test_osm_roads_fimpact_0.csv

    '''

    parser = argparse.ArgumentParser(description='Heals HAND for osm bridges')

    parser.add_argument(
        '-g', '--hand_grid_raster', help='REQUIRED: Path for HAND grid raster file', required=True
    )

    parser.add_argument(
        '-r',
        '--osm_road_vector',
        help='REQUIRED: Path to a GPKG file containing the osm roads centerline ',
        required=True,
    )

    parser.add_argument(
        '-c',
        '--catchments_path',
        help='REQUIRED: Path and file name of the catchments geopackage',
        required=True,
    )

    parser.add_argument(
        '-o', '--output_path', help='REQUIRED: Path where the output GPKG file will be saved', required=True
    )

    args = vars(parser.parse_args())

    process_roads_fimpact(**args)

    '''
    for inundation:
        possibility of one road with multilple feature id comining from dfferent branchs
            - if at least one of the branches inundated that osmid_catchid, we flag the entire road segment as inundated.

    save a new gpkg for only inundated roads
    '''
