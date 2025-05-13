import argparse
import glob
import os
import re

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from rasterio import features
from rasterio.warp import Resampling, reproject
from rasterstats import zonal_stats


def process_roads_fimpact(hand_grid_raster: str, osm_road_vector: str, output_path: str) -> gpd.GeoDataFrame:
    """
    Processes road impacts within a HUC region using the FIMpact framework.

    Parameters:
    - source_hand_raster (str): REQUIRED. Path to the source HAND raster file
    - osm_road_vector (str): REQUIRED. Path to a GeoPackage (GPKG) file containing the splitted road centerline vectors.
    - output_path (str): REQUIRED. Path where the output GeoPackage (GPKG) file will be saved.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame containing processed road impact data.
    """

    # read hand grid
    with rasterio.open(hand_grid_raster, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    # read roads data
    roads_gdf = gpd.read_file(osm_road_vector)

    # Get median of hand values for each bridge since using min would catch random hand-cells with zero value and the entire road is reported as inundated always
    # when we got lidar data for roads, then we can use min which provides more conservative/safe results
    selected_stat = "percentile_25"  # "median"
    stats = zonal_stats(
        roads_gdf['geometry'],
        hand_grid_array,
        affine=hand_grid_profile['transform'],
        stats=selected_stat,
        nodata=hand_grid_profile["nodata"],
        all_touched=True,
    )

    roads_gdf.loc[:, 'threshold_hand'] = [x.get(selected_stat) for x in stats]

    # # Add the branch id to the catchments
    # branch_dir = re.search(r'branches/(\d{10}|0)/', catchments).group()
    # branch_id = re.search(r'(\d{10}|0)', branch_dir).group()
    # roads_gdf['branch'] = branch_id
    # roads_gdf['mainstem'] = 0 if branch_id == '0' else 1

    roads_gdf.to_file(output_path)


if __name__ == "__main__":
    # note that we do not apply any buffer for roads since especially it can catch min HAND from neighboring catchment.
    '''
    Sample usage :
        python foss_fim/src/process_roads_fimpact.py
        -g outputs/roads/bugfixes_02050206_final/02050206/branches/0/rem_zeroed_masked_0.tif
        -r outputs/roads/results/roads_02050206.gpkg
        -o outputs/roads/results/roads_fimpact_02050206.gpkg

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
        '-o', '--output_path', help='REQUIRED: Path where the output GPKG file will be saved', required=True
    )

    args = vars(parser.parse_args())

    process_roads_fimpact(**args)
