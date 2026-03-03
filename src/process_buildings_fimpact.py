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


def min_hand_excluding_zero(values):
    # Convert to unmasked array and drop 0 and masked/nodata
    # return np.nan if on NoData Hand to be able to filter them later
    data = np.ma.filled(values.astype(float), np.nan)  # Convert masked to nan
    valid = data[(data != 0) & (~np.isnan(data))]
    return float(np.min(valid)) if valid.size > 0 else np.nan


def process_buildings_fimpact(
    hand_grid_raster: str, buildings_polygons: str, catchments_path: str, output_path: str
) -> None:
    """
    Processes Buildings impacts within a HUC region using the FIMpact framework and  saves the result to a csv file.

    Parameters:
    - source_hand_raster (str): REQUIRED. Path to the source HAND raster file
    - buildings_polygons (str): REQUIRED. Path to a GeoPackage (GPKG) file containing the buildings segments.
    - catchments (srr): REQUIRED. Path to HAND catchment
    - output_path (str): REQUIRED. Path where the output CSV file will be saved.

    """
    # get branch id from output file passed from fim pipeline
    branch_id = Path(output_path).parent.name

    # read hand grid
    with rasterio.open(hand_grid_raster, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    # read buildings data
    buildings_gdf = gpd.read_file(buildings_polygons)

    # read catchments to get the HYDROID for each building
    catchments_df = gpd.read_file(catchments_path, columns=['HydroID', 'feature_id', 'geometry'])

    joined = gpd.sjoin(buildings_gdf, catchments_df, how="left", predicate="intersects")

    # keep only one match per building (first intersect found)
    one = joined.loc[~joined.index.duplicated(keep="first")]

    # assign columns back to original dataframe
    buildings_gdf["HydroID"] = one["HydroID"]
    buildings_gdf["feature_id"] = one["feature_id"]

    if not buildings_gdf.empty:
        buildings_gdf['branch'] = branch_id

        # Call zonal_stats with the custom stat
        stats = zonal_stats(
            buildings_gdf['geometry'],
            hand_grid_array,
            affine=hand_grid_profile['transform'],
            nodata=hand_grid_profile["nodata"],
            all_touched=True,
            stats=[],  # No built-in stats needed
            add_stats={"min_ex0": min_hand_excluding_zero},
        )

        buildings_gdf.loc[:, 'threshold_hand'] = [x.get('min_ex0') for x in stats]

        # it is possible that buildings cross areas of a HAND with nan data (levee), so make sure to remove those Nan threshold hands
        buildings_gdf = buildings_gdf.dropna(subset=['threshold_hand'])

        # no need to save geometry --helpful to save disc size
        buildings_gdf = buildings_gdf.drop(columns='geometry')

        # group by UUID, and report the min of threshold hand
        min_idx = buildings_gdf.groupby(['UUID', 'HydroID'])['threshold_hand'].idxmin()
        buildings_gdf = buildings_gdf.loc[min_idx]

        buildings_gdf.to_csv(output_path, index=False)
    else:
        print('no building polygons to process FIMpacts')


if __name__ == "__main__":
    '''
    Sample usage :
        python foss_fim/src/process_buildings_fimpact.py
        -g outputs/buildings/02050206/branches/0/rem_zeroed_masked_0.tif
        -r outputs/buildings/02050206/buildings_subset.gpkg
        -c outputs/roads/02050206/branches/0/gw_catchments_reaches_filtered_addedAttributes_crosswalked_0.gpkg
        -o outputs/buildings/02050206/branches/0/buildings_fimpact_0.csv

    '''

    parser = argparse.ArgumentParser(description='Process buildings FIMpact')

    parser.add_argument(
        '-g', '--hand_grid_raster', help='REQUIRED: Path for HAND grid raster file', required=True
    )

    parser.add_argument(
        '-r',
        '--buildings_polygons',
        help='REQUIRED: Path to a GPKG file containing the buildings polygons ',
        required=True,
    )

    parser.add_argument(
        '-c',
        '--catchments_path',
        help='REQUIRED: Path and file name of the HAND catchments geopackage',
        required=True,
    )

    parser.add_argument(
        '-o', '--output_path', help='REQUIRED: Path where the output csv file will be saved', required=True
    )

    args = vars(parser.parse_args())

    process_buildings_fimpact(**args)
