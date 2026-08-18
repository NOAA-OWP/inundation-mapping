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

    # read buildings data
    buildings_gdf = gpd.read_parquet(buildings_polygons)

    # read catchments to split the building polygons for each intersected HYDROID/feature_id.
    catchments_df = gpd.read_file(catchments_path, columns=['HydroID', 'feature_id', 'geometry'])

    # possible that feature id and hydro id be as type float. first make them int and then str
    catchments_df['feature_id'] = catchments_df['feature_id'].astype(int).astype(str)
    catchments_df['HydroID'] = catchments_df['HydroID'].astype(int).astype(str)

    # split buildings by HAND catchment boundaries so each piece has the correct HydroID
    buildings_gdf = gpd.overlay(buildings_gdf, catchments_df, how="intersection")
    buildings_gdf = buildings_gdf.explode(index_parts=True).reset_index(drop=True)

    if not buildings_gdf.empty:
        buildings_gdf['branch'] = branch_id

        with rasterio.open(hand_grid_raster, 'r') as hand_grid:
            hand_grid_profile = hand_grid.profile
            hand_grid_array = hand_grid.read(1)

        # HAND uses 0 to mark channel cells, which we need to exclude from the min threshold.
        # remap 0 -> nodata once here (single vectorized pass over the raster) so the built-in
        # `min` stat below can exclude it natively, instead of a per-feature python callback.
        nodata = hand_grid_profile['nodata']
        hand_grid_array = np.where(hand_grid_array == 0, nodata, hand_grid_array)

        # Call zonal_stats with the built-in min stat
        with rasterio.Env():
            stats = zonal_stats(
                buildings_gdf['geometry'],
                hand_grid_array,
                affine=hand_grid_profile['transform'],
                nodata=nodata,
                all_touched=True,
                stats=['min'],
            )

        buildings_gdf.loc[:, 'threshold_hand'] = [x.get('min') for x in stats]

        # it is possible that buildings cross areas of a HAND with nan data (levee), so make sure to remove those Nan threshold hands
        buildings_gdf = buildings_gdf.dropna(subset=['threshold_hand'])

        # no need to save geometry --helpful to save disc size
        buildings_gdf = buildings_gdf.drop(columns='geometry')

        # keep the minimum HAND threshold per UUID/HydroID after geometric splitting
        min_idx = buildings_gdf.groupby(['UUID', 'HydroID'])['threshold_hand'].idxmin()
        buildings_gdf = buildings_gdf.loc[min_idx]

        # make sure to record ids as str for csv output file
        cols_to_str = ['huc8', 'HydroID', 'feature_id', 'branch']
        buildings_gdf[cols_to_str] = buildings_gdf[cols_to_str].astype(str)

        buildings_gdf.to_csv(output_path, index=False)
    else:
        print(f'no split buildings for {branch_id}')
    del catchments_df


if __name__ == "__main__":
    '''
    Sample usage :
        python foss_fim/src/process_buildings_fimpact.py
        -g outputs/buildings/02050206/branches/0/rem_zeroed_masked_0.tif
        -r outputs/buildings/02050206/buildings_subset.parquet
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
