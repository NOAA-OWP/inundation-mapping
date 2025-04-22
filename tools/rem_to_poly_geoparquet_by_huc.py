import argparse
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape


SCALE_FACTOR = 1000
INT_NODATA_VALUE = 65535


logger = logging.getLogger("flood_processing")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def interpolate_discharge(rem_value, hydro_id, htable_df):
    src_data = htable_df[htable_df['HydroID'] == hydro_id]
    if len(src_data) > 1:
        discharge = np.interp(rem_value, src_data['stage'], src_data['discharge_cms'])
        volume_m3 = np.interp(rem_value, src_data['stage'], src_data['Volume (m3)'])
    else:
        discharge = np.nan
        volume_m3 = np.nan
    return discharge, volume_m3


def polygonize_combined_rasters(elevation, catchment_ids, transform, threshold, branch_id, htable_df, logger):
    start_time = time.time()
    mask = (elevation < threshold) & (elevation != INT_NODATA_VALUE)
    combined_mask = np.where(mask, catchment_ids, 0).astype(catchment_ids.dtype)
    features = []
    true_rem = threshold / SCALE_FACTOR

    for geom, value in shapes(combined_mask, mask=(combined_mask > 0), transform=transform):
        if value > 0:
            geom_shape = shape(geom)
            catchment_id = int(value)
            discharge_cms, volume_m3 = interpolate_discharge(true_rem, catchment_id, htable_df)
            features.append(
                {
                    'geometry': geom_shape,
                    'properties': {
                        'rem': true_rem,
                        'catchment_id': catchment_id,
                        'discharge_cms': discharge_cms,
                        'volume_m3': volume_m3,
                        'branch_id': branch_id,
                    },
                }
            )

    elapsed = time.time() - start_time
    logger.info(
        f"[Branch {branch_id}] Finished processing threshold {true_rem:.4f} in {elapsed:.2f} seconds."
    )
    print(f"[Branch {branch_id}] Finished processing threshold {true_rem:.4f} in {elapsed:.2f} seconds.")
    return features


def process_branch(branch_path, branch_id, log_dir):
    branch_start = time.time()
    branch_log_path = os.path.join(log_dir, f"branch_{branch_id}.log")
    file_handler = logging.FileHandler(branch_log_path, mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"[Branch {branch_id}] Starting processing")
    print(f"[Branch {branch_id}] Starting processing")

    elevation_raster_path = os.path.join(branch_path, f'rem_zeroed_masked_{branch_id}.tif')
    catchment_raster_path = os.path.join(
        branch_path, f'gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif'
    )
    catchment_gpkg_path = os.path.join(
        branch_path, f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg'
    )
    htable_path = os.path.join(branch_path, f'hydroTable_{branch_id}.csv')

    if not all(
        os.path.exists(path)
        for path in [elevation_raster_path, catchment_raster_path, catchment_gpkg_path, htable_path]
    ):
        logger.warning(f"[Branch {branch_id}] Skipping due to missing files.")
        print(f"[Branch {branch_id}] Skipping due to missing files.")
        return []

    htable_df = pd.read_csv(htable_path)

    with rasterio.open(elevation_raster_path) as elev_src, rasterio.open(catchment_raster_path) as catch_src:
        elev_data = elev_src.read(1)
        catchment_ids = catch_src.read(1)
        transform = elev_src.transform
        mask_invalid = (elev_data > 25.1) | (elev_data == elev_src.nodata)
        elevation = np.floor(elev_data * SCALE_FACTOR).astype(np.uint16)
        elevation[mask_invalid] = INT_NODATA_VALUE

    thresholds = (
        np.concatenate((np.arange(0, 12.5, 0.0762), np.arange(12.5, 25.0, 0.1524))) * SCALE_FACTOR
    ).astype(np.uint16)

    all_features = []
    with ThreadPoolExecutor() as executor:
        for threshold_features in executor.map(
            lambda thr: polygonize_combined_rasters(
                elevation, catchment_ids, transform, thr, branch_id, htable_df, logger
            ),
            thresholds,
        ):
            all_features.extend(threshold_features)

    logger.info(f"[Branch {branch_id}] Total time: {time.time() - branch_start:.2f} seconds")
    print(f"[Branch {branch_id}] Total time: {time.time() - branch_start:.2f} seconds")

    logger.removeHandler(file_handler)
    file_handler.close()

    return all_features


def main(fim_output_dir):
    start_time = time.time()
    log_dir = os.path.join(fim_output_dir, 'logs', 'polysrc_geoparquet_processing')
    os.makedirs(log_dir, exist_ok=True)

    branches_dirs = []
    for root, dirs, files in os.walk(fim_output_dir):
        if 'branches' in dirs:
            branches_dir = os.path.join(root, 'branches')
            for branch_id in os.listdir(branches_dir):
                branch_path = os.path.join(branches_dir, branch_id)
                if os.path.isdir(branch_path):
                    branches_dirs.append((branch_path, branch_id, log_dir))

    with Pool() as pool:
        results = pool.starmap(process_branch, branches_dirs)

    all_features = [feature for result in results if result for feature in result]
    gdf = gpd.GeoDataFrame.from_features(all_features)

    # columns_to_keep = ['geometry', 'rem', 'catchment_id', 'discharge_cms', 'volume_m3', 'branch_id', 'rem_ft', 'HydroID', 'SO', 'LakeID', 'feature_id', 'order_']
    # gdf = gdf[columns_to_keep]

    gdf['rem_ft'] = np.round(gdf['rem'] / 0.3048, 2)
    gdf.set_crs("EPSG:5070", inplace=True)
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.buffer(0) if not geom.is_valid else geom)
    gdf = gdf[gdf.is_valid]

    # Write single output file
    output_path = os.path.join(fim_output_dir, 'hand_geosrc_all_branches.parquet')
    gdf.to_parquet(output_path, index=False, engine='pyarrow', compression='lz4')
    logger.info(f"Saved combined output to {output_path}")

    # Write index
    index_df = gdf[['branch_id', 'HydroID', 'catchment_id', 'discharge_cms']].drop_duplicates()
    index_df.to_csv(os.path.join(fim_output_dir, 'hand_geosrc_index.csv'), index=False)

    logger.info(f"Total run time: {time.time() - start_time:.2f} seconds.")
    print(f"Total run time: {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process FIM output directories.')
    parser.add_argument('-i', '--fim_output_dir', type=str, help='Path to the FIM output directory.')
    args = parser.parse_args()
    main(args.fim_output_dir)
