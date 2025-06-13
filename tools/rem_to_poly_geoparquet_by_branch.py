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
from scipy.ndimage import label
from shapely.geometry import shape


# Set scale factor for converting float elevation to integer
SCALE_FACTOR = 1000
INT_NODATA_VALUE = 32767  # max of int16 for no data handling

# Setup logger
logger = logging.getLogger("flood_processing")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def timed_step(step_name, logger=None):
    """
    A context manager for timing and logging code blocks.
    """

    class Timer:
        def __enter__(self):
            self.start = time.time()
            return self

        def __exit__(self, *args):
            self.end = time.time()
            self.elapsed = self.end - self.start
            msg = f"{step_name} took {self.elapsed:.2f} seconds."
            if logger:
                logger.info(msg)
            print(msg)

    return Timer()


# Function to interpolate discharge based on stage value and HydroID
def interpolate_discharge(rem_value, hydro_id, htable_df):
    src_data = htable_df[htable_df['HydroID_join'] == hydro_id]

    if len(src_data) > 1:
        discharge = np.interp(rem_value, src_data['stage'], src_data['discharge_cms'])
        volume_m3 = np.interp(rem_value, src_data['stage'], src_data['Volume (m3)'])
    else:
        discharge = np.nan
        volume_m3 = np.nan

    return discharge, volume_m3


# Function to generate polygons from the combined elevation and catchment data below the threshold
def polygonize_combined_rasters(elevation, catchment_ids, transform, threshold, branch_id, htable_df, logger):
    threshold_start = time.time()
    true_rem = threshold / SCALE_FACTOR

    # Build mask: elevation < threshold and elevation is not NoData
    mask = (elevation < threshold) & (elevation != INT_NODATA_VALUE)

    # Pre-mask: assign catchment_id only where flooded, else set to 0
    if catchment_ids.dtype == 'int32':
        combined_mask = np.where(mask, catchment_ids, 0).astype(np.int32)
        # can also try int16, int32, uint8, uint16, float32, float64, int8
    else:
        combined_mask = np.where(mask, catchment_ids, 0).astype(np.int16)

    # Bounding box for valid (flooded) pixels
    valid_indices = np.argwhere(combined_mask > 0)
    if valid_indices.size == 0:
        logger.info(f"[Branch {branch_id}] Threshold {true_rem:.4f} has no flooded pixels.")
        return []

    min_row, min_col = valid_indices.min(axis=0)
    max_row, max_col = valid_indices.max(axis=0) + 1
    window = (slice(min_row, max_row), slice(min_col, max_col))

    combined_crop = combined_mask[window]
    mask_crop = combined_crop > 0

    # Label connected regions (connected pixels > 0)
    labeled, num_features = label(mask_crop)

    # Count pixels in each connected region (label)
    pixel_counts = np.bincount(labeled.ravel())

    # Build a mask of labels that have enough area (e.g., > 5 pixels)
    min_pixels = 3  # adjust based on cell size and minimum area threshold
    valid_labels = np.where(pixel_counts >= min_pixels)[0]

    # Mask out small components
    filtered_mask = np.isin(labeled, valid_labels) & (combined_crop > 0)

    # Calculate adjusted transform for the window
    transform_crop = rasterio.transform.from_origin(
        transform.c + min_col * transform.a, transform.f + min_row * transform.e, transform.a, -transform.e
    )

    features = []
    rasterize_start = time.time()

    try:
        for geom, value in shapes(combined_crop, mask=filtered_mask, transform=transform_crop):
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
    except Exception as e:
        logger.error(f"[Branch {branch_id}] Error processing threshold {true_rem:.4f}: {e}")
        return []

    rasterize_time = time.time() - rasterize_start
    total_time = time.time() - threshold_start

    logger.info(
        f"[Branch {branch_id}] Threshold {true_rem:.4f} processed: {valid_indices.shape[0]} flooded pixels --> "
        f"{len(features)} polygons | rasterize time: {rasterize_time:.2f}s | total: {total_time:.2f}s"
    )

    return features


def process_branch(branch_path, branch_id, log_dir, huc_id):
    branch_start = time.time()
    branch_log_path = os.path.join(log_dir, f"{huc_id}_{branch_id}.log")
    file_handler = logging.FileHandler(branch_log_path, mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"[Branch {branch_id}] Starting processing")
    print(f"[Branch {branch_id}] Starting processing")

    with timed_step(f"[Branch {branch_id}] Reading input files", logger):
        elevation_raster_path = os.path.join(branch_path, f'rem_zeroed_masked_{branch_id}.tif')
        catchment_raster_path = os.path.join(
            branch_path, f'gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif'
        )
        # no longer using catchment poly --> can remove this later
        catchment_gpkg_path = os.path.join(
            branch_path, f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg'
        )
        htable_path = os.path.join(branch_path, f'hydroTable_{branch_id}.csv')
        output_geoparquet_path = os.path.join(branch_path, f'hand_geosrc_{branch_id}.parquet')

        if not all(
            os.path.exists(path)
            for path in [elevation_raster_path, catchment_raster_path, catchment_gpkg_path, htable_path]
        ):
            logger.warning(f"[Branch {branch_id}] Skipping due to missing files.")
            return

        htable_df = pd.read_csv(htable_path)

    with timed_step(f"[Branch {branch_id}] Reading and preprocessing rasters", logger):
        with rasterio.open(elevation_raster_path) as elev_src, rasterio.open(
            catchment_raster_path
        ) as catch_src:
            elev_data = elev_src.read(1)
            catchment_ids = catch_src.read(1)
            transform = elev_src.transform

            # Check if the elevation raster is float32 before applying the scale factor
            # pre HAND 4.8 rem values are meters; 4.8+ rem values are millimeters
            if elev_src.dtypes[0] == "float32":
                mask_invalid = (elev_data > 25.1) | (elev_data == elev_src.nodata)
                elevation = np.floor(elev_data * SCALE_FACTOR).astype(np.int16)
            else:
                mask_invalid = (elev_data > 25001) | (elev_data == elev_src.nodata)
                elevation = elev_data.astype(np.int16)

            elevation[mask_invalid] = INT_NODATA_VALUE

    with timed_step(f"[Branch {branch_id}] Generating features", logger):
        # Combine coarse and fine threshold intervals
        low_range = np.arange(
            0.3048, 1.2192, 0.3048
        )  # 1ft interval for first 4ft (prone to long run times due to disjointed polys)
        mid_range = np.arange(
            1.2192, 12.192, 0.0762
        )  # 0.25ft interval for 4-40ft (most common for inundation - want resolution)
        high_range = np.arange(12.192, 25.0, 0.1524)  # 0.5ft for 40-82ft (uncommon extreme inundation)
        thresholds = (np.concatenate((low_range, mid_range, high_range)) * SCALE_FACTOR).astype(np.uint16)

        foot_range = np.arange(0.6096, 25.0, 0.3048) * SCALE_FACTOR
        thresholds = foot_range.astype(np.uint16)

        if catchment_ids.dtype == 'int16':
            htable_df["HydroID_join"] = htable_df["HydroID"].astype(str).str[-4:].astype(np.int16)
        else:
            htable_df["HydroID_join"] = htable_df["HydroID"]
        htable_df_interp = htable_df[['HydroID_join', 'stage', 'discharge_cms', 'Volume (m3)']]

        all_features = []
        with ThreadPoolExecutor() as executor:
            for threshold_features in executor.map(
                lambda thr: polygonize_combined_rasters(
                    elevation, catchment_ids, transform, thr, branch_id, htable_df_interp, logger
                ),
                thresholds,
            ):
                all_features.extend(threshold_features)

    with timed_step(f"[Branch {branch_id}] Post-processing features", logger):
        gdf = gpd.GeoDataFrame.from_features(all_features)
        gdf['rem_ft'] = np.round(gdf['rem'] / 0.3048, 2)

        if elev_src.crs:
            gdf.set_crs(elev_src.crs, inplace=True)
        else:
            gdf.set_crs("EPSG:4326", inplace=True)

        num_invalid = (~gdf.is_valid).sum()
        logger.info(f"[Branch {branch_id}] Dropping {num_invalid} invalid geometries.")
        gdf = gdf[gdf.is_valid]

        # Subset and deduplicate htable for join (retain one row per HydroID)
        htable_df_sub = htable_df[
            [
                'HydroID',
                'HydroID_join',
                'feature_id',
                'SLOPE',
                'HUC',
                'Bathymetry_source',
                'subdiv_applied',
                'channel_n',
                'overbank_n',
                'obs_source',
                'calb_coef_final',
                'calb_applied',
            ]
        ].drop_duplicates(subset='HydroID')

        gdf = gdf.merge(htable_df_sub, left_on='catchment_id', right_on='HydroID_join', how='left')
        ## Replaced below with variables from htable
        # catchment_gdf = gpd.read_file(catchment_gpkg_path)
        # catchment_gdf = catchment_gdf.drop(columns=['geometry', 'distance'])
        # gdf = gdf.merge(catchment_gdf, left_on="catchment_id", right_on="HydroID", how="left")

    with timed_step(f"[Branch {branch_id}] Saving output", logger):
        gdf = gdf.set_geometry("geometry")
        gdf.to_parquet(output_geoparquet_path, index=False)

    logger.info(f"[Branch {branch_id}] Saved output to {output_geoparquet_path}")
    logger.info(f"[Branch {branch_id}] Total time: {time.time() - branch_start:.2f} seconds")

    logger.removeHandler(file_handler)
    file_handler.close()


def main(fim_output_dir):
    start_time = time.time()

    log_dir = os.path.join(fim_output_dir, 'logs', 'polysrc_geoparquet_processing')
    os.makedirs(log_dir, exist_ok=True)

    branches_dirs = []
    for root, dirs, files in os.walk(fim_output_dir):
        if 'branches' in dirs:
            branches_dir = os.path.join(root, 'branches')
            huc_id = os.path.basename(root)
            for branch_id in os.listdir(branches_dir):
                branch_path = os.path.join(branches_dir, branch_id)
                if os.path.isdir(branch_path):
                    branches_dirs.append((branch_path, branch_id, log_dir, huc_id))

    with Pool() as pool:
        pool.starmap(process_branch, branches_dirs)

    logger.info(f"Total run time: {time.time() - start_time:.2f} seconds.")
    print(f"Total run time: {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process FIM output directories.')
    parser.add_argument('-i', '--fim_output_dir', type=str, help='Path to the FIM output directory.')
    args = parser.parse_args()
    main(args.fim_output_dir)
