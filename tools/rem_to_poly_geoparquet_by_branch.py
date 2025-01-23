import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape


# Function to interpolate discharge based on stage value and HydroID
def interpolate_discharge(rem_value, hydro_id, htable_df):
    src_data = htable_df[htable_df['HydroID'] == hydro_id]

    if len(src_data) > 1:
        # Perform linear interpolation
        discharge = np.interp(rem_value, src_data['stage'], src_data['discharge_cms'])
    else:
        discharge = np.nan  # If no data available for interpolation, return NaN

    return discharge


# Function to generate polygons from the combined elevation and catchment data below the threshold
def polygonize_combined_rasters(elevation, catchment_ids, transform, threshold, no_data_value, htable_df):
    start_time = time.time()

    # Create mask for areas below threshold and with valid elevation values
    mask = (elevation < threshold) & (elevation != no_data_value)
    combined_mask = np.where(mask, catchment_ids, 0).astype(catchment_ids.dtype)

    features = []
    for geom, value in shapes(combined_mask, mask=(combined_mask > 0), transform=transform):
        if value > 0:
            geom_shape = shape(geom)
            catchment_id = int(value)
            discharge_cms = interpolate_discharge(threshold, catchment_id, htable_df)

            features.append(
                {
                    'geometry': geom_shape,
                    'properties': {
                        'rem': threshold,
                        'catchment_id': catchment_id,
                        'discharge_cms': discharge_cms,
                    },
                }
            )

    print(f"Finished processing threshold {threshold} in {time.time() - start_time:.2f} seconds.")
    return features


# Function to process a single branch
def process_branch(branch_path, branch_id):
    print(f"Processing branch ID: {branch_id}")

    # Construct file paths
    elevation_raster_path = os.path.join(branch_path, f'rem_zeroed_masked_{branch_id}.tif')
    catchment_raster_path = os.path.join(
        branch_path, f'gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif'
    )
    catchment_gpkg_path = os.path.join(
        branch_path, f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg'
    )
    htable_path = os.path.join(branch_path, f'hydroTable_{branch_id}.csv')
    output_geoparquet_path = os.path.join(branch_path, f'hand_geosrc_{branch_id}.parquet')

    # Check if all required files exist
    if not all(
        os.path.exists(path)
        for path in [elevation_raster_path, catchment_raster_path, catchment_gpkg_path, htable_path]
    ):
        print(f"Skipping branch {branch_id} due to missing files.")
        return

    # Load the hydro table for discharge interpolation
    htable_df = pd.read_csv(htable_path)

    # Load rasters and initialize timing
    with rasterio.open(elevation_raster_path) as elev_src, rasterio.open(catchment_raster_path) as catch_src:
        elevation = elev_src.read(1)
        no_data_value = elev_src.nodata
        catchment_ids = catch_src.read(1)
        transform = elev_src.transform

        # Round the elevation values to 3 decimal places and cast to float16
        # FUTURE: review options for optimial data type (int23, float32, etc.)
        elevation = np.round(elevation, 3).astype(np.float16)

        # Pre-filter elevation raster to ignore values above 25.0
        elevation = np.where(elevation > 25.0, no_data_value, elevation)

    all_features = []

    # Threshold values
    thresholds = np.concatenate((np.arange(0, 12.5, 0.0762), np.arange(12.5, 25.0, 0.1524)))

    # Process each threshold in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        for threshold_features in executor.map(
            lambda thr: polygonize_combined_rasters(
                elevation, catchment_ids, transform, thr, no_data_value, htable_df
            ),
            thresholds,
        ):
            all_features.extend(threshold_features)

    # Convert features to a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(all_features)
    gdf['rem_ft'] = np.round(gdf['rem'] / 0.3048, 2)

    # Set CRS with validation
    if elev_src.crs:
        gdf.set_crs(elev_src.crs, inplace=True)
    else:
        gdf.set_crs("EPSG:4326", inplace=True)

    # Attempt to fix invalid geometries using buffer(0)
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.buffer(0) if not geom.is_valid else geom)

    # Ensure valid geometries before exporting
    gdf = gdf[gdf.is_valid]

    # Read catchment attributes from GeoPackage
    catchment_gdf = gpd.read_file(catchment_gpkg_path)
    catchment_gdf = catchment_gdf.drop(columns='geometry')

    # Join attributes using 'catchment_id' in gdf and 'HydroID' in catchment_gdf
    gdf = gdf.merge(catchment_gdf, left_on="catchment_id", right_on="HydroID", how="left")

    # Save the final GeoDataFrame with joined attributes as a GeoParquet file
    gdf.to_parquet(output_geoparquet_path, index=False)
    print(f"Saved output for branch ID {branch_id} to {output_geoparquet_path}")


# Main function
def main(fim_output_dir):
    # Start overall timing
    initial_time = time.time()
    # Identify all "branches" directories
    branches_dirs = []
    for root, dirs, files in os.walk(fim_output_dir):
        if 'branches' in dirs:
            branches_dir = os.path.join(root, 'branches')
            for branch_id in os.listdir(branches_dir):
                branch_path = os.path.join(branches_dir, branch_id)
                if os.path.isdir(branch_path):
                    branches_dirs.append((branch_path, branch_id))

    # Use multiprocessing to process multiple branches concurrently
    with Pool() as pool:
        pool.starmap(process_branch, branches_dirs)

    print(f"Total run time: {time.time() - initial_time:.2f} seconds.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process FIM output directories.')
    parser.add_argument('-i', '--fim_output_dir', type=str, help='Path to the FIM output directory.')
    args = parser.parse_args()

    main(args.fim_output_dir)
