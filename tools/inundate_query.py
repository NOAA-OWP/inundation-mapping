import os
import sys
import argparse
import pandas as pd
import geopandas as gpd
import numpy as np
import time
from multiprocessing import Pool, cpu_count

def find_closest_polygons_in_branch(branch_path, branch_id, feature_discharge_map, depth_calc=False):
    """Find the closest matching polygons for each HydroID in the parquet file based on discharge_cms.
    If depth_calc is True, also return polygons with rem_ft values that are 2, 4, 6, etc. feet less than the closest polygon."""
    parquet_path = os.path.join(branch_path, f'thresholded_catchment_polygons_{branch_id}.parquet')
    if not os.path.exists(parquet_path):
        return []
    else:
        print(f"Processing: {parquet_path}")

    gdf = gpd.read_parquet(parquet_path)
    discharge_ceiling_df = pd.DataFrame(feature_discharge_map.items(), columns=['feature_id', 'discharge'])
    merged_gdf = gdf.merge(discharge_ceiling_df, on='feature_id', how='inner', suffixes=('', '_ceiling'))
    merged_gdf['discharge_diff'] = np.abs(merged_gdf['discharge_cms'] - merged_gdf['discharge'])
    min_discharge_diff = merged_gdf.groupby(['HydroID'])['discharge_diff'].transform('min')
    closest_polygons = merged_gdf[merged_gdf['discharge_diff'] == min_discharge_diff]

    accumulated_polygons = []

    if depth_calc:
        for hydro_id, group in closest_polygons.groupby('HydroID'):
            closest_rem_ft = group['rem_ft'].max()
            group['depth_ft'] = 0
            accumulated_polygons.extend(group.to_dict('records'))

            for depth in range(2, int(closest_rem_ft) + 1, 2):
                target_rem_ft = closest_rem_ft - depth
                matching_polygons = merged_gdf[(merged_gdf['HydroID'] == hydro_id) & (merged_gdf['rem_ft'] == target_rem_ft)]

                if not matching_polygons.empty:
                    matching_polygons = matching_polygons.copy()
                    matching_polygons['depth_ft'] = depth
                    accumulated_polygons.extend(matching_polygons.to_dict('records'))
    else:
        accumulated_polygons.extend(closest_polygons.to_dict('records'))

    return accumulated_polygons

def process_branch(args):
    """Helper function to process a branch with multiprocessing."""
    branch_path, branch_id, feature_discharge_map, depth_calc = args
    return find_closest_polygons_in_branch(branch_path, branch_id, feature_discharge_map, depth_calc)

def main(flow_csv_path, fim_output_dir, output_gpkg_path, depth_calc=False):
    initial_time = time.time()
    flow_df = pd.read_csv(flow_csv_path)

    if 'feature_id' not in flow_df.columns or 'discharge' not in flow_df.columns:
        print("Error: Flow CSV must contain 'feature_id' and 'discharge' columns.")
        sys.exit(1)

    crosswalk_path = os.path.join(fim_output_dir, 'crosswalk_table.csv')
    if not os.path.exists(crosswalk_path):
        print("Error: crosswalk_table.csv not found in the specified FIM output directory.")
        sys.exit(1)

    crosswalk_df = pd.read_csv(crosswalk_path, dtype={'huc8': str, 'branch_id': str})
    merged_df = crosswalk_df.merge(flow_df, on='feature_id', how='left').drop_duplicates(subset=['feature_id', 'huc8', 'branch_id'])

    if merged_df['huc8'].isnull().any() or merged_df['branch_id'].isnull().any():
        print("Warning: Some feature_ids were not found in the crosswalk table and will be skipped.")
        merged_df = merged_df.dropna(subset=['huc8', 'branch_id'])

    branch_args = []
    for (huc, branch_id), group in merged_df.groupby(['huc8', 'branch_id']):
        branch_path = os.path.join(fim_output_dir, huc, 'branches', str(branch_id))
        feature_discharge_map = dict(zip(group['feature_id'], group['discharge']))
        branch_args.append((branch_path, branch_id, feature_discharge_map, depth_calc))

    with Pool(processes=cpu_count()-2) as pool:
        results = pool.map(process_branch, branch_args)

    accumulated_polygons = [polygon for result in results if result for polygon in result]

    if accumulated_polygons:
        result_gdf = gpd.GeoDataFrame(accumulated_polygons)
        result_gdf.set_crs('EPSG:5070', inplace=True)
        result_gdf.to_file(output_gpkg_path, driver='GPKG')
        print(f"Exported polygons to {output_gpkg_path}")
    else:
        print("No polygons were found or matched.")

    print(f"Total run time: {time.time() - initial_time:.2f} seconds.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract flood polygons based on feature IDs and flow values.")
    parser.add_argument(
        '-i',
        '--fim_output_dir',
        type=str,
        help='Path to the FIM output directory.',
    )
    parser.add_argument(
        '-f',
        '--flow_csv_path',
        type=str,
        help='Path to the input CSV file containing flow data.',
    )
    parser.add_argument(
        '-o',
        '--output_gpkg_path',
        type=str,
        help='Path to the output GeoPackage file.',
    )
    parser.add_argument(
        '-d',
        '--depth',
        action='store_true',
        help='Include depth polygons calculation.',
    )

    args = parser.parse_args()
    flow_csv_path = args.flow_csv_path
    fim_output_dir = args.fim_output_dir
    output_gpkg_path = args.output_gpkg_path
    depth_calc = args.depth

    main(flow_csv_path, fim_output_dir, output_gpkg_path, depth_calc)
