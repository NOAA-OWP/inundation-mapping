import argparse
import os
import time

import geopandas as gpd
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats


def load_huc12(wbd, ratio_file_path):
    """
    Load HUC12 and merge it with flood watch ratio file, returning a GPKG.

    Parameters:
        - wbd (str): Path to the HUC12 file.
        - ratio_file_path (str): Path to the CSV file containing ratio data for an event.
    Returns:
        - surface_area_gdf (gdf.GeoDataFrame).
    """
    # Read Flood Watch file
    try:
        surface_areas = pd.read_csv(ratio_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f'ratio file not found: {ratio_file_path}')

    # HUC12 data
    huc12 = gpd.read_parquet(wbd, columns=['HUC12', 'geometry'])
    huc12['HUC12'] = huc12['HUC12'].astype('int64')
    if huc12.crs != 'EPSG:5070':
        huc12 = huc12.to_crs('EPSG:5070')

    surface_areas_huc12 = surface_areas.merge(huc12, on='HUC12', how='left')
    surface_areas_gdf = gpd.GeoDataFrame(surface_areas_huc12, geometry='geometry', crs='EPSG:5070')

    return surface_areas_gdf


def building_layer(surface_areas_gdf, buildings_file_path, output_dir):
    """
    This function processes impacted building data within HUC to compute a density metric and
    flood watch layer based on impacted buildings.

     Parameters:
        - surface_area_gdf (gdf.GeoDataFrame): GeoDataFrame with HUC12 and ratio.
        - buildings_file_path (str): Path to the GPKG impacted building.
        - output_dir (str): Path to save the output GPKG file.
    """

    # Impacted buildings
    buildings = gpd.read_file(buildings_file_path).to_crs('EPSG:5070')

    # Remove duplicate buildings
    buildings = buildings.drop_duplicates(subset='build_id', keep='first')
    matched_buildings = gpd.sjoin(buildings, surface_areas_gdf, predicate='intersects')
    # Count buildings per HUC12
    buildings_count = matched_buildings.groupby('HUC12').size().reset_index(name='building_count')
    aggregate_final = surface_areas_gdf.merge(buildings_count, on=['HUC12'], how='left').fillna(0)
    # Calculate density
    density = aggregate_final['building_count'] + (
        aggregate_final['building_count'] * (1 / (1 + np.exp(-5 * (aggregate_final['ratio'] - 1))))
    )
    aggregate_final.insert(4, 'density', density)

    os.makedirs(os.path.dirname(output_dir), exist_ok=True)
    aggregate_final.to_file(output_dir, layer='buildings', index=False, driver='GPKG')


def land_use(nbm_flow_path, huc_file_path, fim_dir, surface_areas_gdf, output_dir):
    """
    This function creates flood watch layer based on NLCD data.

    Parameters:
        - nbm_flow_path (str): Path to the CSV file containing NBM high flow data.
        - huc_file_path (str): Path to the text file containing the list of impacted HUCs.
        - fim_dir (str): Directory path to FIM hydrofabric by processing unit.
        - surface_area_gdf (gdf.GeoDataFrame): GeoDataFrame with HUC12 and ratio.
        - output_dir (str): Path to save the output GPKG file.

        Note:
            To identify all HUCs impacted by an event, perform a spatial join between the max_high_flow_magnitude layer
            and the HUC8 boundaries.
    """

    srcDir = os.getenv('srcDir')
    load_dotenv(f'{srcDir}/bash_variables.env')
    input_nlcd = os.getenv('input_nlcd')
    catchments_to_huc12 = os.getenv('input_catchments_to_huc12')
    input_huc12_landuse = os.getenv('input_huc12_landuse')

    nbm_high_flow = pd.read_csv(nbm_flow_path)
    nbm_df_bflows = nbm_high_flow.rename(columns={'discharge': 'high_flow_nbm'})

    # Read the list of HUCs from the text file
    with open(huc_file_path, 'r') as f:
        huc_list = [line.strip() for line in f]
    total_hucs = len(huc_list)

    # Find flooded catchments
    all_huc = []
    for i, huc in enumerate(huc_list, start=1):
        print(f'Processing HUC {huc} ({i}/{total_hucs})')
        hydrotable_path = f'{fim_dir}/{huc}/hydrotable.csv'

        if not os.path.exists(hydrotable_path):
            print(f'skipping HUC {huc}, hydrotable not found')
            continue
        hydrotable = pd.read_csv(hydrotable_path, low_memory=False)
        df_src = hydrotable.merge(nbm_df_bflows, how='left', on='feature_id')
        df_src = df_src.dropna(subset=['high_flow_nbm'])

        all_cat = []
        for branch in df_src['branch_id'].unique():
            catch = gpd.read_file(
                f'{fim_dir}/{huc}/branches/{branch}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch}.gpkg'
            )
            per_branch = df_src[df_src['branch_id'] == branch]
            common_cat = catch[catch['HydroID'].isin(per_branch['HydroID'])].reset_index(drop=True)
            common_cat['branch_id'] = branch
            all_cat.append(common_cat)
        if all_cat:
            common_cat = pd.concat(all_cat, axis=0, ignore_index=True)
            all_huc.append(common_cat)
    if all_huc:
        common_cat = pd.concat(all_huc, axis=0, ignore_index=True)

    # Read NLCD data and reproject if needed
    with rasterio.open(input_nlcd) as nlcd_raster:
        if common_cat.crs != nlcd_raster.crs:
            common_cat = common_cat.to_crs(nlcd_raster.crs)
        stats = zonal_stats(common_cat, input_nlcd, categorical=True, nodata=250)
        nlcd_classes = {
            11: 'open water',
            12: 'Ice/Snow',
            21: 'Open Space',
            22: 'Low Intensity',
            23: 'Medium Intensity',
            24: 'High Intensity',
            31: 'Barren Land',
            41: 'Deciduous Forest',
            42: 'Evergreen Forest',
            43: 'Mixed Forest',
            52: 'Shrub/Scrub',
            71: 'Grassland',
            81: 'Pasture',
            82: 'Cultivated Crops',
            90: 'Woody Wetlands',
            95: 'Emergent Wetlands',
        }

        # Initialize columns for each NLCD class
        for code in nlcd_classes.keys():
            common_cat[f'{int(code)}'] = 0
        for idx, stat in enumerate(stats):
            total_pixels = sum(stat.values()) if stat else 0
            common_cat.at[idx, 'Flooded_total'] = total_pixels
            if total_pixels > 0:
                for code in nlcd_classes.keys():
                    common_cat.at[idx, f'{int(code)}'] = stat.get(code, 0)

    # Load catchment huc12 lookup file
    catchment_huc12 = pd.read_csv(catchments_to_huc12, dtype={'HUC12': 'string'})
    cat_land_huc12 = common_cat.merge(
        catchment_huc12[['HydroID', 'feature_id', 'HUC12', 'branch_id']],
        on=['HydroID', 'feature_id', 'branch_id'],
        how='left',
    )

    # Define grouped land use categories
    grouped = {
        'Flooded_urban': [21, 22, 23, 24],
        'Flooded_ag': [81, 82],
        'Flooded_forest': [41, 42, 43],
        'Flooded_wetlands': [90, 95],
        'Flooded_water': [11],
        'Flooded_barren': [31],
        'Flooded_shrubland': [52],
        'Flooded_herbaceous': [71],
    }
    for group in grouped.keys():
        cat_land_huc12.loc[:, f'{group}'] = 0
    for group_name, codes in grouped.items():
        area_column = [f'{code}' for code in codes]
        for col in area_column:
            if col in cat_land_huc12.columns:
                cat_land_huc12.loc[:, f'{group_name}'] += cat_land_huc12[col].fillna(0)

    # Define weights for land use classes
    weights = {
        'urban': 1,
        'ag': 0.6,
        'forest': 0.3,
        'wetlands': 0.05,
        'water': 0.05,
        'barren': 0.2,
        'shrubland': 0.3,
        'herbaceous': 0.3,
    }

    # Calculate weighted average score
    cat_land_huc12['score'] = sum(cat_land_huc12[f'Flooded_{k}'] * v for k, v in weights.items()) / sum(
        weights.values()
    )
    cat_land_huc12 = cat_land_huc12.dropna(subset=['Flooded_total', 'score'])

    # Aggregate data by HUC12, calculating mean for land use categories and score
    agg_funcs = {col: 'mean' for col in grouped.keys()}
    agg_funcs.update({'feature_id': 'first', 'Flooded_total': 'mean', 'score': 'mean'})
    aggregation_nlcd_huc12_sum = cat_land_huc12.groupby('HUC12').agg(agg_funcs).reset_index()

    # Load flood watch ratio data
    surface_areas_gdf['HUC12'] = surface_areas_gdf['HUC12'].astype(str).str.strip()
    surface_areas_gdf['HUC12'] = surface_areas_gdf['HUC12'].str.zfill(12)

    aggregation_nlcd_huc12_sum['HUC12'] = aggregation_nlcd_huc12_sum['HUC12'].astype(str).str.strip()
    aggregation_nlcd_huc12_sum['HUC12'] = aggregation_nlcd_huc12_sum['HUC12'].str.zfill(12)

    # Filter data to include only HUC12 present in flood watch ratio data
    hucs_flood_watch = aggregation_nlcd_huc12_sum[
        aggregation_nlcd_huc12_sum['HUC12'].isin(surface_areas_gdf['HUC12'])
    ].reset_index(drop=True)
    # Merge with ratio data
    merged_huc_fw = surface_areas_gdf.merge(hucs_flood_watch, on='HUC12', how='right')
    # Calculate density based on score and ratio
    merged_huc_fw['density_nlcd'] = merged_huc_fw['score'] + (
        merged_huc_fw['score'] * (1 / (1 + np.exp(-5 * (merged_huc_fw['ratio'] - 1))))
    )

    # Load landuse percentage data for HUC12
    percentage_per_huc = gpd.read_parquet(
        input_huc12_landuse,
        columns=[
            'HUC12',
            '%Urban',
            '%Agriculture',
            '%Forest',
            '%Wetlands',
            '%Water',
            '%Barren',
            '%Shrubland',
            '%Herbaceous',
            'geometry'
        ],
    )
    if 'geometry' in percentage_per_huc.columns:
        percentage_per_huc = percentage_per_huc.drop(columns=['geometry'])

    final = merged_huc_fw.merge(percentage_per_huc, on='HUC12', how='left').drop_duplicates(
        subset='HUC12', keep='first'
    )

    os.makedirs(os.path.dirname(output_dir), exist_ok=True)

    final.to_file(output_dir, layer='landuse', index=False, driver='GPKG')


def infrastructure_layer(infrastructure_file, inundation_file, surface_area_gdf, output_dir):
    """
    Processes critical infrastructure data for multiple BUILD_TYPE groups.

    Parameters:
    - infrastructure_file (str): Path to the GeoPackage containing infrastructure data with BUILD_TYPE.
    - inundation_file (str): Path to the GeoPackage containing inundation layer data.
    - surface_area_gdf (gdf.GeoDataFrame): GeoDataFrame with HUC12 and ratio.
    - output_dir (str): Path to save the output GeoPackage file.
    """
    try:
        infrastructure = gpd.read_file(infrastructure_file)
    except Exception as e:
        raise FileNotFoundError(f"Failed to read infrastructure file: {infrastructure_file}, error: {str(e)}")

    if not {'BUILD_TYPE', 'ADDRESS', 'geometry'}.issubset(infrastructure.columns):
        raise KeyError("Infrastructure file must contain 'BUILD_TYPE', 'ADDRESS', and 'geometry' columns")
    # print(f"Infrastructure file loaded with {len(infrastructure)} features")
    # print(f"Unique BUILD_TYPE values: {infrastructure['BUILD_TYPE'].unique().tolist()}")

    try:
        inun_layer = gpd.read_file(inundation_file).to_crs('EPSG:5070')
    except Exception as e:
        raise FileNotFoundError(f"Failed to read inundation file: {inundation_file}, error: {str(e)}")
    if 'geometry' not in inun_layer.columns:
        raise KeyError("Inundation file must contain 'geometry' column")

    surface_area_gdf['HUC12'] = surface_area_gdf['HUC12'].astype(str).str.strip()
    surface_area_gdf['HUC12'] = surface_area_gdf['HUC12'].str.zfill(12)

    # Define BUILD_TYPE groups
    build_types = infrastructure['BUILD_TYPE'].unique()

    # Create output directory
    os.makedirs(os.path.dirname(output_dir), exist_ok=True)

    # Process each BUILD_TYPE
    for build_type in build_types:
        print(f"Processing BUILD_TYPE: {build_type}")
        infra_type = infrastructure[infrastructure['BUILD_TYPE'] == build_type].to_crs('EPSG:5070')

        # Intersect with inundation layer
        intersecting_infra = gpd.sjoin(infra_type, inun_layer, predicate='intersects', how='inner')

        # Drop unnecessary columns if they exist
        columns_to_drop = ['index_right']
        if 'name' in intersecting_infra.columns:
            columns_to_drop.append('name')
        intersecting_infra = intersecting_infra.drop(
            columns=[col for col in columns_to_drop if col in intersecting_infra.columns]
        )

        # Remove duplicates based on ADDRESS
        intersecting_infra = intersecting_infra.drop_duplicates(subset='ADDRESS', keep='first')
        # Intersect with flood watch data
        infra_fw = gpd.sjoin(surface_area_gdf, intersecting_infra, predicate='intersects', how='inner')

        # Count buildings per HUC12
        infra_counts = infra_fw.groupby('HUC12').size().reset_index(name='building_count')

        # Merge counts with flood watch data
        infra_flood_watch = infra_counts.merge(
            surface_area_gdf[['HUC12', 'SurfaceArea_nbm', 'SurfaceArea_nrp', 'ratio', 'geometry']],
            on='HUC12',
            how='left',
        )

        # Calculate density
        infra_flood_watch['density'] = infra_flood_watch['building_count'] + (
            infra_flood_watch['building_count'] * (1 / (1 + np.exp(-5 * (infra_flood_watch['ratio'] - 1))))
        )

        # Convert to GeoDataFrame
        infra_flood_watch = gpd.GeoDataFrame(infra_flood_watch, geometry='geometry', crs='EPSG:5070')

        # Save to output GeoPackage
        layer_name = build_type.lower().replace(' ', '_')  # e.g., 'SCHOOLS' -> 'schools'
        infra_flood_watch.to_file(output_dir, layer=layer_name, index=False, driver='GPKG')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one or more layer for Flood Watch.")

    # Task 1: Impacted building
    parser.add_argument(
        '-task1', '--task1', action='store_true', help='Run task 1: Create Impacted Buildings layer'
    )
    parser.add_argument('-task2', '--task2', action='store_true', help='Run task 2: Create NLCD layer')
    parser.add_argument(
        '-task3', '--task3', action='store_true', help='Run task 3: Create Critical Infrastructure layers'
    )
    parser.add_argument('-ratio', '--ratio_file_path', help="Path to flood watch ratio file for an event")
    parser.add_argument('-building', '--buildings_file_path', help="Path to impacted buildings")
    parser.add_argument('-wbd', '--wbd', help="Path to HUC12 gpkg")
    parser.add_argument('-o', '--output_dir', help="Path to save GPKG output file")
    parser.add_argument('-nbm', '--nbm_flow_path', help="path to NBM high flow csv")
    parser.add_argument('-huc_list', '--huc_file_path', help="Path to the text file containing list of HUCs")
    parser.add_argument('-d', '--fim_dir', help="Directory path to FIM hydrofabric by processing unit")
    parser.add_argument(
        '-infrastructure', '--infrastructure_file', help="Path to critical infrastructure gpkg"
    )
    parser.add_argument('-inundation', '--inundation_file', help="Path to inundation file for the event")
    parser.add_argument(
        '-inputs', '--help-tasks', action='store_true', help="show detailed input requirements for each task"
    )
    args = parser.parse_args()

    start_total_time = time.time()
    if args.help_tasks:
        print(
            """
              Required inputs for
              - Task1, Impacted buildings:
                    -ratio, -building, -wbd, -o
              Task2, landuse:
                    -nbm, -huc_list, -d, -ratio, -wbd, -o
              Task3, Critical Infrastructures:
                    -infrastructure, -inundation, -ratio, -o, -wbd
              Notes:
                - Use -task1 and/or task2 and/or task3 to run specific tasks.
                - If running all tasks with the same -o, outputs are saved as separate layers.
                - Ensure all file paths are accessible and have correct formats """
        )
        exit(0)

    # Load HUC12 and flood watch ratio data if any tasks requiring them is selected
    surface_area_gdf = None
    timing_summary = []
    if args.task1 or args.task2 or args.task3:
        if not args.ratio_file_path or not args.wbd:
            raise ValueError("Tasks require -ratio and -wbd")
        surface_area_gdf = load_huc12(args.wbd, args.ratio_file_path)
        timing_summary.append(('Data loading', (time.time() - start_total_time) / 60))
    if args.task1:
        if not all([args.buildings_file_path or not args.output_dir]):
            raise ValueError("Task1 requires -building, -o")
        start_time = time.time()
        building_layer(surface_area_gdf, args.buildings_file_path, args.output_dir)
        task_time = (time.time() - start_time) / 60
        print(f'Task1 took {task_time: .2f} minutes')
        timing_summary.append(('Task 1', task_time))
    if args.task2:
        if (
            not args.nbm_flow_path
            or not args.huc_file_path
            or not args.fim_dir
            or not args.output_dir
        ):
            raise ValueError(
                "Task2 requires -nbm, -huc_list, -d, -ratio, -o, -wbd"
            )
        start_time = time.time()
        land_use(
            args.nbm_flow_path,
            args.huc_file_path,
            args.fim_dir,
            surface_area_gdf,
            args.output_dir,
        )
        task_time = (time.time() - start_time) / 60
        print(f'Task2 took {task_time: .2f} minutes')
        timing_summary.append(('Task 2', task_time))
    if args.task3:
        if not args.infrastructure_file or not args.inundation_file or not args.output_dir:
            raise ValueError("Task3 requires -infrastructure, -inundation, -ratio, -o, -wbd")
        start_time = time.time()
        infrastructure_layer(
            args.infrastructure_file, args.inundation_file, surface_area_gdf, args.output_dir
        )
        task_time = (time.time() - start_time) / 60
        print(f'Task3 took {task_time: .2f} minutes')
        timing_summary.append(('Task 3', task_time))
    if not (args.task1 or args.task2 or args.task3):
        print("No tasks selected. Use -task1 or -task2 or -task3")
    # Print time
    total_time = (time.time() - start_total_time) / 60
    print('\n=== Timing Summary ===')
    for task_name, task_time in timing_summary:
        print(f'{task_name}: {task_time: .2f} minutes')
    print(f'Total execuation time: {total_time: .2f} minutes')
