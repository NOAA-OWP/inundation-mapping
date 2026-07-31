"""
This script calculates flood depth for geometries (e.g., roads, points, or buildings) for a given flow file.
Input:
     A geopackage containing desired geometries.The gpkg file can have any projection.

Workflow:
    1. For each input geometry, the script first identifies all intersecting HUCs from the available HUCs in a given FIM run.
      Geometries that span multiple HUCs are then processed independently within each HUC, and the most conservative (maximum)
        flood depth across all relevant HUCs is ultimately retained.

    2. Extract threshold HAND values: For each geometry segment within a HUC, the script identifies all branches that intersect
        that geometry. For each intersecting branch, it computes the minimum HAND value along the portion of the geometry that overlaps
        that branch, yielding one threshold_hand value per branch. All intersecting branches within the HUC are processed in this way.
        The output includes:
        - threshold_hand: Minimum HAND elevation (meters)
        - HydroID
        - feature_id
        - branch: Branch identifier within the HUC

    3. Interpolate threshold discharge: Using the HydroTable for each branch, interpolate the discharge
       value corresponding to each threshold_hand. This represents the flow rate at which flooding
       would begin. Geometries with threshold_hand > 25m are excluded as they exceed HydroTable limits.

    4. Determine inundation status:  The `evaluated discharge` from the input flow file is compared to the
    `threshold_discharge`. A geometry is marked as inundated if `evaluated_discharge > threshold_discharge`.

    5. Compute flood depth: The evaluated stage is obtained by interpolating within the branch-specific
      HydroTables using the corresponding evaluated discharge values. Flood depth is computed as:
        flood_depth = evaluated_stage - threshold_hand.
        At this time, any negative flood depths (which may occur due to non-monotonic SRC behavior) are set to zero.

Output
    - A gpkg file containing geometries along with their flooding status (Y or N) and the computed flood depths.
    - For geometries intersecting multiple HUCs or branches, the maximum flood depth across all intersections is reported.
    - Geometries that do not intersect any HUCs will have NULL for all output fields.
"""

import argparse
import os
import re
import traceback
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats

from src.heal_bridges_osm import flow_lookup
from src.process_roads_fimpact import min_hand_excluding_zero
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger
from tools.road_inundation import stage_lookup


# Constants
MAX_HAND_THRESHOLD_M = 25  # Maximum HAND value in HydroTable (meters)
METERS_TO_FEET = 3.28084
CMS_TO_CFS = 35.3147
MM_TO_METERS = 1000


def add_imperial_units(final_result_gdf):
    for col in ['threshold_discharge', 'evaluated_discharge']:
        final_result_gdf.loc[:, f'{col}_cfs'] = final_result_gdf[col] * CMS_TO_CFS

    for col in ['threshold_hand', 'evaluated_stage', 'flood_depth']:
        final_result_gdf.loc[:, f'{col}_ft'] = final_result_gdf[col] * METERS_TO_FEET

    return final_result_gdf


def get_evaluated_stage(fim_path, huc, branch, fimpact_df):
    """
    Calculate evaluated stage for geometries corresponding to evaluated discharge.

    Args:
        fim_path: Path to FIM outputs directory
        huc:
        branch: Branch identifier within the HUC
        fimpact_df: DataFrame with evaluated discharge values

    Returns:
        DataFrame with evaluated_stage column added
    """
    fimpact_df['evaluated_stage'] = np.nan

    hydrotable_path = os.path.join(fim_path, huc, 'branches', branch, f'hydroTable_{branch}.csv')
    hydrotable_df = pd.read_csv(
        hydrotable_path,
        dtype={'HydroID': str, 'stage': float, 'discharge_cms': float},
        usecols=['HydroID', 'discharge_cms', 'stage'],
    )

    for _, row in fimpact_df.iterrows():
        hydro_data = hydrotable_df[hydrotable_df.HydroID == row.HydroID]
        evaluated_stage = stage_lookup(
            row.evaluated_discharge, hydro_data['discharge_cms'], hydro_data['stage']
        )
        fimpact_df.at[row.name, 'evaluated_stage'] = evaluated_stage

    return fimpact_df


def compute_depth(fim_path, huc, branch, fimpact_df, flow_file_df):
    """
    - Identify flooded objects by comparing evaluated flow vs threshold flow
    - Compute flood depth by comparing evaluated stage against threshold hand.
    - Keep the non-inundated objects in the final output with flood depth of zero


    Args:
        fim_path: Path to FIM outputs directory
        huc:
        branch: Branch identifier within the HUC
        fimpact_df : DataFrame with threshold hand and discharge values
        flow_file_df: DataFrame with feature_id and discharge columns

    Returns:
        DataFrame containing only flooded geometries with flood_depth column
    """
    fimpact_df = fimpact_df.merge(flow_file_df, on='feature_id')
    fimpact_df.rename(columns={'discharge': 'evaluated_discharge'}, inplace=True)

    # Calculate evaluated stage for geometries
    fimpact_df = get_evaluated_stage(fim_path, huc, branch, fimpact_df)

    flooded_status_bool = fimpact_df['evaluated_discharge'] > fimpact_df['threshold_discharge']
    fimpact_df['flooded'] = np.where(flooded_status_bool, 'Y', 'N')

    # Calculate flood depth
    fimpact_df['flood_depth'] = fimpact_df['evaluated_stage'] - fimpact_df['threshold_hand']

    # convert negative flood depths (can occur due to non-monotonic SRC) to zero
    # do not remove non-inundated objects
    fimpact_df['flood_depth'] = np.where(fimpact_df['flood_depth'] > 0, fimpact_df['flood_depth'], 0)

    return fimpact_df


def get_threshold_discharge(fim_path, huc, branch, fimpact_df):
    """
    Calculate threshold discharge from threshold HAND values using HydroTable interpolation.

    Args:
        fim_path: Path to FIM outputs directory
        huc:
        branch: Branch identifier within the HUC
        fimpact_df : DataFrame with threshold_hand values

    Returns:
        DataFrame with threshold_discharge column added, or None if no valid records
    """
    hydrotable_path = os.path.join(fim_path, huc, 'branches', branch, f'hydroTable_{branch}.csv')
    hydrotable_df = pd.read_csv(
        hydrotable_path,
        dtype={'HydroID': str, 'stage': float, 'discharge_cms': float},
        usecols=['HydroID', 'stage', 'discharge_cms'],
    )

    # Filter out geometries with threshold_hand exceeding HydroTable limits
    fimpact_df = fimpact_df[fimpact_df['threshold_hand'] <= MAX_HAND_THRESHOLD_M].copy()

    if fimpact_df.empty:
        return None

    fimpact_df.loc[:, 'threshold_discharge'] = fimpact_df.apply(
        lambda row: flow_lookup(row.threshold_hand, row.HydroID, hydrotable_df), axis=1
    )

    return fimpact_df


def get_threshold_hand(fim_path, huc, branch, huc_geometries_gdf, file_logger, screen_queue, task_id):
    """
    Extract threshold HAND values for input geometries from HAND grids.

    Args:
        fim_path: Path to FIM outputs directory
        huc:
        branch: Branch identifier within the HUC
        huc_geometries_gdf: GeoDataFrame with geometries for this HUC

    Returns:
        DataFrame with threshold_hand, HydroID, feature_id, and branch columns
    """
    hand_grid_path = os.path.join(fim_path, huc, 'branches', branch, f'rem_zeroed_masked_{branch}.tif')
    catchments_path = os.path.join(
        fim_path,
        huc,
        'branches',
        branch,
        f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch}.parquet',
    )

    with rasterio.open(hand_grid_path, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    # Read catchments to split geometries by HydroID
    catchments_df = gpd.read_file(catchments_path, columns=['HydroID', 'feature_id', 'order_', 'geometry'])

    # Ensure IDs are strings for consistency
    catchments_df['feature_id'] = catchments_df['feature_id'].astype(int).astype(str)
    catchments_df['HydroID'] = catchments_df['HydroID'].astype(int).astype(str)

    # Split geometries based on catchment boundaries
    huc_geometries_split = gpd.overlay(huc_geometries_gdf, catchments_df, how="intersection")

    # Explode to handle jagged geometries from overlay operation
    huc_geometries_split = huc_geometries_split.explode(index_parts=True).reset_index(drop=True)

    if huc_geometries_split.empty:
        file_logger.warning(f'No geometries found in huc {huc} branch {branch}')
        return None

    huc_geometries_split['branch'] = branch

    # Calculate minimum HAND value for each geometry segment
    stats = zonal_stats(
        huc_geometries_split['geometry'],
        hand_grid_array,
        affine=hand_grid_profile['transform'],
        nodata=hand_grid_profile["nodata"],
        all_touched=True,
        stats=[],
        add_stats={"min_ex0": min_hand_excluding_zero},
    )

    huc_geometries_split.loc[:, 'threshold_hand'] = [x.get('min_ex0') for x in stats]

    # Convert from mm to meters (REM units changed in FIM pipeline)
    huc_geometries_split['threshold_hand'] = huc_geometries_split['threshold_hand'] / MM_TO_METERS

    # Remove geometries crossing NoData areas (e.g., levees)
    huc_geometries_split = huc_geometries_split.dropna(subset=['threshold_hand'])

    # Drop geometry for performance (will be re-merged with original geometries later)
    huc_geometries_split = huc_geometries_split.drop(columns='geometry')

    # Keep minimum threshold_hand for each geometry-HydroID combination
    min_idx = huc_geometries_split.groupby(['geometry_unique_id', 'HydroID'])['threshold_hand'].idxmin()
    fimpact_df = huc_geometries_split.loc[min_idx]

    # Ensure IDs are strings for output consistency
    cols_to_str = ['HydroID', 'feature_id', 'branch']
    fimpact_df[cols_to_str] = fimpact_df[cols_to_str].astype(str)

    return fimpact_df


def find_intersecting_hucs(fim_path, geometries_gdf):
    """
    Identify which HUCs intersect with input geometries.

    For each geometry, determines all intersecting HUCs in the FIM outputs. If a geometry
    crosses multiple HUCs, separate records are created for each HUC.

    Args:
        fim_path: Path to FIM outputs directory
        geometries_gdf: GeoDataFrame with input geometries

    Returns:
        GeoDataFrame with HUC and original_crs columns added
    """
    print("Identifying intersected HUCs for each geometry")

    # Find all HUC directories
    hucs = [huc for huc in os.listdir(fim_path) if re.match(r'\d{8}', huc)]

    # Load HUC boundaries
    huc_boundaries_list = []
    for huc in hucs:
        huc_boundary_gdf = gpd.read_file(os.path.join(fim_path, huc, 'wbd8_clp.gpkg'))
        huc_boundary_gdf["HUC"] = huc
        huc_boundary_gdf["huc_crs"] = huc_boundary_gdf.crs.to_string()
        huc_boundary_gdf = huc_boundary_gdf.to_crs(4326)

        # Dissolve multi-polygon HUCs into single geometry
        huc_boundary_gdf = huc_boundary_gdf.dissolve().reset_index(drop=True)

        huc_boundaries_list.append(huc_boundary_gdf[["HUC", "geometry", "huc_crs"]])

    huc_boundaries_gdf = gpd.GeoDataFrame(pd.concat(huc_boundaries_list, ignore_index=True), crs="EPSG:4326")

    # Spatial join to find intersecting HUCs
    geometries_with_hucs_gdf = gpd.sjoin(
        geometries_gdf, huc_boundaries_gdf, how="inner", predicate="intersects"
    )
    geometries_with_hucs_gdf = geometries_with_hucs_gdf.drop(columns=["index_right"], errors="ignore")

    return geometries_with_hucs_gdf


def process_one_huc_branch(
    fim_path, unique_id, huc_geometries_gdf, flow_file_df, file_logger, screen_queue, task_id
):
    """
    Process a single HUC-branch combination to calculate flood depth.

    Args:
        fim_path: Path to FIM outputs directory
        unique_id: Unique identifier in format "HUC_branch"
        huc_geometries_gdf: GeoDataFrame with geometries for this HUC
        flow_file_df: DataFrame with feature_id and discharge columns
        file_logger: File Logger instance
        screen_queue: Screen logger instance
        task_id: Task identifier for logging

    Returns:
        Tuple of (success_code, [result_datafram])
    """
    file_logger.info(f"Started processing {task_id}")
    try:
        huc, branch = unique_id.split("_")

        fimpact_df = get_threshold_hand(
            fim_path, huc, branch, huc_geometries_gdf, file_logger, screen_queue, task_id
        )
        if fimpact_df is None:
            return 1, [None]

        fimpact_df = get_threshold_discharge(fim_path, huc, branch, fimpact_df)
        if fimpact_df is None:
            return 1, [None]

        fimpact_df = compute_depth(fim_path, huc, branch, fimpact_df, flow_file_df)

        return 1, [fimpact_df]

    except Exception as e:
        file_logger.error(f"Exception in {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        screen_queue.put(f"Exception in {task_id}")
        return 0, [None]


def flood_depth_main(
    fim_run_dir: str, flow_file: str, geometry_file: str, output_file_path: str, max_workers: int = 8
):
    """
    Compute flood depth for input geometries using FIM outputs and a flow file.

    Args:
        fim_run_dir: Path to FIM outputs directory
        flow_file: Path to CSV flow file with 'feature_id' and 'discharge' columns
        geometry_file: Path to input geometry file (e.g., roads geopackage)
        output_file_path: Path to output geopackage file
        max_workers: Number of parallel workers for multiprocessing (default: 8)
    """
    # Validate output file extension
    if not output_file_path.lower().endswith('.gpkg'):
        raise ValueError("Output file must have a .gpkg extension.")

    # Create output directory if needed
    output_dir = os.path.dirname(output_file_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Setup logging
    log_file_path = os.path.join(output_dir, "flood_depth.log")
    file_logger = setup_mp_file_logger(log_file_path, logger_name='depth_logger')
    print('Started flood depth analysis')
    file_logger.info('Started flood depth analysis')

    # Read geometry data, make unique id, and manage crs
    geometries_gdf = gpd.read_file(geometry_file)[['geometry']]
    geometries_gdf["geometry_unique_id"] = range(1, len(geometries_gdf) + 1)

    # Reproject to WGS84 for spatial join
    geoms_original_crs = geometries_gdf.crs.to_string()
    if geometries_gdf.crs != 'EPSG:4326':
        geometries_gdf = geometries_gdf.to_crs(4326)

    flow_file_df = pd.read_csv(flow_file, dtype={'feature_id': str})

    # Find intersecting HUCs for each geometry
    geometries_with_hucs_gdf = find_intersecting_hucs(fim_run_dir, geometries_gdf)

    if geometries_with_hucs_gdf.empty:
        print("No HUC intersects given geometries.")
        file_logger.info("No HUC intersects given geometries.")
        return

    # Prepare multiprocessing tasks
    tasks_args_list = []
    available_hucs = geometries_with_hucs_gdf['HUC'].unique().tolist()
    print(f'There are {len(available_hucs)} hucs intersecting the geometries. ')

    for huc in available_hucs:
        huc_dir = os.path.join(fim_run_dir, huc)
        huc_geometries_gdf = geometries_with_hucs_gdf[geometries_with_hucs_gdf['HUC'] == huc]

        # Reproject to original CRS for processing within huc outputs
        huc_crs = huc_geometries_gdf["huc_crs"].iloc[0]
        huc_geometries_gdf = huc_geometries_gdf.to_crs(huc_crs)

        # Create task for all branches because geometries can be anywhere across the huc
        branches = [branch for branch in os.listdir(os.path.join(huc_dir, 'branches'))]
        for branch in branches:
            tasks_args_list.append(
                {
                    "fim_path": fim_run_dir,
                    "unique_id": f"{huc}_{branch}",
                    "huc_geometries_gdf": huc_geometries_gdf,
                    "flow_file_df": flow_file_df,
                }
            )

    # Run multiprocessing for all hucs/branches
    mp_results = run_with_mp(
        task_function=process_one_huc_branch,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=max_workers,
        task_id_key="unique_id",
        show_progress=True,
    )

    # Collect and consolidate results
    all_results_dfs = []
    for task_id, task_payload in mp_results.items():
        result_df = task_payload[0]
        if result_df is not None:
            all_results_dfs.append(result_df)

    if not all_results_dfs:
        print("No geometries intersected with huc branches found.")
        file_logger.info("No geometries intersected with huc branches found.")
        return

    all_results_df = pd.concat(all_results_dfs, ignore_index=True)

    # Keep maximum flood depth for each geometry (across all HUCs/branches)
    final_result_df = all_results_df.loc[
        all_results_df.groupby(['geometry_unique_id'])['flood_depth'].idxmax()
    ]

    # Merge with original geometry to report full-length features
    final_result_gdf = final_result_df.merge(
        geometries_gdf[['geometry', 'geometry_unique_id']], on='geometry_unique_id', how='right'
    )

    # all spatial processing was done using epsg:4326
    final_result_gdf = gpd.GeoDataFrame(final_result_gdf, geometry='geometry', crs='epsg:4326')

    # for final output reproject to original crs of input gpkg
    final_result_gdf = final_result_gdf.to_crs(geoms_original_crs)

    # add imperial units
    final_result_gdf = add_imperial_units(final_result_gdf)

    # Save output
    final_result_gdf.to_file(output_file_path, driver="GPKG")

    print(f'Flood depth analysis completed. Output saved to: {output_file_path}')
    file_logger.info(f'Flood depth analysis completed. Output saved to: {output_file_path}')


if __name__ == "__main__":
    '''
    sample usage:
    python foss_fim/tools/compute_flood_depth.py
    -fim 'outputs/post_to_huc/my_branch/New/pipeline/'
    -flow 'data/inputs/rating_curve/nwm_recur_flows/nwm3_17C_recurr_50_0_cms.csv'
    -geom "outputs/flood_depth/run_4_hucs/combined.gpkg"
    -o 'outputs/flood_depth/run_4_hucs/flood_depth_output/new.gpkg
    '''
    parser = argparse.ArgumentParser(
        description="Compute flood depth for input geometries using FIM outputs and a flow file."
    )
    parser.add_argument(
        "-fim", "--fim_run_dir", help="Directory path to FIM run directory.", required=True, type=str
    )
    parser.add_argument(
        "-flow",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns must be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument(
        "-geom",
        "--geometry_file",
        help="Path to input geometry file (e.g., roads geopackage).",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o", "--output_file_path", help="Path to geopackage output.", required=True, type=str
    )
    parser.add_argument(
        "-j",
        "--max_workers",
        help="Number of parallel workers for multiprocessing. Default is 8.",
        required=False,
        type=int,
        default=8,
    )

    start = timer()

    flood_depth_main(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
