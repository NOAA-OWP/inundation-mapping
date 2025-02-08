import argparse
import errno
import os
import re
import warnings
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask


def process_impact_statement(huc_path, impact_statement_dir, NWSLID, huc):
    """
    This function uses the National Weather Service's Georeferenced Impact Statement for gauged locations to calibrate the rating curve.
    Step 1: It uses impact statement polygons for a specific site and samples the REM values under each polygon.
    Step 2: For each impact stage (action, minor, moderate, and major), it calculates the median, 75th percentile, and upper extreme HAND values.
    Step 3: It finds the closest matching stage to the user-provided HAND value and copies the corresponding hydroTable values for the matching stage.
    Step 4: It calculates a weighted average calibration coefficient and adjusts Manning’s roughness.
    Step 5: It recalculates the discharge and updates the hydroTable.
    """
    hydrotable_huc_path = os.path.join(huc_path, 'hydrotable.csv')
    hydrotable_huc = pd.read_csv(hydrotable_huc_path, low_memory=False)
    branch_list = hydrotable_huc['branch_id'].unique().tolist()
    branch_folder = os.listdir(huc_path)
    all_df = []
    for branch_folder in os.listdir(huc_path):
        branch_path = os.path.join(huc_path, branch_folder)
        if os.path.isdir(branch_path):
            branch_sub_dire = branch_path
    for branch in branch_list:
        branch_sub = os.path.join(branch_sub_dire, str(branch))
        catchments_path = os.path.join(
            branch_sub, f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch}.gpkg'
        )
        catchments = gpd.read_file(catchments_path)
        impact_statement_path = impact_statement_dir
        NWSLID = NWSLID

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            warnings.simplefilter('ignore', category=RuntimeWarning)
            impact_data = gpd.read_file(impact_statement_path, layer='NWS_Impact_Statements___Polygons')
        impact_data = impact_data.reset_index()
        target_polygon = impact_data[impact_data['NWSLID'] == NWSLID]
        # Check if the target polygons exist for the specific lid
        if target_polygon.empty:
            print('No polygons found for this NWSLID.')
            return

        target_polygon = target_polygon.to_crs(catchments.crs)

        # Check if catchment and the polygons intersect
        catchments_results = gpd.sjoin(target_polygon, catchments, predicate='intersects')
        if catchments_results.empty:
            # print(f'No overlap found with branch {branch}, skipping...')
            continue

        hydroid_list = (
            catchments_results['HydroID'].unique().tolist()
        )  # List of all overlapped catchments in the branch

        polygon_types = {
            'Action': {
                'polygon': target_polygon[target_polygon['ImpactStatus'] == 'Action'],
                'flow_col': 'ActionFlow',
                'weighted_variables': None,
            },
            'Minor': {
                'polygon': target_polygon[target_polygon['ImpactStatus'] == 'Minor'],
                'flow_col': 'MinorFlow',
                'weighted_variables': None,
            },
            'Moderate': {
                'polygon': target_polygon[target_polygon['ImpactStatus'] == 'Moderate'],
                'flow_col': 'ModerateFlow',
                'weighted_variables': None,
            },
            'Major': {
                'polygon': target_polygon[target_polygon['ImpactStatus'] == 'Major'],
                'flow_col': 'MajorFlow',
                'weighted_variables': None,
            },
        }

        impc_stm_df = pd.DataFrame(columns=['HydroID', 'discharge', 'stage'])
        cal_data = []
        weithed_values = []  # Store weighted calibration values

        for catchment in hydroid_list:
            htable_branch = hydrotable_huc[hydrotable_huc['branch_id'] == branch]
            if "impact_sta" in htable_branch.columns:
                print(f'georeferenced impact statement calibration has been already applied for HUC {huc}')
                return
            print(f'Processing catchment {catchment} in branch {branch}')
            target_hid = htable_branch[htable_branch['HydroID'] == catchment]
            rem_path = os.path.join(branch_sub, f'rem_zeroed_masked_{branch}.tif')
            # Assign weights to median, 75th percentile, and upper extreme hand values for each stage window (minor, moderate, major)
            weights = np.array([0.1, 0.7, 0.2])  # median, 75th percentile, upper extreme

            with rasterio.open(rem_path) as src:
                for poly_type, poly_data in polygon_types.items():
                    polygon = poly_data['polygon']
                    flow_col = poly_data['flow_col']
                    # Sample the REM values under each polygon
                    if polygon.empty:
                        continue
                    if polygon.crs != src.crs:
                        polygon = polygon.to_crs(src.crs)
                    geometry_ = [g.__geo_interface__ for g in polygon.geometry]
                    out_image, out_transform = mask(src, geometry_, crop=True)
                    data = out_image[0]
                    data = data[data != src.nodata]
                    y = data.flatten()
                    df = pd.DataFrame({'y': y})
                    # Statistic calculation
                    percentile_75 = df['y'].quantile(0.75)
                    med = df['y'].quantile(0.5)
                    q1 = df['y'].quantile(0.25)
                    iqr = percentile_75 - q1
                    upper = percentile_75 + (1.5 * iqr)
                    upper_extreme = df[df['y'] <= upper]['y'].max()

                    # Find closest matching stage to the user provided HAND value
                    find_src_stage_med = target_hid.loc[target_hid['stage'].sub(med).abs().idxmin()]
                    find_src_stage_75 = target_hid.loc[target_hid['stage'].sub(percentile_75).abs().idxmin()]
                    find_src_stage_max = target_hid.loc[target_hid['stage'].sub(upper_extreme).abs().idxmin()]

                    # Copy the corresponding hydroTable discharge for the matching stage
                    discharge_obs = polygon[flow_col].iloc[0] * 0.028316847
                    src_discharge_med = find_src_stage_med.discharge_cms
                    src_discharge_75 = find_src_stage_75.discharge_cms
                    src_discharge_max = find_src_stage_max.discharge_cms

                    # Calculate calibration coefficient
                    calib_co_med = src_discharge_med / discharge_obs
                    calib_co_75 = src_discharge_75 / discharge_obs
                    calib_co_max = src_discharge_max / discharge_obs
                    weighted = np.dot([calib_co_med, calib_co_75, calib_co_max], weights)
                    weithed_values.append(weighted)

            # Median calibration coefficient
            median_cal_coefficient = np.median(weithed_values)
            # Adjusted roughness
            new_roughness = median_cal_coefficient * 0.06
            # Recalculating discharge
            area = np.array(target_hid['WetArea (m2)'])
            h_radius = np.array(target_hid['HydraulicRadius (m)'])
            slope = np.array(target_hid['SLOPE'])
            new_Q = (1 / new_roughness) * area * (h_radius ** (2 / 3)) * slope ** (1 / 2)
            stage_column = target_hid['stage']
            stage_column.reset_index(inplace=True, drop=True)
            stage_column = stage_column.to_numpy()
            catchment_array = np.full(len(new_Q), catchment)
            branch_array = np.full(len(new_Q), branch)
            df_ = pd.DataFrame(
                {
                    'HydroID': catchment_array,
                    'discharge': new_Q,
                    'stage': stage_column,
                    'branch_id': branch_array,
                }
            )
            cal_data.append(df_)
        concat_df = pd.concat(cal_data, axis=0, ignore_index=True)
        all_df.append(concat_df)

    impc_stm_df = pd.concat(cal_data, axis=0, ignore_index=True)
    impc_stm_df['impact_sta'] = 'True'
    impc_stm_df['HydroID'] = impc_stm_df['HydroID'].astype(int)
    impc_stm_df['stage'] = impc_stm_df['stage'].astype(float)
    impc_stm_df['branch_id'] = impc_stm_df['branch_id'].astype(int)

    new_hydrotable = hydrotable_huc.copy()
    new_hydrotable = new_hydrotable.merge(impc_stm_df, how='left', on=['HydroID', 'stage', 'branch_id'])
    new_hydrotable['pre_impact_statement_discharge'] = new_hydrotable['discharge_cms']
    branch = int(branch)

    new_hydrotable.loc[(new_hydrotable['impact_sta'] == 'True'), 'discharge_cms'] = new_hydrotable[
        'discharge'
    ]
    new_hydrotable = new_hydrotable.drop(columns=['discharge'])
    new_htable_path = os.path.join(huc_path, 'hydrotable.csv')
    new_hydrotable.to_csv(new_htable_path, index=False)


def impact_statement_calibration(fim_dir, imp_stm_path, NWSLID, limit_hucs):
    # Check that hydrofabric_dir exists
    if not os.path.exists(fim_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fim_dir)
    # Get the list of all hucs in the directory
    hucs = [d for d in os.listdir(fim_dir) if re.match(r'^\d{8}$', d)]
    if limit_hucs:
        hucs = [h for h in limit_hucs if h in hucs]
    for huc in hucs:
        print(f'Start georeferenced impact statement calibration for HUC {huc}')
        # create the full path of the entry
        huc_path = os.path.join(fim_dir, huc)
        # check if the huc is a directory
        if os.path.isdir(huc_path):
            process_impact_statement(huc_path, imp_stm_path, NWSLID, huc)


if __name__ == "__main__":
    # Sample usage:
    '''
    python /foss_fim/tools/georeferenced_impact_statement_cal.py \
        -d /data/previous_fim/hand_4_5_11_1 \
        -imp /data/inputs/georeferenced_impact_statement/NWS_Impact_Statements_v3.gpkg  \
        -lid BLSK2 \
        -u 05100101
    '''
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Calibrate rating curve using Georeferenced Impact statement "
    )
    parser.add_argument(
        "-d",
        "--fim_dir",
        help="Directory path to FIM hydrofabric by processing unit.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-imp",
        "--imp_stm_path",
        help='Path to the georefrenced impact statement geopackage.',
        required=True,
        type=str,
    )
    parser.add_argument("-lid", "--NWSLID", help="NWSLID of the site.", required=True, type=str)
    parser.add_argument("-u", "--limit_hucs", help="Optional.", required=False, type=str, nargs="+")
    start = timer()

    # Extract to dictionary and run
    impact_statement_calibration(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
