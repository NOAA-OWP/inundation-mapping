#!/usr/bin/env python3
# Please Note that this script is being only applied on ALL GMS branches, NOT on branch 0.

import datetime as dt
import os
import re
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import join

import geopandas as gpd
import numpy as np
import pandas as pd


stage_interval = float(os.getenv('stage_interval_meters'))


# -------------------------------------------------------
def extract_longitudinal_variables(src_df, hydroid, stage):
    """
    Function for extracting hydraulic variable to longitudinal smooth
    along a stream in synthetic rating curves.
    Candidate_variables_to_smooth_longitudinally = [
        'BedArea (m2)',
        'Volume (m3)',
        'SurfaceArea (m2)',
        'WetArea (m2)',
        'HydraulicRadius (m)',
        'Discharge (m3s-1)'
    ]
        Parameters
        ----------
        src_df : dataframe
            Synthetic rating curve dataframe.
        hydroid : str
            Fim hydroid string.
        stage : float
            Fim stage.

        Returns
        ----------
        voi_hid_stage : list

    """

    src = src_df.loc[src_df.HydroID == hydroid]

    if src.LakeID.iloc[0] > 0:
        return [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
    else:
        surfacearea = round(np.interp(stage, src.Stage, src['SurfaceArea (m2)']), 2)
        bedArea = round(np.interp(stage, src.Stage, src['BedArea (m2)']), 2)
        volume = round(np.interp(stage, src.Stage, src['Volume (m3)']), 2)
        wetarea = round(np.interp(stage, src.Stage, src['WetArea (m2)']), 2)
        hydraulicRadius = round(np.interp(stage, src.Stage, src['HydraulicRadius (m)']), 3)
        flow = round(np.interp(stage, src.Stage, src['Discharge (m3s-1)']), 2)

    voi_hid_stage = [surfacearea, bedArea, volume, wetarea, hydraulicRadius, flow]

    return voi_hid_stage


# -------------------------------------------------------
def detect_anomalies(df, factor_thresh):
    """Boolean mask of anomalous (reach, stage) using neighbor rule."""

    n_reach, n_stage = df.shape
    mask_anomalies = pd.DataFrame(False, index=df.index, columns=df.columns)

    for i in range(1, n_reach - 1):  # internal reaches only
        us = df.iloc[i - 1]
        ds = df.iloc[i + 1]
        cur = df.iloc[i]
        high_vs_up = cur > factor_thresh * us
        high_vs_dn = cur > factor_thresh * ds
        mask_anomalies.iloc[i] = high_vs_up & high_vs_dn

    # Find rows that have any True
    rows_with_true = mask_anomalies.any(axis=1)

    # For those rows, set all columns to True
    mask_anomalies.loc[rows_with_true, :] = True

    return mask_anomalies


# -------------------------------------------------------
def estimated_from_neighbors(df_sa, df_q):
    """Expected SA by weighted interpolation between upstream and downstream SA and Q."""
    n_reach, n_stage = df_sa.shape
    est_sa_df = df_sa.copy().astype(float)

    for i in range(1, n_reach - 1):
        us_sa = df_sa.iloc[i - 1]
        ds_sa = df_sa.iloc[i + 1]

        us_q = df_q.iloc[i - 1]
        ds_q = df_q.iloc[i + 1]
        # simple average at each stage; could use distance‐weighted version
        est_sa_df.iloc[i] = (us_q * us_sa + ds_q * ds_sa) / (us_q + ds_q)
    return est_sa_df


# -------------------------------------------------------
def smooth_multi_stage(df_sa, df_q):

    # parameters
    factor_thresh = 2.0  # spike must be >2× both neighbors
    # max_frac_diff = 0.5  # allow 50% deviation from expected before correcting

    anomalies_mask = detect_anomalies(df_sa, factor_thresh)
    expected_sa_df = estimated_from_neighbors(df_sa, df_q)

    smoothed_df = df_sa.copy().astype(float)
    n_reach, n_stage = df_sa.shape

    for i in range(1, n_reach - 1):
        cur = df_sa.iloc[i].copy().astype(float)
        exp = expected_sa_df.iloc[i]
        mask_row = anomalies_mask.iloc[i]

        # only adjust if anomaly occurs in at least one stage
        if not mask_row.any():
            continue

        # optional: only correct anomalies that are also far from expected
        frac_diff = np.zeros_like(cur, dtype=float)
        # avoid division by zero: where exp == 0, treat frac_diff as 0 (no forced change)
        nonzero = exp != 0
        frac_diff[nonzero] = np.abs(cur[nonzero] - exp[nonzero]) / np.abs(exp[nonzero])

        to_fix = mask_row  # & (frac_diff > max_frac_diff)

        # apply neighbor estimate only to anomalous-and-far cells
        cur[to_fix] = exp[to_fix]

        smoothed_df.iloc[i] = cur
        # # compute correction factors stage by stage
        # factors = exp / cur
        # # limit corrections to values where deviation is large
        # large_dev = (np.abs(cur - exp) / exp) > max_frac_diff
        # factors[~large_dev] = 1.0  # keep stages that are close to expected

        # # single factor per reach to preserve curve shape:
        # # use median of stage-wise factors where large_dev is True
        # if large_dev.any():
        #     reach_factor = np.median(factors[large_dev])
        #     smoothed_df.iloc[i] = cur * reach_factor

    return smoothed_df


# -------------------------------------------------------
def fix_nonmonotonic_step(arr, step=None):
    arr = list(arr)
    n = len(arr)

    # infer step from last monotone increment if not given
    if step is None:
        step = None
        for i in range(n - 1):
            diff = arr[i + 1] - arr[i]
            if diff > 0:
                step = diff
        if step is None:
            step = 1  # fallback

    i = 0
    while i < n - 1:
        if arr[i + 1] >= arr[i]:
            i += 1
            continue

        # start of non-monotone segment
        start_idx = i
        start_val = arr[start_idx]

        # find end of segment: first index where values stop being non-monotone
        j = i + 1
        while j < n and arr[j] < arr[j - 1]:
            j += 1

        end_idx = j  # first index after the bad run (could be n)

        # fill from start_idx+1 up to end_idx-1 using constant step
        val = start_val
        for k in range(start_idx + 1, end_idx):
            val += step
            arr[k] = val

        # if there is a remaining tail after end_idx, continue from the last fixed value
        if end_idx < n:
            # ensure monotone step from the last filled value
            arr[end_idx] = arr[end_idx - 1] + step

        i = end_idx

    return arr


# -------------------------------------------------------
def fix_nonmonotonic(arr):
    arr = list(arr)
    n = len(arr)
    i = 0
    while i < n - 1:
        # if monotone non-decreasing, move on
        if arr[i + 1] >= arr[i]:
            i += 1
            continue

        # found start of non-monotonic segment
        start_idx = i
        start_val = arr[start_idx]

        # find end of non-monotonic segment (next value >= start_val)
        j = i + 1
        while j < n and arr[j] < start_val:
            j += 1

        # if no later point recovers, go with step approach
        if j == n:
            # break
            arr = fix_nonmonotonic_step(arr, step=None)
            return arr

        end_idx = j
        end_val = arr[end_idx]

        # number of steps between start and end (inclusive of end, exclusive of start)
        steps = end_idx - start_idx

        # linear interpolation between start_val and end_val
        step_size = (end_val - start_val) / steps
        for k in range(1, steps):
            arr[start_idx + k] = start_val + step_size * k

        # continue after the end of the corrected segment
        i = end_idx

    return arr


# -------------------------------------------------------
def enforce_stage_monotonicity(smoothed_df, stages):
    """
    For each reach (row), encourage SA(S1) < SA(S2) < ... < SA(Sn)
    by updating vals[j] to the average of vals[j-1] and vals[j]
    whenever vals[j] <= vals[j-1].
    """
    monotonic_df = smoothed_df.copy()

    for idx, row in monotonic_df.iterrows():
        vals = row[stages].to_numpy()

        arr = fix_nonmonotonic(vals)
        monotonic_df.loc[idx, stages] = arr

    return monotonic_df


# -------------------------------------------------------
def filter_longitudinal_discharge_jitters(huc_dir, huc, stage_interval):
    """
    Function for smoothing longitudinal jitters in any variables
    of interest along a stream in synthetic rating curves.
    This will only correct GMS branch's SRCs based on the hydro_ids.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        huc : str
            HUC-8 string.

        Returns
        ----------
        log_text : str

    """
    log_text = f'Filtering Longitudinal Flow Fluctuation for HUC8: {huc}\n'
    # fim_huc_dir = join(fim_dir, huc)

    # Get src_full, hydrotable and catchment from each branch
    src_all_branches_path = []
    cathment_gpkg_path = []
    branches = os.listdir(join(huc_dir, 'branches'))
    for branch in branches:
        if int(branch) > 0:  # Just for GMS branches
            src_full = join(huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
            cathment_gpkg = join(
                huc_dir,
                'branches',
                str(branch),
                f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch}.parquet',
            )
            if os.path.isfile(src_full):
                src_all_branches_path.append(src_full)
            if os.path.isfile(src_full):
                cathment_gpkg_path.append(cathment_gpkg)

    # Longitudinally adjust srcs for WSE
    for isrc in range(len(src_all_branches_path)):  # 5

        branch = re.search(r'branches/(\d{10}|0)/', src_all_branches_path[isrc]).group()[9:-1]
        print(f'Processing Longitudinal flow adjustment for HUC {huc} Branch: {branch}')
        log_text += f'Processing Longitudinal flow adjustment for HUC {huc} Branch: {branch}\n'

        catchment_gdf0 = gpd.read_parquet(cathment_gpkg_path[isrc])
        catchment_gdf = catchment_gdf0.drop_duplicates(subset=['HydroID'], keep='first')
        lakeID_df = catchment_gdf[['HydroID', 'LakeID']].drop_duplicates(subset=['HydroID'])
        # Read src tables
        src_df = pd.read_csv(src_all_branches_path[isrc], low_memory=False)
        src_df = src_df.merge(lakeID_df, on='HydroID', how='inner')
        stages = [round(num, 4) for num in src_df['Stage'][0:84]]

        # Find the BedArea and SurfaceArea relation
        sa0_mask = src_df['SurfaceArea (m2)'] == 0
        src_df.loc[sa0_mask, 'BedArea (m2)'] = 0

        # Clean data
        # clean = src_df.replace([np.inf, -np.inf], np.nan).dropna(
        #     subset=['SurfaceArea (m2)', 'BedArea (m2)']
        # )

        # x = clean['SurfaceArea (m2)'].to_numpy()
        # y = clean['BedArea (m2)'].to_numpy()

        # # Skip degenerate cases
        # if len(clean) < 2:
        #     print(f"Skipping HUC {huc}: not enough points")
        #     continue

        if np.all(src_df['SurfaceArea (m2)'] == 0) or np.all(src_df['BedArea (m2)'] == 0):
            print(f"Skipping branch {branch}: all zero data in Surface/Bed area")
            log_text += f'Skipping branch {branch}: all zero data in Surface/Bed area\n'
            continue

        if np.unique(src_df['SurfaceArea (m2)']).size < 2:
            print(f"branch {branch}: SurfaceArea has no variation")
            log_text += f'branch {branch}: SurfaceArea has no variation\n'
            continue

        # Safe to fit
        a_coef, b_coef = np.polyfit(src_df['SurfaceArea (m2)'], src_df['BedArea (m2)'], 1)
        src_df.loc[:, 'a_coef'] = a_coef
        src_df.loc[:, 'b_coef'] = b_coef

        # Volume (m3), BedArea (m2) and 'SurfaceArea (m2)' pre longitodinal adjustment
        src_df['SurfaceArea (m2)_default'] = src_df['SurfaceArea (m2)'].copy()
        src_df['BedArea (m2)_default'] = src_df['BedArea (m2)'].copy()
        src_df['Volume (m3)_default'] = src_df['Volume (m3)'].copy()

        # Defining stages with discharge = 0 and Number of Cells = 0 for later masking
        Q0_mask = src_df['Discharge (m3s-1)'] == 0
        nocell0_mask = src_df['Number of Cells'] == 0

        # Finding the headwaters
        headwaters_rows = catchment_gdf.loc[
            ~catchment_gdf.HydroID.isin(catchment_gdf.NextDownID.astype(int)),
        ]
        # Remove headwaters with lakeID
        headwaters = list(headwaters_rows[headwaters_rows['LakeID'] < 0]['HydroID'])
        # TODO : What if lake hydroID is in the middle of a chain?

        # Build hydroid chain first
        hydroid_chain_mhws = []
        for headwater in headwaters:

            hydroid_chain = [headwater]
            nexthydroid = headwater

        # Finding the headwaters
        headwaters_rows = catchment_gdf.loc[
            ~catchment_gdf.HydroID.isin(catchment_gdf.NextDownID.astype(int)),
        ]
        # Remove headwaters with lakeID
        headwaters = list(headwaters_rows[headwaters_rows['LakeID'] < 0]['HydroID'])
        # TODO : What if lake hydroID is in the middle of a chain?

        # Build hydroid chain first
        hydroid_chain_mhws = []
        for headwater in headwaters:

            hydroid_chain = [headwater]
            nexthydroid = headwater

            # While loop to create the list of hydroids
            while catchment_gdf.HydroID.isin([nexthydroid]).any():
                nexthydroid = int(
                    catchment_gdf.loc[catchment_gdf.HydroID == nexthydroid, "NextDownID"].item()
                )
                hydroid_chain.append(nexthydroid)

            if len(hydroid_chain[:-1]) > 2:  # Excluding headwaters with len 2 or smaller
                hydroid_chain_mhws.append(hydroid_chain)

        # longitudinally adjust SurfaceArea (m2) using neighbor Q and SA
        original_all_voi = {}
        filtered_all_voi = {}
        if len(hydroid_chain_mhws) > 0:
            voi2smooth_mhws = []
            filtered_voi_mhws = []
            for hydroid_chain in hydroid_chain_mhws:
                voi2smooth_df = dict()
                discharge_df = dict()
                filtered_voi_df = dict()
                long_index = 0
                for nexthydroid in hydroid_chain[:-1]:  # Excluding the ast HydroID
                    voi2smooth_list = []
                    discharge_list = []
                    for stage in stages:
                        voi2smooth_list.append(extract_longitudinal_variables(src_df, nexthydroid, stage)[0])
                        discharge_list.append(extract_longitudinal_variables(src_df, nexthydroid, stage)[-1])
                    voi2smooth_df[nexthydroid] = voi2smooth_list + [long_index]
                    discharge_df[nexthydroid] = discharge_list + [long_index]
                    long_index += 1

                stages_cols = [str(istg) for istg in stages]
                voi2smooth_df = pd.DataFrame.from_dict(
                    voi2smooth_df, orient="index", columns=stages_cols + ['long_position']
                )
                discharge_df = pd.DataFrame.from_dict(
                    discharge_df, orient="index", columns=stages_cols + ['long_position']
                )
                # Applies filtering/smoothing on the logitudinal surface area
                if len(hydroid_chain) > 1:

                    smoothed_voi_df = smooth_multi_stage(voi2smooth_df, discharge_df)
                    filtered_voi_df = smoothed_voi_df.copy()
                    # stages2filter = smoothed_voi_df.columns
                    # filtered_voi_df = enforce_stage_monotonicity(smoothed_voi_df, stages2filter[:-1])

                    # Align indices and add "long_position"
                    filtered_voi_df.index = voi2smooth_df.index  # Ensure indices match
                    filtered_voi_df["long_position"] = voi2smooth_df["long_position"]

                    voi2smooth_mhws.append(voi2smooth_df)
                    filtered_voi_mhws.append(filtered_voi_df)
                else:
                    voi2smooth_mhws.append(voi2smooth_df)
                    filtered_voi_mhws.append(voi2smooth_df)
                    print(f'No longitudinal filtering applied for branch {branch}, headwater {hydroid_chain}')

            voi2smooth_mhws_df = pd.concat(voi2smooth_mhws)
            filtered_voi_mhws_df = pd.concat(filtered_voi_mhws)

            # Add the dataframe to the dictionary
            original_all_voi['SurfaceArea (m2)'] = voi2smooth_mhws_df  # keys[ikey]
            filtered_all_voi['SurfaceArea (m2)'] = filtered_voi_mhws_df  # keys[ikey]

            # Defining a lake_discharge dataframe
            Q_lake_hydroID = src_df[['HydroID', 'LakeID', 'Stage', 'Discharge (m3s-1)']]
            # Reshaping variables of SA to be included in src
            filtered_voi = filtered_all_voi['SurfaceArea (m2)'].drop('long_position', axis=1)  # keys[jkey]
            reshaped_filtered_voi = filtered_voi.reset_index().melt(
                id_vars='index',
                var_name='Stage',
                value_name='SurfaceArea (m2)_longitudinalAdjusted',  # f'{keys[jkey]}
            )

            reshaped_filtered_voi.rename(columns={'index': 'HydroID'}, inplace=True)
            reshaped_filtered_voi['Stage'] = reshaped_filtered_voi['Stage'].astype(float)

            # Adding filtered SurfaceArea to src
            src_df = src_df.merge(
                reshaped_filtered_voi[
                    ['HydroID', 'Stage', 'SurfaceArea (m2)_longitudinalAdjusted']
                ],  # f'{keys[jkey]}
                on=['HydroID', 'Stage'],
                how='left',
            )
            src_df['SurfaceArea (m2)'] = src_df['SurfaceArea (m2)'].astype(float)
            # Force increased SA back to default
            # Compute mean(SurfaceArea (m2)_longitudinalAdjusted - default) per HydroID
            mean_diff = src_df.groupby('HydroID').apply(
                lambda g: (g['SurfaceArea (m2)_longitudinalAdjusted'] - g['SurfaceArea (m2)_default']).mean(),
                include_groups=False,
            )
            # HydroIDs where mean difference > 0
            hids_to_replace = mean_diff[mean_diff > 0].index
            # For those HydroIDs, set SurfaceArea (m2)_longitudinalAdjusted = default
            src_df.loc[src_df['HydroID'].isin(hids_to_replace), 'SurfaceArea (m2)_longitudinalAdjusted'] = (
                src_df.loc[src_df['HydroID'].isin(hids_to_replace), 'SurfaceArea (m2)_default']
            )
            # Update SurfaceArea (m2) in src
            # Update src_df where LakeID < 0 and Stage matches
            # TODO Fill NAN
            mask_src = (src_df['SurfaceArea (m2)_longitudinalAdjusted'].notna()) & (
                src_df['LakeID'] < 0
            )  # f'{keys[jkey]}
            src_df.loc[mask_src, 'SurfaceArea (m2)'] = src_df.loc[
                mask_src, 'SurfaceArea (m2)_longitudinalAdjusted'
            ]  # f'{keys[jkey]}

            # Recalculating discharge variables
            # Recalculating BedArea
            bed_area_long = src_df[['HydroID', 'Stage', 'SurfaceArea (m2)']].copy()
            bed_area_long['BedArea (m2)'] = a_coef * bed_area_long['SurfaceArea (m2)'] + b_coef
            # Merge on columns 'HydroID' and 'Stage'
            src_df = pd.merge(
                src_df,
                bed_area_long[['HydroID', 'Stage', 'BedArea (m2)']],
                on=['HydroID', 'Stage'],
                how='left',
                suffixes=('', '_longitudinalAdjusted'),
            )
            # Set BedArea(m2)_longitudinalAdjusted = default
            mask_defaultSA = src_df['SurfaceArea (m2)'] == src_df['SurfaceArea (m2)_default']
            src_df.loc[mask_defaultSA, 'BedArea (m2)_longitudinalAdjusted'] = src_df.loc[
                mask_defaultSA, 'BedArea (m2)_default'
            ]
            # mask_smoothedSA = src_df['SurfaceArea (m2)'] != src_df['SurfaceArea (m2)_default']
            # Assigning calculated Bed Area to main co
            src_df.loc[mask_src, 'BedArea (m2)'] = src_df.loc[mask_src, 'BedArea (m2)_longitudinalAdjusted']

            # Recalculating Volume
            src_df['SurfaceArea-1'] = src_df.groupby('HydroID')['SurfaceArea (m2)'].shift(1, fill_value=0)
            src_df['volume_stage'] = (
                (src_df['SurfaceArea-1'] + src_df['SurfaceArea (m2)']) * stage_interval / 2.0
            )
            # Set stage_volume = 0 for stage 0
            stage0_mask = src_df['Stage'] == 0
            src_df.loc[stage0_mask, 'volume_stage'] = 0
            src_df['Volume (m3)_longitudinalAdjusted'] = src_df.groupby('HydroID')['volume_stage'].cumsum()
            # For those HydroIDs, set Volume(m3)_longitudinalAdjusted = default
            src_df.loc[mask_defaultSA, 'Volume (m3)_longitudinalAdjusted'] = src_df.loc[
                mask_defaultSA, 'Volume (m3)_default'
            ]
            # Assigning calculated Volume
            src_df.loc[mask_src, 'Volume (m3)'] = src_df.loc[mask_src, 'Volume (m3)_longitudinalAdjusted']

            # Ensure lakes manning variables has not changed
            mask_lake = src_df['LakeID'] > 0
            src_df.loc[mask_lake, 'SurfaceArea (m2)'] = src_df.loc[mask_lake, 'SurfaceArea (m2)_default']
            src_df.loc[mask_lake, 'BedArea (m2)'] = src_df.loc[mask_lake, 'BedArea (m2)_default']
            src_df.loc[mask_lake, 'Volume (m3)'] = src_df.loc[mask_lake, 'Volume (m3)_default']

            # Recalculating rest of the Manning's variables
            src_df['WettedPerimeter (m)'] = src_df['BedArea (m2)'] / src_df['LENGTHKM'] / 1000
            src_df['WetArea (m2)'] = src_df['Volume (m3)'] / src_df['LENGTHKM'] / 1000
            src_df['HydraulicRadius (m)'] = src_df['WetArea (m2)'] / src_df['WettedPerimeter (m)']
            src_df['HydraulicRadius (m)'] = src_df['HydraulicRadius (m)'].fillna(0)

            # Recalculating the discharge
            src_df['Discharge (m3s-1)'] = (
                src_df['WetArea (m2)']
                * pow(src_df['HydraulicRadius (m)'], 2.0 / 3)
                * pow(src_df['SLOPE'], 0.5)
                / src_df['ManningN']
            )
            # Refining Discharge for lake hydroIDs with the original Q
            # Merge with src_df
            src_df_merged = src_df.merge(
                Q_lake_hydroID, on=['HydroID', 'LakeID', 'Stage'], how='left', suffixes=('', '_lake')
            )
            # Update src_df where LakeID > 0 and Stage matches
            mask = (src_df_merged['LakeID'] > 0) & (src_df_merged['Discharge (m3s-1)'].notnull())
            src_df.loc[mask, 'Discharge (m3s-1)'] = src_df_merged.loc[mask, 'Discharge (m3s-1)']

            # Preserve slope columns exactly as-is
            slope_cols = ['SLOPE', 'default_SLOPE']
            slope_backup = src_df[slope_cols].copy()
            # Round all columns
            src_df = src_df.round(5)
            # Restore slope precision
            src_df[slope_cols] = slope_backup

            # Set Hydraulic properties of original stages with discharge = 0 back to 0
            src_df.loc[Q0_mask, 'Discharge (m3s-1)'] = 0
            src_df.loc[Q0_mask, 'Volume (m3)'] = 0
            src_df.loc[Q0_mask, 'WettedPerimeter (m)'] = 0
            src_df.loc[Q0_mask, 'WetArea (m2)'] = 0
            src_df.loc[Q0_mask, 'HydraulicRadius (m)'] = 0

            # Set channel properties of original stages with Number of Cells = 0 back to 0
            src_df.loc[nocell0_mask, 'BedArea (m2)'] = 0
            src_df.loc[nocell0_mask, 'Number of Cells'] = 0
            src_df.loc[nocell0_mask, 'SurfaceArea (m2)'] = 0
            src_df.loc[nocell0_mask, 'TopWidth (m)'] = 0

            # Set nans to 0
            src_df.loc[src_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0

            src_df2 = src_df.copy()
            discharge_longitudinal = src_df2['Discharge (m3s-1)']
            src_df['Discharge (m3s-1)_longitudinalAdjusted'] = discharge_longitudinal

            src_df['Longitudinal_adjustment_applied'] = False
            if 'Discharge (m3s-1)_thalwegAdjusted' in src_df.columns:
                long_col = abs(
                    src_df['Discharge (m3s-1)_thalwegAdjusted']
                    - src_df['Discharge (m3s-1)_longitudinalAdjusted']
                )
                cond_thalweg_rows = long_col > 0
                src_df.loc[cond_thalweg_rows, 'Longitudinal_adjustment_applied'] = True
            else:
                print('Warning: thalweg_noches_adjustment routine has not been completed')
                log_text += f'Thalweg_noches_adjustment routine has not been completed for HUC {huc} Branch: {branch}\n'

            # Drop intermediate columns
            src_df = src_df.drop(columns=['SurfaceArea-1', 'volume_stage', 'a_coef', 'b_coef'])
            # Write src back to file
            src_df.to_csv(src_all_branches_path[isrc], index=False)

    log_text += f'Successfully recalculated discharge for HUC {huc}\n'
    print(f'Successfully recalculated discharges for HUC {huc}\n')
    return log_text


# --------------------------------------------------------
# Apply longitudinal dischage adjustment
def apply_longitudinal_dischage_adjustment(huc_dir, huc, log_file_path):  # bankfull_flows_file,
    """
    Function for applying longitudinal dischage adjustment to synthetic rating curves.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        Please refer to correct_src_thalweg_notches and
        process longitudinal dischage functions parameters.

        Returns
        ----------
        log_text : str
    """
    stage_interval = float(os.getenv('stage_interval_meters'))
    log_text = ""
    try:
        msg = f"Correcting rating curve for longitudinal discharge ajustment SRC for HUC : {huc}\n"
        log_text += msg
        print(msg)
        log_text += filter_longitudinal_discharge_jitters(huc_dir, huc, stage_interval)  # bankfull_flows_file

    except Exception:
        log_text += f"An error has occurred while processing longitudinal adjustment for huc {huc}\n"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}\n")


# -------------------------------------------------------
def process_longitudinal_flow_adjustment(huc_dir):
    """
    Function for correcting synthetic rating curves using Multi-Proc approach.
    It will correct each branch's SRCs in serial based on the HydroIDs.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.

    """
    # Set up log file
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file_path = os.path.join(log_dir, 'longitudinal_filter.log')
    print(f'Writing progress to log file here: {log_file_path}')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)

    ## Initiate log file
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""

    # Find applicable HUCs to apply longitudinal filter
    # fim_hucs = [h for h in os.listdir(fim_dir) if re.match(r'\d{8}', h)]

    msg = "Applying longitudinal discharge adjustment"  # on {len(fim_hucs)} HUCs: {fim_hucs}\n"
    log_text += msg

    huc = os.path.basename(os.path.normpath(huc_dir))
    apply_longitudinal_dischage_adjustment(huc_dir, huc, log_file_path)

    ## Record run time and close log file
    end_time = dt.datetime.now(dt.timezone.utc)
    log_text += 'END TIME: ' + str(end_time) + '\n'
    tot_run_time = end_time - begin_time
    log_text += 'TOTAL RUN TIME: ' + str(tot_run_time).split('.')[0]
    log_file.close()


if __name__ == '__main__':

    """
    Parameters
    ----------
    huc_dir : str
        Directory path for fim_pipeline output. Log file will be placed in
        fim_dir/logs/longitudinal_filter.log.

    Sample Usage
    ----------
    python3 /foss_fim/src/filter_longitudinal_flow.py
        -fim_dir /outputs/fim_run_dir
    """
    parser = ArgumentParser(description="Longitudinal depth/flow filter")
    parser.add_argument('-huc_dir', '--huc_dir', help='FIM output dir', required=True, type=str)

    args = vars(parser.parse_args())

    huc_dir = args['huc_dir']

    process_longitudinal_flow_adjustment(huc_dir)
