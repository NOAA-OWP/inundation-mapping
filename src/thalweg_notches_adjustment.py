#!/usr/bin/env python3
# This script may move to before subdivision routine.
# Consider it after FIM6.0 release
# Note: This routine does not Update any in-channel or over-bank variables in SRCs and HTs.

import datetime as dt
import os
import re
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import join

import numpy as np
import pandas as pd


# -------------------------------------------------------
# Reseting stage column in SRCs for fixing thalweg notches
def reset_stage(srcs_df):
    # Re-inject grouping column for HydroID if dropped by Pandas 2.0+ include_groups=False
    if 'HydroID' not in srcs_df.columns:
        srcs_df['HydroID'] = srcs_df.name

    stage_interval = 0.3048  # float(os.getenv('stage_interval_meters'))

    srcs_df = srcs_df.sort_values('Stage').reset_index(drop=True)
    srcs_df['Stage'] = np.array([round(i * stage_interval, 4) for i in range(len(srcs_df))])

    return srcs_df


# -------------------------------------------------------
# Extending src_df with linear_extrapolation for missing stages in thalweg notches
def extend_src_linear_extrapolation(srcs_df, stages_full):
    # Re-inject grouping column for HydroID if dropped by Pandas 2.0+ include_groups=False
    if 'HydroID' not in srcs_df.columns:
        srcs_df['HydroID'] = srcs_df.name

    # Number of the last rows of src to include in extrapolation
    num_rows = 3
    # Identify all value columns except 'Stage'
    src_cols = [col for col in srcs_df.columns if col not in ['Stage']]

    existing_stages = srcs_df['Stage'].values
    # If already complete, return early
    if len(existing_stages) == len(stages_full):
        return srcs_df

    existing_src = srcs_df.set_index('Stage')

    # Build DataFrame for all target stages
    extended_src = pd.DataFrame({'Stage': stages_full})
    extended_src['HydroID'] = srcs_df['HydroID'].iloc[0] if 'HydroID' in srcs_df.columns else srcs_df.name
    extended_src = extended_src.set_index('Stage')

    # For each value column, interpolate/extrapolate as needed
    for col in src_cols:
        col_variables = [
            'Number of Cells',
            'SurfaceArea (m2)',
            'BedArea (m2)',
            'Volume (m3)',
            'TopWidth (m)',
            'WettedPerimeter (m)',
            'WetArea (m2)',
            'HydraulicRadius (m)',
            'Discharge (m3s-1)',
        ]
        if col in col_variables:
            mask = ~np.isnan(existing_src.index.values[-num_rows:]) & ~np.isnan(
                existing_src[col].values[-num_rows:]
            )
            x = existing_src.index.values[-num_rows:][mask]
            y = existing_src[col].values[-num_rows:][mask]

            if len(x) >= 2 and np.var(x) > 1e-8:  # Ensure valid data & non-constant x
                try:
                    coeffs = np.polyfit(x, y, 1)
                    extended_src[col] = np.polyval(coeffs, extended_src.index.values)
                except np.linalg.LinAlgError:
                    # Fallback: Use last valid value if linear fit fails
                    last_valid = y[-1] if len(y) > 0 else existing_src[col].iloc[-1]
                    extended_src[col] = last_valid
            else:  # Not enough data or constant x-values
                last_valid = existing_src[col].iloc[-1]
                extended_src[col] = last_valid

            # Overwrite with original values where available
            for stage in existing_src.index.values:
                extended_src.at[stage, col] = existing_src.at[stage, col]

        else:  # Repeat last value for missing
            extended_src[col] = np.nan
            for stage in existing_src.index.values:
                extended_src.at[stage, col] = existing_src.at[stage, col]
            existing_stages_sorted = np.sort(existing_src.index.values)
            last_value = existing_src[col].loc[existing_stages_sorted[-1]]

            for stage in stages_full:
                if pd.isna(extended_src.at[stage, col]):
                    extended_src.at[stage, col] = last_value

    extended_src = extended_src.reset_index()

    return extended_src


# -------------------------------------------------------
# Correcting thalweg notches in SRC
def correct_thalweg_notches(huc_dir, huc, stage_interval):
    log_text = f'Processing thalweg notches in SRCs for HUC {huc}\n'

    # Get src_full from each branch
    src_all_branch_paths = []
    branches = os.listdir(join(huc_dir, 'branches'))
    for branch in branches:
        if int(branch) > 0:  # Just for GMS branches
            src_full = join(huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
            if os.path.isfile(src_full):
                src_all_branch_paths.append(src_full)

    # Defining integer columns
    cols_int = ['Number of Cells', 'SurfaceArea (m2)', 'HydroID', 'NextDownID', 'order_', 'feature_id']

    # Update parameters for thalweg notches in SRC
    for src in src_all_branch_paths:
        src_name = os.path.basename(src)
        branch = src_name.split(".")[0].split("_")[-1]
        log_text += f'Adjusting Thalweg Notches in SRC for HUC {huc} Branch: {branch}\n'

        src_df = pd.read_csv(src, low_memory=False)

        src_df1 = src_df.copy()
        prethalweg_discharge = src_df1['Discharge (m3s-1)']

        src_df2 = src_df.copy()
        src_df2 = src_df2.drop_duplicates(subset=['HydroID', 'Stage'], keep='first').reset_index(drop=True)

        # Removing thalweg notch rows from SRCs and
        # Applying extend_src_linear_extrapolation to add missing rows
        cond_ThalwegNRows = (src_df2['Number of Cells'] == 0) & (src_df2['Stage'] > 0)
        if cond_ThalwegNRows.sum() > 0:
            src_df_skipTwNRows = src_df2[~cond_ThalwegNRows].copy()

            # Ensure HydroID is a column before groupby
            if 'HydroID' not in src_df_skipTwNRows.columns:
                src_df_skipTwNRows = src_df_skipTwNRows.reset_index()

            src_df_skipTwNRows_gb = (
                src_df_skipTwNRows.groupby('HydroID', group_keys=False)
                .apply(reset_stage, include_groups=False)
                .reset_index(drop=True)
            )

            src_df3 = src_df_skipTwNRows_gb.copy()

            # Ensure HydroID is present as a column in src_df3 before second groupby
            if 'HydroID' not in src_df3.columns:
                if src_df3.index.name == 'HydroID':
                    src_df3 = src_df3.reset_index()
                elif 'HydroID' in src_df3.index.names:
                    src_df3 = src_df3.reset_index()

            print(f'Fixing for thalweg notches for HUC {huc} Branch: {branch}')

            stages_full = np.array([round(i * stage_interval, 4) for i in range(84)])

            # Apply extend_src_linear_extrapolation to each src_df group
            src_df3 = (
                src_df3.groupby('HydroID', group_keys=False)
                .apply(
                    lambda src_g: extend_src_linear_extrapolation(src_g, stages_full), include_groups=False
                )
                .sort_values(['HydroID', 'Stage'])
                .reset_index(drop=True)
            )
            src_df3[cols_int] = src_df3[cols_int].astype(int)

        else:
            src_df3 = src_df2.copy()

        # Force zero stage to have zero discharge
        src_df3.loc[src_df3['Stage'] == 0, 'Discharge (m3s-1)'] = 0

        # Write src back to file
        src_df4 = src_df3.copy()
        discharge_thalweg = src_df4['Discharge (m3s-1)']
        src_df3['Discharge (m3s-1)_thalwegAdjusted'] = discharge_thalweg
        src_df3['prethalweg_Discharge (m3s-1)'] = prethalweg_discharge

        src_df5 = src_df3.copy()
        src_df5['Thalweg_adjustment_applied'] = False
        thalweg_col = abs(
            src_df5['Discharge (m3s-1)_thalwegAdjusted'] - src_df5['prethalweg_Discharge (m3s-1)']
        )
        cond_thalweg_rows = thalweg_col > 0
        src_df5.loc[cond_thalweg_rows, 'Thalweg_adjustment_applied'] = True

        src_df = src_df5.copy()
        src_df.to_csv(src, index=False)

    return log_text


# --------------------------------------------------------
# Apply thalweg notches adjustment
def apply_thalweg_notches_adjustment(huc_dir, huc, stage_interval, log_file_path):
    log_text = ""

    try:
        msg = f"Correcting rating curve for thalweg notches for HUC : {huc}\n"
        log_text += msg
        print(msg)
        log_text += correct_thalweg_notches(huc_dir, huc, stage_interval)

    except Exception:
        err_msg = f"An error has occurred while processing thalweg notches for huc {huc}\n"
        log_text += err_msg
        log_text += traceback.format_exc()

        print(err_msg)
        print(traceback.format_exc())

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}")
        print(traceback.format_exc())


# -------------------------------------------------------
def process_thalweg_notches_adjustment(huc_dir):
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file_path = os.path.join(log_dir, 'thalweg_notches_adjustment.log')

    try:
        print(f'Writing progress to log file here: {log_file_path}')
        begin_time = dt.datetime.now(dt.timezone.utc)

        with open(log_file_path, "w") as log_file:
            log_file.write('START TIME: ' + str(begin_time) + '\n')
            log_file.write('#########################################################\n\n')

        log_text = ""
        stage_interval = 0.3048

        huc = os.path.basename(os.path.normpath(huc_dir))
        apply_thalweg_notches_adjustment(huc_dir, huc, stage_interval, log_file_path)

        end_time = dt.datetime.now(dt.timezone.utc)
        log_text += 'END TIME: ' + str(end_time) + '\n'
        tot_run_time = end_time - begin_time
        log_text += 'TOTAL RUN TIME: ' + str(tot_run_time).split('.')[0]

    except Exception as ex:
        print(f"An exception occurred while processing thalweg notch adjustments for {huc_dir}.")
        print(f"Details: {traceback.format_exc()}")
        raise ex


if __name__ == '__main__':
    parser = ArgumentParser(description="thalweg notches in SRC Adjustment")
    parser.add_argument('-huc_dir', '--huc_dir', help='Path to huc dir', required=True, type=str)

    args = vars(parser.parse_args())
    huc_dir = args['huc_dir']

    process_thalweg_notches_adjustment(huc_dir)
