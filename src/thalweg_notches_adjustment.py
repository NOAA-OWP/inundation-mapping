#!/usr/bin/env python3
# This script may move to before subdivision routine.
# Consider it after FIM6.0 release
# Note: This routine does not Update any in-channel or over-bank variablesin SRCs and HTs.

import datetime as dt
import os
import traceback
from argparse import ArgumentParser
from os.path import join

import numpy as np
import pandas as pd


# -------------------------------------------------------
# Reseting stage column in SRCs for fixing thalweg notches
def reset_stage(srcs_df):
    # Re-inject grouping column for hydroid
    srcs_df['HydroID'] = srcs_df.name

    stage_interval = 0.3048  # float(os.getenv('stage_interval_meters'))

    srcs_df = srcs_df.sort_values('Stage').reset_index(drop=True)
    srcs_df['Stage'] = np.array([round(i * stage_interval, 4) for i in range(len(srcs_df))])

    return srcs_df


# -------------------------------------------------------
# Extending src_df with linear_extrapolation for missing stages in thalweg notches
def extend_src_linear_extrapolation(srcs_df, stages_full):
    # Re-inject grouping column for hydroid
    srcs_df['HydroID'] = srcs_df.name

    # Number of the last rows of src to include in extrapolation
    num_rows = 3
    # Identify all value columns except 'Stage'
    src_cols = [col for col in srcs_df.columns if col not in ['Stage']]

    existing_stages = srcs_df['Stage'].values
    # If already complete, return early
    if len(existing_stages) == len(stages_full):
        return srcs_df

    # Prepare existing src_df
    # existing = srcs_df.sort_values('Stage')
    existing_src = srcs_df.set_index('Stage')

    # Build DataFrame for all target stages
    extended_src = pd.DataFrame({'Stage': stages_full})
    extended_src['HydroID'] = srcs_df['HydroID'].iloc[0]
    extended_src = extended_src.set_index('Stage')

    # For each value column, interpolate/extrapolate as needed
    for col in src_cols:
        # Columns to extrapolate
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
            # 'Discharge (m3s-1)_subdiv',
        ]
        # if np.issubdtype(srcs_df[col].dtype, np.number):
        # if srcs_df[col].iloc[-1] != srcs_df[col].iloc[-2]:
        if col in col_variables:
            # Only use non-NaN values for fitting
            # x = existing_src.index.values[-num_rows:]
            # y = existing_src[col].values[-num_rows:]
            # coeffs = np.polyfit(x, y, 1)
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
            # extended_src[col] = np.polyval(coeffs, extended_src.index.values)

            # Overwrite with original values where available
            for stage in existing_src.index.values:
                extended_src.at[stage, col] = existing_src.at[stage, col]

        else:  # Repeat last value for missing
            # Fill with NaN first
            extended_src[col] = np.nan
            # Assign existing values
            for stage in existing_src.index.values:
                extended_src.at[stage, col] = existing_src.at[stage, col]
            # Find missing stages and fill with last value
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
    """Function for correcting thalweg notches in synthetic rating curves.
    For GMS branches, it will correct each hydroID SRC in serial based
    that shows thalweg notches behavior within in-channel stages.

        Parameters
        ----------
        huc_dir : str
            Directory path for huc output.
        huc : str
            HUC-8 string.

        Returns
        ----------
        log_text : str

    """
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

        # Removing thalweg notche rows from SRCs and
        # Applying extend_src_linear_extrapolation to add missing rows
        cond_ThalwegNRows = (src_df2['Number of Cells'] == 0) & (src_df2['Stage'] > 0)
        if cond_ThalwegNRows.sum() > 0:
            src_df_skipTwNRows = src_df2[~cond_ThalwegNRows].copy()
            src_df_skipTwNRows_gb = (
                src_df_skipTwNRows.groupby('HydroID', group_keys=False)
                .apply(reset_stage, include_groups=False)
                .reset_index(drop=True)
            )

            src_df3 = src_df_skipTwNRows_gb.copy()

            # Applying extend_src_linear_extrapolation to add missing rows
            # Identify the standard stages
            print(f'Fixing for thalweg nothes for  HUC {huc} Branch: {branch}')

            stages_full = np.array([round(i * stage_interval, 4) for i in range(84)])

            # Apply extend_src_linear_extrapolation to each src_df
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

        # # Adjusting hydro tables for thalweg notches
        # # Reporting in the log file
        # log_text += f'Adjusting Thalweg Notches in hydroTable for HUC {huc} Branch {branch}'

        # ht_branch_path = join(fim_huc_dir, 'branches', str(branch), f'hydroTable_{branch}.csv')
        # ht_df = pd.read_csv(ht_branch_path, low_memory=False)
        # ht_df = ht_df.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)
        # ht_df[cols_int] = ht_df[cols_int].astype(int)

        # ## Use the subdivision discharge column when it is being applied
        # if 'subdiv_discharge_cms' in ht_df.columns:
        #     ht_df['subdiv_discharge_cms'] = src_df['Discharge (m3s-1)_subdiv']
        # ht_df['Number of Cells'] = src_df['Number of Cells']
        # ht_df['SurfaceArea (m2)'] = src_df['SurfaceArea (m2)']
        # ht_df['BedArea (m2)'] = src_df['BedArea (m2)']
        # ht_df['TopWidth (m)'] = src_df['TopWidth (m)']
        # ht_df['WettedPerimeter (m)'] = src_df['WettedPerimeter (m)']
        # ht_df['HydraulicRadius (m)'] = src_df['HydraulicRadius (m)']
        # ht_df['WetArea (m2)'] = src_df['WetArea (m2)']
        # ht_df['Volume (m3)'] = src_df['Volume (m3)']
        # ht_df['discharge_cms'] = src_df['Discharge (m3s-1)']

        # # Write ht back to file
        # ht_df.to_csv(ht_branch_path, index=False)

    return log_text


# --------------------------------------------------------
# Apply thalweg notches adjustment
def apply_thalweg_notches_adjustment(huc_dir, huc, stage_interval, log_file_path):  # bankfull_flows_file,
    """
    Function for applying thalweg notches adjustment to synthetic rating curves.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        Please refer to correct_src_thalweg_notches and
        process_thalweg_notches functions parameters.

        Returns
        ----------
        log_text : str
    """
    log_text = ""

    try:
        msg = f"Correcting rating curve for thalweg notches for HUC : {huc}\n"
        log_text += msg
        print(msg)
        log_text += correct_thalweg_notches(huc_dir, huc, stage_interval)  # bankfull_flows_file

    except Exception:
        log_text += f"An error has occurred while processing thalweg notches for huc {huc}\n"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}\n")


# -------------------------------------------------------
def process_thalweg_notches_adjustment(huc_dir):  # stage_interval,
    """
    Function for correcting thalweg notches in synthetic rating curves using Multi-Proc function
    for each HUC8. For GMS branches, it will correct each hydroID SRC in serial based that
    has thalweg notches.

        Parameters
        ----------
        huc_dir : str
            Directory path for huc output.
        strm_order : int
            stream order on or higher for which you want to apply thalweg notches in SRC adjustment.
            default = 4
    """
    # Set up log file
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file_path = os.path.join(log_dir, 'thalweg_notches_adjustment.log')
    print(f'Writing progress to log file here: {log_file_path}')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)

    ## Initiate log file
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""

    stage_interval = 0.3048  # float(os.getenv('stage_interval_meters'))

    huc = os.path.basename(os.path.normpath(huc_dir))
    apply_thalweg_notches_adjustment(huc_dir, huc, stage_interval, log_file_path)

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
        Directory path for huc output. Log file will be placed in
        huc/logs/src_calibrations/thalweg_notch_adjustment.log.
    stage_interval : int

    Sample Usage
    ----------
    python3 /foss_fim/src/thalweg_notches_src_adjustment.py -huc_dir /outputs/fim_run_dir/huc_dir
        -j $jobLimit -sor 4
    """
    parser = ArgumentParser(description="thalweg notches in SRC Adjustment")
    parser.add_argument('-huc_dir', '--huc_dir', help='Path to huc dir', required=True, type=str)
    # parser.add_argument(
    #     '-i',
    #     '--stage_interval',
    #     help="stage_interval which equals 0.3048",
    #     required=True,
    #     type=int,
    # )

    args = vars(parser.parse_args())

    huc_dir = args['huc_dir']

    process_thalweg_notches_adjustment(huc_dir)  # stage_interval,
