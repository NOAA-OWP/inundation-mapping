#!/usr/bin/env python3

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
# calculating bankfull stage in SRCs
def src_bankfull_lookup(src_df, bankfull_flows_file):

    df_bflows = pd.read_csv(bankfull_flows_file, dtype={'feature_id': int})
    df_src = src_df.copy()

    # NWM recurr rename discharge var
    df_bflows = df_bflows.rename(columns={'discharge': 'bankfull_flow'})

    # Combine the nwm bankfull estimated flows into the SRC via feature_id
    df_src = df_src.merge(df_bflows, how='left', on='feature_id')

    # Check if there are any missing data, negative or zero flow values in the bankfull_flow
    check_null = df_src['bankfull_flow'].isnull().sum()
    if check_null > 0:
        df_src['bankfull_flow'] = df_src['bankfull_flow'].fillna(-999)

    # Locate the closest SRC discharge value to the NWM bankfull estimated flow
    df_src['Q_bfull_find'] = (df_src['bankfull_flow'] - df_src['Discharge (m3s-1)']).abs()

    # Check for any missing/null entries in the input SRC
    # There may be null values for lake or coastal flow lines
    # (need to set a value to do groupby idxmin below)
    if df_src['Q_bfull_find'].isnull().values.any():
        df_src['Q_bfull_find'] = df_src['Q_bfull_find'].fillna(999999)

    # create new subset df to perform the Q_1_5 lookup
    df_bankfull_calc = df_src[['Stage', 'HydroID', 'Q_bfull_find']]
    # Ensure bankfull stage is greater than stage=0
    df_bankfull_calc = df_bankfull_calc[df_bankfull_calc['Stage'] > 0.0]
    df_bankfull_calc = df_bankfull_calc.reset_index(drop=True)

    # find the index of the Q_bfull_find (closest matching flow)
    df_bankfull_calc = df_bankfull_calc.loc[
        df_bankfull_calc.groupby('HydroID')['Q_bfull_find'].idxmin()
    ].reset_index(drop=True)
    # rename volume to use later for channel portion calc
    df_bankfull_calc = df_bankfull_calc.rename(columns={'Stage': 'Stage_bankfull'})
    df_src = df_src.merge(df_bankfull_calc[['Stage_bankfull', 'HydroID']], how='left', on='HydroID')

    df_src = df_src.drop(['Q_bfull_find'], axis=1)

    ## mask bankfull variables when the bankfull estimated flow value is <= 0
    df_src['Stage_bankfull'].mask(df_src['bankfull_flow'] <= 0.0, inplace=True)

    ## Create a new column to identify channel/floodplain via the bankfull stage value
    df_src.loc[df_src['Stage'] <= df_src['Stage_bankfull'], 'bankfull_proxy'] = 'channel'
    df_src.loc[df_src['Stage'] > df_src['Stage_bankfull'], 'bankfull_proxy'] = 'floodplain'
    df_src['bankfull_proxy'] = df_src['bankfull_proxy'].fillna('channel')

    return df_src


# -------------------------------------------------------
# Reseting stage column in SRCs
def reset_stage(srcs_df):

    srcs_df = srcs_df.sort_values('Stage').reset_index(drop=True)
    step = srcs_df['Stage'].diff().dropna().round(4).mode()[0] if len(srcs_df) > 1 else 0
    srcs_df['Stage'] = [i * step for i in range(len(srcs_df))]

    return srcs_df


# -------------------------------------------------------
# Analysing each HydroID SRC for nonmonotonic SRC
def analyze_nonmonotonic_src(srcs_df, strm_order, thalweg_hydroids):

    # Only apply on stream orders >= strm_order
    if srcs_df['order_'].iloc[0] < strm_order:
        return srcs_df

    # Only apply on HydroIDs that have not been fixed in thalweg notches adjustment
    if srcs_df['HydroID'].iloc[0] in thalweg_hydroids:
        return srcs_df

    cond_chan = srcs_df['bankfull_proxy'] == 'channel'
    srcs_df_chan = srcs_df[cond_chan]
    non_monotonic_index = srcs_df_chan.index[srcs_df_chan['Discharge (m3s-1)'].diff().lt(0)].tolist()

    # Recalculate 'Discharge' values before the last non-monotonic row
    # Note: No change has been applied on WetArea, Volume, LENGTHKM
    if non_monotonic_index:

        # src_cols = ['Number of Cells', 'SurfaceArea (m2)', 'BedArea (m2)']
        # # Recalculate Number of Cells, SurfaceArea (m2) and BedArea (m2)
        # for col in src_cols:
        # # Only use non-NaN values for fitting
        #     x = existing_src.index.values[-num_rows:]
        #     y = existing_src[col].values[-num_rows:]

        #     # Numeric columns: extrapolate
        #     if np.issubdtype(srcs_df[col].dtype, np.number):
        #         # If only one data point, repeat it
        #         if len(x) == 1:
        #             extended_src[col] = y[0]
        #         else:
        #             coeffs = np.polyfit(x, y, 1)
        #             extended_src[col] = np.polyval(coeffs, extended_src.index.values)

        #         # Overwrite with original values where available
        #         for stage in existing_src.index.values:
        #             extended_src.at[stage, col] = existing_src.at[stage, col]

        target_numCells = srcs_df.loc[non_monotonic_index[-1], 'Number of Cells']
        target_SurfaceArea = srcs_df.loc[non_monotonic_index[-1], 'SurfaceArea (m2)']
        target_BedArea = srcs_df.loc[non_monotonic_index[-1], 'BedArea (m2)']
        srcs_df.loc[: non_monotonic_index[-1] - 1, 'Number of Cells'] = target_numCells
        srcs_df.loc[: non_monotonic_index[-1] - 1, 'SurfaceArea (m2)'] = target_SurfaceArea
        srcs_df.loc[: non_monotonic_index[-1] - 1, 'BedArea (m2)'] = target_BedArea

        # Recalculating discharge variables
        srcs_df['TopWidth (m)'] = srcs_df['SurfaceArea (m2)'] / srcs_df['LENGTHKM'] / 1000
        srcs_df['WettedPerimeter (m)'] = srcs_df['BedArea (m2)'] / srcs_df['LENGTHKM'] / 1000
        srcs_df['HydraulicRadius (m)'] = srcs_df['WetArea (m2)'] / srcs_df['WettedPerimeter (m)']
        srcs_df['HydraulicRadius (m)'].fillna(0, inplace=True)

        # Recalculate Discharge (m3s-1)
        srcs_df['Discharge (m3s-1)'][: non_monotonic_index[-1] - 1] = (
            srcs_df['WetArea (m2)'][: non_monotonic_index[-1] - 1]
            * pow(srcs_df['HydraulicRadius (m)'][: non_monotonic_index[-1] - 1], 2.0 / 3)
            * pow(srcs_df['SLOPE'][: non_monotonic_index[-1] - 1], 0.5)
            / srcs_df['ManningN'][: non_monotonic_index[-1] - 1]
        )

    return srcs_df


# -------------------------------------------------------
# Extending src_df with linear_extrapolation for missing stages in thalweg notches
def extend_src_linear_extrapolation(srcs_df, stages_full):

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
        # Only use non-NaN values for fitting
        x = existing_src.index.values[-num_rows:]
        y = existing_src[col].values[-num_rows:]

        # Numeric columns: extrapolate
        if np.issubdtype(srcs_df[col].dtype, np.number):
            coeffs = np.polyfit(x, y, 1)
            extended_src[col] = np.polyval(coeffs, extended_src.index.values)

            # Overwrite with original values where available
            for stage in existing_src.index.values:
                extended_src.at[stage, col] = existing_src.at[stage, col]

        else:  # Non-numeric columns: repeat last value for missing
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
# Correcting nonmonotonic SRC
def correct_nonmonotonic_src(fim_dir, huc, strm_order, bankfull_flows_file):
    """Function for correcting nonmonotonic synthetic rating curves.
    For GMS branches, it will correct each hydroID SRC in serial based
    that shows nonmonotonic behavior within in-channel stages.

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
    log_text = f'Processing nonmonotonic SRCs for HUC {huc}\n'

    fim_huc_dir = join(fim_dir, huc)
    # Get src_full from each branch
    src_all_branch_paths = []
    branches = os.listdir(join(fim_huc_dir, 'branches'))
    for branch in branches:
        if int(branch) > 0:  # Just for GMS branches
            src_full = join(fim_huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
            if os.path.isfile(src_full):
                src_all_branch_paths.append(src_full)

    # Update parameters for nonmonotonic SRC
    for src in src_all_branch_paths:
        src_name = os.path.basename(src)
        branch = src_name.split(".")[0].split("_")[-1]
        log_text += f'Adjusting Nonmonotonic SRC for HUC {huc} Branch: {branch}'
        print(f'Adjusting Nonmonotonic SRC for HUC {huc} Branch: {branch}')

        src_df = pd.read_csv(src, low_memory=False)

        # Calculating bankfull stage
        # src_df2 = src_bankfull_lookup(src_df, bankfull_flows_file)
        src_df2 = src_df.copy()

        # Removing thalweg notche rows from SRCs and
        # Applying extend_src_linear_extrapolation to add missing rows
        cond_ThalwegNRows = (src_df2['Number of Cells'] == 0) & (src_df2['Stage'] > 0)
        if cond_ThalwegNRows.sum() > 0:
            src_df_skipTwNRows = src_df2[~cond_ThalwegNRows].copy()
            src_df_skipTwNRows_gb = (
                src_df_skipTwNRows.groupby('HydroID', group_keys=False)
                .apply(reset_stage)
                .reset_index(drop=True)
            )

            src_df3 = src_df_skipTwNRows_gb.copy()

            # Applying extend_src_linear_extrapolation to add missing rows
            # Identify the standard stages
            stages_full = np.sort(src_df3.groupby('HydroID').filter(lambda x: len(x) == 84)['Stage'].unique())
            print(stages_full)

            # Apply extend_src_linear_extrapolation to each src_df
            src_df3 = (
                src_df3.groupby('HydroID', group_keys=False).apply(
                    extend_src_linear_extrapolation(stages_full=stages_full)
                )
                # .apply(lambda src_g: extend_src_linear_extrapolation(src_g, stages_full, src_cols, num_rows))
                # .sort_values(['HydroID', 'Stage'])
                .reset_index(drop=True)
            )
            cols_int = [
                'Number of Cells',
                'SurfaceArea (m2)',
                'HydroID',
                'NextDownID',
                'order_',
                'feature_id',
            ]
            src_df3[cols_int] = src_df3[cols_int].astype(int)

        else:
            src_df3 = src_df2.copy()

        # Adjusting src tables for nonmonotonic SRCs
        # Excluding hydroIDs that already fixed in thalweg notches adjustment
        # thalweg_hydroids = [hyid for hyid in src_df[cond_ThalwegNRows]['HydroID'].drop_duplicates()]
        # src_df4 = src_df3.groupby('HydroID', group_keys=False).apply(
        #     analyze_nonmonotonic_src, strm_order=strm_order, thalweg_hydroids = thalweg_hydroids
        # )

        src_df4 = src_df3.copy()
        # Make sure nonmonotonic adjustment just applied within in-channel stages
        cond_bankfull = src_df3['bankfull_proxy'] == 'floodplain'
        src_df4.loc[cond_bankfull, 'Discharge (m3s-1)'] = src_df3.loc[cond_bankfull, 'Discharge (m3s-1)']
        src_df4.loc[cond_bankfull, 'SurfaceArea (m2)'] = src_df3.loc[cond_bankfull, 'SurfaceArea (m2)']
        src_df4.loc[cond_bankfull, 'BedArea (m2)'] = src_df3.loc[cond_bankfull, 'BedArea (m2)']
        src_df4.loc[cond_bankfull, 'TopWidth (m)'] = src_df3.loc[cond_bankfull, 'TopWidth (m)']
        src_df4.loc[cond_bankfull, 'WettedPerimeter (m)'] = src_df3.loc[cond_bankfull, 'WettedPerimeter (m)']
        src_df4.loc[cond_bankfull, 'HydraulicRadius (m)'] = src_df3.loc[cond_bankfull, 'HydraulicRadius (m)']

        # Drop bankfull_columns
        # src_df4 = src_df4.drop(['bankfull_flow', 'Stage_bankfull', 'bankfull_proxy'], axis=1)

        # Force zero stage to have zero discharge
        src_df4.loc[src_df4['Stage'] == 0, ['Discharge (m3s-1)']] = 0

        # Write src back to file
        src_df = src_df4.copy()
        src_df.to_csv(src, index=False)

        # # Adjusting hydro tables for nonmonotonic SRC
        # log_text += f'Adjusting Nonmonotonic hydroTable for HUC {huc} Branch: {branch}'

        # ht_branch_path = join(fim_huc_dir, 'branches', str(branch), f'hydroTable_{branch}.csv')
        # ht_df = pd.read_csv(ht_branch_path, low_memory=False)

        # ht_df.loc[Q0_cond, 'discharge_cms'] = 0
        # ht_df.loc[Q0_cond, 'Volume (m3)'] = 0
        # ht_df.loc[Q0_cond, 'WetArea (m2)'] = 0
        # ht_df.loc[Q0_cond, 'BedArea (m2)'] = 0
        # ht_df.loc[Q0_cond, 'HydraulicRadius (m)'] = 0

        # ht_df.to_csv(ht_branch_path, index=False)

    return log_text


# --------------------------------------------------------
# Apply nonmonotonic src adjustment
def apply_nonmonotonic_src_adjustment(fim_dir, huc, strm_order, bankfull_flows_file, log_file_path):
    """
    Function for applying nonmonotonic SRC adjustment to synthetic rating curves.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        Please refer to correct_src_thalweg_notches and
        process_nonmonotonic_srcs functions parameters.

        Returns
        ----------
        log_text : str
    """
    log_text = ""

    try:
        msg = f"Correcting rating curve for nonmonotonic SRC for HUC : {huc}"
        log_text += msg + '\n'
        print(msg)
        log_text += correct_nonmonotonic_src(fim_dir, huc, strm_order, bankfull_flows_file)

    except Exception:
        log_text += f"An error has occurred while processing nonmonotonic SRC for huc {huc}"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}")


# -------------------------------------------------------
def process_nonmonotonic_src_adjustment(fim_dir, strm_order, bankfull_flows_file, number_of_jobs):
    """
    Function for correcting nonmonotonic synthetic rating curves using Multi-Proc function
    for each HUC8. For GMS branches, it will correct each hydroID SRC in serial based that
    shows nonmonotonic behavior within in-channel stages.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        strm_order : int
            stream order on or higher for which you want to apply nonmonotonic SRC adjustment.
            default = 4
        number_of_jobs : int
            Number of CPU cores to parallelize HUC processing.
    """
    # Set up log file
    log_file_path = os.path.join(fim_dir, 'logs', 'nonmonotonic_src_adjustment' + '.log')
    print(f'Writing progress to log file here: {log_file_path}')
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)

    ## Initiate log file
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""

    # Find HUCs to apply nonmonotonic SRC adjustment
    fim_hucs = [h for h in os.listdir(fim_dir) if re.match(r'\d{8}', h)]
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        # Loop through all hucs, build the arguments, and submit them to the process pool
        futures = {}
        for huc in fim_hucs:
            args = {
                'fim_dir': fim_dir,
                'huc': huc,
                'strm_order': strm_order,
                'bankfull_flows_file': bankfull_flows_file,
                'log_file_path': log_file_path,
            }
            future = executor.submit(apply_nonmonotonic_src_adjustment, **args)
            futures[future] = future

        for future in as_completed(futures):
            if future is not None:
                if future.exception():
                    raise future.exception()

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
    fim_dir : str
        Directory path for fim_pipeline output. Log file will be placed in
        fim_dir/logs/thalweg_notch_adjustment.log.
    strm_order : int
        stream order on or higher for which you want to apply nonmonotonic SRC adjustment.
        default = 4
    number_of_jobs : int
        Optional. Number of CPU cores to parallelize HUC processing. Defaults to 1.

    Sample Usage
    ----------
    python3 /foss_fim/src/nonmonotonic_src_adjustment.py -fim_dir /outputs/fim_run_dir
        -j $jobLimit -sor 4
    """
    parser = ArgumentParser(description="nonmonotonic SRC Adjustment")
    parser.add_argument('-fim_dir', '--fim-dir', help='Path to FIM output dir', required=True, type=str)
    parser.add_argument(
        '-sor',
        '--strm_order',
        help="stream order on or higher for which nonmonotonic SRC adjustment is applied",
        default=4,
        required=False,
        type=int,
    )
    parser.add_argument(
        '-flows',
        '--bankfull_flows_file',
        help="Path to bankfull flow values per feature-id",
        required=True,
        type=str,
    )
    parser.add_argument(
        '-j',
        '--number-of-jobs',
        help='OPTIONAL: number of workers (default=1)',
        required=False,
        default=1,
        type=int,
    )
    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    strm_order = args['strm_order']
    bankfull_flows_file = args['bankfull_flows_file']
    number_of_jobs = args['number_of_jobs']

    process_nonmonotonic_src_adjustment(fim_dir, strm_order, bankfull_flows_file, number_of_jobs)
