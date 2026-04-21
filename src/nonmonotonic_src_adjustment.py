#!/usr/bin/env python3
# This script may move to before subdivision routine.
# Consider it after FIM6.0 release
# Note: This routine does not Update any in-channel or over-bank variables
# in SRCs and HTs. It just updates "Discharge (m3s-1)_subdiv"

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
# Analysing each HydroID SRC for nonmonotonic SRC
def analyze_nonmonotonic_src(srcs_df, strm_order):  # , thalweg_hydroids

    # Only apply on stream orders >= strm_order
    if srcs_df['order_'].iloc[0] < strm_order:
        return srcs_df

    srcs_df.loc[srcs_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0

    cond_chan = srcs_df['bankfull_proxy'] == 'channel'
    srcs_df_chan = srcs_df[cond_chan]
    non_monotonic_index = srcs_df_chan.index[srcs_df_chan['Discharge (m3s-1)'].diff().lt(0)].tolist()

    # Recalculate 'Discharge' values before the last non-monotonic row
    # Note: No change has been applied on WetArea, Volume, LENGTHKM
    if non_monotonic_index:
        # Get the target values from the last non-monotonic index
        target_idx = non_monotonic_index[-1]
        target_numCells = srcs_df.loc[target_idx, 'Number of Cells']
        target_SurfaceArea = srcs_df.loc[target_idx, 'SurfaceArea (m2)']
        target_BedArea = srcs_df.loc[target_idx, 'BedArea (m2)']

        # Define the slice (up to but not including target_idx)
        row_slice = slice(0, target_idx)

        # Assign target values to the selected rows
        srcs_df.loc[row_slice, 'Number of Cells'] = target_numCells
        srcs_df.loc[row_slice, 'SurfaceArea (m2)'] = target_SurfaceArea
        srcs_df.loc[row_slice, 'BedArea (m2)'] = target_BedArea

        # Recalculate discharge variables
        length_km = srcs_df.loc[row_slice, 'LENGTHKM']
        # Avoid division by zero
        length_km = length_km.replace(0, np.nan)

        target_TopWidth = target_SurfaceArea / length_km / 1000
        target_WettedPerimeter = target_BedArea / length_km / 1000

        wet_area = srcs_df.loc[row_slice, 'WetArea (m2)']
        target_HydraulicRadius = wet_area / target_WettedPerimeter

        srcs_df.loc[row_slice, 'TopWidth (m)'] = target_TopWidth
        srcs_df.loc[row_slice, 'WettedPerimeter (m)'] = target_WettedPerimeter
        srcs_df.loc[row_slice, 'HydraulicRadius (m)'] = target_HydraulicRadius
        srcs_df['HydraulicRadius (m)'] = srcs_df['HydraulicRadius (m)'].fillna(0)

        # Recalculate Discharge (m3s-1) for the selected rows
        srcs_df.loc[row_slice, 'Discharge (m3s-1)'] = (
            wet_area
            * (srcs_df.loc[row_slice, 'HydraulicRadius (m)'] ** (2.0 / 3))
            * (srcs_df.loc[row_slice, 'SLOPE'] ** 0.5)
            / srcs_df.loc[row_slice, 'channel_n']
        )
        # srcs_df['Discharge (m3s-1)_subdiv'] = np.where(
        #     srcs_df['subdiv_applied'] == True,
        #     srcs_df['Discharge (m3s-1)'],
        #     srcs_df['Discharge (m3s-1)_subdiv'],
        # )

    return srcs_df


# -------------------------------------------------------
# Correcting nonmonotonic SRC
def correct_nonmonotonic_src(huc_dir, huc, strm_order):  # , bankfull_flows_file
    """Function for correcting nonmonotonic synthetic rating curves.
    For GMS branches, it will correct each hydroID SRC in serial based
    that shows nonmonotonic behavior within in-channel stages.

        Parameters
        ----------
        huc_dir : str
            Directory path for fim_pipeline output.
        huc : str
            HUC-8 string.

        Returns
        ----------
        log_text : str

    """
    log_text = f'Processing nonmonotonic SRCs for HUC {huc}\n'

    # Fix the bathymetry source first
    # Replacing "0" with "No Bathymetry Applied"
    src_full_0 = join(huc_dir, 'branches', str(0), 'src_full_crosswalked_0.csv')
    ht_0_path = join(huc_dir, 'branches', str(0), 'hydroTable_0.csv')

    if os.path.isfile(src_full_0) and os.path.isfile(ht_0_path):
        src_0_df = pd.read_csv(src_full_0, low_memory=False)
        ht_0_df = pd.read_csv(ht_0_path, low_memory=False)

        src_0_df.loc[src_0_df['Bathymetry_source'] == str(0), 'Bathymetry_source'] = 'No Bathymetry Applied'
        src_0_df.loc[src_0_df['Bathymetry_source'] == 0, 'Bathymetry_source'] = 'No Bathymetry Applied'
        src_0_df['Bathymetry_source'] = src_0_df['Bathymetry_source'].fillna('No Bathymetry Applied')
        ht_0_df['Bathymetry_source'] = src_0_df['Bathymetry_source']

        src_0_df['Longitudinal_adjustment_applied'] = False
        src_0_df['Thalweg_adjustment_applied'] = False
        src_0_df['Nonmonotonic_adjustment_applied'] = False

        # Save updated branch 0 ht and src tables
        src_0_df = src_0_df.drop_duplicates(
            subset=['HydroID', 'feature_id', 'Stage'], keep='first'
        ).reset_index(drop=True)
        src_0_df.to_csv(src_full_0, index=False)
        ht_0_df = ht_0_df.drop_duplicates(
            subset=['HydroID', 'feature_id', 'stage'], keep='first'
        ).reset_index(drop=True)
        ht_0_df.to_csv(ht_0_path, index=False)
    else:
        print("Files do not exist: src_full_crosswalked_0.csv and hydroTable_0.csv")

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

    # Update parameters for nonmonotonic SRC
    for src in src_all_branch_paths:  # [4:5]
        src_name = os.path.basename(src)
        branch = src_name.split(".")[0].split("_")[-1]
        log_text += f'Adjusting Nonmonotonic SRC for HUC {huc} Branch: {branch}\n'
        print(f'Adjusting Nonmonotonic SRC for HUC {huc} Branch: {branch}')

        src_df = pd.read_csv(src, low_memory=False)

        # Calculating bankfull stage
        # src_df2 = src_bankfull_lookup(src_df, bankfull_flows_file)
        src_df2 = src_df.copy()
        src_df2 = src_df2.drop_duplicates(subset=['HydroID', 'Stage'], keep='first').reset_index(drop=True)

        ## Use the subdivision discharge column when is being applied
        src_df2['Discharge (m3s-1)'] = np.where(
            src_df2['subdiv_applied'] == True,
            src_df2['Discharge (m3s-1)_subdiv'],
            src_df2['Discharge (m3s-1)'],
        )

        src_df3 = src_df2.copy()
        prenonmono_discharge = src_df3['Discharge (m3s-1)']

        # Adjusting src tables for nonmonotonic SRCs
        src_df4 = src_df2.groupby('HydroID', group_keys=False).apply(
            analyze_nonmonotonic_src, strm_order=strm_order, include_groups=False
        )

        # Make sure nonmonotonic adjustment just applied within in-channel stages
        cond_bankfull = src_df2['bankfull_proxy'] == 'floodplain'
        if 'Discharge (m3s-1)_subdiv' in src_df2.columns:
            src_df4.loc[cond_bankfull, 'Discharge (m3s-1)_subdiv'] = src_df2.loc[
                cond_bankfull, 'Discharge (m3s-1)_subdiv'
            ]
        src_df4.loc[cond_bankfull, 'Discharge (m3s-1)'] = src_df2.loc[cond_bankfull, 'Discharge (m3s-1)']
        src_df4.loc[cond_bankfull, 'SurfaceArea (m2)'] = src_df2.loc[cond_bankfull, 'SurfaceArea (m2)']
        src_df4.loc[cond_bankfull, 'BedArea (m2)'] = src_df2.loc[cond_bankfull, 'BedArea (m2)']
        src_df4.loc[cond_bankfull, 'TopWidth (m)'] = src_df2.loc[cond_bankfull, 'TopWidth (m)']
        src_df4.loc[cond_bankfull, 'WettedPerimeter (m)'] = src_df2.loc[cond_bankfull, 'WettedPerimeter (m)']
        src_df4.loc[cond_bankfull, 'HydraulicRadius (m)'] = src_df2.loc[cond_bankfull, 'HydraulicRadius (m)']

        # Force zero stage to have zero discharge
        src_df4.loc[src_df4['Stage'] == 0, 'Discharge (m3s-1)'] = 0
        if 'Discharge (m3s-1)_subdiv' in src_df4.columns:
            src_df4.loc[src_df4['Stage'] == 0, 'Discharge (m3s-1)_subdiv'] = 0

        # Write src back to file
        src_df = src_df4.copy()
        # Make sure there is no nan values
        src_df.loc[src_df['Bathymetry_source'] == 0, 'Bathymetry_source'] = 'No Bathymetry Applied'
        src_df['Bathymetry_source'] = src_df['Bathymetry_source'].fillna('No Bathymetry Applied')

        src_df['subdiv_applied'] = src_df.groupby('HydroID')['subdiv_applied'].ffill()
        src_df['channel_n'] = src_df.groupby('HydroID')['channel_n'].ffill()
        src_df['overbank_n'] = src_df.groupby('HydroID')['overbank_n'].ffill()

        src_df22 = src_df.copy()
        discharge_nonmono = src_df22['Discharge (m3s-1)']
        src_df22['Discharge (m3s-1)_nonmonotonicAdjusted'] = discharge_nonmono

        src_df5 = src_df22.copy()
        src_df5['Nonmonotonic_adjustment_applied'] = False
        nonmono_col = abs(discharge_nonmono - prenonmono_discharge)
        cond_nonmono_rows = nonmono_col > 0.00001
        src_df5.loc[cond_nonmono_rows, 'Nonmonotonic_adjustment_applied'] = True

        src_df = src_df5.copy()
        src_df.to_csv(src, index=False)

        # Adjusting hydro tables for nonmonotonic SRC
        # Reporting in the log file
        log_text += f'Adjusting Nonmonotonic hydroTable for HUC {huc} Branch {branch}'

        ht_branch_path = join(huc_dir, 'branches', str(branch), f'hydroTable_{branch}.csv')
        ht_df = pd.read_csv(ht_branch_path, low_memory=False)
        ht_df = ht_df.drop_duplicates(subset=['HydroID', 'stage'], keep='first').reset_index(drop=True)
        ht_df[cols_int] = ht_df[cols_int].astype(int)

        ## Use the subdivision discharge column when it is being applied
        if 'subdiv_discharge_cms' in ht_df.columns:
            ht_df['subdiv_discharge_cms'] = src_df['Discharge (m3s-1)_subdiv']
        ht_df['Number of Cells'] = src_df['Number of Cells']
        ht_df['SurfaceArea (m2)'] = src_df['SurfaceArea (m2)']
        ht_df['BedArea (m2)'] = src_df['BedArea (m2)']
        ht_df['TopWidth (m)'] = src_df['TopWidth (m)']
        ht_df['WettedPerimeter (m)'] = src_df['WettedPerimeter (m)']
        ht_df['HydraulicRadius (m)'] = src_df['HydraulicRadius (m)']
        ht_df['WetArea (m2)'] = src_df['WetArea (m2)']
        ht_df['Volume (m3)'] = src_df['Volume (m3)']
        ht_df['discharge_cms'] = src_df['Discharge (m3s-1)']
        # For the sake of aggregation routine
        ht_df['Bathymetry_source'] = src_df['Bathymetry_source']
        ht_df['subdiv_applied'] = src_df['subdiv_applied']
        ht_df['channel_n'] = src_df['channel_n']
        ht_df['overbank_n'] = src_df['overbank_n']

        # Write ht back to file
        ht_df.to_csv(ht_branch_path, index=False)

    return log_text


# --------------------------------------------------------
# Apply nonmonotonic src adjustment
def apply_nonmonotonic_src_adjustment(huc_dir, huc, strm_order, log_file_path):  # bankfull_flows_file,
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
        msg = f"Correcting rating curve for nonmonotonic SRC for HUC : {huc}\n"
        log_text += msg
        print(msg)
        log_text += correct_nonmonotonic_src(huc_dir, huc, strm_order)  # bankfull_flows_file

    except Exception:
        log_text += f"An error has occurred while processing nonmonotonic SRC for huc {huc}\n"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}\n")


# -------------------------------------------------------
def process_nonmonotonic_src_adjustment(huc_dir, strm_order):  # bankfull_flows_file,
    """
    Function for correcting nonmonotonic synthetic rating curves using Multi-Proc function
    for each HUC8. For GMS branches, it will correct each hydroID SRC in serial based that
    shows nonmonotonic behavior within in-channel stages.

        Parameters
        ----------
        huc_dir : str
            Directory path for huc output.
        strm_order : int
            stream order on or higher for which you want to apply nonmonotonic SRC adjustment.
            default = 4
    """
    # Set up log file
    log_dir = os.path.join(huc_dir, "logs", "src_calibrations")
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file_path = os.path.join(log_dir, 'nonmonotonic_src_adjustment.log')
    print(f'Writing progress to log file here: {log_file_path}')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)

    ## Initiate log file
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""

    huc = os.path.basename(os.path.normpath(huc_dir))
    apply_nonmonotonic_src_adjustment(huc_dir, huc, strm_order, log_file_path)

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

    Sample Usage
    ----------
    python3 /foss_fim/src/nonmonotonic_src_adjustment.py -fim_dir /outputs/fim_run_dir
         -sor 4
    """
    parser = ArgumentParser(description="nonmonotonic SRC Adjustment")
    parser.add_argument('-huc_dir', '--huc_dir', help='Path to FIM output dir', required=True, type=str)
    parser.add_argument(
        '-sor',
        '--strm_order',
        help="stream order on or higher for which nonmonotonic SRC adjustment is applied",
        default=4,
        required=False,
        type=int,
    )
    # parser.add_argument(
    #     '-flows',
    #     '--bankfull_flows_file',
    #     help="Path to bankfull flow values per feature-id",
    #     required=True,
    #     type=str,
    # )
    args = vars(parser.parse_args())

    huc_dir = args['huc_dir']
    strm_order = args['strm_order']
    #  bankfull_flows_file = args['bankfull_flows_file']

    process_nonmonotonic_src_adjustment(huc_dir, strm_order)  # bankfull_flows_file,
