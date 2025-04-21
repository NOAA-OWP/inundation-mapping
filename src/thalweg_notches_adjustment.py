#!/usr/bin/env python3

import datetime as dt
import os
import re
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import join

import pandas as pd

# -------------------------------------------------------
# Correcting each HydroID SRC for thalweg notches
def process_nonmonotonic_src(srcs_df, strm_order):

    if srcs_df['order_'].iloc[0] < strm_order:
        return srcs_df

    non_monotonic_index = srcs_df.index[srcs_df['Discharge (m3s-1)'].diff().lt(0)].tolist()

    # Set 'Discharge' values before the last non-monotonic row to zero
    if non_monotonic_index:
        # print(non_monotonic_index)
        # print(srcs_df.loc[:non_monotonic_index[-1], 'TopWidth (m)'])
        # print(srcs_df.loc[:non_monotonic_index[-1], 'Discharge (m3s-1)'])
        srcs_df.loc[:non_monotonic_index[-1] - 1, 'Discharge (m3s-1)'] = 0
    return srcs_df

# -------------------------------------------------------
# Correcting SRCs for thalweg notches
def correct_src_thalweg_notches(fim_dir, huc, strm_order):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the hydroIDs that show thalweg notch.

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
    log_text = f'Processing thalweg notches for HUC {huc}\n'

    fim_huc_dir = join(fim_dir, huc)
    # Get src_full from each branch
    src_all_branch_paths = []
    branches = os.listdir(join(fim_huc_dir, 'branches'))
    for branch in branches:
        if int(branch) > 0:
            src_full = join(fim_huc_dir, 'branches', str(branch), f'src_full_crosswalked_{branch}.csv')
            if os.path.isfile(src_full):
                src_all_branch_paths.append(src_full)

    # Update src parameters for thalweg notch
    for src in src_all_branch_paths:
        src_name = os.path.basename(src)
        branch = src_name.split(".")[0].split("_")[-1]
        log_text += f'  Adjusting SRC for Branch: {branch}'
        print(f'  Adjusting SRC for Branch: {branch}')

        # Adjusting src tables for thalweg notches
        src_df = pd.read_csv(src, low_memory=False)

        src_df2 = src_df.groupby('HydroID', group_keys=False).apply(
            process_nonmonotonic_src,
            strm_order=strm_order,
        )
        src_df = src_df2.copy()
        Q0_cond = (src_df['Discharge (m3s-1)']==0)
        src_df.loc[Q0_cond, 'Volume (m3)'] = 0
        src_df.loc[Q0_cond, 'WetArea (m2)'] = 0        
        src_df.loc[Q0_cond, 'BedArea (m2)'] = 0
        src_df.loc[Q0_cond, 'HydraulicRadius (m)'] = 0
        
        src_df.to_csv(src, index=False)

        # Adjusting hydro tables for thalweg notches
        log_text += f'  Adjusting hydroTable for Branch: {branch}'
        print(f'  Adjusting hydroTable for Branch: {branch}')

        ht_branch_path = join(fim_huc_dir, 'branches', str(branch), f'hydroTable_{branch}.csv')
        ht_df = pd.read_csv(ht_branch_path, low_memory=False)

        ht_df.loc[Q0_cond, 'discharge_cms'] = 0
        ht_df.loc[Q0_cond, 'Volume (m3)'] = 0
        ht_df.loc[Q0_cond, 'WetArea (m2)'] = 0        
        ht_df.loc[Q0_cond, 'BedArea (m2)'] = 0
        ht_df.loc[Q0_cond, 'HydraulicRadius (m)'] = 0

        ht_df.to_csv(ht_branch_path, index=False)

    return log_text


# --------------------------------------------------------
# Apply src_adjustment_for_thalwegnotches
def apply_src_adjustment_for_thalwegnotches(
    fim_dir, huc, strm_order, log_file_path
):
    """
    Function for applying thalweg notch adjustment to synthetic rating curves.

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
        msg = f"Correcting rating curve for thalweg notch for HUC : {huc}"
        log_text += msg + '\n'
        print(msg)
        log_text += correct_src_thalweg_notches(fim_dir, huc, strm_order)

    except Exception:
        log_text += f"An error has occurred while processing thalweg notch for huc {huc}"
        log_text += traceback.format_exc()
    
    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}")


# -------------------------------------------------------
def process_thalweg_notches_adjustment(
    fim_dir,
    strm_order,
    number_of_jobs,
):
    """Function for correcting synthetic rating curves. It will correct each branch's
    SRCs in serial based on the feature_ids.

        Parameters
        ----------
        fim_dir : str
            Directory path for fim_pipeline output.
        strm_order : int
            stream order on or higher for which you want to apply thalweg notch adjustment.
            default = 4
        number_of_jobs : int
            Number of CPU cores to parallelize HUC processing.
    """
    # Set up log file
    log_file_path = os.path.join(fim_dir, 'logs', 'thalwag_notches_adjustment' + '.log')
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

    # Find HUCs to apply thalweg notch adjustment
    fim_hucs = [h for h in os.listdir(fim_dir) if re.match(r'\d{8}', h)]
    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        # Loop through all hucs, build the arguments, and submit them to the process pool
        futures = {}
        for huc in fim_hucs:
            args = {
                'fim_dir': fim_dir,
                'huc': huc,
                'strm_order': strm_order,
                'log_file_path': log_file_path,
            }
            future = executor.submit(apply_src_adjustment_for_thalwegnotches, **args)
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
        stream order on or higher for which you want to apply thalweg notch adjustment.
        default = 4
    number_of_jobs : int
        Optional. Number of CPU cores to parallelize HUC processing. Defaults to 1.

    Sample Usage
    ----------
    python3 /foss_fim/src/thalweg_notches_adjustment.py -fim_dir /outputs/fim_run_dir
        -j $jobLimit -sor 4
    """
    parser = ArgumentParser(description="Thalweg Notches Adjustment")
    parser.add_argument('-fim_dir', '--fim-dir', help='FIM output dir', required=True, type=str)
    parser.add_argument(
        '-sor',
        '--strm_order',
        help="stream order on or higher for which thalweg notch adjustment is applied",
        default=4,
        required=False,
        type=int,
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
    number_of_jobs = args['number_of_jobs']

    process_thalweg_notches_adjustment(
        fim_dir,
        strm_order,
        number_of_jobs
    )
