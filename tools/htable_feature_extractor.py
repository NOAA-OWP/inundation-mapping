#!/usr/bin/env python3
import argparse
import logging
import os
import shutil
import uuid
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

"""
Step 1 of the data preparation for ML calibration coefficient workflow:
Hydrotable Feature and Rating Curve Parameter Extractor.
Scans HUC run directories, streams HUC-level hydrotable.csv files,
extracts reach metadata (slope, submitter, obs_source, calb_coef_final),
and computes synthetic rating curve power-law parameters (a, b):
    Q = a * H^b  =>  ln(Q) = ln(a) + b * ln(H)

Outputs a consolidated CSV.
"""

logger = logging.getLogger("PR_Pipeline.Stage1")

# HUC prefixes to exclude (OCONUS)
OCONUS_PREFIXES = ('19', '20', '21', '22')

DESIRED_HYDRO_COLS = [
    'branch_id',
    'feature_id',
    'HydroID',
    'calb_coef_final',
    'obs_source',
    'submitter',
    'SLOPE',
    'SLOPE_RISE_RUN',
    'stage',
    'precalb_discharge_cms',
    'discharge_cms',
]


def append_file_to_master(source_path: str, dest_handle, chunk_size: int = 1024 * 1024) -> None:
    """
    Append a temporary file to the master CSV in chunks

    Parameters
    ----------
    source_path : str
        Path to temporary source CSV chunk.
    dest_handle : file object
    chunk_size : int, optional
        Binary read buffer size in bytes (default: 1MB).
    """
    with open(source_path, 'rb') as fsrc:
        while True:
            buf = fsrc.read(chunk_size)
            if not buf:
                break
            dest_handle.write(buf)


def process_single_huc_hydrotable(task_info: Tuple[str, str]) -> Optional[str]:
    """
    Processes HUC hydrotable and computes rating curve parameters (a, b).

    Parameters
    ----------
    task_info : Tuple[str, str]
        - Path to the HUC hydrotable.
        - Directory where temporary worker CSV chunk will be written.

    Returns
    -------
    Optional[str]
        Path to temporary output file, or None if no valid data,
        or an error string if an exception occurred.
    """
    file_path, temp_dir = task_info

    try:
        huc8 = os.path.basename(os.path.dirname(os.path.normpath(file_path)))
        huc8 = str(huc8).zfill(8)

        # Inspect headers to read only available target columns
        file_cols = pd.read_csv(file_path, nrows=0).columns.tolist()
        cols_to_read = [col for col in DESIRED_HYDRO_COLS if col in file_cols]

        if 'stage' not in cols_to_read or (
            'precalb_discharge_cms' not in cols_to_read and 'discharge_cms' not in cols_to_read
        ):
            return None

        dtype_spec = {
            'feature_id': str,
            'HydroID': str,
            'branch_id': str,
            'stage': 'float64',
            'calb_coef_final': 'float32',
            'obs_source': 'object',
            'submitter': 'object',
            'SLOPE': 'float32',
            'SLOPE_RISE_RUN': 'float32',
        }
        dtype_filtered = {k: v for k, v in dtype_spec.items() if k in cols_to_read}
        hydro_df = pd.read_csv(file_path, usecols=cols_to_read, dtype=dtype_filtered)

        if hydro_df.empty:
            return None

        # Fallback discharge calculation
        if 'precalb_discharge_cms' in hydro_df.columns:
            if 'discharge_cms' in hydro_df.columns:
                hydro_df['precalb_discharge_cms'] = hydro_df['precalb_discharge_cms'].fillna(
                    hydro_df['discharge_cms']
                )
        elif 'discharge_cms' in hydro_df.columns:
            hydro_df['precalb_discharge_cms'] = hydro_df['discharge_cms']
        else:
            return None

        group_keys = ['branch_id', 'feature_id', 'HydroID']

        # Extract static reach attributes (first occurrence per reach group)
        attrib_cols = ['calb_coef_final', 'obs_source', 'submitter', 'SLOPE', 'SLOPE_RISE_RUN']
        avail_attribs = [col for col in attrib_cols if col in hydro_df.columns]
        attrib_agg = {col: 'first' for col in avail_attribs}
        static_df = hydro_df.groupby(group_keys, as_index=False, sort=False).agg(attrib_agg)

        # Fit rating curve: Q = a * H^b --> ln(Q) = ln(a) + b * ln(H)
        valid_rc = hydro_df[
            (hydro_df['stage'] > 0) & (hydro_df['precalb_discharge_cms'] > 0)
        ].copy()

        if not valid_rc.empty:
            valid_rc['log_H'] = np.log(valid_rc['stage'])
            valid_rc['log_Q'] = np.log(valid_rc['precalb_discharge_cms'])
            valid_rc['log_H_sq'] = valid_rc['log_H'] ** 2
            valid_rc['log_H_log_Q'] = valid_rc['log_H'] * valid_rc['log_Q']

            rc_sums = valid_rc.groupby(group_keys, sort=False).agg(
                n=('log_H', 'count'),
                sum_x=('log_H', 'sum'),
                sum_y=('log_Q', 'sum'),
                sum_xx=('log_H_sq', 'sum'),
                sum_xy=('log_H_log_Q', 'sum'),
            ).reset_index()

            # Require at least 2 points for valid linear fit
            valid_fit = rc_sums[rc_sums['n'] >= 2].copy()
            denom = valid_fit['n'] * valid_fit['sum_xx'] - valid_fit['sum_x'] ** 2

            valid_fit['b'] = np.where(
                denom != 0,
                (valid_fit['n'] * valid_fit['sum_xy'] - valid_fit['sum_x'] * valid_fit['sum_y']) / denom,
                np.nan,
            )
            valid_fit['intercept'] = (valid_fit['sum_y'] - valid_fit['b'] * valid_fit['sum_x']) / valid_fit['n']
            valid_fit['a'] = np.exp(valid_fit['intercept'])

            rc_df = valid_fit[group_keys + ['a', 'b']]
            merged_result = static_df.merge(rc_df, on=group_keys, how='left')
        else:
            static_df['a'] = np.nan
            static_df['b'] = np.nan
            merged_result = static_df

        # Insert HUC8 and standardize output columns
        merged_result.insert(0, 'huc8', huc8)
        for col in attrib_cols:
            if col not in merged_result.columns:
                merged_result[col] = np.nan

        output_order = [
            'huc8',
            'branch_id',
            'feature_id',
            'HydroID',
            'calb_coef_final',
            'obs_source',
            'submitter',
            'SLOPE',
            'SLOPE_RISE_RUN',
            'a',
            'b',
        ]
        merged_result = merged_result[output_order]

        unique_id = uuid.uuid4().hex
        temp_file_path = os.path.join(temp_dir, f"{huc8}_{unique_id}.csv")
        merged_result.to_csv(temp_file_path, header=False, index=False)

        return temp_file_path

    except Exception as e:
        return f"Error processing {file_path}: {str(e)}"


def run_hydro_extraction(
    root_dir: str,
    target_filename: str = 'hydrotable.csv',
    output_file: str = './hydro_features_rc.csv',
    temp_dir: str = None,
    max_workers: Optional[int] = None,
) -> bool:
    """
    Executes hydrotable features and calculates rating curve parameters.

    Parameters
    ----------
    root_dir : str
        Root directory containing HUC8 output folders.
    target_filename : str, optional
        Target hydrotable filename (default: hydrotable.csv).
    output_file : str, optional
        Path for the output CSV file.
    temp_dir : str, optional
        Temporary directory for worker chunks.
    max_workers : int, optional
        Number of worker processes. If None, the number is based on the availble CPU.

    Returns
    -------
    bool
        True if extraction succeeded, False otherwise.
    """
    logger.info("=" * 80)
    logger.info("Step 1: Extracting hydrotable features & rating curve (a, b) parameters")
    logger.info("=" * 80)

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)

    logger.info(f"Scanning '{root_dir}' for '{target_filename}' files...")
    file_list: List[str] = []
    for root, _, files in os.walk(root_dir):
        for f in files:
            if f.lower() == target_filename.lower():
                huc8 = os.path.basename(root)
                if huc8.startswith(OCONUS_PREFIXES):
                    continue
                file_list.append(os.path.join(root, f))

    logger.info(f"Found {len(file_list):,} HUC hydrotable files.")
    if not file_list:
        logger.warning(f"No matching '{target_filename}' files found in {root_dir}. Exiting.")
        return False

    output_columns = [
        'huc8',
        'branch_id',
        'feature_id',
        'HydroID',
        'calb_coef_final',
        'obs_source',
        'submitter',
        'SLOPE',
        'SLOPE_RISE_RUN',
        'a',
        'b',
    ]
    pd.DataFrame(columns=output_columns).to_csv(output_file, index=False)

    if max_workers is None:
        max_workers = min(16, max(1, (os.cpu_count() or 4) - 1))

    logger.info(f"Processing with {max_workers} parallel CPU workers...")
    
    # Safe temp directory
    with tempfile.TemporaryDirectory(prefix="Htable_chunks_", dir=temp_dir) as safe_temp_dir:
        tasks = [(f, safe_temp_dir) for f in file_list]
        errors_count = 0
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(process_single_huc_hydrotable, t): t[0] for t in tasks}

            with open(output_file, 'ab') as master_file:
                with tqdm(total=len(file_list), unit="HUC", desc="Extracting Hydrotables") as pbar:
                    for future in as_completed(future_to_file):
                        result = future.result()
                        if result and not result.startswith("Error"):
                            try:
                                if os.path.exists(result):
                                    append_file_to_master(result, master_file)
                                    os.remove(result)
                            except Exception as e:
                                logger.error(f"Error appending chunk {result}: {e}")
                        elif result and result.startswith("Error"):
                            logger.warning(result)
                            errors_count += 1
                        pbar.update(1)

    if errors_count > 0:
        logger.warning(f"Processing finished with error in {errors_count} HUC files.")
    logger.info(f"Extraction complete. Output saved to: {output_file}")
    return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(
        description="Extract hydrotable features and rating curve (a, b) parameters."
    )
    parser.add_argument('-r', '--root_dir', help='Root directory containing HUC8 run folders.', required=True, type=str)
    parser.add_argument('-f', '--target_filename', help='Target hydrotable filename.', default='hydrotable.csv', type=str)
    parser.add_argument('-o', '--output_file', help='Full path for output CSV.', required=True, type=str)
    parser.add_argument('-t', '--temp_dir', help='Temporary directory for worker chunks.', default='./temp_hydro_chunks', type=str)
    parser.add_argument('-w', '--workers', help='Number of worker processes.', default=None, type=int)

    args = parser.parse_args()

    run_hydro_extraction(
        root_dir=args.root_dir,
        target_filename=args.target_filename,
        output_file=args.output_file,
        temp_dir=args.temp_dir,
        max_workers=args.workers,
    )