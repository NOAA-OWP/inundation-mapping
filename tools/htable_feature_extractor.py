#!/usr/bin/env python3

import argparse
import os
import shutil
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm


cols_to_read = [
    "calb_coef_final",
    "HydroID",
    "feature_id",
    "branch_id",
    "obs_source",
    'submitter',
    "SLOPE",
    'SLOPE_RISE_RUN',
]


dtype = {
    "calb_coef_final": 'float32',
    "HydroID": 'int64',
    "feature_id": 'int64',
    "branch_id": 'int64',
    'obs_source': 'object',
    'submitter': 'object',
    "SLOPE": 'float32',
    "SLOPE_RISE_RUN": 'float32',
}

chunk_size = 100000

# To exclude oconus hucs (19: Alaska, 20: Hawaii, 22: Guam and American Samoa)
oconus_prefixes = ('19', '20', '22')


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def append_file_to_master(source_path, dest_handle, chunk_size=1024 * 1024):
    """
    Reads a source file in binary chunks and writes to the destination handle.
    Merging files without loading them into RAM.
    """
    with open(source_path, 'rb') as fsrc:
        while True:
            buf = fsrc.read(chunk_size)
            if not buf:
                break
            dest_handle.write(buf)


def process_csv_to_temp(file_info):
    """
    Reads input in chunks, filters them, and writes directly to a unique temp file.
    Returns: Path to the temp file (if data was found), or None.
    """
    file_path, temp_dir = file_info

    # Extract huc8
    huc8 = os.path.basename(os.path.dirname(os.path.normpath(file_path)))

    # Create a unique temp filename to avoid collisions between workers
    unique_id = uuid.uuid4().hex
    temp_file_path = os.path.join(temp_dir, f"{huc8}_{unique_id}.csv")

    data_found = False
    # a set to remember which [HydroID, feature_id] combos we have seen in this file
    seen_combos = set()
    try:
        output_cols = ['huc8'] + cols_to_read
        # Open the temp file in append mode
        with open(temp_file_path, 'w', encoding='utf-8') as f_out:
            # Iterate through the source file in chunks
            with pd.read_csv(file_path, usecols=cols_to_read, dtype=dtype, chunksize=chunk_size) as reader:
                for chunk in reader:
                    # drop duplicates in each chunk
                    chunk = chunk.drop_duplicates(subset=['HydroID', 'feature_id'])
                    is_new = [
                        (h, f) not in seen_combos for h, f in zip(chunk['HydroID'], chunk['feature_id'])
                    ]

                    # Filter logic
                    valid_rows = chunk[is_new].copy()

                    if not valid_rows.empty:
                        valid_rows['huc8'] = huc8
                        # Track combos so we drop them next time
                        seen_combos.update(zip(valid_rows['HydroID'], valid_rows['feature_id']))
                        valid_rows = valid_rows[output_cols]

                        # Avoid writing header to temp file,
                        # because we will append this purely as data rows later.
                        csv_chunk = valid_rows.to_csv(header=False, index=False)

                        f_out.write(csv_chunk)
                        data_found = True

        # Delete the empty temp file and return None
        if not data_found:
            os.remove(temp_file_path)
            return None

        return temp_file_path

    except Exception as e:
        return f"Error processing {file_path}: {str(e)}"


def run_prep(root_dir, target_filename, output_file, temp_dir):
    ensure_dir(temp_dir)

    print(f"Scanning {root_dir} for {target_filename}...")
    file_list = []
    for root, dirs, files in os.walk(root_dir):
        if target_filename in files:
            huc8 = os.path.basename(root)

            # Skip oconus hucs
            if huc8.startswith(oconus_prefixes):
                continue
            file_list.append(os.path.join(root, target_filename))

    print(f"Found {len(file_list)} files.")

    if len(file_list) == 0:
        print(f"No files matching {target_filename} found. Exiting.")
        return

    # Initialize Master Output File with Header
    output_columns = ['huc8'] + cols_to_read
    pd.DataFrame(columns=output_columns).to_csv(output_file, index=False)

    print("Starting processing...")

    # Limit workers to CPU count to avoid thrashing disk I/O
    max_workers = min(6, os.cpu_count() or 4)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        tasks = [(f, temp_dir) for f in file_list]
        future_to_file = {executor.submit(process_csv_to_temp, t): t[0] for t in tasks}

        with open(output_file, 'ab') as master_file:
            with tqdm(total=len(file_list), unit="file") as pbar:
                for future in as_completed(future_to_file):
                    result_path = future.result()

                    if result_path and os.path.exists(result_path) and not result_path.startswith("Error"):
                        try:
                            append_file_to_master(result_path, master_file)
                            os.remove(result_path)
                        except Exception as e:
                            print(f"Error merging {result_path}: {e}")

                    elif result_path and result_path.startswith("Error"):
                        print(f"\n{result_path}")

                    pbar.update(1)

    # Cleanup
    try:
        os.rmdir(temp_dir)
    except Exception:
        pass

    print(f"\nProcessing complete. All data merged into {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract and concatenate specific columns from hydrotables.")
    parser.add_argument('-r', '--root_dir', help='Root directory to scan for files.', required=True, type=str)
    parser.add_argument(
        '-f',
        '--target_filename',
        help='Target filename to search for (default: hydrotable.csv).',
        default='hydrotable.csv',
        required=False,
        type=str,
    )
    parser.add_argument(
        '-o', '--output_file', help='Full filepath for the master output csv.', required=True, type=str
    )
    parser.add_argument(
        '-t', '--temp_dir', help='Temporary directory for worker outputs.', required=True, type=str
    )

    args = vars(parser.parse_args())

    run_prep(
        root_dir=args['root_dir'],
        target_filename=args['target_filename'],
        output_file=args['output_file'],
        temp_dir=args['temp_dir'],
    )
