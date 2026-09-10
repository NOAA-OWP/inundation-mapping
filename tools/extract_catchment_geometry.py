import argparse
import logging
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


"""
Step 2 of the data preparation for ML calibration coefficient workflow.
Extracts catchment attributes (areasqkm and LengthKm) from branch GeoPackages
via SQLite queries.
Left-merges geometry attributes onto the extracted hydrotable feature table from step 1.
"""

logger = logging.getLogger("PR_Pipeline.Step2")


def ensure_dir(directory: str) -> None:
    """
    Ensure directory exists, creating parent directories if necessary.

    Parameters
    ----------
    directory : str
        Path to directory.
    """
    os.makedirs(directory, exist_ok=True)


def fetch_branch_attributes(task: Tuple[str, str, str]) -> Optional[pd.DataFrame]:
    """
    Read HydroID, areasqkm, and LengthKm from a branch GPKG.

    Parameters
    ----------
    task : Tuple[str, str, str]
       Root directory, huc8, branch_id.

    Returns
    -------
    Optional[pd.DataFrame]
        DataFrame with huc8, branch_id, HydroID, areasqkm, LengthKm,
        or None if file is missing, empty, or unreadable.
    """
    root_dir, huc8, branch_id = task
    folder = os.path.join(root_dir, huc8, 'branches', str(branch_id))
    gpkg_path = os.path.join(
        folder, f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg"
    )

    if not os.path.exists(gpkg_path):
        return None

    try:
        uri_path = f"file:{gpkg_path}?mode=ro"
        with sqlite3.connect(uri_path, uri=True) as conn:
            cursor = conn.cursor()

            # Retrieve layer table name from GPKG metadata
            cursor.execute("SELECT table_name FROM gpkg_contents WHERE data_type = 'features' LIMIT 1")
            row = cursor.fetchone()
            if not row:
                return None
            table_name = row[0]

            # Inspect available columns
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            cols = {c[1] for c in cursor.fetchall()}

            target_cols = ['HydroID', 'areasqkm', 'LengthKm']
            query_cols = [c for c in target_cols if c in cols]

            if 'HydroID' not in query_cols or len(query_cols) < 2:
                return None

            query = f"SELECT {', '.join(query_cols)} FROM '{table_name}'"
            cursor.execute(query)
            data = cursor.fetchall()

            df_subset = pd.DataFrame(data, columns=query_cols)
            df_subset['huc8'] = str(huc8).zfill(8)
            df_subset['branch_id'] = str(branch_id)
            df_subset['HydroID'] = df_subset['HydroID'].astype(str)

            return df_subset

    except Exception:
        return None


def run_gpkg_extraction(
    input_csv: str, root_dir: str, output_file: str, max_threads: Optional[int] = None
) -> bool:
    """
    Extracts geometry attributes from all branch GPKGs and merges with features from step 1.

    Parameters
    ----------
    input_csv : str
        Path to Step 1 features CSV.
    root_dir : str
        Root directory containing HUC8 run folders.
    output_file : str
        Destination path for geometry CSV.
    max_threads : int, optional
        Number of worker threads for parallel SQLite reads. If None, auto-detected.

    Returns
    -------
    bool
        True if extraction and merging succeeded, False otherwise.
    """
    logger.info("=" * 80)
    logger.info("Extracting GeoPackage Geometry (areasqkm, LengthKm)")
    logger.info("=" * 80)

    if not os.path.isfile(input_csv):
        logger.error(
            f"Input feature file '{input_csv}' does not exist. Run Step 1 first (htable_feature_extractor.py)."
        )
        return False

    ensure_dir(os.path.dirname(os.path.abspath(output_file)))

    logger.info(f"Loading input features from '{input_csv}'...")
    dtype_dict = {'huc8': str, 'branch_id': str, 'feature_id': str, 'HydroID': str}
    df = pd.read_csv(input_csv, dtype=dtype_dict)
    df['huc8'] = df['huc8'].str.zfill(8)

    # Clean existing geometry columns if present
    for col in ['areasqkm', 'LengthKm']:
        if col in df.columns:
            df = df.drop(columns=[col])

    unique_branches = df[['huc8', 'branch_id']].drop_duplicates().to_dict('records')
    tasks = [(root_dir, item['huc8'], item['branch_id']) for item in unique_branches]

    if max_threads is None:
        max_threads = min(32, (os.cpu_count() or 4) * 4)

    logger.info(f"Querying {len(tasks):,} branch GPKGs using {max_threads} concurrent threads...")
    extracted_data: List[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(fetch_branch_attributes, t) for t in tasks]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Reading GPKGs"):
            res = future.result()
            if res is not None and not res.empty:
                extracted_data.append(res)

    logger.info(f"Extracted geometry records from {len(extracted_data):,} branch GPKGs.")
    logger.info("Merging geometry attributes with feature table...")
    if extracted_data:
        lookup_df = pd.concat(extracted_data, ignore_index=True)
        lookup_df = lookup_df.drop_duplicates(subset=['huc8', 'branch_id', 'HydroID'])
        final_df = df.merge(lookup_df, on=['huc8', 'branch_id', 'HydroID'], how='left')
    else:
        final_df = df.copy()

    if 'areasqkm' not in final_df.columns:
        final_df['areasqkm'] = np.nan
    if 'LengthKm' not in final_df.columns:
        final_df['LengthKm'] = np.nan

    matched_area = final_df['areasqkm'].notna().sum()
    logger.info(
        f"Geometry coverage: {matched_area:,} / {len(final_df):,} reaches ({matched_area / len(final_df) * 100:.1f}%) matched."
    )

    final_df.to_csv(output_file, index=False)
    logger.info(f"[Step 2 Complete] Geometry-enriched features saved to: {output_file}")
    return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(
        description="Extract areasqkm and LengthKm from branch GeoPackages and merge with hydro features."
    )
    parser.add_argument('-i', '--input_csv', help='Input feature CSV from Step 1.', required=True, type=str)
    parser.add_argument(
        '-r', '--root_dir', help='Root directory containing HUC8 run folders.', required=True, type=str
    )
    parser.add_argument('-o', '--output_file', help='Output enriched CSV filepath.', required=True, type=str)
    parser.add_argument(
        '-w', '--workers', help='Number of worker threads for GPKG reading.', default=None, type=int
    )

    args = parser.parse_args()

    run_gpkg_extraction(
        input_csv=args.input_csv,
        root_dir=args.root_dir,
        output_file=args.output_file,
        max_threads=args.workers,
    )
