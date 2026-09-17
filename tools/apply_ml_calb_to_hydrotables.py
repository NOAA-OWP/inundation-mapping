#!/usr/bin/env python3
import argparse
import glob
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


"""
Apply Machine Learning Calibration Coefficients to FIM Branch HydroTables.

This script:
1. Loads ML calibration coefficient predictions.
2. Traverses FIM output directory HUCs and branch folders.
3. For each branch hydroTable:
   - Preserves uncalibrated original discharge in 'precalb_discharge_cms'.
   - Merges reach-level ML calibration coefficient.
   - Calibrates discharge_cms = precalb_discharge_cms / prediction_calb.
   - Sets calibration tracking metadata (calb_applied, calb_coef_final,
     obs_source='ml_calb', submitter='ml_calb_xgb', last_updated).
4. Optionally re-aggregates branch hydroTables into HUC-level hydrotable.csv,
   hydrotable.feather, and hydrotable.parquet.
"""

logger = logging.getLogger("apply_ml_calb")


def setup_logger(output_dir: Optional[str] = None) -> str:
    """
    Set up dual console and file logging.

    Parameters
    ----------
    output_dir : Optional[str]
        Directory where log file will be saved. If None, uses current working directory.

    Returns
    -------
    str
        Path to the created log file.
    """
    log_dir = output_dir if output_dir else os.getcwd()
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"apply_ml_calb_{timestamp}.log")

    logger.setLevel(logging.INFO)
    logger.handlers = []

    file_handler = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return log_file_path


def apply_calibration_to_branch(
    file_path: str, branch_preds: pd.DataFrame, overwrite_existing: bool = False, dry_run: bool = False
) -> Dict[str, Any]:
    """
    Apply ML calibration predictions to a single branch hydroTable.

    file_path : str
        Path to branch hydroTable.
    branch_preds : pd.DataFrame
        DataFrame containing reach predictions for this branch with columns.
    overwrite_existing : bool, optional
        If True, overwrites existing calibrations. If False, only applies to
        uncalibrated reaches (calb_applied != True or calb_coef_final is NaN).
    dry_run : bool, optional
        If True, computes updates and stats without saving to disk.

    Returns
    -------
    Dict[str, Any]
        Dictionary with execution status and metrics.
    """
    if not os.path.isfile(file_path):
        return {"status": "missing", "file_path": file_path, "rows_updated": 0, "reaches_updated": 0}

    try:
        hydro_df = pd.read_csv(file_path, low_memory=False)

        hydro_df["HydroID"] = pd.to_numeric(hydro_df["HydroID"], errors="coerce").astype("Int64")
        if "feature_id" in hydro_df.columns:
            hydro_df["feature_id"] = pd.to_numeric(hydro_df["feature_id"], errors="coerce").astype("Int64")

        # Ensure baseline uncalibrated discharge exists in 'precalb_discharge_cms'
        if "precalb_discharge_cms" not in hydro_df.columns:
            if "discharge_cms" in hydro_df.columns:
                hydro_df["precalb_discharge_cms"] = hydro_df["discharge_cms"]
            else:
                return {
                    "status": "error",
                    "file_path": file_path,
                    "error": "Missing discharge_cms column",
                    "rows_updated": 0,
                    "reaches_updated": 0,
                }
        else:
            if "discharge_cms" in hydro_df.columns:
                hydro_df["precalb_discharge_cms"] = hydro_df["precalb_discharge_cms"].fillna(
                    hydro_df["discharge_cms"]
                )

        join_cols = ["HydroID"]
        if "feature_id" in hydro_df.columns and "feature_id" in branch_preds.columns:
            join_cols.append("feature_id")

        preds_dedup = branch_preds[join_cols + ["prediction_calb"]].drop_duplicates(subset=join_cols)

        hydro_df = hydro_df.merge(preds_dedup, on=join_cols, how="left")

        valid_pred = (hydro_df["prediction_calb"].notna()) & (hydro_df["prediction_calb"] > 0)
        if not valid_pred.any():
            hydro_df.drop(columns=["prediction_calb"], inplace=True)
            return {"status": "no_match", "file_path": file_path, "rows_updated": 0, "reaches_updated": 0}

        if overwrite_existing:
            apply_mask = valid_pred
        else:
            # To make sure, we don't update the calibrated gage
            is_calibrated = pd.Series(False, index=hydro_df.index)
            if "calb_applied" in hydro_df.columns:
                is_calibrated = is_calibrated | (hydro_df["calb_applied"].astype(str).str.lower() == "true")
            if "calb_coef_final" in hydro_df.columns:
                is_calibrated = is_calibrated | (hydro_df["calb_coef_final"].notna())

            apply_mask = valid_pred & (~is_calibrated)

        if not apply_mask.any():
            hydro_df.drop(columns=["prediction_calb"], inplace=True)
            return {
                "status": "skipped_existing",
                "file_path": file_path,
                "rows_updated": 0,
                "reaches_updated": 0,
            }

        # Apply calibration: discharge_cms = precalb_discharge_cms / prediction_calb
        hydro_df.loc[apply_mask, "discharge_cms"] = (
            hydro_df.loc[apply_mask, "precalb_discharge_cms"] / hydro_df.loc[apply_mask, "prediction_calb"]
        )

        # Maintain zeros and nodata flags (-999.0)
        zero_mask = hydro_df["precalb_discharge_cms"] == 0.0
        nodata_mask = hydro_df["precalb_discharge_cms"] == -999.0
        hydro_df.loc[zero_mask, "discharge_cms"] = 0.0
        hydro_df.loc[nodata_mask, "discharge_cms"] = -999.0

        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        #  Set tracking columns
        if "calb_applied" not in hydro_df.columns:
            hydro_df["calb_applied"] = False
        if "calb_coef_final" not in hydro_df.columns:
            hydro_df["calb_coef_final"] = np.nan
        if "obs_source" not in hydro_df.columns:
            hydro_df["obs_source"] = pd.NA
        if "submitter" not in hydro_df.columns:
            hydro_df["submitter"] = pd.NA
        if "last_updated" not in hydro_df.columns:
            hydro_df["last_updated"] = pd.NA

        hydro_df.loc[apply_mask, "calb_applied"] = True
        hydro_df.loc[apply_mask, "calb_coef_final"] = hydro_df.loc[apply_mask, "prediction_calb"]
        hydro_df.loc[apply_mask, "obs_source"] = "ml_calb"
        hydro_df.loc[apply_mask, "submitter"] = "ml_calb_xgb"
        hydro_df.loc[apply_mask, "last_updated"] = current_time

        rows_updated = int(apply_mask.sum())
        reaches_updated = int(hydro_df.loc[apply_mask, "HydroID"].nunique())

        hydro_df.drop(columns=["prediction_calb"], inplace=True)

        if not dry_run:
            hydro_df.to_csv(file_path, index=False)

        return {
            "status": "updated",
            "file_path": file_path,
            "rows_updated": rows_updated,
            "reaches_updated": reaches_updated,
        }

    except Exception as e:
        return {
            "status": "error",
            "file_path": file_path,
            "error": str(e),
            "rows_updated": 0,
            "reaches_updated": 0,
        }


def reaggregate_huc_tables(huc_dir: str) -> bool:
    """
    Re-aggregate branch hydroTable into HUC level hydrotable.csv, .feather, and .parquet.

    huc_dir : str
        Path to the HUC directory.

    Returns
    -------
    bool
        True if reaggregation succeeded, False otherwise.
    """
    try:
        branches_dir = os.path.join(huc_dir, "branches")
        if not os.path.isdir(branches_dir):
            return False

        branch_hydro_files = []
        for b_id in os.listdir(branches_dir):
            b_dir = os.path.join(branches_dir, b_id)
            if not os.path.isdir(b_dir):
                continue
            for fname in os.listdir(b_dir):
                if fname.lower().startswith("hydrotable_") and fname.lower().endswith(".csv"):
                    branch_hydro_files.append((b_id, os.path.join(b_dir, fname)))

        if not branch_hydro_files:
            return False

        tables = []
        for b_id, bf in branch_hydro_files:
            b_df = pd.read_csv(bf, low_memory=False)
            try:
                b_df["branch_id"] = int(b_id)
            except (ValueError, TypeError):
                b_df["branch_id"] = pd.NA
            tables.append(b_df)

        if not tables:
            return False

        agg_df = pd.concat(tables, ignore_index=True)
        huc_id = os.path.basename(os.path.normpath(huc_dir))

        if "HUC" not in agg_df.columns:
            agg_df["HUC"] = huc_id
        else:
            agg_df["HUC"] = agg_df["HUC"].fillna(huc_id)

        huc_csv = os.path.join(huc_dir, "hydrotable.csv")
        huc_feather = os.path.join(huc_dir, "hydrotable.feather")
        huc_parquet = os.path.join(huc_dir, "hydrotable.parquet")

        agg_df.to_csv(huc_csv, index=False)

        htable_req_cols = [
            "HUC",
            "branch_id",
            "feature_id",
            "HydroID",
            "stage",
            "discharge_cms",
            "SurfaceArea (m2)",
            "LakeID",
            "Bathymetry_source",
        ]

        available_cols = [c for c in htable_req_cols if c in agg_df.columns]
        if len(available_cols) >= 6:
            temp_df = agg_df[available_cols].copy()
            if "HUC" in temp_df.columns:
                temp_df["HUC"] = temp_df["HUC"].astype(str)
            if "branch_id" in temp_df.columns:
                temp_df["branch_id"] = (
                    pd.to_numeric(temp_df["branch_id"], errors="coerce").fillna(0).astype(int)
                )
            if "feature_id" in temp_df.columns:
                temp_df["feature_id"] = temp_df["feature_id"].astype(str)
            if "HydroID" in temp_df.columns:
                temp_df["HydroID"] = temp_df["HydroID"].astype(str)
            if "stage" in temp_df.columns:
                temp_df["stage"] = pd.to_numeric(temp_df["stage"], errors="coerce").astype(float)
            if "discharge_cms" in temp_df.columns:
                temp_df["discharge_cms"] = pd.to_numeric(temp_df["discharge_cms"], errors="coerce").astype(
                    float
                )
            if "SurfaceArea (m2)" in temp_df.columns:
                temp_df["SurfaceArea (m2)"] = (
                    pd.to_numeric(temp_df["SurfaceArea (m2)"], errors="coerce").fillna(0).astype(int)
                )
            if "LakeID" in temp_df.columns:
                temp_df["LakeID"] = pd.to_numeric(temp_df["LakeID"], errors="coerce").fillna(0).astype(int)

            try:
                temp_df.reset_index(drop=True).to_feather(huc_feather)
            except Exception:
                pass

            try:
                temp_df.to_parquet(huc_parquet, index=False)
            except Exception:
                pass

        return True

    except Exception as e:
        logger.warning(f"Reaggregation failed for {huc_dir}: {e}")
        return False


def process_huc(task_args: Tuple[str, str, pd.DataFrame, bool, bool, bool]) -> Dict[str, Any]:
    """
    Process all branches within a single HUC.

        - huc_dir: Directory path for the HUC.
        - huc8: HUC8 identifier.
        - huc_preds: DataFrame of predictions for this HUC.
        - overwrite_existing: Overwrite policy.
        - reaggregate: Whether to reaggregate HUC tables.
        - dry_run: Dry-run simulation mode.

    Returns
    -------
    Dict[str, Any]
        Aggregated processing metrics for the HUC.
    """
    huc_dir, huc8, huc_preds, overwrite_existing, reaggregate, dry_run = task_args

    branches_dir = os.path.join(huc_dir, "branches")
    if not os.path.isdir(branches_dir):
        return {
            "huc8": huc8,
            "status": "no_branches_dir",
            "branches_updated": 0,
            "rows_updated": 0,
            "reaches_updated": 0,
            "errors": [],
        }

    branch_ids = [d for d in os.listdir(branches_dir) if os.path.isdir(os.path.join(branches_dir, d))]
    total_branches_updated = 0
    total_rows_updated = 0
    total_reaches_updated = 0
    errors = []

    has_branch_col = "branch_id" in huc_preds.columns
    if has_branch_col:
        preds_by_branch = {b_id: grp for b_id, grp in huc_preds.groupby("branch_id")}
    else:
        preds_by_branch = {}

    for b_id_str in branch_ids:
        hydro_table_path = os.path.join(branches_dir, b_id_str, f"hydroTable_{b_id_str}.csv")
        if not os.path.isfile(hydro_table_path):
            continue

        if has_branch_col:
            try:
                b_id_num = int(b_id_str)
                b_preds = preds_by_branch.get(b_id_num, pd.DataFrame())
            except ValueError:
                b_preds = pd.DataFrame()

            if b_preds.empty:
                b_preds = huc_preds
        else:
            b_preds = huc_preds

        if b_preds.empty:
            continue

        res = apply_calibration_to_branch(
            file_path=hydro_table_path,
            branch_preds=b_preds,
            overwrite_existing=overwrite_existing,
            dry_run=dry_run,
        )

        if res["status"] == "updated":
            total_branches_updated += 1
            total_rows_updated += res["rows_updated"]
            total_reaches_updated += res["reaches_updated"]
        elif res["status"] == "error":
            errors.append(f"Branch {b_id_str}: {res.get('error', 'unknown error')}")

    reagg_success = None
    if reaggregate and total_branches_updated > 0 and not dry_run:
        reagg_success = reaggregate_huc_tables(huc_dir)

    return {
        "huc8": huc8,
        "status": "partial_error" if errors else "success",
        "branches_updated": total_branches_updated,
        "rows_updated": total_rows_updated,
        "reaches_updated": total_reaches_updated,
        "reaggregate_success": reagg_success,
        "errors": errors,
    }


def run_ml_application(
    fim_dir: str,
    pred_file: str,
    max_workers: Optional[int] = None,
    limit_hucs: Optional[List[str]] = None,
    overwrite_existing: bool = False,
    reaggregate: bool = False,
    dry_run: bool = False,
    log_dir: Optional[str] = None,
) -> bool:
    """
    Orchestrate ML calibration application across all HUCs and branches.
    """
    start_total = time.time()
    resolved_log_dir = log_dir if log_dir else os.path.join(fim_dir, "logs")
    log_file = setup_logger(resolved_log_dir)

    logger.info("Apply ML Calibration Coefficients to FIM Hydrotables")
    logger.info(f"Log File:             {log_file}")

    if not os.path.isdir(fim_dir):
        logger.error(f"FIM directory '{fim_dir}' does not exist.")
        return False

    if not os.path.isfile(pred_file):
        logger.error(f"Predictions file '{pred_file}' does not exist.")
        return False

    t_load = time.time()
    if pred_file.endswith(".parquet"):
        pred_df = pd.read_parquet(pred_file)
    else:
        pred_df = pd.read_csv(pred_file, low_memory=False)

    logger.info(f"Loaded {len(pred_df):,} prediction records in {time.time() - t_load:.2f}s.")

    required_cols = {"huc8", "HydroID", "prediction_calb"}
    missing = required_cols - set(pred_df.columns)
    if missing:
        logger.error(f"Prediction file is missing required columns: {sorted(missing)}")
        return False

    pred_df["huc8"] = pred_df["huc8"].astype(str).str.zfill(8)
    pred_df["HydroID"] = pd.to_numeric(pred_df["HydroID"], errors="coerce").astype("Int64")
    if "feature_id" in pred_df.columns:
        pred_df["feature_id"] = pd.to_numeric(pred_df["feature_id"], errors="coerce").astype("Int64")
    if "branch_id" in pred_df.columns:
        pred_df["branch_id"] = pd.to_numeric(pred_df["branch_id"], errors="coerce").astype("Int64")

    pred_df = pred_df[(pred_df["prediction_calb"].notna()) & (pred_df["prediction_calb"] > 0)].copy()

    candidate_hucs = [
        d
        for d in os.listdir(fim_dir)
        if len(d) == 8 and d.isdigit() and os.path.isdir(os.path.join(fim_dir, d))
    ]

    if limit_hucs:
        limit_set = {str(h).zfill(8) for h in limit_hucs}
        active_hucs = [h for h in candidate_hucs if h in limit_set]
    else:
        active_hucs = candidate_hucs

    pred_huc_set = set(pred_df["huc8"].unique())
    active_hucs = [h for h in active_hucs if h in pred_huc_set]
    active_hucs.sort()

    logger.info(f"Found {len(active_hucs)} HUCs matching predictions to process.")
    if not active_hucs:
        logger.warning("No matching HUCs to process. Exiting.")
        return True

    huc_grouped = {huc: grp for huc, grp in pred_df.groupby("huc8") if huc in set(active_hucs)}

    task_data = [
        (os.path.join(fim_dir, huc), huc, huc_grouped[huc], overwrite_existing, reaggregate, dry_run)
        for huc in active_hucs
    ]

    workers = max_workers if max_workers else min(os.cpu_count() or 4, 8)

    total_branches_updated = 0
    total_rows_updated = 0
    total_reaches_updated = 0
    error_count = 0

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_huc, task): task[1] for task in task_data}
        with tqdm(total=len(futures), desc="Applying ML Calibration", unit="HUC") as pbar:
            for future in as_completed(futures):
                huc = futures[future]
                try:
                    result = future.result()
                    b_up = result["branches_updated"]
                    r_up = result["rows_updated"]
                    total_branches_updated += b_up
                    total_rows_updated += r_up
                    total_reaches_updated += result["reaches_updated"]

                    if result["errors"]:
                        error_count += len(result["errors"])
                        for err in result["errors"]:
                            logger.warning(f"[{huc}] {err}")

                    if b_up > 0:
                        logger.info(
                            f"[{huc}] Updated {b_up} branch(es), {result['reaches_updated']:,} reaches, "
                            f"{r_up:,} rows."
                        )
                except Exception as exc:
                    error_count += 1
                    logger.error(f"[{huc}] Task generated unhandled exception: {exc}")

                pbar.update(1)

    elapsed = time.time() - start_total
    logger.info(f"PROCESSING COMPLETE in {int(elapsed // 60)}m {int(elapsed % 60)}s")
    logger.info(f"Total Branches Updated: {total_branches_updated:,}")
    logger.info(f"Total Reaches Updated:  {total_reaches_updated:,}")
    logger.info(f"Errors Encountered:     {error_count}")
    logger.info(f"Log written to:         {log_file}")

    return error_count == 0


def main():
    """Apply machine learning calibration coefficients to branch hydroTable."""
    parser = argparse.ArgumentParser(
        description="Apply machine learning calibration coefficients to branch hydroTable."
    )
    parser.add_argument(
        "-r",
        "--fim-dir",
        dest="fim_dir",
        required=True,
        type=str,
        help="Root FIM run directory containing HUC subdirectories.",
    )
    parser.add_argument(
        "-p", "--pred-file", dest="pred_file", required=True, type=str, help=("Path to ML predictions file.")
    )
    parser.add_argument(
        "-w",
        "--workers",
        dest="workers",
        type=int,
        default=None,
        help="Number of parallel worker processes (default: min(CPU count, 8)).",
    )
    parser.add_argument(
        "-u",
        "--limit-hucs",
        dest="limit_hucs",
        nargs="+",
        default=None,
        help="Optional list of 8-digit HUCs to process (e.g. -u 12090301 12090302).",
    )
    parser.add_argument(
        "--overwrite-existing",
        dest="overwrite_existing",
        action="store_true",
        default=False,
        help=(
            "If set, overwrite existing calibrated reaches. Default: False "
            "(protects ground-truth gage calibrations)."
        ),
    )
    parser.add_argument(
        "--reaggregate",
        dest="reaggregate",
        action="store_true",
        default=False,
        help=(
            "If set, re-aggregates branch hydroTables into HUC-level "
            "hydrotable.csv, .feather, and .parquet."
        ),
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Simulate updates and calculate statistics without writing changes to disk.",
    )
    parser.add_argument(
        "-o",
        "--log-dir",
        dest="log_dir",
        type=str,
        default=None,
        help="Directory to write log files (default: <fim-dir>/logs).",
    )

    args = parser.parse_args()

    success = run_ml_application(
        fim_dir=args.fim_dir,
        pred_file=args.pred_file,
        max_workers=args.workers,
        limit_hucs=args.limit_hucs,
        overwrite_existing=args.overwrite_existing,
        reaggregate=args.reaggregate,
        dry_run=args.dry_run,
        log_dir=args.log_dir,
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
