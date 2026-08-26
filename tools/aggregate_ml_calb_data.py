import argparse
import logging
import os
import re
from typing import Optional

import pandas as pd
from tools_shared_functions import filter_usgs_by_acceptance_criteria


"""
Step 3 of the data preparation for ML calibration coefficient workflow:
Preparing and cleaning Machine Learning Dataset.

This script:
1. Aggregates USGS elevation tables (usgs_elev_table.csv) across HUCs and applies
   USGS rating acceptance criteria filtering (for USGS rating curve adjustment).
2. Isolates ground-truth calibration reaches (requiring non-null submitter).
3. Cleans & deduplicates USGS ratings against aggregated USGS elevation records (to only use the gage locations).
4. Aggregates multi-observation reaches for Point Observations
   and RAS2FIM using reach-level median calibration coefficients (for reaches with more than one observation).
5. Merges CONUS constant environmental covariates (ML_conus_constant_inputs).
6. Exports:
   - clean_calb_train_data.csv (ground-truth calibration records)
   - ml_input.parquet / ml_input.csv (Training dataset for ML models)
   - prediction_input.parquet (Inference dataset of ungaged reaches across CONUS)
"""

logger = logging.getLogger("PR_Pipeline.Step3")

# HUC prefixes to exclude (OCONUS)
OCONUS_PREFIXES = ('19', '20', '21', '22')


def ensure_dir(directory: str) -> None:
    """
    Ensure directory exists, creating parent directories if necessary.

    Parameters
    ----------
    directory : str
        Path to directory.
    """
    os.makedirs(directory, exist_ok=True)


def aggregate_usgs_elevations(fim_dir: str, output_dir: str, apply_filter: bool = True) -> pd.DataFrame:
    """
    Scans HUC directories within a FIM run output directory, filters USGS records
    by acceptance criteria, and aggregates usgs_elev_table.csv.

    Parameters
    ----------
    fim_dir : str
        Path to FIM run output directory containing HUC subfolders.
    output_dir : str
        Directory where aggregated USGS elevation table will be saved.
    apply_filter : bool, optional
        Whether to apply USGS rating acceptance criteria filtering (default: True).

    Returns
    -------
    pd.DataFrame
        Aggregated USGS elevation DataFrame.
    """
    if not os.path.isdir(fim_dir):
        logger.error(f"FIM directory '{fim_dir}' does not exist.")
        return pd.DataFrame()

    ensure_dir(output_dir)
    huc_list = [
        h for h in os.listdir(fim_dir) if re.search(r"^\d{6,8}$", h) and not h.startswith(OCONUS_PREFIXES)
    ]
    huc_list.sort()

    merged_tables = []
    logger.info(f"Scanning '{fim_dir}' for HUC-level 'usgs_elev_table.csv' files ({len(huc_list)} HUCs)...")

    for huc in huc_list:
        elev_table_path = os.path.join(fim_dir, huc, "usgs_elev_table.csv")
        if os.path.isfile(elev_table_path):
            try:
                df = pd.read_csv(
                    elev_table_path, dtype={"location_id": str, "HydroID": str, "huc": str, "feature_id": int}
                )
                df["huc"] = str(huc).zfill(8)

                if apply_filter:
                    if filter_usgs_by_acceptance_criteria is not None:
                        df = filter_usgs_by_acceptance_criteria(df)
                    else:
                        logger.warning(
                            "'filter_usgs_by_acceptance_criteria' unavailable. Skipping acceptance filter."
                        )

                merged_tables.append(df)
            except Exception as e:
                logger.error(f"Error reading {elev_table_path}: {e}")

    if merged_tables:
        agg_df = pd.concat(merged_tables, ignore_index=True)
        if "dem_elevation" in agg_df.columns and "dem_adj_elevation" in agg_df.columns:
            agg_df["thal_burn_depth_meters"] = agg_df["dem_elevation"] - agg_df["dem_adj_elevation"]

        output_csv_path = os.path.join(output_dir, "agg_usgs_elev_table.csv")
        agg_df.to_csv(output_csv_path, index=False)
        logger.info(
            f"Aggregated {len(merged_tables)} USGS elevation tables ({len(agg_df):,} records) into: {output_csv_path}"
        )
        return agg_df
    else:
        logger.warning("No 'usgs_elev_table.csv' files found.")
        return pd.DataFrame()


def aggregate_branch_observations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the median calibration coefficient (calb_coef_final) per unique
    (huc8, branch_id, feature_id) reach while preserving the first instance
    of all other metadata columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input observations DataFrame containing calibration records.

    Returns
    -------
    pd.DataFrame
        Aggregated reach-level DataFrame with median calibration coefficients.
    """
    if df.empty:
        return df

    # Deduplicate repeated values on the same reach before computing median
    df = df.drop_duplicates(subset=["huc8", "branch_id", "feature_id", "calb_coef_final"])

    group_cols = ["huc8", "branch_id", "feature_id"]
    agg_dict = {col: "first" for col in df.columns if col not in group_cols and col != "calb_coef_final"}
    agg_dict["calb_coef_final"] = "median"

    return df.groupby(group_cols, as_index=False).agg(agg_dict)


def run_ml_prep(
    features_csv_path: str,
    fim_dir: str,
    output_dir: str = './ml_output',
    const_inputs_path: Optional[str] = None,
    apply_acceptance_filter: bool = True,
) -> bool:
    """
    Cleans calibration coefficient and prepares ML training & prediction datasets.

    Parameters
    ----------
    features_csv_path : str
        Path to step 2 geometry-enriched features file.
    fim_dir : str
        Path to FIM run output directory with HUC folders.
    output_dir : str, optional
        Output directory for ML datasets and intermediate tables (default: ./pr_output).
    const_inputs_path : str, optional
        Path to conus_constant_inputs file (.parquet or .csv).
    apply_acceptance_filter : bool, optional
        Whether to apply USGS rating acceptance criteria filtering (default: True).

    Returns
    -------
    bool
        True if ML dataset preparation succeeded, False otherwise.
    """
    logger.info("=" * 80)
    logger.info("Data cleaning & ML training & prediction dataset preparation")
    logger.info("=" * 80)

    if not os.path.isfile(features_csv_path):
        logger.error(f"Features file '{features_csv_path}' not found. Run step 2 first.")
        return False

    ensure_dir(output_dir)

    logger.info(f"Loading extracted features from '{features_csv_path}'...")
    reaches_var = pd.read_csv(
        features_csv_path, dtype={"huc8": str, "HydroID": str, "feature_id": int, "branch_id": int}
    )
    reaches_var["huc8"] = reaches_var["huc8"].str.zfill(8)

    # Separate calibrated vs uncalibrated reaches
    calibrated_reaches = reaches_var.dropna(subset=["calb_coef_final"])
    uncalibrated_reaches = reaches_var[reaches_var["calb_coef_final"].isna()].copy()
    logger.info(
        f"Calibrated reach records: {len(calibrated_reaches):,} | "
        f"Uncalibrated (inference) records: {len(uncalibrated_reaches):,}"
    )

    # Filter out traced/propagated reaches by requiring non-null submitter for ground truth
    # For RAS2FIM and point obs, if the submitter column is NaN, it meas the feature_id/HydroID is not a gage.
    calibrated_reaches_cleaned = calibrated_reaches.dropna(subset=["submitter"]).copy()

    # Aggregate USGS elevation tables
    usgs_elev = aggregate_usgs_elevations(fim_dir, output_dir, apply_filter=apply_acceptance_filter)

    # Process USGS Ratings
    usgs_rating = calibrated_reaches_cleaned[calibrated_reaches_cleaned["obs_source"] == "usgs_rating"].copy()
    if not usgs_elev.empty and not usgs_rating.empty:
        usgs_elev["HydroID"] = usgs_elev["HydroID"].astype(str)
        usgs_elev["branch_id"] = usgs_elev["levpa_id"].astype(int)

        # For USGS rating curve adjustments, a non-null 'submitter' does not
        # guarantee the reach contains a USGS gage (e.g. adjustments may be propagated along the reach).
        # Join with 'usgs_elev_table' to isolate reaches with gages.
        usgs_gage = usgs_rating.merge(
            usgs_elev[["feature_id", "HydroID", "branch_id"]],
            on=["feature_id", "HydroID", "branch_id"],
            how="inner",
        )
        final_usgs = usgs_gage.drop_duplicates(subset=["huc8", "branch_id", "feature_id"]).reset_index(
            drop=True
        )
    else:
        final_usgs = usgs_rating.drop_duplicates(subset=["huc8", "branch_id", "feature_id"]).reset_index(
            drop=True
        )

    logger.info(f"Processed USGS ratings: {len(final_usgs):,} branch reaches.")

    # Process point observations
    point_obs = calibrated_reaches_cleaned[calibrated_reaches_cleaned["obs_source"] == "point_obs"].copy()
    final_point_obs = aggregate_branch_observations(point_obs)
    logger.info(f"Processed Point observations: {len(final_point_obs):,} branch reaches.")

    # Process RAS2FIM Ratings
    ras2fim = calibrated_reaches_cleaned[calibrated_reaches_cleaned["obs_source"] == "ras2fim_rating"].copy()
    final_ras2fim = aggregate_branch_observations(ras2fim)
    logger.info(f"Processed RAS2FIM ratings: {len(final_ras2fim):,} branch reaches.")

    # Concatenate all observation sources
    all_calibrations = pd.concat([final_ras2fim, final_point_obs, final_usgs], ignore_index=True)
    clean_calb_csv = os.path.join(output_dir, "clean_calb_train_data.csv")
    all_calibrations.to_csv(clean_calb_csv, index=False)
    logger.info(
        f"Saved aggregated training calibrations ({len(all_calibrations):,} records) to: {clean_calb_csv}"
    )

    # Merge with CONUS constant environmental inputs
    if const_inputs_path and os.path.exists(const_inputs_path):
        logger.info(f"Merging with CONUS constant attributes from '{const_inputs_path}'...")
        const = pd.read_parquet(const_inputs_path)

        const["huc8"] = const["huc8"].astype(str).str.zfill(8)
        all_calibrations["huc8"] = all_calibrations["huc8"].astype(str).str.zfill(8)
        uncalibrated_reaches["huc8"] = uncalibrated_reaches["huc8"].astype(str).str.zfill(8)

        # Training dataset
        ml_input = all_calibrations.merge(const, on=["feature_id", "huc8"], how="left")
        ml_parquet_out = os.path.join(output_dir, "ml_training_input.parquet")
        ml_input.to_parquet(ml_parquet_out, index=False)
        logger.info(
            f"-> Saved ML Training Dataset ({ml_input.shape[0]:,} rows, {ml_input.shape[1]} columns): {ml_parquet_out}"
        )

        # Inference dataset
        prediction_input = uncalibrated_reaches.merge(const, on=["feature_id", "huc8"], how="left")
        pred_parquet_out = os.path.join(output_dir, "prediction_input.parquet")
        prediction_input.to_parquet(pred_parquet_out, index=False)
        logger.info(
            f"-> Saved Ungauged Prediction Dataset ({prediction_input.shape[0]:,} rows, {prediction_input.shape[1]} columns): {pred_parquet_out}"
        )

    logger.info("All datasets successfully prepared!")
    return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(
        description="Clean calibration observations and merge CONUS constants for ML training."
    )
    parser.add_argument(
        '-i', '--features_csv', help='Input feature CSV from Stage 2.', required=True, type=str
    )
    parser.add_argument(
        '-d', '--fim_dir', help='Root FIM directory containing HUC subfolders.', required=True, type=str
    )
    parser.add_argument(
        '-c', '--const_inputs', help='Path to conus_constant_inputs.parquet.', default=None, type=str
    )
    parser.add_argument('-o', '--output_dir', help='Output directory.', default='./ml_output', type=str)
    parser.add_argument(
        '--no-filter', dest='apply_filter', action='store_false', help='Disable USGS acceptance filter.'
    )

    args = parser.parse_args()

    run_ml_prep(
        features_csv_path=args.features_csv,
        fim_dir=args.fim_dir,
        output_dir=args.output_dir,
        const_inputs_path=args.const_inputs,
        apply_acceptance_filter=args.apply_filter,
    )
