import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

from aggregate_ml_calb_data import ensure_dir, run_ml_prep
from extract_catchment_geometry import run_gpkg_extraction
from htable_feature_extractor import run_hydro_extraction


"""
Run the full data preparation pipeline for ML calibration.
This pipelien has three steps:

  Step 1: Extract hydrotable features and rating curve parameters (a & b) (htable_feature_extractor.py)
           - Process hydrotable.csv across all HUCs
           - Extracts reach attributes such as slope, submitter, obs_source, calb_coef_final
           - Calculate the rating curve parameters (a, b)
           - Output: <output_dir>/step1_hydro_features_rc.csv

  Step 2: Add catchment geometry attributes (extract_catchment_geometry.py)
           - Extract areaskm and LengthKM.
           - Add these attributes to the step 1 output.
           - Output: <output_dir>/step2_features_with_geometry.csv

  Step 3: Build ML training & prediction dataset (aggregate_ml_calb_data.py)
           - Cleans calibration coefficients using reach-level medians (USGS, Point Obs, RAS2FIM)
           - Add CONUS constant environmental covariates
           - Outputs: clean_calb_train_data.csv, ml_training_input.parquet, prediction_input.parquet
"""

logger = logging.getLogger("ML_calb")


def setup_logger(output_dir: str) -> str:
    """
    Set up logging file.

    Parameters
    ----------
    output_dir : str
        Directory where output and log directory are stored.

    Returns
    -------
    str
        Path to the log file.
    """
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"ml_calb_prep_{timestamp}.log")

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Root logger configuration
    root_logger = logging.getLogger("ML_calb")
    root_logger.setLevel(logging.INFO)

    # File Handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Console Handler (if not already attached)
    if not any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root_logger.handlers
    ):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    return log_file


def main() -> None:
    """
    Run the ML calibration data preparation.
    """
    parser = argparse.ArgumentParser(description="Prepare data for the ML calibration model.")
    parser.add_argument(
        "-r",
        "--root-dir",
        dest="root_dir",
        type=str,
        required=True,
        help="Root directory containing HUC8 run output folders.",
    )
    parser.add_argument(
        "-c",
        "--const-inputs",
        dest="const_inputs",
        type=str,
        default=None,
        help="Path to conus_constant_inputs.parquet.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        dest="output_dir",
        type=str,
        default="./pr_pipeline_output",
        help="Output directory to save checkpoints, logs, and final ML datasets.",
    )
    parser.add_argument(
        "-t",
        "--temp-dir",
        dest="temp_dir",
        type=str,
        default="./temp_ml_chunks",
        help="Temporary directory for parallel worker chunks.",
    )
    parser.add_argument(
        "-f",
        "--target-filename",
        dest="target_filename",
        type=str,
        default="hydrotable.csv",
        help="Target hydrotable filename to search for (default: hydrotable.csv).",
    )
    parser.add_argument(
        "-w",
        "--workers",
        dest="workers",
        type=int,
        default=None,
        help="Number of CPU worker processes/threads (default: auto-detected).",
    )
    parser.add_argument(
        "--steps",
        dest="steps",
        type=str,
        default="1,2,3",
        help="Comma-separated list of stages to run (e.g., '1,2,3', '2,3', or '3'). Default: '1,2,3'.",
    )
    parser.add_argument(
        "--no-filter",
        dest="apply_filter",
        action="store_false",
        help="Disable USGS acceptance criteria filtering.",
    )
    parser.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="Force re-running stages even if checkpoint files already exist.",
    )

    args = parser.parse_args()

    ensure_dir(args.output_dir)
    log_file_path = setup_logger(args.output_dir)

    active_steps = [int(s.strip()) for s in args.steps.split(",") if s.strip().isdigit()]

    # Checkpoint File Paths
    step1_checkpoint = os.path.join(args.output_dir, "step1_hydro_features_rc.csv")
    step2_checkpoint = os.path.join(args.output_dir, "step2_features_with_geometry.csv")

    start_total = time.time()

    logger.info("=" * 80)
    logger.info("ML calibration dataset preparation")
    logger.info(f"Log File:             {log_file_path}")
    logger.info(f"Root FIM Directory:   {args.root_dir}")
    logger.info(f"Output Directory:     {args.output_dir}")
    logger.info(f"Active Steps:        {active_steps}")
    logger.info(f"Constant Inputs:      {args.const_inputs}")
    logger.info(f"Target Filename:      {args.target_filename}")
    logger.info(f"Worker Allocation:    {args.workers or 'Auto'}")
    logger.info(f"Force Rerun:          {args.force}")
    logger.info("=" * 80)

    # Step 1
    if 1 in active_steps:
        if not args.force and os.path.exists(step1_checkpoint):
            logger.info(f"Step 1 checkpoint exists at '{step1_checkpoint}'.")
        else:
            t1_start = time.time()
            success = run_hydro_extraction(
                root_dir=args.root_dir,
                target_filename=args.target_filename,
                output_file=step1_checkpoint,
                temp_dir=args.temp_dir,
                max_workers=args.workers,
            )
            if not success:
                logger.error("Step 1 failed. Aborting pipeline.")
                sys.exit(1)
            t1_elapsed = time.time() - t1_start
            logger.info(f"Step 1 Duration: {int(t1_elapsed // 60)}m {int(t1_elapsed % 60)}s")

    # Step 2
    if 2 in active_steps:
        if not args.force and os.path.exists(step2_checkpoint):
            logger.info(f"Step 2 checkpoint exists at '{step2_checkpoint}'.")
        else:
            if not os.path.exists(step1_checkpoint):
                logger.error(f"Step 1 checkpoint not found at '{step1_checkpoint}'. Cannot run Step 2.")
                sys.exit(1)

            t2_start = time.time()
            success = run_gpkg_extraction(
                input_csv=step1_checkpoint,
                root_dir=args.root_dir,
                output_file=step2_checkpoint,
                max_threads=args.workers,
            )
            if not success:
                logger.error("Step 2 failed. Aborting pipeline.")
                sys.exit(1)
            t2_elapsed = time.time() - t2_start
            logger.info(f"Step 2 Duration: {int(t2_elapsed // 60)}m {int(t2_elapsed % 60)}s")

    # Step 3
    if 3 in active_steps:
        if not os.path.exists(step2_checkpoint):
            logger.error(f"Step 2 checkpoint not found at '{step2_checkpoint}'. Cannot run Step 3.")
            sys.exit(1)

        t3_start = time.time()
        success = run_ml_prep(
            features_csv_path=step2_checkpoint,
            fim_dir=args.root_dir,
            output_dir=args.output_dir,
            const_inputs_path=args.const_inputs,
            apply_acceptance_filter=args.apply_filter,
        )
        if not success:
            logger.error("Step 3 failed. Aborting pipeline.")
            sys.exit(1)
        t3_elapsed = time.time() - t3_start
        logger.info(f"Step 3 Duration: {int(t3_elapsed // 60)}m {int(t3_elapsed % 60)}s")

    total_elapsed = time.time() - start_total
    logger.info("=" * 80)
    logger.info(
        f"ML CALIBRATION PIPELINE COMPLETED SUCCESSFULLY in {int(total_elapsed // 60)}m {int(total_elapsed % 60)}s!"
    )
    logger.info(f"Final Outputs saved in: {args.output_dir}")
    logger.info(f"Log written to: {log_file_path}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
