#!/usr/bin/env python3
import argparse
import datetime
import os
import subprocess
import sys
import time
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Post-processing for HAND dataset creation")
    parser.add_argument("-n", "--runName", required=True, help="Run name tag")
    return parser.parse_args()


def log_msg(msg: str, log_file: Path):
    print(msg)
    with open(log_file, "a") as f:
        f.write(f"{msg}\n")


def run_and_log(cmd: list, log_file: Path, err_file: Path):
    with open(log_file, "a") as out_f, open(err_file, "a") as err_f:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out_f.write(res.stdout)
        if res.stderr:
            err_f.write(res.stderr)
        return res.returncode


def main():
    args = parse_args()
    outputs_dir = Path(os.getenv("outputsDir", "/outputs"))
    src_dir = Path(os.getenv("srcDir", "/foss_fim/src"))
    tools_dir = Path(os.getenv("toolsDir", "/foss_fim/tools"))

    output_dest_dir = outputs_dir / args.runName
    if not output_dest_dir.exists():
        sys.exit(f"ERROR: Output folder '{output_dest_dir}' does not exist.")

    pp_log = output_dest_dir / "logs" / "post_processing.log"
    pp_err_log = output_dest_dir / "logs" / "post_processing_errors.log"
    all_errors_csv = output_dest_dir / "logs" / "all_huc_errors_report.csv"
    branch_accepted_csv = output_dest_dir / "logs" / "all_branches_with_accepted_codes.csv"
    duration_csv = output_dest_dir / "logs" / "total_duration_run_by_unit_all_HUCs.csv"
    fim_inputs = output_dest_dir / "fim_inputs.csv"

    # Clean existing logs
    if pp_log.exists():
        pp_log.unlink()
    if pp_err_log.exists():
        pp_err_log.unlink()

    start_time = time.time()
    log_msg("---- Start of fim_post_processing", pp_log)

    try:
        # 1. Compile Error Reports
        log_msg("=== Compiling all HUC error reports", pp_log)
        rc = run_and_log(
            [
                "python3",
                str(src_dir / "utils" / "post_process_error_report.py"),
                "-n",
                str(output_dest_dir),
                "-o",
                str(all_errors_csv),
                "-b",
                str(branch_accepted_csv),
            ],
            pp_log,
            pp_err_log,
        )
        if rc != 0:
            return

        # 2. Process Duration Reports
        log_msg("=== Concatenate processing time files into CSV", pp_log)
        rc = run_and_log(
            [
                "python3",
                str(src_dir / "duration_system.py"),
                "-fim",
                str(output_dest_dir),
                "-o",
                str(duration_csv),
            ],
            pp_log,
            pp_err_log,
        )
        if rc != 0:
            return

        # 3. Branch Aggregation
        log_msg("=== Start branch aggregation", pp_log)
        rc = run_and_log(
            [
                "python3",
                str(src_dir / "aggregate_branch_lists.py"),
                "-d",
                str(output_dest_dir),
                "-f",
                "branch_ids.csv",
                "-o",
                str(fim_inputs),
            ],
            pp_log,
            pp_err_log,
        )
        if rc != 0:
            return

        # 4. Combine Crosswalk Tables
        log_msg("=== Combining crosswalk tables", pp_log)
        rc = run_and_log(
            [
                "python3",
                str(tools_dir / "combine_crosswalk_tables.py"),
                "-d",
                str(output_dest_dir),
                "-o",
                str(output_dest_dir / "crosswalk_table.csv"),
            ],
            pp_log,
            pp_err_log,
        )
        if rc != 0:
            return

    finally:
        elapsed = str(datetime.timedelta(seconds=int(time.time() - start_time)))
        if pp_err_log.exists() and pp_err_log.stat().st_size > 0:
            log_msg("**** Errors were found while processing post processing", pp_log)
            log_msg("**** Check the post processing error log for details", pp_log)
        log_msg(f"---- End of fim_post_processing. Duration: {elapsed}", pp_log)


if __name__ == "__main__":
    main()
