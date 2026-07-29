#!/usr/bin/env python3
import argparse
import datetime
import os
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Process a single stream branch for a given HUC.")
    parser.add_argument("-n", "--runName", required=True, help="Run name tag (e.g. test_run)")
    parser.add_argument("-u", "--hucNumber", required=True, help="HUC number (e.g. 05030104)")
    parser.add_argument("-b", "--branchId", required=True, help="Branch ID to process (e.g. 7701 or 0)")
    parser.add_argument(
        "-l",
        "--level",
        default="branch",
        choices=["branch", "unit"],
        help="Processing level (branch or unit)",
    )
    return parser.parse_args()


def compile_branch_error_report(
    huc_number: str, branch_id: str, log_file: Path, error_csv: Path, src_dir: Path
):
    """Parses branch logs and appends any error codes to the branch error CSV."""
    cmd = [
        "python3",
        str(src_dir / "utils" / "huc_process_error_report.py"),
        "-u",
        huc_number,
        "-b",
        branch_id,
        "-s",
        str(log_file),
        "-o",
        str(error_csv),
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    args = parse_args()

    if not args.hucNumber.isdigit():
        sys.exit("Error: hucNumber must be numeric.")

    # Fetch directories from environment or default Docker paths
    work_dir = Path(os.getenv("workDir", "/fim_temp"))
    outputs_dir = Path(os.getenv("outputsDir", "/outputs"))
    src_dir = Path(os.getenv("srcDir", "/foss_fim/src"))

    temp_huc_dir = work_dir / args.runName / args.hucNumber
    output_huc_dir = outputs_dir / args.runName / args.hucNumber

    # Set branch folder structure
    if args.branchId == "0":
        temp_branch_dir = temp_huc_dir / "branch_zero"
        output_branch_dir = output_huc_dir / "branch_zero"
    else:
        temp_branch_dir = temp_huc_dir / "branches" / args.branchId
        output_branch_dir = output_huc_dir / "branches" / args.branchId

    # Cleanup prior runs if they exist
    if output_branch_dir.exists():
        shutil.rmtree(output_branch_dir)
    if temp_branch_dir.exists():
        shutil.rmtree(temp_branch_dir)

    logs_dir = temp_branch_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    branch_log_file = logs_dir / f"branch_{args.branchId}.log"
    warning_log_file = logs_dir / f"branch_{args.branchId}_warnings.log"
    error_csv_file = logs_dir / f"branch_{args.branchId}_error_report.csv"

    print("==========================================================================")
    print(f"---- Start processing Branch {args.branchId} for HUC {args.hucNumber}")

    # Set up child process environment
    env = os.environ.copy()
    env["runName"] = args.runName
    env["hucNumber"] = args.hucNumber
    env["current_branch_id"] = str(args.branchId)
    env["tempHucDataDir"] = str(temp_huc_dir)
    env["tempCurrentBranchDataDir"] = str(temp_branch_dir)
    env["outputHucDataDir"] = str(output_huc_dir)
    env["outputCurrentBranchDataDir"] = str(output_branch_dir)

    try:
        # Run modern process_branch.py
        branch_script = src_dir / "process_branch.py"

        with open(branch_log_file, "w") as log_f:
            proc = subprocess.run(
                ["python3", str(branch_script), args.level],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            log_f.write(proc.stdout)

            return_code = proc.returncode
            if return_code == 0:
                print(f"--> Branch {args.branchId} completed successfully.")
            else:
                log_f.write(f"\n***** Error status: {return_code} detected in Branch {args.branchId} *****\n")
                print(f"--> Error: Branch {args.branchId} failed with code {return_code}")

        # Extract warnings from the log
        if branch_log_file.exists():
            with open(branch_log_file, "r") as lf, open(warning_log_file, "w") as wf:
                for line in lf:
                    if "warning" in line.lower():
                        wf.write(line)

    finally:
        # Guarantee compile error report & copy output files on completion or failure
        compile_branch_error_report(args.hucNumber, args.branchId, branch_log_file, error_csv_file, src_dir)

        print(f"***** Copying temp branch dir ({temp_branch_dir}) to output dir ({output_branch_dir})")
        output_branch_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(temp_branch_dir, output_branch_dir, dirs_exist_ok=True)

        # Clean up temporary directory
        shutil.rmtree(temp_branch_dir, ignore_errors=True)
        print("***** Copy complete, temporary directory removed.")
        print("==========================================================================")


if __name__ == "__main__":
    main()
