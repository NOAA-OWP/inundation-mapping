#!/usr/bin/env python3
"""
Exact 1:1 Python Modernization of process_branch.sh
----------------------------------------------------
Wrapper module for executing individual branch processing (run_by_branch.py).
Handles random delay staggering, log teeing/rollups, and explicit trapping of
FIM exit codes (61: no valid flowlines, 64: no crosswalks, 65: Int16 overflow).
"""

import os
import random
import subprocess
import sys
import time
from pathlib import Path

# Import the native Python branch processing routine
import run_by_branch


SRC_DIR = Path(__file__).resolve().parent


# ------------------------------------------------------------------------------
# 1. Environment & Utility Helpers
# ------------------------------------------------------------------------------
def get_env(key: str, default: str = "") -> str:
    """Gets an environment variable or returns default."""
    return os.getenv(key, default)


def calc_duration(start_time: float) -> str:
    """Calculates formatted elapsed duration string."""
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


# ------------------------------------------------------------------------------
# 2. Main Branch Execution Wrapper
# ------------------------------------------------------------------------------
def process_branch_wrapper():
    # Parse positional arguments passed from run_huc.py or CLI
    # Expected: runName, hucNumber, branchId
    run_name = sys.argv[1] if len(sys.argv) > 1 else get_env("runName", "dev-run")
    huc_number = sys.argv[2] if len(sys.argv) > 2 else get_env("hucNumber")
    branch_id = str(sys.argv[3] if len(sys.argv) > 3 else get_env("current_branch_id"))

    if not huc_number or not branch_id:
        print("Usage: process_branch.py <runName> <hucNumber> <branchId>")
        sys.exit(0)  # Standard hardcoded success exit as designed in process_branch.sh

    branch_start_time = time.time()

    # Environment setup
    tempHucDataDir = Path(get_env("tempHucDataDir", f"/fim_temp/{run_name}/{huc_number}"))
    branch_log_dir = tempHucDataDir / "logs" / "branch"
    branch_log_dir.mkdir(parents=True, exist_ok=True)
    branch_log_file = branch_log_dir / f"{huc_number}_branch_{branch_id}.log"

    print("++++++++++++++++++++++++++++++++++++")
    print(f"--> Processing HUC: {huc_number} - branch_id: {branch_id}")
    print(f"--> Start Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC")

    # Put in a random 0 to 10 second sleep to help space out parallel workers
    jitter_sleep = random.randint(0, 10)
    time.sleep(jitter_sleep)

    exit_code = 0
    err_message = ""

    # Execute the branch logic directly via run_by_branch module
    try:
        # Redirect stdout and stderr to both screen and the branch log file (simulating tee)
        class TeeLogger:
            def __init__(self, filepath):
                self.terminal = sys.stdout
                self.log = open(filepath, "a")

            def write(self, message):
                self.terminal.write(message)
                self.log.write(message)

            def flush(self):
                self.terminal.flush()
                self.log.flush()

        logger = TeeLogger(branch_log_file)
        sys.stdout = logger
        sys.stderr = logger

        # Run native Python branch execution
        run_by_branch.run_branch_processing(run_name=run_name, huc_number=huc_number, branch_id=branch_id)

    except RuntimeError as ex:
        err_message = str(ex)
        # Check if exit code was embedded in the exception
        if "exit code" in err_message.lower():
            try:
                exit_code = int(err_message.split("exit code")[-1].split(":")[0].strip())
            except ValueError:
                exit_code = 1
        else:
            exit_code = 1
    except Exception as ex:
        err_message = str(ex)
        exit_code = 1

    # Map return codes to FIM enumerations (matches PIPESTATUS checking in process_branch.sh)
    if exit_code == 0:
        pass
    elif exit_code == 61:
        print(
            f"\nAcceptable Exit Status: {exit_code} -- Branch has no valid flowlines. [[BranchID: {branch_id}]]"
        )
    elif exit_code == 64:
        print(f"\nAcceptable Exit Status: {exit_code} -- Branch has no crosswalks. [[BranchID: {branch_id}]]")
    elif exit_code == 65:
        print(
            f"\nAcceptable Exit Status: {exit_code} -- Too many HydroIDs or a HydroID with more than 8 digits in gw catchments to convert to Int16 [[BranchID: {branch_id}]]"
        )
    else:
        print(
            f"\n***** ERROR - Unknown Exit status: {exit_code} detected for [[BranchID: {branch_id}]] *****"
        )
        if err_message:
            print(f"Details: {err_message}")

    duration_str = calc_duration(branch_start_time)
    print(f"--> End Branch Processing {huc_number} {branch_id} ...")
    print(f"--> End Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC")
    print(f"--> Duration : {duration_str}")
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n")

    # process_branch.sh always exits with status 0 so parallel worker pool isn't killed
    sys.exit(0)


if __name__ == "__main__":
    process_branch_wrapper()
