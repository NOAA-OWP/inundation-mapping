#!/usr/bin/env python3
"""
Exact 1:1 Python Modernization of fim_pipeline.sh
--------------------------------------------------
Orchestrates pre-processing, dispatches HUC-level processing across single or multiple
units using ProcessPoolExecutor, and executes post-processing routines.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
PROJECT_DIR = Path(__file__).resolve().parent


def get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="FIM Pipeline Runner (Python modernization of fim_pipeline.sh)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-u", "--hucList", required=True, help="HUC8s to run (space-delimited string or path to .lst file)"
    )
    parser.add_argument("-n", "--runName", required=True, help="Run name tag (alphanumeric)")
    parser.add_argument(
        "-c", "--config", default="config/params_template.env", help="Path to config params file"
    )
    parser.add_argument("-ud", "--unitDenylist", default="config/deny_unit.lst", help="Unit denylist file")
    parser.add_argument(
        "-bd", "--branchDenylist", default="config/deny_branches.lst", help="Branch denylist file"
    )
    parser.add_argument(
        "-zd", "--branchZeroDenylist", default="config/deny_branch_zero.lst", help="Branch zero denylist file"
    )
    parser.add_argument(
        "-jh", "--jobLimit", type=int, default=1, help="Max concurrent HUC jobs (jobHucLimit)"
    )
    parser.add_argument("-jb", "--jobBranchLimit", type=int, default=1, help="Max concurrent Branch jobs")
    parser.add_argument("-o", "--overwrite", action="store_true", help="Overwrite existing outputs")
    parser.add_argument("-x", "--evalCrosswalk", action="store_true", help="Evaluate crosswalk")

    return parser.parse_args()


def load_huc_list(huc_input: str) -> list:
    """Parses a space-delimited string or reads a .lst file of HUC numbers."""
    huc_path = Path(huc_input)
    if huc_path.is_file() or huc_input.endswith(".lst"):
        with open(huc_path, "r") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return [h.strip() for h in huc_input.split() if h.strip()]


def run_cmd(cmd: list, check: bool = True):
    """Executes a command line process with real-time output streaming."""
    print(f"--> Executing: {' '.join(str(c) for c in cmd)}")
    res = subprocess.run(cmd, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {res.returncode}: {' '.join(str(c) for c in cmd)}")
    return res.returncode


def process_single_huc(run_name: str, huc_id: str, src_dir: Path):
    """
    Worker function to process a single HUC.
    Dispatches to fim_process_huc.py or run_huc.py natively via sys.executable.
    """
    process_huc_script = PROJECT_DIR / "fim_process_huc.py"
    if process_huc_script.is_file():
        cmd = [sys.executable, str(process_huc_script), run_name, huc_id]
    elif (src_dir / "run_huc.py").is_file():
        cmd = [sys.executable, str(src_dir / "run_huc.py"), run_name, huc_id]
    else:
        cmd = ["bash", str(PROJECT_DIR / "fim_process_huc.sh"), run_name, huc_id]

    print(f"=== [HUC {huc_id}] Starting processing ===")
    start_time = time.time()

    res_code = run_cmd(cmd, check=False)

    elapsed = time.time() - start_time
    if res_code == 0:
        print(f"=== [HUC {huc_id} SUCCESS] Completed in {elapsed:.2f}s ===")
    else:
        print(f"=== [HUC {huc_id} WARNING] Finished with code {res_code} after {elapsed:.2f}s ===")
    return (huc_id, res_code)


def main():
    args = parse_arguments()
    pipeline_start_time = time.time()

    print("\n======================= Start of fim_pipeline =========================")
    print(f"---- Started: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC")

    src_dir = Path(get_env("srcDir", str(SRC_DIR)))
    if not src_dir.is_dir():
        src_dir = PROJECT_DIR / "src"

    # 1. Execute Pre-Processing Phase
    pre_processing_script = PROJECT_DIR / "fim_pre_processing.py"
    if pre_processing_script.is_file():
        pre_args = [sys.executable, str(pre_processing_script)] + sys.argv[1:]
    else:
        pre_processing_script = PROJECT_DIR / "fim_pre_processing.sh"
        pre_args = ["bash", str(pre_processing_script)] + sys.argv[1:]

    print("--> Launching Pre-Processing Phase...")
    run_cmd(pre_args, check=True)

    # 2. Parse HUC targets
    hucs = load_huc_list(args.hucList)
    job_huc_limit = args.jobLimit

    print(
        f"---- Unit (HUC) processing started for {len(hucs)} unit(s) using {job_huc_limit} parallel worker(s)"
    )

    # 3. Parallel Dispatch across HUCs
    if job_huc_limit == 1:
        for huc in hucs:
            process_single_huc(args.runName, huc, src_dir)
    else:
        with ProcessPoolExecutor(max_workers=job_huc_limit) as executor:
            futures = [executor.submit(process_single_huc, args.runName, huc, src_dir) for huc in hucs]
            for future in as_completed(futures):
                huc_id, code = future.result()

    print("---- Unit (HUC) processing is complete")
    elapsed_units = time.time() - pipeline_start_time
    print(f"Duration: {elapsed_units:.2f}s")
    print("---------------------------------------------------")

    # 4. Clean up workDir temporary folders
    work_dir = Path(get_env("workDir", "/fim_temp"))
    temp_run_dir = work_dir / args.runName
    if temp_run_dir.is_dir():
        try:
            shutil.rmtree(temp_run_dir, ignore_errors=True)
        except Exception as ex:
            print(f"Notice: Could not remove temporary run dir {temp_run_dir}: {ex}")

    # 5. Execute Post-Processing Phase
    post_processing_script = PROJECT_DIR / "fim_post_processing.py"
    if post_processing_script.is_file():
        post_args = [sys.executable, str(post_processing_script), "-n", args.runName]
    else:
        post_processing_script = PROJECT_DIR / "fim_post_processing.sh"
        post_args = ["bash", str(post_processing_script), "-n", args.runName]

    print("--> Launching Post-Processing Phase...")
    run_cmd(post_args, check=True)

    total_duration = time.time() - pipeline_start_time
    print(f"\n======================== End of fim_pipeline for {args.runName} ==========")
    print(f"Total Duration: {total_duration:.2f}s\n")


if __name__ == "__main__":
    main()
