#!/usr/bin/env python3
import argparse
import errno
import glob
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd

from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


def compile_error_logs(fim_run_dir, hucs):
    """
    Aggregate error logs from calibration rerun across all HUCs.

    Searches for 'huc_{huc}_errors_calib_rerun.log' files in each HUC's logs
    directory and combines them into a single timestamped aggregated log file.
    If no error log files are found, no output file is created.

    Parameters
    ----------
    fim_run_dir : str
        Root directory of the FIM run containing HUC subdirectories.
        Example: '/data/outputs/fim_run_20250106'
    hucs : list of str
        List of HUC identifiers (8-digit strings) to search for error logs.
        Example: ['12090301', '12090302']

    Returns
    -------
    None
        Writes output to: {fim_run_dir}/logs/all_errors_calib_rerun_{timestamp}.log
        Prints message to console indicating success or if no logs were found.

    Notes
    -----
    - Only processes files from calibration reruns (not initial runs)
    - Uses timestamp format: YYYYMMDD_HHMM
    - Each HUC's errors are separated by headers in the output file

    """

    error_logs = []

    # Collect existing HUC error logs
    for huc in hucs:
        huc_log_file = os.path.join(fim_run_dir, huc, "logs", f"huc_{huc}_errors_calib_rerun.log")
        if os.path.isfile(huc_log_file):
            error_logs.append(huc_log_file)

    # Exit early if none found
    if not error_logs:
        print("No 'huc_errors_calib_rerun.log' files found — no combined log created.")
        return

    # Create output file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    outfile = os.path.join(fim_run_dir, "logs", f"all_errors_calib_rerun_{timestamp}.log")

    # Combine logs
    with open(outfile, "w") as out_f:
        for file in error_logs:
            out_f.write(f"===== From {file} =====\n")
            with open(file, "r") as f:
                out_f.write(f.read())
                out_f.write("\n")

    print(f"Combined error log created: {outfile}")


def run_shell_for_huc(
    huc, script_path, task_env, branch_jobs, file_logger=None, screen_queue=None, task_id=None
):
    # note that sub-processes python codes have their own custom print statments and logging which do not follow file_logger here.
    #  therefore file_logger and  in this function only provide overall status of rerunning the calibration.
    file_logger.info(f"Rerunning calibration Started for {task_id}")
    try:
        cmd = ["bash", script_path, "True", str(branch_jobs)]

        _ = subprocess.run(
            cmd,
            env=task_env,
            capture_output=False,
            text=True,
            check=True,  # will raise CalledProcessError immediately when a calibration routine fails and the subsequent python code are not run
        )

        msg = f"[{task_id}] ✅ Rerunning calibration succeeded for HUC {huc}"
        screen_queue.put(msg)
        file_logger.info(msg)
        return 1, [True]

    except subprocess.CalledProcessError as e:
        msg = f"[{task_id}] ❌ Rerunning calibration failed for HUC {huc}: {e}"
        screen_queue.put(msg)
        file_logger.error(msg)
        return 0, [False]

    except Exception as ex:
        msg = f"[{task_id}] ❌ Rerunning calibration failed with unexpected error for HUC {huc}: {ex}"
        screen_queue.put(msg)
        file_logger.error(msg)
        return 0, [False]


def rerun_calibration(fim_run_dir: str, limit_hucs: list = [], huc_jobs: int = 6, branch_jobs: int = 2):
    # Check that fim run directory exists
    if not os.path.exists(fim_run_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fim_run_dir)

    # validate the number of jobs
    total_cpus_available = os.cpu_count() - 2
    if huc_jobs * branch_jobs > total_cpus_available:
        raise ValueError(
            f"Invalid job configuration: jh*jb={huc_jobs*branch_jobs} "
            f"exceeds available CPUs minus 2 ({total_cpus_available-2}). "
            "Adjust values of -jh and -jb accordingly."
        )

    # Get the list of all hucs in the directory
    all_existing_hucs = [d for d in os.listdir(fim_run_dir) if re.match(r'^\d{8}$', d)]
    hucs = []
    for huc in all_existing_hucs:
        if os.path.isdir(os.path.join(fim_run_dir, huc)):
            hucs.append(huc)

    if limit_hucs:
        hucs = [h for h in limit_hucs if h in hucs]

    # as env variables, pass fim run directory (containing params.env) and src directory (containing bash_variables.env) into calibrate_rating_curves.sh
    env = os.environ.copy()
    env["outputDestDir"] = fim_run_dir

    # create path to the target script (calibrate_rating_curves.sh)
    script_path = str(Path(env.get("srcDir")) / "calibrate_rating_curves.sh")

    tasks_args_list = []
    for huc in sorted(hucs):
        task_env = env.copy()
        task_env["tempHucDataDir"] = os.path.join(fim_run_dir, huc)

        tasks_args_list.append(
            {
                "huc": huc,
                "script_path": script_path,
                "task_env": task_env,  # Copy so each task gets its own env
                "branch_jobs": branch_jobs,
            }
        )

    # Create the logger
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_file_path = os.path.join(fim_run_dir, 'logs', f"rerun_calibration_{timestamp}.log")
    file_logger = setup_mp_file_logger(log_file_path, logger_name='rerunning_calibration')
    print('started rerunning calibration...')
    file_logger.info('started rerunning calibration...')

    # Run multiprocessing
    mp_results = run_with_mp(
        task_function=run_shell_for_huc,
        tasks_args_list=tasks_args_list,
        file_logger=file_logger,
        max_workers=huc_jobs,
        task_id_key="huc",  # to label logs by HUC ID
        show_progress=False,  # there are many print statments in each calibration scripts
    )

    print('multiprocessing tasks finished!')
    # only report if all succeeded or the failed ones
    failed_keys = [tid for tid, payload in mp_results.items() if not payload[0]]

    if not failed_keys:
        file_logger.info("✅ All multiprocessing tasks Succeeded")
        print("✅ All multiprocessing tasks Succeeded")
    else:
        file_logger.info(f"❌ {len(failed_keys)} failed:")
        print(f"❌ {len(failed_keys)} failed:")
        for tid in failed_keys:
            file_logger.info(f"  - {tid}")
            print(f"  - {tid}")

    print("Done!")
    file_logger.info("Done!")

    # finally compile error log files, if exist
    compile_error_logs(fim_run_dir, hucs)


if __name__ == "__main__":
    # notes
    # - this tool will read an existing FIM run (with mmultiple HUC results) and will overwrite the hyrotable (and src table)
    # - accordingly, the log files are overwrtten to be consistent with updated hyrtables.
    # - The flags to activate/deactivate each calibration script is still read from $outputDestDir/params.env file. So if a step is not required
    # the $outputDestDir/params.env file (available in output folder) needs to be updated (and not config/params_template.env of the code itself)

    # sample usage
    # python foss_fim/tools/rerun_calibrate_rating_curves.py
    # -i fim_dir -jh 6 -jb 2

    # Parse arguments
    parser = argparse.ArgumentParser(description="Rerun calibrating rating curves (after a fim pipeline run)")
    parser.add_argument(
        "-i", "--fim_run_dir", help="Directory path to FIM run directory.", required=True, type=str
    )

    parser.add_argument(
        "-u",
        "--limit_hucs",
        help="Optional. If specified, rerunning calibration steps are applied only for these limited HUCs.",
        required=False,
        type=str,
        nargs="+",
    )

    parser.add_argument(
        '-jh',
        '--huc_jobs',
        help='OPTIONAL: Number of jobs for HUCs processing, default is 6. ',
        required=False,
        default=6,
        type=int,
    )

    parser.add_argument(
        '-jb',
        '--branch_jobs',
        help='OPTIONAL: Number of jobs for branches processing within each huc, default is 2. ',
        required=False,
        default=2,
        type=int,
    )

    start = timer()

    rerun_calibration(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
