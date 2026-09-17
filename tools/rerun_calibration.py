#!/usr/bin/env python3
import argparse
import errno
import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from timeit import default_timer as timer

from src.utils.shared_functions import run_with_mp, setup_mp_file_logger


#####################################
'''
July 2026
CRITICAL NOTE:
    Due to major time constraints getting the FIM 6.2 release out, this part of the system received minimal testing
    in relation to the process_rerun_calibration_hucs.sh and rerun_calibration.py chain. It was heavily tested
    as part of the fim_pipeline chain.
    and any fixes required will come in near future PR (after FIM 6.2)

    Some adjustments and testing where done as part of the FIM 6.2 release and its addition of the possible
    temp file of process_rerun_calibration_hucs.sh workflow pattern. More changes and optimization in the
    rerun workflow is expected. It also may include the removal of the new process_rerun_calibratation_hucs.sh
    workflow in favour of once again talking directly to calibrate_rating_curves.sh.
    TBD

'''


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
        huc_log_file = os.path.join(fim_run_dir, huc, "logs", f"huc_{huc}_calib_rerun_errors.log")
        if os.path.isfile(huc_log_file):
            error_logs.append(huc_log_file)

    # Exit early if none found
    if not error_logs:
        print("No 'huc_calib_rerun_error.log' files found — no combined log created.")
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
        # we are putting time and tee right the command so it can catch the echos and prints
        # as the bash level, like our other part of pipeline processing.
        # Then we can have the error checking at the bottom of the script and it won't be out
        # of order.

        # The magic with logging, bash and how we use our shell scripts
        # is the relationship between exit codes, StdOut and StdErr and the timing of them.

        # This needs to be fixed a bit and may need some single quotes to let calibrate_rating_curves.sh
        # pick up the variables as ?
        cmd = ["bash", script_path, "true", str(branch_jobs), huc]

        # The first line in the calibrate_rating_curves.sh must have a least
        #   #!/bin/bash -e   (The -e means immeditely stop on error which is a feature
        #   we absolutely have to have. Especially when that is script is called by
        #   non re-calibrate scenarios such as run_by_branch.sh
        # calibrate_rating_curves.sh now has, and must have, #!/bin/bash -e as it's top line (with the -e)
        # the -e command (or at least set -e in a bash file means exit on fail)

        # This scenario is slightly complicated that calibrate_rating_curves.sh is called
        # in two places, once in python script and the other as a bash script and they
        # both handle StdOut, StnErr, and exit code different by default.

        # We have to catch three things from our .sh scripts
        # 1) The exit code, which usually is 0 (success) or 1 (fail with unknown reasons)
        #    There are other types of codes that can be retrieved, but we can just
        #    worry about 0 or any other code treat as a fail.
        # 2) StdOut:  Any messages that might be returned by the shell script. Normally
        #    we don't have any but keep the door open and see we have anything.
        # 3) StdErr:  If available, and it is not always available, is the reason that
        #    the .sh failed.

        # Use Popen for real-time output streaming while also capturing for error logging
        process = subprocess.Popen(
            cmd,
            env=task_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Merge stderr into stdout
            text=True,
        )

        # Stream output in real-time and capture it
        output_lines = []
        for line in process.stdout:
            screen_queue.put(f"[{task_id}] {line.rstrip()}")  # Real-time display
            output_lines.append(line)  # Also capture for error logging

        process.wait()  # Wait for process to complete
        captured_output = ''.join(output_lines)
        returncode = process.returncode

        if returncode != 0:
            msg = f"[{task_id}] ❌ Rerunning calibration failed for HUC {huc}."
            msg += f" Exit code: {returncode}"
            msg += f"\n--- OUTPUT ---\n{captured_output}"
            screen_queue.put(msg)
            file_logger.error(msg)
            return 0, [False]
        else:
            msg = f"[{task_id}] ✅ Rerunning calibration succeeded for HUC {huc}"
            screen_queue.put(msg)
            file_logger.info(msg)
            return 1, [True]

    except Exception as ex:
        msg = f"[{task_id}] ❌ Rerunning calibration failed with unexpected error for HUC {huc}: Exception: {ex}."
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

    # Create the logger
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M")
    log_file_path = os.path.join(fim_run_dir, 'logs', f"calib_rerun_{timestamp}.log")
    print(f"logs will be saved to {log_file_path}")
    file_logger = setup_mp_file_logger(log_file_path, logger_name='rerunning_calibration')
    print('started rerunning calibration...')
    file_logger.info(f'started rerunning calibration at: {timestamp}')

    # as env variables, pass fim run directory and src directory into calibrate_rating_curves.sh
    # Note: calibrate_rating_curves.sh will create and source params_rerun.env (from params_template.env)
    # instead of sourcing params.env when running in rerun mode
    env = os.environ.copy()
    env["outputDestDir"] = fim_run_dir

    # For the params.env file, when in re-run mode, we want to take a copy of the one from the code
    # via the "projectDir" enviro value, and rename it as we process
    proj_dir = os.getenv("projectDir")
    proj_params_file = os.path.join(proj_dir, "config", "params_template.env")
    rerun_params_files = os.path.join(fim_run_dir, "params_rerun.env")
    shutil.copy2(proj_params_file, rerun_params_files)

    script_path = str(Path(env.get("srcDir")) / "process_rerun_calibration_huc.sh")

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
    print('')
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

    # compile error log files, if exist
    compile_error_logs(fim_run_dir, hucs)

    # Calculate and log total duration
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"Done! Total duration: {duration}")
    file_logger.info(f"Finished at: {end_time}")
    file_logger.info(f"Total duration: {duration}")


if __name__ == "__main__":
    # notes
    # - this tool will read an existing FIM run (with mmultiple HUC results) and will overwrite the hyrotable (and src table)
    # - accordingly, the log files are overwrtten to be consistent with updated hydrotables.
    # - The flags to activate/deactivate each calibration script is still manage from config/params_template.env of the code
    # A new params_rerun.env will be created (from config/params_template.env) and is used for rerun.

    # sample usage
    # python /foss_fim/tools/rerun_calibration.py
    # -i /outputs/hand_4_9_5_8_test/ -jh 6 -jb 2

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

    # add duration into log file as well
