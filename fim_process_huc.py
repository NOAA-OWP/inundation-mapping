#!/usr/bin/env python3
"""
Exact 1:1 Python Modernization of fim_process_huc.sh
---------------------------------------------------
Supervises HUC-level processing for a single unit. Sets up environment variables,
captures and tees logs, calls run_huc.py, compiles error reports, harvests warning logs,
and executes cleanup & output transfer to /outputs directory.
"""

import os
import re
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
if not SRC_DIR.is_dir():
    SRC_DIR = Path(__file__).resolve().parent

sys.path.insert(0, str(SRC_DIR))
import run_huc  # noqa: E402


# ------------------------------------------------------------------------------
# 1. Environment & Helper Functions
# ------------------------------------------------------------------------------
def get_env(key: str, default: str = "") -> str:
    """Gets an environment variable or returns default."""
    return os.getenv(key, default)


def load_environment_params():
    """Parses params_template.env, params.env, runtime_args.env, and bash_variables.env into os.environ."""
    project_root = Path(__file__).resolve().parent
    run_name = get_env("runName", "dev-run")

    candidate_files = [
        project_root / "config" / "params_template.env",
        project_root / "config" / "params.env",
        SRC_DIR / "bash_variables.env",
        Path("/fim_temp") / run_name / "runtime_args.env",
        Path("/outputs") / run_name / "runtime_args.env",
        Path("/fim_temp") / run_name / "params.env",
        Path("/outputs") / run_name / "params.env",
    ]

    parsed_vars = {
        "dataDir": os.environ.get("dataDir", "/data"),
        "inputsDir": os.environ.get("inputsDir", "/data/inputs"),
        "outputsDir": os.environ.get("outputsDir", "/outputs"),
        "projectDir": os.environ.get("projectDir", "/foss_fim"),
        "srcDir": os.environ.get("srcDir", str(SRC_DIR)),
        "toolsDir": os.environ.get("toolsDir", "/foss_fim/tools"),
        "workDir": os.environ.get("workDir", "/fim_temp"),
    }

    for k, v in parsed_vars.items():
        if k not in os.environ:
            os.environ[k] = v

    for env_file in candidate_files:
        if env_file.is_file():
            with open(env_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("#") or not line or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip().replace("export ", "")
                    val = v.split("#")[0].strip().strip('"').strip("'")
                    parsed_vars[k] = val

    for _ in range(3):
        for k, v in parsed_vars.items():
            expanded = os.path.expandvars(v)
            os.environ[k] = expanded
            parsed_vars[k] = expanded


class TeeLogger:
    """Simulates shell tee command: routes stdout statements to screen and logfile simultaneously."""

    def __init__(self, log_filepath: Path):
        self.terminal = sys.stdout
        self.log_file = open(log_filepath, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()


def compile_error_report(huc_number: str, huc_log_file: Path, error_log_file: Path, scan_failed_log: Path):
    """Executes huc_process_error_report.py to compile CSV error metrics."""
    print("----------------------------------------")
    print(f"--> Compiling error report for HUC {huc_number}")

    error_script = SRC_DIR / "utils" / "huc_process_error_report.py"
    if not error_script.is_file():
        error_script = Path(__file__).resolve().parent / "tools" / "huc_process_error_report.py"

    if error_script.is_file():
        cmd = [
            sys.executable,
            str(error_script),
            "-u",
            str(huc_number),
            "-s",
            str(huc_log_file),
            "-o",
            str(error_log_file),
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)

        if res.stdout:
            print(res.stdout.strip())

        if res.stderr and res.returncode != 0:
            with open(scan_failed_log, "a") as sf:
                sf.write(res.stderr)
            print("Continuing processing of other hucs when applicable, stand by")
        elif scan_failed_log.is_file() and scan_failed_log.stat().st_size == 0:
            scan_failed_log.unlink()


def exit_and_copy(
    temp_huc_dir: Path,
    output_huc_dir: Path,
    huc_log_file: Path,
    huc_number: str,
    error_log_file: Path,
    scan_failed_log: Path,
):
    """
    Guaranteed exit handler that compiles error reports, sets permissions,
    copies the temp workspace to outputs directory, and cleans up temp directories.
    """
    try:
        compile_error_report(huc_number, huc_log_file, error_log_file, scan_failed_log)
    except Exception as ex:
        print(f"Notice during error report compilation: {ex}")

    # Set permissions
    for root, dirs, files in os.walk(temp_huc_dir):
        for d in dirs:
            os.chmod(os.path.join(root, d), 0o774)
        for f in files:
            os.chmod(os.path.join(root, f), 0o774)

    print("=============================================================================================")
    print()
    print(
        f"***** Starting copying folder temp directory: {temp_huc_dir} to output directory: {output_huc_dir}"
    )

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"Timestamp: {timestamp} UTC")

    if huc_log_file.is_file():
        with open(huc_log_file, "a") as f:
            f.write(f"{timestamp}\n")

    # Copy temp to outputs
    if output_huc_dir.exists():
        shutil.rmtree(output_huc_dir, ignore_errors=True)

    shutil.copytree(temp_huc_dir, output_huc_dir, dirs_exist_ok=True)

    # Remove temp folder
    shutil.rmtree(temp_huc_dir, ignore_errors=True)

    print("***** Copy complete, removed old temp directory")
    print("=============================================================================================")
    print()


# ------------------------------------------------------------------------------
# 2. Main Process Execution
# ------------------------------------------------------------------------------
def process_huc():
    if len(sys.argv) < 3 or sys.argv[1] in ["-h", "--help"]:
        print("Usage: python3 fim_process_huc.py <name_of_your_run> <huc8>")
        print("Example: python3 fim_process_huc.py dev-gdal-modernization 05030104")
        sys.exit(22 if len(sys.argv) < 3 else 0)

    runName = sys.argv[1]
    hucNumber = sys.argv[2]

    if not runName or not hucNumber or not re.match(r"^[0-9]+$", hucNumber):
        print("ERROR: Invalid or missing runName or non-numeric hucNumber.")
        sys.exit(22)

    os.environ["runName"] = runName
    os.environ["hucNumber"] = hucNumber

    # Load parameters into os.environ
    load_environment_params()

    workDir = Path(get_env("workDir", "/fim_temp"))
    outputsDir = Path(get_env("outputsDir", "/outputs"))

    tempRunDir = workDir / runName
    outputDestDir = outputsDir / runName
    tempHucDataDir = tempRunDir / hucNumber
    outputHucDataDir = outputDestDir / hucNumber
    tempBranchDataDir = tempHucDataDir / "branches"

    os.environ["tempRunDir"] = str(tempRunDir)
    os.environ["outputDestDir"] = str(outputDestDir)
    os.environ["tempHucDataDir"] = str(tempHucDataDir)
    os.environ["outputHucDataDir"] = str(outputHucDataDir)
    os.environ["tempBranchDataDir"] = str(tempBranchDataDir)
    os.environ["current_branch_id"] = "0"

    # Clean existing temp / output dirs
    if outputHucDataDir.exists():
        shutil.rmtree(outputHucDataDir, ignore_errors=True)
    if tempHucDataDir.exists():
        shutil.rmtree(tempHucDataDir, ignore_errors=True)

    # Recreate directory structures
    tempHucDataDir.mkdir(parents=True, exist_ok=True)
    tempBranchDataDir.mkdir(parents=True, exist_ok=True)
    logs_dir = tempHucDataDir / "logs"
    (logs_dir / "branch").mkdir(parents=True, exist_ok=True)

    hucLogFile = logs_dir / f"huc_{hucNumber}_unit.log"
    warningLogFile = logs_dir / f"huc_{hucNumber}_warnings.log"
    log_scan_failed_file = logs_dir / f"log_scan_tool_failed_{hucNumber}.log"
    errorLogFile = logs_dir / f"huc_{hucNumber}_error_report.csv"

    os.environ["hucLogFile"] = str(hucLogFile)
    os.environ["warningLogFile"] = str(warningLogFile)
    os.environ["log_scan_tool_failed_file"] = str(log_scan_failed_file)
    os.environ["errorLogFile"] = str(errorLogFile)

    # Attach Tee Logger to pipe stdout / stderr simultaneously to file
    tee_logger = TeeLogger(hucLogFile)
    sys.stdout = tee_logger
    sys.stderr = tee_logger

    print("==========================================================================")
    print(f"---- Start of huc processing for {hucNumber}")
    print(f"---- Started: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC")

    exit_code = 0
    try:
        # Dispatch run_huc processing routine natively
        run_huc.run_huc_processing(run_name=runName, huc_number=hucNumber, temp_huc_dir=tempHucDataDir)
    except Exception as ex:
        err_msg = str(ex) if str(ex) else type(ex).__name__
        if "Exit Status: 60" in err_msg or "code 60" in err_msg:
            exit_code = 60
        elif "Exit Status: 61" in err_msg or "code 61" in err_msg:
            exit_code = 61
        else:
            exit_code = 1
            print(f"***** Error during HUC processing: {err_msg} *****")
            traceback.print_exc()

    # Evaluate Return Code
    if exit_code == 0:
        pass
    elif exit_code == 60:
        print("----------------------------------------")
        print(f"***** Acceptable Exit status: {exit_code} - HUC has no valid branches [[HUC: {hucNumber}]]")
        print("----------------------------------------")
    elif exit_code == 61:
        print("----------------------------------------")
        print(
            f"***** Acceptable Exit status: {exit_code} - HUC has no remaining valid flowlines [[HUC: {hucNumber}]]"
        )
        print("----------------------------------------")
    else:
        print("----------------------------------------")
        print(f"***** Error Exit status: {exit_code} detected *****")
        print("----------------------------------------")

    # Filter warning messages into warningLogFile
    if hucLogFile.is_file():
        with (
            open(hucLogFile, "r", encoding="utf-8", errors="ignore") as f_in,
            open(warningLogFile, "w", encoding="utf-8") as f_out,
        ):
            for line_no, line in enumerate(f_in, 1):
                if "warning" in line.lower():
                    f_out.write(f"{line_no}:{line}")

    # Copy files to output destination and clean up temp workspace
    exit_and_copy(
        temp_huc_dir=tempHucDataDir,
        output_huc_dir=outputHucDataDir,
        huc_log_file=hucLogFile,
        huc_number=hucNumber,
        error_log_file=errorLogFile,
        scan_failed_log=log_scan_failed_file,
    )

    tee_logger.close()
    sys.exit(0)


if __name__ == "__main__":
    process_huc()
