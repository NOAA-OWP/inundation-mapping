#!/usr/bin/env python3
"""
Exact 1:1 Python Modernization of fim_pre_processing.sh
--------------------------------------------------------
Collects & validates CLI inputs, manages temporary/output directory trees,
copies params files, invokes check_huc_inputs.py, and generates runtime_args.env.
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
PROJECT_DIR = Path(__file__).resolve().parent


def get_env(key: str, default: str = "") -> str:
    """Gets an environment variable or returns default."""
    return os.getenv(key, default)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="FIM Pre-Processing Runner (Python modernization of fim_pre_processing.sh)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-u", "--hucList", required=True, help="HUC8s to run (space-delimited or path to .lst)"
    )
    parser.add_argument("-n", "--runName", required=True, help="Run name tag (alphanumeric)")
    parser.add_argument("-c", "--configFile", default="", help="Configuration file path")
    parser.add_argument("-ud", "--unitDenylist", default="", help="Unit denylist file")
    parser.add_argument("-bd", "--branchDenylist", default="", help="Branch denylist file")
    parser.add_argument("-zd", "--branchZeroDenylist", default="", help="Branch zero denylist file")
    parser.add_argument("-jh", "--jobHucLimit", type=int, default=1, help="Max concurrent HUC jobs")
    parser.add_argument("-jb", "--jobBranchLimit", type=int, default=1, help="Max concurrent Branch jobs")
    parser.add_argument("-o", "--overwrite", action="store_true", help="Overwrite existing outputs")
    parser.add_argument("-x", "--evalCrosswalk", action="store_true", help="Evaluate crosswalk flag")

    return parser.parse_args()


def load_environment_params():
    """Parses params_template.env, params.env, and bash_variables.env into os.environ."""
    candidate_files = [
        PROJECT_DIR / "config" / "params_template.env",
        PROJECT_DIR / "config" / "params.env",
        SRC_DIR / "bash_variables.env",
    ]
    parsed_vars = {
        "dataDir": os.environ.get("dataDir", "/data"),
        "inputsDir": os.environ.get("inputsDir", "/data/inputs"),
        "outputsDir": os.environ.get("outputsDir", "/outputs"),
        "projectDir": os.environ.get("projectDir", str(PROJECT_DIR)),
        "srcDir": os.environ.get("srcDir", str(SRC_DIR)),
        "toolsDir": os.environ.get("toolsDir", str(PROJECT_DIR / "tools")),
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


def main():
    args = parse_arguments()

    if not args.hucList:
        print("ERROR: Missing -u Huclist argument")
        sys.exit(22)
    if not args.runName:
        print("ERROR: Missing -n run time name argument")
        sys.exit(22)

    load_environment_params()

    workDir = Path(get_env("workDir", "/fim_temp"))
    outputsDir = Path(get_env("outputsDir", "/outputs"))
    projectDir = Path(get_env("projectDir", str(PROJECT_DIR)))
    srcDir = Path(get_env("srcDir", str(SRC_DIR)))

    outputDestDir = outputsDir / args.runName
    tempRunDir = workDir / args.runName

    # Default parameters
    envFile = Path(args.configFile) if args.configFile else (projectDir / "config" / "params_template.env")
    jobHucLimit = args.jobHucLimit
    jobBranchLimit = args.jobBranchLimit
    overwrite = 1 if args.overwrite else 0
    evaluateCrosswalk = 1 if args.evalCrosswalk else 0

    # Validate Unit Denylist
    deny_unit_list = args.unitDenylist
    if not deny_unit_list:
        deny_unit_list = str(projectDir / "config" / "deny_unit.lst")
    elif deny_unit_list.upper() != "NONE" and not Path(deny_unit_list).is_file():
        print("Error: The -ud <unit deny file> does not exist and is not the word NONE")
        sys.exit(22)

    # Validate Branch Denylist
    deny_branches_list = args.branchDenylist
    if not deny_branches_list:
        deny_branches_list = str(projectDir / "config" / "deny_branches.lst")
    elif deny_branches_list.upper() != "NONE" and not Path(deny_branches_list).is_file():
        print("Error: The -bd <branch deny file> does not exist and is not the word NONE")
        sys.exit(22)

    # Validate Branch Zero Denylist
    deny_branch_zero_list = args.branchZeroDenylist
    has_deny_branch_zero_override = 0
    if not deny_branch_zero_list:
        deny_branch_zero_list = str(projectDir / "config" / "deny_branch_zero.lst")
    elif deny_branch_zero_list.upper() != "NONE":
        if not Path(deny_branch_zero_list).is_file():
            print("Error: The -zd <branch zero deny file> does not exist and is not the word NONE")
            sys.exit(22)
        else:
            has_deny_branch_zero_override = 1
    else:
        has_deny_branch_zero_override = 1

    # Overwrite check
    if outputDestDir.is_dir() and overwrite == 0:
        print(f"\nERROR: Output dir {outputDestDir} exists. Use overwrite -o to run.\n")
        sys.exit(22)

    # Prepare temp run directory
    if tempRunDir.is_dir():
        shutil.rmtree(tempRunDir, ignore_errors=True)
    tempRunDir.mkdir(parents=True, exist_ok=True)

    # Prepare output destination directory
    if not outputDestDir.is_dir():
        outputDestDir.mkdir(parents=True, exist_ok=True)
    else:
        for item in ["logs", "eval"]:
            p = outputDestDir / item
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)

        for pattern in ["crosswalk_table.csv", "fim_inputs*", "*.env", "huc_list.txt"]:
            for f in outputDestDir.glob(pattern):
                if f.is_file():
                    f.unlink()

    (outputDestDir / "logs").mkdir(parents=True, exist_ok=True)

    huc_list_output_file = outputDestDir / "huc_list.txt"
    full_huc_list_path = get_env(
        "FULL_HUC_LIST_PATH", str(get_env("inputsDir", "/data/inputs") + "/wbd/huc_list.txt")
    )

    # Execute check_huc_inputs.py
    check_huc_script = srcDir / "check_huc_inputs.py"
    if check_huc_script.is_file():
        cmd = [
            sys.executable,
            str(check_huc_script),
            "-u",
            str(args.hucList),
            "-i",
            str(full_huc_list_path),
            "-o",
            str(huc_list_output_file),
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        num_hucs = res.stdout.strip()
        print(f"\n--- Number of HUCs to process is {num_hucs}")

    # Copy params file
    if envFile.is_file():
        shutil.copy(envFile, outputDestDir / "params.env")

    # Write runtime_args.env file
    args_file = outputDestDir / "runtime_args.env"
    with open(args_file, "w") as f:
        f.write(f"export runName={args.runName}\n")
        f.write(f"export jobHucLimit={jobHucLimit}\n")
        f.write(f"export jobBranchLimit={jobBranchLimit}\n")
        f.write(f"export deny_unit_list={deny_unit_list}\n")
        f.write(f"export deny_branches_list={deny_branches_list}\n")
        f.write(f"export deny_branch_zero_list={deny_branch_zero_list}\n")
        f.write(f"export has_deny_branch_zero_override={has_deny_branch_zero_override}\n")
        f.write(f"export evaluateCrosswalk={evaluateCrosswalk}\n")

    os.chmod(args_file, 0o777)
    print("--- Pre-processing is complete\n")


if __name__ == "__main__":
    main()
