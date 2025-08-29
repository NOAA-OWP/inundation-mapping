#!/usr/bin/env python3
import subprocess
import os
from pathlib import Path
import errno
import argparse
import re
import glob
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd
from datetime import datetime

def compile_error_logs(fim_run_dir,hucs):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    outfile = os.path.join(fim_run_dir,'logs', f"all_errors_from_logs_{timestamp}.log")

    with open(outfile, "w") as out_f:
        for huc in hucs:
            huc_log_dir = os.path.join(fim_run_dir, huc, "logs")

            # Find all files named all_errors_from_logs.log in this folder
            for file in glob.glob(os.path.join(huc_log_dir, "huc_errors_from_logs.log")):
                out_f.write(f"===== From {file} =====\n")
                with open(file, "r") as f:
                    out_f.write(f.read())
                    out_f.write("\n")

    
def manual_postprocessing(fim_run_dir: str, limit_hucs: list = []):
    #notes
    #1- this manual postprocessing will read an existing FIM run with mmultiple HUC results and will overwrite the hyrotable (or src table)
    #2- accordingly, the log files are overwrtten to be consistent with updated hyrtables.
    #3- The flags to activate/deactivate each postprocessing script is still read from $outputDestDir/params.env file. So if a step is not required 
    # the $outputDestDir/params.env file (available in output folder) needs to be updated (and not config/params_template.env of the code itself)
    # this is again to have consistent results across a fim output--the param.env be consistent with the last postprocessing applied. 

    # Check that fim run directory exists
    if not os.path.exists(fim_run_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fim_run_dir)
    
    # Get the list of all hucs in the directory
    all_existing_hucs = [d for d in os.listdir(fim_run_dir) if re.match(r'^\d{8}$', d)]
    hucs = []
    for huc in all_existing_hucs:
        if os.path.isdir(os.path.join(fim_run_dir, huc)):
            hucs.append(huc)

    if limit_hucs:
        hucs = [h for h in limit_hucs if h in hucs]

    # as env variables, pass fim run directory (containing params.env) and src directory (containing bash_variables.env) into calibrate_htable.sh 
    env = os.environ.copy()
    env["outputDestDir"] = fim_run_dir

    srcDir = env.get("srcDir")
    env["srcDir"] = srcDir

    #create path to the target script (calibrate_htable.sh)
    script_path = str(Path(srcDir) / "calibrate_htable.sh")

    for huc in hucs:
        huc_path=os.path.join(fim_run_dir, huc)
        env["tempHucDataDir"] = huc_path

        # Call the shell script
        # TODO make sure to stop the code or log properly if one postprocessing step failed
        result = subprocess.run(
            ["bash", script_path, huc, 'True'],
            env=env,
            capture_output=False,  
            text=True 
        )

        if result.returncode != 0:
            print(f"[ERROR] Postprocessing failed for HUC {huc}")
            continue
        print(f"Postprocessing finished for HUC {huc}")


    # finally compile error log files
    compile_error_logs(fim_run_dir,hucs)
    

 

if __name__ == "__main__":

    # sample usage
    # python foss_fim/tools/manual_fim_postprocessing.py


    # Parse arguments
    parser = argparse.ArgumentParser(description="Perform manual fim postprocessing (after a fim pipeline run)")
    parser.add_argument(
        "-i", "--fim_run_dir", help="Directory path to FIM run directory.", required=True, type=str
    )


    parser.add_argument(
        "-u",
        "--limit_hucs",
        help="Optional. If specified, postprocessing steps are applied only for these limited HUCs.",
        required=False,
        type=str,
        nargs="+",
    )

    start = timer()

    manual_postprocessing(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")

