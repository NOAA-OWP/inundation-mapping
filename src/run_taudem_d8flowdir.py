#!/usr/bin/env python3
import argparse
import subprocess
import sys


def run_d8flowdir(ncores, taudem_dir, fel_path, p_path=None, sd8_path=None):
    """
    Executes TauDEM d8flowdir, intercepting and formatting output streams.
    Throws an error if the underlying process fails.
    """

    # Check that at least one output is requested (-p or -sd8)
    if not p_path and not sd8_path:
        print("CRITICAL: You must provide at least one output path (-p or -sd8).", file=sys.stderr)
        sys.exit(1)

    # Construct the base command dynamically
    cmd = ["mpiexec", "-n", str(ncores), f"{taudem_dir}/d8flowdir", "-fel", fel_path]

    # Add optional arguments
    if p_path:
        cmd.extend(["-p", p_path])
    if sd8_path:
        cmd.extend(["-sd8", sd8_path])

    try:
        # We merge stderr into stdout (stderr=subprocess.STDOUT) to process both in one loop.
        # bufsize=1 and universal_newlines=True (text=True) ensure line-buffered text output.
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )

        # Read the output line by line as it is generated
        for line in process.stdout:

            # Drop the specific GDAL ERROR 6 message entirely
            if "ERROR 6:" in line and "Dataset does not support the AddBand() method." in line:
                continue

            # 3. Reformat the warnings specifically based on which file is missing
            elif "no output sd8 file specified" in line:
                print("INFO: TauDEM d8flowdir running without optional sd8 slope output.")
            elif "no output p file specified" in line:
                print("INFO: TauDEM d8flowdir running without optional p flow direction output.")

            # Print everything else normally (errors, standard output, etc.)
            else:
                # Use end="" because the line already has a newline character from the stream
                print(line, end="")

        # Wait for the process to fully complete and grab the exit code
        process.wait()

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"CRITICAL: TauDEM d8flowdir failed with exit code {process.returncode}")

    except Exception as e:
        # Ensure the exception is printed to standard error for downstream catch
        print(f"\n{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run TauDEM d8flowdir with custom stream filtering.")
    parser.add_argument("-n", "--ncores", type=int, required=True, help="Number of MPI cores to use")
    parser.add_argument("-t", "--taudem_dir", type=str, required=True, help="Path to TauDEM binaries")
    parser.add_argument("-fel", "--fel_path", type=str, required=True, help="Input filled DEM path")
    parser.add_argument("-p", "--p_path", type=str, required=False, help="Output flow direction path")
    parser.add_argument("-sd8", "--sd8_path", type=str, required=False, help="Output slope path")

    args = parser.parse_args()

    run_d8flowdir(args.ncores, args.taudem_dir, args.fel_path, args.p_path, args.sd8_path)
