#!/usr/bin/env python3
import argparse
import subprocess
import sys


def run_d8flowdir(ncores, taudem_dir, fel_path, p_path=None, sd8_path=None):
    """
    Executes TauDEM d8flowdir, intercepting and formatting output streams.
    Throws an error if the underlying process fails.
    """

    try:
        # Check that at least one output is requested (-p or -sd8)
        if not p_path and not sd8_path:
            raise Exception(
                "CRITICAL ERROR: You must provide at least one output path (-p or -sd8).", file=sys.stderr
            )

            sys.exit(1)

        # Construct the base command dynamically
        cmd = ["mpiexec", "-n", str(ncores), f"{taudem_dir}/d8flowdir", "-fel", fel_path]

        # Add optional arguments
        if p_path:
            cmd.extend(["-p", p_path])
        if sd8_path:
            cmd.extend(["-sd8", sd8_path])

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

            # Reformat the warnings specifically based on which file is missing
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
            raise RuntimeError(f"TauDEM d8flowdir failed with exit code {process.returncode}")

    except Exception as e:
        # Ensure the exception is printed to standard error for downstream catch
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)


def run_flowdircond(taudem_dir, p_path, z_path, zfdc_path):
    """
    Executes TauDEM flowdircond, intercepting and filtering error streams.
    Ignores benign GDAL AddBand() errors and throws an error if the process fails.

    Args:
        taudem_dir: Path to TauDEM binaries
        p_path: Input flow direction file path
        z_path: Input DEM file path
        zfdc_path: Output conditioned DEM file path
    """

    # Construct the command
    cmd = [f"{taudem_dir}/flowdircond", "-p", p_path, "-z", z_path, "-zfdc", zfdc_path]

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

            # Print everything else normally (errors, standard output, etc.)
            else:
                # Use end="" because the line already has a newline character from the stream
                print(line, end="")

        # Wait for the process to fully complete and grab the exit code
        process.wait()

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM flowdircond failed with exit code {process.returncode}")

    except Exception as e:
        # Ensure the exception is printed to standard error for downstream catch
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)


def run_streamnet(
    taudem_dir, p_path, fel_path, ad8_path, src_path, ord_path, tree_path, coord_path, w_path, net_path
):
    """
    Executes TauDEM streamnet, intercepting and filtering error streams.
    Ignores benign GDAL AddBand() errors and throws an error if the process fails.

    Args:
        taudem_dir: Path to TauDEM binaries
        p_path: Input flow direction file path
        fel_path: Input DEM file path
        ad8_path: Input flow accumulation file path
        src_path: Input stream pixels file path
        ord_path: Output stream order file path
        tree_path: Output tree file path
        coord_path: Output coordinate file path
        w_path: Output catchments/weights file path
        net_path: Output reaches shapefile path
    """

    # Construct the command
    cmd = [
        f"{taudem_dir}/streamnet",
        "-p",
        p_path,
        "-fel",
        fel_path,
        "-ad8",
        ad8_path,
        "-src",
        src_path,
        "-ord",
        ord_path,
        "-tree",
        tree_path,
        "-coord",
        coord_path,
        "-w",
        w_path,
        "-net",
        net_path,
    ]

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

            # Print everything else normally (errors, standard output, etc.)
            else:
                # Use end="" because the line already has a newline character from the stream
                print(line, end="")

        # Wait for the process to fully complete and grab the exit code
        process.wait()

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM streamnet failed with exit code {process.returncode}")

    except Exception as e:
        # Ensure the exception is printed to standard error for downstream catch
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)


def run_gagewatershed(ncores, taudem_dir, p_path, gw_path, o_path, id_path):
    """
    Executes TauDEM gagewatershed, intercepting and filtering error streams.
    Ignores benign GDAL AddBand() errors and throws an error if the process fails.

    Args:
        ncores: Number of MPI cores to use
        taudem_dir: Path to TauDEM binaries
        p_path: Input flow direction file path
        gw_path: Output gage watershed file path
        o_path: Output outlet/split points file path
        id_path: Input ID file path
    """

    # Construct the command with MPI
    cmd = [
        "mpiexec",
        "-n",
        str(ncores),
        f"{taudem_dir}/gagewatershed",
        "-p",
        p_path,
        "-gw",
        gw_path,
        "-o",
        o_path,
        "-id",
        id_path,
    ]

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

            # Print everything else normally (errors, standard output, etc.)
            else:
                # Use end="" because the line already has a newline character from the stream
                print(line, end="")

        # Wait for the process to fully complete and grab the exit code
        process.wait()

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM gagewatershed failed with exit code {process.returncode}")

    except Exception as e:
        # Ensure the exception is printed to standard error for downstream catch
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)


def run_catchhydrogeo(taudem_dir, hand_path, catch_path, catchlist_path, slp_path, h_path, table_path):
    """
    Executes TauDEM catchhydrogeo, intercepting and filtering error streams.
    Ignores benign GDAL AddBand() errors and throws an error if the process fails.

    Args:
        taudem_dir: Path to TauDEM binaries
        hand_path: Input HAND (Height Above Nearest Drain) file path
        catch_path: Input catchment file path
        catchlist_path: Input catchment list file path
        slp_path: Input slope file path
        h_path: Input stage/height file path
        table_path: Output table file path
    """

    # Construct the command
    cmd = [
        f"{taudem_dir}/catchhydrogeo",
        "-hand",
        hand_path,
        "-catch",
        catch_path,
        "-catchlist",
        catchlist_path,
        "-slp",
        slp_path,
        "-h",
        h_path,
        "-table",
        table_path,
    ]

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

            # Print everything else normally (errors, standard output, etc.)
            else:
                # Use end="" because the line already has a newline character from the stream
                print(line, end="")

        # Wait for the process to fully complete and grab the exit code
        process.wait()

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM catchhydrogeo failed with exit code {process.returncode}")

    except Exception as e:
        # Ensure the exception is printed to standard error for downstream catch
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """
    Main CLI entry point for TauDEM subprocess wrapper.
    Parses command-line arguments and routes to the appropriate TauDEM function.
    """
    parser = argparse.ArgumentParser(
        description="Run TauDEM tools (d8flowdir, flowdircond, streamnet, gagewatershed, or catchhydrogeo)"
    )

    # Create subparsers for different TauDEM commands
    subparsers = parser.add_subparsers(dest="command", help="TauDEM command to run")

    # d8flowdir subcommand
    d8_parser = subparsers.add_parser("d8flowdir", help="Run TauDEM d8flowdir")
    d8_parser.add_argument("-n", "--ncores", type=int, required=True, help="Number of MPI cores to use")
    d8_parser.add_argument("-t", "--taudem_dir", type=str, required=True, help="Path to TauDEM binaries")
    d8_parser.add_argument("-fel", "--fel_path", type=str, required=True, help="Input filled DEM path")
    d8_parser.add_argument("-p", "--p_path", type=str, required=False, help="Output flow direction path")
    d8_parser.add_argument("-sd8", "--sd8_path", type=str, required=False, help="Output slope path")

    # flowdircond subcommand
    fdc_parser = subparsers.add_parser("flowdircond", help="Run TauDEM flowdircond")
    fdc_parser.add_argument("-t", "--taudem_dir", type=str, required=True, help="Path to TauDEM binaries")
    fdc_parser.add_argument("-p", "--p_path", type=str, required=True, help="Input flow direction file path")
    fdc_parser.add_argument("-z", "--z_path", type=str, required=True, help="Input DEM file path")
    fdc_parser.add_argument(
        "-zfdc", "--zfdc_path", type=str, required=True, help="Output conditioned DEM file path"
    )

    # streamnet subcommand
    sn_parser = subparsers.add_parser("streamnet", help="Run TauDEM streamnet")
    sn_parser.add_argument("-t", "--taudem_dir", type=str, required=True, help="Path to TauDEM binaries")
    sn_parser.add_argument("-p", "--p_path", type=str, required=True, help="Input flow direction file path")
    sn_parser.add_argument("-fel", "--fel_path", type=str, required=True, help="Input DEM file path")
    sn_parser.add_argument(
        "-ad8", "--ad8_path", type=str, required=True, help="Input flow accumulation file path"
    )
    sn_parser.add_argument(
        "-src", "--src_path", type=str, required=True, help="Input stream pixels file path"
    )
    sn_parser.add_argument(
        "-ord", "--ord_path", type=str, required=True, help="Output stream order file path"
    )
    sn_parser.add_argument("-tree", "--tree_path", type=str, required=True, help="Output tree file path")
    sn_parser.add_argument(
        "-coord", "--coord_path", type=str, required=True, help="Output coordinate file path"
    )
    sn_parser.add_argument(
        "-w", "--w_path", type=str, required=True, help="Output catchments/weights file path"
    )
    sn_parser.add_argument(
        "-net", "--net_path", type=str, required=True, help="Output reaches shapefile path"
    )

    # gagewatershed subcommand
    gw_parser = subparsers.add_parser("gagewatershed", help="Run TauDEM gagewatershed")
    gw_parser.add_argument("-n", "--ncores", type=int, required=True, help="Number of MPI cores to use")
    gw_parser.add_argument("-t", "--taudem_dir", type=str, required=True, help="Path to TauDEM binaries")
    gw_parser.add_argument("-p", "--p_path", type=str, required=True, help="Input flow direction file path")
    gw_parser.add_argument(
        "-gw", "--gw_path", type=str, required=True, help="Output gage watershed file path"
    )
    gw_parser.add_argument(
        "-o", "--o_path", type=str, required=True, help="Output outlet/split points file path"
    )
    gw_parser.add_argument("-id", "--id_path", type=str, required=True, help="Input ID file path")

    # catchhydrogeo subcommand
    ch_parser = subparsers.add_parser("catchhydrogeo", help="Run TauDEM catchhydrogeo")
    ch_parser.add_argument("-t", "--taudem_dir", type=str, required=True, help="Path to TauDEM binaries")
    ch_parser.add_argument("-hand", "--hand_path", type=str, required=True, help="Input HAND file path")
    ch_parser.add_argument(
        "-catch", "--catch_path", type=str, required=True, help="Input catchment file path"
    )
    ch_parser.add_argument(
        "-catchlist", "--catchlist_path", type=str, required=True, help="Input catchment list file path"
    )
    ch_parser.add_argument("-slp", "--slp_path", type=str, required=True, help="Input slope file path")
    ch_parser.add_argument("-H", "--h_path", type=str, required=True, help="Input stage/height file path")
    ch_parser.add_argument("-table", "--table_path", type=str, required=True, help="Output table file path")

    args = parser.parse_args()

    if args.command == "d8flowdir":
        run_d8flowdir(args.ncores, args.taudem_dir, args.fel_path, args.p_path, args.sd8_path)
    elif args.command == "flowdircond":
        run_flowdircond(args.taudem_dir, args.p_path, args.z_path, args.zfdc_path)
    elif args.command == "streamnet":
        run_streamnet(
            args.taudem_dir,
            args.p_path,
            args.fel_path,
            args.ad8_path,
            args.src_path,
            args.ord_path,
            args.tree_path,
            args.coord_path,
            args.w_path,
            args.net_path,
        )
    elif args.command == "gagewatershed":
        run_gagewatershed(args.ncores, args.taudem_dir, args.p_path, args.gw_path, args.o_path, args.id_path)
    elif args.command == "catchhydrogeo":
        run_catchhydrogeo(
            args.taudem_dir,
            args.hand_path,
            args.catch_path,
            args.catchlist_path,
            args.slp_path,
            args.h_path,
            args.table_path,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
