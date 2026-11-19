#!/usr/bin/env python3
import argparse
import os
import shutil
import signal
import subprocess
import sys
import traceback

from pathlib import Path

import numpy as np
import pandas as pd

from src.utils.io import calculate_gdal_cache_mb


def run_cmd_streamed(
    cmd: list[str],
    env: dict[str, str],
    do_return_std_out = False
):
    """Run a command and process combined stdout/stderr line by line without pipe deadlocks.
    Returns:
        - a list of strings. It will always include errors, but if do_return_std_out, then it also
          returns the stdout value.
    """
    rtn_output = []

    # if do_return_std_out is True, then we capture the stdOut and strErr and return them
    # as a list of strings.
    # We merge stderr into stdout (stderr=subprocess.STDOUT) to process both in one loop.
    # bufsize=1 and universal_newlines=True (text=True) ensure line-buffered text output.
    with subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE if do_return_std_out else subprocess.DEVNULL,
        # stdout=subprocess.DEVNULL,
        # stderr=subprocess.PIPE,
        stderr=subprocess.STDOUT if do_return_std_out else subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True
    ) as proc:

        try:
            # We will always get either one or the other back
            rtn_stdout, rtn_stderr = proc.communicate()
              
            if do_return_std_out:
                # stderr and stdout are both merged into rtn_stdout by default (subprocess.STDOUT)
                if proc.returncode != 0:
                    raise RuntimeError(f"TauDEM d8flowdir failed with exit code {proc.returncode}."
                                       f"Details: {rtn_stdout.strip()}")
                else:
                    rtn_output = rtn_stdout.splitlines()
            else:
                # We throw an error with stderr output, but only if there is an error
                # otherwise we drop it
                if proc.returncode != 0:
                    raise RuntimeError(f"TauDEM d8flowdir failed with exit code {proc.returncode}."
                                       f" Details: {rtn_stderr.strip()}")
            
        except BaseException:
            # Kill mpiexec and all child ranks cleanly on error or Ctrl+C
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
            raise

    return rtn_output


# Note: Nov 2026: Taudem 5.5, mandates both the p and s8 path even if you don't use both
# In the calls to run_taudem_subprocess for here, I just put in temp files.
def run_d8flowdir(ncores, taudem_dir, fel_path, out_p_path="", out_sd8_path=""):
    """
    Executes TauDEM d8flowdir, intercepting and formatting output streams.
    Throws an error if the underlying process fails.

    Args:
        ncores: Number of MPI cores to use
        taudem_dir: Path to TauDEM binaries
        fel_path: Input flow direction file path
        gw_path: Output gage watershed file path
        o_path: Output outlet/split points file path
        id_path: Input ID file path    

    """

    # NOTE: Nov 2026: TauDEM 5.5 now mandiates both p and sd8

    try:
        # Check that at least one output is requested (-p or -sd8)
        if out_p_path == None or out_p_path == "" or out_sd8_path == None or out_sd8_path == "":
            raise Exception(
                "CRITICAL ERROR: You must provide both -p and -sd8 paths, even if you only need one.",
                  file=sys.stderr
            )

        # Temp Test: Adjust number of workers based on file size of the input raster. Most of our rastsers here are between
        # 5 and 15 MiB in size but some of the AK ones jump to 400 to 450 MiB and it is affecting MPIexec and
        # Taudem performance. We will use file size to gage some args for processing. In this case
        # we will give it more jobs
        # file_size_bytes = os.path.getsize("path/to/raster.tif")


        # Nov 2026:  Taudem is very, very slow and struggles to process d8flowdir.
        # After lots of Google/Gemini, it was recommended that various reasons, especially volite sizes
        # do this in three steps. Uncompress, process, recompress with ZSTD. NoteL GDAL Tier config
        # cache sizes, and various other global GDAL settings are critical here to manage resourcees
        # and performance.

        # # Temp staging processing files.
        # tmp_folder, tmp_file_name = os.path.split(fel_path)


        # TEMP.. only needed it we want to do stage for un-encrypt, process, encrypt

        # stage_fel_path = Path(os.path.join(tmp_folder, f"stage_taudem_fel_{tmp_file_name}"))
        # stage_fel_path.unlink(missing_ok=True)  # In case residue exists

        # # tmp_file_name = os.path.basename(out_p_path)
        # stage_p_path = Path(os.path.join(tmp_folder, f"stage_taudem_p_{tmp_file_name}"))
        # stage_p_path.unlink(missing_ok=True)  # In case residue exists

        # # tmp_file_name = os.path.basename(out_sd8_path)
        # stage_sd8_path = Path(os.path.join(tmp_folder, f"stage_taudem_sd8_{tmp_file_name}"))
        # stage_sd8_path.unlink(missing_ok=True)  # In case residue exists


        # # Dynamically size GDAL_CACHEMAX based on compressed input file size
        cache_mb = calculate_gdal_cache_mb(Path(fel_path))

        print(f"cache size is {cache_mb}")
        env = os.environ.copy()
        env["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
        env["GDAL_CACHEMAX"] = str(cache_mb)
        # # env["GDAL_CACHEMAX"] = "512"
        # # env["GDAL_NUM_THREADS"] = "1"
        # # env["OMP_NUM_THREADS"] = "1"
        # # env["GDAL_MAX_DATASET_POOL_RAM_USAGE"] = "268435456" # (256 MB per rank)
        # # env["GDAL_MAX_DATASET_POOL_RAM_USAGE"] = "2147483648"

        # # Note: Uncompressing, process, recompress slows small files down but only a tad
        # # but for big files, it makes a major difference in processing time.

        # # ---------------------------
        # # 1. UNCOMPRESS: Single-pass LZW/ZSTD -> uncompressed row-striped BigTIFF
        # print("start uncompress")
        # _ = run_cmd_streamed(
        #     [
        #         "gdal_translate", "-q", "-of", "GTiff",
        #         "-co", "TILED=NO",
        #         "-co", "COMPRESS=NONE",
        #         "-co", "BIGTIFF=YES",
        #         str(fel_path),
        #         str(stage_fel_path),
        #     ],
        #     env=env
        # )

        # # ---------------------------
        # # 2. Run TauDEM d8flowdir
        # # Construct the base command dynamically
        # # NOTE: Nov 2026: TauDEM 5.5 now mandiates both p and sd8
        # print("start processing")        
        # d8_cmd = ["mpiexec", 
        #           "-n", str(ncores),
        #          f"{taudem_dir}/d8flowdir",
        #           "-fel", str(stage_fel_path),
        #           "-p", str(stage_p_path),
        #           "-sd8", str(stage_sd8_path),
        #          ]

        # rtn_output = run_cmd_streamed(
        #     d8_cmd,
        #     env=env,
        #     do_return_std_out=False
        # )

        # # Free uncompressed input immediately
        # stage_fel_path.unlink(missing_ok=True)

        # # ---------------------------
        # # 2.b Parse the output from processing d8flowdir
        # for line in rtn_output:

        #     # Drop the specific GDAL ERROR 6 message entirely
        #     # NOTE: Nov 2026: To we even need this now with TauDEM 5.5?
        #     if "ERROR 6:" in line and "Dataset does not support the AddBand() method." in line:
        #         continue
        #     if "......." in line:
        #         continue

        #     # Print everything else normally (errors, standard output, etc.)
        #     else:
        #         print(line)

        # # if do_return_std_out is False: it is ok to let it print nothing


        # # ---------------------------
        # # 3. RE-COMPRESS: Export final ZSTD COGs (512x512 blocks)
        # print("start re-compress")        
        # for src, dst, pred in [
        #     (stage_p_path, out_p_path, "2"),
        #     (stage_sd8_path, out_sd8_path, "3"),
        # ]:
        #     _ = run_cmd_streamed(
        #         [
        #             "gdal_translate", "-q", "-of", "COG",
        #             "-co", "COMPRESS=ZSTD",
        #             "-co", "LEVEL=3",
        #             "-co", f"PREDICTOR={pred}",
        #             "-co", "BLOCKSIZE=512",
        #             "-co", "OVERVIEWS=NONE",
        #             "-co", "NUM_THREADS=1",
        #             "-co", "BIGTIFF=IF_SAFER",
        #             str(src),
        #             str(dst),
        #         ],
        #         env=env,
        #     )
        #     src.unlink(missing_ok=True)
        # return 0

        # # We merge stderr into stdout (stderr=subprocess.STDOUT) to process both in one loop.
        # # Note: Bufsize=1 slow things down a lot.

        # Write them to docker tmp file which is faster than docker mounts even EBS

        ncores = 4
        cmd = ["mpiexec", "-n", str(ncores),
               f"{taudem_dir}/d8flowdir",
                "-fel", fel_path,
                "-p", out_p_path,
                "-sd8", out_sd8_path,
               ]
        # with (
        #     subprocess.Popen(
        #         cmd,
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.STDOUT,
        #         text=True,
        #         bufsize=1,
        #         start_new_session=True,
        #         ) as proc
        # ):
                
            # assert proc.stdout is not None
            # err_details = ""

            # for line in proc.stdout:
            #     # Drop the specific GDAL ERROR 6 message entirely
            #     # TODO Nov 2026: With TauDEM 5.5, do we even need this anymore?
            #     if "ERROR 6:" in line and "Dataset does not support the AddBand() method." in line:
            #         continue
            #     elif "ERROR:":
            #         err_details += line
            #         continue  # there can be more than one error
            #     # Reformat the warnings specifically based on which file is missing

            #     if "......." in line:
            #         continue

            #     # Print everything else normally (errors, standard output, etc.)
            #     else:
            #         # Use end="" because the line already has a newline character from the stream
            #         print(line, end="")

            # # Enforce strict exit code checking. If it failed, raise an exception.
            # if proc.returncode != 0:
            #     raise RuntimeError(f"TauDEM d8flowdir failed with exit code {proc.returncode}." \
            #                     f" Details: {err_details}")

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
                print(line, end="", flush=True)

        # Wait for the process to fully complete and grab the exit code
        process.wait()

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM d8flowdir failed with exit code {process.returncode}")
        
        # print("Moving files from Docker memory temp to Docker mount folders")
        # if out_p_path != None and out_p_path != "":
        #     shutil.move(stage_p_path, out_p_path)

        # if out_sd8_path != None and out_sd8_path != "":
        #     shutil.move(stage_sd8_path, out_sd8_path)

    except Exception as e:
        # Ensure the exception is printed to standard error for downstream catch
        print(f"\nERROR: {e}", file=sys.stderr, flush=True)
        print(traceback.format_exc())
        sys.exit(1)

    # finally:
    #     for f in (stage_fel_path, stage_p_path, stage_sd8_path):
    #         f.unlink(missing_ok=True)        


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


    try:
        err_details = ""

        # Construct the command
        cmd = ["mpiexec", "-n", "1",
                f"{taudem_dir}/flowdircond", "-p", p_path, "-z", z_path, "-zfdc", zfdc_path]

        # We merge stderr into stdout (stderr=subprocess.STDOUT) to process both in one loop.
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        ) as process:

            # Read the output line by line as it is generated
            for line in process.stdout:

                # Drop the specific GDAL ERROR 6 message entirely
                if "ERROR 6:" in line and "Dataset does not support the AddBand() method." in line:
                    continue
                elif "ERROR:":
                    err_details += line
                    continue  # there can be more than one error
                # Print everything else normally (errors, standard output, etc.)
                else:
                    # Use end="" because the line already has a newline character from the stream
                    print(line, end="")

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM flowdircond failed with exit code {process.returncode}." \
                f" Details: {err_details}")

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
    cmd = ["mpiexec", "-n", "1",
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
        err_details = ""

        # We merge stderr into stdout (stderr=subprocess.STDOUT) to process both in one loop.
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        ) as process:
            # Read the output line by line as it is generated
            for line in process.stdout:

                # Drop the specific GDAL ERROR 6 message entirely
                if "ERROR 6:" in line and "Dataset does not support the AddBand() method." in line:
                    continue
                elif "ERROR:":
                    err_details += line
                    continue  # there can be more than one error
                # Print everything else normally (errors, standard output, etc.)
                else:
                    # Use end="" because the line already has a newline character from the stream
                    print(line, end="")

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM streamnet failed with exit code {process.returncode}." \
				f" Details: {err_details}")

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

    # Construct the command
    cmd = ["mpiexec",
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
            id_path
    ]

    try:
        err_details = ""

        # We merge stderr into stdout (stderr=subprocess.STDOUT) to process both in one loop.
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        ) as process:
            # Read the output line by line as it is generated
            for line in process.stdout:

                # Drop the specific GDAL ERROR 6 message entirely
                if "ERROR 6:" in line and "Dataset does not support the AddBand() method." in line:
                    continue
                elif "ERROR:":
                    err_details += line
                    continue  # there can be more than one error
                # Print everything else normally (errors, standard output, etc.)
                else:
                    # Use end="" because the line already has a newline character from the stream
                    print(line, end="")

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM gagewatershed failed with exit code {process.returncode}." \
				f" Details: {err_details}")

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
    cmd = ["mpiexec", "-n", "1",
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
            table_path
    ]

    try:
        err_details = ""

        # We merge stderr into stdout (stderr=subprocess.STDOUT) to process both in one loop.
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        ) as process:

            # Run the process and capture the streams, it will wait for it.
            # stdout, stderr = process.communicate()

            # Read the output line by line as it is generated
            for line in process.stdout:
                # Drop the specific GDAL ERROR 6 message entirely
                if "ERROR 6:" in line and "Dataset does not support the AddBand() method." in line:
                    continue
                elif "ERROR:":
                    err_details += line
                    continue  # there can be more than one error
                # Print everything else normally (errors, standard output, etc.)
                else:
                    # Use end="" because the line already has a newline character from the stream
                    print(line, end="")

        # Enforce strict exit code checking. If it failed, raise an exception.
        if process.returncode != 0:
            raise RuntimeError(f"TauDEM catchhydrogeo failed with exit code {process.returncode}." \
				f" Details: {err_details}")
                
        # ++++++++++++++++++++++++++++++++
        # Oct 2026,
        # After talking to Carson, we thought it would be better to adjust things here for now.
        
        # With the new TauDEM 5.5, it also completely changed column names.
        # As we have so many downstream that is affected, just load only the columns we need
        # and change the column names.
        col_mapping = {'Id': 'CatchId',
                    'Stage_m': 'Stage',
                    'ReachWetArea_m2': 'SurfaceArea (m2)',
                    'ReachBedArea_m2': 'BedArea (m2)',
                    'ReachVolume_m3': 'Volume (m3)',
                    'ReachSlope': 'SLOPE',
                    'ReachLength_m': 'LENGTHKM',
                    'CatchArea_m2': 'AREASQKM'
        }
        # ++++ Note: While our original column was incorrectly named "LENGTHKM", it was really in meters
        #  even in our pre TauDEM 5.5 code.
        hydro_df = pd.read_csv(table_path)
        
        # For reasons unknown, when TauDEM makes the src_base_{branchid}.csv  (table_path), it is having a bit of trouble
        # with the column headers. The first column 'ID' is fine but all of the rest of the column headers names have a space
        # in front of them. ie) ' Stage_m' instead of `Stage_m` and ` ReachSlope`, instead of `ReachSlope.`
        # We will load the file back up, strip the header names and resave.
        # Strip spaces first, then rename if it matches your dictionary
        hydro_df = hydro_df.rename(columns=lambda x: col_mapping.get(x.strip(), x.strip()))

        hydro_df['AREASQKM'] = np.where(hydro_df['AREASQKM'] != 0, hydro_df['AREASQKM'] / 1000000, hydro_df['AREASQKM'])

        hydro_df.to_csv(table_path, index=False)

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
