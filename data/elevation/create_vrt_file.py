#!/usr/bin/env python3

import argparse
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from osgeo import gdal

import src.utils.shared_functions as sf
from src.utils.shared_functions import FIM_Helpers as fh


gdal.UseExceptions()


def valid_dir(path_str):
    """Validates that a string path exists and is a directory."""
    path = Path(path_str)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"The path '{path_str}' does not exist.")
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"The path '{path_str}' is a file, not a directory.")
    return str(path)


def _validate_shared_ancestor(vrt_output_dir: str, src_directories: list[str]) -> str:
    """
    Ensures that the VRT output folder and all source DEM folders share a
    valid directory ancestor above the root filesystem ('/').

    Returns the common ancestor path if valid; raises ValueError otherwise.
    """
    real_out = os.path.realpath(vrt_output_dir)
    real_srcs = [os.path.realpath(d) for d in src_directories]

    common_root = os.path.commonpath([real_out] + real_srcs)

    if common_root in ("/", "") or os.path.splitdrive(common_root)[1] == "\\":
        raise ValueError(
            f"Portability Error: The VRT output directory '{vrt_output_dir}' and the "
            f"input directories {src_directories} do not share a common mount/data subtree. "
            f"Their lowest common ancestor is the root directory ('{common_root}').\n"
            f"To guarantee portable relative paths across Docker, QGIS, and host machines, "
            f"place the VRT inside the same parent volume tree as the DEM inputs."
        )

    logging.info(f"Portability check passed. Shared root: {common_root}")
    return common_root


def create_vrt_file(src_directories, vrt_basename, vrt_output_dir=None, vrt_suffix=None):
    """
    Takes all .tif files in one or more given directories, generates a single
    relative VRT, and validates portability across container/host boundaries.
    """
    if not src_directories:
        raise ValueError("At least one source directory must be specified.")

    if isinstance(src_directories, str):
        src_directories = [src_directories]

    src_directories = [os.path.realpath(d) for d in src_directories]
    for src_dir in src_directories:
        if not os.path.exists(src_dir):
            raise ValueError(f'Source directory "{src_dir}" does not exist.')

    # Resolve Output Directory
    if not vrt_output_dir:
        vrt_out_dir = src_directories[0]
    else:
        vrt_out_dir = os.path.realpath(vrt_output_dir)

    # Validate that VRT and inputs share a common non-root parent
    _validate_shared_ancestor(vrt_out_dir, src_directories)

    os.makedirs(vrt_out_dir, exist_ok=True)

    base_name_clean, _ = os.path.splitext(vrt_basename)
    suffix_str = f"_{vrt_suffix}" if vrt_suffix else ""
    vrt_file_name = f"{base_name_clean}{suffix_str}.vrt"
    target_vrt_file_path = os.path.join(vrt_out_dir, vrt_file_name)

    start_time = datetime.now(timezone.utc)
    fh.print_start_header("Creating vrt file", start_time)

    __setup_logger(vrt_out_dir)
    logging.info(f"Saving VRT to {target_vrt_file_path}")

    # Discover TIFFs
    all_tif_files = []
    for src_dir in src_directories:
        logging.info(f"Searching for .tif files in: {src_dir}")
        tif_files = fh.get_file_names(src_dir, ".tif")
        for f in tif_files:
            full_path = f if os.path.isabs(f) else os.path.join(src_dir, f)
            all_tif_files.append(os.path.realpath(full_path))

    all_tif_files = sorted(list(set(all_tif_files)))
    if not all_tif_files:
        raise FileNotFoundError("No .tif files were found in any of the specified directories.")

    # 1. Build the VRT with GDAL and flush
    __create_vrt(all_tif_files, target_vrt_file_path)

    # 2. Patch XML paths strictly relative to the VRT directory
    __force_relative_paths(target_vrt_file_path)

    end_time = datetime.now(timezone.utc)
    fh.print_end_header("Finished creating vrt file", start_time, end_time)
    logging.info(fh.print_date_time_duration(start_time, end_time))


def __create_vrt(tif_file_names, target_vrt_file_path):
    logging.info("Building VRT with source files:")
    for file_name in tif_file_names:
        logging.info(f" - {file_name}")

    vrt_options = gdal.BuildVRTOptions(srcNodata=-999999.0, VRTNodata=-999999.0)
    ds = gdal.BuildVRT(target_vrt_file_path, tif_file_names, options=vrt_options)

    # Flush SWIG memory handle to disk before XML modifications
    ds.FlushCache()
    del ds
    logging.info(f"GDAL BuildVRT completed: {target_vrt_file_path}")


def __force_relative_paths(vrt_file_path):
    """
    Parses the generated VRT XML and guarantees every SourceFilename tag
    uses a valid relative path from the VRT's parent folder.
    """
    vrt_dir = os.path.dirname(os.path.realpath(vrt_file_path))
    tree = ET.parse(vrt_file_path)
    root = tree.getroot()

    modified = False
    for source_fn in root.iter("SourceFilename"):
        current_path = source_fn.text.strip() if source_fn.text else ""
        if not current_path:
            continue

        if not os.path.isabs(current_path):
            candidate_abs = os.path.normpath(os.path.join(vrt_dir, current_path))
        else:
            candidate_abs = current_path

        candidate_real = os.path.realpath(candidate_abs)
        true_rel_path = os.path.relpath(candidate_real, start=vrt_dir)

        if source_fn.text != true_rel_path or source_fn.get("relativeToVRT") != "1":
            source_fn.text = true_rel_path
            source_fn.set("relativeToVRT", "1")
            modified = True

    if modified:
        tree.write(vrt_file_path, encoding="utf-8", xml_declaration=False)
        logging.info("VRT XML successfully updated with validated relative paths.")


def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"vrt_build-{file_dt_string}.log"
    log_file_path = os.path.join(output_folder_path, log_file_name)

    logger = logging.getLogger()
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a VRT and optional DEM_Domain GeoParquet from source datasets."
    )

    parser.add_argument(
        "-s",
        "--src_directories",
        nargs="+",
        type=valid_dir,
        help="Space-separated list of directories where the .tif files exist.",
        required=True,
    )
    parser.add_argument("-n", "--vrt_basename", help="The base name for the VRT file", required=True)
    parser.add_argument(
        "-o",
        "--vrt_output_dir",
        type=str,
        help="The output directory where the VRT will be saved.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-x",
        "--vrt_suffix",
        type=str,
        help='Optional suffix for the VRT (example: "Alaska_3338_20260708")',
        required=False,
        default=None,
    )

    args = vars(parser.parse_args())
    create_vrt_file(**args)
