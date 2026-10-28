#!/usr/bin/env python3

import argparse
import glob
import logging
import os
from pathlib import Path

import src.utils.shared_functions as sf
from data.dem.create_vrt_file import create_vrt_file
from src.utils.polygonize_raster import polygonize_extent
from src.utils.shared_functions import FIM_Helpers as fh


def valid_dir(path_str):
    """Validates that a string path exists and is a directory."""
    path = Path(path_str)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"The path '{path_str}' does not exist.")
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"The path '{path_str}' is a file, not a directory.")
    return str(path)


def create_domain_vrt(input_dirs, suffix_str, output_dir=None, number_of_jobs=1, run_polygonize=False):
    # Fallback to the first input directory if unspecified
    if not output_dir:
        output_dir = input_dirs[0]

    os.makedirs(output_dir, exist_ok=True)

    # 1. Collect all individual GeoTIFF files across all input directories
    dem_files = []
    for d in input_dirs:
        dem_files.extend(glob.glob(os.path.join(d, "*.tif")))
        dem_files.extend(glob.glob(os.path.join(d, "**", "*.tif"), recursive=True))

    dem_files = sorted(list(set(dem_files)))
    if not dem_files:
        raise FileNotFoundError(f"No .tif files found in specified directories: {input_dirs}")

    # 2. Build domain parquet output file path
    domain_file_name = f"DEM_Domain_{suffix_str}.parquet"
    vrt_file_name = f"hand_seamless_dems_10m_{suffix_str}.vrt"
    dem_domain_output_path = os.path.join(output_dir, domain_file_name)

    # 3. Generate domain polygon from DEM file list
    if run_polygonize:
        file_logger = logging.getLogger("fim_logger")
        polygonize_extent(dem_files, dem_domain_output_path, number_of_jobs, file_logger, sf=sf, fh=fh)

    # 4. Generate VRT
    create_vrt_file(input_dirs, vrt_file_name, output_dir)


def main():
    """
    Creates a VRT of the DEM(s) in the input folder(s) and DEM_domain.parquet polygon of the spatial extent.

    Example usage:
    python3 /foss_fim/data/dem/create_domain_vrt.py \
        -i '/data/inputs/dems/3dep_dems/10m_SouthAlaska/20260619' \
           '/data/inputs/dems/ifsar_dtm/10m_NorthAlaska/20260708' \
        -o '/data/inputs/dems/vrt' \
        -s 'Alaska_3338_20261026' \
        -j 14
    """
    parser = argparse.ArgumentParser(
        description='Create a VRT and DEM_Domain GeoParquet from source folders.'
    )
    parser.add_argument(
        '-i',
        '--input_dirs',
        nargs='+',
        type=valid_dir,
        help='Space-separated list of directories where the .tif files exist.',
        required=True,
    )
    parser.add_argument('-s', '--suffix_str', type=str, required=True, help='Output file names.')
    parser.add_argument(
        '-o',
        '--output_dir',
        type=str,
        required=False,
        default=None,
        help='Folder to save output files. Will be saved in the first input folder if unspecified.',
    )
    parser.add_argument('-j', '--number_of_jobs', type=int, required=False, default=1)
    parser.add_argument(
        '-p',
        '--run_polygonize',
        dest='run_polygonize',
        action='store_true',
        help='If True, also generate the DEM_Domain GeoParquet file.',
        required=False,
    )

    args = parser.parse_args()

    create_domain_vrt(
        input_dirs=args.input_dirs,
        output_dir=args.output_dir,
        suffix_str=args.suffix_str,
        number_of_jobs=args.number_of_jobs,
        run_polygonize=args.run_polygonize,
    )


if __name__ == "__main__":
    main()
