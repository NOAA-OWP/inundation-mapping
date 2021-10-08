#!/usr/bin/env python3

import os
import sys

from osgeo import gdal

sys.path.append("/foss_fim/src")
import argparse
import shutil
from multiprocessing import Pool

from utils.shared_variables import PREP_PROJECTION_CM


def reproject_dem(args):

    raster_dir = args[0]
    elev_cm = args[1]
    elev_cm_proj = args[2]
    reprojection = args[3]

    if os.path.exists(elev_cm_proj):
        os.remove(elev_cm_proj)

    shutil.copy(elev_cm, elev_cm_proj)

    print(f"Reprojecting {elev_cm_proj}")
    gdal.Warp(elev_cm_proj, elev_cm_proj, dstSRS=reprojection)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Burn in NLD elevations")
    parser.add_argument(
        "-dem_dir", "--dem-dir", help="DEM filename", required=True, type=str
    )
    parser.add_argument(
        "-j",
        "--number-of-jobs",
        help="Number of processes to use. Default is 1.",
        required=False,
        default="1",
        type=int,
    )

    args = vars(parser.parse_args())

    dem_dir = args["dem_dir"]
    number_of_jobs = args["number_of_jobs"]

    reproject_procs_list = []

    for huc in os.listdir(dem_dir):
        raster_dir = os.path.join(dem_dir, huc)
        elev_cm = os.path.join(raster_dir, "elev_cm.tif")
        elev_cm_proj = os.path.join(raster_dir, "elev_cm_proj.tif")
        reproject_procs_list.append(
            [raster_dir, elev_cm, elev_cm_proj, PREP_PROJECTION_CM]
        )

    # Multiprocess reprojection
    with Pool(processes=number_of_jobs) as pool:
        pool.map(reproject_dem, reproject_procs_list)
