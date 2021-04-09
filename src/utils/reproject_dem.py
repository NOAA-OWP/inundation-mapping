#!/usr/bin/env python3

import os
from osgeo import gdal
import sys
sys.path.append('/foss_fim/src')
from utils.shared_variables import PREP_PROJECTION, PREP_PROJECTION_CM
from os.path import splitext
import shutil
from multiprocessing import Pool


def reproject_dem(args):

    raster_dir            = args[0]
    reprojection          = args[1]

    # raster_list = ['2002','2003','2004','2005','2006','2007','2008','2101','2102','2201','2202','2203','0430']
    elev_cm = os.path.join(raster_dir, 'elev_cm.tif')
    elev_m = os.path.join(raster_dir, 'elev_m.tif')
    elev_cm_proj = os.path.join(raster_dir, 'elev_cm_proj.tif')

    if os.path.exists(elev_cm_proj):
        os.remove(elev_cm_proj)

    if os.path.exists(elev_m):
        os.remove(elev_m)

    shutil.copy(elev_cm, elev_cm_proj)

    print(f"Reprojecting {elev_cm_proj}")
    gdal.Warp(elev_cm_proj,elev_cm_proj,dstSRS=reprojection)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Burn in NLD elevations')
    parser.add_argument('-dem_dir','--dem-dir', help='DEM filename', required=True,type=str)

    args = vars(parser.parse_args())

    dem_dir = args['dem_dir']

    # dem_dir = '/data/inputs/nhdplus_rasters'

    number_of_jobs = 5
    procs_list = []

    for huc in os.listdir(dem_dir):
        huc_rast_path = os.path.join(dem_dir,huc)
        procs_list.append([huc_rast_path, PREP_PROJECTION_CM])

    # Multiprocess with instructions
    pool = Pool(number_of_jobs)
    pool.map(reproject_dem, procs_list)
