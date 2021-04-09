#!/usr/bin/env python3

import os
from osgeo import gdal
import sys
sys.path.append('/foss_fim/src')
from multiprocessing import Pool
import argparse
from utils.reproject_dem import reproject_dem
from utils.shared_functions import update_raster_profile
from utils.shared_variables import PREP_PROJECTION, PREP_PROJECTION_CM


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Reproject Elevation rasters and update profile')
    parser.add_argument('-dem_dir','--dem-dir', help='DEM filename', required=True,type=str)
    parser.add_argument('-j','--number-of-jobs',help='Number of processes to use. Default is 1.',required=False, default="1",type=int)
    parser.add_argument('-nodata','--nodata-val', help='DEM nodata value', required=False,type=float,default=-9999.0)
    parser.add_argument('-block','--blocksize', help='DEM blocksize', required=False,type=int,default=512)
    parser.add_argument('-keep','--keep-intermediate', help='keep intermediate files', required=False,type=bool,default=True)

    args = vars(parser.parse_args())

    dem_dir            = args['dem_dir']
    number_of_jobs     = args['number_of_jobs']
    nodata_val         = args['nodata_val']
    blocksize          = args['blocksize']
    keep_intermediate  = args['keep_intermediate']

    reproject_procs_list = []

    for huc in os.listdir(dem_dir):
        raster_dir = os.path.join(dem_dir,huc)
        elev_cm = os.path.join(raster_dir, 'elev_cm.tif')
        elev_cm_proj = os.path.join(raster_dir, 'elev_cm_proj.tif')
        reproject_procs_list.append([raster_dir, elev_cm, elev_cm_proj, PREP_PROJECTION_CM])

    # Multiprocess reprojection
    pool = Pool(number_of_jobs)
    pool.map(reproject_dem, reproject_procs_list)

    profile_procs_list = []

    for huc in os.listdir(dem_dir):
        elev_m_tif = os.path.join(dem_dir,huc, 'elev_m.tif')
        if not os.path.exists(elev_m_tif):
            raster_dir = os.path.join(dem_dir,huc)
            elev_cm_proj = os.path.join(raster_dir, 'elev_cm_proj.tif')
            elev_m = os.path.join(raster_dir, 'elev_m.tif')
            profile_procs_list.append([elev_cm_proj, elev_m,PREP_PROJECTION,nodata_val,blocksize,keep_intermediate])

    # Multiprocess update profile
    pool = Pool(2) #number_of_jobs (max jobs = 2 on the VM)
    # TODO read in windows becasue gdal rasters are massive
    pool.map(update_raster_profile, profile_procs_list)
