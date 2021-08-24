#!/usr/bin/env python3

import os
from osgeo import gdal
import sys
sys.path.append('/foss_fim/src')
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
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
    parser.add_argument('-keep','--keep-intermediate', help='keep intermediate files', required=False,default=True,action='store_true')
    parser.add_argument('-op','--overwrite-projected', help='Overwrite projected files', required=False,default=False,action='store_true')
    parser.add_argument('-om','--overwrite-meters', help='Overwrite meter files', required=False,default=False,action='store_true')

    args = vars(parser.parse_args())

    dem_dir            = args['dem_dir']
    number_of_jobs     = args['number_of_jobs']
    nodata_val         = args['nodata_val']
    blocksize          = args['blocksize']
    keep_intermediate  = args['keep_intermediate']
    overwrite_projected  = args['overwrite_projected']
    overwrite_meters  = args['overwrite_meters']

    reproject_procs_list = []

    for huc in os.listdir(dem_dir):
        raster_dir = os.path.join(dem_dir,huc)
        elev_cm = os.path.join(raster_dir, 'elev_cm.tif')
        elev_cm_proj = os.path.join(raster_dir, 'elev_cm_proj.tif')
        reproject_procs_list.append([raster_dir, elev_cm, elev_cm_proj, PREP_PROJECTION_CM,overwrite_projected])

    # Multiprocess reprojection
    with Pool(processes=number_of_jobs) as pool:
        pool.map(reproject_dem, reproject_procs_list)

    profile_procs_list = []
    
    for huc in os.listdir(dem_dir):
        elev_m_tif = os.path.join(dem_dir,huc, 'elev_m.tif')
        raster_dir = os.path.join(dem_dir,huc)
        elev_cm_proj = os.path.join(raster_dir, 'elev_cm_proj.tif')
        elev_m = os.path.join(raster_dir, 'elev_m.tif')
        profile_procs_list.append((huc,[elev_cm_proj, elev_m,PREP_PROJECTION,nodata_val,blocksize,keep_intermediate,overwrite_meters]))

    executor = ThreadPoolExecutor(max_workers=number_of_jobs)
    
    executor_generator = { 
                          executor.submit(update_raster_profile,p):h for h,p in profile_procs_list
                         }
    
    for future in tqdm(as_completed(executor_generator),
                       total=len(profile_procs_list),
                       desc="Processing rasters with {} workers ".format(number_of_jobs)
                      ):
        hucCode = executor_generator[future]
        #try:
        future.result()
        #except Exception as exc:
        #    print('{},{},{}'.format(hucCode,exc.__class__.__name__,exc))
    
    # power down pool
    executor.shutdown(wait=True)
    
    # Multiprocess update profile
 #   with Pool(processes=2) as pool:
        # TODO read in windows becasue gdal rasters are massive
#        pool.map(update_raster_profile, profile_procs_list)
