#!/usr/bin/env python3

import sys
sys.path.append('/foss_fim/src')
import rasterio
import numpy as np
from utils.shared_variables import PREP_PROJECTION,VIZ_PROJECTION,PREP_PROJECTION_CM
import argparse



with rasterio.open(input_raster_name) as src:
    # check projection
    if src.crs.to_string() != reprojection:
        if src.crs.to_string().startswith('EPSG'):
            epsg = src.crs.to_epsg()
            proj_crs = CRS.from_epsg(epsg)
            rio_crs = rasterio.crs.CRS.from_user_input(proj_crs).to_string()
        else:
            rio_crs = src.crs.to_string()
    if rio_crs != reprojection:
        print(f"{input_raster_name} not projected")
        # print(f"Reprojecting from {rio_crs} to {reprojection}")

    # dem_dir = '/data/inputs/nhdplus_rasters'

raster_dir = '/data/inputs/nhdplus_rasters'
m_proj_count = 0
for huc in os.listdir(raster_dir):
    # elev_m_tif = os.path.join(raster_dir,huc, 'elev_m.tif')
    # elev_cm_OG = os.path.join(raster_dir,huc, 'elev_cm_orig.tif')
    elev_cm_proj_tif = os.path.join(raster_dir,huc, 'elev_cm_proj.tif')
    elev_m_tif = os.path.join(raster_dir,huc, 'elev_m.tif')
    if os.path.exists(elev_m_tif):
        os.remove(elev_cm_proj_tif)
    if not os.path.exists(elev_m_tif):
        # print(f"missubg huc {elev_cm_proj_tif}")
        m_proj_count = m_proj_count + 1



################################################################################
    # Windowed reading/calculating/writing
    with rasterio.open(elev_cm_filename) as dem_cm:
        no_data = dem_cm.nodata
        for block_index, window in dem_cm.block_windows(1):
            block_data = dem_cm.read(window=window)
            dem_m = np.where(block_data == int(no_data), nodata_val, (block_data/100).astype(rasterio.float32))

        dem_m_profile = dem_cm.profile.copy()

        dem_m_profile.update(driver='GTiff',tiled=True,nodata=nodata_val,
                             blockxsize=blocksize, blockysize=blocksize,
                             dtype='float32',crs=projection,compress='lzw',interleave='band')
    write_window = Window.from_slices((30, 269), (50, 313))
    # write_window.height = 239, write_window.width = 263

    with rasterio.open(
            elev_m_filename, 'w',
            driver='GTiff', width=500, height=300, count=3,
            dtype=r.dtype) as dst:
        for k, arr in [(1, b), (2, g), (3, r)]:
            dst.write(arr, indexes=k, window=write_window)
################################################################################






raster_dir = '/data/inputs/nhdplus_rasters'
cm_proj_count = 0
m_proj_count = 0
other_proj_hucs = []
for huc in os.listdir(raster_dir):
    # elev_cm_tif = os.path.join(raster_dir,huc, 'elev_cm.tif')
    # elev_cm_OG = os.path.join(raster_dir,huc, 'elev_cm_orig.tif')
    # elev_cm_proj_tif = os.path.join(raster_dir,huc, 'elev_cm_proj.tif')
    elev_m_tif = os.path.join(raster_dir,huc, 'elev_m.tif')
    src =  rasterio.open(elev_cm_tif)
    # check projection
    if src.crs.to_string() == PREP_PROJECTION_CM:
        cm_proj_count = cm_proj_count + 1
    elif src.crs.to_string() == PREP_PROJECTION:
        m_proj_count = m_proj_count + 1
    else:
        other_proj_hucs = other_proj_hucs + [huc]
    tot_proj_count = cm_proj_count + m_proj_count
            if src.crs.to_string().startswith('EPSG'):
                epsg = src.crs.to_epsg()
                proj_crs = CRS.from_epsg(epsg)
                rio_crs = rasterio.crs.CRS.from_user_input(proj_crs).to_string()
            else:
                rio_crs = src.crs.to_string()
            if rio_crs != PREP_PROJECTION:
                print(f"{elev_cm_tif} not projected")
                # print(f"{rio_crs}")




    if not os.path.exists(elev_m_tif):
        print(f"missubg huc {elev_m_tif}")
    if os.path.exists(elev_cm_OG):
        reproject_raster(elev_cm_OG,PREP_PROJECTION_CM,512,elev_cm_proj_tif)
    if os.path.exists(elev_cm_proj_tif):
        print(f"reprojected huc {huc}")
    # update_raster_profile(elev_cm_tif,elev_m_tif)


def update_raster_profile(elev_cm_filename,elev_m_filename):

    # Update nodata value and convert from cm to meters
    dem_cm = rasterio.open(elev_cm_filename)
    no_data = dem_cm.nodata
    data = dem_cm.read(1)
    dem_m = np.where(dem_cm == int(no_data), -9999.0, (dem_cm/100).astype(rasterio.float32))

    dem_m_profile = dem_cm.profile.copy()
    dem_m_profile.update(driver='GTiff',tiled=True,nodata=-9999.0,dtype='float32',compress='lzw',interleave='band')

    with rasterio.open(elev_m_filename, "w", **dem_m_profile, BIGTIFF='YES') as dest:
        dest.write(dem_m, indexes = 1)

    dem_cm.close()




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Update nodata value')
    parser.add_argument('-in_dem','--in-dem-filename', help='DEM filename', required=True,type=str)
    parser.add_argument('-out_dem','--out-dem-filename', help='out DEM filename', required=True,type=str)

    args = vars(parser.parse_args())

    in_dem_filename = args['in_dem_filename']
    out_dem_filename = args['out_dem_filename']

    update_raster_profile(in_dem_filename,out_dem_filename)
