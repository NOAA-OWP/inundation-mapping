#!/usr/bin/env python3

import rasterio 
import numpy as np
import argparse
import os
from glob import glob
import geopandas as gpd
import pandas as pd
from foss_fim.src.utils.shared_variables import PREP_PROJECTION
import csv
from pygeohydro import WBD
from tqdm.dask import TqdmCallback
from utils.shared_variables import DEM_3DEP_RASTER_DIR_NAME, PREP_PROJECTION
from inundation import __append_huc_code_to_file_name
import dask


def Compare_dems(hucs, desired_crs=PREP_PROJECTION, output_raster_file=None, output_stats_file=None):

    # Parse HUCs from hucs.
    if isinstance(hucs,list):
        if len(hucs) == 1:
            try:
                with open(hucs[0]) as csv_file:  # Does not have to be CSV format.
                    hucs = [i[0] for i in csv.reader(csv_file)]
            except FileNotFoundError:
                hucs = hucs
        else:
                hucs = hucs
    elif isinstance(hucs,str):
        try:
            with open(hucs) as csv_file:  # Does not have to be CSV format.
                hucs = [i[0] for i in csv.reader(csv_file)]
        except FileNotFoundError:
            hucs = list(hucs)
        
    huc_length = [ len(h) for h in hucs ]
    huc_length = set(huc_length)

    if len(huc_length) > 1:
        raise ValueError("Pass equivalent length HUCs")

    huc_length = list(huc_length)[0]

    wbd = WBD(f'huc{huc_length}')
    wbd_df = wbd.byids(f'huc{huc_length}',hucs)
    wbd_df = wbd_df.to_crs(PREP_PROJECTION)

    dask.compute(__process_comparison(wbd_df,huc_length,output_raster_file,output_stats_file))


def __process_comparison(wbd_df,huc_length,output_raster_file=None,output_stats_file=None):

    for _,e in wbd_df.iterrows():
    
        huc = e[f'huc{huc_length}']
        huc_geom = e['geometry']
        
        # get NHD dems
        nhd_dem = __get_nhd_dem(huc,huc_geom)
        #nhd_dem_arr, nhd_dem_trans, nhd_dem_profile = nhd_dem[0],nhd_dem[1],nhd_dem[2]

        # get 3DEP dems
        dem_3dep = __get_3dep_dem(huc, huc_geom)

        # reproject
        dem_3dep = __reproject(dem_3dep,nhd_dem)

        # get data mask
        get_data_mask = lambda a,b : np.logical_and(a[0] != a[1]['nodata'],b[0] != b[1]['nodata'])
        data_mask = dask.delayed(get_data_mask)(dem_3dep,nhd_dem)
        
        # subtract
        diff_arr = __make_diff_array(dem_3dep,nhd_dem,data_mask)

        # make map
        if output_raster_file is not None:
            __save_map(output_raster_file, diff_arr, nhd_dem[1], huc)

        # stats: mean, median, std, 5-95 percentiles
        diff_arr_data = diff_arr[data_mask]
        
        stats = pd.Series({ 
                            'mean' : diff_arr_data.mean().compute(),
                            'median' : diff_arr_data.median().compute(),
                            'std' : diff_arr_data.std().compute(),
                            'min' : diff_arr_data.min().compute(),
                            'max' : diff_arr_data.max().compute(),
                            'fifth_percentile' : np.percentile(diff_arr_data,5).compute(),
                            'ninety_fifth_percentile' : np.percentile(diff_arr_data,95).compute() 
                           } ,name=huc
                          )
        
        if output_stats_file is not None:
            osf = __append_huc_code_to_file_name(output_stats_file,huc)
            stats.to_csv(osf)

@dask.delayed
def __make_diff_array(dem_3dep,nhd_dem,data_mask):
    
    diff_arr = np.full( nhd_dem[0].shape, nhd_dem[1]['nodata'])
    diff_arr[data_mask] = dem_3dep[0][data_mask] - nhd_dem[0][data_mask]
    
    return(diff_arr)


@dask.delayed
def __reproject(src,dst):
    
    proj = rasterio.warp.reproject( src[0],
                                    destination=dst[0].copy(),
                                    src_transform=src[1]['transform'],
                                    src_crs=src[1]['crs'],
                                    dst_transform=dst[1]['transform'],
                                    dst_crs=dst[1]['crs'],
                                    resampling=rasterio.warp.Resampling.nearest,
                                    src_nodata=src[1]['nodata'],
                                    dest_nodata=dst[1]['nodata']
                                   )
    
    # update
    prof = dst[1].copy()
    prof.update({'transform': proj[1]})
    
    return(proj[0], prof)


@dask.delayed
def __save_map(output_raster_file, diff_arr, desired_profile):
     
    orf = __append_huc_code_to_file_name(output_raster_file,huc)

    with rasterio.open( orf,'w+', **desired_profile['driver']) as dst:
        dst.write(diff_arr,1)


@dask.delayed
def __get_nhd_dem(huc,huc_geom):

    huc = str(huc)

    huc4 = huc[:4]

    huc4_fp = os.path.join(os.environ['nhdplus_rasters_dir'],'HRNHDPlusRasters'+huc4,'elev_m.tif')

    huc4_dem_ds = rasterio.open(huc4_fp,mode='r')

    arr, trans = rasterio.mask.mask(huc4_dem_ds, [huc_geom], all_touched=True, crop=True, indexes=1)
    
    huc4_dem_ds.profile.update({'transform' : trans})

    return(arr, huc4_dem_ds.profile)


@dask.delayed
def __get_3dep_dem(huc, huc_geom):
    
    huc = str(huc)

    huc4 = huc[:4]

    huc_res = huc4 + '_10m'

    huc4_fp = os.path.join(os.environ['input_dir'], DEM_3DEP_RASTER_DIR_NAME, 'dem_3dep_'+huc_res+'.vrt')

    huc4_dem_ds = rasterio.open(huc4_fp,mode='r')

    arr, trans = rasterio.mask.mask(huc4_dem_ds, [huc_geom], all_touched=True, crop=True, indexes=1)

    huc4_dem_ds.profile.update({'transform' : trans})

    return(arr,  huc4_dem_ds.profile)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Compare DEMs')
    parser.add_argument('-u','--hucs',help='HUCs consistent size',required=True,nargs='+')
    parser.add_argument('-c','--desired-crs',help='Desired CRS',required=False,default=PREP_PROJECTION)
    parser.add_argument('-o','--output-raster-file',help='Output raster file name',required=False,default=None)
    parser.add_argument('-s','--output-stats-file',help='Output stats file name',required=False,default=None)

    args = vars(parser.parse_args())

    Compare_dems(**args)
