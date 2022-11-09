#!/usr/bin/env python3

import os
from itertools import product
from time import time

import numpy as np
import geopandas as gpd
import rioxarray as rxr
import xarray as xr
from geocube.api.core import make_geocube
from tqdm import tqdm
from memory_profiler import profile
from xrspatial.zonal import stats
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster

from foss_fim.tools.tools_shared_functions import csi, tpr, far, mcc

# variable declarations
root_dir = os.path.abspath(os.sep)
data_dir = os.path.join(root_dir,'data','misc','lidar_manuscript_data')
nwm_catchments_fn = os.path.join(data_dir,'nwm_catchments_1202.gpkg')

# agreement raster function
def build_agreement_raster_data_dir(huc, resolution, year):
    return os.path.join(root_dir,'data','test_cases','ble_test_cases',
                        f'{huc}_ble','testing_versions',
                        f'3dep_test_1202_{resolution}m_GMS_n_12',
                        f'{year}yr','total_area_agreement.tif')

# agreement factors
#resolutions = [3,5,10,15,20]
resolutions = [20]
years = [100,500]
hucs = ['12020001','12020002','12020003','12020004','12020005','12020006','12020007']
chunk_size = 10240

# metrics dict
metrics_dict = { 'csi': csi, 'tpr': tpr, 'far': far, 'mcc': mcc } 

# convert to primary metrics
def compute_primary_metrics(arr):
    
    agreement_encoding_digits_to_names = { 0: "TN",
                                           1: "FN",
                                           2: "FP",
                                           3: "TP"
                                          }
    
    unique, counts = np.unique(arr,return_counts=True)

    # change unique to string
    unique = [agreement_rasters_string_template[u] for u in unique]

    primary_metrics = dict(zip(unique,counts))

    return primary_metrics


def compute_metrics_by_catchment( nwm_catchments_fn, 
                                  resolutions, years, hucs, chunk_size ):
    
    # load catchments
    nwm_catchments = gpd.read_file(nwm_catchments_fn)

    # loop over every combination of resolutions and magnitude years
    prev_h, prev_r = [None] * 2
    combos = list(product(resolutions,hucs,years))
    for r,h,y in tqdm(combos,desc='Agreement By Catchment'):
        
        # load agreement raster
        agreement_raster_fn = build_agreement_raster_data_dir(h,r,y)
        agreement_raster = rxr.open_rasterio(
                                             agreement_raster_fn,
                                             #chunks=True,
                                             chunks=chunk_size,
                                             mask_and_scale=True,
                                             variable='agreement',
                                             default_name='agreement',
                                             lock=False
                                            ).sel(band=1,drop=True) \
                                             .astype(np.uint16)

        # avoid recomputing nwm_catchments_xr for same magnitude
        if (h != prev_h) & (r != prev_r):
            # making xarray from catchment vectors
            print(f"Making geocube for {h} at {r}m ...")
            nwm_catchments_xr = make_geocube(nwm_catchments,['ID'],like=agreement_raster) \
                                             .to_array(dim='band', name='nwm_catchments') \
                                             .sel(band='ID') \
                                             .drop('band') \
                                             .astype(np.uint16) \
                                             .chunk(agreement_raster.chunksizes)
            
            # this is for an alternative zonal method. grouping consumes too much RAM according to experiments.
            # merge to dataset
            catchments_agreement_merged = xr.merge([nwm_catchments_xr,agreement_raster])
        
        # if nwm catchments are the same from last iteration in loop (only change in yr from 100 to 500)
        else:
            
            catchments_agreement_merged['agreement'] = agreement_raster

        breakpoint()
        # assign previous h and r
        prev_h, prev_r = h, r
        
        #"""

        # remove old datasets
        del nwm_catchments_xr, agreement_raster
        
        # grouping by catchment
        start = time()
        grouped_catchments_agreement_merged = catchments_agreement_merged \
                                                   .drop("spatial_ref") \
                                                   .groupby(catchments_agreement_merged.nwm_catchments)
        grouping = time() - start
        print(f'Grouping: {grouping}')

        # zonal stats
        breakpoint()
        start = time()
        grid_mean_built_in = grouped_catchments_agreement_merged.mean().rename({"agreement": "agreement_mean"}) 
        built_in_mean = time() - start
        print(f'Grouping: {grouping} | Built In Method: {built_in_mean}')
        
        """
        start = time()
        grouped_catchments_agreement_merged_df = catchments_agreement_merged \
                                                      .drop('spatial_ref') \
                                                      .to_dataframe().groupby('nwm_catchments')
        pure_df_grouping = time() - start
        print(f'Grouping: {grouping} | Built In Method: {built_in_mean} | Numpy: {numpy_mean} | Pure DF Grouping: {pure_df_grouping}')

        start = time()
        grid_mean_pure_df = grouped_catchments_agreement_merged_df.mean() 
        pure_df_mean = time() - start
        print(f'Grouping: {grouping} | Built In Method: {built_in_mean} | Numpy: {numpy_mean} | Pure DF Grouping: {pure_df_grouping} | Pure DF: {pure_df_mean}')
        breakpoint()
        """
        
        """
        # zonal stats
        breakpoint()
        #try:
        stats_by_catchment = stats(nwm_catchments_xr, agreement_raster,
                               #stats_funcs={'primary_metrics': compute_primary_metrics}
                               #stats_funcs={'mean':np.mean,'median':np.median,'max':np.max,'min':np.min,'std':np.std}
                               stats_funcs=['mean','max','min','std'],
                               return_type='xarray.DataArray'
                              )
        #except:
        #    pass
        breakpoint()
        """
        
        
        
if __name__ == '__main__':
    
    # dask client
    #cluster = LocalCluster()
    #client = Client(cluster)

    compute_metrics_by_catchment(nwm_catchments_fn,resolutions,years,hucs, chunk_size)
