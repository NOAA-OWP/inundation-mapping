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
from xrspatial.zonal import stats, crosstab
#from memory_profiler import profile

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
chunk_size = 256*4

# metrics dict
metrics_dict = { 'csi': csi, 'tpr': tpr, 'far': far, 'mcc': mcc } 

# convert to primary metrics
def compute_primary_metrics(arr):
    
    agreement_encoding_digits_to_names = { 0: "TN",
                                           1: "FN",
                                           2: "FP",
                                           3: "TP",
                                           4: "Water body"
                                          }
    
    unique, counts = np.unique(arr['agreement'].values,
                               return_counts=True)

    # change unique to string then convert to dict
    unique = zip( ((agreement_encoding_digits_to_names[u] , c) for u,c in zip(unique,counts)) )

    # make dict
    #primary_metrics = dict(zip(unique,counts))
    
    # pop water body
    primary_metrics.pop("Water body")

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
                                            ).sel(band=1,drop=True) 

        # avoid recomputing nwm_catchments_xr for same magnitude
        if (h != prev_h) & (r != prev_r):
            
            # making xarray from catchment vectors
            print(f"Rasterizing NWM catchments for {h} at {r}m ...")
            nwm_catchments_xr = make_geocube(nwm_catchments,['ID'],
                                             like=agreement_raster,
                                             fill=np.nan) \
                                                .to_array(dim='band', name='nwm_catchments') \
                                                 .sel(band='ID') \
                                                 .drop('band') \
                                                 .chunk(agreement_raster.chunksizes)
        
        # assign previous h and r
        prev_h, prev_r = h, r

        # compute cross tabulation table for ct_dask_df
        ct_dask_df = crosstab(nwm_catchments_xr,agreement_raster,nodata_values=np.nan)
        #ct_pd_df = ct_dask_df.compute()

        ## NEXT: CONVERT CT INDICES TO STR OF PRIMARY METRICS AND COMPUTE SECONDARY METRICS PER ZONE WITH APPLY FUNCTIONALITY
        breakpoint()


if __name__ == '__main__':
    
    # dask client
    #cluster = LocalCluster()
    #client = Client(cluster)

    compute_metrics_by_catchment(nwm_catchments_fn,resolutions,years,hucs, chunk_size)
