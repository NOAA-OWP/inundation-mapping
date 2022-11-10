#!/usr/bin/env python3

import os
from itertools import product
from time import time
import warnings

import numpy as np
import geopandas as gpd
import pandas as pd
import rioxarray as rxr
import xarray as xr
from geocube.api.core import make_geocube
from tqdm import tqdm
from xrspatial.zonal import stats, crosstab
from dask.dataframe.multi import concat
from tqdm.dask import TqdmCallback
#from memory_profiler import profile

from foss_fim.tools.tools_shared_functions import csi, tpr, far, mcc

"""
- Primary and Secondary metrics can be object inhereting from pd/dask df
- Functions and variables here can be methods and attributes, respectively, of those objects
"""


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
#hucs = ['12020001','12020002','12020003','12020004','12020005','12020006','12020007']
hucs = ['12020001']
chunk_size = 256*4

# metrics dict
metrics_dict = { 'csi': csi, 'tpr': tpr, 'far': far, 'mcc': mcc } 


# changing numbers to str encodings
agreement_encoding_digits_to_names = { 0: "TN", 1: "FN",
                                       2: "FP", 3: "TP",
                                       4: "Waterbody"
                                     }

### getting this warning when receiving catchments with no samples ###
# /foss_fim/tools/tools_shared_functions.py:1508: RuntimeWarning: invalid value encountered in long_scalars
# maybe do some sort of check within metrics to verify prior to computing. Retrun NaN or None otherwise?

# meta for dask df apply
#meta = ( 'CSI' , 'f8' )
#meta = [('CSI' , 'f8'),('TPR' , 'f8'),('FAR' , 'f8'),('MCC' , 'f8')]

# metrics
def make_input_dict(row):
    """ make dictionary of primary metrics from row object when using pd/dask df apply """
    return {'TP' : row.TP, 'TN' : row.TN, 'FP' : row.FP, 'FN' : row.FN}


def compute_secondary_metrics_on_df(func):
    """ decorator function to compute secondary metrics when using pd/dask df apply """
    
    def wrapper(row):
        
        # makes input dict from row object when using pd/dask df apply
        input_dict = make_input_dict(row)
        
        # ignore warnings should be it's own decorator??? 
        with warnings.catch_warnings():
            warnings.simplefilter("ignore",category='RuntimeWarning')
            metric = func(**input_dict)
        
        return metric
    
    return wrapper

def all_metrics(TP,FP,FN,TN=None):
    
    return { 'CSI' : csi(TP,FP,FN,TN),
             'TPR' : tpr(TP,FP,FN,TN),
             'FAR' : far(TP,FP,FN,TN),
             'MCC' : mcc(TP,FP,FN,TN) }

# wrap functions to work with rows
calc_all_metrics = compute_secondary_metrics_on_df(all_metrics)


def compute_metrics_by_catchment( nwm_catchments_fn, 
                                  resolutions, years, hucs, chunk_size ):
    
    # load catchments
    nwm_catchments = gpd.read_file(nwm_catchments_fn)

    # prepare combinations of resolutions, hucs, and years
    combos = list(product(resolutions,hucs,years))
    num_of_combos = len(combos)
    
    # prepare outputs
    list_of_secondary_metrics_df = [None] * num_of_combos
    
    print('Agreement By Catchment')
    # loop over every combination of resolutions and magnitude years
    for idx,(r,h,y) in tqdm(enumerate(combos),desc='Rasterizing Catchments & Crosstabbing',total=num_of_combos):
        
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

        # making xarray from catchment vectors
        nwm_catchments_xr = make_geocube(nwm_catchments,['ID'],
                                         like=agreement_raster,
                                         fill=np.nan) \
                                            .to_array(dim='band', name='nwm_catchments') \
                                             .sel(band='ID') \
                                             .drop('band') \
                                             .chunk(agreement_raster.chunksizes)
    
        # compute cross tabulation table for ct_dask_df
        ct_dask_df = crosstab(nwm_catchments_xr,agreement_raster,nodata_values=np.nan) \
                                    .rename(columns=agreement_encoding_digits_to_names) \
                                    .astype(np.int64) \
                                    .set_index('zone', drop=True, npartitions='auto') # set index on zone

        #### calculate metrics ###
        # provides framework for computed secondary metrics df
        meta = pd.DataFrame(columns=('CSI' ,'TPR', 'FAR', 'MCC'), dtype='f8')
        
        # applies function to calc secondary metrics across rows. Uses expand to accomodate multiple columns
        secondary_metrics_df = ct_dask_df.apply(calc_all_metrics,axis=1, meta=meta, result_type='expand') \
                                         .dropna(how='all')
        
        # add categoricals: resolution, year, huc

        # aggregate to list
        list_of_secondary_metrics_df[idx] = secondary_metrics_df  
        
    # concat list of secondary metrics df
    secondary_metrics_dask_df = concat(list_of_secondary_metrics_df)
    
    # compute
    with TqdmCallback(desc='Computing Metrics'):
        secondary_metrics_pd_df = secondary_metrics_dask_df.compute()

if __name__ == '__main__':
    
    # computes 4 secondary contingency metrics by nwm catchment for year, huc, and resolution
    compute_metrics_by_catchment(nwm_catchments_fn,resolutions,years,hucs, chunk_size)
