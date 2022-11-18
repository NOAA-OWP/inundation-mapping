#!/usr/bin/env python3

import os
from itertools import product, combinations
from time import time
import warnings
from functools import partial

import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_image
from tqdm import tqdm
from xrspatial.zonal import stats, crosstab
from dask.dataframe.multi import concat
from tqdm.dask import TqdmCallback
import statsmodels.formula.api as smf
from statsmodels.formula.api import ols
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import SequentialFeatureSelector as SFS
from sklearn_pandas import DataFrameMapper
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder

from foss_fim.tools.tools_shared_functions import csi, tpr, far, mcc

"""
- Primary and Secondary metrics can be object inhereting from pd/dask df
- Functions and variables here can be methods and attributes, respectively, of those objects
"""


# variable declarations
root_dir = os.path.abspath(os.sep)
data_dir = os.path.join(root_dir,'data','misc','lidar_manuscript_data')
nwm_catchments_fn = os.path.join(data_dir,'nwm_catchments_1202.gpkg')
nwm_streams_fn = os.path.join(data_dir,'nwm_flows_1202.gpkg')
huc8s_vector_fn = os.path.join(data_dir,'huc8s_1202.gpkg')

# agreement raster function
def build_agreement_raster_data_dir(huc, resolution, year, source):
    
    if source == '3dep':

        return os.path.join(root_dir,'data','test_cases','ble_test_cases',
                            f'{huc}_ble','testing_versions',
                            f'3dep_test_1202_{resolution}m_GMS_n_12',
                            f'{year}yr','total_area_agreement.tif')
    elif source == 'nhd':
        
        return os.path.join(root_dir,'data','test_cases','ble_test_cases',
                            f'{huc}_ble','testing_versions',
                            '20210902_C892f8075_allBle_GMS_n_12',
                            f'{year}yr','total_area_agreement.tif')

# agreement factors
resolutions = [3,5,10,15,20]
#resolutions = [5,10,15,20]
#resolutions = [20]
years = [100,500]
hucs = ['12020001','12020002','12020003','12020004','12020005','12020006','12020007']
#hucs = ['12020001', '12020002']
chunk_size = 256*4
save_filename = os.path.join(data_dir,'experiment_data.h5')
hdf_key = 'data'
from_file = None
#from_file = save_filename
#feature_cols = ['huc8','spatial_resolution','magnitude','mainstem','order_','Lake','gages','Length']
feature_cols = ['spatial_resolution','magnitude','order_','Lake','Length','Slope']
# encoded_features = ['order_','Lake','magnitude']
#target_cols = ['CSI']
#target_cols = ['MCC']
#target_cols = ['TPR']
target_cols = ['FAR']
nhd_to_3dep_plot_fn = os.path.join(data_dir,'nhd_to_3dep_plot.png')

# pipeline switches 
add_two_way_interactions = True
run_anova = True
make_nhd_plot = True


# metrics dict
metrics_dict = { 'csi': csi, 'tpr': tpr, 'far': far, 'mcc': mcc } 

# changing numbers to str encodings
agreement_encoding_digits_to_names = { 0: "TN", 1: "FN",
                                       2: "FP", 3: "TP",
                                       4: "Waterbody"
                                     }

# metrics
def make_input_dict(row):
    """ make dictionary of primary metrics from row object when using pd/dask df apply """
    return {'TP' : row.TP, 'TN' : row.TN, 'FP' : row.FP, 'FN' : row.FN}


def compute_secondary_metrics_on_df(func):
    """ decorator function to compute secondary metrics when using pd/dask df apply """
    
    def wrapper(row):
        
        # makes input dict from row object when using pd/dask df apply
        input_dict = make_input_dict(row)
        
        metric = func(**input_dict)
        
        return metric
    
    return wrapper


def total_samples(TP,FP,FN,TN=None):
    if TN == None: TN = 0
    return np.nansum([TP,FP,FN,TN])
   
def frequency(TP,FP,FN,TN=None):
    return (TP + FP) / total_samples(TP,FP,FN,TN)

def cohens_kappa(TP,FP,FN,TN=None):
    return (2* (TP*TN - FN*FP)) / ( (TP+FP) * (FP+TN) + (TP+FN) * (FN+TN) )

def all_metrics(TP,FP,FN,TN=None):
    
    return { 'CSI' : csi(TP,FP,FN,TN),
             'TPR' : tpr(TP,FP,FN,TN),
             'FAR' : far(TP,FP,FN,TN),
             'MCC' : mcc(TP,FP,FN,TN),
             'Cohens Kappa' : cohens_kappa(TP,FP,FN,TN),
             'Total Samples' : total_samples(TP,FP,FN,TN),
             'Frequency' : frequency(TP,FP,FN,TN)
           }

# wrap functions to work with rows
calc_all_metrics = compute_secondary_metrics_on_df(all_metrics)

def compute_metrics_by_catchment( nwm_catchments_fn, 
                                  nwm_streams_fn,
                                  huc8s_vector_fn,
                                  resolutions, years, hucs,
                                  chunk_size,
                                  save_filename=None,
                                  hdf_key=None,
                                  write_debugging_files=False):
    
    # load catchments and streams
    nwm_streams = gpd.read_file(nwm_streams_fn)
    nwm_catchments = gpd.read_file(nwm_catchments_fn)
    huc8s_df = gpd.read_file(huc8s_vector_fn)

    # spatial join with columns from huc8s_df to get catchment assignments by huc8
    nwm_catchments = nwm_catchments \
                             .sjoin(huc8s_df.loc[:,['huc8','geometry']],how='inner',predicate='intersects') \
                             .drop('index_right',axis=1) \
                             .reset_index(drop=True)
    
    def __loop_experiments():
        
        # prepare combinations of resolutions, hucs, and years
        combos = list(product(hucs,resolutions,years))
        
        # append source
        combos = [(h,r,y,'3dep') for h,r,y in combos]
        nhd_combos = [(h,10,y,'nhd') for h,y in product(hucs,years)] 
        combos = nhd_combos + combos

        # prepare outputs
        num_of_combos = len(combos)
        list_of_secondary_metrics_df = [None] * num_of_combos
    
        print('Agreement By Catchment')
        # loop over every combination of resolutions and magnitude years
        for idx,(h,r,y,s) in tqdm(enumerate(combos),
                                desc='Rasterizing Catchments & Crosstabbing',
                                total=num_of_combos):
            
            # load agreement raster
            agreement_raster_fn = build_agreement_raster_data_dir(h,r,y,s)
            print(agreement_raster_fn);exit()
            agreement_raster = rxr.open_rasterio(
                                                 agreement_raster_fn,
                                                 #chunks=True,
                                                 chunks=chunk_size,
                                                 mask_and_scale=True,
                                                 variable='agreement',
                                                 default_name='agreement',
                                                 lock=False
                                                ).sel(band=1,drop=True) 

            # filter out for current huc
            nwm_catchments_current_huc8 = nwm_catchments.loc[nwm_catchments.loc[:,'huc8'] == h,:] \
                                                        .reset_index(drop=True)

            # making xarray from catchment vectors
            nwm_catchments_xr = make_geocube(nwm_catchments_current_huc8,['ID'],
                                             like=agreement_raster,
                                             fill=np.nan,
                                             rasterize_function=partial(rasterize_image,
                                                                        filter_nan=True,
                                                                        all_touched=True)) \
                                                 .to_array(dim='band', name='nwm_catchments') \
                                                 .sel(band='ID') \
                                                 .drop('band') \
                                                 .chunk(agreement_raster.chunksizes) \
                                                 .rio.set_nodata(np.nan) \
                                                 .rio.write_nodata(-9999,encoded=True)
            
            if write_debugging_files:
                # write rasterized nwm_catchments just for inspection
                nwm_catchments_xr.rio.to_raster(os.path.join(data_dir,f'nwm_catchments_{h}_{r}m_{y}yr.tif'),
                                                 tiled=True,windowed=True,lock=True,dtype=rasterio.int32,
                                                 compress='lzw')
                
                nwm_catchments_current_huc8.to_file(os.path.join(data_dir,f'nwm_catchments_{h}_{r}m_{y}yr.gpkg'),
                                                    driver='GPKG',index=False)
            
            # compute cross tabulation table for ct_dask_df
            ct_dask_df = crosstab(nwm_catchments_xr,agreement_raster,nodata_values=np.nan) \
                                        .rename(columns=agreement_encoding_digits_to_names) \
                                        .astype(np.float64) \
                                        .rename(columns={'zone':'ID'}) \
                                        .set_index('ID', drop=True, npartitions='auto') # set index on zone

            # remove files
            del agreement_raster, nwm_catchments_xr

            #### calculate metrics ###
            # provides framework for computed secondary metrics df
            meta = pd.DataFrame(columns=('CSI' ,'TPR', 'FAR', 'MCC', 'Cohens Kappa','Total Samples', 'Frequency'), dtype='f8')
            
            # applies function to calc secondary metrics across rows. Uses expand to accomodate multiple columns
            # drops rows that are all na
            # adds resolution, year, and huc. renames index to ID, and converts datatypes
            secondary_metrics_df = ct_dask_df.apply(calc_all_metrics,axis=1, meta=meta, result_type='expand') \
                                             .dropna(how='all') \
                                             .assign(huc8=lambda x : h) \
                                             .assign(spatial_resolution=lambda x : r) \
                                             .assign(magnitude=lambda x : y) \
                                             .assign(dem_source=lambda x : s) \
                                             .reset_index(drop=False) \
                                             .rename(columns={'index':'ID'}) \
                                             .astype({'ID' : np.int64,'huc8' : np.int64})
            
            # merge in primary metrics
            secondary_metrics_df = secondary_metrics_df.merge(ct_dask_df, left_on='ID', right_index=True)

            # aggregate to list
            list_of_secondary_metrics_df[idx] = secondary_metrics_df
        
        return list_of_secondary_metrics_df


    # build list of secondary metrics dfs
    secondary_metrics_df = __loop_experiments()

    # concat list of secondary metrics df
    secondary_metrics_df = concat(secondary_metrics_df)
    
    # compute to pandas
    with TqdmCallback(desc='Computing Metrics'):
        secondary_metrics_df = secondary_metrics_df.compute()

    # reset index
    secondary_metrics_df = secondary_metrics_df.reset_index(drop=False)
    
    # what about repeat ID's
    # should we group by all factors and sum to aggregate???
    # checking for duplicated IDs within resolution and magnitude
    #print("Number of duplicate ID's within resolution and magnitude factor-level combinations", 
    #    secondary_metrics_df.set_index(['spatial_resolution', 'magnitude','ID']).index.duplicated().sum())
    
    # join with nwm streams and convert datatypes
    secondary_metrics_df = secondary_metrics_df.merge(nwm_streams.loc[:,['ID','mainstem','order_',
                                                                               'Lake','gages',
                                                                               'Slope', 'Length']],
                                                            on='ID') \
                                                     .drop(columns='index') \
                                                     .astype({'huc8': np.int64,
                                                              'spatial_resolution':np.float64,
                                                              'magnitude': np.int64,
                                                              'ID' : np.int64,
                                                              'mainstem' : np.int64,
                                                              'order_' : np.float64,
                                                              'Lake' : np.int64,
                                                              'gages' : np.int64})
    """
                                                     .astype({'huc8':'category',
                                                              'spatial_resolution':np.float64,
                                                              'magnitude':'category',
                                                              'ID' : 'category',
                                                              'mainstem' : 'category',
                                                              'order_' : np.float64,
                                                              'Lake' : 'category',
                                                              'gages' : 'category'})
    """
    
    # save file
    if isinstance(save_filename,str) & isinstance(hdf_key,str):
        print(f'Writing to {save_filename}')
        secondary_metrics_df.to_hdf(save_filename,
                                       key=hdf_key,
                                       format='table',
                                       index=False)
    
    return(secondary_metrics_df)


def anova(secondary_metrics_df):
    
    """
    Index(['ID', 'CSI', 'TPR', 'FAR', 'MCC', 'huc8', 'spatial_resolution',
       'magnitude', 'TN', 'FN', 'FP', 'TP', 'Waterbody', 'mainstem', 'order_',
       'Lake', 'gages', 'Slope', 'Length'],
      dtype='object')
    """
    """
    # build linear model
    linear_model = ols("MCC ~ C(huc8) + C(spatial_resolution) +" 
                  "C(magnitude) + C(mainstem) + C(order_) +"
                  "C(Lake) + Slope + Length",
                  data=secondary_metrics_df).fit()
    
    # build anova table
    anova_table = sm.stats.anova_lm(linear_model, type=2)

    print(linear_model.summary())
    print(anova_table)
    
    sm_df_for_model = secondary_metrics_df.dropna(how='any',subset=target_cols) \
                                          .loc[:,feature_cols+target_cols] \

    nonencoded_features = list(set(feature_cols) - set(encoded_features))
    mapper = DataFrameMapper([([ef],OrdinalEncoder()) for ef in encoded_features] + \
                               [([nef], None) for nef in nonencoded_features],
                             input_df=True,
                             df_out=True)
    feature_df = mapper.fit_transform(sm_df_for_model.loc[:,feature_cols])

    sfs=SFS(LinearRegression(),n_features_to_select='auto',tol=0.0001,direction='backward',scoring='r2')
    
    sfs.fit(X=feature_df,
            y=sm_df_for_model.loc[:,target_cols])
    
    selected_features = list(sfs.get_feature_names_out()) #+ ['Lake*order_']

    """
    selected_features = feature_cols

    def __forward_model_selection(selected_features,tol=0.001,formula=None,prev_metric_val=None):

        remaining_features = set(selected_features)

        while remaining_features:
            
            results = []
            for sf in remaining_features:
                
                if formula == None:
                    formula_try = f"{target_cols[0]} ~ {sf}"
                else:
                    formula_try = formula + f" + {sf}"
                
                linear_model = ols(formula_try, 
                                   data=secondary_metrics_df).fit()
                print(f"R2 for {formula_try}: {linear_model.rsquared_adj}")
                results += [(sf,linear_model.rsquared)]

            results = sorted(results,key=lambda a: a[1],reverse=True)
            
            try:
                lead_factor = results.pop(0)[0]
            except IndexError:
                break
            
            if formula == None:
                prop_formula = f"{target_cols[0]} ~ {lead_factor}"
            else:
                prop_formula = formula + f" + {lead_factor}"
            
            prop_linear_model = ols(prop_formula, 
                                    data=secondary_metrics_df).fit()
            
            if prev_metric_val == None:
                prev_metric_val = 0

            delta = prop_linear_model.rsquared_adj - prev_metric_val
            
            prev_metric_val = linear_model.rsquared_adj

            if delta < tol:
                print(f"BROKEN: R2 for {formula}: {linear_model.rsquared_adj} | delta: {delta}")
                return(linear_model)
            else:
                linear_model = prop_linear_model
                formula = prop_formula
                print(f"Locked in Change R2 for {formula}: {linear_model.rsquared_adj} | delta: {delta}")


            remaining_features.remove(lead_factor)

        return(linear_model)
    
    linear_models = __forward_model_selection(selected_features)
    
    # make two way interactions
    if add_two_way_interactions:
        two_way = [f"{i}:{ii}" for i,ii in combinations(selected_features,2)]
        linear_models = __forward_model_selection(two_way,formula=linear_models.model.formula,prev_metric_val=linear_models.rsquared_adj)

    print(f"Final Formula: {linear_models.model.formula} | Adj-R2: {linear_models.rsquared_adj}")
    breakpoint()
    # CSI ~ Lake + order_ + Slope + spatial_resolution + Length + magnitude + order_:Slope + order_:Lake + spatial_resolution:Lake + magnitude:Length | Adj-R2: 0.43326573000631363
    # MCC ~ Lake + order_ + spatial_resolution + Slope + magnitude + Length + order_:Slope + order_:Lake + spatial_resolution:Lake + Lake:Slope + Length:Slope + order_:Length + spatial_resolution:order_ + spatial_resolution:magnitude | Adj-R2: 0.27142143145244746
    # TPR ~ Lake + spatial_resolution + order_ + Length + Slope + magnitude + order_:Lake + spatial_resolution:Lake + order_:Slope + spatial_resolution:order_ + spatial_resolution:Length + Lake:Length + magnitude:Length | Adj-R2: 0.4575340086432875
    # FAR ~ Slope + order_ + Lake + spatial_resolution + Length + magnitude + order_:Slope + order_:Lake + Length:Slope + Lake:Length + spatial_resolution:Slope | Adj-R2: 0.2752411117288007


    #return linear_model, anova_table

def nhd_to_3dep_plot():
    
    """
    histogram of differences (3dep 10m - nhd 10m) by catchment sorted and centered at zero and vertically oriented
    two columns by magnitude (100, 500yr), three rows by metric (CSI, TPR, FAR)
    """
    pass

def resolution_plot():
    
    """
    violin plots of metric values oriented horizontally for 3dep data by resolution (3,5,10,15,20m)
    split magnitudes by half along magnitude
    make three subplots along one row one for each metric
    """
    pass


def lake_plot():  
    """
    Illustrate issue with lakes????
    Investigate why we are getting scores in lake catchments first. They should be masked out.
    Maybe this is best presented with geospatial maps only for presentation?
    """
    pass

def slope_plot():
    pass

def channel_length_plot():
    pass

if __name__ == '__main__':
    
    # computes 4 secondary contingency metrics by nwm catchment for year, huc, and resolution
    if isinstance(from_file,str):
        secondary_metrics_df = pd.read_hdf(from_file,hdf_key) # read hdf
    else: # compute
        secondary_metrics_df = compute_metrics_by_catchment(nwm_catchments_fn,nwm_streams_fn, huc8s_vector_fn,
                                                               resolutions,years,hucs, chunk_size,
                                                               save_filename,hdf_key)
    if run_anova:
        anova(secondary_metrics_df)

    if make_nhd_plot:
        nhd_to_3dep_plot(secondary_metrics_df,nhd_to_3dep_plot_fn)

    # RUN THIS TO GET CATCHMENTS WITH LAKE METRICS
    # secondary_metrics_df.loc[secondary_metrics_df.loc[:,'Lake'] != -9999,'MCC'].dropna()
