#!/usr/bin/env python3

import os
from itertools import product, combinations
from time import time
import warnings
from functools import partial
from textwrap import wrap
import gc
from shutil import rmtree

import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_image
from tqdm import tqdm
from xrspatial.zonal import stats, crosstab, apply
from dask.dataframe.multi import concat, merge
from dask.dataframe import read_parquet
from dask.distributed import Client, LocalCluster
from tqdm.dask import TqdmCallback
import statsmodels.formula.api as smf
from statsmodels.formula.api import ols
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import SequentialFeatureSelector as SFS
from sklearn_pandas import DataFrameMapper
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, MinMaxScaler
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
#import ptitprince as pt
import seaborn as sns

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
resolutions = [20,15,10,5,3]
#resolutions = [20]
years = [100,500]
#years = [100]
hucs = ['12020001','12020002','12020003','12020004','12020005','12020006','12020007']
#hucs = ['12020001']
dem_sources = ['3dep','nhd']
#dem_sources = ['3dep']
chunk_size = 256*8
experiment_fn = os.path.join(data_dir,'experiment_data.h5')
temp_experiment_fn = os.path.join(data_dir,'TEMP_experiment_data')
hdf_key = 'data'
save_nwm_catchments_file = os.path.join(data_dir,'nwm_catchments_with_metrics_1202.gpkg')
#feature_cols = ['huc8','spatial_resolution','magnitude','mainstem','order_','Lake','gages','Length']
#feature_cols = ['spatial_resolution','magnitude','order_','Lake','Length','Slope']
feature_cols = ['spatial_resolution','dominant_lulc','magnitude','order_','Lake','Length','Slope','dem_source','area_sqkm']
one_way_interactions = ['spatial_resolution','']
# encoded_features = ['order_','Lake','magnitude']
#target_cols = ['CSI']
target_cols = ['MCC']
#target_cols = ['TPR']
#target_cols = ['FAR']
#target_cols = ['Cohens Kappa']
nhd_to_3dep_plot_fn = os.path.join(data_dir,'nhd_to_3dep_plot.png')
nwm_catchments_raster_fn = os.path.join(data_dir,'nwm_catchments','nwm_catchments_{}_{}_{}m_{}yr.tif')
dem_resolution_plot_fn = os.path.join(data_dir,'dem_resolution_3dep_plot.png')
reservoir_plot_fn = os.path.join(data_dir,'reservoir_plot.png')
slope_plot_fn = os.path.join(data_dir,'slope_plot.png')
#land_cover_fn = os.path.join(data_dir,'cover2019_lulc_1202.tif')
land_cover_fn = os.path.join(data_dir,'landcovers','land_cover_{}.tif')
landcover_plot_fn = os.path.join(data_dir,'lulc_metrics_plot.png')

# pipeline switches 
compute_secondary_metrics = True
burn_nwm_catchments = False
prepare_lulc = True
write_debugging_files = False
build_secondary_metrics = False
finalize_metrics = False

run_anova = False
add_two_way_interactions = True

make_nhd_plot = False
make_dem_resolution_plot = False
make_reservoir_plot = False
make_slope_plot = False
make_landcover_plot = False

# metrics dict
metrics_dict = { 'csi': csi, 'tpr': tpr, 'far': far, 'mcc': mcc } 
full_secondary_metrics = ['CSI' ,'TPR', 'FAR', 'MCC', 'Cohens Kappa','Total Samples', 'Frequency']

# changing numbers to str encodings
agreement_encoding_digits_to_names = { 0: "TN", 1: "FN",
                                       2: "FP", 3: "TP",
                                       4: "Waterbody"
                                     }

landcover_encoding_digits_to_names = {  11 : "Water",
                                        12 : "Perennial Ice Snow",
                                        21 : "Developed, Open Space",
                                        22 : "Developed, Low Intensity",
                                        23 : "Developed, Medium Intensity",
                                        24 : "Developed High Intensity",
                                        31 : "Bare Rock/Sand/Clay",
                                        41 : "Deciduous Forest",
                                        42 : "Evergreen Forest",
                                        43 : "Mixed Forest",
                                        52 : "Shrub/Scrub",
                                        71 : "Grasslands/Herbaceous",
                                        81 : "Pasture/Hay",
                                        82 : "Cultivated Crops",
                                        90 : "Woody Wetlands",
                                        95 : "Emergent Herbaceous Wetlands",
                                        45 : "Other_45",
                                        46 : "Other_46"
                                      }

flip_dict = lambda d : { v:k for k,v in d.items() }

landcover_encoding_names_to_digits = flip_dict(landcover_encoding_digits_to_names)

# metrics
def make_input_dict(row):
    """ make dictionary of primary metrics from row object when using pd/dask df apply """
    return {'TP' : row.TP, 'TN' : row.TN, 'FP' : row.FP, 'FN' : row.FN}


def compute_secondary_metrics_on_df(func):
    """ decorator function to compute secondary metrics when using pd/dask df apply """
    
    def wrapper(row):
        
        # makes input dict from row object when using pd/dask df apply
        input_dict = make_input_dict(row)
        
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore",category=RuntimeWarning)
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
calc_total_samples = compute_secondary_metrics_on_df(total_samples)


def build_land_cover_raster_fn(h,r,y,s):
    return os.path.join(data_dir,'landcovers',f'landcover_{r}m_{h}_{y}yr_{s}.tif')

def load_agreement_raster(h,r,y,s):
    
    # load agreement raster
    agreement_raster_fn = build_agreement_raster_data_dir(h,r,y,s)
    
    try:
        agreement_raster = rxr.open_rasterio(
                                             agreement_raster_fn,
                                             #chunks=True,
                                             chunks=chunk_size,
                                             mask_and_scale=True,
                                             variable='agreement',
                                             default_name='agreement',
                                             lock=True,
                                             cache=False
                                            ).sel(band=1,drop=True) 
    

    except rasterio.errors.RasterioIOError:
        print(f'No agreement raster for {h} found. Skipping')
        agreement_raster = None
    
    return agreement_raster


def determine_terrain_slope_by_catchment(nwm_catchments_xr, terrain_slope_fn,
                                         huc, agg_func='mean'):
    
    #agg_func_dict = { 'median': np.median, 'mean' : np.mean}

    terrain_slope_xr = rxr.open_rasterio(
                                     terrain_slope_fn.format(huc),
                                     chunks=chunk_size,
                                     mask_and_scale=True,
                                     variable='terrain_slope',
                                     default_name='terrain_slope',
                                     lock=True,
                                     cache=False
                                    ).sel(band=1,drop=True) \
                                     .rio.reproject_match(nwm_catchments_xr)

    ct_dask_slopes_df = stats(nwm_catchments_xr, terrain_slope_fn,
                              stats_func=agg_func, nodata_values=np.nan,
                              return_type='xarray.DataArray') \
                               .astype(np.float64) \
                               .rename(columns={'zone':'ID'}) \
                               .set_index('ID', drop=True) \
                               .repartition(partition_size='100MB')
                               #.rename(columns=landcover_encoding_digits_to_names) \

    breakpoint()
    
    return ct_dask_slopes_df


def determine_dominant_inundated_landcover(nwm_catchments_xr, land_cover_fn,
                                             agreement_raster,
                                             predicted_inundated_encodings=[2,3]): 
    
    land_cover_xr = rxr.open_rasterio(
                                     land_cover_fn,
                                     chunks=chunk_size,
                                     mask_and_scale=True,
                                     variable='landcover',
                                     default_name='landcover',
                                     lock=True,
                                     cache=False
                                    ).sel(band=1,drop=True) \
                                     .rio.reproject_match(agreement_raster)
    
    # masking out dry aras
    nwm_catchments_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),nwm_catchments_xr,np.nan)
    land_cover_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),land_cover_xr,np.nan)
    
    ct_dask_df_catchment_lc = crosstab(nwm_catchments_xr,land_cover_xr,nodata_values=np.nan) \
                                           .astype(np.float64) \
                                           .rename(columns={'zone':'ID'}) \
                                           .set_index('ID', drop=True) \
                                           .rename(columns=landcover_encoding_digits_to_names) \
                                           .repartition(partition_size='100MB')

    # remove catchments with no inundation and then find max landcover count. Returns that landcover
    ct_dask_df_catchment_lc = ct_dask_df_catchment_lc.loc[(ct_dask_df_catchment_lc!=0).any(axis=1)] \
                                                     .idxmax(1) \
                                                     .rename('dominant_lulc') \
                                                     .fillna('None') \
                                                     .astype({'dominant_lulc':str})
    
    return ct_dask_df_catchment_lc


def compute_metrics_by_catchment( nwm_catchments_fn, 
                                  nwm_streams_fn,
                                  huc8s_vector_fn,
                                  resolutions, years, hucs,
                                  dem_sources,
                                  chunk_size,
                                  full_secondary_metrics,
                                  experiment_fn=None,
                                  temp_experiment_fn=None,
                                  hdf_key=None,
                                  write_debugging_files=False,
                                  save_nwm_catchments_file=None,
                                  nwm_catchments_raster_fn=None,
                                  burn_nwm_catchments=False,
                                  prepare_lulc=False,
                                  land_cover_fn=None):
    
    
    def __burn_nwm_catchments():
        
        # loop over every combination of resolutions and magnitude years
        for idx,(h,r,y,s) in tqdm(enumerate(combos),
                                desc='Burning Catchments',
                                total=num_of_combos):
            
            agreement_raster = load_agreement_raster(h,r,y,s)

            # filter out for current huc
            nwm_catchments_current_huc8 = nwm_catchments.loc[nwm_catchments.loc[:,'huc8'] == h,:] \
                                                        .reset_index(drop=True)

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
                
            # write rasterized nwm_catchments just for inspection
            nwm_catchments_xr.rio.to_raster(nwm_catchments_raster_fn.format(s,h,r,y),
                                            tiled=True,windowed=True,
                                            lock=True,dtype=rasterio.int32,
                                            compress='lzw')
                
            if write_debugging_files:
                nwm_catchments_current_huc8.to_file(
                                                os.path.join(data_dir,'nwm_catchments',f'nwm_catchments_{s}_{h}_{r}m_{y}yr.gpkg'),
                                                driver='GPKG',index=False)
    
    def __prepare_lulc():
            
        # loop over every combination of resolutions and magnitude years
        for idx,(h,r,y,s) in tqdm(enumerate(combos),
                                desc='Preparing LULC',
                                total=num_of_combos):
            
            agreement_raster = load_agreement_raster(h,r,y,s)
        
            land_cover_xr = rxr.open_rasterio(
                                             land_cover_fn.format(h),
                                             chunks=chunk_size,
                                             mask_and_scale=True,
                                             variable='landcover',
                                             default_name='landcover',
                                             lock=True,
                                             cache=False
                                            ).sel(band=1,drop=True) \
                                         .rio.reproject_match(agreement_raster) \
                                         .rio.to_raster(build_land_cover_raster_fn(h,r,y,s),
                                                        tiled=True,windowed=True,
                                                        lock=True,dtype=rasterio.int32,
                                                        compress='lzw')


    # builds primary and lulc metrics and secondary metrics
    def __build_secondary_metrics():
        
        # provides framework for computed secondary metrics df
        meta = pd.DataFrame(columns=full_secondary_metrics, dtype='f8')
        
        # remove temp experiment file
        rmtree(temp_experiment_fn,ignore_errors=True)

        # loop over every combination of resolutions and magnitude years
        for idx,(h,r,y,s) in tqdm(enumerate(combos),
                                desc='Compute Metrics By Catchment',
                                total=num_of_combos):
            
            nwm_catchments_xr = rxr.open_rasterio(nwm_catchments_raster_fn.format(s,h,r,y),
                                                  chunks=chunk_size,
                                                  mask_and_scale=True,
                                                  variable='ID',
                                                  default_name='nwm_catchments',
                                                  lock=True,
                                                  cache=False
                                                 ).sel(band=1,drop=True) 
            
            # load agreement raster
            agreement_raster = load_agreement_raster(h,r,y,s)

            # compute cross tabulation table for ct_dask_df
            ct_dask_df = crosstab(nwm_catchments_xr,agreement_raster,nodata_values=np.nan) \
                                        .rename(columns=agreement_encoding_digits_to_names) \
                                        .astype(np.float64) \
                                        .rename(columns={'zone':'ID'}) \
                                        .set_index('ID', drop=True) \
                                        .repartition(partition_size='100MB')
            
            #ct_dask_df.visualize(filename=os.path.join(data_dir,'dask_graph.png'),optimize_graph=True)
            #breakpoint()
            
            # determines dominant inundated landcover by catchment
            ct_dask_df_catchment_lc = determine_dominant_inundated_landcover(nwm_catchments_xr,
                                                                             #land_cover_fn.format(h),
                                                                             build_land_cover_raster_fn(h,r,y,s),
                                                                             agreement_raster,
                                                                             predicted_inundated_encodings=[2,3])
            #del agreement_raster, nwm_catchments_xr
            #gc.collect()

            #### calculate metrics ###
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
                                             .astype({'ID':np.int64,'huc8':str, 'dem_source':str})
            
            # merge in primary metrics
            secondary_metrics_df = secondary_metrics_df.merge(ct_dask_df, left_on='ID', right_index=True)
            
            # append waterbody column if not already there
            if 'Waterbody' not in secondary_metrics_df.columns:
                secondary_metrics_df = secondary_metrics_df.assign(Waterbody=lambda r: np.nan) \
                                                           .astype({'Waterbody' : np.float64})

            #del ct_dask_df
            #gc.collect()

            # merge in landcovers
            secondary_metrics_df = merge(secondary_metrics_df,ct_dask_df_catchment_lc,
                                         how='left', left_on='ID',right_index=True)

            # dropnas
            secondary_metrics_df = secondary_metrics_df.dropna(subset=full_secondary_metrics,how='any') \
                                                       .reset_index(drop=True)
            
            # write to parquet
            secondary_metrics_df.to_parquet(temp_experiment_fn,
                                            write_metadata_file=True,
                                            append=True,
                                            write_index=False,
                                            compute=True,
                                            engine='fastparquet')


    def __finalize_files():

        print("Finalizing files ...")
        # read parquet file
        secondary_metrics_df = read_parquet(temp_experiment_fn,
                                            engine='fastparquet'
                                            ).compute()
        
        # what about repeat ID's
        # should we group by all factors and sum to aggregate???
        # checking for duplicated IDs within resolution and magnitude
        #print("Number of duplicate ID's within resolution and magnitude factor-level combinations", 
        #    secondary_metrics_df.set_index(['spatial_resolution', 'magnitude','ID']).index.duplicated().sum())
        
        # read parquet file

        # reset index
        #secondary_metrics_df = secondary_metrics_df.dropna(subset=full_secondary_metrics,how='any') \
        #                                           .reset_index(drop=True)
        
        # merge back into nwm_catchments
        nwm_catchments_with_metrics = nwm_catchments.merge(secondary_metrics_df.drop(columns='huc8'),on='ID') \
                                                    .merge(nwm_streams.loc[:,['ID','Slope','Lake',
                                                                              'gages','Length']],
                                                           on='ID')

        # join with nwm streams and convert datatypes
        secondary_metrics_df = secondary_metrics_df.merge(nwm_streams.loc[:,['ID','mainstem',
                                                                             'order_','Lake','gages',
                                                                             'Slope', 'Length']],
                                                                on='ID') \
                                                   .merge(nwm_catchments.loc[:,['ID','area_sqkm']],on='ID') \
                                                   .astype({'huc8': 'category',
                                                            'spatial_resolution': np.float64,
                                                            'area_sqkm': np.float64,
                                                            'magnitude': 'category',
                                                            'ID' : 'category',
                                                            'mainstem' : 'category',
                                                            'order_' : np.float64,
                                                            'Lake' : 'category',
                                                            'gages' : 'category',
                                                            'dem_source': 'category'})

        # saving nwm catchments with metrics
        if save_nwm_catchments_file:
            nwm_catchments_with_metrics.to_file(save_nwm_catchments_file,index=False,driver='GPKG')
        

        # save file
        if isinstance(experiment_fn,str) & isinstance(hdf_key,str):
            print(f'Writing to {experiment_fn}')
            secondary_metrics_df.to_hdf(experiment_fn,
                                           key=hdf_key,
                                           format='table',
                                           index=False)
        
        return(secondary_metrics_df)

    
    # load catchments and streams
    print("Loading and prepping files ...")
    nwm_streams = gpd.read_file(nwm_streams_fn)
    nwm_catchments = gpd.read_file(nwm_catchments_fn)
    huc8s_df = gpd.read_file(huc8s_vector_fn)

    # compute catchment areas
    nwm_catchments['area_sqkm'] = nwm_catchments.loc[:,'geometry'].area / (1000*1000)
    
    # spatial join with columns from huc8s_df to get catchment assignments by huc8
    nwm_catchments = nwm_catchments \
                             .sjoin(huc8s_df.loc[:,['huc8','geometry']],how='inner',predicate='intersects') \
                             .drop('index_right',axis=1) \
                             .reset_index(drop=True)
    
    # prepare combinations of resolutions, hucs, and years
    combos = list(product(hucs,resolutions,years))
    
    # append source
    combos = [(h,r,y,'3dep') for h,r,y in combos]
    
    if 'nhd' in dem_sources:
        nhd_combos = [(h,10,y,'nhd') for h,y in product(hucs,years)] 
        combos = nhd_combos + combos

    # prepare outputs
    num_of_combos = len(combos)
    list_of_secondary_metrics_df = [None] * num_of_combos

    # burning catchments
    if burn_nwm_catchments:
        __burn_nwm_catchments()

    if prepare_lulc:
        __prepare_lulc()

    if build_secondary_metrics:
        __build_secondary_metrics()
    
    if finalize_metrics:
        secondary_metrics_df = __finalize_files()

        return secondary_metrics_df


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
    
    
    # scale numerics
    scaler = MinMaxScaler(copy=True)
    secondary_metrics_df = pd.concat([ pd.DataFrame(scaler.fit_transform(secondary_metrics_df.select_dtypes(np.number).to_numpy()),
                                         columns=secondary_metrics_df.select_dtypes(np.number).columns),
                                       secondary_metrics_df.select_dtypes('category')],
                                  axis=1)


    linear_models = __forward_model_selection(selected_features)
    
    # make two way interactions
    if add_two_way_interactions:
        two_way = [f"{i}:{ii}" for i,ii in combinations(selected_features,2)]
        linear_models = __forward_model_selection(two_way,formula=linear_models.model.formula,prev_metric_val=linear_models.rsquared_adj)

    print(f"Final Formula: {linear_models.model.formula} | Adj-R2: {linear_models.rsquared_adj}")
    breakpoint()
    # MCC ~ Lake + dominant_lulc + dem_source + order_ + Slope + area_sqkm + magnitude + Length + spatial_resolution + dominant_lulc:order_ + dominant_lulc:Slope + dominant_lulc:Lake + dominant_lulc:Length + order_:Slope + dominant_lulc:area_sqkm + Slope:area_sqkm + order_:area_sqkm + order_:Lake + Lake:dem_source + order_:dem_source + spatial_resolution:dominant_lulc + Lake:Length + Lake:Slope | Adj-R2: 0.29276087401948514

    """
    # code on how to plot significant linear model parameters by normalized values
    # adopted from: https://stats.stackexchange.com/questions/89747/how-to-describe-or-visualize-a-multiple-linear-regression-model
    X_norm = X4.copy() # This is a pd.Dataframe of the independent variables
    X_norm = (X_norm - X_norm.mean()) / X_norm.std()
    res_norm = sm.OLS(y_log, sm.add_constant(X_norm)).fit()

    to_include = res_norm.params[res_norm.pvalues < 0.05][1:].sort_values() # get only those with significant pvalues
    fig, ax = plt.subplots(figsize=(5,6), dpi=100)
    ax.scatter(to_include, range(len(to_include)), color="#1a9988", zorder=2)
    ax.set_yticks(range(len(to_include)), to_include.index) # label the y axis with the ind. variable names
    ax.set_xlabel("Proportional Effect")
    ax.set_title("Strength of Relationships")

    # add the confidence interval error bars
    for idx, ci in enumerate(res_norm.conf_int().loc[to_include.index].iterrows()):
        ax.hlines(idx, ci[1][0], ci[1][1], color="#eb5600", zorder=1, linewidth=3)

    plt.axline((0,0), (0,1), color="#eb5600", linestyle="--")
    """

    #return linear_model, anova_table

def nhd_to_3dep_plot(secondary_metrics_df,output_fn):
    
    """
    histogram of differences (3dep 10m - nhd 10m) by catchment sorted and centered at zero and vertically oriented
    two columns by magnitude (100, 500yr), three rows by metric (CSI, TPR, FAR)
    """
    
    """
    Index(['ID', 'CSI', 'TPR', 'FAR', 'MCC', 'huc8', 'spatial_resolution',
       'magnitude', 'TN', 'FN', 'FP', 'TP', 'Waterbody', 'mainstem', 'order_',
       'Lake', 'gages', 'Slope', 'Length','dem_source'],
      dtype='object')
    """
    # prepare secondary metrics
    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Corr. Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    def get_terrain_label(metric):
        label_dict = {'_diff':'Difference (3DEP-NHD)','_3dep': "3DEP",'_nhd':"NHD"}
        metric.split('_')[1]
    #metric = metrics[0]

    prepared_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                           .set_index(['huc8','magnitude','spatial_resolution','dem_source','ID']) \
                                           .sort_index() 
    
    metrics_3dep = prepared_metrics.xs('3dep',level="dem_source") \
                                   .xs(10,level='spatial_resolution') \
                                   .loc[:,metrics]
    metrics_nhd = prepared_metrics.xs('nhd',level="dem_source") \
                                   .xs(10,level='spatial_resolution') \
                                   .loc[:,metrics]
    
    difference = (metrics_3dep - metrics_nhd).dropna(how='any') 
    
    all_metrics = difference.join(metrics_3dep,how='left',lsuffix='_diff',rsuffix='_3dep') \
                            .join(metrics_nhd,how='left') \
                            .rename(columns=dict(zip(metrics,[m+'_nhd' for m in metrics])))

    for mag in [100,500]:
        fig,axs = plt.subplots(2,2,dpi=300,figsize=(8,8),layout='tight')

        for i,(ax,metric) in enumerate(zip(axs.ravel(),metrics)):

            sorted_metrics = all_metrics.sort_values(metric+'_diff',ascending=False) \
                                        .xs(mag,level='magnitude')
            
            if metric == 'FAR':
                improved_indices = sorted_metrics.loc[:,metric+"_diff"]<=0
            else:
                improved_indices = sorted_metrics.loc[:,metric+"_diff"]>=0
            
            reduced_indices = ~improved_indices
            
            #y = range(len(sorted_metrics))
            #x = sorted_metrics.loc[:,metric+'_diff'] + sorted_metrics.loc[:,[metric+'_nhd', metric+'_3dep']].min(axis=1)
            
            # errorbars
            print(f"Metric employed: {metric}")
            proportion_above_zero = ((sorted_metrics.loc[:,metric+'_3dep'] -sorted_metrics.loc[:,metric+'_nhd'])>0).sum()/len(sorted_metrics)
            print(f"Proportion of catchments that perform better with 3dep: {proportion_above_zero}")
            median_diff = sorted_metrics.loc[:,metric+'_diff'].median()
            mean_diff = sorted_metrics.loc[:,metric+'_diff'].mean()
            std_diff = sorted_metrics.loc[:,metric+'_diff'].std()
            print(f"Median, mean, and std improvements: {median_diff} | {mean_diff} | {std_diff}")
            
            # TRY:
            def assign_color(series,metric):
                color_dict = {True:'green',False:'red'}
                if metric == 'FAR': color_dict = {False:'green',True:'red'}

                return (series >=0 ).apply(lambda x: color_dict[x])
            
            
            improvement = ax.scatter(sorted_metrics.loc[improved_indices,metric+'_nhd'],
                                     sorted_metrics.loc[improved_indices,metric+'_3dep'],
                                     c='green',
                                     s=1)
            
            reduction = ax.scatter(sorted_metrics.loc[reduced_indices,metric+'_nhd'],
                                   sorted_metrics.loc[reduced_indices,metric+'_3dep'],
                                   c='red',
                                   s=1)
            
            ax.axline((0,0),slope=1,color='black')

            metric_min = sorted_metrics.loc[:,[metric+'_3dep',metric+'_nhd']].min().min()
            metric_max = sorted_metrics.loc[:,[metric+'_3dep',metric+'_nhd']].max().max()

            ax.set_xlim(metric_min,metric_max)
            ax.set_ylim(metric_min,metric_max)
            ax.tick_params(axis='both', labelsize=12)
            
            #ax.set_title(metric_dict[metric]+"\n"+"("+metric+")",fontsize=15)
            ax.set_title(metric_dict[metric]+" ("+metric+")",fontsize=15)
            
            if i in {2,3}: ax.set_xlabel("Metric Values"+"\n"+"NHDPlusHR DEM",fontsize=12)
            if i in {0,2}: ax.set_ylabel("Metric Values"+"\n"+"3DEP DEM",fontsize=12)

            ax.text(0.55,0.16,f'Mean: {np.round(mean_diff,3)}',transform=ax.transAxes,fontsize=12)
            ax.text(0.55,0.1,f'Std: {np.round(std_diff,3)}',transform=ax.transAxes,fontsize=12)
            ax.text(0.55,0.04,f'Perc.>0: {np.round(proportion_above_zero*100,1)}%',transform=ax.transAxes,fontsize=12)
    # code on how to plot significant linear model parameters by normalized values

        lgd = fig.legend(handles=[improvement,reduction],
                       labels=['Improvement or no change (difference >=0)','Reduction (difference<0)'],
                       loc='lower center',
                       frameon=True,
                       framealpha=0.75,
                       fontsize=12,
                       title_fontsize=14,
                       borderpad=0.25,
                       markerscale=3,
                       bbox_to_anchor=(0.5,-.1),
                       borderaxespad=0,
                       title="Metric Value Difference (3DEP - NHDPlusHR DEM)"
                       )
        
        """
        axs.errorbar(x, y,
                     xerr=sorted_metrics.loc[:,metric+'_diff'],
                     elinewidth=0.05,
                     capsize=0.1,
                     capthick=0.01,
                     mec='r',
                     marker='o',
                     mfc='r')
        """

        fig.savefig(os.path.join(data_dir,f'nhd_vs_3dep_{mag}yr.png'), bbox_extra_artists=(lgd,), bbox_inches='tight')
        plt.close(fig)

    pass

def resolution_plot(secondary_metrics_df, dem_resolution_plot_fn=None):
    
    """
    violin plots of metric values oriented horizontally for 3dep data by resolution (3,5,10,15,20m)
    split magnitudes by half along magnitude
    make three subplots along one row one for each metric
    """
    # prepare secondary metrics
    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Correlation Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .query('dem_source == "3dep"')

    fig,axs = plt.subplots(2,2,dpi=300,figsize=(8,8),layout='tight')

    for i,(ax,metric) in enumerate(zip(axs.ravel(),metrics)):

        ax = sns.violinplot(data=all_metrics,
                            x='spatial_resolution',
                            y=metric,
                            hue='magnitude',
                            hue_order=[100,500],
                            order=[3,5,10,15,20],
                            bw='scott',
                            ax=ax, split=True,
                            inner='quartile',
                            cut=True,
                            palette=[mcolors.CSS4_COLORS["cornflowerblue"],
                                     mcolors.CSS4_COLORS["palegoldenrod"]],
                            linewidth=2,
                            saturation=0.75,
                            alpha=0.75
                           )
        
        # fit model
        bool_100yr = all_metrics.loc[:,'magnitude'] == 100
        linear_model_100yr = ols(f"{metric} ~ spatial_resolution", 
                                 data=all_metrics.loc[bool_100yr,:]).fit()
        bool_500yr = all_metrics.loc[:,'magnitude'] == 500
        linear_model_500yr = ols(f"{metric} ~ spatial_resolution", 
                                 data=all_metrics.loc[bool_500yr,:]).fit()
        reg_func = lambda x,lm: lm.params.Intercept + (lm.params.spatial_resolution * x)
        compute_y = lambda x, lm: list(map(partial(reg_func,lm=lm),x))
        
        trendline_100yr_color = mcolors.CSS4_COLORS["green"]
        trendline_500yr_color = mcolors.CSS4_COLORS["red"]

        # plot 100yr
        x = list(ax.get_xlim())
        y = compute_y(x,linear_model_100yr)
        trendline_100yr = ax.plot(x,y,color=trendline_100yr_color,
                                  linewidth=3,label='Trendline: 100yr')
        
        # plot 500yr
        y = compute_y(x,linear_model_500yr)
        trendline_500yr = ax.plot(x,y,color=trendline_500yr_color,
                                  linewidth=3,linestyle='dashed',label='Trendline: 500yr')

        if metric == 'FAR':
            ax.set_ylim([0,0.30])
        elif metric == 'TPR':
            ax.set_ylim([0.6,1])
        else:
            ax.set_ylim([0.4,1])

        # used to normalize all y limits to same for better comparison
        ax.set_ylim([0,1])

        ax.tick_params(axis='both', labelsize=12)

        if i in {2,3}:
            ax.set_xlabel('Spatial Resolution (m)',fontsize=12)
        else:
            ax.set_xlabel(None)

        if i in {0,2}:
            ax.set_ylabel('Metric Value',fontsize=12)
        else:
            ax.set_ylabel(None)
        
        #ax.set_title(metric_dict[metric]+"\n"+"("+metric+")",fontsize=15,y=1.15)
        ax.set_title(metric_dict[metric]+" ("+metric+")",fontsize=15,y=1.12)

        # parameter labels
        ax.text(0.1+.4,1.08,'slope:',fontsize=12,color='black')
        ax.text(-0.2+.4,1.02,'p-value:',fontsize=12,color='black')
        
        # 100yr
        ax.text(1+.4,1.08,
                '{:.1E}'.format(linear_model_100yr.params.spatial_resolution),
                fontsize=12, color=trendline_100yr_color)
        ax.text(1+.4,1.02,
                '{:.1E}'.format(linear_model_100yr.pvalues.spatial_resolution),
                fontsize=12, color=trendline_100yr_color)
        
        # 500yr
        ax.text(2.6,1.08,
                '{:.1E}'.format(linear_model_500yr.params.spatial_resolution),
                fontsize=12, color=trendline_500yr_color)
        ax.text(2.6,1.02,
                '{:.1E}'.format(linear_model_500yr.pvalues.spatial_resolution),
                fontsize=12, color=trendline_500yr_color)
        
        #ax.text(2.5,1.02,'p-value: {:.1E}'.format(linear_model_100yr.pvalues.spatial_resolution),
        #        fontsize=12, color=trendline_500yr_color)

        # magnitude
        #ax.legend_.set_title('Magnitude (yr)')
        ax.legend_ = None

    h,l = ax.get_legend_handles_labels()
    l[:2] = ['KDE: 100yr','KDE: 500yr']
    lgd = fig.legend(h,l,
           loc='lower center',
           ncols=2,
           frameon=True,
           framealpha=0.75,
           fontsize=12,
           title_fontsize=14,
           borderpad=0.25,
           markerscale=3,
           bbox_to_anchor=(0.55,-.06),
           borderaxespad=0,
           title=None)
    
    if dem_resolution_plot_fn != None:
        fig.savefig(dem_resolution_plot_fn, bbox_inches='tight')
    
    plt.close(fig)


def reservoir_plot(secondary_metrics_df,reservoir_plot_fn):  
    """
    Illustrate issue with lakes
    """
    
    # prepare secondary metrics
    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Correlation Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .query('dem_source == "3dep"')

    # threshold Lake
    all_metrics_thresholded_lakes = all_metrics.copy()
    lake_bool = all_metrics_thresholded_lakes.loc[:,'Lake'] != -9999
    all_metrics_thresholded_lakes.loc[:,'Lake'] = all_metrics_thresholded_lakes.loc[:,'Lake'].astype(bool)
    all_metrics_thresholded_lakes.loc[lake_bool,'Lake'] = True
    all_metrics_thresholded_lakes.loc[~lake_bool,'Lake'] = False
    #all_metrics_thresholded_lakes.loc[:,"Lake"] = all_metrics_thresholded_lakes.loc[:,"Lake"].astype("Category")

    fig,axs = plt.subplots(2,2,dpi=300,figsize=(8,8),layout='tight')

    for i,(ax,metric) in enumerate(zip(axs.ravel(),metrics)):

        ax = sns.violinplot(data=all_metrics_thresholded_lakes,
                            x='spatial_resolution',
                            y=metric,
                            hue='Lake',
                            hue_order=[True,False],
                            order=[3,5,10,15,20],
                            bw='scott',
                            ax=ax, split=True,
                            inner='quartile',
                            cut=True,
                            palette=[mcolors.CSS4_COLORS["cornflowerblue"],
                                     mcolors.CSS4_COLORS["palegoldenrod"]],
                            linewidth=2,
                            saturation=0.75,
                            alpha=0.75
                           )
        
        # fit model
        bool_noreservoir = all_metrics_thresholded_lakes.loc[:,'Lake'] == False
        linear_model_noreservoir = ols(f"{metric} ~ spatial_resolution", 
                                 data=all_metrics_thresholded_lakes.loc[bool_noreservoir,:]).fit()
        bool_reservoir = all_metrics_thresholded_lakes.loc[:,'Lake'] == True
        linear_model_reservoir = ols(f"{metric} ~ spatial_resolution", 
                                 data=all_metrics_thresholded_lakes.loc[bool_reservoir,:]).fit()
        reg_func = lambda x,lm: lm.params.Intercept + (lm.params.spatial_resolution * x)
        compute_y = lambda x, lm: list(map(partial(reg_func,lm=lm),x))
        
        trendline_noreservoir_color = mcolors.CSS4_COLORS["green"]
        trendline_reservoir_color = mcolors.CSS4_COLORS["red"]

        # plot 100yr
        x = list(ax.get_xlim())
        y = compute_y(x,linear_model_noreservoir)
        trendline_noreservoir = ax.plot(x,y,color=trendline_noreservoir_color,
                                  linewidth=3,label='Trendline: No Reservoir')
        
        # plot 500yr
        y = compute_y(x,linear_model_reservoir)
        trendline_reservoir = ax.plot(x,y,color=trendline_reservoir_color,
                                  linewidth=3,linestyle='dashed',label='Trendline: Reservoir')

        if metric == 'FAR':
            ax.set_ylim([0,0.30])
        elif metric == 'TPR':
            ax.set_ylim([0.6,1])
        else:
            ax.set_ylim([0.4,1])

        # used to normalize all y limits to same for better comparison
        ax.set_ylim([0,1])

        ax.tick_params(axis='both', labelsize=12)

        if i in {2,3}:
            ax.set_xlabel('Spatial Resolution (m)',fontsize=12)
        else:
            ax.set_xlabel(None)

        if i in {0,2}:
            ax.set_ylabel('Metric Value',fontsize=12)
        else:
            ax.set_ylabel(None)
        
        #ax.set_title(metric_dict[metric]+"\n"+"("+metric+")",fontsize=15,y=1.15)
        ax.set_title(metric_dict[metric]+" ("+metric+")",fontsize=15,y=1.12)
        #ax.set_title(metric_dict[metric]+" ("+metric+")",fontsize=15)

        # parameter labels
        ax.text(0.1+.4,1.08,'slope:',fontsize=12,color='black')
        ax.text(-0.2+.4,1.02,'p-value:',fontsize=12,color='black')
        
        # No Reservoir
        ax.text(1+.4,1.08,
                '{:.1E}'.format(linear_model_noreservoir.params.spatial_resolution),
                fontsize=12, color=trendline_noreservoir_color)
        ax.text(1+.4,1.02,
                '{:.1E}'.format(linear_model_noreservoir.pvalues.spatial_resolution),
                fontsize=12, color=trendline_noreservoir_color)
        
        # Reservoir
        ax.text(2.60,1.08,
                '{:.1E}'.format(linear_model_reservoir.params.spatial_resolution),
                fontsize=12, color=trendline_reservoir_color)
        ax.text(2.60,1.02,
                '{:.1E}'.format(linear_model_reservoir.pvalues.spatial_resolution),
                fontsize=12, color=trendline_reservoir_color)
        
        #ax.text(2.5,1.02,'p-value: {:.1E}'.format(linear_model_noreservoir.pvalues.spatial_resolution),
        #        fontsize=12, color=trendline_reservoir_color)

        # magnitude
        #ax.legend_.set_title('Magnitude (yr)')
        ax.legend_ = None

    h,l = ax.get_legend_handles_labels()
    l[:2] = ['KDE: Reservoir','KDE: No Reservoir']
    lgd = fig.legend(h,l,
           loc='lower center',
           ncols=2,
           frameon=True,
           framealpha=0.75,
           fontsize=12,
           title_fontsize=14,
           borderpad=0.25,
           markerscale=3,
           bbox_to_anchor=(0.55,-.06),
           borderaxespad=0,
           title=None)
    
    fig.savefig(reservoir_plot_fn, bbox_inches='tight')
    
    plt.close(fig)
    

def slope_plot(secondary_metrics_df,slope_plot_fn):
    
    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Corr. Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .query('dem_source == "3dep"') \
                                      .query('Lake == -9999')
    
    # convert to percentage
    all_metrics.loc[:,'Slope'] = all_metrics.loc[:,'Slope'] * 100

    bool_100yr = all_metrics.loc[:,'magnitude'] == 100
    bool_500yr = all_metrics.loc[:,'magnitude'] == 500

    fig,axs = plt.subplots(2,2,dpi=300,figsize=(8,8),layout='tight')

    for i,(ax,metric) in enumerate(zip(axs.ravel(),metrics)):
        
        xlim=(0,0.01)
        xlim=(0,1)
        
        if metric == 'FAR':
            ylim = (0,0.25)
        else:
            ylim = (0.5,1)

        """
        hist = ax.hist2d(all_metrics.loc[:,'Slope'],
                   all_metrics.loc[:,metric],
                   range=[xlim,ylim],
                   density=True,
                   bins=50,
                   cmap='Blues',
                   cmin=0.1
                   #cmax=0.8
                   )
        """
        pts_100yr = ax.scatter(all_metrics.loc[bool_100yr,'Slope'],
                               all_metrics.loc[bool_100yr,metric],
                               alpha=0.3,s=0.08,c='red',
                               label='Catchments: 100yr'
                              )
        
        pts_500yr = ax.scatter(all_metrics.loc[bool_500yr,'Slope'],
                               all_metrics.loc[bool_500yr,metric],
                               alpha=0.2,s=0.08,c='blue',
                               label='Catchments: 500yr'
                              )
        
        # fit model
        linear_model_100yr = sm.RLM(all_metrics.loc[bool_100yr,metric],
                                    sm.add_constant(all_metrics.loc[bool_100yr,"Slope"]),
                                    M=sm.robust.norms.TrimmedMean()
                                   ).fit()
        linear_model_500yr = sm.RLM(all_metrics.loc[bool_500yr,metric],
                                    sm.add_constant(all_metrics.loc[bool_500yr,"Slope"]),
                                    M=sm.robust.norms.TrimmedMean()
                                   ).fit()

        reg_func = lambda x,lm: lm.params.const + (lm.params.Slope * x)
        compute_y = lambda x, lm: list(map(partial(reg_func,lm=lm),x))
        
        """
        lci_reg_func = lambda x,lm: lm.conf_int().loc['const',0] + (lm.conf_int().loc['Slope',0] * x)
        compute_lci = lambda x, lm: list(map(partial(lci_reg_func,lm=lm),x))
        
        uci_reg_func = lambda x,lm: lm.conf_int().loc['const',1] + (lm.conf_int().loc['Slope',1] * x)
        compute_uci = lambda x, lm: list(map(partial(uci_reg_func,lm=lm),x))
        """
        
        trendline_100yr_color = mcolors.CSS4_COLORS["red"]
        trendline_500yr_color = mcolors.CSS4_COLORS["blue"]

        # plot 100yr
        x = list(ax.get_xlim())
        y = compute_y(x,linear_model_100yr)
        trendline_100yr = ax.plot(x,y,color=trendline_100yr_color,
                                  linewidth=3,label='Trendline: 100yr')
        
        """
        y = compute_lci(x,linear_model_100yr)
        trendline_lci_100yr = ax.plot(x,y,color=trendline_100yr_color,
                                      linewidth=1,label='Lower 95% CI: 100yr')
        
        y = compute_uci(x,linear_model_100yr)
        trendline_uci_100yr = ax.plot(x,y,color=trendline_100yr_color,
                                      linewidth=1,label='Upper 95% CI: 100yr')
        """
        
        # plot 500yr
        y = compute_y(x,linear_model_500yr)
        trendline_500yr = ax.plot(x,y,color=trendline_500yr_color,
                                  linewidth=3,
                                  label='Trendline: 500yr')
        
        """
        y = compute_lci(x,linear_model_500yr)
        trendline_lci_500yr = ax.plot(x,y,color=trendline_500yr_color,
                                      linewidth=1,linestyle='dashed',
                                      label='Lower 95% CI: 500yr')
        
        y = compute_uci(x,linear_model_500yr)
        trendline_uci_500yr = ax.plot(x,y,color=trendline_500yr_color,
                                      linewidth=1,linestyle='dashed',
                                      label='Upper 95% CI: 500yr')
        """
        
        # parameter labels
        if metric != 'FAR':
            y_slope_loc = ylim[1] + 0.04
            y_pval_loc = ylim[1] + 0.01
        else:
            y_slope_loc = ylim[1] + 0.02
            y_pval_loc = ylim[1] + 0.005
        
        ax.text(0.20,y_slope_loc,'slope:',fontsize=12,color='black')
        ax.text(0.15,y_pval_loc,'p-value:',fontsize=12,color='black')
        
        # 100yr
        ax.text(.40,y_slope_loc,
                '{:.1E}'.format(linear_model_100yr.params.Slope),
                fontsize=12, color=trendline_100yr_color)
        ax.text(.40,y_pval_loc,
                '{:.1E}'.format(linear_model_100yr.pvalues.Slope),
                fontsize=12, color=trendline_100yr_color)
        
        coef_of_deter_100yr = (np.corrcoef(all_metrics.loc[bool_100yr,"Slope"],all_metrics.loc[bool_100yr,metric])**2)[0,1]
        ax.text(0.6,ylim[0]+((ylim[1]-ylim[0])/8),
                'R2: {:.4f}'.format(coef_of_deter_100yr),
                fontsize=12, color=trendline_100yr_color)

        
        # 500yr
        ax.text(.65,y_slope_loc,
                '{:.1E}'.format(linear_model_500yr.params.Slope),
                fontsize=12, color=trendline_500yr_color)
        ax.text(.65,y_pval_loc,
                '{:.1E}'.format(linear_model_500yr.pvalues.Slope),
                fontsize=12, color=trendline_500yr_color)
        
        coef_of_deter_500yr = (np.corrcoef(all_metrics.loc[bool_500yr,"Slope"],all_metrics.loc[bool_500yr,metric])**2)[0,1]
        ax.text(0.6,ylim[0]+((ylim[1]-ylim[0])/14),
                'R2: {:.4f}'.format(coef_of_deter_500yr),
                fontsize=12, color=trendline_500yr_color)
        
        ax.set_xlim(xlim)
        ax.set_ylim(ylim)
        
        ax.tick_params(axis='both', labelsize=12)
        #ax.tick_params(axis='x', rotation=45)
        #ax.ticklabel_format(axis='xx',style='scientific',scilimits=(0,0))

        if i in {2,3}:
            ax.set_xlabel("Channel Slope"+"\n"+"(vertical/horizontal)",fontsize=12)
            ax.set_xlabel("Channel Slope (%)",fontsize=12)
        else:
            ax.set_xlabel(None)

        if i in {0,2}:
            ax.set_ylabel('Metric Value',fontsize=12)
        else:
            ax.set_ylabel(None)
        
        #ax.set_title(metric_dict[metric]+"\n"+"("+metric+")",fontsize=15,y=1.15)
        ax.set_title(metric_dict[metric]+" ("+metric+")",fontsize=15,y=1.12)

    h,l = ax.get_legend_handles_labels()
    lgd = fig.legend(h,l,
           loc='lower center',
           ncols=2,
           frameon=True,
           framealpha=0.75,
           fontsize=12,
           title_fontsize=14,
           borderpad=0.25,
           markerscale=15,
           bbox_to_anchor=(0.55,-.06),
           borderaxespad=0,
           title=None)


    fig.savefig(slope_plot_fn, bbox_inches='tight')
    plt.close()

def landcover_plot(secondary_metrics_df,landcover_plot_fn):
    
    metrics = ['MCC','CSI','TPR','FAR']
    metrics = ['FAR','TPR','CSI','MCC']
    metric_dict = {'MCC': "Matthew's Correlation Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .query('dem_source == "3dep"') \
                                      .query('dominant_lulc != "Other_45"') \
                                      .query('dominant_lulc != "Other_46"') 

    # dropping low occurrences
    if False:
        drops = all_metrics.groupby('dominant_lulc').count().loc[:,'ID'] > 26
        all_metrics = all_metrics.set_index('dominant_lulc',drop=True) \
                                 .loc[drops,:] \
                                 .reset_index(drop=False)
    
    order_strs = all_metrics.loc[:,'dominant_lulc'] \
                            .unique() \
                            .tolist()
    order_digits = [landcover_encoding_names_to_digits[s] for s in order_strs]

    def sort_two_lists(list1,list2):
        return (list(x) for x in zip(*sorted(zip(list1,list2), key=lambda pair:pair[0])))

    order_digits,order_strs = sort_two_lists(order_digits,order_strs)
    
    #all_metrics.loc[:,'dominant_lulc_digit'] =  all_metrics.loc[:,'dominant_lulc'] \
    #                                                       .apply(lambda s : landcover_encoding_names_to_digits[s])

    #breakpoint()
    #all_metrics.loc[:,'dominant_lulc_digit'] = order_digits

    #breakpoint()
    #all_metrics = all_metrics.sort_values('dominant_lulc_digit',ascending=True)
    #breakpoint()

    fig,axs = plt.subplots(4,1,dpi=300,figsize=(8.5,11),layout='tight')

    for i,(ax,metric) in enumerate(zip(axs.ravel(),metrics)):
        
        """
        if metric == 'FAR':
            ascending=True
        else:
            ascending=False

        # determine order for strings and digits lulc encodings
        order_strs = all_metrics.groupby('dominant_lulc') \
                                .median(numeric_only=True) \
                                .loc[drops,:] \
                                .sort_values(metric,ascending=ascending) \
                                .index \
                                .tolist()
        order_digits = [landcover_encoding_names_to_digits[s] for s in order_strs]
        """

        ax = sns.boxplot(data=all_metrics,
                            x='dominant_lulc',
                            y=metric,
                            hue='magnitude',
                            hue_order=[100,500],
                            order=order_strs,
                            ax=ax, 
                            palette=[mcolors.CSS4_COLORS["cornflowerblue"],
                                     mcolors.CSS4_COLORS["palegoldenrod"]],
                            linewidth=2,
                            saturation=0.75,
                           )
        
        ax.tick_params(axis='y', labelsize=12)
        ax.set_ylabel(metric_dict[metric]+"\n("+metric+")",fontsize=12)

        if i == 3:
            #ax.set_xlabel("Channel Slope"+"\n"+"(vertical/horizontal)",fontsize=12)
            ax.set_xlabel("Landcover / Landuse Categories",fontsize=12)
            wrapped_order_strs = [ '\n'.join(wrap(l, 17)) for l in order_strs]
            ax.set_xticklabels([f'{s} ({d})' for s,d in zip(wrapped_order_strs,order_digits)])
            #ax.tick_params(axis='x', rotation=70)
            plt.setp( ax.xaxis.get_majorticklabels(), rotation=45, ha="right", rotation_mode='anchor' )
            ax.tick_params(axis='x', labelsize=10)
        else:
            ax.set_xticklabels(order_digits)
            ax.tick_params(axis='x', labelsize=12)
            ax.set_xlabel(None)

        #ax.set_title(metric_dict[metric]+" ("+metric+")",fontsize=12)
        
        #ax.set_title(metric_dict[metric]+"\n"+"("+metric+")",fontsize=15,y=1.15)
        #ax.set_title(metric_dict[metric]+" ("+metric+")",fontsize=15,y=1.12)
        
        ax.set_ylim([0,1])

        ax.legend_ = None
    
    h,l = ax.get_legend_handles_labels()
    l[:2] = ['100yr','500yr']
    lgd = fig.legend(h,l,
           loc='lower center',
           ncols=2,
           frameon=True,
           framealpha=0.75,
           fontsize=12,
           title_fontsize=14,
           borderpad=0.25,
           markerscale=3,
           bbox_to_anchor=(0.2,-.02),
           borderaxespad=0,
           title='Magnitude'
           #title=None
           )

    if landcover_plot_fn != None:
        fig.savefig(landcover_plot_fn, bbox_inches='tight')
    
    plt.close(fig)


def channel_length_plot():
    pass

if __name__ == '__main__':
    

    if compute_secondary_metrics:
        ## dask cluster and client
        with LocalCluster(n_workers=1,threads_per_worker=6,
                          memory_limit="25GB"
            ) as cluster, Client(cluster) as client:
            
            secondary_metrics_df = compute_metrics_by_catchment(nwm_catchments_fn,nwm_streams_fn, huc8s_vector_fn,
                                                                   resolutions,years,hucs,
                                                                   dem_sources,
                                                                   chunk_size,
                                                                   full_secondary_metrics,
                                                                   experiment_fn,
                                                                   temp_experiment_fn,
                                                                   hdf_key, write_debugging_files,
                                                                   save_nwm_catchments_file,
                                                                   nwm_catchments_raster_fn,
                                                                   burn_nwm_catchments,
                                                                   prepare_lulc,
                                                                   land_cover_fn)
    else: 
        secondary_metrics_df = pd.read_hdf(experiment_fn,hdf_key) # read hdf

    if run_anova:
        anova(secondary_metrics_df)

    if make_nhd_plot:
        nhd_to_3dep_plot(secondary_metrics_df,nhd_to_3dep_plot_fn)

    # RUN THIS TO GET CATCHMENTS WITH LAKE METRICS
    # secondary_metrics_df.loc[secondary_metrics_df.loc[:,'Lake'] != -9999,'MCC'].dropna()
    if make_dem_resolution_plot:
        resolution_plot(secondary_metrics_df,dem_resolution_plot_fn)

    if make_reservoir_plot:
        reservoir_plot(secondary_metrics_df,reservoir_plot_fn)

    if make_slope_plot:
        slope_plot(secondary_metrics_df,slope_plot_fn)

    if make_landcover_plot:
        landcover_plot(secondary_metrics_df,landcover_plot_fn)
