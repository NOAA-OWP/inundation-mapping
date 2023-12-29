#!/usr/bin/env python3

import os
from itertools import product, combinations
from time import time
import warnings
from functools import partial
from textwrap import wrap
import gc
from shutil import rmtree
from glob import glob
import pickle
from math import copysign

import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
import pygeohydro as gh
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_image
from tqdm import tqdm
from xrspatial.zonal import stats, crosstab, apply
from dask.dataframe.multi import concat, merge
from dask.dataframe import read_parquet, read_csv
from dask.distributed import Client, LocalCluster
from tqdm.dask import TqdmCallback
import statsmodels.formula.api as smf
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import SequentialFeatureSelector as SFS
from sklearn_pandas import DataFrameMapper
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, MinMaxScaler
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.lines as mlines
#import ptitprince as pt
import seaborn as sns
import dask

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
                            f'dem_evals_1202_3dep_{resolution}m_predicted',
                            #f'3dep_test_1202_{resolution}m_GMS_n_12',
                            f'{year}yr','total_area_agreement.tif')
    elif source == 'nhd':
        
        return os.path.join(root_dir,'data','test_cases','ble_test_cases',
                            f'{huc}_ble','testing_versions',
                            f'dem_evals_1202_{source}_{resolution}m',
                            #'20210902_C892f8075_allBle_GMS_n_12',
                            f'{year}yr','total_area_agreement.tif')

def build_inundation_raster_data_dir(huc, resolution, year, source):
    
    if source == '3dep':

        return os.path.join(root_dir,'data','test_cases','ble_test_cases',
                            f'{huc}_ble','testing_versions',
                            f'dem_evals_1202_3dep_{resolution}m_predicted',
                            #f'3dep_test_1202_{resolution}m_GMS_n_12',
                            f'{year}yr',f'inundation_extent_{huc}.tif')
    elif source == 'nhd':
        
        return os.path.join(root_dir,'data','test_cases','ble_test_cases',
                            f'{huc}_ble','testing_versions',
                            f'dem_evals_1202_{source}_{resolution}m',
                            f'{year}yr',f'inundation_extent_{huc}.tif')

# agreement factors
#resolutions = [90, 60, 20,15,10,5,3]
resolutions = [20, 15, 10, 5, 3]
#resolutions = [10]
years = [100,500]
#years = [500]
hucs = ['12020001','12020002','12020003','12020004','12020005','12020006','12020007']
#hucs = ['12020001']
dem_sources = ['3dep','nhd']
#dem_sources = ['3dep']
base_chunk_size = 128
chunk_size = base_chunk_size * 4
crs = "EPSG:5070"
partition_size = "8MB"
#experiment_fn = os.path.join(data_dir,'experiment_data_with_60_90m.h5')
experiment_fn = os.path.join(data_dir,'experiment_data.h5')
temp_experiment_fn = os.path.join(data_dir,'TEMP_experiment_data')
hdf_key = 'data'
save_nwm_catchments_file = os.path.join(data_dir,'nwm_catchments_with_metrics_1202_with_60_90m.gpkg')
linear_models_pickle_file = os.path.join(data_dir,'linear_models.pickle')

covariates = [ 'channel_slope_perc','area_sqkm','imperviousness_perc_mean',
               'overland_roughness_mean','terrain_slope_perc_mean' ]
factors = [ 'spatial_resolution','dominant_lulc_anthropogenic_influence','magnitude',
             'Reservoir','dem_source','order_']
feature_cols = covariates + factors
anova_tol = 0.001
#target_cols = ['CSI']
target_cols = ['MCC','CSI','TPR','FAR']
#target_cols = ['MCC']
#target_cols = ['TPR']
#target_cols = ['FAR']
nhd_to_3dep_plot_fn = os.path.join(data_dir,'nhd_to_3dep_plot.png')
nwm_catchments_raster_fn = os.path.join(data_dir,'nwm_catchments','nwm_catchments_{}_{}_{}m_{}yr.tif')
dem_resolution_plot_fn = os.path.join(data_dir,'dem_resolution_3dep_plot.png')
reservoir_plot_fn = os.path.join(data_dir,'reservoir_plot.png')
slope_plot_fn = os.path.join(data_dir,'slope_plot.png')
terrain_slope_plot_fn = os.path.join(data_dir,'terrain_slope_plot.png')
orig_land_cover_fn = os.path.join(data_dir,'cover2019_lulc_1202.tif')
grouped_land_cover_fn = os.path.join(data_dir,'grouped_cover2019_lulc_1202.tif')
land_cover_fn = os.path.join(data_dir,'landcovers','land_cover_{}.tif')
terrain_slope_fn = os.path.join(data_dir,'slopes','slope_{}.tif')
imperviousness_fn = os.path.join(data_dir,'impervious','impervious_{}.tif')
landcover_plot_fn = os.path.join(data_dir,'lulc_metrics_plot.png')
grouped_landcover_plot_fn = os.path.join(data_dir,'lulc_grouped_metrics_plot.png')
nwm_catchments_joined_streams_fn = os.path.join(data_dir,'nwm_catchments_joined_with_streams.gpkg')
nwm_catchments_with_attributes_fn = os.path.join(data_dir,'nwm_catchments_with_attributes.gpkg')
catchment_level_dir = os.path.join(data_dir,'catchment_level_aggregates')
terrain_slope_parquet_fn = os.path.join(data_dir,catchment_level_dir,'terrain_slope')
land_cover_parquet_fn = os.path.join(data_dir,catchment_level_dir,'land_cover')
roughness_parquet_fn = os.path.join(data_dir,catchment_level_dir,'roughness')
imperviousness_parquet_fn = os.path.join(data_dir,catchment_level_dir,'imperviousness')
rating_curve_parquet_filename = os.path.join(data_dir,'rating_curve.parquet')
rating_curve_metrics_filename = os.path.join(data_dir,'rating_curve_metrics.parquet')
inundated_areas_parquet = os.path.join(data_dir,'inundated_areas.parquet')
rating_curve_plot_fn = os.path.join(data_dir,'rating_curves.png')

# pipeline switches 
compute_secondary_metrics = False
prepare_catchments = False
burn_nwm_catchments = False
prepare_lulc = False
prepare_terrain_slope = False
prepare_imperviousness = False
write_debugging_files = False
build_catchment_level_attributes = False
aggregate_catchment_level_attributes = False
build_secondary_metrics = False
finalize_metrics = False

rating_curves_to_parquet = False
aggregate_rating_curves = False
plot_rating_curves = False
compute_inundated_area = False

run_anova = True
add_two_way_interactions = True
make_regression_plot = False

make_nhd_plot = False
make_dem_resolution_plot = False
make_reservoir_plot = False
make_slope_plot = False
make_terrain_slope_plot = False
make_landcover_plot = False
make_grouped_landcover_plot = False
prepare_point_value_table = False
make_tukey_hsd = False
group_lulc_map = False

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
                                        24 : "Developed, High Intensity",
                                        31 : "Bare Rock/Sand/Clay",
                                        41 : "Deciduous Forest",
                                        42 : "Evergreen Forest",
                                        43 : "Mixed Forest",
                                        45 : "Shrub-Forest",
                                        46 : "Herbaceous-Forest",
                                        52 : "Shrub/Scrub",
                                        71 : "Grasslands/Herbaceous",
                                        81 : "Pasture/Hay",
                                        82 : "Cultivated Crops",
                                        90 : "Woody Wetlands",
                                        95 : "Emergent Herbaceous Wetlands"
                                      }

flip_dict = lambda d : { v:k for k,v in d.items() }

landcover_encoding_names_to_digits = flip_dict(landcover_encoding_digits_to_names)

more_anthropogenic = {21,22,23,24,82}
low_anthropogenic = {11,31,41,42,43,45,46,71,81,90,95}


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

def build_terrain_slope_raster_fn(h,r,y,s):
    return os.path.join(data_dir,'slopes',f'slopes_{r}m_{h}_{y}yr_{s}.tif')

def build_imperviousness_raster_fn(h,r,y,s):
    return os.path.join(data_dir,'impervious',f'impervious_{r}m_{h}_{y}yr_{s}.tif')

def prepare_combos(hucs,resolutions,years,sources):
    
    # prepare combinations of resolutions, hucs, and years
    combos = list(product(hucs,resolutions,years))
    
    # append source
    combos = [(h,r,y,'3dep') for h,r,y in combos]
    
    if 'nhd' in sources:
        nhd_combos = [(h,10,y,'nhd') for h,y in product(hucs,years)] 
        combos = nhd_combos + combos

    return combos


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
                                             lock=False,
                                             cache=False
                                            ).sel(band=1,drop=True) 
    

    except rasterio.errors.RasterioIOError:
        print(f'No agreement raster for {h} found. Skipping')
        agreement_raster = None
    
    return agreement_raster

def load_inundation_raster(h,r,y,s):
    
    # load agreement raster
    inundation_raster_fn = build_inundation_raster_data_dir(h,r,y,s)
    
    try:
        inundation_raster = rxr.open_rasterio(
                                             inundation_raster_fn,
                                             #chunks=True,
                                             chunks=chunk_size,
                                             mask_and_scale=True,
                                             variable='inundation',
                                             default_name='inundation',
                                             lock=False,
                                             cache=False
                                            ).sel(band=1,drop=True) 
    

    except rasterio.errors.RasterioIOError:
        print(f'No inundation raster for {h} found. Skipping')
        inundation_raster = None
    
    return inundation_raster


def load_hand_rasters(hucs,resolutions):

    h,r = combo

    src_path_template = os.path.join(root_dir,'data','outputs',
                                    f'dem_evals_1202_3dep_{r}m',
                                    f'{h}','branches','*',
                                    'rem_zeroed_masked_*.tif')
    
    branch_id = get_branch_id_for_current_file(rem_path)


def parquet_rating_curves(hucs, resolutions):
    
    combos = list(product(hucs,resolutions))
    num_of_combos = len(combos)

    # remove parquet file
    if os.path.exists(rating_curve_parquet_filename):
        try:
            os.remove(rating_curve_parquet_filename)
        except IsADirectoryError:
            rmtree(rating_curve_parquet_filename)

    
    def __aggregate_combos(combo,idx=0):

        h,r = combo

        src_path_template = os.path.join(root_dir,'data','outputs',
                                        f'dem_evals_1202_3dep_{r}m',
                                        f'{h}','branches','*',
                                        'src_full_crosswalked_*.csv')
        
        src_paths = glob(src_path_template)
        num_of_src_paths = len(src_paths)

        if idx == 0:
            append = False
        else:
            append = True

        def read_src(src_path): 
            return pd.read_csv(src_path).astype({'feature_id' : np.int64}) \
                                        .assign(huc8=h,spatial_resolution=r)

        all_srcs = [read_src(sp) for sp in src_paths]

        all_srcs = pd.concat(all_srcs)
    
        all_srcs.to_parquet(rating_curve_parquet_filename,
                            engine='fastparquet',
                            append=append 
                           )

    
    # run the first combo to create parquet file
    for idx,combo in tqdm(enumerate(combos),
                          total=num_of_combos,
                          desc='Write aggregated SRC parquet'):
        __aggregate_combos(combo,idx=idx)
    

def rating_curves_aggregation():

        srcs = pd.read_parquet(rating_curve_parquet_filename,
                               engine='fastparquet')

        srcs = srcs.rename(columns={
                                    'Volume (m3)' : 'volume',
                                    'BedArea (m2)' : 'bed_area',
                                    'SLOPE' : 'channel_slope',
                                    'Discharge (m3s-1)' : 'discharge'
                                    })

        grouping_vars = ['spatial_resolution','Stage']
        computing_vars = ['volume','bed_area','channel_slope','discharge']

        all_vars = grouping_vars + computing_vars

        srcs = srcs.loc[:,all_vars]
        srcs_gb = srcs.groupby(grouping_vars) 

        srcs_percentile = srcs_gb.quantile([0.05,0.25,0.5,0.75,0.95])

        srcs_percentile.index.names = srcs_percentile.index.names[0:2] + ['percentiles']

        if os.path.exists(rating_curve_metrics_filename):
            os.remove(rating_curve_metrics_filename)

        srcs_percentile.to_parquet(rating_curve_metrics_filename,
                                   engine='fastparquet',
                                   append=False
                                  )

def rating_curve_plot():
    
    srcs_percentile = pd.read_parquet(rating_curve_metrics_filename, engine='fastparquet')

    #stages = srcs_percentile.index.to_frame().Stage.unique()
    #for s in stages:

    variables = ['discharge','volume','bed_area']

    plotting_dict = {"disharge" : 'Discharge (CMS)',
                     "volume" : "Volume (m^3)",
                     "bed_area" : "Bed Area (m^2)",
                     "Stage" : "Stage (m)"}

    fig,axs = plt.subplots(3,1,dpi=300,figsize=(8,4),layout='tight')

    for i,(ax,var) in enumerate(zip(axs.ravel(),variables)):
        
        #xlim=(0,0.01)
        #xlim=(0,1)
        
        #if metric == 'FAR':
        #    ylim = (0,0.25)
        #else:
        #    ylim = (0.5,1)

        #ax.set_xlim(xlim)
        #ax.set_ylim(ylim)
        
        #ax.tick_params(axis='both', labelsize=12)

        if i in {2,3}:
            ax.set_xlabel("Discharge (CMS)",fontsize=12)
            #ax.set_xlabel("Channel Slope (%)",fontsize=12)
        else:
            ax.set_xlabel(None)

        if i in {0,2}:
            ax.set_ylabel('Stage (m)',fontsize=12)
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


    fig.savefig(rating_curve_plot_fn, bbox_inches='tight')
    plt.close()

    breakpoint()


def write_catchment_level_data(catchment_level_data, catchment_level_data_fn):
    
    catchment_level_data.to_parquet(catchment_level_data_fn,
                                    write_metadata_file=True,
                                    append=True,
                                    write_index=False,
                                    compute=True,
                                    engine='fastparquet')


def determine_terrain_slope_by_catchment(nwm_catchments_xr,
                                         agreement_raster,
                                         huc, resolution, year, dem_source,
                                         agg_func=['mean'],
                                         predicted_inundated_encodings=[2,3],
                                         output_parquet_fn=terrain_slope_parquet_fn): 
    
    terrain_slope_xr = rxr.open_rasterio(
                                     build_terrain_slope_raster_fn(huc,resolution,year,dem_source),
                                     chunks=chunk_size,
                                     mask_and_scale=True,
                                     variable='terrain_slope',
                                     default_name='terrain_slope',
                                     lock=False,
                                     cache=False
                                    ).sel(band=1,drop=True)
    
    # masking out dry areas
    #nwm_catchments_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),nwm_catchments_xr,np.nan)
    #terrain_slope_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),terrain_slope_xr,np.nan)

    # compute as percentage
    terrain_slope_xr = terrain_slope_xr * 100

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore",category=UserWarning)
        
        ct_dask_df_slopes = stats(nwm_catchments_xr,
                                  terrain_slope_xr,
                                  stats_funcs=agg_func,
                                  nodata_values=np.nan) \
                                   .rename(columns={ f:"terrain_slope_perc_{}".format(f) for f in agg_func}) \
                                   .rename(columns={'zone':'ID'}) \
                                   .astype({'ID' : np.int64,'terrain_slope_perc_mean': np.float64}) \
                                   .repartition(partition_size=partition_size)
    
    write_catchment_level_data(ct_dask_df_slopes, output_parquet_fn)
    

def determine_impervious_by_catchment(nwm_catchments_xr,
                                         agreement_raster,
                                         huc, resolution, year, dem_source,
                                         agg_func=['mean'],
                                         predicted_inundated_encodings=[2,3],
                                         output_parquet_fn=imperviousness_parquet_fn): 
    
    impervious_xr = rxr.open_rasterio(
                                     build_imperviousness_raster_fn(huc,resolution,year,dem_source),
                                     chunks=chunk_size,
                                     mask_and_scale=True,
                                     variable='impervious_2019',
                                     default_name='impervious_2019',
                                     lock=False,
                                     cache=False
                                    ).sel(band=1,drop=True)

    # calculate as percent
    #impervious_xr = impervious_xr / 100

    # masking out dry areas
    #nwm_catchments_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),nwm_catchments_xr,np.nan)
    #impervious_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),impervious_xr,np.nan)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore",category=UserWarning)
        
        ct_dask_df_impervious = stats(nwm_catchments_xr,
                                      impervious_xr,
                                      stats_funcs=agg_func,
                                      nodata_values=np.nan) \
                                       .rename(columns={ f:"imperviousness_perc_{}".format(f) for f in agg_func}) \
                                       .rename(columns={'zone':'ID'}) \
                                       .astype({'ID' : np.int64,'imperviousness_perc_mean': np.float64}) \
                                       .repartition(partition_size=partition_size) \

    write_catchment_level_data(ct_dask_df_impervious, output_parquet_fn)
    

def determine_dominant_landcover(nwm_catchments_xr,
                                 agreement_raster,
                                 huc, resolution, year, dem_source,
                                 agg_func=['mean'],
                                 predicted_inundated_encodings=[2,3],
                                 output_parquet_fn_1=land_cover_parquet_fn, 
                                 output_parquet_fn_2=roughness_parquet_fn): 

    land_cover_xr = rxr.open_rasterio(
                                     build_land_cover_raster_fn(huc,resolution,year,dem_source),
                                     chunks=chunk_size,
                                     mask_and_scale=True,
                                     variable='landcover',
                                     default_name='landcover',
                                     lock=False,
                                     cache=False
                                    ).sel(band=1,drop=True) 
    
    # masking out dry areas
    #nwm_catchments_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),nwm_catchments_xr,np.nan)
    #land_cover_xr = xr.where(agreement_raster.isin(predicted_inundated_encodings),land_cover_xr,np.nan)
    
    ct_dask_df_catchment_lc = crosstab(nwm_catchments_xr,land_cover_xr,nodata_values=np.nan) \
                                           .astype(np.float64) \
                                           .rename(columns={'zone':'ID'}) \
                                           .set_index('ID', drop=True) \
                                           .rename(columns=landcover_encoding_digits_to_names) \
                                           .repartition(partition_size=partition_size)

    # remove catchments with no inundation and then find max landcover count. Returns that landcover
    ct_dask_df_catchment_lc = ct_dask_df_catchment_lc.loc[(ct_dask_df_catchment_lc!=0).any(axis=1)] \
                                                     .idxmax(1) \
                                                     .rename('dominant_lulc') \
                                                     .fillna('None') \
                                                     .astype({'dominant_lulc':str})
                                                     
    # converts names to digits
    meta = pd.Series(name='dominant_lulc_digits', dtype=np.int64)
    dominant_lulc_digits = ct_dask_df_catchment_lc.apply(lambda s: landcover_encoding_names_to_digits[s],meta=meta)
    ct_dask_df_catchment_lc = ct_dask_df_catchment_lc.to_frame() \
                                                     .assign(dominant_lulc_digits=dominant_lulc_digits) \
                                                     .astype({"dominant_lulc_digits": np.int64})
    
    def assign_grouping(r):
        d = r['dominant_lulc_digits']
        if d in more_anthropogenic:
            return 'More'
        elif d in low_anthropogenic:
            return 'Less'
        else:
            return ValueError(f"Landcover, {d}, not able to group")
    
    # create column
    meta = pd.Series(name='dominant_lulc_anthropogenic_influence', dtype=str)
    dominant_lulc_anthropogenic_influence = ct_dask_df_catchment_lc.apply(assign_grouping,axis=1,meta=meta)

    ct_dask_df_catchment_lc = ct_dask_df_catchment_lc.assign( 
                                                 dominant_lulc_anthropogenic_influence=dominant_lulc_anthropogenic_influence) \
                                                     .astype({'dominant_lulc_anthropogenic_influence': str}) \
                                                     .reset_index(drop=False)
    
    """
    Index(['ID', 'dominant_lulc', 'dominant_lulc_digits',
       'dominant_lulc_anthropogenic_influence'],
    """
    write_catchment_level_data(ct_dask_df_catchment_lc, output_parquet_fn_1)
    del ct_dask_df_catchment_lc
   
    # compute catchment level mannings n value
    overland_roughness_xr = gh.pygeohydro.overland_roughness(land_cover_xr) \
                                                .chunk(chunk_size)
    
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore",category=UserWarning)
        
        ct_dask_df_roughness = stats(nwm_catchments_xr,
                                      overland_roughness_xr,
                                      stats_funcs=agg_func,
                                      nodata_values=np.nan) \
                                       .rename(columns={ f:"overland_roughness_{}".format(f) for f in agg_func}) \
                                       .rename(columns={'zone':'ID'}) \
                                       .astype({'ID' : np.int64,'overland_roughness_mean': np.float64}) \
                                       .repartition(partition_size=partition_size)
    
    # merge with catchments
    #ct_dask_df_catchment_lc = ct_dask_df_catchment_lc.merge(ct_dask_df_roughness,
    #                                                        left_index=True,
    #                                                        right_on='ID')
    
    """
    Index(['ID', 'overland_roughness_mean'], dtype='object')
    """
    write_catchment_level_data(ct_dask_df_roughness, output_parquet_fn_2)
    

def determine_inundated_area(hucs, resolutions, years, dem_sources, output_parquet=None):

    combos = prepare_combos(hucs, resolutions, years, dem_sources)

    final_combos = []
    for idx,combo in tqdm(enumerate(combos),
                          total=len(combos),
                          desc='Computing inundated areas'):
        
        inundation_raster = load_inundation_raster(*combo)
        
        resolution = inundation_raster.rio.resolution()
        pixel_area = abs(resolution[0] * resolution[1]) / (1000**2)

        inundated_area_km = ((inundation_raster > 0).sum() * pixel_area).to_numpy().item()

        final_combos.append(tuple(combo + (inundated_area_km,)))

    final_combos = pd.DataFrame.from_records(final_combos,
                                             columns=['huc8','spatial_resolution','magnitude',
                                                      'dem_source','inundated_area_km'])
    
    # overall
    fc_gb = final_combos.groupby(['dem_source','spatial_resolution'])
    mean_inundated_areas = fc_gb.mean(numeric_only=True)['inundated_area_km']
    std_inundated_areas = fc_gb.std(numeric_only=True)['inundated_area_km']
    
    print("Inundated areas (km2) by spatial resolution for 3DEP derived FIMs across HUC8s and DEM sources")
    print("Mean:",mean_inundated_areas)
    print("Std:",std_inundated_areas)

    if os.path.exists(output_parquet):
        os.remove(output_parquet)

    final_combos.to_parquet(output_parquet,
                            engine='fastparquet',
                            append=False
                           )
    
        




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
                                  prepare_terrain_slope=False,
                                  prepare_imperviousness=False,
                                  land_cover_fn=None):
    
    
    def __prepare_catchments():
        
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
        
        # make reservoir bool
        nwm_streams = nwm_streams.assign(
                                Reservoir=np.where(nwm_streams.loc[:,'Lake'] != -9999,'Reservoir', 'Not Reservoir')) 

        nwm_catchments.drop(columns={'AreaSqKM', 'Shape_Length', 'Shape_Area'},inplace=True)

        stream_cols = ['ID','order_','Lake','gages','Slope', 'Length','Reservoir']

        nwm_catchments = nwm_catchments.merge(nwm_streams.loc[:,stream_cols],on='ID')

        nwm_catchments['channel_slope_perc'] = nwm_catchments.loc[:,'Slope'] * 100
                                       
        nwm_catchments.to_file(nwm_catchments_joined_streams_fn,driver='GPKG',index=False)
        """
                   .astype({'huc8': str,
                            'area_sqkm': np.float64,
                            'ID' : np.int64,
                            'mainstem' : np.int32,
                            'order_' : np.float64,
                            'Lake' : np.int64,
                            'gages' : np.int64,
                            'Reservoir' : str}) \
        """
        
        return nwm_catchments
    

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
                                            BLOCKXSIZE=base_chunk_size,
                                            BLOCKYSIZE=base_chunk_size,
                                            lock=False,dtype=rasterio.int32,
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
                                             lock=False,
                                             cache=False
                                            ).sel(band=1,drop=True) \
                                         .rio.reproject_match(agreement_raster) \
                                         .rio.to_raster(build_land_cover_raster_fn(h,r,y,s),
                                                        tiled=True,windowed=True,
                                                        BLOCKXSIZE=base_chunk_size,
                                                        BLOCKYSIZE=base_chunk_size,
                                                        lock=False,dtype=rasterio.int32,
                                                        compress='lzw')
    
    def __prepare_terrain_slope():
        
        # loop over every combination of resolutions and magnitude years
        for idx,(h,r,y,s) in tqdm(enumerate(combos),
                                desc='Preparing Terrain Slopes',
                                total=num_of_combos):
            
            agreement_raster = load_agreement_raster(h,r,y,s)
            
            terrain_slope_xr = rxr.open_rasterio(
                                             terrain_slope_fn.format(h),
                                             chunks=chunk_size,
                                             mask_and_scale=True,
                                             variable='terrain_slope',
                                             default_name='terrain_slope',
                                             lock=False,
                                             cache=False
                                            ).sel(band=1,drop=True) \
                                             .rio.reproject_match(agreement_raster) \
                                             .rio.to_raster(build_terrain_slope_raster_fn(h,r,y,s),
                                                            tiled=True,windowed=True,
                                                            BLOCKXSIZE=base_chunk_size,
                                                            BLOCKYSIZE=base_chunk_size,
                                                            lock=False,dtype=rasterio.float32,
                                                            compress='lzw')

    def __prepare_imperviousness():
        
        # loop over every combination of resolutions and magnitude years
        for idx,(h,r,y,s) in tqdm(enumerate(combos),
                                desc='Preparing Imperviousness',
                                total=num_of_combos):
            
            agreement_raster = load_agreement_raster(h,r,y,s)
            
            imperviousness_xr = rxr.open_rasterio(
                                             imperviousness_fn.format(h),
                                             chunks=chunk_size,
                                             mask_and_scale=True,
                                             variable='impervious_2019',
                                             default_name='impervious_2019',
                                             lock=False,
                                             cache=False
                                            ).sel(band=1,drop=True) \
                                             .rio.reproject_match(agreement_raster) \
                                             .rio.to_raster(build_imperviousness_raster_fn(h,r,y,s),
                                                            tiled=True,windowed=True,
                                                            BLOCKXSIZE=base_chunk_size,
                                                            BLOCKYSIZE=base_chunk_size,
                                                            lock=False,dtype=rasterio.float32,
                                                            compress='lzw')

    def __build_catchment_level_landcover():
    
        # remove temp experiment file
        rmtree(land_cover_parquet_fn,ignore_errors=True)
        rmtree(roughness_parquet_fn,ignore_errors=True)
    
        for idx,(h,r,y,s) in tqdm(enumerate(catchment_level_combos),
                                desc='Catchment level landcover',
                                total=num_of_catchment_level_combos):
            
            nwm_catchments_xr = rxr.open_rasterio(nwm_catchments_raster_fn.format(s,h,r,y),
                                                  chunks=chunk_size,
                                                  mask_and_scale=True,
                                                  variable='ID',
                                                  default_name='nwm_catchments',
                                                  lock=False,
                                                  cache=False
                                                 ).sel(band=1,drop=True) 
            
            agreement_raster = load_agreement_raster(h,r,y,s)

            # determines dominant inundated landcover by catchment
            ct_dask_df_catchment_lc = determine_dominant_landcover(nwm_catchments_xr,
                                                                    agreement_raster,
                                                                    h,r,y,s,
                                                                    agg_func=['mean'],
                                                                    predicted_inundated_encodings=[2,3])

            # merge in landcovers
            #secondary_metrics_df = merge(secondary_metrics_df,ct_dask_df_catchment_lc,
            #                             how='left', on='ID')
    
    def __build_catchment_level_terrain_slope():
        
        # remove temp experiment file
        rmtree(terrain_slope_parquet_fn,ignore_errors=True)

        for idx,(h,r,y,s) in tqdm(enumerate(catchment_level_combos),
                                desc='Catchment level terrain slope df',
                                total=num_of_catchment_level_combos):
            
            nwm_catchments_xr = rxr.open_rasterio(nwm_catchments_raster_fn.format(s,h,r,y),
                                                  chunks=chunk_size,
                                                  mask_and_scale=True,
                                                  variable='ID',
                                                  default_name='nwm_catchments',
                                                  lock=False,
                                                  cache=False
                                                 ).sel(band=1,drop=True) 
            
            agreement_raster = load_agreement_raster(h,r,y,s)

            
            # aggregates slope by catchment
            ct_dask_df_slopes = determine_terrain_slope_by_catchment(nwm_catchments_xr,
                                                                     agreement_raster,
                                                                     h,r,y,s,
                                                                     agg_func=['mean'],
                                                                     predicted_inundated_encodings=[2,3])

            # merge in slopes
            #secondary_metrics_df = merge(secondary_metrics_df,ct_dask_df_slopes,
            #                             how='left', on='ID')
    
    def __build_catchment_level_imperviousness():
        
        # remove temp experiment file
        rmtree(imperviousness_parquet_fn,ignore_errors=True)
    
        for idx,(h,r,y,s) in tqdm(enumerate(catchment_level_combos),
                                desc='Catchment level imperviousness df',
                                total=num_of_catchment_level_combos):
            
            nwm_catchments_xr = rxr.open_rasterio(nwm_catchments_raster_fn.format(s,h,r,y),
                                                  chunks=chunk_size,
                                                  mask_and_scale=True,
                                                  variable='ID',
                                                  default_name='nwm_catchments',
                                                  lock=False,
                                                  cache=False
                                                 ).sel(band=1,drop=True) 
            
            agreement_raster = load_agreement_raster(h,r,y,s)

            # aggregate imperviousness by catchment
            ct_dask_df_impervious = determine_impervious_by_catchment(nwm_catchments_xr,
                                                                      agreement_raster,
                                                                      h,r,y,s,
                                                                      agg_func=['mean'],
                                                                      predicted_inundated_encodings=[2,3])
            
            # merge in impervious
            #secondary_metrics_df = merge(secondary_metrics_df,ct_dask_df_impervious,
            #                             how='left', on='ID')
            

    def __aggregate_catchment_level_attributes():

        input_parquets = [ land_cover_parquet_fn,
                           imperviousness_parquet_fn,
                           terrain_slope_parquet_fn,
                           roughness_parquet_fn
                         ]

        nwm_catchments_with_attributes = nwm_catchments.copy()
        for ip in input_parquets:
            ip_df = read_parquet(ip,
                                 engine='fastparquet'
                                ).compute()

            nwm_catchments_with_attributes = nwm_catchments_with_attributes.merge(ip_df, on='ID')
        
        nwm_catchments_with_attributes.to_file(nwm_catchments_with_attributes_fn,driver='GPKG',index=False)


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
                                                  lock=False,
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
                                        .repartition(partition_size=partition_size)
            
            #ct_dask_df.visualize(filename=os.path.join(data_dir,'dask_graph.png'),optimize_graph=True)
            #breakpoint()
            
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
                secondary_metrics_df = secondary_metrics_df.assign(Waterbody=lambda r: 0) \
                                                           .astype({'Waterbody' : np.int64})
            else:
                secondary_metrics_df['Waterbody'] = secondary_metrics_df['Waterbody'].fillna(0) \
                                                                                     .astype({'Waterbody' : np.int64})
            # dropnas
            secondary_metrics_df = secondary_metrics_df.dropna(subset=full_secondary_metrics,how='any') \
                                                       .reset_index(drop=True) \
            
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
        
        nwm_catchments_with_attributes = gpd.read_file(nwm_catchments_with_attributes_fn)

        # this won't be necessary once agreement maps are recomputed
        secondary_metrics_df['Waterbody'] = secondary_metrics_df['Waterbody'].fillna(0)
        
        nwm_catchments_with_metrics = nwm_catchments_with_attributes.merge(secondary_metrics_df.drop(columns='huc8'),on='ID')

        # join with nwm streams and convert datatypes
        secondary_metrics_df = nwm_catchments_with_metrics.drop(columns='geometry') \
                                                          .dropna(subset=full_secondary_metrics,how='any') \
                                                          .drop_duplicates() \
                                                          .astype({ 'huc8': 'category',
                                                                    'spatial_resolution': np.float64,
                                                                    'area_sqkm': np.float64,
                                                                    'magnitude': 'category',
                                                                    'ID' : 'category',
                                                                    'mainstem' : 'category',
                                                                    'order_' : np.int32,
                                                                    'TP' : np.int64,
                                                                    'FP' : np.int64,
                                                                    'TN' : np.int64,
                                                                    'FN' : np.int64,
                                                                    'Waterbody' : np.int64,
                                                                    'Total Samples' : np.int64,
                                                                    'Lake' : 'category',
                                                                    'gages' : 'category',
                                                                    'dem_source': 'category',
                                                                    'dominant_lulc': 'category',
                                                                    'dominant_lulc_digits': 'category',
                                                                    'terrain_slope_perc_mean': np.float64,
                                                                    'imperviousness_perc_mean': np.float64,
                                                                    'dominant_lulc_anthropogenic_influence': 'category',
                                                                    'Reservoir' : 'category'})

        # saving nwm catchments with metrics
        if save_nwm_catchments_file:
            nwm_catchments_with_metrics.astype({'dominant_lulc_anthropogenic_influence':str}) \
                                       .to_file(save_nwm_catchments_file,index=False,driver='GPKG')
        

        # save file
        if isinstance(experiment_fn,str) & isinstance(hdf_key,str):
            print(f'Writing to {experiment_fn}')
            secondary_metrics_df.to_hdf(experiment_fn,
                                           key=hdf_key,
                                           format='table',
                                           index=False)
        
        return(secondary_metrics_df)

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

    if prepare_catchments:
        nwm_catchments = __prepare_catchments()
    else:
        nwm_catchments = gpd.read_file(nwm_catchments_joined_streams_fn)

    if prepare_lulc:
        __prepare_lulc()

    if prepare_terrain_slope:
        __prepare_terrain_slope()
    
    if prepare_imperviousness:
        __prepare_imperviousness()
    
    # burning catchments
    if burn_nwm_catchments:
        __burn_nwm_catchments()

    if build_catchment_level_attributes:

        catchment_level_combos = []
        for h,r,y,s in combos:
            if (r == 10) & (y == 500) & (s == '3dep'):
                catchment_level_combos += [(h,r,y,s)]
       
        num_of_catchment_level_combos = len(catchment_level_combos)
        
        __build_catchment_level_terrain_slope()
        __build_catchment_level_landcover()
        __build_catchment_level_imperviousness()

    if aggregate_catchment_level_attributes:
        __aggregate_catchment_level_attributes()

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

    def __forward_model_selection(selected_features,tol=0.001,formula=None,prev_metric_val=None):

        remaining_features = set(selected_features)

        rsquared_values = []
        while remaining_features:
            
            results = []
            for sf in remaining_features:
                
                if formula == None:
                    formula_try = f"{target_col} ~ {sf}"
                else:
                    formula_try = formula + f" + {sf}"
                
                linear_model = ols(formula_try, 
                                   data=secondary_metrics_df).fit()
                print(f"R2 for {formula_try}: {np.round(linear_model.aic,4)}/{np.round(linear_model.rsquared_adj,4)}")
                results += [(sf,linear_model.rsquared)]
                #results += [(sf,linear_model.aic)]

            results = sorted(results,key=lambda a: a[1],reverse=True)
            #results = sorted(results,key=lambda a: a[1],reverse=False)
            
            try:
                lead_factor = results.pop(0)[0]
            except IndexError:
                break
            
            if formula == None:
                prop_formula = f"{target_col} ~ {lead_factor}"
            else:
                prop_formula = formula + f" + {lead_factor}"
            
            prop_linear_model = ols(prop_formula, 
                                    data=secondary_metrics_df).fit()
            
            if prev_metric_val == None:
                prev_metric_val = 0

            delta = prop_linear_model.rsquared_adj - prev_metric_val
            prev_metric_val = linear_model.rsquared_adj
            #delta = prop_linear_model.aic - prev_metric_val
            #prev_metric_val = linear_model.aic

            #print(prop_linear_model.aic,prev_metric_val,delta,tol)
            #if (abs(delta) < tol) | (delta >= 0):
            if delta < tol:
                print(f"BROKEN: AIC/R2 for {formula}: {np.round(linear_model.aic,4)}/{np.round(linear_model.rsquared_adj,4)} | delta: {delta}")
                linear_model = ols(formula,data=secondary_metrics_df).fit()
                #rsquared_values += [linear_model.rsquared]
                return(linear_model, rsquared_values)
            else:
                linear_model = prop_linear_model
                formula = prop_formula
                rsquared_values += [linear_model.rsquared]
                print(f"Locked in Change AIC/R2 for {formula}: {np.round(linear_model.aic,4)}/{np.round(linear_model.rsquared_adj,4)} | delta: {delta}")

            remaining_features.remove(lead_factor)

        return(linear_model, rsquared_values)
    
    
    secondary_metrics_df = secondary_metrics_df.loc[:,target_cols + feature_cols] #\
                                               #.astype({'order_' : np.int32})

    # drop resolutions not in list
    secondary_metrics_df = secondary_metrics_df.loc[
        secondary_metrics_df.loc[:,'spatial_resolution'].isin(resolutions),:
    ]

    # scale numerics
    scaler = MinMaxScaler(copy=True)
    secondary_metrics_df = pd.concat([ pd.DataFrame(scaler.fit_transform(secondary_metrics_df.loc[:,covariates].to_numpy()),
                                         columns=covariates),
                                       secondary_metrics_df.loc[:,factors + target_cols]],
                                  axis=1)

    # remove dem source
    secondary_metrics_df = secondary_metrics_df.loc[secondary_metrics_df.loc[:,'dem_source'] == '3dep',:]
    feature_cols.remove('dem_source')
    factors.remove('dem_source')

    linear_models, rsq = {},{}
    for target_col in target_cols:

        # initiate dicts with list
        rsq[target_col] = []

        linear_model, rsquared = __forward_model_selection(selected_features=feature_cols,
                                                  tol=anova_tol)
        
        rsq[target_col] += rsquared
    
        # make two way interactions
        if add_two_way_interactions:

            # get updated feature_cols    
            params_list = []
            params_series = linear_model.params.drop('Intercept')
            for i in params_series.index:
                if single_split(i)[1] == 1:
                    params_list += [single_split(i)[0][0]]
                elif single_split(i)[1] == 2:
                    params_list += [single_split(i)[0][0]]
                else:
                    params_list += [double_split(i)]

            two_way = [f"{i}:{ii}" for i,ii in combinations(params_list,2)]

            linear_model, rsquared = __forward_model_selection(two_way,tol=anova_tol,formula=linear_model.model.formula,prev_metric_val=linear_model.rsquared_adj)

            linear_models[target_col] = linear_model
            rsq[target_col] += rsquared

        print(f"Final Formula: {linear_model.model.formula} | Adj-R2: {linear_model.rsquared_adj}")
        #breakpoint()

        """
        MCC ~ Reservoir + order_ + overland_roughness_mean + terrain_slope_perc_mean + imperviousness_perc_mean + channel_slope_perc + dominant_lulc_anthropogenic_influence + magnitude + area_sqkm + overland_roughness_mean:channel_slope_perc + terrain_slope_perc_mean:channel_slope_perc + order_:overland_roughness_mean + channel_slope_perc:area_sqkm + dominant_lulc_anthropogenic_influence:overland_roughness_mean + Reservoir:overland_roughness_mean + Reservoir:order_ + order_:area_sqkm + order_:terrain_slope_perc_mean + order_:channel_slope_perc + imperviousness_perc_mean:channel_slope_perc + terrain_slope_perc_mean:area_sqkm + overland_roughness_mean:terrain_slope_perc_mean + dominant_lulc_anthropogenic_influence:imperviousness_perc_mean + order_:imperviousness_perc_mean + dominant_lulc_anthropogenic_influence:terrain_slope_perc_mean + order_:dominant_lulc_anthropogenic_influence | Adj-R2: 0.3238388995539966

        CSI ~ order_ + Reservoir + channel_slope_perc + terrain_slope_perc_mean + area_sqkm + imperviousness_perc_mean + magnitude + overland_roughness_mean + order_:area_sqkm + order_:overland_roughness_mean + order_:channel_slope_perc + order_:Reservoir + Reservoir:overland_roughness_mean + order_:terrain_slope_perc_mean + channel_slope_perc:terrain_slope_perc_mean + order_:imperviousness_perc_mean + channel_slope_perc:imperviousness_perc_mean + channel_slope_perc:area_sqkm + terrain_slope_perc_mean:area_sqkm | Adj-R2: 0.315705019504620

        TPR ~ Reservoir + order_ + area_sqkm + imperviousness_perc_mean + magnitude + spatial_resolution + channel_slope_perc + dominant_lulc_anthropogenic_influence + Reservoir:order_ + area_sqkm:channel_slope_perc + order_:area_sqkm + order_:imperviousness_perc_mean + order_:channel_slope_perc + imperviousness_perc_mean:channel_slope_perc | Adj-R2: 0.20906931285534258

        FAR ~ channel_slope_perc + terrain_slope_perc_mean + order_ + area_sqkm + overland_roughness_mean + spatial_resolution + dominant_lulc_anthropogenic_influence + Reservoir + channel_slope_perc:area_sqkm + order_:channel_slope_perc + dominant_lulc_anthropogenic_influence:overland_roughness_mean + order_:terrain_slope_perc_mean + order_:area_sqkm + Reservoir:area_sqkm + order_:overland_roughness_mean + order_:dominant_lulc_anthropogenic_influence + channel_slope_perc:overland_roughness_mean + channel_slope_perc:terrain_slope_perc_mean + dominant_lulc_anthropogenic_influence:terrain_slope_perc_mean + channel_slope_perc:spatial_resolution + order_:Reservoir | Adj-R2: 0.33125429387274563

        MCC ~ Reservoir + terrain_slope_perc_mean + overland_roughness_mean + order_ + channel_slope_perc + imperviousness_perc_mean + dominant_lulc_anthropogenic_influence + magnitude + area_sqkm + overland_roughness_mean:channel_slope_perc + terrain_slope_perc_mean:channel_slope_perc + Reservoir:overland_roughness_mean + dominant_lulc_anthropogenic_influence:overland_roughness_mean + Reservoir:order_ + order_:channel_slope_perc + channel_slope_perc:imperviousness_perc_mean + channel_slope_perc:area_sqkm + terrain_slope_perc_mean:area_sqkm + terrain_slope_perc_mean:order_ + dominant_lulc_anthropogenic_influence:imperviousness_perc_mean + terrain_slope_perc_mean:overland_roughness_mean + Reservoir:dominant_lulc_anthropogenic_influence + overland_roughness_mean:order_ + order_:area_sqkm | Adj-R2: 0.2915453123899441

        CSI ~ order_ + Reservoir + channel_slope_perc + terrain_slope_perc_mean + area_sqkm + imperviousness_perc_mean + magnitude + dominant_lulc_anthropogenic_influence + order_:channel_slope_perc + Reservoir:order_ + channel_slope_perc:terrain_slope_perc_mean + order_:terrain_slope_perc_mean + Reservoir:terrain_slope_perc_mean + order_:area_sqkm + Reservoir:area_sqkm + Reservoir:channel_slope_perc + order_:imperviousness_perc_mean + terrain_slope_perc_mean:area_sqkm | Adj-R2: 0.27880253407255107
        """

    breakpoint()
    save_models = {'linear_models' : linear_models, 'rsq' : rsq}
    
    if os.path.exists(linear_models_pickle_file):
        os.remove(linear_models_pickle_file)

    # Store data (serialize)
    with open(linear_models_pickle_file, 'wb') as handle:
        pickle.dump(save_models, handle, protocol=pickle.HIGHEST_PROTOCOL)


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

def single_split(s,c='['): 
    split = s.split(c)
    return split,len(split)

def double_split(s):
    return single_split(s)[0][0] + ':' + single_split(single_split(s)[0][1],':')[0][1]

def plot_regression(linear_models_pickle_file):

    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Corr. Coeff.",'CSI':"Critical Success Index",
                    'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}

    # RENAME COLUMNS TO SOMETHING PLOT FRIENDLY
    def produce_y_labels(param_names):
        
        full_names_dict = {  "channel_slope_perc" : 'Channel Slope',
                             "terrain_slope_perc_mean" : 'Terrain Slope',
                             "overland_roughness_mean" : 'Overland Roughness',
                             "area_sqkm" : 'Area',
                             "dominant_lulc_anthropogenic_influence" : "Anthropogenic Influence of LULC",
                             "spatial_resolution" : "Spatial Resolution",
                             "magnitude" : "Magnitude",
                             "imperviousness_perc_mean" : "Imperviousness",
                             "Reservoir" : "Reservoir",
                             "order_" : "Order"
                           }                       
        
        full_names_dict = {  "channel_slope_perc" : 'CS',
                             "terrain_slope_perc_mean" : 'TS',
                             "overland_roughness_mean" : 'OR',
                             "area_sqkm" : 'A',
                             "dominant_lulc_anthropogenic_influence" : "LC",
                             "spatial_resolution" : "SR",
                             "magnitude" : "M",
                             "imperviousness_perc_mean" : "IM",
                             "Reservoir" : "R",
                             "Intercept" : "IN",
                             "order_" : "SO"
                           }                       

        full_names = []
        for pm in param_names:
            
            if len(pm.split(':')) == 1:
                full_names += [full_names_dict[pm]]
            else:
                full_names += [":".join([full_names_dict[p] for p in pm.split(':')])]
                            
        return full_names

    # Load data (deserialize)
    with open(linear_models_pickle_file, 'rb') as handle:
        unserialized_data = pickle.load(handle)

    linear_models = unserialized_data['linear_models']
    rsquared_values = unserialized_data['rsq']
    del unserialized_data

    breakpoint()
    fig,axs = plt.subplots(1,4,dpi=300,figsize=(8,11),layout='tight')
    plt.subplots_adjust(wspace=0.0)

    ax_handles,ax_labels = [],[]
    prev_sig_levels = set()

    for i,(ax,metric) in tqdm(enumerate(zip(axs.ravel(),metrics)),
                              desc='Plotting regression models',
                              total=len(metrics)):

        # sort parameters and their values based on adjusted r-squared
        explanatory_vars = ['Intercept'] + linear_models[metric].model.formula.split(' ~ ')[1].split(' + ')
        
        params_series = linear_models[metric].params
        if metric == 'TPR':
            params_series.drop(index=['dominant_lulc_anthropogenic_influence[T.More]:Reservoir[Not Reservoir]',
                                'overland_roughness_mean:Reservoir[Not Reservoir]'],
                                errors='ignore',
                                inplace=True)

        params_list = []
        for i in params_series.index:
            #if single_split(i)[0][0] == 'dominant_lulc_anthropogenic_influence:Reservoir':
            #    breakpoint()
            if single_split(i)[1] == 1:
                params_list += [single_split(i)[0][0]]
            elif single_split(i)[1] == 2:
                if ':' in single_split(i)[0][1]:
                    params_list += [single_split(i)[0][0] + ':' + single_split(single_split(i)[0][1],':')[0][1]]
                else:
                    params_list += [single_split(i)[0][0]]
            else:
                params_list += [double_split(i)]
        
        params_series.index = params_list
        params_series = params_series.loc[explanatory_vars]

        # added
        params_series = params_series.groupby(params_series.index).median().loc[explanatory_vars]
        
        params_confint = linear_models[metric].conf_int()
        if metric == 'TPR':
            params_confint.drop(index=['dominant_lulc_anthropogenic_influence[T.More]:Reservoir[Not Reservoir]',
                                'overland_roughness_mean:Reservoir[Not Reservoir]'],
                                errors='ignore',
                                inplace=True)

        params_list = []
        for i in params_confint.index:
            if single_split(i)[1] == 1:
                params_list += [single_split(i)[0][0]]
            elif single_split(i)[1] == 2:
                if ':' in single_split(i)[0][1]:
                    params_list += [single_split(i)[0][0] + ':' + single_split(single_split(i)[0][1],':')[0][1]]
                else:
                    params_list += [single_split(i)[0][0]]
            else:
                params_list += [double_split(i)]

        params_confint.index = params_list
        params_confint = params_confint.loc[explanatory_vars]

        params_confint = params_confint.loc[:,1] - params_series

        # added
        params_confint = params_confint.groupby(params_confint.index).median().loc[explanatory_vars]

        # pvalues
        pvalues = linear_models[metric].pvalues
        if metric == 'TPR':
            pvalues.drop(index=['dominant_lulc_anthropogenic_influence[T.More]:Reservoir[Not Reservoir]',
                                'overland_roughness_mean:Reservoir[Not Reservoir]'],
                                errors='ignore',
                                inplace=True)

        params_list = []
        for i in pvalues.index:
            if single_split(i)[1] == 1:
                params_list += [single_split(i)[0][0]]
            elif single_split(i)[1] == 2:
                if ':' in single_split(i)[0][1]:
                    params_list += [single_split(i)[0][0] + ':' + single_split(single_split(i)[0][1],':')[0][1]]
                else:
                    params_list += [single_split(i)[0][0]]
            else:
                params_list += [double_split(i)]
        
        pvalues.index = params_list
        pvalues = pvalues.loc[explanatory_vars]

        # added
        pvalues = pvalues.groupby(pvalues.index).median().loc[explanatory_vars]

        # make integers for each parameter for y axis
        y = np.array(list(reversed(range(len(params_series)))))
        x = np.array(list(params_series))
        err =  np.array(list(params_confint))
        pval = np.array(list(pvalues))
        rsq = np.concatenate( [np.array([None]),np.array(list(rsquared_values[metric]))])
        
        y_labels = produce_y_labels(params_confint.index)
              
        ax.set_yticks(y)
        ax.set_yticklabels(y_labels,rotation=45,ha='right',fontsize=12)
        ax.set_xlim(-2,2)
        ax.set_xticks([-2,0,2],minor=False)
        ax.set_xticklabels([-2,0,2],fontsize=12)
        ax.set_xticks([-1,0,1],minor=True)
        ax.xaxis.grid(True, alpha=0.75, lw=0.75, which='major')

        color_idx = (x > 0).astype(np.int32)
        colors = [['r','g'][c] for c in color_idx]

        def size_shape_idx_func(pval):
            if pval >= 0.05:
                i = 0
            if pval < 0.05:
                i = 1
            if pval < 0.01:
                i = 2
            if pval < 0.001:
                i = 3

            size = [50,45,42,45][i]
            shape = [".","p","^","*"][i]
            sig_level = [">= 0.05","< 0.05","< 0.01","< 0.001"][i]

            return size,shape,sig_level
        
        sizes_and_shapes = [size_shape_idx_func(p) for p in pval]
        sizes,shapes,sig_level = list(zip(*sizes_and_shapes))

        offset = 0.22
        for i,j,s,c,m,l in zip(x,y,sizes,colors,shapes,sig_level):
            
            if c =='g':
                slope = '+'
            else:
                slope = '-'

            label = f'{slope} slope, p-value {l}'
            hdl = ax.scatter(i,j, s=s,c=c,marker=m, label=label)
            if i < 0:
                ha = 'left'
                ii = i + offset
            else:
                ha = 'right'
                ii = i - offset
            
            if abs(i) >= 2:            
                ii =  copysign(1,i) * (2 - offset)    
            
            ax.text(ii,j,np.round(i,3),ha=ha,va='center',fontsize=11) # adds point labels
            
            if (c,l) not in prev_sig_levels:
                prev_sig_levels.add((c,l))
                ax_handles += [hdl]
                ax_labels += [label]

        secax = ax.twiny()

        label = 'Coefficient of Determination ($R^2$)'
        rsq_handle = secax.plot(rsq,y,linestyle='-',marker=None,c='b',label=label)

        secax.set_xlim(0,0.34)
        secax.set_xticks([0,0.17,0.34],minor=False)
        secax.set_xticklabels([0,0.17,.34],fontsize=12)
        secax.set_xticks([0.085,0.17],minor=True)

        ax.set_title(metric,fontsize=16,pad=40)
        fig.text(0.5,0,'Parameter Value',ha='center',fontsize=12)
        fig.text(0.5,0.95,'Coefficient of Determination ($R^2$)',ha='center',fontsize=12)

        txtstr = '\n'.join(['IN = Intercept','R = Reservoir','TS = Terrain Slope',
                            'OR = Overland Roughness','CS = Channel Slope', 'SO = Stream Order'])
        fig.text(0.02,-.001,txtstr,ha='left',va='top',fontsize=12,
                bbox={'alpha':0.05,"facecolor":'none',"boxstyle":'round'})

        txtstr = '\n'.join(['M = Magnitude','LC = Landcover (LULC)','A = Catchment Area',
                            'SR = Spatial Resolution','IM = Imperviousness'])
        fig.text(0.98,-.001,txtstr,ha='right',va='top',fontsize=12,
                 bbox={'alpha':0.05,"facecolor":'none',"boxstyle":'round'})
        
        if metric == 'MCC':
            px,py = (0,-0.80)
        elif metric == 'CSI':
            px,py = (0,-0.70)
        elif metric == 'TPR':
            px,py = (0,-0.50)
        elif metric == 'FAR':
            px,py = (0,-0.65)

        ax.text(px,py,'Final $R^{2}$:' + f' {np.round(rsq[-1],4)}',ha='center',va='center',c='b',fontsize=10)

    ax_labels += [label]
    ax_handles += rsq_handle
    lgd = fig.legend(handles=ax_handles,
                       labels=ax_labels,
                       loc='lower center',
                       frameon=True,
                       framealpha=0.75,
                       fontsize=12,
                       title_fontsize=14,
                       borderpad=0.25,
                       markerscale=1.5,
                       bbox_to_anchor=(0.515,-0.14),
                       borderaxespad=0,
                       title=None
                       )
    
    plt.tight_layout(w_pad=0)
    fig.savefig(os.path.join(data_dir,f'regression_plot.png'), #bbox_extra_artists=(lgd,),
                bbox_inches='tight')
    

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

    add_suffix = lambda suffix : [m + suffix for m in metrics] 

    prepared_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                           .set_index(['huc8','magnitude','spatial_resolution','dem_source','ID']) \
                                           .sort_index() \
                                           .drop_duplicates()
    
    # get 3dep only samples
    metrics_3dep = prepared_metrics.xs('3dep',level="dem_source") \
                                   .xs(10,level='spatial_resolution') \
                                   .loc[:,metrics] \
                                   .drop_duplicates()

    # get nhd only samples
    metrics_nhd = prepared_metrics.xs('nhd',level="dem_source") \
                                   .xs(10,level='spatial_resolution') \
                                   .loc[:,metrics] \
                                   .drop_duplicates()
    
    # align indices for 3dep and nhd samples
    all_metrics = metrics_3dep.join(metrics_nhd,how='inner',lsuffix='_3dep',rsuffix='_nhd') \
                              .dropna(how='any') \
                              .drop_duplicates()

    # redivide across dem source
    metrics_3dep = all_metrics.loc[:,add_suffix('_3dep')]
    metrics_nhd = all_metrics.loc[:,add_suffix('_nhd')]

    # homogenize column names for differencing
    metrics_3dep.columns = range(len(metrics))
    metrics_nhd.columns = range(len(metrics))

    # differencing
    difference = metrics_3dep - metrics_nhd

    # remakes column names
    metrics_3dep.columns = add_suffix('_3dep')
    metrics_nhd.columns = add_suffix('_nhd')
    difference.columns = add_suffix('_diff')

    # rejoins differences
    all_metrics = difference.join(metrics_3dep,how='left',lsuffix='_diff',rsuffix='_3dep') \
                            .join(metrics_nhd,how='left') \
                            .drop_duplicates() \
                            .dropna(how='any')

    fig,axs = plt.subplots(4,2,dpi=300,figsize=(5,8),layout='tight')

    mags = [100,100,100,100,500,500,500,500]
    metrics = metrics * 2
    for i,(ax,metric,mag) in enumerate(zip(axs.ravel('F'),metrics,mags)):

        #sorted_metrics = all_metrics.sort_values(metric+'_diff',ascending=False) \
        sorted_metrics = all_metrics.xs(mag,level='magnitude')
        
        diff = (sorted_metrics.loc[:,metric+"_3dep"] - sorted_metrics.loc[:,metric+"_nhd"])
        if metric == 'FAR':
            improved_indices = diff <= 0
            reduced_indices = diff > 0
        else:
            improved_indices = diff >= 0
            reduced_indices = diff < 0
        
        #breakpoint()
        """
        breakpoint()
        if metric == 'FAR':
            indices_to_remove = sorted_metrics.loc[reduced_indices,:].index[(sorted_metrics.loc[reduced_indices,metric+'_3dep'] - sorted_metrics.loc[reduced_indices,metric+'_nhd']) < 0]
            sorted_metrics.drop(index=indices_to_remove,inplace=True)
        else:
            indices_to_remove = sorted_metrics.loc[reduced_indices,:].index[(sorted_metrics.loc[reduced_indices,metric+'_3dep'] - sorted_metrics.loc[reduced_indices,metric+'_nhd']) > 0]
            sorted_metrics.drop(index=indices_to_remove,inplace=True)

        breakpoint()
        if metric == 'FAR':
            improved_indices = (sorted_metrics.loc[:,metric+"_diff"]<=0).index
            reduced_indices = (sorted_metrics.loc[:,metric+"_diff"]>=0).index
        else:
            improved_indices = (sorted_metrics.loc[:,metric+"_diff"]>=0).index
            reduced_indices = (sorted_metrics.loc[:,metric+"_diff"]<=0).index
        
        ##########################################
        # GETTING RED POINTS IN POSITIVE TERRITORY
        ##########################################
        #y = range(len(sorted_metrics))
        #x = sorted_metrics.loc[:,metric+'_diff'] + sorted_metrics.loc[:,[metric+'_nhd', metric+'_3dep']].min(axis=1)
        """
        # errorbars
        #print(f"Metric employed: {metric}")
        proportion_above_zero = (sorted_metrics.loc[:,metric+'_diff']>0).sum()/len(sorted_metrics)
        #print(f"Proportion of catchments that perform better with 3dep: {proportion_above_zero}")
        median_diff = sorted_metrics.loc[:,metric+'_diff'].median()
        mean_diff = sorted_metrics.loc[:,metric+'_diff'].mean()
        std_diff = sorted_metrics.loc[:,metric+'_diff'].std()
        #print(f"Median, mean, and std improvements: {median_diff} | {mean_diff} | {std_diff}")
        
        # TRY:
        def assign_color(series,metric):
            color_dict = {True:'green',False:'red'}
            if metric == 'FAR': color_dict = {False:'green',True:'red'}

            return (series >=0 ).apply(lambda x: color_dict[x])
        
        ax.set_aspect('equal')

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

        #ax.set_xlim(metric_min,metric_max)
        #ax.set_ylim(metric_min,metric_max)

        ax.set_xlim(0,1)
        ax.set_ylim(0,1)
        ax.set_xticks([0,0.5,1])
        ax.set_yticks([0,0.5,1])
        
        #ax.set_title(metric_dict[metric]+"\n"+"("+metric+")",fontsize=15)
        
        if i in {3,7}: 
            ax.set_xlabel("NHDPlusHR DEM",fontsize=12)
            ax.tick_params(axis='x', labelsize=12)
        else:
            ax.set(xticklabels=[])
        
        if i in {0,1,2,3}:
            ax.set_ylabel(f"{metric_dict[metric]}" +"\n" + f"({metric})"+"\n"+"3DEP DEM",fontsize=10)
            ax.tick_params(axis='y', labelsize=12)
        else:
            ax.set(yticklabels=[])

        if i in {0,4}:
            ax.set_title(f"{mag} yr Magnitude",fontsize=13)

        """
        if metric == 'FAR':
            ax.text(0.01,0.94,f'Mean: {np.round(mean_diff,3)}',transform=ax.transAxes,fontsize=8,weight="bold")
            ax.text(0.01,0.88,f'Std: {np.round(std_diff,3)}',transform=ax.transAxes,fontsize=8,weight="bold")
            ax.text(0.01,0.82,f'Perc.<0: {np.round((1-proportion_above_zero)*100,1)}%',transform=ax.transAxes,fontsize=8,weight="bold")
        else:
            ax.text(0.37,0.16,f'Mean: {np.round(mean_diff,3)}',transform=ax.transAxes,fontsize=8,weight="bold")
            ax.text(0.37,0.1,f'Std: {np.round(std_diff,3)}',transform=ax.transAxes,fontsize=8,weight="bold")
            ax.text(0.37,0.04,f'Perc.>0: {np.round(proportion_above_zero*100,1)}%',transform=ax.transAxes,fontsize=8,weight="bold")
        """
    # code on how to plot significant linear model parameters by normalized values

    lgd = fig.legend(handles=[improvement,reduction],
                    labels=['Improvement or no change','Reduction'],
                    loc='lower center',
                    frameon=True,
                    framealpha=0.75,
                    fontsize=9,
                    title_fontsize=11,
                    borderpad=0.25,
                    markerscale=3,
                    bbox_to_anchor=(0.5,-0.06),
                    borderaxespad=0,
                    title=r"Metric Value Difference (3DEP - NHDPlusHR DEM)"
                    )

    fig.savefig(os.path.join(data_dir,f'nhd_vs_3dep.png'), bbox_extra_artists=(lgd,), bbox_inches='tight')
    plt.close(fig)


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
                                      .drop_duplicates() \
                                      .query('dem_source == "3dep"')
    
    # temp
    #all_metrics.set_index(['magnitude','spatial_resolution'],inplace=True)
    #for y in [100,500]:
    #    diff = all_metrics.loc[(y,[5,10,15,20]),metrics].mean() - all_metrics.loc[(y,3),metrics].mean() -0.001
    #    all_metrics.loc[(y,3),metrics] = all_metrics.loc[(y,3),metrics] + diff
    #all_metrics.reset_index(drop=False,inplace=True)

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
           markerscaxle=3,
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
                                      .drop_duplicates() \
                                      .query('dem_source == "3dep"')

    # threshold Lake
    all_metrics_thresholded_lakes = all_metrics.copy()
    lake_bool = all_metrics_thresholded_lakes.loc[:,'Lake'] != -9999
    all_metrics_thresholded_lakes.loc[:,'Lake'] = all_metrics_thresholded_lakes.loc[:,'Lake'].astype(bool)
    all_metrics_thresholded_lakes.loc[lake_bool,'Lake'] = True
    all_metrics_thresholded_lakes.loc[~lake_bool,'Lake'] = False
    #all_metrics_thresholded_lakes.loc[:,"Lake"] = all_metrics_thresholded_lakes.loc[:,"Lake"].astype("Category")
    
    # temp
    #all_metrics_thresholded_lakes.set_index(['Lake','spatial_resolution'],inplace=True)
    #resolutions = [3,5,10,15,20]
    #for l in [False,True]:
    #    for r in [3,5]:
    #        current_resolutions = resolutions.copy()
    #        current_resolutions.remove(r)
    #        diff = all_metrics_thresholded_lakes.loc[(l,current_resolutions),metrics].mean() - all_metrics_thresholded_lakes.loc[(l,r),metrics].mean() -0.001
    #        all_metrics_thresholded_lakes.loc[(l,r),metrics] = all_metrics_thresholded_lakes.loc[(l,r),metrics] + diff
    #all_metrics_thresholded_lakes.reset_index(drop=False,inplace=True)

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


def point_values_table(secondary_metrics_df):

    metrics = ['MCC','CSI','TPR','FAR']

    prepared_metrics = (
        secondary_metrics_df
        .dropna(subset=metrics,how='any')
        .set_index(['spatial_resolution','dem_source'])
        .sort_index()
        .drop_duplicates()
    )
    
    vals_gb = (
        prepared_metrics
        .loc[:,metrics]
        .groupby(['dem_source', 'spatial_resolution'])
    )

    vals_means = (
        vals_gb
        .mean()
        .dropna()
    )

    vals_std = (
        vals_gb
        .std()
        .dropna()
    )

    print("\nMean metrics:\n", vals_means, "\n\n")
    print("\nStd metrics:\n", vals_std, "\n\n")
    breakpoint()


def tukey_hsd_for_60_90_m(secondary_metrics_df):

    # prepare secondary metrics
    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Correlation Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    breakpoint()
    all_metrics = (
        secondary_metrics_df
        .dropna(subset=metrics,how='any')
        .drop_duplicates()
        .set_index(['dem_source','spatial_resolution'])
    )
                                      #.query('dem_source == "3dep"')
    
    resolutions = [10, 60, 90]
    #resolutions = [3, 5, 10, 15, 20, 60, 90]

    # only use spatial_resolutions in resolutions
    all_metrics = all_metrics.loc[all_metrics.loc[:,'spatial_resolution'].isin(resolutions),:]


    for metric in metrics:
        print(f"Metric: {metric}")

        # groupby median for spatial_resolution
        print(
            all_metrics
            .groupby(['dem_source', 'spatial_resolution'])
            .mean(numeric_only=True)
            .loc[:,metric].reset_index()
        )

        tukey = pairwise_tukeyhsd(
            all_metrics.loc[:,metric],all_metrics.loc[:,'spatial_resolution'],
            alpha=0.001
        )

        print(tukey)

    breakpoint()


def slope_plot(secondary_metrics_df,slope_plot_fn):
    
    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Corr. Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .drop_duplicates() \
                                      .query('dem_source == "3dep"') \
                                      .query('Lake == -9999')
    
    # convert to percentage
    #all_metrics.loc[:,'Slope'] = all_metrics.loc[:,'Slope'] * 100

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
        pts_100yr = ax.scatter(all_metrics.loc[bool_100yr,'channel_slope_perc'],
                               all_metrics.loc[bool_100yr,metric],
                               alpha=0.3,s=0.08,c='red',
                               label='Catchments: 100yr'
                              )
        
        pts_500yr = ax.scatter(all_metrics.loc[bool_500yr,'channel_slope_perc'],
                               all_metrics.loc[bool_500yr,metric],
                               alpha=0.2,s=0.08,c='blue',
                               label='Catchments: 500yr'
                              )
        
        # fit model
        linear_model_100yr = sm.RLM(all_metrics.loc[bool_100yr,metric],
                                    sm.add_constant(all_metrics.loc[bool_100yr,"channel_slope_perc"]),
                                    M=sm.robust.norms.TrimmedMean()
                                   ).fit()
        linear_model_500yr = sm.RLM(all_metrics.loc[bool_500yr,metric],
                                    sm.add_constant(all_metrics.loc[bool_500yr,"channel_slope_perc"]),
                                    M=sm.robust.norms.TrimmedMean()
                                   ).fit()

        reg_func = lambda x,lm: lm.params.const + (lm.params.channel_slope_perc * x)
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
                '{:.1E}'.format(linear_model_100yr.params.channel_slope_perc),
                fontsize=12, color=trendline_100yr_color)
        ax.text(.40,y_pval_loc,
                '{:.1E}'.format(linear_model_100yr.pvalues.channel_slope_perc),
                fontsize=12, color=trendline_100yr_color)
        
        coef_of_deter_100yr = (np.corrcoef(all_metrics.loc[bool_100yr,"channel_slope_perc"],all_metrics.loc[bool_100yr,metric])**2)[0,1]
        ax.text(0.6,ylim[0]+((ylim[1]-ylim[0])/8),
                'R2: {:.4f}'.format(coef_of_deter_100yr),
                fontsize=12, color=trendline_100yr_color)

        
        # 500yr
        ax.text(.65,y_slope_loc,
                '{:.1E}'.format(linear_model_500yr.params.channel_slope_perc),
                fontsize=12, color=trendline_500yr_color)
        ax.text(.65,y_pval_loc,
                '{:.1E}'.format(linear_model_500yr.pvalues.channel_slope_perc),
                fontsize=12, color=trendline_500yr_color)
        
        coef_of_deter_500yr = (np.corrcoef(all_metrics.loc[bool_500yr,"channel_slope_perc"],all_metrics.loc[bool_500yr,metric])**2)[0,1]
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

def terrain_slope_plot(secondary_metrics_df,terrain_slope_plot_fn):
    
    metrics = ['MCC','CSI','TPR','FAR']
    metric_dict = {'MCC': "Matthew's Corr. Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .drop_duplicates() \
                                      .query('dem_source == "3dep"') \
                                      .query('Lake == -9999')
    
    all_metrics['overland_roughness_mean'] = np.log(all_metrics['overland_roughness_mean'])

    bool_100yr = all_metrics.loc[:,'magnitude'] == 100
    bool_500yr = all_metrics.loc[:,'magnitude'] == 500

    fig,axs = plt.subplots(2,2,dpi=300,figsize=(8,8),layout='tight')

    for i,(ax,metric) in enumerate(zip(axs.ravel(),metrics)):
        
        #xlim=(0,0.01)
        xlim=(-3,-1)
        
        if metric == 'FAR':
            ylim = (-4.5,0)
        else:
            ylim = (-1,0)

        all_metrics.loc[:,metric] = np.log(all_metrics.loc[:,metric])
        all_metrics = all_metrics.dropna()
        all_metrics = all_metrics.loc[~np.isinf(all_metrics.loc[:,metric]),:]

        pts_100yr = ax.scatter(all_metrics.loc[bool_100yr,'overland_roughness_mean'],
                               all_metrics.loc[bool_100yr,metric],
                               alpha=0.3,s=0.08,c='red',
                               label='Catchments: 100yr'
                              )
        
        pts_500yr = ax.scatter(all_metrics.loc[bool_500yr,'overland_roughness_mean'],
                               all_metrics.loc[bool_500yr,metric],
                               alpha=0.2,s=0.08,c='blue',
                               label='Catchments: 500yr'
                              )
        
        # fit model
        linear_model_100yr = sm.RLM(all_metrics.loc[bool_100yr,metric],
                                    sm.add_constant(all_metrics.loc[bool_100yr,"overland_roughness_mean"]),
                                    M=sm.robust.norms.TrimmedMean()
                                   ).fit()
        linear_model_500yr = sm.RLM(all_metrics.loc[bool_500yr,metric],
                                    sm.add_constant(all_metrics.loc[bool_500yr,"overland_roughness_mean"]),
                                    M=sm.robust.norms.TrimmedMean()
                                   ).fit()

        reg_func = lambda x,lm: lm.params.const + (lm.params.overland_roughness_mean * x)
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
        
        ax.text(-1.5,y_slope_loc,'slope:',fontsize=12,color='black')
        ax.text(-1.5,y_pval_loc,'p-value:',fontsize=12,color='black')
        
        # 100yr
        ax.text(-1.5,y_slope_loc,
                '{:.1E}'.format(linear_model_100yr.params.overland_roughness_mean),
                fontsize=12, color=trendline_100yr_color)
        ax.text(-1.5,y_pval_loc,
                '{:.1E}'.format(linear_model_100yr.pvalues.overland_roughness_mean),
                fontsize=12, color=trendline_100yr_color)
        
        coef_of_deter_100yr = (np.corrcoef(all_metrics.loc[bool_100yr,"overland_roughness_mean"],all_metrics.loc[bool_100yr,metric])**2)[0,1]
        ax.text(-1.5,ylim[0]+((ylim[1]-ylim[0])/8),
                'R2: {:.4f}'.format(coef_of_deter_100yr),
                fontsize=12, color=trendline_100yr_color)

        
        # 500yr
        ax.text(-1.5,y_slope_loc,
                '{:.1E}'.format(linear_model_500yr.params.overland_roughness_mean),
                fontsize=12, color=trendline_500yr_color)
        ax.text(-1.5,y_pval_loc,
                '{:.1E}'.format(linear_model_500yr.pvalues.overland_roughness_mean),
                fontsize=12, color=trendline_500yr_color)
        
        coef_of_deter_500yr = (np.corrcoef(all_metrics.loc[bool_500yr,"overland_roughness_mean"],all_metrics.loc[bool_500yr,metric])**2)[0,1]
        ax.text(-1.5,ylim[0]+((ylim[1]-ylim[0])/14),
                'R2: {:.4f}'.format(coef_of_deter_500yr),
                fontsize=12, color=trendline_500yr_color)
        
        ax.set_xlim(xlim)
        ax.set_ylim(ylim)
        
        ax.tick_params(axis='both', labelsize=12)
        #ax.tick_params(axis='x', rotation=45)
        #ax.ticklabel_format(axis='xx',style='scientific',scilimits=(0,0))

        if i in {2,3}:
            #ax.set_xlabel("Terrain Slope"+"\n"+"(vertical/horizontal)",fontsize=12)
            ax.set_xlabel("Terrain Slope (%)",fontsize=12)
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


    fig.savefig(terrain_slope_plot_fn, bbox_inches='tight')
    plt.close()


def landcover_plot(secondary_metrics_df,landcover_plot_fn):
    
    metrics = ['MCC','CSI','TPR','FAR']
    metrics = ['FAR','TPR','CSI','MCC']
    metric_dict = {'MCC': "Matthew's Correlation Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .drop_duplicates() \
                                      .query('dem_source == "3dep"') 
                                      #.query('dominant_lulc != "Other_45"') \
                                      #.query('dominant_lulc != "Other_46"') 

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

    #all_metrics.loc[:,'dominant_lulc_digit'] = order_digits

    
    #all_metrics = all_metrics.sort_values('dominant_lulc_digit',ascending=True)

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


def grouped_landcover_plot(secondary_metrics_df,landcover_plot_fn):
    
    metrics = ['MCC','CSI','TPR','FAR']
    metrics = ['FAR','TPR','CSI','MCC']
    metric_dict = {'MCC': "Matthew's Correlation Coeff.",'CSI':"Critical Success Index",
                   'TPR':"True Positive Rate",'FAR':"False Alarm Rate"}
    
    # drop NAs and only use 3dep source
    all_metrics = secondary_metrics_df.dropna(subset=metrics,how='any') \
                                      .drop_duplicates() \
                                      .query('dem_source == "3dep"') 

    # grouping
    dominant_lulc_digits = [landcover_encoding_names_to_digits[s] for s in all_metrics.loc[:,'dominant_lulc'] ]
    all_metrics = all_metrics.assign(dominant_lulc_digits=dominant_lulc_digits)
    
    def assign_grouping(r):
        d = r['dominant_lulc_digits']
        if d in more_anthropogenic:
            return 'More'
        elif d in low_anthropogenic:
            return 'Less'
        else:
            return ValueError(f"Landcover, {d}, not able to group")
    
    all_metrics = all_metrics.assign(dominant_lulc_digits_grouped= all_metrics.apply(assign_grouping,axis=1)) \
                             .astype({'dominant_lulc_digits_grouped':'category'})
    
    fig,axs = plt.subplots(4,1,dpi=300,figsize=(6,8),layout='tight')

    for i,(ax,metric) in enumerate(zip(axs.ravel(),metrics)):
        
        ax = sns.boxplot(data=all_metrics,
                            x='dominant_lulc_digits_grouped',
                            y=metric,
                            hue='magnitude',
                            hue_order=[100,500],
                            order=['Less','More'],
                            ax=ax, 
                            palette=[mcolors.CSS4_COLORS["cornflowerblue"],
                                     mcolors.CSS4_COLORS["palegoldenrod"]],
                            linewidth=2,
                            saturation=0.75,
                           )
        
        ax.tick_params(axis='y', labelsize=10)
        ax.set_ylabel(metric_dict[metric]+"\n("+metric+")",fontsize=10)

        if i == 3:
            #ax.set_xlabel("Channel Slope"+"\n"+"(vertical/horizontal)",fontsize=12)
            ax.set_xlabel("Land Cover / Land Use\nGrouped By Two Levels of Anthropogenic Influence",fontsize=10)
            #wrapped_order_strs = [ '\n'.join(wrap(l, 17)) for l in order_strs]
            #ax.set_xticklabels([f'{s} ({d})' for s,d in zip(wrapped_order_strs,order_digits)])
            #ax.tick_params(axis='x', rotation=70)
            #plt.setp( ax.xaxis.get_majorticklabels(), rotation=45, ha="right", rotation_mode='anchor' )
            ax.tick_params(axis='x', labelsize=10)
        else:
            #ax.tick_params(axis='x', labelsize=12)
            ax.set_xticklabels([])
            ax.set_xlabel(None)

        ax.set_ylim([0,1])
        ax.legend_ = None

        # linear models
        # CONSIDER MAKING THESE LINE PLOTS AS AN INTERACTION PLOT USING STATSMODELS AND DETERMINING THE VALUES WITH LINEAR MODEL BELOW WITH TWO WAY INTERACTIONS OF GROUPED LULC AND MAGNITUDE
        # fit model
        bool_100yr = all_metrics.loc[:,'magnitude'] == 100
        linear_model_100yr = ols(f"{metric} ~ dominant_lulc_digits_grouped", 
                                 data=all_metrics.loc[bool_100yr,:]).fit()
        bool_500yr = all_metrics.loc[:,'magnitude'] == 500
        linear_model_500yr = ols(f"{metric} ~ dominant_lulc_digits_grouped", 
                                 data=all_metrics.loc[bool_500yr,:]).fit()

        reg_func = lambda x,lm: lm.params.Intercept + (lm.params.loc['dominant_lulc_digits_grouped[T.More]'] * x)
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
        
        linear_model = ols(f"{metric} ~ dominant_lulc_digits_grouped * magnitude", 
                                 data=all_metrics).fit()
        
        def print_formula_with_coefficients(model, metric, round_digits=4):
            
            formula = ""
            for i,p in model.params.items():

                pv = model.pvalues.loc[i]
                
                if i == 'dominant_lulc_digits_grouped[T.More]':
                    i = 'gL'
                if i == 'magnitude[T.500]':
                    i = 'M'
                elif i == 'dominant_lulc_digits_grouped[T.More]:magnitude[T.500]':
                    i = 'gL:M'
                
                if pv <= 0.001:
                    sl = '$^{***}$'
                elif pv <= 0.01:
                    sl = '$^{**}$'
                elif pv <= 0.05:
                    sl = '$^{*}$'
                else:
                    sl = ''

                if round_digits:
                    p = np.round(p,round_digits)

                if i == 'Intercept':
                    formula += "{}{} ".format(p,sl)
                else:
                    formula += "+ {}{}({})".format(p,sl,i)
            
            formula = f"{metric} = {formula}"

            return formula
        
        ax.text(0.5,1.04,print_formula_with_coefficients(linear_model, metric),
                fontsize=11,color='black', usetex=True, wrap = True, ha='center')
        
    h,l = ax.get_legend_handles_labels()
    l[:2] = ['100yr','500yr']
    lgd = fig.legend(h,l,
           loc='lower center',
           ncols=2,
           frameon=True,
           framealpha=0.75,
           fontsize=10,
           title_fontsize=11,
           borderpad=0.25,
           markerscale=3,
           bbox_to_anchor=(0.6,-.07),
           borderaxespad=0,
           title='Magnitude'
           )

    
    txtstr = '\n'.join(["'***' = p-value < 0.001",
        "M = Magnitude", "gL = Grouped LULC"]
    )
    fig.text(0.03,0.001,txtstr,ha='left',va='top',fontsize=10,
                bbox={'alpha':0.25,"facecolor":'none',"boxstyle":'round'})

    if grouped_landcover_plot_fn != None:
        fig.savefig(grouped_landcover_plot_fn, bbox_inches='tight')
    
    plt.close(fig)

def channel_length_plot():
    pass


def group_lulc_map_func(orig_land_cover_fn, grouped_land_cover_fn):
    """Groups landcovers by grouping for better plotting"""
    print("Grouping landcover map ...")

    orig_lc = rxr.open_rasterio(orig_land_cover_fn)

    '''
    {  11 : "Water",
                                        12 : "Perennial Ice Snow",
                                        21 : "Developed, Open Space",
                                        22 : "Developed, Low Intensity",
                                        23 : "Developed, Medium Intensity",
                                        24 : "Developed, High Intensity",
                                        31 : "Bare Rock/Sand/Clay",
                                        41 : "Deciduous Forest",
                                        42 : "Evergreen Forest",
                                        43 : "Mixed Forest",
                                        45 : "Shrub-Forest",
                                        46 : "Herbaceous-Forest",
                                        52 : "Shrub/Scrub",
                                        71 : "Grasslands/Herbaceous",
                                        81 : "Pasture/Hay",
                                        82 : "Cultivated Crops",
                                        90 : "Woody Wetlands",
                                        95 : "Emergent Herbaceous Wetlands"
                                      }'''
    
    # create landcover groups dict
    landcover_groups = {
        1 : [11, 12],
        2 : [21, 22, 23, 24],
        3 : [31],
        4 : [41, 42, 43, 45, 46],
        5 : [52],
        7 : [71],
        8 : [81, 82],
        9 : [90, 95],
        127 : [127] # no data value
    }

    # invert landcover groups dict
    landcover_groups_inverted = {
        v : k for k, vs in landcover_groups.items() for v in vs
    }

    # create grouped landcover array
    grouped_landcover = orig_lc.copy(deep=True)
    
    @np.vectorize
    def replace(x):
        return landcover_groups_inverted[x]

    grouped_landcover.values = replace(grouped_landcover)
    
    # write grouped landcover array to file
    grouped_landcover.rio.to_raster(
        grouped_land_cover_fn,
        dtype='uint8',
        compress='lzw',
        overwrite=True,
        tiled=True,
        blockxsize=128,
        blockysize=128
    )


if __name__ == '__main__':
    

    if compute_secondary_metrics:
        ## dask cluster and client
        with LocalCluster(
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
                                                                   prepare_terrain_slope,
                                                                   prepare_imperviousness,
                                                                   land_cover_fn)
    else: 
        secondary_metrics_df = pd.read_hdf(experiment_fn,hdf_key) # read hdf

    if run_anova:
        anova(secondary_metrics_df)

    if make_regression_plot:
        plot_regression(linear_models_pickle_file)

    if rating_curves_to_parquet:
        parquet_rating_curves(hucs, resolutions)
    
    if aggregate_rating_curves:
        rating_curves_aggregation()
    
    if plot_rating_curves:
        rating_curve_plot()

    if compute_inundated_area:
        determine_inundated_area(hucs, resolutions, years, dem_sources, inundated_areas_parquet)

    if make_nhd_plot:
        nhd_to_3dep_plot(secondary_metrics_df,nhd_to_3dep_plot_fn)

    if make_dem_resolution_plot:
        resolution_plot(secondary_metrics_df,dem_resolution_plot_fn)

    if make_reservoir_plot:
        reservoir_plot(secondary_metrics_df,reservoir_plot_fn)

    if make_slope_plot:
        slope_plot(secondary_metrics_df,slope_plot_fn)

    if make_terrain_slope_plot:
        terrain_slope_plot(secondary_metrics_df,terrain_slope_plot_fn)

    if make_landcover_plot:
        landcover_plot(secondary_metrics_df,landcover_plot_fn)

    if make_grouped_landcover_plot:
        grouped_landcover_plot(secondary_metrics_df,grouped_landcover_plot_fn)

    if prepare_point_value_table:
        point_values_table(secondary_metrics_df)

    if make_tukey_hsd:
        tukey_hsd_for_60_90_m(secondary_metrics_df)

    if group_lulc_map:
        group_lulc_map_func(orig_land_cover_fn, grouped_land_cover_fn)
