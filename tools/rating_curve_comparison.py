#!/usr/bin/env python3

import os
import sys
import geopandas as gpd
import pandas as pd
import numpy as np
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
from functools import reduce
from multiprocessing import Pool
from os.path import isfile, join, dirname
sys.path.append('/foss_fim/src')
# from utils.shared_functions import getDriver

"""
    Plot Rating Curves and Compare to USGS Gages

    Parameters
    ----------
    output_dir : str
        Directory containing FIM output folders.
    usgs_gages_filename : str
        File name of USGS rating curves.
    nwm_flow_dir : str
        Directory containing NWM recurrence flows files.
"""

# recurr_intervals = ['recurr_1_5_cms.csv','recurr_5_0_cms.csv','recurr_10_0_cms.csv']

def generate_rating_curve_metrics(args):

    elev_table_filename         = args[0]
    hydrotable_filename         = args[1]
    usgs_gages_filename         = args[2]
    usgs_recurr_stats_filename  = args[3]
    nwm_recurr_data_filename    = args[4]
    rc_comparison_plot_filename = args[5]
    nwm_flow_dir                = args[6]
    huc                         = args[7]

    elev_table = pd.read_csv(elev_table_filename)
    hydrotable = pd.read_csv(hydrotable_filename)
    usgs_gages = pd.read_csv(usgs_gages_filename)
    
    # Join rating curves with elevation data
    hydrotable = hydrotable.merge(elev_table, on="HydroID")
    relevant_gages = list(hydrotable.location_id.unique())
    usgs_gages = usgs_gages[usgs_gages['location_id'].isin(relevant_gages)]
    usgs_gages = usgs_gages.reset_index(drop=True)

    if len(usgs_gages) > 0:

        # Adjust rating curve to elevation
        hydrotable['elevation'] = (hydrotable.stage + hydrotable.dem_adj_elevation) * 3.28084 # convert from m to ft
        # hydrotable['raw_elevation'] = (hydrotable.stage + hydrotable.dem_elevation) * 3.28084 # convert from m to ft
        hydrotable['discharge_cfs'] = hydrotable.discharge_cms * 35.3147
        usgs_gages = usgs_gages.rename(columns={"flow": "discharge_cfs", "elevation_navd88": "elevation"})
        
        hydrotable['Source'] = "FIM"
        usgs_gages['Source'] = "USGS"
        limited_hydrotable = hydrotable.filter(items=['location_id','elevation','discharge_cfs','Source'])
        select_usgs_gages = usgs_gages.filter(items=['location_id', 'elevation', 'discharge_cfs','Source'])
        
        rating_curves = limited_hydrotable.append(select_usgs_gages)
        
        # Add stream order
        stream_order = hydrotable.filter(items=['location_id','str_order']).drop_duplicates()
        rating_curves = rating_curves.merge(stream_order, on='location_id')
        rating_curves['str_order'] = rating_curves['str_order'].astype('int')
        
        generate_facet_plot(rating_curves, rc_comparison_plot_filename)
        
        ## Calculate metrics for NWM reccurence intervals
        # NWM recurr intervals
        recurr_1_5_yr_filename = join(nwm_flow_dir,'recurr_1_5_cms.csv')
        recurr_5_yr_filename = join(nwm_flow_dir,'recurr_5_0_cms.csv')
        recurr_10_yr_filename = join(nwm_flow_dir,'recurr_10_0_cms.csv')
        
        recurr_1_5_yr = pd.read_csv(recurr_1_5_yr_filename)
        recurr_1_5_yr = recurr_1_5_yr.rename(columns={"discharge": "1.5"})
        recurr_5_yr = pd.read_csv(recurr_5_yr_filename)
        recurr_5_yr = recurr_5_yr.rename(columns={"discharge": "5.0"})
        recurr_10_yr = pd.read_csv(recurr_10_yr_filename)
        recurr_10_yr = recurr_10_yr.rename(columns={"discharge": "10.0"})
        
        nwm_recurr_intervals_all = reduce(lambda x,y: pd.merge(x,y, on='feature_id', how='outer'), [recurr_1_5_yr, recurr_5_yr, recurr_10_yr])
        nwm_recurr_intervals_all = pd.melt(nwm_recurr_intervals_all, id_vars=['feature_id'], value_vars=['1.5','5.0','10.0'], var_name='recurr_interval', value_name='discharge_cms')
        nwm_recurr_intervals_all['discharge_cfs'] = nwm_recurr_intervals_all.discharge_cms * 35.3147
        nwm_recurr_intervals_all = nwm_recurr_intervals_all.filter(items=['discharge_cfs', 'recurr_interval','feature_id']).drop_duplicates()
        
        usgs_crosswalk = hydrotable.filter(items=['location_id', 'feature_id']).drop_duplicates()
        
        nwm_recurr_data_table = pd.DataFrame()
        usgs_recurr_data = pd.DataFrame()

        for index, gage in usgs_crosswalk.iterrows():
            print(gage)
            ## Interpolate USGS/FIM elevation at NWM recurrence intervals
            # Interpolate USGS elevation at NWM recurrence intervals
            usgs_rc = rating_curves.loc[(rating_curves.location_id==gage.location_id) & (rating_curves.Source=="USGS")]
            
            str_order = np.unique(usgs_rc.str_order).item()
                        
            usgs_pred_elev = get_reccur_intervals(usgs_rc, usgs_crosswalk,nwm_recurr_intervals_all)

            # Handle sites missing data
            if len(usgs_pred_elev) <1:
                continue

            # Clean up data
            usgs_pred_elev['location_id'] = gage.location_id
            usgs_pred_elev = usgs_pred_elev.filter(items=['location_id','recurr_interval', 'discharge_cfs','pred_elev'])
            usgs_pred_elev = usgs_pred_elev.rename(columns={"pred_elev": "USGS"})
            
            # Interpolate FIM elevation at NWM recurrence intervals
            fim_rc = rating_curves.loc[(rating_curves.location_id==gage.location_id) & (rating_curves.Source=="FIM")]
            fim_pred_elev = get_reccur_intervals(fim_rc, usgs_crosswalk,nwm_recurr_intervals_all)

            # Handle sites missing data
            if len(fim_pred_elev) <1:
                print(f"missing fim elevation data for usgs station {gage.location_id} in huc {huc}")
                continue

            # Clean up data
            fim_pred_elev = fim_pred_elev.rename(columns={"pred_elev": "FIM"})
            fim_pred_elev = fim_pred_elev.filter(items=['recurr_interval', 'discharge_cfs','FIM'])
            usgs_pred_elev = usgs_pred_elev.merge(fim_pred_elev, on=['recurr_interval','discharge_cfs'])
            
            usgs_pred_elev['HUC'] = huc
            usgs_pred_elev['str_order'] = str_order
            
            usgs_pred_elev = pd.melt(usgs_pred_elev, id_vars=['location_id','recurr_interval','discharge_cfs','HUC','str_order'], value_vars=['USGS','FIM'], var_name="Source", value_name='elevation')
            nwm_recurr_data_table = nwm_recurr_data_table.append(usgs_pred_elev)
            
            ## Interpolate FIM elevation at USGS observations
            # Sort stage in ascending order
            usgs_rc = usgs_rc.rename(columns={"elevation": "USGS"})
            usgs_rc = usgs_rc.sort_values('USGS',ascending=True)
            fim_rc = fim_rc.merge(usgs_crosswalk, on="location_id")
            
            usgs_rc['FIM'] = np.interp(usgs_rc.discharge_cfs.values, fim_rc['discharge_cfs'], fim_rc['elevation'], left = np.nan, right = np.nan)
            usgs_rc = usgs_rc[usgs_rc['FIM'].notna()]
            usgs_rc = usgs_rc.drop(columns=["Source"])
            
            usgs_rc = pd.melt(usgs_rc, id_vars=['location_id','discharge_cfs','str_order'], value_vars=['USGS','FIM'], var_name="Source", value_name='elevation')

            if not usgs_rc.empty:
                usgs_recurr_data = usgs_recurr_data.append(usgs_rc)

        # Generate stats for all sites in huc
        if not usgs_recurr_data.empty:
            usgs_recurr_stats_table = calculate_rc_stats_elev(usgs_recurr_data)
            usgs_recurr_stats_table.to_csv(usgs_recurr_stats_filename,index=False)

        # Generate plots
        fim_elev_at_USGS_rc_plot_filename = join(dirname(rc_comparison_plot_filename),'FIM_elevations_at_USGS_rc_' + str(huc) +'.png')
        generate_facet_plot(usgs_recurr_data, fim_elev_at_USGS_rc_plot_filename)

        if not nwm_recurr_data_table.empty:
            nwm_recurr_data_table.to_csv(nwm_recurr_data_filename,index=False)

    else:
        print(f"no USGS data for gage(s): {relevant_gages} in huc {huc}")

def aggregate_metrics(output_dir,procs_list,stat_groups):

    agg_usgs_interp_elev_stats = join(output_dir,'agg_usgs_interp_elev_stats.csv')
    agg_nwm_recurr_flow_elev = join(output_dir,'agg_nwm_recurr_flow_elevations.csv')

    for huc in procs_list:
        if os.path.isfile(huc[3]):
            usgs_recurr_stats = pd.read_csv(huc[3])
    
            # Write/append usgs_recurr_stats
            if os.path.isfile(agg_usgs_interp_elev_stats):
                usgs_recurr_stats.to_csv(agg_usgs_interp_elev_stats,index=False, mode='a',header=False)
            else:
                usgs_recurr_stats.to_csv(agg_usgs_interp_elev_stats,index=False)
    
        if os.path.isfile(huc[4]):
            nwm_recurr_data = pd.read_csv(huc[4])
    
            # Write/append nwm_recurr_data
            if os.path.isfile(agg_nwm_recurr_flow_elev):
                nwm_recurr_data.to_csv(agg_nwm_recurr_flow_elev,index=False, mode='a',header=False)
            else:
                nwm_recurr_data.to_csv(agg_nwm_recurr_flow_elev,index=False)

    agg_stats = pd.read_csv(agg_nwm_recurr_flow_elev)
    
    agg_recurr_stats_table = calculate_rc_stats_elev(agg_stats,stat_groups)


def generate_facet_plot(rc, plot_filename):
    # Filter FIM elevation based on USGS data
    for gage in rc.location_id.unique():

        min_elev = rc.loc[(rc.location_id==gage) & (rc.Source=='USGS')].elevation.min()
        max_elev = rc.loc[(rc.location_id==gage) & (rc.Source=='USGS')].elevation.max()

        rc = rc.drop(rc[(rc.location_id==gage) & (rc.Source=='FIM') & (rc.elevation > (max_elev + 2))].index)
        rc = rc.drop(rc[(rc.location_id==gage) & (rc.Source=='FIM') & (rc.elevation < min_elev - 2)].index)

    rc = rc.rename(columns={"location_id": "USGS Gage"})

    ## Generate rating curve plots
    sns.set(style="ticks")
    g = sns.FacetGrid(rc, col="USGS Gage", hue="Source",sharex=False, sharey=False,col_wrap=3)
    g.map(sns.scatterplot, "discharge_cfs", "elevation", palette="tab20c", marker="o")
    g.set_axis_labels(x_var="Discharge (cfs)", y_var="Elevation (ft)")
    
     ## Change labels
    # axes = g.axes.flatten()
    # for ax in axes:
    #     ax.set_xlabel("Rating Curve Plot ({})\nNRMSE = {}; Mean Abs Diff = {} ft; Bias = {}%".format(
    #     station,
    #     round(NRMSE, 2),
    #     round(mean_abs_y_diff, 2),
    #     round(percent_bias, 1),
    # ))

    # Adjust the arrangement of the plots
    g.fig.tight_layout(w_pad=1)
    g.add_legend()

    plt.savefig(plot_filename)
    plt.close()


def get_reccur_intervals(site_rc, usgs_crosswalk,nwm_recurr_intervals):

    usgs_site = site_rc.merge(usgs_crosswalk, on="location_id")
    nwm_ids = len(usgs_site.feature_id.drop_duplicates())

    if nwm_ids > 0:

        nwm_recurr_intervals = nwm_recurr_intervals.copy().loc[nwm_recurr_intervals.feature_id==usgs_site.feature_id.drop_duplicates().item()]
        nwm_recurr_intervals['pred_elev'] = np.interp(nwm_recurr_intervals.discharge_cfs.values, usgs_site['discharge_cfs'], usgs_site['elevation'], left = np.nan, right = np.nan)

        return nwm_recurr_intervals

    else:
        return []


def calculate_rc_stats_elev(rc,stat_groups=None):
    
    usgs_elev = "USGS"
    src_elev = "FIM"
    
    # Collect any extra columns not associated with melt
    col_index = list(rc.columns)
    pivot_vars = ['Source','elevation']
    col_index = [col for col in col_index if col not in pivot_vars]
    
    # Unmelt elevation/Source
    rc_unmelt = (rc.set_index(col_index)
        .pivot(columns="Source")['elevation']
        .reset_index()
        .rename_axis(None, axis=1)
     )
    
    if stat_groups is None:
        stat_groups = ['location_id']
    
    # Calculate variables for NRMSE
    rc_unmelt["yhat_minus_y"] = rc_unmelt[src_elev] - rc_unmelt[usgs_elev]
    rc_unmelt["yhat_minus_y_squared"] = rc_unmelt["yhat_minus_y"] ** 2
    
    station_rc = rc_unmelt.groupby(stat_groups)     

    ## Calculate metrics by group
    # Calculate variables for NRMSE
    sum_y_diff = station_rc.apply(lambda x: x["yhat_minus_y_squared"].sum())\
        .reset_index(stat_groups, drop = False).rename({0: "sum_y_diff"}, axis=1)
        
    # Determine number of events that are modeled
    n = station_rc.apply(lambda x: x[usgs_elev].count())\
        .reset_index(stat_groups, drop = False).rename({0: "n"}, axis=1)
    
    # Determine the maximum/minimum USGS elevation
    y_max = station_rc.apply(lambda x: x[usgs_elev].max())\
        .reset_index(stat_groups, drop = False).rename({0: "y_max"}, axis=1)
    y_min = station_rc.apply(lambda x: x[usgs_elev].min())\
        .reset_index(stat_groups, drop = False).rename({0: "y_min"}, axis=1)
    
    # Collect variables for NRMSE
    NRMSE_table = reduce(lambda x,y: pd.merge(x,y, on=stat_groups, how='outer'), [sum_y_diff, n, y_max, y_min])
    NRMSE_table_group = NRMSE_table.groupby(stat_groups)  
    
    # Calculate NRMSE
    NRMSE = NRMSE_table_group.apply(lambda x: ((x['sum_y_diff'] / x['n']) ** 0.5)/x['y_max'] - x['y_min'])\
        .reset_index(stat_groups, drop = False).rename({0: "NRMSE"}, axis=1)
    
    # Calculate Mean Absolute Depth Difference
    mean_abs_y_diff = station_rc.apply(lambda x: abs(x["yhat_minus_y"]).mean())\
        .reset_index(stat_groups, drop = False).rename({0: "mean_abs_y_diff"}, axis=1)
    
    # Calculate Percent Bias
    percent_bias = station_rc.apply(lambda x: 100 * (x["yhat_minus_y"].sum()/x[usgs_elev].sum()))\
        .reset_index(stat_groups, drop = False).rename({0: "percent_bias"}, axis=1)
    
    rc_stat_table = reduce(lambda x,y: pd.merge(x,y, on=stat_groups, how='outer'), [NRMSE, mean_abs_y_diff, percent_bias])


    return rc_stat_table

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='generate rating curve plots and tables for FIM and USGS gages')
    parser.add_argument('-output_dir','--output-dir', help='FIM output dir', required=True)
    parser.add_argument('-gages','--usgs-gages-filename',help='USGS rating curves',required=True)
    parser.add_argument('-flows','--nwm-flow-dir',help='NWM recurrence flows dir',required=True)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-group','--stats-groups',help='column(s) to group stats',required=False,default=['location_id'])

    args = vars(parser.parse_args())

    output_dir = args['output_dir']
    usgs_gages_filename = args['usgs_gages_filename']
    nwm_flow_dir = args['nwm_flow_dir']
    number_of_jobs = args['number_of_jobs']
    stat_groups = args['stat_groups']
    
    
    procs_list = []
    
    huc_list  = os.listdir(output_dir)
    for huc in huc_list:
        elev_table_filename = join(output_dir,huc,'usgs_elev_table.csv')
        hydrotable_filename = join(output_dir,huc,'hydroTable.csv')
        usgs_recurr_stats_filename = join(output_dir,huc,'usgs_interpolated_elevation_stats.csv')
        nwm_recurr_data_filename = join(output_dir,huc,'nwm_recurrence_flow_elevations.csv')
        rc_comparison_plot_filename = join(output_dir,huc,'FIM-USGS_rating_curve_comparison.png')
    
        if isfile(elev_table_filename):
            procs_list.append([elev_table_filename, hydrotable_filename, usgs_gages_filename, usgs_recurr_stats_filename, nwm_recurr_data_filename, rc_comparison_plot_filename,nwm_flow_dir,huc])

    # Initiate multiprocessing
    print(f"Generating rating curve metrics for {len(procs_list)} hucs using {number_of_jobs} jobs")
    pool = Pool(number_of_jobs)
    pool.map(generate_rating_curve_metrics, procs_list)

    print(f"Aggregating rating curve metrics for {len(procs_list)} hucs")
    aggregate_metrics(output_dir,procs_list,stat_groups)
