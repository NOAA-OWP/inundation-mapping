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
import shutil
import warnings
from pathlib import Path
import time
warnings.simplefilter(action='ignore', category=FutureWarning)

"""
    Plot Rating Curves and Compare to USGS Gages

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    output_dir : str
        Directory containing rating curve plots and tables.
    usgs_gages_filename : str
        File name of USGS rating curves.
    nwm_flow_dir : str
        Directory containing NWM recurrence flows files.
    number_of_jobs : str
        Number of jobs.
    stat_groups : str
        string of columns to group eval metrics.
"""
def check_file_age(file):
    '''
    Checks if file exists, determines the file age, and recommends
    updating if older than 1 month.

    Returns
    -------
    None.

    '''
    file = Path(file)
    if file.is_file():
        modification_time = file.stat().st_mtime
        current_time = time.time()
        file_age_days = (current_time - modification_time)/86400
        if file_age_days > 30:
            check = f'{file.name} is {int(file_age_days)} days old, consider updating.\nUpdate with rating_curve_get_usgs_curves.py'
        else:
            check = f'{file.name} is {int(file_age_days)} days old.'
    return check

# recurr_intervals = ['recurr_1_5_cms.csv','recurr_5_0_cms.csv','recurr_10_0_cms.csv']

def generate_rating_curve_metrics(args):

    elev_table_filename         = args[0]
    hydrotable_filename         = args[1]
    usgs_gages_filename         = args[2]
    usgs_recurr_stats_filename  = args[3]
    nwm_recurr_data_filename    = args[4]
    rc_comparison_plot_filename = args[5]
    nwm_flow_dir                = args[6]
    catfim_flows_filename       = args[7]
    huc                         = args[8]

    elev_table = pd.read_csv(elev_table_filename,dtype={'location_id': str})
    hydrotable = pd.read_csv(hydrotable_filename,dtype={'HUC': str,'feature_id': str})
    usgs_gages = pd.read_csv(usgs_gages_filename,dtype={'location_id': str})

    # Join rating curves with elevation data
    hydrotable = hydrotable.merge(elev_table, on="HydroID")
    relevant_gages = list(hydrotable.location_id.unique())
    usgs_gages = usgs_gages[usgs_gages['location_id'].isin(relevant_gages)]
    usgs_gages = usgs_gages.reset_index(drop=True)

    if len(usgs_gages) > 0:

        # Adjust rating curve to elevation
        hydrotable['elevation_ft'] = (hydrotable.stage + hydrotable.dem_adj_elevation) * 3.28084 # convert from m to ft
        # hydrotable['raw_elevation_ft'] = (hydrotable.stage + hydrotable.dem_elevation) * 3.28084 # convert from m to ft
        hydrotable['discharge_cfs'] = hydrotable.discharge_cms * 35.3147
        usgs_gages = usgs_gages.rename(columns={"flow": "discharge_cfs", "elevation_navd88": "elevation_ft"})

        hydrotable['source'] = "FIM"
        usgs_gages['source'] = "USGS"
        limited_hydrotable = hydrotable.filter(items=['location_id','elevation_ft','discharge_cfs','source'])
        select_usgs_gages = usgs_gages.filter(items=['location_id', 'elevation_ft', 'discharge_cfs','source'])

        rating_curves = limited_hydrotable.append(select_usgs_gages)

        # Add stream order
        stream_orders = hydrotable.filter(items=['location_id','str_order']).drop_duplicates()
        rating_curves = rating_curves.merge(stream_orders, on='location_id')
        rating_curves['str_order'] = rating_curves['str_order'].astype('int')

        # plot rating curves
        generate_facet_plot(rating_curves, rc_comparison_plot_filename)

        # NWM recurr intervals
        recurr_1_5_yr_filename = join(nwm_flow_dir,'recurr_1_5_cms.csv')
        recurr_5_yr_filename = join(nwm_flow_dir,'recurr_5_0_cms.csv')
        recurr_10_yr_filename = join(nwm_flow_dir,'recurr_10_0_cms.csv')
        
        # Update column names
        recurr_1_5_yr = pd.read_csv(recurr_1_5_yr_filename,dtype={'feature_id': str})
        recurr_1_5_yr = recurr_1_5_yr.rename(columns={"discharge": "1.5"})
        recurr_5_yr = pd.read_csv(recurr_5_yr_filename,dtype={'feature_id': str})
        recurr_5_yr = recurr_5_yr.rename(columns={"discharge": "5.0"})
        recurr_10_yr = pd.read_csv(recurr_10_yr_filename,dtype={'feature_id': str})
        recurr_10_yr = recurr_10_yr.rename(columns={"discharge": "10.0"})
                        
        # Merge NWM recurr intervals into a single layer
        nwm_recurr_intervals_all = reduce(lambda x,y: pd.merge(x,y, on='feature_id', how='outer'), [recurr_1_5_yr, recurr_5_yr, recurr_10_yr])
        nwm_recurr_intervals_all = pd.melt(nwm_recurr_intervals_all, id_vars=['feature_id'], value_vars=['1.5','5.0','10.0'], var_name='recurr_interval', value_name='discharge_cms')
        
        # Append catfim data (already set up in format similar to nwm_recurr_intervals_all)
        cat_fim = pd.read_csv(catfim_flows_filename, dtype={'feature_id':str})
        nwm_recurr_intervals_all = nwm_recurr_intervals_all.append(cat_fim)
        
        # Convert discharge to cfs and filter
        nwm_recurr_intervals_all['discharge_cfs'] = nwm_recurr_intervals_all.discharge_cms * 35.3147
        nwm_recurr_intervals_all = nwm_recurr_intervals_all.filter(items=['discharge_cfs', 'recurr_interval','feature_id']).drop_duplicates()

        # Identify unique gages
        usgs_crosswalk = hydrotable.filter(items=['location_id', 'feature_id']).drop_duplicates()

        nwm_recurr_data_table = pd.DataFrame()
        usgs_recurr_data = pd.DataFrame()

        # Interpolate USGS/FIM elevation at each gage
        for index, gage in usgs_crosswalk.iterrows():

            # Interpolate USGS elevation at NWM recurrence intervals
            usgs_rc = rating_curves.loc[(rating_curves.location_id==gage.location_id) & (rating_curves.source=="USGS")]

            if len(usgs_rc) <1:
                print(f"missing USGS rating curve data for usgs station {gage.location_id} in huc {huc}")
                continue

            str_order = np.unique(usgs_rc.str_order).item()
            feature_id = str(gage.feature_id)

            usgs_pred_elev = get_reccur_intervals(usgs_rc, usgs_crosswalk,nwm_recurr_intervals_all)

            # Handle sites missing data
            if len(usgs_pred_elev) <1:
                print(f"missing USGS elevation data for usgs station {gage.location_id} in huc {huc}")
                continue

            # Clean up data
            usgs_pred_elev['location_id'] = gage.location_id
            usgs_pred_elev = usgs_pred_elev.filter(items=['location_id','recurr_interval', 'discharge_cfs','pred_elev'])
            usgs_pred_elev = usgs_pred_elev.rename(columns={"pred_elev": "USGS"})

            # Interpolate FIM elevation at NWM recurrence intervals
            fim_rc = rating_curves.loc[(rating_curves.location_id==gage.location_id) & (rating_curves.source=="FIM")]

            if len(fim_rc) <1:
                print(f"missing FIM rating curve data for usgs station {gage.location_id} in huc {huc}")
                continue

            fim_pred_elev = get_reccur_intervals(fim_rc, usgs_crosswalk,nwm_recurr_intervals_all)

            # Handle sites missing data
            if len(fim_pred_elev) <1:
                print(f"missing FIM elevation data for usgs station {gage.location_id} in huc {huc}")
                continue

            # Clean up data
            fim_pred_elev = fim_pred_elev.rename(columns={"pred_elev": "FIM"})
            fim_pred_elev = fim_pred_elev.filter(items=['recurr_interval', 'discharge_cfs','FIM'])
            usgs_pred_elev = usgs_pred_elev.merge(fim_pred_elev, on=['recurr_interval','discharge_cfs'])

            # Add attributes
            usgs_pred_elev['HUC'] = huc
            usgs_pred_elev['HUC4'] = huc[0:4]
            usgs_pred_elev['str_order'] = str_order
            usgs_pred_elev['feature_id'] = feature_id

            # Melt dataframe
            usgs_pred_elev = pd.melt(usgs_pred_elev, id_vars=['location_id','feature_id','recurr_interval','discharge_cfs','HUC','HUC4','str_order'], value_vars=['USGS','FIM'], var_name="source", value_name='elevation_ft')
            nwm_recurr_data_table = nwm_recurr_data_table.append(usgs_pred_elev)

            # Interpolate FIM elevation at USGS observations
            # fim_rc = fim_rc.merge(usgs_crosswalk, on="location_id")
            # usgs_rc = usgs_rc.rename(columns={"elevation_ft": "USGS"})
            #
            # # Sort stage in ascending order
            # usgs_rc = usgs_rc.sort_values('USGS',ascending=True)
            #
            # # Interpolate FIM elevation at USGS observations
            # usgs_rc['FIM'] = np.interp(usgs_rc.discharge_cfs.values, fim_rc['discharge_cfs'], fim_rc['elevation_ft'], left = np.nan, right = np.nan)
            # usgs_rc = usgs_rc[usgs_rc['FIM'].notna()]
            # usgs_rc = usgs_rc.drop(columns=["source"])
            #
            # # Melt dataframe
            # usgs_rc = pd.melt(usgs_rc, id_vars=['location_id','discharge_cfs','str_order'], value_vars=['USGS','FIM'], var_name="source", value_name='elevation_ft')
            #
            # if not usgs_rc.empty:
            #     usgs_recurr_data = usgs_recurr_data.append(usgs_rc)

        # Generate stats for all sites in huc
        # if not usgs_recurr_data.empty:
        #     usgs_recurr_stats_table = calculate_rc_stats_elev(usgs_recurr_data)
        #     usgs_recurr_stats_table.to_csv(usgs_recurr_stats_filename,index=False)

        # # Generate plots (not currently being used)
        # fim_elev_at_USGS_rc_plot_filename = join(dirname(rc_comparison_plot_filename),'FIM_elevations_at_USGS_rc_' + str(huc) +'.png')
        # generate_facet_plot(usgs_recurr_data, fim_elev_at_USGS_rc_plot_filename)

        if not nwm_recurr_data_table.empty:
            nwm_recurr_data_table.discharge_cfs = np.round(nwm_recurr_data_table.discharge_cfs,2)
            nwm_recurr_data_table.elevation_ft = np.round(nwm_recurr_data_table.elevation_ft,2)
            nwm_recurr_data_table.to_csv(nwm_recurr_data_filename,index=False)

    else:
        print(f"no USGS data for gage(s): {relevant_gages} in huc {huc}")

def aggregate_metrics(output_dir,procs_list,stat_groups):

    # agg_usgs_interp_elev_stats = join(output_dir,'agg_usgs_interp_elev_stats.csv')
    agg_nwm_recurr_flow_elev = join(output_dir,'agg_nwm_recurr_flow_elevations.csv')
    agg_nwm_recurr_flow_elev_stats = join(output_dir,f"agg_nwm_recurr_flow_elev_stats_{'_'.join(stat_groups)}.csv")

    # if os.path.isfile(agg_usgs_interp_elev_stats):
    #     os.remove(agg_usgs_interp_elev_stats)
    if os.path.isfile(agg_nwm_recurr_flow_elev):
        os.remove(agg_nwm_recurr_flow_elev)
    if os.path.isfile(agg_nwm_recurr_flow_elev_stats):
        os.remove(agg_nwm_recurr_flow_elev_stats)

    for huc in procs_list:
        # if os.path.isfile(huc[3]):
        #     usgs_recurr_stats = pd.read_csv(huc[3])
        #
        #     # Write/append usgs_recurr_stats
        #     if os.path.isfile(agg_usgs_interp_elev_stats):
        #         usgs_recurr_stats.to_csv(agg_usgs_interp_elev_stats,index=False, mode='a',header=False)
        #     else:
        #         usgs_recurr_stats.to_csv(agg_usgs_interp_elev_stats,index=False)

        if os.path.isfile(huc[4]):
            nwm_recurr_data = pd.read_csv(huc[4],dtype={'location_id': str,
                                                        'feature_id': str})

            # Write/append nwm_recurr_data
            if os.path.isfile(agg_nwm_recurr_flow_elev):
                nwm_recurr_data.to_csv(agg_nwm_recurr_flow_elev,index=False, mode='a',header=False)
            else:
                nwm_recurr_data.to_csv(agg_nwm_recurr_flow_elev,index=False)

    agg_stats = pd.read_csv(agg_nwm_recurr_flow_elev,dtype={'location_id': str,
                                                            'feature_id': str})

    agg_recurr_stats_table = calculate_rc_stats_elev(agg_stats,stat_groups)

    agg_recurr_stats_table.to_csv(agg_nwm_recurr_flow_elev_stats,index=False)


def generate_facet_plot(rc, plot_filename):

    # Filter FIM elevation based on USGS data
    for gage in rc.location_id.unique():

        min_elev = rc.loc[(rc.location_id==gage) & (rc.source=='USGS')].elevation_ft.min()
        max_elev = rc.loc[(rc.location_id==gage) & (rc.source=='USGS')].elevation_ft.max()

        rc = rc.drop(rc[(rc.location_id==gage) & (rc.source=='FIM') & (rc.elevation_ft > (max_elev + 2))].index)
        rc = rc.drop(rc[(rc.location_id==gage) & (rc.source=='FIM') & (rc.elevation_ft < min_elev - 2)].index)

    rc = rc.rename(columns={"location_id": "USGS Gage"})

    ## Generate rating curve plots
    num_plots = len(rc["USGS Gage"].unique())
    if num_plots > 3:
        columns = num_plots // 3
    else:
        columns = 1

    sns.set(style="ticks")
    g = sns.FacetGrid(rc, col="USGS Gage", hue="source",sharex=False, sharey=False,col_wrap=columns)
    g.map(sns.scatterplot, "discharge_cfs", "elevation_ft", palette="tab20c", marker="o")
    g.set_axis_labels(x_var="Discharge (cfs)", y_var="Elevation (ft)")

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
        nwm_recurr_intervals['pred_elev'] = np.interp(nwm_recurr_intervals.discharge_cfs.values, usgs_site['discharge_cfs'], usgs_site['elevation_ft'], left = np.nan, right = np.nan)

        return nwm_recurr_intervals

    else:
        return []


def calculate_rc_stats_elev(rc,stat_groups=None):

    usgs_elev = "USGS"
    src_elev = "FIM"

    # Collect any extra columns not associated with melt
    col_index = list(rc.columns)
    pivot_vars = ['source','elevation_ft']
    col_index = [col for col in col_index if col not in pivot_vars]

    # Unmelt elevation/source
    rc_unmelt = (rc.set_index(col_index)
        .pivot(columns="source")['elevation_ft']
        .reset_index()
        .rename_axis(None, axis=1)
     )

    if stat_groups is None:
        stat_groups = ['location_id']

    # Calculate variables for NRMSE
    rc_unmelt["yhat_minus_y"] = rc_unmelt[src_elev] - rc_unmelt[usgs_elev]
    rc_unmelt["yhat_minus_y_squared"] = rc_unmelt["yhat_minus_y"] ** 2

    # Calculate metrics by group
    station_rc = rc_unmelt.groupby(stat_groups)

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
    nrmse_table = reduce(lambda x,y: pd.merge(x,y, on=stat_groups, how='outer'), [sum_y_diff, n, y_max, y_min])
    nrmse_table_group = nrmse_table.groupby(stat_groups)

    # Calculate nrmse
    nrmse = nrmse_table_group.apply(lambda x: ((x['sum_y_diff'] / x['n']) ** 0.5) / (x['y_max'] - x['y_min']))\
        .reset_index(stat_groups, drop = False).rename({0: "nrmse"}, axis=1)

    # Calculate Mean Absolute Depth Difference
    mean_abs_y_diff = station_rc.apply(lambda x: (abs(x["yhat_minus_y"]).mean()))\
        .reset_index(stat_groups, drop = False).rename({0: "mean_abs_y_diff_ft"}, axis=1)

    # Calculate Percent Bias
    percent_bias = station_rc.apply(lambda x: 100 * (x["yhat_minus_y"].sum() / x[usgs_elev].sum()))\
        .reset_index(stat_groups, drop = False).rename({0: "percent_bias"}, axis=1)

    rc_stat_table = reduce(lambda x,y: pd.merge(x,y, on=stat_groups, how='outer'), [nrmse, mean_abs_y_diff, percent_bias])

    return rc_stat_table


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='generate rating curve plots and tables for FIM and USGS gages')
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True)
    parser.add_argument('-output_dir','--output-dir', help='rating curves output folder', required=True)
    parser.add_argument('-gages','--usgs-gages-filename',help='USGS rating curves',required=True)
    parser.add_argument('-flows','--nwm-flow-dir',help='NWM recurrence flows dir',required=True)
    parser.add_argument('-catfim', '--catfim-flows-filename', help='Categorical FIM flows file',required = True)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-group','--stat-groups',help='column(s) to group stats',required=False)

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    output_dir = args['output_dir']
    usgs_gages_filename = args['usgs_gages_filename']
    nwm_flow_dir = args['nwm_flow_dir']
    catfim_flows_filename = args['catfim_flows_filename']
    number_of_jobs = args['number_of_jobs']
    stat_groups = args['stat_groups']

    stat_groups = stat_groups.split()
    procs_list = []

    plots_dir = join(output_dir,'plots')
    os.makedirs(plots_dir, exist_ok=True)
    tables_dir = join(output_dir,'tables')
    os.makedirs(tables_dir, exist_ok=True)

    #Check age of gages csv and recommend updating if older than 30 days.
    print(check_file_age(usgs_gages_filename))

    # Open log file
    sys.__stdout__ = sys.stdout
    log_file = open(join(output_dir,'rating_curve_comparison.log'),"w")
    sys.stdout = log_file

    huc_list  = os.listdir(fim_dir)
    for huc in huc_list:

        if huc != 'logs':
            elev_table_filename = join(fim_dir,huc,'usgs_elev_table.csv')
            hydrotable_filename = join(fim_dir,huc,'hydroTable.csv')
            usgs_recurr_stats_filename = join(tables_dir,f"usgs_interpolated_elevation_stats_{huc}.csv")
            nwm_recurr_data_filename = join(tables_dir,f"nwm_recurrence_flow_elevations_{huc}.csv")
            rc_comparison_plot_filename = join(plots_dir,f"FIM-USGS_rating_curve_comparison_{huc}.png")

            if isfile(elev_table_filename):
                procs_list.append([elev_table_filename, hydrotable_filename, usgs_gages_filename, usgs_recurr_stats_filename, nwm_recurr_data_filename, rc_comparison_plot_filename,nwm_flow_dir, catfim_flows_filename, huc])

    # Initiate multiprocessing
    print(f"Generating rating curve metrics for {len(procs_list)} hucs using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        pool.map(generate_rating_curve_metrics, procs_list)

    print(f"Aggregating rating curve metrics for {len(procs_list)} hucs")
    aggregate_metrics(output_dir,procs_list,stat_groups)

    print('Delete intermediate tables')
    shutil.rmtree(tables_dir, ignore_errors=True)

    # Close log file
    sys.stdout = sys.__stdout__
    log_file.close()
