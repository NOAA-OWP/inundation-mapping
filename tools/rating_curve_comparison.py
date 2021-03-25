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
from utils.shared_functions import getDriver

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

        rating_curves = rating_curves.append(select_usgs_gages)

        # add stream order
        stream_order = hydrotable.filter(items=['location_id','str_order'])
        rating_curves = rating_curves.merge(stream_order, on='location_id')

        rating_curves = rating_curves.rename(columns={"location_id": "USGS Gage"})


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
        columns = ['usgs_gage','NRMSE','mean_abs_y_diff','percent_bias']
        usgs_recurr_stats = []

        for index, gage in usgs_crosswalk.iterrows():
            ## Interpolate USGS/FIM elevation at NWM recurrence intervals
            # Interpolate USGS elevation at NWM recurrence intervals
            usgs_rc = rating_curves.loc[(rating_curves["USGS Gage"]==gage.location_id) & (rating_curves.Source=="USGS")]
            usgs_pred_elev = get_reccur_intervals(usgs_rc, usgs_crosswalk,nwm_recurr_intervals_all)

            # handle sites missing data
            if len(usgs_pred_elev) <1:
                continue

            # clean up data
            usgs_pred_elev['usgs_gage'] = gage.location_id
            usgs_pred_elev = usgs_pred_elev.filter(items=['usgs_gage','recurr_interval', 'discharge_cfs','pred_elev'])
            usgs_pred_elev = usgs_pred_elev.rename(columns={"pred_elev": "usgs_pred_elev"})

            # Interpolate FIM elevation at NWM recurrence intervals
            fim_rc = rating_curves.loc[(rating_curves["USGS Gage"]==gage.location_id) & (rating_curves.Source=="FIM")]
            fim_pred_elev = get_reccur_intervals(fim_rc, usgs_crosswalk,nwm_recurr_intervals_all)

            # handle sites missing data
            if len(fim_pred_elev) <1:
                print(f"missing fim elevation data for usgs station {gage.location_id} in huc {huc}")
                continue

            # clean up data
            fim_pred_elev = fim_pred_elev.rename(columns={"pred_elev": "fim_pred_elev"})
            fim_pred_elev = fim_pred_elev.filter(items=['recurr_interval', 'discharge_cfs','fim_pred_elev'])
            usgs_pred_elev = usgs_pred_elev.merge(fim_pred_elev, on=['recurr_interval','discharge_cfs']) # str_order
            usgs_pred_elev['HUC'] = huc
            nwm_recurr_data_table = nwm_recurr_data_table.append(usgs_pred_elev)

            ## Interpolate FIM elevation at USGS observations
            # Sort stage in ascending order
            usgs_rc = usgs_rc.sort_values('elevation',ascending=True)
            fim_rc = fim_rc.merge(usgs_crosswalk, left_on="USGS Gage", right_on="location_id")
            usgs_rc['pred_elev'] = np.interp(usgs_rc.discharge_cfs.values, fim_rc['discharge_cfs'], fim_rc['elevation'], left = np.nan, right = np.nan)

            usgs_rc = usgs_rc[usgs_rc['pred_elev'].notna()]
            rc_stats_plot_filename = join(dirname(rc_comparison_plot_filename),'rating_curve_stats' + str(gage.location_id) +'.png')

            if not usgs_rc.empty:
                gage_stats = calculate_rc_stats_stage(usgs_rc,rc_stats_plot_filename)

                usgs_recurr_stats.append(gage_stats)

        usgs_recurr_stats_table = pd.DataFrame(usgs_recurr_stats, columns=columns)

        if not usgs_recurr_stats_table.empty:
            usgs_recurr_stats_table.to_csv(usgs_recurr_stats_filename,index=False)

        if not nwm_recurr_data_table.empty:
            nwm_recurr_data_table.to_csv(nwm_recurr_data_filename,index=False)

    else:
        print(f"no USGS data for gage(s): {relevant_gages} in huc {huc}")

def aggregate_metrics(output_dir,procs_list):

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


def generate_facet_plot(rating_curves, rc_comparison_plot_filename):
    # Filter FIM elevation based on USGS data
    for gage in rating_curves['USGS Gage'].unique():

        min_elev = rating_curves.loc[(rating_curves['USGS Gage']==gage) & (rating_curves.Source=='USGS')].elevation.min()
        max_elev = rating_curves.loc[(rating_curves['USGS Gage']==gage) & (rating_curves.Source=='USGS')].elevation.max()

        rating_curves_map = rating_curves.drop(rating_curves[(rating_curves['USGS Gage']==gage) & (rating_curves.Source=='FIM') & (rating_curves.elevation > (max_elev + 2))].index)
        rating_curves_map = rating_curves.drop(rating_curves[(rating_curves['USGS Gage']==gage) & (rating_curves.Source=='FIM') & (rating_curves.elevation < min_elev - 2)].index)

    ## Generate rating curve plots
    sns.set(style="ticks")
    g = sns.FacetGrid(rating_curves_map, col="USGS Gage", hue="Source",sharex=False, sharey=False,col_wrap=3)
    g.map(sns.scatterplot, "discharge_cfs", "elevation", palette="tab20c", marker="o")
    g.set_axis_labels(x_var="Discharge (cfs)", y_var="Stage (ft)")

    # Adjust the arrangement of the plots
    g.fig.tight_layout(w_pad=1)
    g.add_legend()

    plt.savefig(rc_comparison_plot_filename)


def get_reccur_intervals(site_rc, usgs_crosswalk,nwm_recurr_intervals):

    usgs_site = site_rc.merge(usgs_crosswalk, left_on="USGS Gage", right_on="location_id")
    nwm_ids = len(usgs_site.feature_id.drop_duplicates())

    if nwm_ids > 0:

        nwm_recurr_intervals = nwm_recurr_intervals.copy().loc[nwm_recurr_intervals.feature_id==usgs_site.feature_id.drop_duplicates().item()]
        nwm_recurr_intervals['pred_elev'] = np.interp(nwm_recurr_intervals.discharge_cfs.values, usgs_site['discharge_cfs'], usgs_site['elevation'], left = np.nan, right = np.nan)

        return nwm_recurr_intervals

    else:
        return []


def calculate_rc_stats_stage(rating_curve, fig_path):
    station = rating_curve["USGS Gage"].unique().item()

    # Get the interpolated hand column, for now it is just the last column but THIS NEEDS TO BE BETTER FORMALIZED.
    usgs_stage = "elevation"
    flows = "discharge_cfs"
    hand_stage = "pred_elev"

    # Calculate variables for NRMSE
    rating_curve["yhat_minus_y"] = rating_curve[hand_stage] - rating_curve[usgs_stage]
    rating_curve["yhat_minus_y_squared"] = rating_curve["yhat_minus_y"] ** 2
    sum_y_diff = rating_curve["yhat_minus_y_squared"].sum()

    # determine number of events that are modeled
    n = rating_curve[usgs_stage].count()

    # Determine the maximum/minimum USGS stage
    y_max = rating_curve[usgs_stage].max()
    y_min = rating_curve[usgs_stage].min()

    # Calculate NRMSE
    NRMSE_numerator = (sum_y_diff / n) ** 0.5
    NRMSE_denominator = y_max - y_min
    NRMSE = NRMSE_numerator / NRMSE_denominator

    # Calculate Mean Absolute Depth Difference
    mean_abs_y_diff = abs(rating_curve["yhat_minus_y"]).mean()

    # Calculate Percent Bias
    percent_bias = 100 * (rating_curve["yhat_minus_y"].sum() / rating_curve[usgs_stage].sum())

    ## plot USGS rating curve and HAND rating curve and display statistics
    fig, ax = plt.subplots()
    rating_curve.plot(
        x=flows,
        y=usgs_stage,
        ax=ax,
        legend=False,
        style="-",
        color="orange",
        zorder=2,
    )
    rating_curve.plot(
        x=flows,
        y=usgs_stage,
        ax=ax,
        legend=False,
        kind="scatter",
        marker="o",
        s=30.0,
        color="black",
        zorder=3,
    )
    rating_curve.plot(
        x=flows, y=hand_stage, ax=ax, legend=False, style="--", color="gray", zorder=2
    )
    rating_curve.plot(
        x=flows,
        y=hand_stage,
        ax=ax,
        legend=False,
        kind="scatter",
        marker="x",
        s=30.0,
        color="blue",
        zorder=3,
    )
    ax.set_xlabel("Flow (cfs)")
    ax.set_ylabel("Elevation (ft)")
    ax.legend(["USGS Curve", "HAND Curve"], loc="best")
    ax.grid(zorder=1)
    fig.suptitle(
        "Rating Curve Plot ({})\nNRMSE = {}; Mean Abs Diff = {} ft; Bias = {}%".format(
            station,
            round(NRMSE, 2),
            round(mean_abs_y_diff, 2),
            round(percent_bias, 1),
        )
    )
    fig.savefig(fig_path)
    plt.close(fig)
    return [station, NRMSE, mean_abs_y_diff, percent_bias]

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='generate rating curve plots and tables for FIM and USGS gages')
    parser.add_argument('-output_dir','--output-dir', help='FIM output dir', required=True)
    parser.add_argument('-gages','--usgs-gages-filename',help='USGS rating curves',required=True)
    parser.add_argument('-flows','--nwm-flow-dir',help='NWM recurrence flows dir',required=True)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)

    args = vars(parser.parse_args())

    output_dir = args['output_dir']
    usgs_gages_filename = args['usgs_gages_filename']
    nwm_flow_dir = args['nwm_flow_dir']
    number_of_jobs = args['number_of_jobs']

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
    aggregate_metrics(output_dir,procs_list)
