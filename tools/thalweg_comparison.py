#!/usr/bin/env python3

import os
import sys
import geopandas as gpd
import rasterio
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
outfolder = '/data/outputs/single_pixel_huc_ms_c/02030103' # dev_v3_0_15_7_adj_huc_test
dem_thalwegCond_filename = os.path.join(outfolder,'dem_thalwegCond.tif')
dem_meters_filename = os.path.join(outfolder,'dem_meters.tif')
reaches_split_points_filename = os.path.join(outfolder,'demDerived_reaches_split_points.gpkg')
reaches_filename = os.path.join(outfolder,'demDerived_reaches_split.gpkg')


def compare_thalweg(args):

    huc                              = args[0]
    reaches_split_points_filename    = args[1]
    reaches_filename                 = args[2]
    dem_thalwegCond_filename         = args[3]
    dem_meters_filename              = args[4]

reaches_split_points = gpd.read_file(reaches_split_points_filename)
reaches = gpd.read_file(reaches_filename)
dem_thalwegCond = rasterio.open(dem_thalwegCond_filename,'r')
dem_meters = rasterio.open(dem_meters_filename,'r')

plot_filename = '/data/outputs/single_pixel_huc_ms_c/02030103/elev_plots.png'

reaches_split_points = reaches_split_points.rename(columns={'id': 'HydroID'})

hydroid = []
index_order = []
thal_adj_elev = []
dem_m_elev = []
for index, point in reaches_split_points.iterrows():
    hydroid = hydroid + [point.HydroID]
    index_order = index_order + [index]
    dem_m_elev = dem_m_elev + [np.array(list(dem_meters.sample((point.geometry.coords), indexes=1))).item()]
    thal_adj_elev = thal_adj_elev + [np.array(list(dem_thalwegCond.sample((point.geometry.coords), indexes=1))).item()]

dem_thalweg_elevations = pd.DataFrame({'HydroID': hydroid, 'pt_order': index_order, 'elevation_m': dem_m_elev,'source': 'dem_meters'})
dem_adj_thalweg_elevations = pd.DataFrame({'HydroID': hydroid, 'pt_order': index_order, 'elevation_m': thal_adj_elev,'source': 'thalweg_adj'})

all_elevations = dem_thalweg_elevations.append(dem_adj_thalweg_elevations)

reach_att = reaches[['HydroID', 'From_Node', 'To_Node', 'NextDownID']]

thalweg_elevations = all_elevations.merge(reach_att, on="HydroID")

# Find segments where elevation drops 5 m per
# drops = thalweg_elevations.loc[thalweg_elevations.HydroID
# all_hydro_ids = dict(thalweg_elevations[['HydroID','elevation_m']])
thalweg_elevations.NextDownID = thalweg_elevations.NextDownID.astype('int')
dem_adj_thalweg_elevations = thalweg_elevations.loc[thalweg_elevations.source=='thalweg_adj']
min_index = dem_adj_thalweg_elevations.groupby(['HydroID']).pt_order.min()
min_index = min_index.reset_index()
min_index = min_index.rename(columns={'pt_order': 'min_index'})

for index, downstream_id in dem_adj_thalweg_elevations.iterrows():
    if index == 1:
        break
    if downstream_id.NextDownID != -1:
        downstream_elevs = dem_adj_thalweg_elevations.loc[(dem_adj_thalweg_elevations.HydroID==downstream_id.NextDownID) & (dem_adj_thalweg_elevations.source=='thalweg_adj')].elevation_m
        if (downstream_id.elevation_m - downstream_elevs[0]) > 5:
            print (f"HydroID {HydroID} drops {(downstream_id.elevation_m - downstream_elev)} meters down from HydroID {NextDownID}")
        upstream_elev = dem_adj_thalweg_elevations.loc[dem_adj_thalweg_elevations.NextDownID==downstream_id.NextDownID].elevation_m

# drops = thalweg_elevations.

select_hydroids = [10680001,10680002,10680020,10680034,10680061,10680076,10680077,10680148,10680094]

select_elevations = thalweg_elevations.loc[thalweg_elevations.HydroID.isin(select_hydroids)]

# Convert index to longitudinal distance

# Find reference index for each segment to convert index to longitudinal distance
min_index = select_elevations.groupby(['HydroID']).pt_order.min()
min_index = min_index.reset_index()
min_index = min_index.rename(columns={'pt_order': 'min_index'})

# Subtract reference index from index and convert to feet
segment_distance = pd.merge(select_elevations[['HydroID', 'pt_order','source']],min_index, on="HydroID").reset_index(drop=True)
segment_distance['distance'] = (segment_distance.pt_order - segment_distance.min_index)* 32.8084
segment_distance.distance = segment_distance.distance.round(1)
# merge distances back into table
select_elevations = select_elevations.reset_index(drop=True)
# segment_distance_sub = segment_distance.filter(items=['HydroID', 'distance']).reset_index(drop=True)
select_elevations = pd.concat([select_elevations.set_index('HydroID'), segment_distance[['HydroID', 'distance']].set_index('HydroID')], axis=1, join="inner")
select_elevations = select_elevations.reset_index()
# Convert elevation to feet
select_elevations['elevation_ft'] = select_elevations.elevation_m * 3.28084 # convert from m to ft
select_elevations.elevation_ft = select_elevations.elevation_ft.round(1)

select_elevations = select_elevations.sort_values(['HydroID', 'distance','elevation_ft'], ascending=[1, 0, 0])
select_elevations = select_elevations.reset_index(drop=True)

## Generate rating curve plots
num_plots = len(select_elevations.HydroID.unique())

if num_plots > 3:
    columns = num_plots // 3
else:
    columns = 1

sns.set(style="ticks")
g = sns.FacetGrid(select_elevations, col="HydroID", hue="source",sharex=True, sharey=False,col_wrap=columns)
g.map(sns.lineplot, "distance", "elevation_ft", palette="tab20c") # , marker="o"
g.set_axis_labels(x_var="Longitudinal Distance (ft)", y_var="Elevation (ft)")

# Iterate thorugh each axis to get individual y-axis bounds
for ax in g.axes.flat:
    print (ax.lines)
    mins = []
    maxes = []
    for line in ax.lines:
        mins = mins + [min(line.get_ydata())]
        maxes = maxes + [max(line.get_ydata())]
    min_y = min(mins) - (max(maxes) - min(mins))/10
    max_y = max(maxes) + (max(maxes) - min(mins))/10
    ax.set_ylim(min_y,max_y)

# Adjust the arrangement of the plots
g.fig.tight_layout(w_pad=1)
g.add_legend()

plt.savefig(plot_filename)
plt.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='generate rating curve plots and tables for FIM and USGS gages')
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-output_dir','--output-dir', help='rating curves output folder', required=True,type=str)
    parser.add_argument('-gages','--usgs-gages-filename',help='USGS rating curves',required=True,type=str)
    parser.add_argument('-flows','--nwm-flow-dir',help='NWM recurrence flows dir',required=True,type=str)
    parser.add_argument('-catfim', '--catfim-flows-filename', help='Categorical FIM flows file',required = True,type=str)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-group','--stat-groups',help='column(s) to group stats',required=False,type=str)

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
