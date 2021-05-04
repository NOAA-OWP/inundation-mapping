#!/usr/bin/env python3

import os
import sys
import geopandas as gpd
from shapely.geometry import Point
import rasterio
import pandas as pd
import numpy as np
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
from collections import deque
from functools import reduce
from os.path import isfile, join, dirname
import shutil
import warnings
from pathlib import Path
from collections import OrderedDict
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
outfolder = '/data/outputs/single_pixel_huc_ms_c/02030103'
# outfolder = '/data/outputs/single_pixel_huc_ms_c/12090301'

dem_meters_filename = os.path.join(outfolder,'dem_meters.tif')
dem_burned_filename = os.path.join(outfolder,'dem_burned.tif')
dem_burned_filled_filename = os.path.join(outfolder,'dem_burned_filled.tif')
dem_lateral_thalweg_adj_filename = os.path.join(outfolder,'dem_lateral_thalweg_adj.tif')
dem_thalwegCond_filename = os.path.join(outfolder,'dem_thalwegCond.tif')

reaches_filename = os.path.join(outfolder,'NHDPlusBurnLineEvent_subset.gpkg')


def compare_thalweg(args):

    huc                              = args[0]
    reaches_split_points_filename    = args[1]
    reaches_filename                 = args[2]
    dem_burned_filename         = args[3]
    dem_meters_filename              = args[4]

# reaches_split_points = gpd.read_file(reaches_split_points_filename)
reaches = gpd.read_file(reaches_filename)
dem_meters = rasterio.open(dem_meters_filename,'r')
dem_burned = rasterio.open(dem_burned_filename,'r')
dem_burned_filled = rasterio.open(dem_burned_filled_filename,'r')
dem_lateral_thalweg_adj = rasterio.open(dem_lateral_thalweg_adj_filename,'r')
dem_thalwegCond = rasterio.open(dem_thalwegCond_filename,'r')

### Get lists of all complete reaches using headwater attributes
#########################################


headwater_col = 'true_headwater'
reaches[headwater_col] = False
reaches.loc[reaches.NHDPlusID==10000100014087.0,headwater_col] = True
headwaters = reaches.loc[reaches[headwater_col]==True]

for index, headwater in headwaters.iterrows():
    reaches["headwater_path"] = headwater.nws_lid
    reaches.set_index('NHDPlusID',inplace=True,drop=False)

    stream_path = get_downstream_segments(reaches,headwater_col, 'downstream')


def get_downstream_segments(streams, headwater_col,flag_column):
    streams[flag_column] = False
    streams.loc[streams[headwater_col],flag_column] = True
    Q = deque(streams.loc[streams[headwater_col],'NHDPlusID'].tolist())
    visited = set()
    while Q:
        q = Q.popleft()
        if q in visited:
            continue
        visited.add(q)
        toNode,DnLevelPat = streams.loc[q,['ToNode','DnLevelPat']]
        try:
            downstream_ids = streams.loc[streams['FromNode'] == toNode,:].index.tolist()
        except ValueError: # 18050002 has duplicate nhd stream feature
            if len(toNode.unique()) == 1:
                toNode = toNode.iloc[0]
                downstream_ids = streams.loc[streams['FromNode'] == toNode,:].index.tolist()
        # If multiple downstream_ids are returned select the ids that are along the main flow path (i.e. exclude segments that are diversions)
        if len(set(downstream_ids))>1: # special case: remove duplicate NHDPlusIDs
            relevant_ids = [segment for segment in downstream_ids if DnLevelPat == streams.loc[segment,'LevelPathI']]
        else:
            relevant_ids = downstream_ids
        streams.loc[relevant_ids,flag_column] = True
        for i in relevant_ids:
            if i not in visited:
                Q.append(i)
    streams = streams.loc[streams[flag_column],:]
    return(streams)

#########################################
# Collect elevation values from multiple grids along each individual reach point

# Get all vertices
for index, path in stream_path.iterrows():
    split_points = []
    stream_ids = []
    dem_m_elev = []
    dem_burned_elev = []
    dem_burned_filled_elev = []
    dem_lat_thal_adj_elev = []
    dem_thal_adj_elev = []
    index_count = []
    count = 0
    headwater_id =
    for index, segment in path.iterrows():
        lineString = segment.geometry

        for point in zip(*lineString.coords.xy):
            stream_ids = stream_ids + [segment.NHDPlusID]
            split_points = split_points + [Point(point)]
            count = count + 1
            index_count = index_count + [count]
            dem_m_elev = dem_m_elev + [np.array(list(dem_meters.sample((Point(point).coords), indexes=1))).item()]
            dem_burned_elev = dem_burned_elev + [np.array(list(dem_burned.sample((Point(point).coords), indexes=1))).item()]
            dem_burned_filled_elev = dem_burned_filled_elev + [np.array(list(dem_burned_filled.sample((Point(point).coords), indexes=1))).item()]
            dem_lat_thal_adj_elev = dem_lat_thal_adj_elev + [np.array(list(dem_lateral_thalweg_adj.sample((Point(point).coords), indexes=1))).item()]
            dem_thal_adj_elev = dem_thal_adj_elev + [np.array(list(dem_thalwegCond.sample((Point(point).coords), indexes=1))).item()]

    dem_m_pts = gpd.GeoDataFrame({'NHDPlusID': stream_ids, 'source': 'dem_m', 'elevation_m': dem_m_elev, 'index_count': index_count, 'geometry': split_points}, crs=reaches.crs, geometry='geometry')
    # dem_burned_pts = gpd.GeoDataFrame({'NHDPlusID': stream_ids, 'source': 'dem_burned', 'elevation_m': dem_burned_elev, 'index_count': index_count, 'geometry': split_points}, crs=reaches.crs, geometry='geometry')
    dem_burned_filled_pts = gpd.GeoDataFrame({'NHDPlusID': stream_ids, 'source': 'dem_burned_filled', 'elevation_m': dem_burned_filled_elev, 'index_count': index_count, 'geometry': split_points}, crs=reaches.crs, geometry='geometry')
    dem_lat_thal_adj_pts = gpd.GeoDataFrame({'NHDPlusID': stream_ids, 'source': 'dem_lat_thal_adj', 'elevation_m': dem_lat_thal_adj_elev, 'index_count': index_count, 'geometry': split_points}, crs=reaches.crs, geometry='geometry')
    dem_thal_adj_pts = gpd.GeoDataFrame({'NHDPlusID': stream_ids, 'source': 'thal_adj_dem', 'elevation_m': dem_thal_adj_elev, 'index_count': index_count, 'geometry': split_points}, crs=reaches.crs, geometry='geometry')

burnline_points = dem_m_pts.append([dem_thal_adj_pts,dem_lat_thal_adj_pts]) # dem_burned_pts, dem_burned_filled_pts,

# remove nodata_pts
burnline_points = burnline_points.loc[burnline_points.elevation_m>-9999.0]
# burnline_points = gpd.GeoDataFrame({'NHDPlusID': stream_ids, 'ToNode': ToNodes, 'FromNode': FromNodes, 'elevation_m': dem_burned_elev, 'index_count': index_count, 'geometry': split_points}, crs=reaches.crs, geometry='geometry')

#########################################
# Identify significant drops in elevation (trace multiple grids)
def find_elevation_drops(burnline_points):
    drop_streams = []
    for index, segment in burnline_points.iterrows():
        upstream_elev = segment.elevation_m
        try:
            downstream_elev = burnline_points.loc[(burnline_points.index_count==(segment.index_count + 1))].elevation_m.item()
            if (downstream_elev - upstream_elev) > 5:
                print (f"elevation drop of {downstream_elev - upstream_elev} meters ")
                drop_streams = drop_streams + [index]
        except: # terminal point
            pass
    return drop_streams

burnline_points["headwater_path"] = 'WNQN4'

profile_plots_filename = '/data/outputs/single_pixel_huc_ms_c/02030103/profile_drop_plots2.png'

# num_plots = len(burnline_points.headwater_path.unique())
num_plots = len(burnline_points.source.unique())

if num_plots > 3:
    columns = num_plots // 3
else:
    columns = 1

sns.set(style="ticks")
# g = sns.FacetGrid(burnline_points, col="headwater_path", hue="source",sharex=True, sharey=False,col_wrap=columns)
# g.map(sns.lineplot, "index_count", "elevation_m", palette="tab20c") # , marker="o"
# g.set_axis_labels(x_var="Longitudinal Distance (ft)", y_var="Elevation (ft)")
g = sns.FacetGrid(burnline_points, col="source", hue="headwater_path",sharex=True, sharey=False,col_wrap=columns)
g.map(sns.lineplot, "index_count", "elevation_m", palette="tab20c") # , marker="o"
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
    # min_y = -100
    max_y = max(maxes) + (max(maxes) - min(mins))/10
    ax.set_ylim(min_y,max_y)

# Adjust the arrangement of the plots
g.fig.tight_layout(w_pad=1)
g.add_legend()

plt.savefig(profile_plots_filename)
plt.close()

###############################################################################################################################################

dem_thalweg_elevations = pd.DataFrame({'HydroID': hydroid, 'pt_order': index_order, 'elevation_m': dem_m_elev,'source': 'thalweg_adj'})
dem_adj_thalweg_elevations = pd.DataFrame({'HydroID': hydroid, 'pt_order': index_order, 'elevation_m': thal_adj_elev,'source': 'dem_meters'})

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
