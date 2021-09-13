#!/usr/bin/env python3

import os
import sys
import geopandas as gpd
sys.path.append('/foss_fim/src')
from shapely.geometry import Point, LineString
import rasterio
import numpy as np
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
from collections import deque
from os.path import join
from multiprocessing import Pool
from utils.shared_functions import getDriver
from rasterio import features
from reachID_grid_to_vector_points import convert_grid_cells_to_points
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

"""
    Plot Rating Curves and Compare to USGS Gages

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    output_dir : str
        Stream layer to be evaluated.
    stream_type : str
        File name of USGS rating curves.
    point_density : str
        Elevation sampling density.
    number_of_jobs : str
        Number of jobs.
"""


def compare_thalweg(args):

    huc_dir                             = args[0]
    stream_type                         = args[1]
    point_density                       = args[2]
    huc                                 = args[3]
    dem_meters_filename                 = args[4]
    dem_lateral_thalweg_adj_filename    = args[5]
    dem_thalwegCond_filename            = args[6]
    profile_plots_filename              = args[7]
    profile_gpkg_filename               = args[8]
    profile_table_filename              = args[9]
    flows_grid_boolean_filename         = args[10]

    if stream_type == 'derived':

        dem_derived_reaches_filename = os.path.join(huc_dir,'demDerived_reaches_split.gpkg')
        streams = gpd.read_file(dem_derived_reaches_filename)
        nhd_headwater_filename = os.path.join(huc_dir,'nhd_headwater_points_subset.gpkg')
        wbd_filename = os.path.join(huc_dir,'wbd.gpkg')
        wbd = gpd.read_file(wbd_filename)
        headwaters_layer = gpd.read_file(nhd_headwater_filename,mask=wbd)
        headwater_list = headwaters_layer.loc[headwaters_layer.pt_type == 'nws_lid']
        stream_id = 'HydroID'

    elif stream_type == 'burnline':

        nhd_reaches_filename = os.path.join(huc_dir,'NHDPlusBurnLineEvent_subset.gpkg')
        nhd_reaches = gpd.read_file(nhd_reaches_filename)
        streams = nhd_reaches.copy()
        headwaters_layer = None

        # Get lists of all complete reaches using headwater attributes
        headwater_list = streams.loc[streams.nws_lid!=''].nws_lid
        stream_id = 'NHDPlusID'

    headwater_col = 'is_headwater'
    streams[headwater_col] = False
    headwater_list = headwater_list.reset_index(drop=True)

    if stream_type == 'derived':
        streams['nws_lid'] = ''

        if streams.NextDownID.dtype != 'int': streams.NextDownID = streams.NextDownID.astype(int)

        min_dist = np.empty(len(headwater_list))
        streams['min_dist'] = 1000

        for i, point in headwater_list.iterrows():
            streams['min_dist'] = [point.geometry.distance(line) for line in streams.geometry]
            streams.loc[streams.min_dist==np.min(streams.min_dist),'nws_lid'] = point.site_id

        headwater_list = headwater_list.site_id

    streams.set_index(stream_id,inplace=True,drop=False)

    # Collect headwater streams
    single_stream_paths = []
    dem_meters = rasterio.open(dem_meters_filename,'r')
    index_option = 'reachID'
    for index, headwater_site in enumerate(headwater_list):
        stream_path = get_downstream_segments(streams.copy(),'nws_lid', headwater_site,'downstream',stream_id,stream_type)
        stream_path = stream_path.reset_index(drop=True)
        stream_path = stream_path.sort_values(by=['downstream_count'])
        stream_path = stream_path.loc[stream_path.downstream==True]
        if stream_type == 'burnline':
            geom_value = []
            for index, segment in stream_path.iterrows():
                lineString = LineString(segment.geometry.coords[::-1])
                geom_value = geom_value + [(lineString, segment.downstream_count)]
            nhd_reaches_raster = features.rasterize(shapes=geom_value , out_shape=[dem_meters.height, dem_meters.width],fill=dem_meters.nodata,transform=dem_meters.transform, all_touched=True, dtype=np.float32)
            flow_bool = rasterio.open(flows_grid_boolean_filename)
            flow_bool_data = flow_bool.read(1)
            nhd_reaches_raster = np.where(flow_bool_data == int(0), -9999.0, (nhd_reaches_raster).astype(rasterio.float32))
            out_dem_filename = os.path.join(huc_dir,'NHDPlusBurnLineEvent_raster.tif')
            with rasterio.open(out_dem_filename, "w", **dem_meters.profile, BIGTIFF='YES') as dest:
                dest.write(nhd_reaches_raster, indexes = 1)
            stream_path = convert_grid_cells_to_points(out_dem_filename,index_option)
        stream_path["headwater_path"] = headwater_site
        single_stream_paths = single_stream_paths + [stream_path]
        print(f"length of {headwater_site} path: {len(stream_path)}")

    # Collect elevation values from multiple grids along each individual reach point
    dem_lateral_thalweg_adj = rasterio.open(dem_lateral_thalweg_adj_filename,'r')
    dem_thalwegCond = rasterio.open(dem_thalwegCond_filename,'r')
    thalweg_points = gpd.GeoDataFrame()
    for path in single_stream_paths:
        split_points = []
        stream_ids = []
        dem_m_elev = []
        dem_burned_filled_elev = []
        dem_lat_thal_adj_elev = []
        dem_thal_adj_elev = []
        headwater_path = []
        index_count = []
        for index, segment in path.iterrows():
            if stream_type == 'derived':
                linestring = segment.geometry
                if point_density == 'midpoints':
                    midpoint = linestring.interpolate(0.5,normalized=True)
                    stream_ids = stream_ids + [segment[stream_id]]
                    split_points = split_points + [midpoint]
                    index_count = index_count + [segment.downstream_count]
                    dem_m_elev = dem_m_elev + [np.array(list(dem_meters.sample((Point(midpoint).coords), indexes=1))).item()]
                    dem_lat_thal_adj_elev = dem_lat_thal_adj_elev + [np.array(list(dem_lateral_thalweg_adj.sample((Point(midpoint).coords), indexes=1))).item()]
                    dem_thal_adj_elev = dem_thal_adj_elev + [np.array(list(dem_thalwegCond.sample((Point(midpoint).coords), indexes=1))).item()]
                    headwater_path = headwater_path + [segment.headwater_path]
                elif point_density == 'all_points':
                    count=0
                    for point in zip(*linestring.coords.xy):
                        stream_ids = stream_ids + [segment[stream_id]]
                        split_points = split_points + [Point(point)]
                        count = count + 1
                        index_count = index_count + [segment.downstream_count*1000 + count]
                        dem_m_elev = dem_m_elev + [np.array(list(dem_meters.sample((Point(point).coords), indexes=1))).item()]
                        dem_lat_thal_adj_elev = dem_lat_thal_adj_elev + [np.array(list(dem_lateral_thalweg_adj.sample((Point(point).coords), indexes=1))).item()]
                        dem_thal_adj_elev = dem_thal_adj_elev + [np.array(list(dem_thalwegCond.sample((Point(point).coords), indexes=1))).item()]
                        headwater_path = headwater_path + [segment.headwater_path]
            elif stream_type == 'burnline':
                stream_ids = stream_ids + [segment['id']]
                split_points = split_points + [Point(segment.geometry)]
                index_count = index_count + [segment['id']]
                dem_m_elev = dem_m_elev + [np.array(list(dem_meters.sample((Point(segment.geometry).coords), indexes=1))).item()]
                dem_lat_thal_adj_elev = dem_lat_thal_adj_elev + [np.array(list(dem_lateral_thalweg_adj.sample((Point(segment.geometry).coords), indexes=1))).item()]
                dem_thal_adj_elev = dem_thal_adj_elev + [np.array(list(dem_thalwegCond.sample((Point(segment.geometry).coords), indexes=1))).item()]
                headwater_path = headwater_path + [segment.headwater_path]
        # gpd.GeoDataFrame({**data, "source": "dem_m"})
        dem_m_pts = gpd.GeoDataFrame({'stream_id': stream_ids, 'source': 'dem_m', 'elevation_m': dem_m_elev, 'headwater_path': headwater_path, 'index_count': index_count, 'geometry': split_points}, crs=path.crs, geometry='geometry')
        dem_lat_thal_adj_pts = gpd.GeoDataFrame({'stream_id': stream_ids, 'source': 'dem_lat_thal_adj', 'elevation_m': dem_lat_thal_adj_elev, 'headwater_path': headwater_path, 'index_count': index_count, 'geometry': split_points}, crs=path.crs, geometry='geometry')
        dem_thal_adj_pts = gpd.GeoDataFrame({'stream_id': stream_ids, 'source': 'thal_adj_dem', 'elevation_m': dem_thal_adj_elev, 'headwater_path': headwater_path, 'index_count': index_count, 'geometry': split_points}, crs=path.crs, geometry='geometry')
        for raster in [dem_m_pts,dem_lat_thal_adj_pts,dem_thal_adj_pts]:
            raster = raster.sort_values(by=['index_count'])
            raster.set_index('index_count',inplace=True,drop=True)
            raster = raster.reset_index(drop=True)
            raster.index.names = ['index_count']
            raster = raster.reset_index(drop=False)
            thalweg_points = thalweg_points.append(raster,ignore_index = True)
            del raster
        del dem_m_pts,dem_lat_thal_adj_pts,dem_thal_adj_pts

    del dem_lateral_thalweg_adj,dem_thalwegCond,dem_meters

    try:
        # Remove nodata_pts and convert elevation to ft
        thalweg_points = thalweg_points.loc[thalweg_points.elevation_m > 0.0]
        thalweg_points.elevation_m =  np.round(thalweg_points.elevation_m,3)
        thalweg_points['elevation_ft'] =  np.round(thalweg_points.elevation_m*3.28084,3)

        # Plot thalweg profile
        plot_profile(thalweg_points, profile_plots_filename)

        # Filter final thalweg ajdusted layer
        thal_adj_points = thalweg_points.loc[thalweg_points.source=='thal_adj_dem'].copy()
        # thal_adj_points.to_file(profile_gpkg_filename,driver=getDriver(profile_gpkg_filename))

        # Identify significant rises/drops in elevation
        thal_adj_points['elev_change'] = thal_adj_points.groupby(['headwater_path', 'source'])['elevation_m'].apply(lambda x: x - x.shift())
        elev_changes = thal_adj_points.loc[(thal_adj_points.elev_change<=-lateral_elevation_threshold) | (thal_adj_points.elev_change>0.0)]

        if not elev_changes.empty:
            # elev_changes.to_csv(profile_table_filename,index=False)
            elev_changes.to_file(profile_gpkg_filename,index=False,driver=getDriver(profile_gpkg_filename))


        # Zoom in to plot only areas with steep elevation changes
        # select_streams = elev_changes.stream_id.to_list()
        # downstream_segments = [index + 1 for index in select_streams]
        # upstream_segments = [index - 1 for index in select_streams]
        # select_streams = list(set(upstream_segments + downstream_segments + select_streams))
        # thal_adj_points_select = thal_adj_points.loc[thal_adj_points.stream_id.isin(select_streams)]
        # plot_profile(thal_adj_points_select, profile_plots_filename_zoom)

    except:
        print(f"huc {huc} has {len(thalweg_points)} thalweg points")

def get_downstream_segments(streams, headwater_col,headwater_id,flag_column,stream_id,stream_type):

    streams[flag_column] = False
    streams['downstream_count'] = -9
    streams.loc[streams[headwater_col]==headwater_id,flag_column] = True
    streams.loc[streams[headwater_col]==headwater_id,'downstream_count'] = 0
    count = 0

    Q = deque(streams.loc[streams[headwater_col]==headwater_id,stream_id].tolist())
    visited = set()

    while Q:
        q = Q.popleft()

        if q in visited:
            continue

        visited.add(q)

        count = count + 1
        if stream_type == 'burnline':

            toNode,DnLevelPat = streams.loc[q,['ToNode','DnLevelPat']]
            downstream_ids = streams.loc[streams['FromNode'] == toNode,:].index.tolist()

            # If multiple downstream_ids are returned select the ids that are along the main flow path (i.e. exclude segments that are diversions)
            if len(set(downstream_ids)) > 1: # special case: remove duplicate NHDPlusIDs

                relevant_ids = [segment for segment in downstream_ids if DnLevelPat == streams.loc[segment,'LevelPathI']]

            else:

                relevant_ids = downstream_ids

        elif stream_type == 'derived':

            toNode = streams.loc[q,['NextDownID']].item()
            relevant_ids = streams.loc[streams[stream_id] == toNode,:].index.tolist()

        streams.loc[relevant_ids,flag_column] = True
        streams.loc[relevant_ids,'downstream_count'] = count

        for i in relevant_ids:

            if i not in visited:
                Q.append(i)

    streams = streams.loc[streams[flag_column],:]

    return streams


def plot_profile(elevation_table,profile_plots_filename):
    num_plots = len(elevation_table.headwater_path.unique())
    unique_rasters = elevation_table.source.unique()
    if num_plots > 3:
        columns = int(np.ceil(num_plots / 3))
    else:
        columns = 1
    # palette = dict(zip(unique_rasters, sns.color_palette(n_colors=len(unique_rasters))))
    # palette.update({'dem_m':'gray'})
    sns.set(style="ticks")
    if len(unique_rasters) > 1:
        g = sns.FacetGrid(elevation_table, col="headwater_path", hue="source", hue_order=['dem_m', 'dem_lat_thal_adj', 'thal_adj_dem'], sharex=False, sharey=False,col_wrap=columns)
    else:
        g = sns.FacetGrid(elevation_table, col="headwater_path", hue="source", sharex=False, sharey=False,col_wrap=columns)
    g.map(sns.lineplot, "index_count", "elevation_ft", palette="tab20c")
    g.set_axis_labels(x_var="Longitudinal Profile (index)", y_var="Elevation (ft)")
    # Iterate thorugh each axis to get individual y-axis bounds
    for ax in g.axes.flat:
        mins = []
        maxes = []
        for line in ax.lines:
            mins = mins + [min(line.get_ydata())]
            maxes = maxes + [max(line.get_ydata())]
        min_y = min(mins) - (max(maxes) - min(mins))/10
        max_y = max(maxes) + (max(maxes) - min(mins))/10
        ax.set_ylim(min_y,max_y)
    # if len(unique_rasters) > 1:
    #     ax.lines[0].set_linestyle("--")
    #     ax.lines[0].set_color('gray')
    # box = ax.get_position()
    # ax.set_position([box.x0, box.y0 + box.height * 0.1,box.width, box.height * 0.9])
    # Adjust the arrangement of the plots
    # g.fig.tight_layout(w_pad=5) #w_pad=2
    g.add_legend()
    # plt.legend(bbox_to_anchor=(1.04,0.5), loc="center left", borderaxespad=0)
    plt.subplots_adjust(bottom=0.25)
    plt.savefig(profile_plots_filename)
    plt.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='generate rating curve plots and tables for FIM and USGS gages')
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-output_dir','--output-dir', help='rating curves output folder', required=True,type=str)
    # parser.add_argument('-rasters','--raster-list',help='list of rasters to be evaluated',required=True,type=str)
    parser.add_argument('-stream_type','--stream-type',help='stream layer to be evaluated',required=True,type=str,choices=['derived','burnline'])
    parser.add_argument('-point_density','--point-density',help='elevation sampling density',required=True,type=str,choices=['midpoints','all_points'])
    parser.add_argument('-th','--elevation_threshold',help='significant elevation drop threshold in meters.',required=True)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    output_dir = args['output_dir']
    # raster_list = args['raster_list']
    stream_type = args['stream_type']
    point_density = args['point_density']
    number_of_jobs = args['number_of_jobs']

    # dem_meters_dir = os.environ.get('dem_meters')

    plots_dir = join(output_dir,'plots')
    os.makedirs(plots_dir, exist_ok=True)
    spatial_dir = os.path.join(output_dir,'spatial_layers')
    os.makedirs(spatial_dir, exist_ok=True)

    # Open log file
    sys.__stdout__ = sys.stdout
    log_file = open(join(output_dir,'thalweg_profile_comparison.log'),"w")
    sys.stdout = log_file

    procs_list = []
    huc_list  = os.listdir(fim_dir)
    for huc in huc_list:
        if huc != 'logs':

            huc_dir = os.path.join(fim_dir,huc)
            flows_grid_boolean_filename = os.path.join(huc_dir,'flows_grid_boolean.tif')
            dem_meters_filename = os.path.join(huc_dir,'dem_meters.tif')
            dem_lateral_thalweg_adj_filename = os.path.join(huc_dir,'dem_lateral_thalweg_adj.tif')
            dem_thalwegCond_filename = os.path.join(huc_dir,'dem_thalwegCond.tif')
            profile_plots_filename = os.path.join(plots_dir,f"profile_drop_plots_{huc}_{point_density}_{stream_type}.png")
            profile_gpkg_filename = os.path.join(spatial_dir,f"thalweg_elevation_changes_{huc}_{point_density}_{stream_type}.gpkg")
            profile_table_filename = os.path.join(spatial_dir,f"thalweg_elevation_changes_{huc}_{point_density}_{stream_type}.csv")

            procs_list.append([huc_dir,stream_type,point_density,huc,dem_meters_filename,dem_lateral_thalweg_adj_filename,dem_thalwegCond_filename,profile_plots_filename,profile_gpkg_filename,profile_table_filename,flows_grid_boolean_filename])

    # Initiate multiprocessing
    print(f"Generating thalweg elevation profiles for {len(procs_list)} hucs using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        # Get elevation values along thalweg for each headwater stream path
        pool.map(compare_thalweg, procs_list)

    # Append all elevation change spatial layers to a single gpkg
    spatial_list  = os.listdir(spatial_dir)
    agg_thalweg_elevations_gpkg_fileName = os.path.join(output_dir, f"agg_thalweg_elevation_changes_{point_density}_{stream_type}.gpkg")
    agg_thalweg_elevation_table_fileName = os.path.join(output_dir, f"agg_thalweg_elevation_changes_{point_density}_{stream_type}.csv")
    for layer in spatial_list:

        huc_gpd = gpd.read_file(os.path.join(spatial_dir,layer))

        # Write aggregate layer
        if os.path.isfile(agg_thalweg_elevations_gpkg_fileName):
            huc_gpd.to_file(agg_thalweg_elevations_gpkg_fileName,driver=getDriver(agg_thalweg_elevations_gpkg_fileName),index=False, mode='a')
        else:
            huc_gpd.to_file(agg_thalweg_elevations_gpkg_fileName,driver=getDriver(agg_thalweg_elevations_gpkg_fileName),index=False)

        del huc_gpd

    # Create csv of elevation table
    huc_table = gpd.read_file(agg_thalweg_elevations_gpkg_fileName)
    huc_table.to_csv(agg_thalweg_elevation_table_fileName,index=False)

    # Close log file
    sys.stdout = sys.__stdout__
    log_file.close()
