#!/usr/bin/env python3

import os, sys
import geopandas as gpd
import pandas as pd
import argparse
from shapely.geometry import MultiPolygon,Polygon
from utils.shared_functions import getDriver, mem_profile
import rasterio as rio

@mem_profile
def subset_vector_layers(hucCode, nwm_streams_filename, nhd_streams_filename, nwm_lakes_filename, nld_lines_filename, nwm_catchments_filename, nhd_headwaters_filename, landsea_filename, wbd_filename, wbd_buffer_filename, subset_nhd_streams_filename, subset_nld_lines_filename, subset_nwm_lakes_filename, subset_nwm_catchments_filename, subset_nhd_headwaters_filename, subset_nwm_streams_filename, subset_landsea_filename, extent, great_lakes_filename, wbd_buffer_distance, lake_buffer_distance, dem_filename):

    hucUnitLength = len(str(hucCode))

    with rio.open(dem_filename) as dem_raster:
        dem_cellsize = max(dem_raster.res)

    # Get wbd buffer
    wbd = gpd.read_file(wbd_filename)
    wbd_buffer = wbd.copy()
    wbd_buffer.geometry = wbd.geometry.buffer(wbd_buffer_distance, resolution=32)

    # Make the streams buffer smaller than the wbd_buffer so streams don't reach the edge of the DEM
    wbd_buffer_filename_split = os.path.splitext(wbd_buffer_filename)
    wbd_streams_buffer_filename = wbd_buffer_filename_split[0] + '_streams' + wbd_buffer_filename_split[1]
    wbd_streams_buffer = wbd.copy()
    wbd_streams_buffer.geometry = wbd.geometry.buffer(wbd_buffer_distance-2*dem_cellsize, resolution=32)

    # projection = wbd_buffer.crs

    great_lakes = gpd.read_file(great_lakes_filename, mask=wbd_buffer).reset_index(drop=True)

    if not great_lakes.empty:
        print("Masking Great Lakes for HUC{} {}".format(hucUnitLength, hucCode), flush=True)

        # Clip excess lake area
        great_lakes = gpd.clip(great_lakes, wbd_buffer)
        great_lakes_streams = gpd.clip(great_lakes, wbd_streams_buffer)

        # Buffer remaining lake area
        great_lakes.geometry = great_lakes.buffer(lake_buffer_distance)
        great_lakes_streams.geometry = great_lakes_streams.buffer(lake_buffer_distance)

        # Removed buffered GL from WBD buffer
        wbd_buffer = gpd.overlay(wbd_buffer, great_lakes, how='difference')
        wbd_streams_buffer = gpd.overlay(wbd_streams_buffer, great_lakes, how='difference')

    wbd_buffer = wbd_buffer[['geometry']]
    wbd_streams_buffer = wbd_streams_buffer[['geometry']]
    wbd_buffer.to_file(wbd_buffer_filename, driver=getDriver(wbd_buffer_filename), index=False)
    wbd_streams_buffer.to_file(wbd_streams_buffer_filename, driver=getDriver(wbd_buffer_filename), index=False)

    del great_lakes

    # Clip ocean water polygon for future masking ocean areas (where applicable)
    landsea = gpd.read_file(landsea_filename, mask=wbd_buffer)
    if not landsea.empty:
        landsea.to_file(subset_landsea_filename, driver=getDriver(subset_landsea_filename), index=False)
    del landsea

    # Find intersecting lakes and writeout
    print("Subsetting NWM Lakes for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_lakes = gpd.read_file(nwm_lakes_filename, mask=wbd_buffer)
    nwm_lakes = nwm_lakes.loc[nwm_lakes.Shape_Area < 18990454000.0]

    if not nwm_lakes.empty:
        # Perform fill process to remove holes/islands in the NWM lake polygons
        nwm_lakes = nwm_lakes.explode()
        nwm_lakes_fill_holes=MultiPolygon(Polygon(p.exterior) for p in nwm_lakes['geometry']) # remove donut hole geometries
        # Loop through the filled polygons and insert the new geometry
        for i in range(len(nwm_lakes_fill_holes)):
            nwm_lakes.loc[i,'geometry'] = nwm_lakes_fill_holes[i]
        nwm_lakes.to_file(subset_nwm_lakes_filename, driver=getDriver(subset_nwm_lakes_filename), index=False)
    del nwm_lakes

    # Find intersecting levee lines
    print("Subsetting NLD levee lines for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nld_lines = gpd.read_file(nld_lines_filename, mask=wbd_buffer)
    if not nld_lines.empty:
        nld_lines.to_file(subset_nld_lines_filename, driver=getDriver(subset_nld_lines_filename), index=False)
    del nld_lines

    # Subset nhd headwaters
    print("Subsetting NHD Headwater Points for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nhd_headwaters = gpd.read_file(nhd_headwaters_filename, mask=wbd_streams_buffer)
    if extent == 'MS':
        # special cases: missing MS headwater points
        # 02030101 (Hudson River) → Resolved (added 2 headwaters on DEM divide)
        # 07060001 (Mississippi River) → Resolved (force the non-mainstem NHD headwater point to be included)
        # 07060003 (Mississippi River) → Resolved  (added 2 headwaters on DEM divide)
        # 08040207 (Ouachita River) → Resolved (added missing NHD headwater at HUC8 boundary)
        # 05120108 (Wabash River) → Resolved  (added 2 headwaters on DEM divide)

        nhd_headwaters_manual_include_all = {'07060001':['22000400022137']}
        nhd_headwaters_manual_exclude_all = {} #{'05120108':['24001301276670','24001301372152']}
        if hucCode in nhd_headwaters_manual_include_all:
            nhd_headwaters_manual_include = nhd_headwaters_manual_include_all[hucCode]
            print('!!Manually including MS headwater point (address missing MS bug)')
            nhd_headwaters = nhd_headwaters.loc[(nhd_headwaters.mainstem==1) | (nhd_headwaters.site_id.isin(nhd_headwaters_manual_include))]
        elif hucCode in nhd_headwaters_manual_exclude_all:
            nhd_headwaters_manual_exclude = nhd_headwaters_manual_exclude_all[hucCode]
            print('!!Manually removing MS headwater point (address missing MS bug)')
            nhd_headwaters = nhd_headwaters.loc[(nhd_headwaters.mainstem==1) & (~nhd_headwaters.site_id.isin(nhd_headwaters_manual_exclude))]
        else:
            nhd_headwaters = nhd_headwaters.loc[(nhd_headwaters.mainstem==1)]
            
        # dataframe below contains new nhd_headwater points to add to the huc subset
        df_manual_add = pd.DataFrame({
            'huc':['08040207','05120108','05120108','07060003','07060003','02030101','02030101'],
            'site_id': ['20000800111111','11111111111112','11111111111113','11111111111114','11111111111115','11111111111116','11111111111116'],
            'pt_type': ['manual_add','manual_add','manual_add','manual_add','manual_add','manual_add','manual_add'],
            'mainstem': [True,True,True,True,True,True,True],
            'Latitude': [32.568401,40.451615,40.451776,42.786042,42.785914,41.196772,41.196883],
            'Longitude': [-92.144639,-86.894058,-86.894025,-91.092300,-91.092294,-73.928789,-73.928848]})
        if str(hucCode) in df_manual_add.huc.values:
            print('!!Manually adding additional MS headwater point (address missing MS bug)')
            df_manual_add = df_manual_add.loc[df_manual_add.huc==str(hucCode)]
            df_manual_add.drop(['huc'], axis=1, inplace=True)
            gdf_manual = gpd.GeoDataFrame(df_manual_add, geometry=gpd.points_from_xy(df_manual_add.Longitude, df_manual_add.Latitude, crs="EPSG:4326"))
            nhd_headwaters_crs = nhd_headwaters.crs
            gdf_manual.to_crs(nhd_headwaters_crs, inplace=True) 
            nhd_headwaters = nhd_headwaters.append(gdf_manual)

    if len(nhd_headwaters) > 0:
        nhd_headwaters.to_file(subset_nhd_headwaters_filename, driver=getDriver(subset_nhd_headwaters_filename), index=False)
    else:
        print ("No headwater point(s) within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nhd_headwaters

    # Subset nhd streams
    print("Querying NHD Streams for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nhd_streams = gpd.read_file(nhd_streams_filename, mask=wbd_streams_buffer)

    if extent == 'MS':
        nhd_streams = nhd_streams.loc[nhd_streams.mainstem==1]

    if len(nhd_streams) > 0:
        nhd_streams = gpd.clip(nhd_streams, wbd_streams_buffer)

        nhd_streams.to_file(subset_nhd_streams_filename, driver=getDriver(subset_nhd_streams_filename), index=False)
    else:
        print ("No NHD streams within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nhd_streams

    # Find intersecting nwm_catchments
    print("Subsetting NWM Catchments for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nwm_catchments = gpd.read_file(nwm_catchments_filename, mask=wbd_buffer)
    if extent == 'MS':
        nwm_catchments = nwm_catchments.loc[nwm_catchments.mainstem==1]

    if len(nwm_catchments) > 0:
        nwm_catchments.to_file(subset_nwm_catchments_filename, driver=getDriver(subset_nwm_catchments_filename), index=False)
    else:
        print ("No NHD catchments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nwm_catchments

    # Subset nwm streams
    print("Subsetting NWM Streams and deriving headwaters for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nwm_streams = gpd.read_file(nwm_streams_filename, mask=wbd_buffer)
    if extent == 'MS':
        nwm_streams = nwm_streams.loc[nwm_streams.mainstem==1]
    if len(nwm_streams) > 0:
        nwm_streams = gpd.clip(nwm_streams, wbd_streams_buffer)

        nwm_streams.to_file(subset_nwm_streams_filename, driver=getDriver(subset_nwm_streams_filename), index=False)
    else:
        print ("No NWM stream segments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nwm_streams


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-d','--hucCode', help='HUC boundary ID', required=True,type=str)
    parser.add_argument('-w','--nwm-streams', help='NWM flowlines', required=True)
    parser.add_argument('-s','--nhd-streams',help='NHDPlus HR burnline',required=True)
    parser.add_argument('-l','--nwm-lakes', help='NWM Lakes', required=True)
    parser.add_argument('-r','--nld-lines', help='Levee vectors to use within project path', required=True)
    parser.add_argument('-g','--wbd', help='HUC boundary', required=True)
    parser.add_argument('-i','--dem-filename', help='DEM filename', required=True)
    parser.add_argument('-f','--wbd-buffer', help='Buffered HUC boundary', required=True)
    parser.add_argument('-m','--nwm-catchments', help='NWM catchments', required=True)
    parser.add_argument('-y','--nhd-headwaters', help='NHD headwaters', required=True)
    parser.add_argument('-v','--landsea', help='LandSea - land boundary', required=True)
    parser.add_argument('-c','--subset-nhd-streams', help='NHD streams subset', required=True)
    parser.add_argument('-z','--subset-nld-lines', help='Subset of NLD levee vectors for HUC', required=True)
    parser.add_argument('-a','--subset-lakes', help='NWM lake subset', required=True)
    parser.add_argument('-n','--subset-catchments', help='NWM catchments subset', required=True)
    parser.add_argument('-e','--subset-nhd-headwaters', help='NHD headwaters subset', required=True, default=None)
    parser.add_argument('-b','--subset-nwm-streams', help='NWM streams subset', required=True)
    parser.add_argument('-x','--subset-landsea', help='LandSea subset', required=True)
    parser.add_argument('-extent','--extent', help='FIM extent', required=True)
    parser.add_argument('-gl','--great-lakes-filename', help='Great Lakes layer', required=True)
    parser.add_argument('-wb','--wbd-buffer-distance', help='WBD Mask buffer distance', required=True, type=int)
    parser.add_argument('-lb','--lake-buffer-distance', help='Great Lakes Mask buffer distance', required=True, type=int)

    args = vars(parser.parse_args())

    hucCode = args['hucCode']
    nwm_streams_filename = args['nwm_streams']
    nhd_streams_filename = args['nhd_streams']
    nwm_lakes_filename = args['nwm_lakes']
    nld_lines_filename = args['nld_lines']
    wbd_filename = args['wbd']
    wbd_buffer_filename = args['wbd_buffer']
    nwm_catchments_filename = args['nwm_catchments']
    nhd_headwaters_filename = args['nhd_headwaters']
    landsea_filename = args['landsea']
    subset_nhd_streams_filename = args['subset_nhd_streams']
    subset_nld_lines_filename = args['subset_nld_lines']
    subset_nwm_lakes_filename = args['subset_lakes']
    subset_nwm_catchments_filename = args['subset_catchments']
    subset_nhd_headwaters_filename = args['subset_nhd_headwaters']
    subset_nwm_streams_filename = args['subset_nwm_streams']
    subset_landsea_filename = args['subset_landsea']
    extent = args['extent']
    great_lakes_filename = args['great_lakes_filename']
    wbd_buffer_distance = args['wbd_buffer_distance']
    lake_buffer_distance  = args['lake_buffer_distance']
    dem_filename = args['dem_filename']

    subset_vector_layers(hucCode,nwm_streams_filename,nhd_streams_filename,nwm_lakes_filename,nld_lines_filename,nwm_catchments_filename,nhd_headwaters_filename,landsea_filename,wbd_filename,wbd_buffer_filename,subset_nhd_streams_filename,subset_nld_lines_filename,subset_nwm_lakes_filename,subset_nwm_catchments_filename,subset_nhd_headwaters_filename,subset_nwm_streams_filename,subset_landsea_filename,extent,great_lakes_filename,wbd_buffer_distance,lake_buffer_distance, dem_filename)
