#!/usr/bin/env python3

import sys
import geopandas as gpd
import argparse
from os.path import splitext
from shapely.geometry import MultiPolygon,Polygon,Point
from utils.shared_functions import getDriver

def subset_vector_layers(hucCode,nwm_streams_filename,nhd_streams_filename,nwm_lakes_filename,nld_lines_filename,nwm_catchments_filename,nhd_headwaters_filename,landsea_filename,wbd_filename,wbd_buffer_filename,subset_nhd_streams_filename,subset_nld_lines_filename,subset_nwm_lakes_filename,subset_nwm_catchments_filename,subset_nhd_headwaters_filename,subset_nwm_streams_filename,subset_landsea_filename,extent,great_lakes_filename,wbd_buffer_distance,lake_buffer_distance,dissolveLinks=False):

    hucUnitLength = len(str(hucCode))

    # Get wbd buffer
    wbd = gpd.read_file(wbd_filename)
    wbd_buffer = wbd.copy()
    wbd_buffer.geometry = wbd.geometry.buffer(wbd_buffer_distance,resolution=32)
    projection = wbd_buffer.crs

    great_lakes = gpd.read_file(great_lakes_filename, mask = wbd_buffer).reset_index(drop=True)

    if not great_lakes.empty:
        print("Masking Great Lakes for HUC{} {}".format(hucUnitLength,hucCode),flush=True)

        # Clip excess lake area
        great_lakes = gpd.clip(great_lakes, wbd_buffer)

        # Buffer remaining lake area
        great_lakes.geometry = great_lakes.buffer(lake_buffer_distance)

        # Removed buffered GL from WBD buffer
        wbd_buffer = gpd.overlay(wbd_buffer, great_lakes, how='difference')
        wbd_buffer = wbd_buffer[['geometry']]
        wbd_buffer.to_file(wbd_buffer_filename,driver=getDriver(wbd_buffer_filename),index=False)

    else:
        wbd_buffer = wbd_buffer[['geometry']]
        wbd_buffer.to_file(wbd_buffer_filename,driver=getDriver(wbd_buffer_filename),index=False)

    del great_lakes

    # Clip ocean water polygon for future masking ocean areas (where applicable)
    landsea = gpd.read_file(landsea_filename, mask = wbd_buffer)
    if not landsea.empty:
        landsea.to_file(subset_landsea_filename,driver=getDriver(subset_landsea_filename),index=False)
    del landsea

    # Find intersecting lakes and writeout
    print("Subsetting NWM Lakes for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_lakes = gpd.read_file(nwm_lakes_filename, mask = wbd_buffer)
    nwm_lakes = nwm_lakes.loc[nwm_lakes.Shape_Area < 18990454000.0]

    if not nwm_lakes.empty:
        # Perform fill process to remove holes/islands in the NWM lake polygons
        nwm_lakes = nwm_lakes.explode()
        nwm_lakes_fill_holes=MultiPolygon(Polygon(p.exterior) for p in nwm_lakes['geometry']) # remove donut hole geometries
        # Loop through the filled polygons and insert the new geometry
        for i in range(len(nwm_lakes_fill_holes)):
            nwm_lakes.loc[i,'geometry'] = nwm_lakes_fill_holes[i]
        nwm_lakes.to_file(subset_nwm_lakes_filename,driver=getDriver(subset_nwm_lakes_filename),index=False)
    del nwm_lakes

    # Find intersecting levee lines
    print("Subsetting NLD levee lines for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nld_lines = gpd.read_file(nld_lines_filename, mask = wbd_buffer)
    if not nld_lines.empty:
        nld_lines.to_file(subset_nld_lines_filename,driver=getDriver(subset_nld_lines_filename),index=False)
    del nld_lines

    # Subset nhd headwaters
    print("Subsetting NHD Headwater Points for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nhd_headwaters = gpd.read_file(nhd_headwaters_filename, mask = wbd_buffer)
    if extent == 'MS':
        nhd_headwaters = nhd_headwaters.loc[nhd_headwaters.mainstem==1]

    if len(nhd_headwaters) > 0:
        nhd_headwaters.to_file(subset_nhd_headwaters_filename,driver=getDriver(subset_nhd_headwaters_filename),index=False)
    else:
        print ("No headwater point(s) within HUC " + str(hucCode) +  " boundaries.")
        sys.exit(0)
    del nhd_headwaters

    # Subset nhd streams
    print("Querying NHD Streams for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nhd_streams = gpd.read_file(nhd_streams_filename, mask = wbd_buffer)

    if extent == 'MS':
        nhd_streams = nhd_streams.loc[nhd_streams.mainstem==1]

    if len(nhd_streams) > 0:
        # Find incoming stream segments (to WBD buffer) and identify which are upstream
        threshold_segments = gpd.overlay(nhd_streams, wbd_buffer, how='symmetric_difference')
        from_list = threshold_segments.FromNode.to_list()
        to_list = nhd_streams.ToNode.to_list()
        missing_segments = list(set(from_list) - set(to_list))

        # Remove incoming stream segment so it won't be routed as outflow during hydroconditioning
        nhd_streams = nhd_streams.loc[~nhd_streams.FromNode.isin(missing_segments)]

        nhd_streams.to_file(subset_nhd_streams_filename,driver=getDriver(subset_nhd_streams_filename),index=False)
    else:
        print ("No NHD streams within HUC " + str(hucCode) +  " boundaries.")
        sys.exit(0)
    del nhd_streams

    # Find intersecting nwm_catchments
    print("Subsetting NWM Catchments for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_catchments = gpd.read_file(nwm_catchments_filename, mask = wbd_buffer)
    if extent == 'MS':
        nwm_catchments = nwm_catchments.loc[nwm_catchments.mainstem==1]

    if len(nwm_catchments) > 0:
        nwm_catchments.to_file(subset_nwm_catchments_filename,driver=getDriver(subset_nwm_catchments_filename),index=False)
    else:
        print ("No NHD catchments within HUC " + str(hucCode) +  " boundaries.")
        sys.exit(0)
    del nwm_catchments

    # Subset nwm streams
    print("Subsetting NWM Streams and deriving headwaters for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_streams = gpd.read_file(nwm_streams_filename, mask = wbd_buffer)
    if extent == 'MS':
        nwm_streams = nwm_streams.loc[nwm_streams.mainstem==1]
    nwm_streams.to_file(subset_nwm_streams_filename,driver=getDriver(subset_nwm_streams_filename),index=False)
    del nwm_streams

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-d','--hucCode', help='HUC boundary ID', required=True,type=str)
    parser.add_argument('-w','--nwm-streams', help='NWM flowlines', required=True)
    parser.add_argument('-s','--nhd-streams',help='NHDPlus HR burnline',required=True)
    parser.add_argument('-l','--nwm-lakes', help='NWM Lakes', required=True)
    parser.add_argument('-r','--nld-lines', help='Levee vectors to use within project path', required=True)
    parser.add_argument('-g','--wbd',help='HUC boundary',required=True)
    parser.add_argument('-f','--wbd-buffer',help='Buffered HUC boundary',required=True)
    parser.add_argument('-m','--nwm-catchments', help='NWM catchments', required=True)
    parser.add_argument('-y','--nhd-headwaters',help='NHD headwaters',required=True)
    parser.add_argument('-v','--landsea',help='LandSea - land boundary',required=True)
    parser.add_argument('-c','--subset-nhd-streams',help='NHD streams subset',required=True)
    parser.add_argument('-z','--subset-nld-lines',help='Subset of NLD levee vectors for HUC',required=True)
    parser.add_argument('-a','--subset-lakes',help='NWM lake subset',required=True)
    parser.add_argument('-n','--subset-catchments',help='NWM catchments subset',required=True)
    parser.add_argument('-e','--subset-nhd-headwaters',help='NHD headwaters subset',required=True,default=None)
    parser.add_argument('-b','--subset-nwm-streams',help='NWM streams subset',required=True)
    parser.add_argument('-x','--subset-landsea',help='LandSea subset',required=True)
    parser.add_argument('-extent','--extent',help='FIM extent',required=True)
    parser.add_argument('-gl','--great-lakes-filename',help='Great Lakes layer',required=True)
    parser.add_argument('-wb','--wbd-buffer-distance',help='WBD Mask buffer distance',required=True,type=int)
    parser.add_argument('-lb','--lake-buffer-distance',help='Great Lakes Mask buffer distance',required=True,type=int)
    parser.add_argument('-o','--dissolve-links',help='remove multi-line strings',action="store_true",default=False)


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
    dissolveLinks = args['dissolve_links']

    subset_vector_layers(hucCode,nwm_streams_filename,nhd_streams_filename,nwm_lakes_filename,nld_lines_filename,nwm_catchments_filename,nhd_headwaters_filename,landsea_filename,wbd_filename,wbd_buffer_filename,subset_nhd_streams_filename,subset_nld_lines_filename,subset_nwm_lakes_filename,subset_nwm_catchments_filename,subset_nhd_headwaters_filename,subset_nwm_streams_filename,subset_landsea_filename,extent,great_lakes_filename,wbd_buffer_distance,lake_buffer_distance,dissolveLinks)
