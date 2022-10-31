#!/usr/bin/env python3

import os
import sys
import pandas as pd
import geopandas as gpd
import argparse
import rasterio as rio

from shapely.geometry import MultiPolygon,Polygon
from utils.shared_functions import getDriver, mem_profile

@mem_profile
def subset_vector_layers(subset_nwm_lakes, 
                         subset_nwm_streams,
                         subset_nhd_streams,
                         hucCode,
                         subset_nhd_headwaters,
                         wbd_buffer_filename,
                         wbd_filename,
                         dem_filename,
                         nwm_lakes,
                         nwm_catchments,
                         subset_nwm_catchments,
                         nld_lines,
                         nhd_streams,
                         landsea,
                         nwm_streams,
                         subset_landsea,
                         nhd_headwaters,
                         subset_nld_lines,
                         great_lakes,
                         lake_buffer_distance,
                         wbd_buffer_distance,
                         levee_protected_areas,
                         subset_levee_protected_areas):
        
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
    wbd_streams_buffer.geometry = wbd.geometry.buffer(wbd_buffer_distance-3*dem_cellsize, resolution=32)

    # projection = wbd_buffer.crs

    great_lakes = gpd.read_file(great_lakes, mask=wbd_buffer).reset_index(drop=True)

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
    landsea = gpd.read_file(landsea, mask=wbd_buffer)
    if not landsea.empty:
        landsea.to_file(subset_landsea, driver = getDriver(subset_landsea), index=False)
    del landsea

    # Clip levee-protected areas polygons for future masking ocean areas (where applicable)
    print("Subsetting Levee Protected Areas", flush=True)
    levee_protected_areas = gpd.read_file(levee_protected_areas, mask=wbd_buffer)
    if not levee_protected_areas.empty:
        levee_protected_areas = levee_protected_areas.to_crs('ESRI:102039')
        levee_protected_areas.to_file(subset_levee_protected_areas, driver = getDriver
                                      (subset_levee_protected_areas), index=False)
    del levee_protected_areas

    # Find intersecting lakes and writeout
    print("Subsetting NWM Lakes for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nwm_lakes = gpd.read_file(nwm_lakes, mask = wbd_buffer)
    nwm_lakes = nwm_lakes.loc[nwm_lakes.Shape_Area < 18990454000.0]

    if not nwm_lakes.empty:
        # Perform fill process to remove holes/islands in the NWM lake polygons
        nwm_lakes = nwm_lakes.explode()
        nwm_lakes_fill_holes = MultiPolygon(Polygon(p.exterior) for p in nwm_lakes['geometry']) # remove donut hole geometries
        # Loop through the filled polygons and insert the new geometry
        for i in range(len(nwm_lakes_fill_holes)):
            nwm_lakes.loc[i, 'geometry'] = nwm_lakes_fill_holes[i]
        nwm_lakes.to_file(subset_nwm_lakes, driver = getDriver(subset_nwm_lakes), index=False)
    del nwm_lakes

    # Find intersecting levee lines
    print("Subsetting NLD levee lines for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nld_lines = gpd.read_file(nld_lines, mask = wbd_buffer)
    if not nld_lines.empty:
        nld_lines.to_file(subset_nld_lines, driver = getDriver(subset_nld_lines), index=False)
    del nld_lines

    # Subset nhd headwaters
    print("Subsetting NHD Headwater Points for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nhd_headwaters = gpd.read_file(nhd_headwaters, mask=wbd_streams_buffer)

    if len(nhd_headwaters) > 0:
        nhd_headwaters.to_file(subset_nhd_headwaters, driver=getDriver(subset_nhd_headwaters), index=False)
    else:
        print ("No headwater point(s) within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nhd_headwaters

    # Subset nhd streams
    print("Querying NHD Streams for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nhd_streams = gpd.read_file(nhd_streams, mask=wbd_streams_buffer)

    if len(nhd_streams) > 0:
        nhd_streams = gpd.clip(nhd_streams, wbd_streams_buffer)

        nhd_streams.to_file(subset_nhd_streams, 
                            driver = getDriver(subset_nhd_streams), index=False)
    else:
        print ("No NHD streams within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nhd_streams

    # Find intersecting nwm_catchments
    print("Subsetting NWM Catchments for HUC{} {}".format(hucUnitLength, hucCode), flush=True)
    nwm_catchments = gpd.read_file(nwm_catchments, mask=wbd_buffer)

    if len(nwm_catchments) > 0:
        nwm_catchments.to_file(subset_nwm_catchments, driver=getDriver(subset_nwm_catchments), index=False)
    else:
        print ("No NHD catchments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nwm_catchments

    # Subset nwm streams

    print("Subsetting NWM Streams and deriving headwaters for HUC{} {}".format(hucUnitLength, hucCode), flush=True)

    nwm_streams = gpd.read_file(nwm_streams, mask = wbd)
    nwm_streams = gpd.clip(nwm_streams, wbd)

     # NWM can have duplicate records, but appear to always be identical duplicates
    nwm_streams.drop_duplicates(subset="ID", keep="first", inplace=True)


    if len(nwm_streams) > 0:
        nwm_streams = gpd.clip(nwm_streams, wbd_streams_buffer)

        nwm_streams.to_file(subset_nwm_streams, driver=getDriver(subset_nwm_streams), index=False)
    else:
        print ("No NWM stream segments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nwm_streams


if __name__ == '__main__':

    #print(sys.argv)

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-a','--subset-nwm-lakes', help='NWM lake subset', 
                        required=True)
    parser.add_argument('-b','--subset-nwm-streams', help='NWM streams subset', 
                        required=True)
    parser.add_argument('-c','--subset-nhd-streams', help='NHD streams subset', 
                        required=True)
    parser.add_argument('-d','--hucCode', help='HUC boundary ID', required=True,
                        type=str)
    parser.add_argument('-e','--subset-nhd-headwaters', help='NHD headwaters subset', 
                        required=True, default=None)
    parser.add_argument('-f','--wbd_buffer_filename', help='Buffered HUC boundary', 
                        required=True)
    parser.add_argument('-g','--wbd-filename', help='HUC boundary', required=True)
    parser.add_argument('-i','--dem-filename', help='DEM filename', required=True)
    parser.add_argument('-l','--nwm-lakes', help='NWM Lakes', required=True)    
    parser.add_argument('-m','--nwm-catchments', help='NWM catchments',
                        required=True)	 
    parser.add_argument('-n','--subset-nwm-catchments', help='NWM catchments subset', 
                        required=True)
    parser.add_argument('-r','--nld-lines', help='Levee vectors to use within project path',
                        required=True)
    parser.add_argument('-s','--nhd-streams', help='NHDPlus HR burnline',
                        required=True)
    parser.add_argument('-v','--landsea', help='LandSea - land boundary',
                        required=True)	 
    parser.add_argument('-w','--nwm-streams', help='NWM flowlines',
                        required=True)
    parser.add_argument('-x','--subset-landsea', help='LandSea subset', 
                        required=True)
    parser.add_argument('-y','--nhd-headwaters', help='NHD headwaters',
                        required=True)	 
    parser.add_argument('-z','--subset-nld-lines', help='Subset of NLD levee vectors for HUC',
                        required=True)
    parser.add_argument('-gl','--great-lakes', help='Great Lakes layer', 
                        required=True)
    parser.add_argument('-lb','--lake-buffer-distance', help='Great Lakes Mask buffer distance',
                        required=True, type=int)
    parser.add_argument('-wb','--wbd-buffer-distance', help='WBD Mask buffer distance', 
                        required=True, type=int)
    parser.add_argument('-lpf','--levee-protected-areas', 
                        help='Levee-protected areas filename', required=True)    
    parser.add_argument('-lps','--subset-levee-protected-areas', 
                        help='Levee-protected areas subset', required=True)
    
    args = vars(parser.parse_args())

    subset_vector_layers(**args)
