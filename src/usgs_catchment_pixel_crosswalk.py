#!/usr/bin/env python3

import os
import geopandas as gpd
import pandas as pd
from numpy import unique
import rasterio
from rasterstats import zonal_stats
import json
import argparse
import sys
from utils.shared_functions import getDriver
import numpy as np
from os.path import splitext
import pygeos
from shapely.geometry import Point,LineString
from shapely.ops import split
from shapely.wkb import dumps, loads


''' crosswalk USGS gages to catchment pixels
3 linear reference to final stream segments layer
5 save to output table either hydroTable, src.json, or '''

def crosswalk_usgs_gage(usgs_gages_filename,catchment_pixels_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename):


    # usgs_gages_filename='/data/temp/tsg/sample_gage_sites/evaluated_active_gages.shp'
    # catchment_pixels_filename='/data/outputs/usgs_rc_xwalk/04050001/gw_catchments_pixels.tif'
    # input_flows_filename='/data/outputs/usgs_rc_xwalk/04050001/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg'
    # input_catchment_filename='/data/outputs/usgs_rc_xwalk/04050001/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg'
    # wbd_buffer_filename='/data/outputs/usgs_rc_xwalk/04050001/wbd_buffered.gpkg'

    wbd_buffer = gpd.read_file(wbd_buffer_filename)
    usgs_gages = gpd.read_file(usgs_gages_filename, mask=wbd_buffer)
    catchment_pixels = rasterio.open(catchment_pixels_filename,'r')
    input_flows = gpd.read_file(input_flows_filename)
    input_catchment = gpd.read_file(input_catchment_filename)

    ##################### Itentify closest HydroID
    closest_catchment = gpd.sjoin(usgs_gages, input_catchment, how='left', op='within').reset_index(drop=True)
    closest_hydro_id = closest_catchment.filter(items=['site_no','HydroID'])

    if input_flows.HydroID.dtype != 'int': input_flows.HydroID = input_flows.HydroID.astype(int)

    ##################### Move USGS gage to stream
    for index, point in usgs_gages.iterrows():
        print (f"usgs gage: {point.site_no}")
        pre_reference_catpix_id = list(rasterio.sample.sample_gen(catchment_pixels,point.geometry.coords))[0].item()
        # find better way to retrieve cat ID
        print(f"pre adjusted catchment pixel ID: {pre_reference_catpix_id}")
        hydro_id = closest_hydro_id.loc[closest_hydro_id.site_no==point.site_no].HydroID.item()
        # convert headwaterpoint geometries to WKB representation
        wkb_points = dumps(point.geometry)
        # create pygeos headwaterpoint geometries from WKB representation
        pointbin_geom = pygeos.io.from_wkb(wkb_points)
        # Closest segment to headwater
        closest_stream = input_flows.loc[input_flows.HydroID==hydro_id]
        wkb_closest_stream = dumps(closest_stream.geometry.item())
        streambin_geom = pygeos.io.from_wkb(wkb_closest_stream)
        # Linear reference headwater to closest stream segment
        pointdistancetoline = pygeos.linear.line_locate_point(streambin_geom, pointbin_geom)
        referencedpoint = pygeos.linear.line_interpolate_point(streambin_geom, pointdistancetoline)
        # convert geometries to wkb representation
        bin_referencedpoint = pygeos.io.to_wkb(referencedpoint)
        # convert to shapely geometries
        shply_referencedpoint = loads(bin_referencedpoint)
        ##################### Sample from
        reference_catpix_id = list(rasterio.sample.sample_gen(catchment_pixels,shply_referencedpoint.coords))[0].item()
        # find better way to retrieve cat ID
        print(f"post adjusted catchment pixel ID: {reference_catpix_id}")

        # append reference_catpix_id, hydro_id, and point.site_no to file


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Crosswalk USGS sites to HydroID and Catchment Pixel ID')
    parser.add_argument('-gages','--usgs-gages-filename', help='USGS gages', required=True)
    parser.add_argument('-catpix','--catchment-pixels-filename',help='catchment pixel raster',required=True)
    parser.add_argument('-flows','--input-flows-filename', help='DEM derived streams', required=True)
    # parser.add_argument('-r','--output-src-filename', help='Output crosswalked synthetic rating curve table', required=True)
    # parser.add_argument('-j','--output-src-json-filename',help='Output synthetic rating curve json',required=True)
    # parser.add_argument('-t','--output-hydro-table-filename',help='Hydrotable',required=True)

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    catchment_pixels_filename = args['catchment_pixels_filename']
    input_flows_filename = args['input_flows_filename']
    # output_src_filename = args['output_src_filename']
    # output_src_json_filename = args['output_src_json_filename']
    # output_hydro_table_filename = args['output_hydro_table_filename']


    crosswalk_usgs_gage(usgs_gages_filename,catchment_pixels_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename)
