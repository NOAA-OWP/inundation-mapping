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
5 save to output table either hydroTable, src.json, or hand_ref_elev_table'''


def crosswalk_usgs_gage(usgs_gages_filename,catchment_pixels_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename,dem_filename,table_filename):

    wbd_buffer = gpd.read_file(wbd_buffer_filename)
    usgs_gages = gpd.read_file(usgs_gages_filename, mask=wbd_buffer)
    catchment_pixels = rasterio.open(catchment_pixels_filename,'r')
    input_flows = gpd.read_file(input_flows_filename)
    input_catchment = gpd.read_file(input_catchment_filename)
    dem = rasterio.open(dem_filename,'r')
    table = pd.read_csv(table_filename)


    # Identify closest HydroID
    closest_catchment = gpd.sjoin(usgs_gages, input_catchment, how='left', op='within').reset_index(drop=True)
    closest_hydro_id = closest_catchment.filter(items=['site_no','HydroID'])

    if input_flows.HydroID.dtype != 'int': input_flows.HydroID = input_flows.HydroID.astype(int)

    # Move USGS gage to stream
    for index, point in usgs_gages.iterrows():

        print (f"usgs gage: {point.site_no}")
        # Get HydroID
        hydro_id = closest_hydro_id.loc[closest_hydro_id.site_no==point.site_no].HydroID.item()

        # Convert headwaterpoint geometries to WKB representation
        wkb_points = dumps(point.geometry)

        # Create pygeos headwaterpoint geometries from WKB representation
        pointbin_geom = pygeos.io.from_wkb(wkb_points)

        # Closest segment to headwater
        closest_stream = input_flows.loc[input_flows.HydroID==hydro_id]
        wkb_closest_stream = dumps(closest_stream.geometry.item())
        streambin_geom = pygeos.io.from_wkb(wkb_closest_stream)

        # Linear reference headwater to closest stream segment
        pointdistancetoline = pygeos.linear.line_locate_point(streambin_geom, pointbin_geom)
        referencedpoint = pygeos.linear.line_interpolate_point(streambin_geom, pointdistancetoline)

        # Convert geometries to wkb representation
        bin_referencedpoint = pygeos.io.to_wkb(referencedpoint)

        # Convert to shapely geometries
        shply_referencedpoint = loads(bin_referencedpoint)

        # Sample rasters at adjusted point
        reference_catpix_id = list(rasterio.sample.sample_gen(catchment_pixels,shply_referencedpoint.coords))[0].item()
        reference_elev = list(rasterio.sample.sample_gen(dem,shply_referencedpoint.coords))[0].item() # round to n decimal places

        # find better way to retrieve cat ID
        print(f"post adjusted catchment pixel ID: {reference_catpix_id}")
        print(f"post adjusted elevation: {reference_elev}")

        # append reference_catpix_id, reference_elev, hydro_id, and point.site_no to table


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Crosswalk USGS sites to HydroID and Catchment Pixel ID')
    parser.add_argument('-gages','--usgs-gages-filename', help='USGS gages', required=True)
    parser.add_argument('-catpix','--catchment-pixels-filename',help='catchment pixel raster',required=True)
    parser.add_argument('-flows','--input-flows-filename', help='DEM derived streams', required=True)
    parser.add_argument('-cat','--input-catchment-filename', help='DEM derived catchments', required=True)
    parser.add_argument('-wbd','--wbd-buffer-filename', help='WBD buffer', required=True)
    parser.add_argument('-dem','--dem-filename', help='Thalweg adjusted DEM', required=True)
    parser.add_argument('-table','--table-filename', help='Table to append data', required=True)

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    catchment_pixels_filename = args['catchment_pixels_filename']
    input_flows_filename = args['input_flows_filename']
    input_catchment_filename = args['input_catchment_filename']
    wbd_buffer_filename = args['wbd_buffer_filename']
    dem_filename = args['dem_filename']
    table_filename = args['table_filename']

    crosswalk_usgs_gage(usgs_gages_filename,catchment_pixels_filename,input_flows_filename,input_catchment_filename,wbd_buffer_filename, dem_filename,table_filename)
