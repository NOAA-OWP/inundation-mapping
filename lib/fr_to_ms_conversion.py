#!/usr/bin/env python3

'''
 Description: Create 'mainstems' stream network using AHPs forecasting pointsa and incoming streams
'''

import sys
import os
import datetime
import argparse
import pandas as pd
import geopandas as gpd
import rasterio.mask
from shapely.geometry import Point

split_flows_fileName      = sys.argv[1]
split_points_fileName     = sys.argv[2]
ahps_points               = sys.argv[3]
MSsplit_flows_fileName    = sys.argv[4]
MSsplit_points_fileName   = sys.argv[5]
fdrFR                     = sys.argv[6]
nhddemFR                  = sys.argv[7]
slpFR                     = sys.argv[8]
fdrMSname                 = sys.argv[9]
nhddemMSname              = sys.argv[10]
slpMSname                 = sys.argv[11]
wbd_fileName              = sys.argv[12]
floodAOIbuf               = sys.argv[13]

# create output layer names
outfolder = os.path.dirname(split_flows_fileName)

wbd = gpd.read_file(wbd_fileName)

# Identify origination points for AHPS and inlets and snap them onto ModelStream lines.
print ("Using NWM forecast and inlet points")
unsnapped_pts = gpd.read_file(ahps_points, mask = wbd)

split_flows = gpd.read_file(split_flows_fileName)

intersecting = split_flows.crosses(wbd.geometry[0])
incoming_flows = split_flows.loc[intersecting,:]
if len(incoming_flows) > 0:
    incoming_points_list = []
    for i,linesting in enumerate(incoming_flows.geometry):
        incoming_points_list = incoming_points_list + [linesting.coords[-1]]

geometry = [Point(xy) for xy in zip(incoming_points_list)]
incoming_points = gpd.GeoDataFrame({'feature_id' : 0 ,'nwsid' : 'huc8_incoming' ,'geometry':geometry}, crs=split_flows.crs, geometry='geometry')


if (len(unsnapped_pts) > 0) or (len(incoming_points) > 0):

    split_points  = gpd.read_file(split_points_fileName)

    if len(unsnapped_pts) > 0:
        print ("Snapping forecasting points to FR stream network")
        streamlines_union = split_flows.geometry.unary_union
        snapped_geoms = []
        snappedpoints_df = pd.DataFrame(unsnapped_pts).drop(columns=['geometry'])

        # snap lines to streams
        for i in range(len(unsnapped_pts)):
            snapped_geoms.append(streamlines_union.interpolate(streamlines_union.project(unsnapped_pts.geometry[i])))

        snappedpoints_df['geometry'] = snapped_geoms
        snapped_points = gpd.GeoDataFrame(snappedpoints_df,crs=split_flows.crs)

    if (len(incoming_points) > 0) and (len(unsnapped_pts) > 0):
        snapped_points = snapped_points.append(incoming_points).reset_index(drop=True)
    elif len(incoming_points) > 0:
        snapped_points = incoming_points.copy().reset_index(drop=True)

    # get HydroID of stream segment that intersects with forecasting point
    print ("Tracing MS network from FR streams using AHPS points as starting points")
    streamlinesID = split_flows.filter(items=['HydroID', 'geometry'])

    def nearest_linestring(points, streamlines):
        idx = streamlines.geometry.distance(points).idxmin()
        return streamlines.loc[idx, 'HydroID']

    MSHydroIDs_list = snapped_points.geometry.apply(nearest_linestring, streamlines=streamlinesID)
    snapped_points['HydroID'] = MSHydroIDs_list

    # Select only segments downstream of forecasting points; use NextDownID to trace network and subset MS stream network
    downstreamnetwork = split_flows.filter(items=['HydroID', 'NextDownID']).astype('int64')
    downIDlist = []

    print ('Building MS network from {} forcasting points'.format(len(MSHydroIDs_list)))

    for startID in MSHydroIDs_list:
        terminalID = downstreamnetwork.set_index('HydroID').loc[startID][0]
        downIDlist.append(startID)
        while terminalID != -1:
            downIDlist.append(terminalID)
            terminalID = downstreamnetwork.set_index('HydroID').loc[terminalID][0]

    print ('Removing duplicate HydroIDs')

    uniqueHydroIDs = set(downIDlist)
    MSsplit_flows_gdf = split_flows[split_flows['HydroID'].isin(uniqueHydroIDs)]
    MSsplit_points_gdf = split_points[split_points['id'].isin(uniqueHydroIDs)]

    print('Writing vector outputs ...')

    if os.path.isfile(MSsplit_flows_fileName):
        os.remove(MSsplit_flows_fileName)

    MSsplit_flows_gdf.to_file(MSsplit_flows_fileName,driver='GPKG',index=False)

    if os.path.isfile(MSsplit_points_fileName):
        os.remove(MSsplit_points_fileName)

    MSsplit_points_gdf.to_file(MSsplit_points_fileName,driver='GPKG',index=False)

    # Limit the rasters to the buffer distance around the draft streams.
    print ("Limiting rasters to buffer area ({} meters) around model streams".format(str(floodAOIbuf)))
    print ("              Creating processing zone (buffer area).")

    MSsplit_flows_gdf_buffered = MSsplit_flows_gdf.unary_union.buffer(int(floodAOIbuf))

    print('Writing raster outputs ...')

    # Mask nhddem
    with rasterio.open(nhddemFR) as src:
        out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open(os.path.join(os.path.dirname(nhddemFR), nhddemMSname), "w", **out_meta) as dest:
        dest.write(out_image)

    # Mask fdr
    with rasterio.open(fdrFR) as src:
        out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open(os.path.join(os.path.dirname(fdrFR), fdrMSname), "w", **out_meta) as dest:
        dest.write(out_image)

    # Mask slope
    with rasterio.open(slpFR) as src:
        out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open(os.path.join(os.path.dirname(slpFR), slpMSname), "w", **out_meta) as dest:
        dest.write(out_image)
