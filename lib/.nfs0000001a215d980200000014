#!/usr/bin/env python3
'''----------------------------------------------------------------------------
 Tool Name:   FR to MS Network Conversion
 Source Name: FRtoMSconversion.py

 Inputs:    
            projection    
            streamlines
            AHPs_points
Outputs:    
            demDerived_reaches_splitMS.gpkg
            demDerived_reaches_split_pointsMS.gpkg 
                                        
 Description: Create model streams for 'Mainstem' approach
----------------------------------------------------------------------------'''

import sys
import os
import datetime
import argparse
import pandas as pd
import geopandas as gpd

def trace():  
    import traceback, inspect
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # script name + line number
    line = tbinfo.split(", ")[1]
    filename = inspect.getfile(inspect.currentframe())
    # Get Python syntax error
    synerror = traceback.format_exc().splitlines()[-1]
    return line, filename, synerror                                            
           
projection                = sys.argv[1]
streamlines               = sys.argv[2]
streamlines_splitpoints   = sys.argv[3]
AHPs_points               = sys.argv[4]
MSsplit_flows_fileName    = sys.argv[5]
MSsplit_points_fileName   = sys.argv[6]   
fdrFR                     = sys.argv[7]
nhddemFR                  = sys.argv[8]   
fdrMSname                 = sys.argv[9]
nhddemMSname              = sys.argv[10]   
floodAOIbuf               = sys.argv[11]

# create output layer names
outfolder = os.path.dirname(streamlines)
  
# tool constants
# floodAOIbuf = 7000 # "7000 METERS"

# Identify origination points for AHPS and inlets and snap them onto ModelStream lines.
print ("{}Using NWM forecast and inlet points".format("     "))
unsnapped_pts = gpd.read_file(AHPs_points, crs = projection)
streamlines_gpd = gpd.read_file(streamlines, crs = projection)
streamlines_splitpoints_gpd  = gpd.read_file(streamlines_splitpoints, crs = projection)

print ("              Snapping forecasting points to FR stream network")
streamlines_union = streamlines_gpd.geometry.unary_union
snapped_geoms = []
snappedpoints_df = pd.DataFrame(unsnapped_pts).drop(columns=['geometry', 'X_cor_', 'Y_cor_'])
# snap lines to streams
for i in range(len(unsnapped_pts)):
    snapped_geoms.append(streamlines_union.interpolate(streamlines_union.project(unsnapped_pts.geometry[i])))

snappedpoints_df['geometry'] = snapped_geoms
snapped_points = gpd.GeoDataFrame(snappedpoints_df, crs = projection)

# get HydroID of stream segment that intersects with forecasting point
print ("{}Tracing MS network from FR streams using AHPS points as starting points".format("     "))
streamlinesID = streamlines_gpd.filter(items=['HydroID', 'geometry'])
# sjoin doesn't always return HydroIDs even though it is snapped; use the function below instead
# snapped_pointswID = gpd.sjoin(snapped_points, streamlinesID, how='left', op='intersects').drop(['index_right'], axis=1)
def nearest_linestring(points, streamlines):
    idx = streamlines.geometry.distance(points).idxmin()
    return streamlines.loc[idx, 'HydroID']
# this needs to be tested more to make sure it always gets the correct segment
MSHydroIDs_list = snapped_points.geometry.apply(nearest_linestring, streamlines=streamlinesID)
snapped_points['HydroID'] = MSHydroIDs_list

# Select only segments downstream of forecasting points; use NextDownID to trace network and subset MS stream network
downstreamnetwork = streamlines_gpd.filter(items=['HydroID', 'NextDownID']).astype('int64')
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
MSsplit_flows_gdf = streamlines_gpd[streamlines_gpd['HydroID'].isin(uniqueHydroIDs)]
MSsplit_points_gdf = streamlines_splitpoints_gpd[streamlines_splitpoints_gpd['id'].isin(uniqueHydroIDs)]
# MSsplit_flows_gdf.to_file(os.path.join(outfolder, 'demDerived_reaches_splitMS.gpkg'), driver='GPKG')

print('Writing outputs ...')
if os.path.isfile(MSsplit_flows_fileName):                
    os.remove(MSsplit_flows_fileName)
MSsplit_flows_gdf.to_file(MSsplit_flows_fileName,driver='GPKG',index=False)

if os.path.isfile(MSsplit_points_fileName):                
    os.remove(MSsplit_points_fileName)
MSsplit_points_gdf.to_file(MSsplit_points_fileName,driver='GPKG',index=False)

# Limit the rasters to the buffer distance around the draft streams.
print ("{}Limiting rasters to buffer area ({} meters) around model streams".format("     ", str(floodAOIbuf)))
print ("              Creating processing zone (buffer area).")
MSsplit_flows_gdf_buffered = MSsplit_flows_gdf.unary_union.buffer(str(floodAOIbuf))

# Mask nhddem
import rasterio.mask
with rasterio.open(nhddemFR) as src:
    out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(nhddemFR), nhddemMSname + '.tiff'), "w", **out_meta) as dest:
    dest.write(out_image)

# Mask fdr
with rasterio.open(fdrFR) as src:
    out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(fdrFR), fdrMSname + '.tiff'), "w", **out_meta) as dest:
    dest.write(out_image)
