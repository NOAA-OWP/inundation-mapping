#!/usr/bin/env python3

import sys
import geopandas as gpd
import pandas as pd
from collections import deque,Counter
import numpy as np
from tqdm import tqdm
import argparse
from os.path import splitext
from shapely.strtree import STRtree
from shapely.geometry import Point,MultiLineString,LineString,mapping,MultiPolygon,Polygon

def subset_vector_layers(hucCode,nwm_streams_fileName,nwm_headwaters_fileName,nhd_streams_fileName,nwm_lakes_fileName,nld_lines_fileName,nwm_catchments_fileName,wbd_fileName,wbd_buffer_fileName,ahps_sites_fileName,landsea_filename,subset_nhd_streams_fileName,subset_nwm_lakes_fileName,subset_nld_lines_fileName,subset_nwm_headwaters_fileName,subset_nwm_catchments_fileName,subset_nwm_streams_fileName,subset_nhd_headwaters_fileName=None,dissolveLinks=False,extent='FR'):

    hucUnitLength = len(str(hucCode))

    wbd = gpd.read_file(wbd_fileName)
    wbd_buffer = gpd.read_file(wbd_buffer_fileName)
    projection = wbd.crs
    
    # Clip WBD to remove ocean areas (if necessary)
    print("Clip WBD in ocean areas for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    print(landsea_filename)
    landsea = gpd.read_file(landsea_filename)
    if not landsea.empty:
        wbd = gpd.overlay(wbd,landsea,how='difference')
        wbd_buffer = gpd.overlay(wbd_buffer,landsea,how='difference')
        wbd.to_file(wbd_fileName,driver=getDriver(wbd_fileName),index=False)
        wbd_buffer.to_file(wbd_buffer_fileName,driver=getDriver(wbd_buffer_fileName),index=False)
        wbd = gpd.read_file(wbd_fileName)
        wbd_buffer = gpd.read_file(wbd_buffer_fileName)

    # find intersecting lakes and writeout
    print("Subsetting NWM Lakes for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_lakes = gpd.read_file(nwm_lakes_fileName, mask = wbd_buffer)

    if not nwm_lakes.empty:
        # perform fill process to remove holes/islands in the NWM lake polygons
        nwm_lakes = nwm_lakes.explode()
        nwm_lakes_fill_holes=MultiPolygon(Polygon(p.exterior) for p in nwm_lakes['geometry']) # remove donut hole geometries
        # loop through the filled polygons and insert the new geometry
        for i in range(len(nwm_lakes_fill_holes)):
            nwm_lakes.loc[i,'geometry'] = nwm_lakes_fill_holes[i]

        nwm_lakes.to_file(subset_nwm_lakes_fileName,driver=getDriver(subset_nwm_lakes_fileName),index=False)
    del nwm_lakes

    # find intersecting levee lines
    print("Subsetting NLD levee lines for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nld_lines = gpd.read_file(nld_lines_fileName, mask = wbd)
    if not nld_lines.empty:
        nld_lines.to_file(subset_nld_lines_fileName,driver=getDriver(subset_nld_lines_fileName),index=False)
    del nld_lines

    # find intersecting nwm_catchments
    print("Subsetting NWM Catchments for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_catchments = gpd.read_file(nwm_catchments_fileName, mask = wbd)
    nwm_catchments.to_file(subset_nwm_catchments_fileName,driver=getDriver(subset_nwm_catchments_fileName),index=False)
    del nwm_catchments

    # query nhd+HR streams for HUC code
    print("Querying NHD Streams for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nhd_streams = gpd.read_file(nhd_streams_fileName, mask = wbd_buffer)
    nhd_streams = nhd_streams.explode()

    # find intersecting nwm_headwaters
    print("Subsetting NWM Streams and deriving headwaters for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_streams = gpd.read_file(nwm_streams_fileName, mask = wbd_buffer)
    nwm_streams.to_file(subset_nwm_streams_fileName,driver=getDriver(subset_nwm_streams_fileName),index=False)
    del nwm_streams

    # get nhd headwaters closest to nwm headwater points
    print('Identify NHD Headwater streams nearest to NWM Headwater points',flush=True)
    nhd_streams.loc[:,'is_nwm_headwater'] = False
    # nhd_streams_tree = STRtree(nhd_streams.geometry)

    if extent == 'FR':
        nwm_headwaters = gpd.read_file(nwm_headwaters_fileName, mask = wbd_buffer)
    elif extent == 'MS':
        nwm_headwaters = gpd.read_file(ahps_sites_fileName, mask = wbd)

        # check for incoming MS streams and convert to points
        intersecting = nhd_streams.crosses(wbd.geometry[0])
        incoming_flows = nhd_streams.loc[intersecting,:]
        incoming_points_list = []

        if len(incoming_flows) > 0:
            for i,linesting in enumerate(incoming_flows.geometry):
                incoming_points_list = incoming_points_list + [linesting.coords[-1]]

        geometry = [Point(xy) for xy in zip(incoming_points_list)]
        incoming_points = gpd.GeoDataFrame({'feature_id' : 0 ,'nwsid' : 'huc8_incoming' ,'geometry':geometry}, crs=nhd_streams.crs, geometry='geometry')

        if (len(nwm_headwaters) > 0) or (len(incoming_points) > 0):

            if len(nwm_headwaters) > 0:
                    print ("Snapping forecasting points to nhd stream network")
                    streamlines_union = nhd_streams.geometry.unary_union
                    snapped_geoms = []
                    snappedpoints_df = pd.DataFrame(nwm_headwaters).drop(columns=['geometry'])

                    # snap lines to streams
                    for i in range(len(nwm_headwaters)):
                        snapped_geoms.append(streamlines_union.interpolate(streamlines_union.project(nwm_headwaters.geometry[i])))

                    snappedpoints_df['geometry'] = snapped_geoms
                    snapped_points = gpd.GeoDataFrame(snappedpoints_df,crs=nhd_streams.crs)

            if (len(incoming_points) > 0) and (len(nwm_headwaters) > 0):
                nwm_headwaters = snapped_points.append(incoming_points).reset_index(drop=True)
            elif len(incoming_points) > 0:
                nwm_headwaters = incoming_points.copy()
        else:
            print ("No AHPs point(s) within HUC " + str(hucCode) +  " boundaries.")
            sys.exit(0)

    for index, row in tqdm(nwm_headwaters.iterrows(),total=len(nwm_headwaters)):
        distances = nhd_streams.distance(row['geometry'])
        # nearestGeom = nhd_streams_tree.nearest(row['geometry'])
        min_index = np.argmin(distances)
        nhd_streams.loc[min_index,'is_nwm_headwater'] = True

    nhd_streams = nhd_streams.loc[nhd_streams.geometry!=None,:] # special case: remove segments without geometries

    # writeout nwm headwaters
    if not nwm_headwaters.empty:
        nwm_headwaters.to_file(subset_nwm_headwaters_fileName,driver=getDriver(subset_nwm_headwaters_fileName),index=False)
    del nwm_headwaters

    # copy over headwater features to nwm streams
    nhd_streams['is_nwm_stream'] = nhd_streams['is_nwm_headwater'].copy()

    # trace down from NWM Headwaters
    print('Identify NHD streams downstream of relevant NHD Headwater streams',flush=True)
    nhd_streams.set_index('NHDPlusID',inplace=True,drop=False)

    Q = deque(nhd_streams.loc[nhd_streams['is_nwm_headwater'],'NHDPlusID'].tolist())
    visited = set()

    while Q:
        q = Q.popleft()
        if q in visited:
            continue
        visited.add(q)
        toNode,DnLevelPat = nhd_streams.loc[q,['ToNode','DnLevelPat']]
        try:
            downstream_ids = nhd_streams.loc[nhd_streams['FromNode'] == toNode,:].index.tolist()
        except ValueError: # 18050002 has duplicate nhd stream feature
            if len(toNode.unique()) == 1:
                toNode = toNode.iloc[0]
                downstream_ids = nhd_streams.loc[nhd_streams['FromNode'] == toNode,:].index.tolist()
        #If multiple downstream_ids are returned select the ids that are along the main flow path (i.e. exclude segments that are diversions)
        if len(set(downstream_ids))>1: # special case: remove duplicate NHDPlusIDs
            relevant_ids = [segment for segment in downstream_ids if DnLevelPat == nhd_streams.loc[segment,'LevelPathI']]
        else:
            relevant_ids = downstream_ids
        nhd_streams.loc[relevant_ids,'is_nwm_stream'] = True
        for i in relevant_ids:
            if i not in visited:
                Q.append(i)

    nhd_streams = nhd_streams.loc[nhd_streams['is_nwm_stream'],:]

    if dissolveLinks:
        # remove multi-line strings
        print("Dissolving NHD reaches to Links (reaches constrained to stream intersections)",flush=True)

        nhd_streams.set_index('NHDPlusID',inplace=True,drop=False)
        nhd_streams['before_confluence'] = nhd_streams.duplicated(subset='ToNode',keep=False)

        nhd_streams.loc[nhd_streams['is_nwm_headwater'],'linkNo'] = np.arange(1,nhd_streams['is_nwm_headwater'].sum()+1)

        Q = deque(nhd_streams.loc[nhd_streams['is_nwm_headwater'],'NHDPlusID'].tolist())
        visited = set()
        linkNo = np.max(nhd_streams.loc[nhd_streams['is_nwm_headwater'],'linkNo']) + 1
        link_geometries = dict()

        # adds all headwaters to link_geometries
        for q in Q:
            link_geometries[nhd_streams.loc[q,'linkNo']] = [p for p in zip(*nhd_streams.loc[q,'geometry'].coords.xy)][::-1]

        # Do BFS
        while Q:
            q = Q.popleft()

            if q in visited:
                continue

            visited.add(q)

            toNode = nhd_streams.loc[q,'ToNode']

            downstream_ids = nhd_streams.loc[nhd_streams['FromNode'] == toNode,:].index.tolist()
            numberOfDownstreamIDs = len(downstream_ids)

            for i in downstream_ids:
                if i not in visited:
                    Q.append(i)

                    if nhd_streams.loc[q,'before_confluence'] or (numberOfDownstreamIDs > 1):
                        # do not dissolve
                        linkNo += 1
                        nhd_streams.loc[i,'linkNo'] = linkNo

                        next_stream_geometry = [p for p in zip(*nhd_streams.loc[i,'geometry'].coords.xy)][::-1]

                        link_geometries[linkNo] = next_stream_geometry

                    else:
                        nhd_streams.loc[i,'linkNo'] = nhd_streams.loc[q,'linkNo']

                        next_stream_geometry = [p for p in zip(*nhd_streams.loc[i,'geometry'].coords.xy)][::-1]

                        link_geometries[nhd_streams.loc[i,'linkNo']] = link_geometries[nhd_streams.loc[i,'linkNo']] + next_stream_geometry


        # convert dictionary to lists for keys (linkNos) and values (geometry linestrings)
        output_links = [] ; output_geometries = []
        for ln_no, ln_geom in link_geometries.items():
            output_links = output_links + [ln_no]
            output_geometries = output_geometries + [LineString(ln_geom)]

        nhd_streams = gpd.GeoDataFrame({'linkNO' : output_links,'geometry': output_geometries},geometry='geometry',crs=projection)

    # write to files
    nhd_streams.reset_index(drop=True,inplace=True)
    nhd_streams.to_file(subset_nhd_streams_fileName,driver=getDriver(subset_nhd_streams_fileName),index=False)

    if subset_nhd_headwaters_fileName is not None:
        # identify all nhd headwaters
        print('Identify NHD headwater points',flush=True)
        nhd_headwater_streams = nhd_streams.loc[nhd_streams['is_nwm_headwater'],:]
        nhd_headwater_streams = nhd_headwater_streams.explode()

        hw_points = np.zeros(len(nhd_headwater_streams),dtype=object)
        for index,lineString in enumerate(nhd_headwater_streams.geometry):
            hw_point = [point for point in zip(*lineString.coords.xy)][-1]
            hw_points[index] = Point(*hw_point)

        nhd_headwater_points = gpd.GeoDataFrame({'NHDPlusID' : nhd_headwater_streams['NHDPlusID'],
                                                'geometry' : hw_points},geometry='geometry',crs=projection)

        nhd_headwater_points.to_file(subset_nhd_headwaters_fileName,driver=getDriver(subset_nhd_headwaters_fileName),index=False)
        del nhd_headwater_streams, nhd_headwater_points

def getDriver(fileName):

    driverDictionary = {'.gpkg' : 'GPKG','.geojson' : 'GeoJSON','.shp' : 'ESRI Shapefile'}
    driver = driverDictionary[splitext(fileName)[1]]

    return(driver)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-d','--hucCode', help='HUC boundary ID', required=True,type=str)
    parser.add_argument('-w','--nwm-streams', help='NWM flowlines', required=True)
    parser.add_argument('-f','--nwm-headwaters', help='NWM headwater points', required=True)
    parser.add_argument('-s','--nhd-streams',help='NHDPlus HR burnline',required=True)
    parser.add_argument('-l','--nwm-lakes', help='NWM Lakes', required=True)
    parser.add_argument('-r','--nld-lines', help='Levee vectors to use within project path', required=True)
    parser.add_argument('-m','--nwm-catchments', help='NWM catchments', required=True)
    parser.add_argument('-u','--wbd',help='HUC boundary',required=True)
    parser.add_argument('-g','--wbd-buffer',help='Buffered HUC boundary',required=True)
    parser.add_argument('-y','--ahps-sites',help='Buffered HUC boundary',required=True)
    parser.add_argument('-v','--landsea',help='NHDPlus LandSea',required=True)
    parser.add_argument('-c','--subset-nhd-streams',help='NHD streams subset',required=True)
    parser.add_argument('-a','--subset-lakes',help='NWM lake subset',required=True)
    parser.add_argument('-t','--subset-nwm-headwaters',help='NWM headwaters subset',required=True)
    parser.add_argument('-z','--subset-nld-lines',help='Subset of NLD levee vectors for HUC',required=True)
    parser.add_argument('-e','--subset-nhd-headwaters',help='NHD headwaters subset',required=True,default=None)
    parser.add_argument('-n','--subset-catchments',help='NWM catchments subset',required=True)
    parser.add_argument('-b','--subset-nwm-streams',help='NWM streams subset',required=True)
    parser.add_argument('-o','--dissolve-links',help='remove multi-line strings',action="store_true",default=False)
    parser.add_argument('-p','--extent',help='MS or FR extent',required=True)

    args = vars(parser.parse_args())

    hucCode = args['hucCode']
    nwm_streams_fileName = args['nwm_streams']
    nwm_headwaters_fileName = args['nwm_headwaters']
    nhd_streams_fileName = args['nhd_streams']
    nwm_lakes_fileName = args['nwm_lakes']
    nld_lines_fileName = args['nld_lines']
    nwm_catchments_fileName = args['nwm_catchments']
    wbd_fileName = args['wbd']
    wbd_buffer_fileName = args['wbd_buffer']
    ahps_sites_fileName = args['ahps_sites']
    landsea_fileName = args['landsea']
    subset_nhd_streams_fileName = args['subset_nhd_streams']
    subset_nwm_lakes_fileName = args['subset_lakes']
    subset_nwm_headwaters_fileName = args['subset_nwm_headwaters']
    subset_nld_lines_fileName = args['subset_nld_lines']
    subset_nwm_catchments_fileName = args['subset_catchments']
    subset_nhd_headwaters_fileName = args['subset_nhd_headwaters']
    subset_nwm_streams_fileName = args['subset_nwm_streams']
    dissolveLinks = args['dissolve_links']
    extent = args['extent']

    subset_vector_layers(hucCode,nwm_streams_fileName,nwm_headwaters_fileName,nhd_streams_fileName,nwm_lakes_fileName,nld_lines_fileName,nwm_catchments_fileName,wbd_fileName,wbd_buffer_fileName,ahps_sites_fileName,landsea_fileName,subset_nhd_streams_fileName,subset_nwm_lakes_fileName,subset_nld_lines_fileName,subset_nwm_headwaters_fileName,subset_nwm_catchments_fileName,subset_nwm_streams_fileName,subset_nhd_headwaters_fileName,dissolveLinks,extent)
