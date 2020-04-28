#!/usr/bin/env python3

import geopandas as gpd
from collections import deque,Counter
import numpy as np
from tqdm import tqdm
import argparse
from os.path import splitext
from shapely.strtree import STRtree
from shapely.geometry import Point,MultiLineString,LineString,mapping

def subset_vector_layers(hucCode,projection,nwm_headwaters_fileName,nhd_streams_fileName,nhd_streams_vaa_fileName,nwm_lakes_fileName,nwm_catchments_fileName,wbd_fileName,subset_nhd_streams_fileName,subset_nwm_lakes_fileName,subset_nwm_headwaters_fileName,subset_nwm_catchments_fileName,subset_nhd_headwaters_fileName=None,dissolveLinks=False):
    
    # loading files
    print("Loading files")
    hucUnitLength = len(str(hucCode))

    nwm_headwaters = gpd.read_file(nwm_headwaters_fileName)
    nhd_streams = gpd.read_file(nhd_streams_fileName)
    nwm_lakes = gpd.read_file(nwm_lakes_fileName)
    nwm_catchments = gpd.read_file(nwm_catchments_fileName)
    wbd = gpd.read_file(wbd_fileName)
    nhd_streams_vaa = gpd.read_file(nhd_streams_vaa_fileName)

    # REPROJECT
    print("Reprojecting")
    nwm_headwaters = nwm_headwaters.to_crs(projection)
    nhd_streams = nhd_streams.to_crs(projection)
    nwm_lakes = nwm_lakes.to_crs(projection)
    nwm_catchments = nwm_catchments.to_crs(projection)

    # query nhd+HR streams for HUC code
    print("Querying NHD Streams for HUC{} {}".format(hucUnitLength,hucCode))
    nhd_streams = nhd_streams.query('ReachCode.str.startswith("{}")'.format(hucCode))
    nhd_streams = nhd_streams.explode()

    # find intersecting lakes
    print("Subsetting NWM Lakes for HUC{} {}".format(hucUnitLength,hucCode))
    nwm_lakes = nwm_lakes.loc[nwm_lakes.intersects(wbd.geometry[0]),:]

    # find intersecting nwm_headwaters
    print("Subsetting NWM Headwaters for HUC{} {}".format(hucUnitLength,hucCode))
    nwm_headwaters = nwm_headwaters.loc[nwm_headwaters.intersects(wbd.geometry[0]),:]

    # find intersecting nwm_headwaters
    print("Subsetting NWM Catchments for HUC{} {}".format(hucUnitLength,hucCode))
    nwm_catchments = nwm_catchments.loc[nwm_catchments.intersects(wbd.geometry[0]),:]

    # merge vaa and nhd streams
    print("Merging VAA into NHD streams")
    nhd_streams = nhd_streams.merge(nhd_streams_vaa[['FromNode','ToNode','NHDPlusID','StreamOrde']],on='NHDPlusID',how='inner')

    # # identify all nhd headwaters
    # print('Identify all NHD headwater points')
    # nhd_headwater_nodes = set(nhd_streams['FromNode'].unique()) - set(nhd_streams['ToNode'].unique())
    # nhd_headwater_boolean = nhd_streams['FromNode'].isin(nhd_headwater_nodes)
    # nhd_headwater_streams = nhd_streams.loc[nhd_headwater_boolean,:]
    # nhd_headwater_streams.reset_index(drop=True,inplace=True)

    # get nhd headwaters closest to nwm headwater points
    print('Identify NHD Headwater streams nearest to NWM Headwater points')
    nhd_streams.loc[:,'is_nwm_headwater'] = False
    # nhd_streams_tree = STRtree(nhd_streams.geometry)
    for index, row in tqdm(nwm_headwaters.iterrows(),total=len(nwm_headwaters)):
        distances = nhd_streams.distance(row['geometry'])
        # nearestGeom = nhd_streams_tree.nearest(row['geometry'])
        min_index = np.argmin(distances)
        nhd_streams.loc[min_index,'is_nwm_headwater'] = True

    # identify inflowing streams
    print("Identify inflowing streams")
    intersecting=nhd_streams.crosses(wbd.geometry[0])
    nhd_streams.loc[intersecting,'is_nwm_headwater'] = True

    # copy over headwater features to nwm streams
    nhd_streams['is_nwm_stream'] = nhd_streams['is_nwm_headwater'].copy()

    if subset_nhd_headwaters_fileName is not None:
        # identify all nhd headwaters
        print('Identify NHD headwater points')
        nhd_headwater_streams = nhd_streams.loc[nhd_streams['is_nwm_headwater'],:]
        nhd_headwater_streams = nhd_headwater_streams.explode()

        hw_points = np.zeros(len(nhd_headwater_streams),dtype=object)
        for index,lineString in enumerate(nhd_headwater_streams.geometry):
            hw_point = [point for point in zip(*lineString.coords.xy)][-1]
            hw_points[index] = Point(*hw_point)

        nhd_headwater_points = gpd.GeoDataFrame({'NHDPlusID' : nhd_headwater_streams['NHDPlusID'],
                                                'geometry' : hw_points},geometry='geometry',crs=projection)

    # trace down from NWM Headwaters
    print('Identify NHD streams downstream of relevant NHD Headwater streams')
    nhd_streams = nhd_streams.explode()
    nhd_streams.set_index('NHDPlusID',inplace=True,drop=False)

    # Q = deque(nhd_headwater_streams['NHDPlusID'].tolist())
    Q = deque(nhd_streams.loc[nhd_streams['is_nwm_headwater'],'NHDPlusID'].tolist())
    visited = set()

    while Q:
        q = Q.popleft()

        if q in visited:
            continue

        visited.add(q)

        toNode = nhd_streams.loc[q,'ToNode']

        downstream_ids = nhd_streams.loc[nhd_streams['FromNode'] == toNode,:].index.tolist()

        nhd_streams.loc[downstream_ids,'is_nwm_stream'] = True

        for i in downstream_ids:
            if i not in visited:
                Q.append(i)

    nhd_streams = nhd_streams.loc[nhd_streams['is_nwm_stream'],:]

    if dissolveLinks:
        # remove multi-line strings
        print("Dissolving NHD reaches to Links (reaches constrained to stream intersections)")

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

    nwm_lakes.reset_index(drop=True,inplace=True)
    nwm_lakes.to_file(subset_nwm_lakes_fileName,driver=getDriver(subset_nwm_lakes_fileName),index=False)

    nwm_headwaters.reset_index(drop=True,inplace=True)
    nwm_headwaters.to_file(subset_nwm_headwaters_fileName,driver=getDriver(subset_nwm_headwaters_fileName),index=False)

    nwm_catchments.reset_index(drop=True,inplace=True)
    nwm_catchments.to_file(subset_nwm_catchments_fileName,driver=getDriver(subset_nwm_headwaters_fileName),index=False)

    if subset_nhd_headwaters_fileName is not None:
        nhd_headwater_points.reset_index(drop=True,inplace=True)
        nhd_headwater_points.to_file(subset_nhd_headwaters_fileName,driver=getDriver(subset_nhd_headwaters_fileName),index=False)


def getDriver(fileName):

    driverDictionary = {'.gpkg' : 'GPKG','.geojson' : 'GeoJSON','.shp' : 'ESRI Shapefile'}
    driver = driverDictionary[splitext(fileName)[1]]

    return(driver)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-d','--hucCode', help='DEM to use within project path', required=True,type=int)
    parser.add_argument('-p','--projection', help='DEM to use within project path', required=True)
    parser.add_argument('-w','--nwm-headwaters', help='DEM to use within project path', required=True)
    parser.add_argument('-s','--nhd-streams',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-v','--nhd-vaa',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-l','--nwm-lakes', help='DEM to use within project path', required=True)
    parser.add_argument('-m','--nwm-catchments', help='DEM to use within project path', required=True)
    parser.add_argument('-u','--wbd',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-c','--subset-streams',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-a','--subset-lakes',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-t','--subset-nwm-headwaters',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-e','--subset-nhd-headwaters',help='Basins polygons to use within project path',required=True,default=None)
    parser.add_argument('-n','--subset-catchments',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-o','--dissolve-links',help='Basins polygons to use within project path',action="store_true",default=False)

    args = vars(parser.parse_args())

    hucCode = args['hucCode']
    projection = args['projection']
    nwm_headwaters_fileName = args['nwm_headwaters']
    nhd_streams_fileName = args['nhd_streams']
    nhd_streams_vaa_fileName = args['nhd_vaa']
    nwm_lakes_fileName = args['nwm_lakes']
    nwm_catchments_fileName = args['nwm_catchments']
    wbd_fileName = args['wbd']
    subset_nhd_streams_fileName = args['subset_streams']
    subset_nwm_lakes_fileName = args['subset_lakes']
    subset_nwm_headwaters_fileName = args['subset_nwm_headwaters']
    subset_nwm_catchments_fileName = args['subset_catchments']
    subset_nhd_headwaters_fileName = args['subset_nhd_headwaters']
    dissolveLinks = args['dissolve_links']

    subset_vector_layers(hucCode,projection,nwm_headwaters_fileName,nhd_streams_fileName,nhd_streams_vaa_fileName,nwm_lakes_fileName,nwm_catchments_fileName,wbd_fileName,subset_nhd_streams_fileName,subset_nwm_lakes_fileName,subset_nwm_headwaters_fileName,subset_nwm_catchments_fileName,subset_nhd_headwaters_fileName,dissolveLinks)
