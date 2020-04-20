#!/usr/bin/env python3

import geopandas as gpd
from collections import deque
import numpy as np
from tqdm import tqdm
import argparse
from os.path import splitext
from shapely.strtree import STRtree
from shapely.geometry import Point

def subset_vector_layers(hucCode,projection,nwm_headwaters_fileName,nhd_streams_fileName,nhd_streams_vaa_fileName,nwm_lakes_fileName,nwm_catchments_fileName,wbd_fileName,subset_nhd_streams_fileName,subset_nwm_lakes_fileName,subset_nwm_headwaters_fileName,subset_nwm_catchments_fileName,subset_nhd_headwaters_fileName):

    # loading files
    print("Loading files")
    hucUnitLength = len(hucCode)
    hucCode = int(hucCode)

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
    nhd_streams = nhd_streams.merge(nhd_streams_vaa[['FromNode','ToNode','NHDPlusID']],on='NHDPlusID',how='inner')

    # # identify all nhd headwaters
    # print('Identify all NHD headwater points')
    # nhd_headwater_nodes = set(nhd_streams['FromNode'].unique()) - set(nhd_streams['ToNode'].unique())
    # nhd_headwater_boolean = nhd_streams['FromNode'].isin(nhd_headwater_nodes)
    # nhd_headwater_streams = nhd_streams.loc[nhd_headwater_boolean,:]
    # nhd_headwater_streams.reset_index(drop=True,inplace=True)

    # get nhd headwaters closest to nwm headwater points
    print('Identify NHD Headwater streams nearest to NWM Headwater points')
    nhd_streams.loc[:,'is_nwm_stream'] = False
    # nhd_streams_tree = STRtree(nhd_streams.geometry)
    for index, row in tqdm(nwm_headwaters.iterrows(),total=len(nwm_headwaters)):
        distances = nhd_streams.distance(row['geometry'])
        # nearestGeom = nhd_streams_tree.nearest(row['geometry'])
        min_index = np.argmin(distances)
        nhd_streams.loc[min_index,'is_nwm_stream'] = True

    # identify inflowing streams
    print("Identify inflowing streams")
    intersecting=nhd_streams.crosses(wbd.geometry[0])
    nhd_streams.loc[intersecting,'is_nwm_stream'] = True
    # nhd_streams.loc[intersecting,:].to_file('test.gpkg',driver='GPKG',index=False)
    # print(intersecting.sum())

    # identify all nhd headwaters
    print('Identify NHD headwater points')
    nhd_headwater_streams = nhd_streams.loc[nhd_streams['is_nwm_stream'],:]
    nhd_headwater_streams = nhd_headwater_streams.explode()

    hw_points = np.zeros(len(nhd_headwater_streams),dtype=object)
    for index,lineString in enumerate(nhd_headwater_streams.geometry):
        hw_point = [point for point in zip(*lineString.coords.xy)][-1]
        hw_points[index] = Point(*hw_point)

    nhd_headwater_points = gpd.GeoDataFrame({'NHDPlusID' : nhd_headwater_streams['NHDPlusID'],
                                            'geometry' : hw_points},geometry='geometry',crs=projection)

    # nhd_headwater_streams.loc[:,'is_nwm_headwater'] = False
    # for index, row in tqdm(nwm_headwaters.iterrows(),total=len(nwm_headwaters)):
    #     distances = nhd_headwater_streams.distance(row['geometry'])
    #     min_index = np.argmin(distances)
    #     # closest_geom = snap(row['geomtry'],closest_geom,0.5)
    #     nhd_headwater_streams.loc[min_index,'is_nwm_headwater'] = True

    # nhd_headwater_streams = nhd_headwater_streams.loc[nhd_headwater_streams['is_nwm_headwater'],:]

    print('Identify NHD Headwater streams downstream of relevant NHD Headwater streams')

    nhd_streams.set_index('NHDPlusID',inplace=True,drop=False)

    # Q = deque(nhd_headwater_streams['NHDPlusID'].tolist())
    Q = deque(nhd_streams.loc[nhd_streams['is_nwm_stream'],'NHDPlusID'].tolist())
    visited = set()

    # nhd_streams.loc[:,'is_nwm_stream'] = False
    # nhd_streams.loc[nhd_headwater_streams['NHDPlusID'],'is_nwm_stream'] = True

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

    nhd_streams.drop(columns='is_nwm_stream',inplace=True)

    # reset indices
    nhd_streams.reset_index(drop=True,inplace=True)
    nwm_lakes.reset_index(drop=True,inplace=True)
    nwm_headwaters.reset_index(drop=True,inplace=True)
    nwm_catchments.reset_index(drop=True,inplace=True)
    nhd_headwater_points.reset_index(drop=True,inplace=True)

    # write to files
    nhd_streams.to_file(subset_nhd_streams_fileName,driver=getDriver(subset_nhd_streams_fileName),index=False)
    nwm_lakes.to_file(subset_nwm_lakes_fileName,driver=getDriver(subset_nwm_lakes_fileName),index=False)
    nwm_headwaters.to_file(subset_nwm_headwaters_fileName,driver=getDriver(subset_nwm_headwaters_fileName),index=False)
    nwm_catchments.to_file(subset_nwm_catchments_fileName,driver=getDriver(subset_nwm_headwaters_fileName),index=False)
    nhd_headwater_points.to_file(subset_nhd_headwaters_fileName,driver=getDriver(subset_nhd_headwaters_fileName),index=False)


def getDriver(fileName):

    driverDictionary = {'.gpkg' : 'GPKG','.geojson' : 'GeoJSON','.shp' : 'ESRI Shapefile'}
    driver = driverDictionary[splitext(fileName)[1]]

    return(driver)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Relative elevation from pixel based watersheds')
    parser.add_argument('-d','--hucCode', help='DEM to use within project path', required=True)
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
    parser.add_argument('-e','--subset-nhd-headwaters',help='Basins polygons to use within project path',required=True)
    parser.add_argument('-n','--subset-catchments',help='Basins polygons to use within project path',required=True)

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


    subset_vector_layers(hucCode,projection,nwm_headwaters_fileName,nhd_streams_fileName,nhd_streams_vaa_fileName,nwm_lakes_fileName,nwm_catchments_fileName,wbd_fileName,subset_nhd_streams_fileName,subset_nwm_lakes_fileName,subset_nwm_headwaters_fileName,subset_nwm_catchments_fileName,subset_nhd_headwaters_fileName)
