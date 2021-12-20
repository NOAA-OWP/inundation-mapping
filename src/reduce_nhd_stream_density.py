#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
from collections import deque
import argparse
import pygeos
from shapely.wkb import dumps
from shapely.geometry import Point
from utils.shared_functions import getDriver


def subset_nhd_network(huc4,huc4_mask,selected_wbd8,nhd_streams_,headwaters_filename,headwater_id,nwm_intersections_filename,mainstem_flag=False):

    headwater_streams = pd.DataFrame()

    if mainstem_flag == False:
        nhd_streams = gpd.read_file(nhd_streams_)
        headwater_col = 'is_headwater'
        id_col = 'headwaters_id'
        n = -1
    else:
        nhd_streams = nhd_streams_.copy()
        headwater_col = 'mainstem'
        id_col = 'nws_lid'
        n = ''

    # Locate the closest NHDPlus HR stream segment to NWM headwater points. Done by HUC8 to reduce processing time and to contain NWM headwater in the same HUC
    for index, row in selected_wbd8.iterrows():
        huc = row["HUC8"]

        # Double check that this is a nested HUC
        if huc.startswith(str(huc4)):

            huc8_mask = selected_wbd8.loc[selected_wbd8.HUC8==huc]
            huc8_mask = huc8_mask.reset_index(drop=True)

            # Masking headwaters by HUC8
            headwaters_mask = gpd.read_file(headwaters_filename, mask = huc8_mask)
            headwaters_mask = headwaters_mask.reset_index(drop=True)

            # Masking subset streams by HUC8
            if mainstem_flag == False:
                streams_subset = gpd.read_file(nhd_streams_, mask = huc8_mask)
            else:
                streams_subset = nhd_streams.loc[nhd_streams.HUC8==huc].copy()
                if headwaters_mask.is_headwater.dtype != 'int': headwaters_mask.is_headwater = headwaters_mask.is_headwater.astype('int')
                if headwaters_mask.is_colocated.dtype != 'int': headwaters_mask.is_colocated = headwaters_mask.is_colocated.astype('int')
                headwaters_mask = headwaters_mask.loc[headwaters_mask.is_headwater==True]

            if not streams_subset.empty:
                streams_subset[headwater_col] = False
                streams_subset = streams_subset.reset_index(drop=True)

                # Create WKB geometry column
                streams_subset['b_geom'] = None
                for index, linestring in enumerate(streams_subset.geometry):
                    streams_subset.at[index, 'b_geom'] = dumps(linestring)

                # Create pygeos nhd stream geometries from WKB representation
                streambin_geom = pygeos.io.from_wkb(streams_subset['b_geom'])

                # Add HUC8 column
                streams_subset['HUC8'] = str(huc)

                # Add headwaters_id column
                streams_subset[id_col] = n
                distance_from_upstream = {}
                for index, point in headwaters_mask.iterrows():

                    # Convert headwater point geometries to WKB representation
                    wkb_point = dumps(point.geometry)

                    # Create pygeos headwater point geometries from WKB representation
                    pointbin_geom = pygeos.io.from_wkb(wkb_point)

                    # Distance to each stream segment
                    distances = pygeos.measurement.distance(streambin_geom, pointbin_geom)

                    # Find minimum distance
                    min_index = np.argmin(distances)
                    headwater_point_name = point[headwater_id]

                    # Find stream segment closest to headwater point
                    if mainstem_flag==True:

                        if point.is_colocated==True:

                            closest_stream = streams_subset.iloc[min_index]
                            distance_to_line = point.geometry.distance(Point(closest_stream.geometry.coords[-1]))
                            print(f"{point.nws_lid} distance on line {closest_stream.NHDPlusID}:  {np.round(distance_to_line,1)}")

                            if not closest_stream.NHDPlusID in distance_from_upstream.keys():
                                distance_from_upstream[closest_stream.NHDPlusID] = [point.nws_lid,distance_to_line]

                            elif distance_from_upstream[closest_stream.NHDPlusID][1] > distance_to_line:
                                distance_from_upstream[closest_stream.NHDPlusID] = [point.nws_lid,distance_to_line]

                            headwater_point_name = distance_from_upstream[closest_stream.NHDPlusID][0]

                    # Closest segment to headwater
                    streams_subset.loc[min_index,headwater_col] = True
                    streams_subset.loc[min_index,id_col] = headwater_point_name

                headwater_streams = headwater_streams.append(streams_subset[['NHDPlusID',headwater_col,id_col,'HUC8']])

    headwater_streams = headwater_streams.sort_values(headwater_col, ascending=False).drop_duplicates('NHDPlusID') # keeps headwater=True for conflicting duplicates

    if mainstem_flag == False:
        nhd_streams = nhd_streams.merge(headwater_streams,on='NHDPlusID',how='inner')
    else:
        headwater_streams = headwater_streams.drop(columns=['HUC8'])
        nhd_streams = nhd_streams.merge(headwater_streams,on='NHDPlusID',how='outer')
        nhd_streams[id_col] = nhd_streams[id_col].fillna(n)
        nhd_streams[headwater_col] = nhd_streams[headwater_col].fillna(0)

    del selected_wbd8, streams_subset, headwater_streams

    huc4_mask_buffer = huc4_mask.buffer(10)

    # Identify inflowing streams
    nwm_intersections = gpd.read_file(nwm_intersections_filename, mask=huc4_mask_buffer)

    if mainstem_flag == False:
        nhd_streams['downstream_of_headwater'] = False
        nhd_streams['is_relevant_stream'] = nhd_streams['is_headwater'].copy()
    else:
        nwm_intersections = nwm_intersections.loc[nwm_intersections.mainstem==1]

    nhd_streams = nhd_streams.explode()
    nhd_streams = nhd_streams.reset_index(drop=True)



    # Find stream segment closest to nwm intersection point
    for index, point in nwm_intersections.iterrows():

        # Distance to each stream segment
        distances = nhd_streams.distance(point.geometry)

        # Find minimum distance
        min_index = np.argmin(distances)

        # Update attributes for incoming stream
        nhd_streams.loc[min_index,headwater_col] = True

        if mainstem_flag == False:
            nhd_streams.loc[min_index,'downstream_of_headwater'] = True
            nhd_streams['is_relevant_stream'] = nhd_streams[headwater_col].copy()

    # Trace down from headwaters
    nhd_streams.set_index('NHDPlusID',inplace=True,drop=False)

    nhd_streams = get_downstream_segments(nhd_streams.copy(),headwater_col,mainstem_flag)

    # nhd_streams.fillna(value = {"is_relevant_stream": False}, inplace=True)
    nhd_streams = nhd_streams.loc[nhd_streams['is_relevant_stream'],:]
    nhd_streams.reset_index(drop=True,inplace=True)

    return nhd_streams


def get_downstream_segments(streams, attribute,mainstem_flag):

    Q = deque(streams.loc[streams[attribute],'NHDPlusID'].tolist())
    visited = set()

    while Q:
        q = Q.popleft()
        if q in visited:
            continue

        visited.add(q)
        toNode,DnLevelPat = streams.loc[q,['ToNode','DnLevelPat']]

        try:
            downstream_ids = streams.loc[streams['FromNode'] == toNode,:].index.tolist()
        except ValueError: # 18050002 has duplicate nhd stream feature
            if len(toNode.unique()) == 1:
                toNode = toNode.iloc[0]
                downstream_ids = streams.loc[streams['FromNode'] == toNode,:].index.tolist()

        # If multiple downstream_ids are returned select the ids that are along the main flow path (i.e. exclude segments that are diversions)
        if len(set(downstream_ids))>1: # special case: remove duplicate NHDPlusIDs
            relevant_ids = [segment for segment in downstream_ids if DnLevelPat == streams.loc[segment,'LevelPathI']]
        else:
            relevant_ids = downstream_ids

        if mainstem_flag == False:

            streams.loc[relevant_ids,'is_relevant_stream'] = True
            streams.loc[relevant_ids,'downstream_of_headwater'] = True
        else:
            streams.loc[relevant_ids,'mainstem'] = True

        for i in relevant_ids:
            if i not in visited:
                Q.append(i)

    return streams


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Reduce NHDPlus HR network based on headwater points')
    parser.add_argument('-n','--huc-number',help='HUC number',required=True,type=str)
    parser.add_argument('-b','--huc4-mask',help='HUC4 mask',required=True)
    parser.add_argument('-w','--selected-wbd8',help='WBD8 layer',required=True)
    parser.add_argument('-t','--nhd-streams',help='NHDPlus HR geodataframe',required=True)
    parser.add_argument('-a','--headwaters-filename',help='Headwaters points layer name',required=True,type=str)
    parser.add_argument('-s','--subset-nhd-streams-fileName',help='Output streams layer name',required=False,type=str,default=None)
    parser.add_argument('-i','--headwater-id',help='Headwater points ID column',required=True)
    parser.add_argument('-c','--nwm-intersections-filename',help='NWM HUC4 intersection points',required=True)
    parser.add_argument('-d','--mainstem-flag',help='flag for mainstems network',required=False,default=False)

    args = vars(parser.parse_args())

    huc_number = args['huc_number']
    huc4_mask = args['huc4_mask']
    selected_wbd8 = args['selected_wbd8']
    nhd_streams = args['nhd_streams']
    headwaters_filename = args['headwaters_filename']
    subset_nhd_streams_fileName = args['subset_nhd_streams_fileName']
    headwater_id = args['headwater_id']
    nwm_intersections_filename = args['nwm_intersections_filename']
    mainstem_flag = args['mainstem_flag']

    subset_streams_gdf = subset_nhd_network(huc_number,huc4_mask,selected_wbd8,nhd_streams,headwaters_filename,headwater_id,nwm_intersections_filename,mainstem_flag=False)

    if subset_nhd_streams_fileName is not None:
        subset_streams_gdf.to_file(subset_nhd_streams_fileName,driver=getDriver(subset_nhd_streams_fileName),index=False)
