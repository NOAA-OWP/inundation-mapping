#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
from os.path import splitext
from tqdm import tqdm
from collections import deque
import argparse
import pygeos
from shapely.wkb import dumps
from utils.shared_functions import getDriver

def subset_nhd_network(huc4,huc4_mask,selected_wbd8,nhd_streams_filename,headwaters_filename,headwater_id,nwm_intersections_filename):

    headwater_streams = pd.DataFrame()

    nhd_streams = gpd.read_file(nhd_streams_filename)

    for index, row in selected_wbd8.iterrows():
        huc = row["HUC8"]

        if huc.startswith(str(huc4)):
            huc8_mask = selected_wbd8.loc[selected_wbd8.HUC8.str.startswith(huc)]
            huc8_mask = huc8_mask.reset_index(drop=True)

            headwaters_mask = gpd.read_file(headwaters_filename, mask = huc8_mask)
            headwaters_mask = headwaters_mask.reset_index(drop=True)

            streams_subset = gpd.read_file(nhd_streams_filename, mask = huc8_mask)

            if not streams_subset.empty:
                streams_subset.loc[:,'is_headwater'] = False
                streams_subset = streams_subset.reset_index(drop=True)

                streams_subset['b_geom'] = None
                for index, linestring in enumerate(streams_subset.geometry):
                    streams_subset.at[index, 'b_geom'] = dumps(linestring)

                # create pygeos nhd stream geometries from WKB representation
                streambin_geom = pygeos.io.from_wkb(streams_subset['b_geom'])

                streams_subset.loc[:,'HUC8'] = str(huc)

                if headwaters_mask[headwater_id].dtype=='int':
                    n = -1
                else:
                    n = ''

                streams_subset.loc[:,'headwaters_id'] = n

                # find stream segment closest to headwater point
                for index, point in headwaters_mask.iterrows():

                    # convert headwaterpoint geometries to WKB representation
                    wkb_points = dumps(point.geometry)

                    # create pygeos headwaterpoint geometries from WKB representation
                    pointbin_geom = pygeos.io.from_wkb(wkb_points)

                    # distance to each stream segment
                    distances = pygeos.measurement.distance(streambin_geom, pointbin_geom)

                    # find minimum distance
                    min_index = np.argmin(distances)

                    # Closest segment to headwater
                    streams_subset.loc[min_index,'is_headwater'] = True
                    streams_subset.loc[min_index,'headwaters_id'] = point[headwater_id]

                headwater_streams = headwater_streams.append(streams_subset[['NHDPlusID','is_headwater','headwaters_id','HUC8']])

    headwater_streams = headwater_streams.sort_values('is_headwater', ascending=False).drop_duplicates('NHDPlusID') # keeps headwater=True for conflicting duplicates
    nhd_streams = nhd_streams.merge(headwater_streams,on='NHDPlusID',how='inner')

    del selected_wbd8, streams_subset, headwater_streams

    huc4_mask_buffer = huc4_mask.buffer(10)

    # identify inflowing streams
    nwm_intersections = gpd.read_file(nwm_intersections_filename, mask=huc4_mask_buffer)

    nhd_streams['downstream_of_headwater'] = False
    nhd_streams = nhd_streams.explode()
    nhd_streams = nhd_streams.reset_index(drop=True)

    # find stream segment closest to nwm intersection point
    for index, point in nwm_intersections.iterrows():

        # distance to each stream segment
        distances = nhd_streams.distance(point.geometry)

        # find minimum distance
        min_index = np.argmin(distances)

        # update attributes for incoming stream
        nhd_streams.loc[min_index,'is_headwater'] = True
        nhd_streams.loc[min_index,'downstream_of_headwater'] = True

    ## subset NHDPlus HR
    nhd_streams['is_relevant_stream'] = nhd_streams['is_headwater'].copy()

    # trace down from headwaters
    nhd_streams.set_index('NHDPlusID',inplace=True,drop=False)

    nhd_streams = get_downstream_segments(nhd_streams, 'is_headwater')

    nhd_streams = nhd_streams.loc[nhd_streams['is_relevant_stream'],:]
    nhd_streams.reset_index(drop=True,inplace=True)

    return(nhd_streams)

def get_downstream_segments(streams, attribute):

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

        streams.loc[relevant_ids,'is_relevant_stream'] = True
        streams.loc[relevant_ids,'downstream_of_headwater'] = True

        for i in relevant_ids:
            if i not in visited:
                Q.append(i)

    return(streams)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Reduce NHDPlus HR network based on headwater points')
    parser.add_argument('-n','--huc-number',help='HUC number',required=True,type=str)
    parser.add_argument('-b','--huc4-mask',help='HUC4 mask',required=True)
    parser.add_argument('-w','--selected-wbd8',help='WBD8 layer',required=True)
    parser.add_argument('-t','--nhd-streams',help='NHDPlus HR geodataframe',required=True)
    parser.add_argument('-a','--headwaters-filename',help='Headwaters points layer name',required=True,type=str)
    parser.add_argument('-s','--subset-nhd-streams-fileName',help='Output streams layer name',required=False,type=str,default=None)
    parser.add_argument('-i','--headwater-id',help='Headwater points ID column',required=True)
    parser.add_argument('-i','--nwm-intersections-filename',help='NWM HUC4 intersection points',required=True)

    args = vars(parser.parse_args())

    subset_streams_gdf = subset_nhd_network(huc_number,huc4_mask,selected_wbd8,nhd_streams,headwaters_filename,headwater_id)

    if subset_nhd_streams_fileName is not None:
        subset_streams_gdf.to_file(args['subset_nhd_streams_fileName'],driver=getDriver(args['subset_nhd_streams_fileName']),index=False)
