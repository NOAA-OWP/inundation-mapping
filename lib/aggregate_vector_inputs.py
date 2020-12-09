#!/usr/bin/env·python3

import os
import geopandas as gpd
import pandas as pd
import numpy as np
from os.path import splitext
from utils.shared_variables import PREP_PROJECTION
from derive_headwaters import findHeadWaterPoints
from tqdm import tqdm
from collections import deque,Counter
import pygeos
from shapely.geometry import Point,LineString
from shapely.ops import split
from shapely.wkb import dumps, loads

in_dir ='data/inputs/nhdplus_vectors'
nhd_dir ='data/inputs/nhdplus_vectors_aggregate'
nwm_dir = 'data/inputs/nwm_hydrofabric'
wbd_dir = 'data/inputs/wbd'

## Generate NWM Headwaters
# print ('deriving NWM headwater points')
# nwm_streams = gpd.read_file(os.path.join(nwm_dir,'nwm_flows.gpkg'))
# nwm_headwaters = findHeadWaterPoints(nwm_streams)
# nwm_headwaters['ID'] = nwm_headwaters.index + 1
# nwm_headwaters.to_file(os.path.join(nwm_dir,'nwm_headwaters.gpkg'),driver='GPKG',index=False)

## Aggregate NHDPlus HR
print ('aggregating NHDPlus HR burnline layers')
nhd_streams_wVAA_fileName_pre=os.path.join(nhd_dir,'NHDPlusBurnLineEvent_wVAA_ftype_testing.gpkg')
nhd_streams_wVAA_fileName_limited=os.path.join(nhd_dir,'NHDPlusBurnLineEvent_wVAA_ftype_limited_testing.gpkg')
nhd_streams_wVAA_fileName_adjusted=os.path.join(nhd_dir,'NHDPlusBurnLineEvent_wVAA_ftype_adjusted_testing.gpkg')
nhd_headwaters_fileName=os.path.join(nhd_dir,'nhd_headwaters_adjusted.gpkg')

schema = {'geometry': 'MultiLineString','properties': {'NHDPlusID': 'str','ReachCode': 'str',
                                                  'FromNode': 'str','ToNode': 'str',
                                                  'StreamOrde': 'str','DnLevelPat': 'str',
                                                  'LevelPathI': 'str'}}

for huc in tqdm(os.listdir(in_dir)):
    if not huc[0]=='#':
        burnline_filename = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')
        vaa_filename = os.path.join(in_dir,huc,'NHDPlusFlowLineVAA' + str(huc) + '.gpkg')
        flowline_filename = os.path.join(in_dir,huc,'NHDFlowline' + str(huc) + '.gpkg')
        # waterbody_filename = os.path.join(in_dir,huc,'NHDPlusBurnWaterbody' + str(huc) + '.gpkg')

        if os.path.exists(os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')):

            burnline = gpd.read_file(burnline_filename)
            burnline = burnline[['NHDPlusID','ReachCode','geometry']]

            nhd_streams_vaa = gpd.read_file(vaa_filename)
            flowline = gpd.read_file(flowline_filename)
            flowline = flowline[['NHDPlusID','FType']]

            # waterbody = gpd.read_file(waterbody_filename)
            # waterbody = waterbody[['NHDPlusID']]
            # waterbody = waterbody.drop_duplicates()

            flowline = flowline.loc[flowline["FType"].isin([334,420,428,460,558])]

            nhd_streams_vaa = nhd_streams_vaa[['FromNode','ToNode','NHDPlusID','StreamOrde','DnLevelPat','LevelPathI']]
            nhd_streams_withVAA = burnline.merge(nhd_streams_vaa,on='NHDPlusID',how='inner')
            nhd_streams_fcode = nhd_streams_withVAA.merge(flowline,on='NHDPlusID',how='inner')

            nhd_streams = nhd_streams_fcode.to_crs(PREP_PROJECTION)
            nhd_streams = nhd_streams.loc[nhd_streams.geometry!=None,:] # special case: remove segments without geometries

            if os.path.isfile(nhd_streams_wVAA_fileName_pre):
                nhd_streams.to_file(nhd_streams_wVAA_fileName_pre,driver='GPKG',index=False, mode='a')
            else:
                nhd_streams.to_file(nhd_streams_wVAA_fileName_pre,driver='GPKG',index=False)
        else:
            print ('missing data for huc ' + str(huc))
    else:
        print ('skipping huc ' + str(huc))

## Identify Headwaters
if os.path.exists(nhd_streams_wVAA_fileName_pre):
    print ('reducing NHDPlus HR stream density')
    # Open WBD HUC4
    wbd_filename = os.path.join(wbd_dir, 'WBD_National.gpkg')
    wbd = gpd.read_file(wbd_filename, layer='WBDHU8')

    headwater_streams = pd.DataFrame()
    for index, row in tqdm(wbd.iterrows(),total=len(wbd)):
        huc = row["HUC8"]
        if huc.startswith('1209'): ######################################### delete #######################################################
            huc_mask = wbd.loc[wbd.HUC8.str.startswith(huc)]
            streams_subset = gpd.read_file(nhd_streams_wVAA_fileName_pre, mask = huc_mask)
            streams_subset.loc[:,'is_nwm_headwater'] = False
            streams_subset.loc[:,'nwm_headwaters_id'] = -9
            nwm_headwaters_mask = gpd.read_file(os.path.join(nwm_dir,'nwm_headwaters.gpkg'), mask = huc_mask)
            for index, row in nwm_headwaters_mask.iterrows():
                distances = streams_subset.distance(row['geometry'])
                min_index = np.argmin(distances)
                streams_subset.loc[min_index,'is_nwm_headwater'] = True
                streams_subset.loc[min_index,'nwm_headwaters_id'] = row['ID']
            headwater_streams = headwater_streams.append(streams_subset[['NHDPlusID','is_nwm_headwater','nwm_headwaters_id']])

    headwater_streams = headwater_streams.sort_values('is_nwm_headwater', ascending=False).drop_duplicates('NHDPlusID') # default keeps headwater=True for conflicting duplicates
    nhd_streams = nhd_streams.merge(headwater_streams,on='NHDPlusID',how='inner')

    ## Subset NHDPlus HR
    nhd_streams['is_nwm_stream'] = nhd_streams['is_nwm_headwater'].copy()
    nhd_streams['downstream_of_headwater'] = False

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
        nhd_streams.loc[relevant_ids,'downstream_of_headwater'] = True
        for i in relevant_ids:
            if i not in visited:
                Q.append(i)

    nhd_streams = nhd_streams.loc[nhd_streams['is_nwm_stream'],:]

    # write to files
    nhd_streams.reset_index(drop=True,inplace=True)
    nhd_streams.to_file(nhd_streams_wVAA_fileName_limited,driver='GPKG',index=False)

################################################################################
## Adjust headwater Streams
if os.path.exists(nhd_streams_wVAA_fileName_limited):
    print ('adjusting headwater stream segments')
    # convert geometries to WKB representation
    nhd_streams = nhd_streams.explode()
    nhd_streams_adj = nhd_streams.loc[(nhd_streams.nwm_headwaters_id > -9) & (nhd_streams.downstream_of_headwater == False),:].copy()

    headwaterstreams = []
    referencedpoints = []

    if nwm_headwaters is None:
        nwm_headwaters = gpd.read_file(os.path.join(nwm_dir,'nwm_headwaters.gpkg'))

    nwm_headwater_limited = nwm_headwaters.merge(nhd_streams_adj["nwm_headwaters_id"],left_on='ID', right_on="nwm_headwaters_id",how='right')

    for index, point in tqdm(nwm_headwater_limited.iterrows(),total=len(nwm_headwater_limited)):
        # convert headwaterpoint geometries to WKB representation
        wkb_points = dumps(point.geometry)
        # create pygeos headwaterpoint geometries from WKB representation
        pointbin_geom = pygeos.io.from_wkb(wkb_points)
        # Closest segment to headwater
        closest_stream = nhd_streams_adj.loc[nhd_streams_adj["nwm_headwaters_id"]==float(point.ID)]
        wkb_closest_stream = dumps(closest_stream.geometry[0])
        streambin_geom = pygeos.io.from_wkb(wkb_closest_stream)
        # Linear reference headwater to closest stream segment
        pointdistancetoline = pygeos.linear.line_locate_point(streambin_geom, pointbin_geom)
        referencedpoint = pygeos.linear.line_interpolate_point(streambin_geom, pointdistancetoline)
        # convert geometries to wkb representation
        bin_referencedpoint = pygeos.io.to_wkb(referencedpoint)
        # convert to shapely geometries
        shply_referencedpoint = loads(bin_referencedpoint)
        shply_linestring = loads(wkb_closest_stream)
        headpoint = Point(shply_referencedpoint.coords)
        cumulative_line = []
        relativedistlst = []
        # collect all nhd stream segment linestring verticies
        for point in zip(*shply_linestring.coords.xy):
            cumulative_line = cumulative_line + [point]
            relativedist = shply_linestring.project(Point(point))
            relativedistlst = relativedistlst + [relativedist]
        # add linear referenced headwater point to closest nhd stream segment
        if not headpoint in cumulative_line:
            cumulative_line = cumulative_line + [headpoint]
            relativedist = shply_linestring.project(headpoint)
            relativedistlst = relativedistlst + [relativedist]
        # sort by relative line distance to place headwater point in linestring
        sortline = pd.DataFrame({'geom' : cumulative_line, 'dist' : relativedistlst}).sort_values('dist')
        shply_linestring = LineString(sortline.geom.tolist())
        referencedpoints = referencedpoints + [headpoint]
        # split the new linestring at the new headwater point
        try:
            line1,line2 = split(shply_linestring, headpoint)
            headwaterstreams = headwaterstreams + [LineString(line1)]
            nhd_streams.loc[nhd_streams.NHDPlusID==closest_stream.NHDPlusID.values[0],'geometry'] = LineString(line1)
        except:
            line1 = split(shply_linestring, headpoint)
            headwaterstreams = headwaterstreams + [LineString(line1[0])]
            nhd_streams.loc[nhd_streams.NHDPlusID==closest_stream.NHDPlusID.values[0],'geometry'] = LineString(line1[0])

    nhd_streams = nhd_streams.drop(columns=['is_nwm_stream', 'nwm_headwaters_id', 'downstream_of_headwater'])
    nhd_streams.to_file(nhd_streams_wVAA_fileName_adjusted,driver='GPKG',index=False)

    ## create NHD adjusted headwater points
    # identify true nhd headwaters
    print('Identify NHD headwater points',flush=True)
    nhd_headwater_streams = nhd_streams.loc[nhd_streams['is_nwm_headwater'],:]
    # nhd_headwater_streams = nhd_streams.loc[(nhd_streams["is_nwm_headwater"]) !& (nhd_streams["downstream_of_headwater"]), :]

    hw_points = np.zeros(len(nhd_headwater_streams),dtype=object)
    for index,lineString in enumerate(nhd_headwater_streams.geometry):
        hw_point = [point for point in zip(*lineString.coords.xy)][-1]
        hw_points[index] = Point(*hw_point)

    nhd_headwater_points = gpd.GeoDataFrame({'NHDPlusID' : nhd_headwater_streams['NHDPlusID'],
                                            'geometry' : hw_points},geometry='geometry',crs=PREP_PROJECTION)

    nhd_headwater_points.to_file(nhd_headwaters_fileName,driver='GPKG',index=False)
    del nhd_headwater_streams, nhd_headwater_points
