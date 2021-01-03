#!/usr/bin/env python3

import os
import geopandas as gpd
from utils.shared_variables import PREP_PROJECTION
from utils.shared_functions import getDriver
from derive_headwaters import findHeadWaterPoints
from reduce_nhd_stream_density import subsetNHDnetwork
from adjust_headwater_streams import adjustHeadwaters
from tqdm import tqdm
from os.path import splitext
from shapely.geometry import Point
# from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor,as_completed

in_dir ='data/inputs/nhdplus_vectors'
nwm_dir = 'data/inputs/nwm_hydrofabric'
wbd_dir = 'data/inputs/wbd'
ahps_dir = 'data/inputs/ahp_sites'
agg_dir = 'data/inputs/nhdplus_vectors_aggregate'

## Generate NWM Headwaters
# print ('deriving NWM headwater points')
# nwm_streams = gpd.read_file(os.path.join(nwm_dir,'nwm_flows.gpkg'))
# nwm_headwaters = findHeadWaterPoints(nwm_streams)
# nwm_headwaters['ID'] = nwm_headwaters.index + 1
# nwm_headwaters.to_file(os.path.join(nwm_dir,'nwm_headwaters.gpkg'),driver='GPKG',index=False)
# del nwm_headwaters, nwm_streams

# subset nwm ms Network
def subset_nwm_ms_streams(args):
    nwm_streams_filename    = args[0]
    in_dir                  = args[1]
    ahps_dir                = args[2]

    # subset nwm network to ms
    ahps_headwaters_filename = os.path.join(ahps_dir,'bed_lids.gpkg')
    ahps_headwaters = gpd.read_file(ahps_headwaters_filename)

    nwm_streams = gpd.read_file(nwm_streams_filename)

    nwm_streams['is_headwater'] = True]

    nwm_streams.loc[nwm_streams.ID.isin(list(ahps_headwaters.nwm_featur)),'is_headwater'] = True
    nwm_streams.loc[nwm_streams.feature_ID.isin(ahps_headwaters.nwm_featur),'downstream_of_headwater'] = True

    ## subset NHDPlus HR
    nwm_streams['is_relevant_stream'] = nwm_streams['is_headwater'].copy()

    # trace down from headwaters
    nwm_streams.set_index('NHDPlusID',inplace=True,drop=False)

    Q = deque(nwm_streams.loc[nwm_streams[attribute],'feature_ID'].tolist())
    visited = set()

    while Q:
        q = Q.popleft()
        if q in visited:
            continue

        visited.add(q)
        toNode,DnLevelPat = nwm_streams.loc[q,['ToNode','DnLevelPat']]

        try:
            downstream_ids = nwm_streams.loc[nwm_streams['FromNode'] == toNode,:].index.tolist()
        except ValueError: # 18050002 has duplicate nhd stream feature
            if len(toNode.unique()) == 1:
                toNode = toNode.iloc[0]
                downstream_ids = nwm_streams.loc[nwm_streams['FromNode'] == toNode,:].index.tolist()

        # If multiple downstream_ids are returned select the ids that are along the main flow path (i.e. exclude segments that are diversions)
        if len(set(downstream_ids))>1: # special case: remove duplicate NHDPlusIDs
            relevant_ids = [segment for segment in downstream_ids if DnLevelPat == nwm_streams.loc[segment,'LevelPathI']]
        else:
            relevant_ids = downstream_ids

        nwm_streams.loc[relevant_ids,'is_relevant_stream'] = True
        nwm_streams.loc[relevant_ids,'downstream_of_headwater'] = True

        for i in relevant_ids:
            if i not in visited:
                Q.append(i)

def find_nwm_incoming_streams(args):

    nwm_streams_filename    = args[0]
    huc_size                = str(args[1])
    wbd_filename            = args[2]
    in_dir                  = args[3]

    layer = 'WBDHU' + str(huc_size)
    wbd = gpd.read_file(wbd_filename, layer=layer)
    #
    intersection_geometries = []
    for index, row in tqdm(wbd.iterrows(),total=len(wbd)):
        col_name = 'HUC' + str(huc_size)
        huc = row[col_name]
        #
        huc_mask = wbd.loc[wbd[col_name]==str(huc)]
        huc_mask = huc_mask.explode()
        huc_mask = huc_mask.reset_index(drop=True)
        #
        nwm_streams = gpd.read_file(nwm_streams_filename, mask=huc_mask)
        nwm_streams = nwm_streams.explode()
        nwm_streams = nwm_streams.reset_index(drop=True)
        #
        crosses=nwm_streams.crosses(huc_mask.geometry[0].exterior)
        nwm_streams = nwm_streams.loc[crosses,:]
        nwm_streams = nwm_streams.reset_index(drop=True)
        #
        intersection_points = set()
        for i,g in enumerate(nwm_streams.geometry):
            g_points = [(x,y) for x,y in zip(*g.coords.xy)]
            intersection_point = g_points[1]
            intersection_points.add(intersection_point)
        #
        intersection_points = list(intersection_points)
        #
        intersection_geometries = intersection_geometries + [Point(*hwp) for hwp in intersection_points]
    fr_huc8_intersection = gpd.GeoDataFrame({'geometry' : intersection_geometries},crs=nwm_streams.crs,geometry='geometry')





## Preprocess NHDPlus HR
def collect_stream_attributes(args, huc):
    print ('Starting huc: ' + str(huc))
    in_dir = args[0]
    nwm_dir = args[1]
    ahps_dir = args[2]

    print ('Collecting NHDPlus HR attributes')
    burnline_filename = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')
    vaa_filename = os.path.join(in_dir,huc,'NHDPlusFlowLineVAA' + str(huc) + '.gpkg')
    flowline_filename = os.path.join(in_dir,huc,'NHDFlowline' + str(huc) + '.gpkg')

    if os.path.exists(os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')):

        burnline = gpd.read_file(burnline_filename)
        burnline = burnline[['NHDPlusID','ReachCode','geometry']]

        flowline = gpd.read_file(flowline_filename)
        flowline = flowline[['NHDPlusID','FType','FCode']]
        # flowline = flowline.loc[flowline["FType"].isin([334,420,428,460,558])]
        flowline = flowline.loc[~flowline["FType"].isin([566,420])]

        nhd_streams_vaa = gpd.read_file(vaa_filename)
        nhd_streams_vaa = nhd_streams_vaa[['FromNode','ToNode','NHDPlusID','StreamOrde','DnLevelPat','LevelPathI']]
        nhd_streams = burnline.merge(nhd_streams_vaa,on='NHDPlusID',how='inner')
        nhd_streams = nhd_streams.merge(flowline,on='NHDPlusID',how='inner')

        del burnline, flowline, nhd_streams_vaa

        nhd_streams = nhd_streams.to_crs(PREP_PROJECTION)
        nhd_streams = nhd_streams.loc[nhd_streams.geometry!=None,:] # special case: remove segments without geometries
        nhd_streams['HUC4'] = str(huc)

        # write out NHDPlus HR aggregated
        nhd_streams_agg_fileName = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg')
        nhd_streams.to_file(nhd_streams_agg_fileName,driver=getDriver(nhd_streams_agg_fileName),index=False)
        del nhd_streams

        print ('finished huc: ' + str(huc))

    else:
        print ('missing data for huc ' + str(huc))

def subset_stream_networks(args, huc):

    nwm_dir    = args[0]
    ahps_dir   = args[1]
    wbd4       = args[2]
    wbd8       = args[3]
    in_dir     = args[4]

    print("starting HUC " + str(huc),flush=True)
    nwm_headwater_id = 'ID'
    nwm_headwaters_filename = os.path.join(nwm_dir,'nwm_headwaters.gpkg')
    ahps_headwater_id = 'nws_lid'
    ahps_headwaters_filename = os.path.join(ahps_dir,'bed_lids.gpkg')
    nhd_streams_filename = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg')

    # subset to reduce footprint
    selected_wbd4 = wbd4.loc[wbd4.HUC4.str.startswith(str(huc))]
    del wbd4
    selected_wbd8 = wbd8.loc[wbd8.HUC8.str.startswith(huc)]
    del wbd8

    huc_mask = selected_wbd4.loc[selected_wbd4.HUC4.str.startswith(str(huc))]
    huc_mask = huc_mask.explode()
    huc_mask = huc_mask.reset_index(drop=True)

    if len(selected_wbd8.HUC8) > 0:
        selected_wbd8 = selected_wbd8.reset_index(drop=True)

        # identify FR/NWM headwaters
        nhd_streams_fr = subsetNHDnetwork(huc,huc_mask,selected_wbd8,nhd_streams_filename,nwm_headwaters_filename,nwm_headwater_id)

        # write out FR subset
        nhd_streams_fr_fileName=os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_fr.gpkg')
        nhd_streams_fr.to_file(nhd_streams_fr_fileName,driver='GPKG',index=False)

        ## identify MS/AHPs headwaters
        nhd_streams_ms = subsetNHDnetwork(huc,huc_mask,selected_wbd8,nhd_streams_filename,ahps_headwaters_filename,ahps_headwater_id)

        ## adjust FR/NWM headwater segments
        nwm_headwaters = gpd.read_file(nwm_headwaters_filename, mask=huc_mask)

        if len(nwm_headwaters) > 0:
            adj_nhd_streams_fr, nhd_headwater_points_fr, adj_nhd_headwater_points_fr = adjustHeadwaters(str(huc),nhd_streams_fr,nwm_headwaters,nwm_headwater_id)


            nhd_streams_fr_adjusted_fileName=os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_fr_adjusted.gpkg')
            nhd_headwaters_fr_fileName=os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_fr.gpkg')
            adj_nhd_headwaters_fr_fileName=os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_fr.gpkg')
            # write out FR adjusted
            adj_nhd_streams_fr.to_file(nhd_streams_fr_adjusted_fileName,driver='GPKG',index=False)
            nhd_headwater_points_fr.to_file(nhd_headwaters_fr_fileName,driver='GPKG',index=False)
            adj_nhd_headwater_points_fr.to_file(adj_nhd_headwaters_fr_fileName,driver='GPKG',index=False)

            del adj_nhd_streams_fr, nhd_headwater_points_fr, adj_nhd_headwater_points_fr
        else:
            print ('skipping FR headwater adjustments for HUC: ' + str(huc))

        del nhd_streams_fr

        ## adjust MS/AHPs headwater segments
        ahps_headwaters = gpd.read_file(ahps_headwaters_filename, mask=huc_mask)

        if len(ahps_headwaters) > 0:
            adj_nhd_streams_ms, nhd_headwater_points_ms, adj_nhd_headwater_points_ms = adjustHeadwaters(str(huc),nhd_streams_ms,ahps_headwaters,ahps_headwater_id)

            # write out MS subset
            nhd_streams_ms_fileName=os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_ms.gpkg')
            nhd_streams_ms.to_file(nhd_streams_ms_fileName,driver='GPKG',index=False)


            nhd_streams_ms_adjusted_fileName=os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_ms_adjusted.gpkg')
            nhd_headwaters_ms_fileName=os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_ms.gpkg')
            adj_nhd_headwaters_ms_fileName=os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_ms.gpkg')
            # write out MS adjusted
            adj_nhd_streams_ms.to_file(nhd_streams_ms_adjusted_fileName,driver='GPKG',index=False)
            nhd_headwater_points_ms.to_file(nhd_headwaters_ms_fileName,driver='GPKG',index=False)
            adj_nhd_headwater_points_ms.to_file(adj_nhd_headwaters_ms_fileName,driver='GPKG',index=False)

            del adj_nhd_streams_ms, nhd_headwater_points_ms, adj_nhd_headwater_points_ms

        else:
            print ('skipping MS headwater adjustments for HUC: ' + str(huc))
            del nhd_streams_ms

def aggregate_stream_networks(in_dir,agg_dir, huc_list):

    for huc in huc_list:

        ## FR
        nhd_streams_fr_fileName=os.path.join(agg_dir,'NHDPlusBurnLineEvent_fr.gpkg')
        nhd_headwaters_fr_fileName=os.path.join(agg_dir,'nhd_headwaters_fr.gpkg')
        nhd_fr_huc_subset = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_fr.gpkg')
        nhd_fr_headwater_subset = os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_fr.gpkg')

        if os.path.isfile(nhd_fr_huc_subset):
            nhd_streams_fr = gpd.read_file(nhd_fr_huc_subset)

            # write out FR subset
            if os.path.isfile(nhd_streams_fr_fileName):
                nhd_streams_fr.to_file(nhd_streams_fr_fileName,driver=getDriver(nhd_streams_fr_fileName),index=False, mode='a')
            else:
                nhd_streams_fr.to_file(nhd_streams_fr_fileName,driver=getDriver(nhd_streams_fr_fileName),index=False)

            del nhd_streams_fr

        if os.path.isfile(nhd_fr_headwater_subset):
            nhd_headwater_points_fr = gpd.read_file(nhd_fr_headwater_subset)

            # write out FR subset
            if os.path.isfile(nhd_headwaters_fr_fileName):
                nhd_headwater_points_fr.to_file(nhd_headwaters_fr_fileName,driver=getDriver(nhd_headwaters_fr_fileName),index=False, mode='a')
            else:
                nhd_headwater_points_fr.to_file(nhd_headwaters_fr_fileName,driver=getDriver(nhd_headwaters_fr_fileName),index=False)

            del nhd_headwater_points_fr

        ## MS
        nhd_streams_ms_fileName=os.path.join(agg_dir,'NHDPlusBurnLineEvent_ms.gpkg')
        nhd_headwaters_ms_fileName=os.path.join(agg_dir,'nhd_headwaters_ms.gpkg')
        nhd_ms_huc_subset = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_ms.gpkg')
        nhd_ms_headwater_subset = os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_ms.gpkg')

        if os.path.isfile(nhd_ms_huc_subset):
            nhd_streams_ms = gpd.read_file(nhd_ms_huc_subset)

            # write out ms subset
            if os.path.isfile(nhd_streams_ms_fileName):
                nhd_streams_ms.to_file(nhd_streams_ms_fileName,driver=getDriver(nhd_streams_ms_fileName),index=False, mode='a')
            else:
                nhd_streams_ms.to_file(nhd_streams_ms_fileName,driver=getDriver(nhd_streams_ms_fileName),index=False)

            del nhd_streams_ms

        if os.path.isfile(nhd_ms_headwater_subset):
            nhd_headwater_points_ms = gpd.read_file(nhd_ms_headwater_subset)

            # write out ms subset
            if os.path.isfile(nhd_headwaters_ms_fileName):
                nhd_headwater_points_ms.to_file(nhd_headwaters_ms_fileName,driver=getDriver(nhd_headwaters_ms_fileName),index=False, mode='a')
            else:
                nhd_headwater_points_ms.to_file(nhd_headwaters_ms_fileName,driver=getDriver(nhd_headwaters_ms_fileName),index=False)

            del nhd_headwater_points_ms

        ## FR adjusted
        adj_nhd_headwaters_fr_fileName=os.path.join(agg_dir,'nhd_headwaters_adjusted_fr.gpkg')
        nhd_fr_adj_huc_subset = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_fr_adjusted.gpkg')
        nhd_streams_fr_adjusted_fileName=os.path.join(agg_dir,'NHDPlusBurnLineEvent_fr_adjusted.gpkg')
        nhd_fr_adj_headwaters_subset = os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_fr.gpkg')

        if os.path.isfile(nhd_fr_adj_huc_subset):
            adj_nhd_streams_fr = gpd.read_file(nhd_fr_adj_huc_subset)

            # write out FR adjusted
            if os.path.isfile(nhd_streams_fr_adjusted_fileName):
                adj_nhd_streams_fr.to_file(nhd_streams_fr_adjusted_fileName,driver=getDriver(nhd_streams_fr_adjusted_fileName),index=False, mode='a')
            else:
                adj_nhd_streams_fr.to_file(nhd_streams_fr_adjusted_fileName,driver=getDriver(nhd_streams_fr_adjusted_fileName),index=False)

            del adj_nhd_streams_fr

        if os.path.isfile(nhd_fr_adj_headwaters_subset):
            adj_nhd_headwater_points_fr = gpd.read_file(nhd_fr_adj_headwaters_subset)

            # write out FR adjusted
            if os.path.isfile(adj_nhd_headwaters_fr_fileName):
                adj_nhd_headwater_points_fr.to_file(adj_nhd_headwaters_fr_fileName,driver=getDriver(adj_nhd_headwaters_fr_fileName),index=False, mode='a')
            else:
                adj_nhd_headwater_points_fr.to_file(adj_nhd_headwaters_fr_fileName,driver=getDriver(adj_nhd_headwaters_fr_fileName),index=False)

            del adj_nhd_headwater_points_fr

        ## MS adjusted
        adj_nhd_headwaters_ms_fileName=os.path.join(agg_dir,'nhd_headwaters_adjusted_ms.gpkg')
        nhd_ms_adj_huc_subset = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_ms_adjusted.gpkg')
        nhd_streams_ms_adjusted_fileName=os.path.join(agg_dir,'NHDPlusBurnLineEvent_ms_adjusted.gpkg')
        nhd_ms_adj_headwater_subset = os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_ms.gpkg')

        if os.path.isfile(nhd_ms_adj_huc_subset):
            adj_nhd_streams_ms = gpd.read_file(nhd_ms_adj_huc_subset)


            # write out ms adjusted
            if os.path.isfile(nhd_streams_ms_adjusted_fileName):
                adj_nhd_streams_ms.to_file(nhd_streams_ms_adjusted_fileName,driver=getDriver(nhd_streams_ms_adjusted_fileName),index=False, mode='a')
            else:
                adj_nhd_streams_ms.to_file(nhd_streams_ms_adjusted_fileName,driver=getDriver(nhd_streams_ms_adjusted_fileName),index=False)

            del adj_nhd_streams_ms

        if os.path.isfile(nhd_ms_adj_headwater_subset):
            adj_nhd_headwater_points_ms = gpd.read_file(nhd_ms_adj_headwater_subset)

            # write out ms adjusted
            if os.path.isfile(adj_nhd_headwaters_ms_fileName):
                adj_nhd_headwater_points_ms.to_file(adj_nhd_headwaters_ms_fileName,driver=getDriver(adj_nhd_headwaters_ms_fileName),index=False, mode='a')
            else:
                adj_nhd_headwater_points_ms.to_file(adj_nhd_headwaters_ms_fileName,driver=getDriver(adj_nhd_headwaters_ms_fileName),index=False)

            del adj_nhd_headwater_points_ms


if(__name__=='__main__'):

    num_workers=8
    wbd_filename = os.path.join(wbd_dir, 'WBD_National.gpkg')
    nwm_streams_filename=os.path.join(nwm_dir,'nwm_flows.gpkg')

    print ('loading wb4')
    wbd4 = gpd.read_file(wbd_filename, layer='WBDHU4')
    print ('loading wb8')
    wbd8 = gpd.read_file(wbd_filename, layer='WBDHU8')
    nwm_incoming_streams_arg_list = (nwm_streams_filename,8,wbd_filename,in_dir)
    subset_arg_list = (nwm_dir,ahps_dir,wbd4,wbd8,in_dir)
    collect_arg_list = (in_dir,nwm_dir,ahps_dir)
    agg_arg_list = (in_dir,agg_dir, os.listdir(in_dir))


    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        collect_attributes = [executor.submit(collect_stream_attributes, collect_arg_list, str(huc)) for huc in os.listdir(in_dir)]
        subset_results = [executor.submit(subset_stream_networks, subset_arg_list, str(huc)) for huc in os.listdir(in_dir)]
    aggregate_stream_networks(in_dir,agg_dir, os.listdir(in_dir))
