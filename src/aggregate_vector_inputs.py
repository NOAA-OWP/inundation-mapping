#!/usr/bin/env python3

import os
import sys
import geopandas as gpd
from tqdm import tqdm
from os.path import splitext
from shapely.geometry import Point
from concurrent.futures import ProcessPoolExecutor,as_completed
from collections import deque
import numpy as np
from shapely.wkb import dumps, loads
import pygeos
sys.path.append('/foss_fim/src')
from utils.shared_variables import PREP_PROJECTION
from utils.shared_functions import getDriver
from derive_headwaters import findHeadWaterPoints
from reduce_nhd_stream_density import subset_nhd_network
from adjust_headwater_streams import adjust_headwaters
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

in_dir ='data/inputs/nhdplus_vectors'
nwm_dir = 'data/inputs/nwm_hydrofabric'
wbd_dir = 'data/inputs/wbd'
ahps_dir = 'data/inputs/ahp_sites'
agg_dir = 'data/inputs/nhdplus_vectors_aggregate'

wbd_filename = os.path.join(wbd_dir, 'WBD_National.gpkg')
nwm_streams_fr_filename = os.path.join(nwm_dir,'nwm_flows.gpkg')
nwm_headwaters_filename = os.path.join(nwm_dir,'nwm_headwaters.gpkg')
nwm_huc4_intersections_filename = os.path.join(nwm_dir,'nwm_huc4_intersections_NEW.gpkg')
nwm_huc8_intersections_filename = os.path.join(nwm_dir,'nwm_huc8_intersections.gpkg')
nhd_streams_ms_adjusted_fileName = os.path.join(agg_dir,'NHDPlusBurnLineEvent_ms_adjusted_NEW.gpkg')
nhd_streams_fr_adjusted_fileName = os.path.join(agg_dir,'NHDPlusBurnLineEvent_fr_adjusted_NEW.gpkg')

def identify_nwm_ms_streams(args):

    nwm_streams_filename    = args[0]
    in_dir                  = args[1]
    ahps_dir                = args[2]

    # Subset nwm network to ms
    ahps_headwaters_filename = os.path.join(ahps_dir,'nws_lid.gpkg')
    ahps_headwaters = gpd.read_file(ahps_headwaters_filename)

    nwm_streams = gpd.read_file(nwm_streams_filename)

    # Remove mainstem column if it already exists
    nwm_streams = nwm_streams.drop(['mainstem'], axis=1, errors='ignore')

    nwm_streams['is_headwater'] = False
    nwm_streams['downstream_of_headwater'] = False

    nwm_streams.loc[nwm_streams.ID.isin(list(ahps_headwaters.nwm_featur)),'is_headwater'] = True

    # Subset NHDPlus HR
    nwm_streams['is_relevant_stream'] = nwm_streams['is_headwater'].copy()

    nwm_streams = nwm_streams.explode()

    # Trace down from headwaters
    nwm_streams.set_index('ID',inplace=True,drop=False)

    Q = deque(nwm_streams.loc[nwm_streams['is_headwater'],'ID'].tolist())
    visited = set()

    while Q:
        q = Q.popleft()
        if q in visited:
            continue

        visited.add(q)
        toNode = nwm_streams.loc[q,'to']

        if not toNode == 0:

            nwm_streams.loc[nwm_streams.ID==toNode,'is_relevant_stream'] = True

            if toNode not in visited:
                Q.append(toNode)

    nwm_streams_ms = nwm_streams.loc[nwm_streams['is_relevant_stream'],:]

    ms_segments = nwm_streams_ms.ID.to_list()

    nwm_streams.reset_index(drop=True,inplace=True)

    # Add column to FR nwm layer to indicate MS segments
    nwm_streams['mainstem'] = np.where(nwm_streams.ID.isin(ms_segments), 1, 0)

    nwm_streams.to_file(nwm_streams_filename,driver=getDriver(nwm_streams_filename),index=False)


def find_nwm_incoming_streams(nwm_streams,wbd,huc_unit,in_dir):

    # input wbd
    if isinstance(wbd,str):
        layer = "WBDHU" + str(huc_unit)
        wbd = gpd.read_file(wbd, layer=layer)
    elif isinstance(wbd,gpd.GeoDataFrame):
        pass
    else:
        raise TypeError("Pass dataframe or filepath for wbd")

    intersecting_points = []
    nhdplus_ids = []
    for index, row in tqdm(wbd.iterrows(),total=len(wbd)):

        col_name = 'HUC' + str(huc_unit)
        huc = row[col_name]
        huc_mask = wbd.loc[wbd[col_name]==str(huc)]
        huc_mask = huc_mask.explode()
        huc_mask = huc_mask.reset_index(drop=True)

        # input nwm streams
        if isinstance(nwm_streams,str):
            nwm_streams = gpd.read_file(nwm_streams_filename, mask=huc_mask)
        elif isinstance(nwm_streams,gpd.GeoDataFrame):
            pass
        else:
            raise TypeError("Pass dataframe or filepath for nwm streams")

        nwm_streams = nwm_streams.explode()
        nwm_streams = nwm_streams.reset_index(drop=True)

        for index, polygon in enumerate(huc_mask.geometry):

            crosses=nwm_streams.crosses(polygon.exterior)
            nwm_streams_subset =nwm_streams[crosses]
            nwm_streams_subset = nwm_streams_subset.reset_index(drop=True)

            for index, segment in nwm_streams_subset.iterrows():

                distances = []
                nhdplus_id = segment.NHDPlusID
                linestring = segment.geometry

                # Distance to each stream segment
                for point in zip(*linestring.coords.xy):
                    distance = Point(point).distance(polygon.exterior)
                    distances = distances + [distance]

                # Find minimum distance
                min_index = np.argmin(distances)

                # Closest segment to headwater
                closest_point = list(linestring.coords)[min_index]
                last_node = Point(closest_point)

                # Convert geometries to WKB representation
                wkb_point = dumps(last_node)
                wkb_poly = dumps(polygon.exterior)

                # Create pygeos geometries from WKB representation
                stream_point_geom = pygeos.io.from_wkb(wkb_point)
                polybin_geom = pygeos.io.from_wkb(wkb_poly)

                # Linear reference end node to huc boundary
                pointdistancetoline = pygeos.linear.line_locate_point(polybin_geom,stream_point_geom)
                referencedpoint = pygeos.linear.line_interpolate_point(polybin_geom, pointdistancetoline)

                # Convert geometries to wkb representation
                bin_referencedpoint = pygeos.io.to_wkb(referencedpoint)

                # Convert to shapely geometries
                shply_referencedpoint = loads(bin_referencedpoint)

                # Collect all nhd stream segment linestring verticies
                intersecting_points = intersecting_points + [shply_referencedpoint]

                nhdplus_ids = nhdplus_ids + [nhdplus_id]

    huc_intersection = gpd.GeoDataFrame({'geometry': intersecting_points, 'NHDPlusID': nhdplus_ids},crs=nwm_streams.crs,geometry='geometry')
    huc_intersection = huc_intersection.drop_duplicates()

    return huc_intersection



def collect_stream_attributes(args, huc):

    print (f"Starting huc: {str(huc)}")
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

        # Write out NHDPlus HR aggregated
        nhd_streams_agg_fileName = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg')
        nhd_streams.to_file(nhd_streams_agg_fileName,driver=getDriver(nhd_streams_agg_fileName),index=False)
        del nhd_streams

        print (f"finished huc: {str(huc)}")

    else:
        print (f"missing data for huc {str(huc)}")


def subset_stream_networks(args, huc):
    nwm_dir                            = args[0]
    ahps_dir                           = args[1]
    wbd4                               = args[2]
    wbd8                               = args[3]
    in_dir                             = args[4]
    nwm_huc4_intersections_filename    = args[5]
    print(f"starting HUC {str(huc)}",flush=True)
    nwm_headwater_id = 'ID'
    nwm_headwaters_filename = os.path.join(nwm_dir,'nwm_headwaters.gpkg')
    ahps_headwater_id = 'nws_lid'
    ahps_headwaters_filename = os.path.join(ahps_dir,'nws_lid.gpkg')
    nhd_streams_filename = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg')
    # Subset to reduce footprint
    selected_wbd4 = wbd4.loc[wbd4.HUC4.str.startswith(str(huc))]
    del wbd4
    selected_wbd8 = wbd8.loc[wbd8.HUC8.str.startswith(huc)]
    del wbd8
    huc_mask = selected_wbd4.loc[selected_wbd4.HUC4.str.startswith(str(huc))]
    huc_mask = huc_mask.explode()
    huc_mask = huc_mask.reset_index(drop=True)
    if len(selected_wbd8.HUC8) > 0:
        selected_wbd8 = selected_wbd8.reset_index(drop=True)
        # Identify FR/NWM headwaters
        nhd_streams_fr = subset_nhd_network(huc,huc_mask,selected_wbd8,nhd_streams_filename,nwm_headwaters_filename,nwm_headwater_id,nwm_huc4_intersections_filename)
        nwm_huc8_intersections_fr = find_nwm_incoming_streams(nhd_streams_fr,selected_wbd8,8,in_dir)
        nwm_huc8_intersections_fr['intersection'] = True
        # Adjust FR/NWM headwater segments
        nwm_headwaters = gpd.read_file(nwm_headwaters_filename, mask=huc_mask)
        if len(nwm_headwaters) > 0:
            adj_nhd_streams_fr, adj_nhd_headwater_points_fr = adjust_headwaters(str(huc),nhd_streams_fr,nwm_headwaters,nwm_headwater_id)
            adj_nhd_headwater_points_fr['intersection'] = False
            adj_nhd_headwater_points_fr = adj_nhd_headwater_points_fr.append(nwm_huc8_intersections_fr)
            nhd_streams_fr_adjusted_fileName=os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_fr_adjusted.gpkg')
            adj_nhd_headwaters_fr_fileName=os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_fr.gpkg')
            # Write out FR adjusted
            adj_nhd_streams_fr.to_file(nhd_streams_fr_adjusted_fileName,driver=getDriver(nhd_streams_fr_adjusted_fileName),index=False)
            adj_nhd_headwater_points_fr.to_file(adj_nhd_headwaters_fr_fileName,driver=getDriver(adj_nhd_headwaters_fr_fileName),index=False)
            del adj_nhd_streams_fr, adj_nhd_headwater_points_fr
        else:
            print (f"skipping FR headwater adjustments for HUC: {str(huc)}")
        del nhd_streams_fr
        # Identify MS/AHPs headwaters
        nhd_streams_ms = subset_nhd_network(huc,huc_mask,selected_wbd8,nhd_streams_filename,ahps_headwaters_filename,ahps_headwater_id,nwm_huc4_intersections_filename,True)
        nwm_huc8_intersections_ms = find_nwm_incoming_streams(nhd_streams_ms,selected_wbd8,8,in_dir)
        nwm_huc8_intersections_ms['intersection'] = True
        nwm_huc8_intersections_ms['mainstem'] = True
        # Adjust MS/AHPs headwater segments
        ahps_headwaters = gpd.read_file(ahps_headwaters_filename, mask=huc_mask)
        if len(ahps_headwaters) > 0:
            adj_nhd_streams_ms, adj_nhd_headwater_points_ms = adjust_headwaters(str(huc),nhd_streams_ms,ahps_headwaters,ahps_headwater_id)
            nhd_streams_ms_adjusted_fileName=os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_ms_adjusted.gpkg')
            adj_nhd_headwaters_ms_fileName=os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_ms.gpkg')
            # Write out MS adjusted
            adj_nhd_streams_ms.to_file(nhd_streams_ms_adjusted_fileName,driver=getDriver(nhd_streams_ms_adjusted_fileName),index=False)
            adj_nhd_headwater_points_ms['intersection'] = False
            ahps_headwaters = ahps_headwaters.drop(['name','nwm_featur'], axis=1, errors='ignore')
            ahps_headwaters['NHDPlusID'] = 0
            nwm_huc8_intersections_ms['nws_lid'] = 'FR'
            adj_nhd_headwater_points_ms = adj_nhd_headwater_points_ms.append(nwm_huc8_intersections_ms)
            adj_nhd_headwater_points_ms.to_file(adj_nhd_headwaters_ms_fileName,driver=getDriver(adj_nhd_headwaters_ms_fileName),index=False)
            del adj_nhd_streams_ms, adj_nhd_headwater_points_ms
        else:
            print (f"skipping MS headwater adjustments for HUC: {str(huc)}")
            del nhd_streams_ms


def aggregate_stream_networks(in_dir,agg_dir, huc_list):

    for huc in huc_list:

        # FR adjusted
        adj_nhd_headwaters_fr_fileName=os.path.join(agg_dir,'nhd_headwaters_adjusted_fr_NEW.gpkg')
        nhd_fr_adj_huc_subset = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_fr_adjusted.gpkg')
        nhd_streams_fr_adjusted_fileName=os.path.join(agg_dir,'NHDPlusBurnLineEvent_fr_adjusted_NEW.gpkg')
        nhd_fr_adj_headwaters_subset = os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_fr.gpkg')

        if os.path.isfile(nhd_fr_adj_huc_subset):
            adj_nhd_streams_fr = gpd.read_file(nhd_fr_adj_huc_subset)

            # Write out FR adjusted
            if os.path.isfile(nhd_streams_fr_adjusted_fileName):
                adj_nhd_streams_fr.to_file(nhd_streams_fr_adjusted_fileName,driver=getDriver(nhd_streams_fr_adjusted_fileName),index=False, mode='a')
            else:
                adj_nhd_streams_fr.to_file(nhd_streams_fr_adjusted_fileName,driver=getDriver(nhd_streams_fr_adjusted_fileName),index=False)
            del adj_nhd_streams_fr

        if os.path.isfile(nhd_fr_adj_headwaters_subset):
            adj_nhd_headwater_points_fr = gpd.read_file(nhd_fr_adj_headwaters_subset)

            # Write out FR adjusted
            if os.path.isfile(adj_nhd_headwaters_fr_fileName):
                adj_nhd_headwater_points_fr.to_file(adj_nhd_headwaters_fr_fileName,driver=getDriver(adj_nhd_headwaters_fr_fileName),index=False, mode='a')
            else:
                adj_nhd_headwater_points_fr.to_file(adj_nhd_headwaters_fr_fileName,driver=getDriver(adj_nhd_headwaters_fr_fileName),index=False)
            del adj_nhd_headwater_points_fr

        # MS adjusted
        adj_nhd_headwaters_ms_fileName=os.path.join(agg_dir,'nhd_headwaters_adjusted_ms_NEW.gpkg')
        nhd_ms_adj_huc_subset = os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_ms_adjusted.gpkg')
        nhd_streams_ms_adjusted_fileName=os.path.join(agg_dir,'NHDPlusBurnLineEvent_ms_adjusted_NEW.gpkg')
        nhd_ms_adj_headwater_subset = os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_ms.gpkg')

        if os.path.isfile(nhd_ms_adj_huc_subset):
            adj_nhd_streams_ms = gpd.read_file(nhd_ms_adj_huc_subset)

            # Write out ms adjusted
            if os.path.isfile(nhd_streams_ms_adjusted_fileName):
                adj_nhd_streams_ms.to_file(nhd_streams_ms_adjusted_fileName,driver=getDriver(nhd_streams_ms_adjusted_fileName),index=False, mode='a')
            else:
                adj_nhd_streams_ms.to_file(nhd_streams_ms_adjusted_fileName,driver=getDriver(nhd_streams_ms_adjusted_fileName),index=False)

            del adj_nhd_streams_ms

        if os.path.isfile(nhd_ms_adj_headwater_subset):
            adj_nhd_headwater_points_ms = gpd.read_file(nhd_ms_adj_headwater_subset)

            # Write out ms adjusted
            if os.path.isfile(adj_nhd_headwaters_ms_fileName):
                adj_nhd_headwater_points_ms.to_file(adj_nhd_headwaters_ms_fileName,driver=getDriver(adj_nhd_headwaters_ms_fileName),index=False, mode='a')
            else:
                adj_nhd_headwater_points_ms.to_file(adj_nhd_headwaters_ms_fileName,driver=getDriver(adj_nhd_headwaters_ms_fileName),index=False)

            del adj_nhd_headwater_points_ms


def clean_up_intermediate_files(in_dir):

    for huc in os.listdir(in_dir):

        # agg_path= os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg')

        fr_adj_path= os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_fr_adjusted.gpkg')

        ms_adj_path= os.path.join(in_dir,huc,'NHDPlusBurnLineEvent' + str(huc) + '_ms_adjusted.gpkg')

        ms_headwater_adj_path= os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_ms.gpkg')
        fr_headwater_adj_path= os.path.join(in_dir,huc,'nhd' + str(huc) + '_headwaters_adjusted_fr.gpkg')

        # if os.path.exists(agg_path):
        #     os.remove(agg_path)

        if os.path.exists(fr_adj_path):
            os.remove(fr_adj_path)

        if os.path.exists(ms_adj_path):
            os.remove(ms_adj_path)

        if os.path.exists(ms_headwater_adj_path):
            os.remove(ms_headwater_adj_path)

        if os.path.exists(fr_headwater_adj_path):
            os.remove(fr_headwater_adj_path)


if(__name__=='__main__'):

    # Generate NWM Headwaters
    print ('deriving nwm headwater points')
    nwm_headwaters = findHeadWaterPoints(nwm_streams_fr_filename)
    nwm_headwaters['ID'] = nwm_headwaters.index + 1
    nwm_headwaters.to_file(nwm_headwaters_filename,driver=getDriver(nwm_headwaters_filename),index=False)

    del nwm_headwaters, nwm_streams

    # Identify NWM MS Streams
    identify_nwm_ms_args = (nwm_streams_fr_filename,in_dir,ahps_dir)
    print ('identifing nwm ms streams')
    identify_nwm_ms_streams(identify_nwm_ms_args)

    # Generate NWM intersection points with WBD4 boundaries
    print ('deriving NWM fr/ms intersection points')
    huc_intersection = find_nwm_incoming_streams(nwm_streams_fr_filename,wbd_filename,4,in_dir)
    huc_intersection.to_file(nwm_huc4_intersections_filename,driver=getDriver(nwm_huc4_intersections_filename))

    print ('loading wb4')
    wbd4 = gpd.read_file(wbd_filename, layer='WBDHU4')
    print ('loading wb8')
    wbd8 = gpd.read_file(wbd_filename, layer='WBDHU8')

    collect_arg_list = (in_dir,nwm_dir,ahps_dir)
    subset_arg_list = (nwm_dir,ahps_dir,wbd4,wbd8,in_dir,nwm_huc4_intersections_filename)

    num_workers = 14

with ProcessPoolExecutor(max_workers=num_workers) as executor:
    # Preprocess NHD HR and add attributes
    # collect_attributes = [executor.submit(collect_stream_attributes, collect_arg_list, str(huc)) for huc in os.listdir(in_dir)]
    # Subset NHD HR network
    subset_results = [executor.submit(subset_stream_networks, subset_arg_list, str(huc)) for huc in os.listdir(in_dir)]

    # Generate NWM intersection points with WBD8 boundaries using subset_stream_networks
    # huc_intersection = find_nwm_incoming_streams(nhd_streams_fr_adjusted_fileName,wbd_filename,8,in_dir)
    # huc_intersection.to_file(nwm_huc8_intersections_filename,driver=getDriver(nwm_huc8_intersections_filename))

    # Aggregate fr and ms nhd netowrks for entire nwm domain
    aggregate_stream_networks(in_dir,agg_dir, os.listdir(in_dir))

    # Remove intermediate files
    clean_up_intermediate_files(in_dir)
