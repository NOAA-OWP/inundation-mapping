#!/usr/bin/env python3

import os
import pandas as pd
import geopandas as gpd
from utils.shared_variables import PREP_PROJECTION
from utils.shared_functions import getDriver
from reduce_nhd_stream_density import subset_nhd_network
from adjust_headwater_streams import adjust_headwaters
from shapely.geometry import Point
from concurrent.futures import ProcessPoolExecutor
from collections import deque
import numpy as np
from shapely.wkb import dumps, loads
import pygeos


def identify_nwm_ms_streams(nwm_streams_filename, ahps_filename, nwm_streams_all_filename):
    # Subset nwm network to ms
    ahps_headwaters = gpd.read_file(ahps_filename)

    nwm_streams = gpd.read_file(nwm_streams_filename)

    # Remove mainstem column if it already exists
    nwm_streams = nwm_streams.drop(['mainstem'], axis=1, errors='ignore')

    nwm_streams['is_headwater'] = False

    nwm_streams.loc[nwm_streams.ID.isin(list(ahps_headwaters.nwm_featur)), 'is_headwater'] = True

    # Subset NHDPlus HR
    nwm_streams['is_relevant_stream'] = nwm_streams['is_headwater'].copy()

    nwm_streams = nwm_streams.explode(index_parts=True)

    # Trace down from headwaters
    nwm_streams.set_index('ID', inplace=True, drop=False)

    Q = deque(nwm_streams.loc[nwm_streams['is_headwater'], 'ID'].tolist())
    visited = set()

    while Q:
        q = Q.popleft()
        if q in visited:
            continue

        visited.add(q)
        toNode = nwm_streams.loc[q, 'to']

        if not toNode == 0:
            nwm_streams.loc[nwm_streams.ID == toNode, 'is_relevant_stream'] = True

            if toNode not in visited:
                Q.append(toNode)

    nwm_streams_ms = nwm_streams.loc[nwm_streams['is_relevant_stream'], :]
    ms_segments = nwm_streams_ms.ID.to_list()

    nwm_streams.reset_index(drop=True, inplace=True)

    # Add column to FR nwm layer to indicate MS segments
    nwm_streams['mainstem'] = np.where(nwm_streams.ID.isin(ms_segments), 1, 0)

    nwm_streams = nwm_streams.drop(['is_relevant_stream', 'is_headwater'], axis=1, errors='ignore')

    nwm_streams.to_file(
        nwm_streams_all_filename,
        driver=getDriver(nwm_streams_all_filename),
        index=False,
        layer='nwm_streams',
    )

    return ms_segments


def find_nwm_incoming_streams(nwm_streams_, wbd, huc_unit):
    # Input wbd
    if isinstance(wbd, str):
        layer = f"WBDHU{huc_unit}"
        wbd = gpd.read_file(wbd, layer=layer)
    elif isinstance(wbd, gpd.GeoDataFrame):
        pass
    else:
        raise TypeError("Pass dataframe or filepath for wbd")

    intersecting_points = []
    nhdplus_ids = []
    mainstem_flag = []
    print(f"iterating through {len(wbd)} hucs")
    for index, row in wbd.iterrows():
        col_name = f"HUC{huc_unit}"
        huc = row[col_name]
        huc_mask = wbd.loc[wbd[col_name] == str(huc)]
        huc_mask = huc_mask.explode(index_parts=True)
        huc_mask = huc_mask.reset_index(drop=True)

        # Input nwm streams
        if isinstance(nwm_streams_, str):
            nwm_streams = gpd.read_file(nwm_streams_, mask=huc_mask)
        elif isinstance(nwm_streams_, gpd.GeoDataFrame):
            nwm_streams = nwm_streams_.copy()
        else:
            raise TypeError("Pass dataframe or filepath for nwm streams")

        nwm_streams = nwm_streams.explode(index_parts=True)
        nwm_streams = nwm_streams.reset_index(drop=True)

        for index, polygon in enumerate(huc_mask.geometry):
            crosses = nwm_streams.crosses(polygon.exterior)
            nwm_streams_subset = nwm_streams[crosses]
            nwm_streams_subset = nwm_streams_subset.reset_index(drop=True)

            for index, segment in nwm_streams_subset.iterrows():
                distances = []

                try:
                    nhdplus_id = segment.ID
                except:
                    nhdplus_id = segment.NHDPlusID

                linestring = segment.geometry
                mainstem = segment.mainstem

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
                pointdistancetoline = pygeos.linear.line_locate_point(
                    polybin_geom, stream_point_geom
                )
                referencedpoint = pygeos.linear.line_interpolate_point(
                    polybin_geom, pointdistancetoline
                )

                # Convert geometries to wkb representation
                bin_referencedpoint = pygeos.io.to_wkb(referencedpoint)

                # Convert to shapely geometries
                shply_referencedpoint = loads(bin_referencedpoint)

                # Collect all nhd stream segment linestring verticies
                intersecting_points = intersecting_points + [shply_referencedpoint]
                nhdplus_ids = nhdplus_ids + [nhdplus_id]
                mainstem_flag = mainstem_flag + [mainstem]

        del huc_mask

    huc_intersection = gpd.GeoDataFrame(
        {'geometry': intersecting_points, 'NHDPlusID': nhdplus_ids, 'mainstem': mainstem_flag},
        crs=nwm_streams.crs,
        geometry='geometry',
    )
    huc_intersection = huc_intersection.drop_duplicates()

    del nwm_streams, wbd

    return huc_intersection


def collect_stream_attributes(nhdplus_vectors_dir, huc):
    print(f"Starting attribute collection for HUC {huc}", flush=True)

    # Collecting NHDPlus HR attributes
    burnline_filename = os.path.join(
        nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '.gpkg'
    )
    vaa_filename = os.path.join(nhdplus_vectors_dir, huc, 'NHDPlusFlowLineVAA' + str(huc) + '.gpkg')
    flowline_filename = os.path.join(nhdplus_vectors_dir, huc, 'NHDFlowline' + str(huc) + '.gpkg')

    if os.path.exists(
        os.path.join(nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '.gpkg')
    ):
        burnline = gpd.read_file(burnline_filename)
        burnline = burnline[['NHDPlusID', 'ReachCode', 'geometry']]
        flowline = gpd.read_file(flowline_filename)
        flowline = flowline[['NHDPlusID', 'FType', 'FCode']]
        # flowline = flowline.loc[flowline["FType"].isin([334,420,428,460,558])]
        flowline = flowline.loc[~flowline["FType"].isin([566, 420])]

        nhd_streams_vaa = gpd.read_file(vaa_filename)
        nhd_streams_vaa = nhd_streams_vaa[
            ['FromNode', 'ToNode', 'NHDPlusID', 'StreamOrde', 'DnLevelPat', 'LevelPathI']
        ]
        nhd_streams = burnline.merge(nhd_streams_vaa, on='NHDPlusID', how='inner')
        nhd_streams = nhd_streams.merge(flowline, on='NHDPlusID', how='inner')

        del burnline, flowline, nhd_streams_vaa

        nhd_streams = nhd_streams.to_crs(PREP_PROJECTION)
        nhd_streams = nhd_streams.loc[
            nhd_streams.geometry != None, :
        ]  # special case: remove segments without geometries
        nhd_streams['HUC4'] = str(huc)

        # special case; breach in network at Tiber Dam
        if (
            huc == '1003'
            and nhd_streams.loc[nhd_streams.NHDPlusID == 23001300078682.0, 'DnLevelPat']
            == 23001300001574.0
        ):
            nhd_streams = nhd_streams.loc[nhd_streams.NHDPlusID != 23001300009084.0]
            nhd_streams.loc[
                nhd_streams.NHDPlusID == 23001300078682.0, 'DnLevelPat'
            ] = 23001300001566.0

        # Write out NHDPlus HR aggregated
        nhd_streams_agg_fileName = os.path.join(
            nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg'
        )
        nhd_streams.to_file(
            nhd_streams_agg_fileName, driver=getDriver(nhd_streams_agg_fileName), index=False
        )

        del nhd_streams

        print(f"finished attribute collection for HUC {huc}", flush=True)

    else:
        print(f"missing data for HUC {huc}", flush=True)


def subset_stream_networks(args, huc):
    nwm_headwaters_filename = args[0]
    ahps_filename = args[1]
    wbd4 = args[2]
    wbd8 = args[3]
    nhdplus_vectors_dir = args[4]
    nwm_huc4_intersections_filename = args[5]

    print(f"starting stream subset for HUC {huc}", flush=True)
    nwm_headwater_id = 'ID'
    ahps_headwater_id = 'nws_lid'
    headwater_pts_id = 'site_id'

    column_order = ['pt_type', headwater_pts_id, 'mainstem', 'geometry']
    nhd_streams_filename = os.path.join(
        nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg'
    )

    # Subset to reduce footprint
    selected_wbd4 = wbd4.loc[wbd4.HUC4.str.startswith(huc)]
    del wbd4
    selected_wbd8 = wbd8.loc[wbd8.HUC8.str.startswith(huc)]
    del wbd8

    huc_mask = selected_wbd4.loc[selected_wbd4.HUC4.str.startswith(huc)]
    huc_mask = huc_mask.explode(index_parts=True)
    huc_mask = huc_mask.reset_index(drop=True)

    if len(selected_wbd8.HUC8) > 0:
        selected_wbd8 = selected_wbd8.reset_index(drop=True)

        # Identify FR/NWM headwaters and subset HR network
        try:
            nhd_streams_fr = subset_nhd_network(
                huc,
                huc_mask,
                selected_wbd8,
                nhd_streams_filename,
                nwm_headwaters_filename,
                nwm_headwater_id,
                nwm_huc4_intersections_filename,
            )
        except:
            print(f"Error subsetting NHD HR network for HUC {huc}", flush=True)

        # Identify nhd mainstem streams
        try:
            nhd_streams_all = subset_nhd_network(
                huc,
                huc_mask,
                selected_wbd8,
                nhd_streams_fr,
                ahps_filename,
                ahps_headwater_id,
                nwm_huc4_intersections_filename,
                True,
            )
        except:
            print(f"Error identifing MS network for HUC {huc}", flush=True)

        # Identify HUC8 intersection points
        nhd_huc8_intersections = find_nwm_incoming_streams(nhd_streams_all, selected_wbd8, 8)

        # Load nwm headwaters
        nwm_headwaters = gpd.read_file(nwm_headwaters_filename, mask=huc_mask)
        nwm_headwaters['pt_type'] = 'nwm_headwater'
        nwm_headwaters = nwm_headwaters.rename(columns={"ID": headwater_pts_id})

        # Load nws lids
        nws_lids = gpd.read_file(ahps_filename, mask=huc_mask)
        nws_lids = nws_lids.drop(
            columns=[
                'name',
                'nwm_feature_id',
                'usgs_site_code',
                'states',
                'HUC8',
                'is_headwater',
                'is_colocated',
            ]
        )
        nws_lids = nws_lids.rename(columns={"nws_lid": headwater_pts_id})
        nws_lids['pt_type'] = 'nws_lid'
        nws_lids['mainstem'] = True

        if (len(nwm_headwaters) > 0) or (len(nws_lids) > 0):
            # Adjust FR/NWM headwater segments
            adj_nhd_streams_all, adj_nhd_headwater_points = adjust_headwaters(
                huc, nhd_streams_all, nwm_headwaters, nws_lids, headwater_pts_id
            )

            adj_nhd_headwater_points = adj_nhd_headwater_points[column_order]
            nhd_huc8_intersections['pt_type'] = 'nhd_huc8_intersections'
            nhd_huc8_intersections = nhd_huc8_intersections.rename(
                columns={"NHDPlusID": headwater_pts_id}
            )
            nhd_huc8_intersections = nhd_huc8_intersections[column_order]
            adj_nhd_headwater_points_all = pd.concat(
                [adj_nhd_headwater_points, nhd_huc8_intersections]
            )
            adj_nhd_headwater_points_all = adj_nhd_headwater_points_all.reset_index(drop=True)

            adj_nhd_streams_all_fileName = os.path.join(
                nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '_adj.gpkg'
            )
            adj_nhd_headwaters_all_fileName = os.path.join(
                nhdplus_vectors_dir, huc, 'nhd' + str(huc) + '_headwaters_adj.gpkg'
            )

            # Write out FR adjusted
            adj_nhd_streams_all.to_file(
                adj_nhd_streams_all_fileName,
                driver=getDriver(adj_nhd_streams_all_fileName),
                index=False,
            )
            adj_nhd_headwater_points_all.to_file(
                adj_nhd_headwaters_all_fileName,
                driver=getDriver(adj_nhd_headwaters_all_fileName),
                index=False,
            )

            del adj_nhd_streams_all, adj_nhd_headwater_points_all

        else:
            print(f"skipping headwater adjustments for HUC {huc}")

        del nhd_streams_fr

    print(f"finished stream subset for HUC {huc}", flush=True)


def clean_up_intermediate_files(nhdplus_vectors_dir):
    for huc in os.listdir(nhdplus_vectors_dir):
        agg_path = os.path.join(
            nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '_agg.gpkg'
        )
        streams_adj_path = os.path.join(
            nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '_adj.gpkg'
        )
        headwater_adj_path = os.path.join(
            nhdplus_vectors_dir, huc, 'nhd' + str(huc) + '_headwaters_adj.gpkg'
        )

        if os.path.exists(agg_path):
            os.remove(agg_path)

        if os.path.exists(streams_adj_path):
            os.remove(streams_adj_path)

        if os.path.exists(headwater_adj_path):
            os.remove(headwater_adj_path)


if __name__ == '__main__':
    # # Generate NWM Headwaters

    print('loading HUC4s')
    wbd4 = gpd.read_file(wbd_filename, layer='WBDHU4')
    print('loading HUC8s')
    wbd8 = gpd.read_file(wbd_filename, layer='WBDHU8')

    subset_arg_list = (
        nwm_headwaters_filename,
        ahps_filename,
        wbd4,
        wbd8,
        nhdplus_vectors_dir,
        nwm_huc4_intersections_filename,
    )
    huc_list = os.listdir(nhdplus_vectors_dir)

    missing_subsets = []
    for huc in os.listdir(nhdplus_vectors_dir):
        streams_adj_path = os.path.join(
            nhdplus_vectors_dir, huc, 'NHDPlusBurnLineEvent' + str(huc) + '_adj.gpkg'
        )
        if not os.path.isfile(streams_adj_path):
            missing_subsets = missing_subsets + [huc]

    print(f"Subsetting stream network for {len(missing_subsets)} HUC4s")
    num_workers = 11

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Preprocess nhd hr and add attributes
        # collect_attributes = [executor.submit(collect_stream_attributes, nhdplus_vectors_dir, str(huc)) for huc in huc_list]
        # Subset nhd hr network
        subset_results = [
            executor.submit(subset_stream_networks, subset_arg_list, str(huc))
            for huc in missing_subsets
        ]

    del wbd4, wbd8

    # Remove intermediate files
    # clean_up_intermediate_files(nhdplus_vectors_dir)
