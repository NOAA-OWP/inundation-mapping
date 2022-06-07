#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
import argparse
import pygeos
from shapely.geometry import Point,LineString
from shapely.ops import split
from shapely.wkb import dumps, loads
from utils.shared_variables import PREP_PROJECTION
from utils.shared_functions import getDriver
import warnings
warnings.simplefilter("ignore")


def adjust_headwaters(huc,nhd_streams,nwm_headwaters,nws_lids,headwater_id):

    # Identify true headwater segments
    nhd_streams_adj = nhd_streams.loc[(nhd_streams.headwaters_id > 0) & (nhd_streams.downstream_of_headwater == False),:].copy()
    nhd_streams_adj = nhd_streams_adj.explode()
    nhd_streams_adj = nhd_streams_adj.reset_index(drop=True)

    if nwm_headwaters["site_id"].dtype != 'int': nwm_headwaters["site_id"] = nwm_headwaters["site_id"].astype(int)
    headwater_limited = nwm_headwaters.merge(nhd_streams_adj[["headwaters_id","mainstem"]],left_on="site_id", right_on="headwaters_id",how='right')
    headwater_limited = headwater_limited.drop(columns=['headwaters_id'])

    nws_lid_limited = nws_lids.merge(nhd_streams[["nws_lid"]],left_on="site_id", right_on="nws_lid",how='right')
    nws_lid_limited = nws_lid_limited.loc[nws_lid_limited.nws_lid!='']
    nws_lid_limited = nws_lid_limited.drop(columns=['nws_lid'])

    # Check for issues in nws_lid layer (now this reports back non-headwater nws_lids)
    # if len(nws_lid_limited) < len(nws_lids):
    #     missing_nws_lids = list(set(nws_lids.site_id) - set(nws_lid_limited.site_id))
    #     print (f"nws lid(s) {missing_nws_lids} missing from aggregate dataset in huc {huc}")

    # Combine NWM headwaters and AHPS sites to be snapped to NHDPlus HR segments
    headwater_pts = headwater_limited.append(nws_lid_limited)
    headwater_pts = headwater_pts.reset_index(drop=True)

    if headwater_pts is not None:

        headwaterstreams = []
        referencedpoints = []
        snapped_ahps = []
        nws_lid = []
        for index, point in headwater_pts.iterrows():

            # Convert headwaterpoint geometries to WKB representation
            wkb_points = dumps(point.geometry)

            # Create pygeos headwaterpoint geometries from WKB representation
            pointbin_geom = pygeos.io.from_wkb(wkb_points)

            if point.pt_type == 'nwm_headwater':
                # Closest segment to headwater
                closest_stream = nhd_streams_adj.loc[nhd_streams_adj["headwaters_id"]==point[headwater_id]]
            else:
                # Closest segment to ahps site
                closest_stream = nhd_streams.loc[nhd_streams["nws_lid"]==point[headwater_id]]

            try: # Seeing inconsistent geometry objects even after exploding nhd_streams_adj; not sure why this is
                closest_stream =closest_stream.explode()
            except:
                pass

            try:
                wkb_closest_stream = dumps(closest_stream.geometry[0])
            except:
                wkb_closest_stream = dumps(closest_stream.geometry[0][0])

            streambin_geom = pygeos.io.from_wkb(wkb_closest_stream)

            # Linear reference headwater to closest stream segment
            pointdistancetoline = pygeos.linear.line_locate_point(streambin_geom, pointbin_geom)
            referencedpoint = pygeos.linear.line_interpolate_point(streambin_geom, pointdistancetoline)

            # Convert geometries to wkb representation
            bin_referencedpoint = pygeos.io.to_wkb(referencedpoint)

            # Convert to shapely geometries
            shply_referencedpoint = loads(bin_referencedpoint)
            shply_linestring = loads(wkb_closest_stream)
            headpoint = Point(shply_referencedpoint.coords)

            if point.pt_type == 'nwm_headwater':
                cumulative_line = []
                relativedistlst = []

                # Collect all nhd stream segment linestring verticies
                for point in zip(*shply_linestring.coords.xy):
                    cumulative_line = cumulative_line + [point]
                    relativedist = shply_linestring.project(Point(point))
                    relativedistlst = relativedistlst + [relativedist]

                # Add linear referenced headwater point to closest nhd stream segment
                if not headpoint in cumulative_line:
                    cumulative_line = cumulative_line + [headpoint]
                    relativedist = shply_linestring.project(headpoint)
                    relativedistlst = relativedistlst + [relativedist]

                # Sort by relative line distance to place headwater point in linestring
                sortline = pd.DataFrame({'geom' : cumulative_line, 'dist' : relativedistlst}).sort_values('dist')
                shply_linestring = LineString(sortline.geom.tolist())
                referencedpoints = referencedpoints + [headpoint]

                # Split the new linestring at the new headwater point
                try:
                    line1,line2 = split(shply_linestring, headpoint)
                    headwaterstreams = headwaterstreams + [LineString(line1)]
                    nhd_streams.loc[nhd_streams.NHDPlusID==closest_stream.NHDPlusID.values[0],'geometry'] = LineString(line1)
                except:
                    line1 = split(shply_linestring, headpoint)
                    headwaterstreams = headwaterstreams + [LineString(line1[0])]
                    nhd_streams.loc[nhd_streams.NHDPlusID==closest_stream.NHDPlusID.values[0],'geometry'] = LineString(line1[0])

                try:
                    del cumulative_line, relativedistlst
                except:
                    print (f"issue deleting adjusted stream variables for huc {huc}")

            else:
                snapped_ahps = snapped_ahps + [headpoint]
                nws_lid = nws_lid + [point[headwater_id]]

        nhd_streams = nhd_streams.drop(columns=['is_relevant_stream', 'headwaters_id', 'downstream_of_headwater'])

        try:
            del nhd_streams_adj, headwater_limited, referencedpoints, headwaterstreams
        except:
            print (f"issue deleting adjusted stream variables for huc {huc}")

        # Create snapped ahps sites
        if len(snapped_ahps) > 0:
            snapped_ahps_points = gpd.GeoDataFrame({'pt_type': 'nws_lid', headwater_id: nws_lid, 'mainstem': True,
                                                    'geometry': snapped_ahps},geometry='geometry',crs=PREP_PROJECTION)

        # Identify ajusted nhd headwaters
        nhd_headwater_streams_adj = nhd_streams.loc[nhd_streams['is_headwater'],:]
        nhd_headwater_streams_adj = nhd_headwater_streams_adj.explode()

        hw_points = np.zeros(len(nhd_headwater_streams_adj),dtype=object)
        for index,lineString in enumerate(nhd_headwater_streams_adj.geometry):
            hw_point = [point for point in zip(*lineString.coords.xy)][-1]
            hw_points[index] = Point(*hw_point)


        nhd_headwater_points_adj = gpd.GeoDataFrame({'pt_type': 'NHDPlusID', headwater_id: nhd_headwater_streams_adj['NHDPlusID'],
                                                 'mainstem': False, 'geometry': hw_points},geometry='geometry',crs=PREP_PROJECTION)

        nhd_headwater_points_adj = nhd_headwater_points_adj.reset_index(drop=True)

        del nhd_headwater_streams_adj

        try:
            combined_pts = snapped_ahps_points.append(nhd_headwater_points_adj)
        except:
            combined_pts = nhd_headwater_points_adj.copy()

        return nhd_streams, combined_pts


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='adjust headwater stream geometery based on headwater start points')
    parser.add_argument('-f','--huc',help='huc number',required=True)
    parser.add_argument('-l','--nhd-streams',help='NHDPlus HR geodataframe',required=True)
    parser.add_argument('-p','--nwm-headwaters',help='Headwater points layer',required=True,type=str)
    parser.add_argument('-s','--subset-nhd-streams-fileName',help='Output streams layer name',required=False,type=str,default=None)
    parser.add_argument('-a','--adj-headwater-points-fileName',help='Output adj headwater points layer name',required=False,type=str,default=None)
    parser.add_argument('-g','--headwater-points-fileName',help='Output headwater points layer name',required=False,type=str,default=None)
    parser.add_argument('-b','--nws-lids',help='NWS lid points',required=True)
    parser.add_argument('-i','--headwater-id',help='Headwater id column name',required=True)

    args = vars(parser.parse_args())

    #TODO variables below are not defined

    adj_streams_gdf, adj_headwaters_gdf = adjust_headwaters(huc,nhd_streams,nwm_headwaters,nws_lids,headwater_id)

    if subset_nhd_streams_fileName is not None:
        adj_streams_gdf.to_file(args['subset_nhd_streams_fileName'],driver=getDriver(args['subset_nhd_streams_fileName']))

    if headwater_points_fileName is not None:
        headwater_points_fileName.to_file(args['headwater_points_fileName'],driver=getDriver(args['headwater_points_fileName']))

    if adj_headwater_points_fileName is not None:
        adj_headwaters_gdf.to_file(args['adj_headwater_points_fileName'],driver=getDriver(args['adj_headwater_points_fileName']))
