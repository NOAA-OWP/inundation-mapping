#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import numpy as np
from os.path import splitext
from tqdm import tqdm
import argparse
import pygeos
from shapely.geometry import Point,LineString
from shapely.ops import split
from shapely.wkb import dumps, loads
from utils.shared_variables import PREP_PROJECTION
from utils.shared_functions import getDriver

def adjust_headwaters(huc,nhd_streams,headwaters,headwater_id):

    # identify true headwater segments
    if nhd_streams['headwaters_id'].dtype=='int':
        nhd_streams_adj = nhd_streams.loc[(nhd_streams.headwaters_id > 0) & (nhd_streams.downstream_of_headwater == False),:].copy()
        if headwaters[headwater_id].dtype != 'int': headwaters[headwater_id] = headwaters[headwater_id].astype(int)
    else:
        nhd_streams_adj = nhd_streams.loc[(nhd_streams.headwaters_id.notna()) & (nhd_streams.downstream_of_headwater == False),:].copy()

    nhd_streams_adj = nhd_streams_adj.explode()
    nhd_streams_adj = nhd_streams_adj.reset_index(drop=True)

    headwater_limited = headwaters.merge(nhd_streams_adj["headwaters_id"],left_on=headwater_id, right_on="headwaters_id",how='right')

    headwaterstreams = []
    referencedpoints = []

    for index, point in headwater_limited.iterrows():

        # convert headwaterpoint geometries to WKB representation
        wkb_points = dumps(point.geometry)

        # create pygeos headwaterpoint geometries from WKB representation
        pointbin_geom = pygeos.io.from_wkb(wkb_points)

        # Closest segment to headwater
        closest_stream = nhd_streams_adj.loc[nhd_streams_adj["headwaters_id"]==point[headwater_id]]

        try: # seeing inconsistent geometry objects even after exploding nhd_streams_adj; not sure why this is
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

    nhd_streams = nhd_streams.drop(columns=['is_relevant_stream', 'headwaters_id', 'downstream_of_headwater'])

    try:
        del nhd_streams_adj, headwaters, headwater_limited, headwaterstreams, referencedpoints, cumulative_line, relativedistlst
    except:
        print ('issue deleting adjusted stream variables for huc ' + str(huc))

    ## identify ajusted nhd headwaters
    # print('Identify NHD headwater points',flush=True)
    nhd_headwater_streams_adj = nhd_streams.loc[nhd_streams['is_headwater'],:]
    nhd_headwater_streams_adj = nhd_headwater_streams_adj.explode()

    hw_points = np.zeros(len(nhd_headwater_streams_adj),dtype=object)
    for index,lineString in enumerate(nhd_headwater_streams_adj.geometry):
        hw_point = [point for point in zip(*lineString.coords.xy)][-1]
        hw_points[index] = Point(*hw_point)

    nhd_headwater_points_adj = gpd.GeoDataFrame({'NHDPlusID' : nhd_headwater_streams_adj['NHDPlusID'],
                                            'geometry' : hw_points},geometry='geometry',crs=PREP_PROJECTION)

    del nhd_headwater_streams_adj

    return(nhd_streams, nhd_headwater_points_adj)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='adjust headwater stream geometery based on headwater start points')
    parser.add_argument('-f','--huc',help='huc number',required=True)
    parser.add_argument('-l','--nhd-streams',help='NHDPlus HR geodataframe',required=True)
    parser.add_argument('-p','--headwaters',help='Headwater points layer',required=True,type=str)
    parser.add_argument('-s','--subset-nhd-streams-fileName',help='Output streams layer name',required=False,type=str,default=None)
    parser.add_argument('-s','--adj-headwater-points-fileName',help='Output adj headwater points layer name',required=False,type=str,default=None)
    parser.add_argument('-g','--headwater-points-fileName',help='Output headwater points layer name',required=False,type=str,default=None)
    parser.add_argument('-i','--headwater-id',help='Output headwaters points',required=True)

    args = vars(parser.parse_args())

    adj_streams_gdf,adj_headwaters_gdf = adjust_headwaters(huc,nhd_streams,headwaters,headwater_id)

    if subset_nhd_streams_fileName is not None:
        adj_streams_gdf.to_file(args['subset_nhd_streams_fileName'],driver=getDriver(args['subset_nhd_streams_fileName']),index=False)

    if headwater_points_fileName is not None:
        headwater_points_fileName.to_file(args['headwater_points_fileName'],driver=getDriver(args['headwater_points_fileName']),index=False)

    if adj_headwater_points_fileName is not None:
        adj_headwaters_gdf.to_file(args['adj_headwater_points_fileName'],driver=getDriver(args['adj_headwater_points_fileName']),index=False)
