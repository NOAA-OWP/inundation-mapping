#!/usr/bin/env python3

'''
Description:
    1) split stream segments based on lake boundaries and input threshold distance
    2) calculate channel slope, manning's n, and LengthKm for each segment
    3) create unique ids using HUC8 boundaries (and unique FIM_ID column)
    4) create network traversal attribute columns (To_Node, From_Node, NextDownID)
    5) create points layer with segment verticies encoded with HydroID's (used for catchment delineation in next step)
'''

import sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, MultiPoint
import rasterio
import numpy as np
import argparse
from tqdm import tqdm
import time
from os.path import isfile
from os import remove,environ
from collections import OrderedDict
import build_stream_traversal
from utils.shared_functions import getDriver, mem_profile
from utils.shared_variables import FIM_ID
import shapely # ornl
import shapely.geometry  # ornl
import rtree  # ornl

# ornl
def connect_seg_by_lake(lakeseq0):
    '''
    lakeseq0 is indexed by flowlines and the value of each element is
    associated lake id. it is likely that a stream line goes in and out 
    of a lake polygon multiple times, which produces a subsequence like
    0,a,a,0,0,a,0,a,0. The intention of lake overlay is to split an
    overlapped stream line into at most three parts: the part to the 
    inlet to the lake, the part in the lake, and the part from the outlet
    of the lake. this function converts the above subsequence to 
    0,a,a,a,a,a,a,a,0 so as to label lake id to consecutive segments 
    that belong to that lake.
    observations: let's add a virtual lake id to the beginning -1
    1. any lake seg begins with that lake id. no zeros in front 
       (otherwise it is not the beginning)
    2. any lake subseq ends with that lake id (zero cannot be at the end)
    3. the code is to implement the lake subseq matching regular expression 
       a(0*a+)*0*b
    
    Parameters
    ----------
    lakeseq0: array_like
        lakeseq0 is indexed by flowlines and the value of each element is
        associated lake id. 

    Returns
    -------
    array_like
        the new sequence with relabeled lake id
    '''
    lakeseq = lakeseq0.copy()
    curlake = -1 # suppose we start from late id -1
    numout = 0 # num of segments outside of the lake but need to be label with this lake id
    for i in range(len(lakeseq)): # now look next
        l = lakeseq[i]
        if l == 0: # find more outside edges
            numout += 1
        elif l == curlake: # belong to the same lake
            for j in range(numout): # fill in-between with the same lake id
                lakeseq[i-j-1] = curlake
            numout = 0 # reset counter
        else: # a new lake. already marked curlake range, outside segs are marked, too. just restart
            curlake = l
            numout = 0

    return lakeseq

# ornl
def split_through_polygons(flowline, ol_polygons, ol_lakes):
    '''
    Split a flowline into segments that overlap multiple lakes.
    usually, a flowline may overlap with only one lake. this
    function is designed to handle long flowlines. 
    when a flowline goes through an overlay of multiple polygons,
    the intention is to keep the segment in any polygon
    as a single segment.
    assumpution: polygons do not overlap.

    Parameters
    flowline : LineString
        the flowline LineString
    ol_polygons: array_like
        the list of lake polygons
    ol_lakes: array_like
        the list of lake IDs corresponding to lake polygons

    Returns
    -------
    tuple
        ([split flow LineStrings], [lake IDs])
    '''
    # use geopandas overlay's identity method to chop flowline
    gdf_flow = gpd.GeoDataFrame({'geometry': gpd.GeoSeries([flowline]), 'flow':[1]})
    gdf_lake = gpd.GeoDataFrame({'geometry': gpd.GeoSeries(ol_polygons), 'lake':ol_lakes})
    gdf_ol_ident = gpd.overlay(gdf_flow, gdf_lake, how='identity')
    #print('identity overlap generated', gdf_ol_ident.shape[0], 'segments')
    gdf_ol_ident = gdf_ol_ident.loc[~gdf_ol_ident.is_empty, :]
    #print('after removing empty geom, there are', gdf_ol_ident.shape[0], 'segments')
    gdf_ol_ident = gdf_ol_ident.explode().reset_index(drop=True)
    num_edges = gdf_ol_ident.shape[0]
    if num_edges == 1: # no split, accelerate by skipping the rest
        lakeid = gdf_ol_ident.iloc[0]['lake']
        if pd.isna(lakeid):
            lakeid = -999
        return [flowline], [lakeid]

    # hash segment end points and segments each point is connected to
    node2edges = {}
    inlake = np.zeros(num_edges, dtype=np.int32)
    for i, (flow, lake, geom) in gdf_ol_ident.iterrows():
        for p in [geom.coords[0], geom.coords[-1]]:
            if not p in node2edges:
                node2edges[p] = [i]
            else:
                node2edges[p].append(i)
        if not pd.isna(lake): # part of lake
            inlake[i] = lake # lake id needs to >0

    # order segments
    h_edges = np.zeros(num_edges, dtype=np.int8) # record if an edge has been visited
    inlet = flowline.coords[0]
    inlets = {inlet} # start from flowline's first end point
    ordered_edges = []
    for i in range(num_edges):
        edges = node2edges[inlet]
        for edge in edges:
            if h_edges[edge] == 0: # not visited before
                ordered_edges.append(edge) # add segment to ordered list
                h_edges[edge] = 1 # visited
                geom = gdf_ol_ident.iloc[edge]['geometry']
                outlet = None
                for p in [geom.coords[0], geom.coords[-1]]:
                    if not p in inlets: # outlet, i.e., next inlet
                        outlet = p
                        break
                if outlet is None:
                    print('ERROR could not find outlet')
                else:
                    inlet = outlet
                    inlets.add(inlet)

    # create splits
    lakeseq = [inlake[i] for i in ordered_edges]
    newseq = connect_seg_by_lake(lakeseq) # label consecutive segments using their lake ids
    #print('lakeseq', lakeseq)
    #print('lakeseq (labeled)', newseq)
    # now merge segments into line geometry
    newseq += [-1] # to handle the end
    lakeids = []
    flow_splits = []
    start = 0
    lakeid = newseq[0]
    for i in range(len(newseq)):
        if newseq[i] != lakeid: # merge prev segments
            lakeids.append(lakeid)
            combined_seg = [pnt for geom in gdf_ol_ident.iloc[ordered_edges[start:i]]['geometry'] for pnt in geom.coords[:-1]]
            combined_seg.append(gdf_ol_ident.iloc[ordered_edges[i-1]]['geometry'].coords[-1])
            flow_splits.append(shapely.geometry.LineString(combined_seg))
            start = i
            lakeid = newseq[i]

    return flow_splits, lakeids # lakeid == 0 if outside of the lake

# ornl
def construct_lakes_rtree(lakes, fn_lakeid):
    '''
    Construct r-tree for lakes in order to speed up queries
    
    Parameters
    ----------
    lakes : GeoDataFrame
        lakes GeoDataFrame
    fn_lakeid : str
        field(column) name of the lake ID

    Returns
    -------
    rtree
        the constructed lakes rtree
    '''
    def generate_items():
        index = 0
        for i, row in lakes.iterrows():
            box = row['geometry'].bounds
            yield (index, box, (i, row[fn_lakeid], row['geometry']))
            index += 1
    return rtree.index.Index(generate_items())

# ornl
def split_flows_on_lakes(flows, lakes, fn_lakeid, fn_new_lakeid):
    '''
    This function replaces the original overlay logic on lakes.

    Parameters
    ----------
    flows : GeoDataFrame
        flows data frame
    lakes : GeoDataFrame
        lakes data frame
    fn_lakeid : str
        field name of the lake ID in the input lakes data frame
    fn_new_lakeid : str
        field name of the lake ID in the output lakes data frame

    Returns
    -------
    GeoDataFrame
        The split flowline data frame
    '''
    if lakes is None or len(lakes) <= 0: # no need to split
        flows[fn_new_lakeid] = -999 # no lake
        return flows
    # build rtree on lake bboxes
    searchtree = construct_lakes_rtree(lakes, fn_lakeid)
    # split each flowline on lakes
    l_flows = []
    l_lakeids = []
    count_rtree_hits = 0
    count_splits = 0
    count_split_flowlines = 0
    count_flowlines_tosplit = 0
    for i, row in flows.iterrows():
        flowline = row['geometry']
        hits = searchtree.intersection(flowline.bounds, objects='raw')
        ol_lakes = []
        ol_polygons = []
        for j, lakeid, lakegeom in hits:
            ol_lakes.append(lakeid)
            ol_polygons.append(lakegeom)
        if len(ol_polygons) == 0: # no hits
            l_flows += [flowline]
            l_lakeids += [-999]
            continue
        count_rtree_hits += len(ol_polygons)
        count_flowlines_tosplit += 1
        # split flows by lake polygons
        flow_splits, lakeids = split_through_polygons(flowline, ol_polygons, ol_lakes)
        lakeids = [-999 if l==0 else l for l in lakeids]
        l_flows += flow_splits
        l_lakeids += lakeids
        if len(flow_splits)>1:
            count_splits += len(flow_splits)
            count_split_flowlines += 1
    print('split_flows_on_lakes: rtree efficiency:', 'space:', len(flows), 'x', len(lakes), '=', len(flows)*len(lakes),
         'match rate:', count_rtree_hits, '/', len(flows)*len(lakes), '=', count_rtree_hits/(len(flows)*len(lakes)))
    print('split_flows_on_lakes: split stats:', 'flowlines_considered', count_flowlines_tosplit,
         'split_ratio:', count_splits, '/', count_split_flowlines, '=', count_splits/count_split_flowlines )
    print('split_flows_on_lakes: preserved stats:', 'not_touched:',  (len(flows) - count_flowlines_tosplit),
          'no_split:', count_flowlines_tosplit - count_split_flowlines)
    return gpd.GeoDataFrame({'geometry': l_flows, fn_new_lakeid: l_lakeids}, crs=flows.crs, geometry='geometry')

# ornl
@mem_profile
def split_flows(max_length, slope_min, lakes_buffer_input, flows_filename, dem_filename, split_flows_filename, split_points_filename, wbd8_clp_filename, lakes_filename):
    wbd = gpd.read_file(wbd8_clp_filename)

    toMetersConversion = 1e-3

    print('Loading data ...')
    flows = gpd.read_file(flows_filename)

    if not len(flows) > 0:
        print ("No relevant streams within HUC boundaries.")
        sys.exit(0)

    wbd8 = gpd.read_file(wbd8_clp_filename)
    dem = rasterio.open(dem_filename,'r')

    if isfile(lakes_filename):
        lakes = gpd.read_file(lakes_filename)
    else:
        lakes = None

    wbd8 = wbd8.filter(items=[FIM_ID, 'geometry'])
    wbd8 = wbd8.set_index(FIM_ID)
    flows = flows.explode()

    # temp
    flows = flows.to_crs(wbd8.crs)

    split_flows = []
    slopes = []
    hydro_id = 'HydroID'

    # split at HUC8 boundaries
    print ('splitting stream segments at HUC8 boundaries')
    flows = gpd.overlay(flows, wbd8, how='union').explode().reset_index(drop=True)

    # check for lake features
    if lakes is not None:
        if len(lakes) > 0:
          print ('splitting stream segments at ' + str(len(lakes)) + ' waterbodies')
          #create splits at lake boundaries
          lakes = lakes.filter(items=['newID', 'geometry'])
          # remove empty geometries # ornl
          flows = flows.loc[~flows.is_empty,:] # ornl: this is necessary
          flows = split_flows_on_lakes(flows, lakes, 'newID', 'LakeID') # ornl

          #lakes = lakes.set_index('newID') # ornl
          #flows = gpd.overlay(flows, lakes, how='union').explode().reset_index(drop=True) # ornl
          #lakes_buffer = lakes.copy() # onrl
          #lakes_buffer['geometry'] = lakes.buffer(lakes_buffer_input) # adding X meter buffer for spatial join comparison (currently using 20meters) # ornl
    else: # ornl
        flows['LakeID'] = -999 # ornl

    print ('splitting ' + str(len(flows)) + ' stream segments based on ' + str(max_length) + ' m max length')

    # remove empty geometries
    flows = flows.loc[~flows.is_empty,:]

    l_lakeid = [] # ornl
    #for i,lineString in tqdm(enumerate(flows.geometry),total=len(flows.geometry)): # ornl
    for i,row in flows.iterrows(): # ornl
        lineString = row['geometry'] # ornl
        lakeid = row['LakeID'] # ornl
        # Reverse geometry order (necessary for BurnLines)
        lineString = LineString(lineString.coords[::-1])

        # skip lines of zero length
        if lineString.length == 0:
            continue

        # existing reaches of less than max_length
        if lineString.length < max_length:
            split_flows = split_flows + [lineString]
            l_lakeid += [lakeid] # ornl
            line_points = [point for point in zip(*lineString.coords.xy)]

            # Calculate channel slope
            start_point = line_points[0]; end_point = line_points[-1]
            start_elev,end_elev = [i[0] for i in rasterio.sample.sample_gen(dem,[start_point,end_point])]
            slope = float(abs(start_elev - end_elev) / lineString.length)
            if slope < slope_min:
                slope = slope_min
            slopes = slopes + [slope]
            continue

        splitLength = lineString.length / np.ceil(lineString.length / max_length)

        cumulative_line = []
        line_points = []
        last_point = []

        last_point_in_entire_lineString = list(zip(*lineString.coords.xy))[-1]

        for point in zip(*lineString.coords.xy):

            cumulative_line = cumulative_line + [point]
            line_points = line_points + [point]
            numberOfPoints_in_cumulative_line = len(cumulative_line)

            if last_point:
                cumulative_line = [last_point] + cumulative_line
                numberOfPoints_in_cumulative_line = len(cumulative_line)
            elif numberOfPoints_in_cumulative_line == 1:
                continue

            cumulative_length = LineString(cumulative_line).length

            if cumulative_length >= splitLength:

                splitLineString = LineString(cumulative_line)
                split_flows = split_flows + [splitLineString]
                l_lakeid += [lakeid] # ornl

                # Calculate channel slope
                start_point = cumulative_line[0]; end_point = cumulative_line[-1]
                start_elev,end_elev = [i[0] for i in rasterio.sample.sample_gen(dem,[start_point,end_point])]
                slope = float(abs(start_elev - end_elev) / splitLineString.length)
                if slope < slope_min:
                    slope = slope_min
                slopes = slopes + [slope]

                last_point = end_point

                if (last_point == last_point_in_entire_lineString):
                    continue

                cumulative_line = []
                line_points = []

        splitLineString = LineString(cumulative_line)
        split_flows = split_flows + [splitLineString]
        l_lakeid += [lakeid] # ornl

        # Calculate channel slope
        start_point = cumulative_line[0]; end_point = cumulative_line[-1]
        start_elev,end_elev = [i[0] for i in rasterio.sample.sample_gen(dem,[start_point,end_point])]
        slope = float(abs(start_elev - end_elev) / splitLineString.length)
        if slope < slope_min:
            slope = slope_min
        slopes = slopes + [slope]

    #split_flows_gdf = gpd.GeoDataFrame({'S0' : slopes ,'geometry':split_flows}, crs=flows.crs, geometry='geometry') # ornl
    split_flows_gdf = gpd.GeoDataFrame({'S0' : slopes ,'geometry':split_flows, 'LakeID': l_lakeid}, crs=flows.crs, geometry='geometry') # ornl
    split_flows_gdf['LengthKm'] = split_flows_gdf.geometry.length * toMetersConversion
    #if lakes is not None: # ornl
    #    split_flows_gdf = gpd.sjoin(split_flows_gdf, lakes_buffer, how='left', op='within') #options: intersects, within, contains, crosses # ornl
    #    split_flows_gdf = split_flows_gdf.rename(columns={"index_right": "LakeID"}).fillna(-999) # ornl
    #else: # ornl
    #    split_flows_gdf['LakeID'] = -999 # ornl

    # need to figure out why so many duplicate stream segments for 04010101 FR
    split_flows_gdf = split_flows_gdf.drop_duplicates()

    # Create Ids and Network Traversal Columns
    addattributes = build_stream_traversal.build_stream_traversal_columns()
    tResults=None
    tResults = addattributes.execute(split_flows_gdf, wbd8, hydro_id)
    if tResults[0] == 'OK':
        split_flows_gdf = tResults[1]
    else:
        print ('Error: Could not add network attributes to stream segments')

    # remove single node segments
    split_flows_gdf = split_flows_gdf.query("From_Node != To_Node")

    # Get all vertices
    split_points = OrderedDict()
    for index, segment in split_flows_gdf.iterrows():
        lineString = segment.geometry

        for point in zip(*lineString.coords.xy):
            if point in split_points:
                if segment.NextDownID == split_points[point]:
                    pass
                else:
                    split_points[point] = segment[hydro_id]
            else:
                split_points[point] = segment[hydro_id]

    hydroIDs_points = [hidp for hidp in split_points.values()]
    split_points = [Point(*point) for point in split_points]

    split_points_gdf = gpd.GeoDataFrame({'id': hydroIDs_points , 'geometry':split_points}, crs=flows.crs, geometry='geometry')

    print('Writing outputs ...')

    if isfile(split_flows_filename):
        remove(split_flows_filename)
    split_flows_gdf.to_file(split_flows_filename,driver=getDriver(split_flows_filename),index=False)

    if isfile(split_points_filename):
        remove(split_points_filename)
    split_points_gdf.to_file(split_points_filename,driver=getDriver(split_points_filename),index=False)


if __name__ == '__main__':
    max_length             = float(environ['max_split_distance_meters'])
    slope_min              = float(environ['slope_min'])
    lakes_buffer_input     = float(environ['lakes_buffer_dist_meters'])

    # Parse arguments.
    parser = argparse.ArgumentParser(description='splitflows.py')
    parser.add_argument('-f', '--flows-filename', help='flows-filename',required=True)
    parser.add_argument('-d', '--dem-filename', help='dem-filename',required=True)
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename',required=True)
    parser.add_argument('-p', '--split-points-filename', help='split-points-filename',required=True)
    parser.add_argument('-w', '--wbd8-clp-filename', help='wbd8-clp-filename',required=True)
    parser.add_argument('-l', '--lakes-filename', help='lakes-filename',required=True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    split_flows(max_length, slope_min, lakes_buffer_input, **args)
