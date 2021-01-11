#!/usr/bin/env python3

import sys
import geopandas as gpd
import argparse
from os.path import splitext
from shapely.geometry import MultiPolygon,Polygon,Point

def subset_vector_layers(hucCode,nwm_streams_filename,nhd_streams_filename,nwm_lakes_filename,nld_lines_filename,nwm_catchments_filename,nhd_headwaters_filename,landsea_filename,wbd_filename,wbd_buffer_filename,subset_nhd_streams_filename,subset_nld_lines_filename,subset_nwm_lakes_filename,subset_nwm_catchments_filename,subset_nhd_headwaters_filename,subset_nwm_streams_filename,subset_landsea_filename,dissolveLinks=False,extent='FR'):

    hucUnitLength = len(str(hucCode))

    # Get wbd buffer
    wbd = gpd.read_file(wbd_filename)
    wbd_buffer = gpd.read_file(wbd_buffer_filename)
    projection = wbd_buffer.crs

    # Clip ocean water polygon for future masking ocean areas (where applicable)
    landsea = gpd.read_file(landsea_filename, mask = wbd_buffer)
    if not landsea.empty:
        landsea.to_file(subset_landsea_filename,driver=getDriver(subset_landsea_filename),index=False)
    del landsea

    # find intersecting lakes and writeout
    print("Subsetting NWM Lakes for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_lakes = gpd.read_file(nwm_lakes_filename, mask = wbd_buffer)

    if not nwm_lakes.empty:
        # perform fill process to remove holes/islands in the NWM lake polygons
        nwm_lakes = nwm_lakes.explode()
        nwm_lakes_fill_holes=MultiPolygon(Polygon(p.exterior) for p in nwm_lakes['geometry']) # remove donut hole geometries
        # loop through the filled polygons and insert the new geometry
        for i in range(len(nwm_lakes_fill_holes)):
            nwm_lakes.loc[i,'geometry'] = nwm_lakes_fill_holes[i]
        nwm_lakes.to_file(subset_nwm_lakes_filename,driver=getDriver(subset_nwm_lakes_filename),index=False)
    del nwm_lakes

    # find intersecting levee lines
    print("Subsetting NLD levee lines for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nld_lines = gpd.read_file(nld_lines_filename, mask = wbd_buffer)
    if not nld_lines.empty:
        nld_lines.to_file(subset_nld_lines_filename,driver=getDriver(subset_nld_lines_filename),index=False)
    del nld_lines

    # find intersecting nwm_catchments
    print("Subsetting NWM Catchments for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_catchments = gpd.read_file(nwm_catchments_filename, mask = wbd_buffer)
    nwm_catchments.to_file(subset_nwm_catchments_filename,driver=getDriver(subset_nwm_catchments_filename),index=False)
    del nwm_catchments

    # subset nhd headwaters
    print("Subsetting NHD Headwater Points for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nhd_headwaters = gpd.read_file(nhd_headwaters_filename, mask = wbd_buffer)

    # subset nhd streams
    print("Querying NHD Streams for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nhd_streams = gpd.read_file(nhd_streams_filename, mask = wbd_buffer)

    ## identify local headwater stream segments
    nhd_streams_subset = gpd.read_file(nhd_streams_filename, mask = wbd)
    nhd_streams_subset = nhd_streams_subset.loc[~nhd_streams_subset.FromNode.isin(list(set(nhd_streams_subset.ToNode) & set(nhd_streams_subset.FromNode)))]
    nhd_streams_subset = nhd_streams_subset[~nhd_streams_subset['is_headwater']]

    if not nhd_streams_subset.empty:
        nhd_streams_subset = nhd_streams_subset.reset_index(drop=True)
        start_coords = []
        NHDPlusIDs = []
        for index, linestring in enumerate(nhd_streams_subset.geometry):
            start_coords = start_coords + [linestring.coords[-1]]
            NHDPlusIDs = NHDPlusIDs + [nhd_streams_subset.iloc[index].NHDPlusID]

        start_geoms = [Point(point) for point in start_coords]
        local_headwaters = gpd.GeoDataFrame({'NHDPlusID': NHDPlusIDs,'geometry': start_geoms}, crs=projection, geometry='geometry')
        nhd_headwaters = nhd_headwaters.append(local_headwaters)

        # nhd_streams = nhd_streams.loc[~nhd_streams.NHDPlusID.isin(NHDPlusIDs)]

    nhd_streams.to_file(subset_nhd_streams_filename,driver=getDriver(subset_nhd_streams_filename),index=False)

    if len(nhd_headwaters) > 0:
        nhd_headwaters.to_file(subset_nhd_headwaters_filename,driver=getDriver(subset_nhd_headwaters_filename),index=False)
        del nhd_headwaters, nhd_streams
    else:
        print ("No headwater point(s) within HUC " + str(hucCode) +  " boundaries.")
        sys.exit(0)

    # subset nwm streams
    print("Subsetting NWM Streams and deriving headwaters for HUC{} {}".format(hucUnitLength,hucCode),flush=True)
    nwm_streams = gpd.read_file(nwm_streams_filename, mask = wbd_buffer)
    nwm_streams.to_file(subset_nwm_streams_filename,driver=getDriver(subset_nwm_streams_filename),index=False)
    del nwm_streams

def getDriver(filename):

    driverDictionary = {'.gpkg' : 'GPKG','.geojson' : 'GeoJSON','.shp' : 'ESRI Shapefile'}
    driver = driverDictionary[splitext(filename)[1]]

    return(driver)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Subset vector layers')
    parser.add_argument('-d','--hucCode', help='HUC boundary ID', required=True,type=str)
    parser.add_argument('-w','--nwm-streams', help='NWM flowlines', required=True)
    parser.add_argument('-s','--nhd-streams',help='NHDPlus HR burnline',required=True)
    parser.add_argument('-l','--nwm-lakes', help='NWM Lakes', required=True)
    parser.add_argument('-r','--nld-lines', help='Levee vectors to use within project path', required=True)
    parser.add_argument('-g','--wbd',help='HUC boundary',required=True)
    parser.add_argument('-f','--wbd-buffer',help='Buffered HUC boundary',required=True)
    parser.add_argument('-m','--nwm-catchments', help='NWM catchments', required=True)
    parser.add_argument('-y','--nhd-headwaters',help='NHD headwaters',required=True)
    parser.add_argument('-v','--landsea',help='LandSea - land boundary',required=True)
    parser.add_argument('-c','--subset-nhd-streams',help='NHD streams subset',required=True)
    parser.add_argument('-z','--subset-nld-lines',help='Subset of NLD levee vectors for HUC',required=True)
    parser.add_argument('-a','--subset-lakes',help='NWM lake subset',required=True)
    parser.add_argument('-n','--subset-catchments',help='NWM catchments subset',required=True)
    parser.add_argument('-e','--subset-nhd-headwaters',help='NHD headwaters subset',required=True,default=None)
    parser.add_argument('-b','--subset-nwm-streams',help='NWM streams subset',required=True)
    parser.add_argument('-x','--subset-landsea',help='LandSea subset',required=True)
    parser.add_argument('-o','--dissolve-links',help='remove multi-line strings',action="store_true",default=False)
    parser.add_argument('-p','--extent',help='MS or FR extent',required=True)

    args = vars(parser.parse_args())

    hucCode = args['hucCode']
    nwm_streams_filename = args['nwm_streams']
    nhd_streams_filename = args['nhd_streams']
    nwm_lakes_filename = args['nwm_lakes']
    nld_lines_filename = args['nld_lines']
    wbd_filename = args['wbd']
    wbd_buffer_filename = args['wbd_buffer']
    nwm_catchments_filename = args['nwm_catchments']
    nhd_headwaters_filename = args['nhd_headwaters']
    landsea_filename = args['landsea']
    subset_nhd_streams_filename = args['subset_nhd_streams']
    subset_nld_lines_filename = args['subset_nld_lines']
    subset_nwm_lakes_filename = args['subset_lakes']
    subset_nwm_catchments_filename = args['subset_catchments']
    subset_nhd_headwaters_filename = args['subset_nhd_headwaters']
    subset_nwm_streams_filename = args['subset_nwm_streams']
    subset_landsea_filename = args['subset_landsea']
    dissolveLinks = args['dissolve_links']
    extent = args['extent']

    subset_vector_layers(hucCode,nwm_streams_filename,nhd_streams_filename,nwm_lakes_filename,nld_lines_filename,nwm_catchments_filename,nhd_headwaters_filename,landsea_filename,wbd_filename,wbd_buffer_filename,subset_nhd_streams_filename,subset_nld_lines_filename,subset_nwm_lakes_filename,subset_nwm_catchments_filename,subset_nhd_headwaters_filename,subset_nwm_streams_filename,subset_landsea_filename,dissolveLinks,extent)
