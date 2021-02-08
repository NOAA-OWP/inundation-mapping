#!/usr/bin/env python3


from stream_branches import StreamNetwork
from stream_branches import StreamBranchPolygons
import argparse


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Clips rasters to branch polygons')
    parser.add_argument('-b','--branches', help='Branch polygons file name', required=True,default=None)
    parser.add_argument('-r','--rasters', help='Raster file name to clip', required=True,default=None)
    parser.add_argument('-c','--clipped-rasters', help='Branch polygons out file name', required=False,default=None)
    parser.add_argument('-v','--verbose', help='Verbose printing', required=False,default=None,action='store_true')

    # extract to dictionary
    args = vars(parser.parse_args())

    streams_polygons_file, rasters, clipped_rasters, verbose = args["branches"], args["rasters"] , args["clipped_rasters"], args["verbose"]
    
    # load file
    stream_polys = StreamBranchPolygons.from_file( filename=stream_polygons_file ,branch_id_attribute=None,
                                              values_excluded=None,attribute_excluded=None, verbose = verbose)
    
    out_rasters = stream_polys.clip(raster,clipped_rasters)
    

