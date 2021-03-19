#!/usr/bin/env python3


from stream_branches import StreamNetwork
from stream_branches import StreamBranchPolygons
import argparse
from tqdm import tqdm


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Clips rasters to branch polygons')
    parser.add_argument('-b','--branches', help='Branch polygons file name', required=True,default=None)
    parser.add_argument('-i','--branch-id', help='Branch ID attribute', required=True,default=None)
    parser.add_argument('-r','--rasters', help='Raster file name to clip', required=True,default=None,nargs="+")
    parser.add_argument('-c','--clipped-rasters', help='Branch polygons out file name', required=False,default=None,nargs="+")
    parser.add_argument('-v','--verbose', help='Verbose printing', required=False,default=None,action='store_true')

    # extract to dictionary
    args = vars(parser.parse_args())

    stream_polygons_file, branch_id_attribute, rasters, clipped_rasters, verbose = args["branches"], args["branch_id"],args["rasters"] , args["clipped_rasters"], args["verbose"]
    
    # load file
    stream_polys = StreamBranchPolygons.from_file( filename=stream_polygons_file, 
                                                   branch_id_attribute=branch_id_attribute,
                                                   values_excluded=None,attribute_excluded=None, verbose = verbose)
    
    for raster, clipped_raster in tqdm(zip(rasters,clipped_rasters),disable=(not verbose),total=len(rasters)):
        if verbose:
            print("Clipping \'{}\' to branch polygons ...".format(raster.split('/')[-1].split('.')[0]))
   
        stream_polys.clip(raster,clipped_raster)
    

