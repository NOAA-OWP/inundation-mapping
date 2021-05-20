#!/usr/bin/env python
# coding: utf-8

from glob import glob
from gms.overlapping_inundation import OverlapWindowMerge
import argparse
import os

def Mosaic_inundation(inundation_maps,mosaic=None,mask=None,nodata=-2147483647,workers=4,remove_inputs=False,verbose=True):
    
    #inundation_maps = glob( join(inundation_files,'*.tif'))

    inundation_maps = list(inundation_maps)
    
    overlap = OverlapWindowMerge( inundation_maps, (30, 30) )

    if verbose:
        print("Mosaicing ...")

    if mosaic is not None:
        overlap.merge_rasters(mosaic, threaded=True, workers=4,nodata=nodata)

    if mask:
        if verbose:
            print("Masking ...")

        overlap.mask_mosaic(mosaic,mask,outfile=mosaic)

    if remove_inputs:
        if verbose:
            print("Removing inputs ...")

        for inun_map in inundation_maps:
            if inun_map is not None:
                if os.path.isfile(inun_map):
                    os.remove(inun_map)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mosaic GMS Inundation Rasters')
    parser.add_argument('-i','--inundation-maps', help='List of file paths to inundation/depth maps to mosaic', required=True,nargs='+')
    parser.add_argument('-a','--mask', help='File path to vector polygon mask to clip mosaic too', required=False,default=None)
    parser.add_argument('-n','--nodata', help='Inundation Maps', required=False,default=-2147483647)
    parser.add_argument('-w','--workers', help='Number of Workers', required=False,default=4)
    parser.add_argument('-m','--mosaic', help='Mosaiced inundation Maps', required=False,default=None)
    parser.add_argument('-r','--remove-inputs', help='Remove original input inundation Maps', required=False,default=False,action='store_true')
    parser.add_argument('-v','--verbose', help='Remove original input inundation Maps', required=False,default=False,action='store_true')

    args = vars(parser.parse_args())
    
    Mosaic_gms_inundation(**args)
