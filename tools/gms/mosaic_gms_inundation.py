#!/usr/bin/env python
# coding: utf-8

from glob import glob
from overlapping_inundation import OverlapWindowMerge
import argparse
from os.path import join

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mosaic GMS Inundation Rasters')
    parser.add_argument('-i','--inundation-dir', help='Inundation Maps', required=True)
    parser.add_argument('-m','--mosaic', help='Mosaiced inundation Maps', required=False,default=None)

    args = vars(parser.parse_args())

    inundation_maps = glob( join(args['inundation_dir'],'*.tif'))

    overlap = OverlapWindowMerge( inundation_maps, (30, 30) )

    if args['mosaic'] is not None:
        overlap.merge_rasters(args['mosaic'], threaded=True, workers=4)
