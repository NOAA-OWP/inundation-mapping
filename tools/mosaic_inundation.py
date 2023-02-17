#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
import sys

from glob import glob
from overlapping_inundation import OverlapWindowMerge
from tqdm import tqdm
from utils.shared_variables import elev_raster_ndv
from utils.shared_functions import FIM_Helpers as fh

def Mosaic_inundation( map_file,
                       mosaic_attribute = 'inundation_rasters',
                       mosaic_output = None,
                       mask = None,
                       unit_attribute_name = 'huc8',
                       nodata = elev_raster_ndv,
                       workers = 1,
                       remove_inputs = False,
                       subset = None,
                       verbose = True,
                       is_mosaic_for_branches = False ):
    
    # Notes:
    #    - If is_mosaic_for_branches is true, the mosaic output name
    #      will add the HUC into the output name for overwrite resons.

    # check input
    if mosaic_attribute not in ('inundation_rasters','depths_rasters'):
        raise ValueError('Pass inundation or depths for mosaic_attribute argument') 

    # load file
    if isinstance(map_file,pd.DataFrame):
        inundation_maps_df = map_file
        del map_file
    elif isinstance(map_file,str):
        inundation_maps_df = pd.read_csv(map_file,
                                         dtype={unit_attribute_name:str,'branchID':str})
    else:
        raise TypeError('Pass Pandas Dataframe or file path string to csv for map_file argument')

    # remove NaNs
    inundation_maps_df.dropna(axis=0, how='all', inplace=True)

    # subset
    if subset is not None:
        subset_mask = inundation_maps_df.loc[:,unit_attribute_name].isin(subset)
        inundation_maps_df = inundation_maps_df.loc[subset_mask,:]
    
    # unique aggregation units
    aggregation_units = inundation_maps_df.loc[:,unit_attribute_name].unique()

    inundation_maps_df.set_index(unit_attribute_name, drop=True, inplace=True)

    # decide upon wheter to display 
    if verbose & len(aggregation_units) == 1:
        tqdm_disable = False
    elif verbose:
        tqdm_disable = False
    else:
        tqdm_disable = True

    ag_mosaic_output = ""

    for ag in tqdm(aggregation_units, disable = tqdm_disable, desc = 'Mosaicing FIMs'):

        try:
            inundation_maps_list = inundation_maps_df.loc[ag,mosaic_attribute].tolist()
        except AttributeError:
            inundation_maps_list = [ inundation_maps_df.loc[ag,mosaic_attribute] ]

        # Some processes may have already added the ag value (if it is a huc) to 
        # the file name, so don't re-add it.
        # Only add the huc into the name if branches are being processed, as 
        # sometimes the mosiac is not for gms branches but maybe mosaic of an
        # fr set with a gms composite map.

        ag_mosaic_output = mosaic_output
        if (is_mosaic_for_branches) and (ag not in mosaic_output):
            ag_mosaic_output = fh.append_id_to_file_name(mosaic_output, ag) # change it

        mosaic_by_unit(inundation_maps_list, 
                      ag_mosaic_output,
                      nodata,
                      workers = workers, 
                      remove_inputs = remove_inputs,
                      mask = mask,
                      verbose = verbose)


    # inundation maps
    inundation_maps_df.reset_index(drop=True)

    # Return file name and path of the final mosaic output file.
    # Might be empty.
    return ag_mosaic_output


# Note: This uses threading and not processes. If the number of workers is more than 
# the number of possible threads, no results will be returned. But it is usually
# pretty fast anyways. This needs to be fixed.
def mosaic_by_unit(inundation_maps_list,
                   mosaic_output,
                   nodata = elev_raster_ndv,
                   workers = 1,
                   remove_inputs = False,
                   mask = None,
                   verbose = False):

    # overlap object instance
    overlap = OverlapWindowMerge( inundation_maps_list, (30, 30) )

    if mosaic_output is not None:
        if workers > 1:
            threaded = True
        else:
            threaded= False
        
        overlap.merge_rasters(mosaic_output, threaded=threaded, workers=workers, nodata=nodata)

        if mask:
            fh.vprint("Masking ...", verbose)                
            overlap.mask_mosaic(mosaic_output, mask, outfile=mosaic_output)
    
    if remove_inputs:
        fh.vprint("Removing inputs ...", verbose)

        for inun_map in inundation_maps_list:
            if inun_map is not None:
                if os.path.isfile(inun_map):
                    os.remove(inun_map)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mosaic GMS Inundation Rasters')
    parser.add_argument('-i','--map-file', 
                        help='List of file paths to inundation/depth maps to mosaic',
                        required=True)
    parser.add_argument('-a','--mask', 
                        help='File path to vector polygon mask to clip mosaic too',
                        required=False, default=None)
    parser.add_argument('-s','--subset', help='Subset units', 
                        required=False, default=None, type=str, nargs='+')
    parser.add_argument('-n','--nodata', help='Inundation Maps',
                        required=False, default=elev_raster_ndv)
    parser.add_argument('-w','--workers', help='Number of Workers', 
                        required=False, default=4, type=int)
    parser.add_argument('-t','--mosaic-attribute', help='Mosaiced inundation Maps', 
                        required=False, default=None)
    parser.add_argument('-m','--mosaic-output', help='Mosaiced inundation Maps file name',
                        required=False, default=None)
    parser.add_argument('-r','--remove-inputs', help='Remove original input inundation Maps',
                        required=False, default=False, action='store_true')
    parser.add_argument('-v','--verbose', help='Remove original input inundation Maps', 
                        required=False, default=False, action='store_true')
    parser.add_argument('-g','--is-mosaic-for-branches', 
                        help='If the mosaic is for branchs, include this arg',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())
    
    Mosaic_inundation(**args)
