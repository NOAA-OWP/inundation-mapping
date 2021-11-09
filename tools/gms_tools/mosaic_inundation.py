#!/usr/bin/env python
# coding: utf-8

from glob import glob
from gms_tools.overlapping_inundation import OverlapWindowMerge
import argparse
import os
import pandas as pd
from tqdm import tqdm
from tools_shared_variables import elev_raster_ndv

def Mosaic_inundation(
                      map_file,mosaic_attribute='inundation_rasters',mosaic_output=None,
                      mask=None,unit_attribute_name='huc8',
                      nodata=elev_raster_ndv,workers=4,
                      remove_inputs=False,
                      subset=None,verbose=True
                      ):
    
    # check input
    if mosaic_attribute not in ('inundation_rasters','depths_rasters'):
        raise ValueError('Pass inundation or depths for mosaic_attribute argument') 

    # load file
    if isinstance(map_file,pd.DataFrame):
        inundation_maps_df = map_file
        del map_file
    elif isinstance(map_file,str):
        inundation_maps_df = pd.read_csv(map_file,
                                         dtype={unit_attribute_name:str,'branchID':str}
                                        )
    else:
        raise TypeError('Pass Pandas Dataframe or file path string to csv for map_file argument')

    # remove NaNs
    inundation_maps_df.dropna(axis=0,how='all',inplace=True)

    # subset
    if subset is not None:
        subset_mask = inundation_maps_df.loc[:,unit_attribute_name].isin(subset)
        inundation_maps_df = inundation_maps_df.loc[subset_mask,:]
    
    # unique aggregation units
    aggregation_units = inundation_maps_df.loc[:,unit_attribute_name].unique()

    inundation_maps_df.set_index(unit_attribute_name,drop=True,inplace=True)

    # decide upon wheter to display 
    if verbose & len(aggregation_units) == 1:
        tqdm_disable = False
    elif verbose:
        tqdm_disable = False
    else:
        tqdm_disable = True

    for ag in tqdm(aggregation_units,disable=tqdm_disable,desc='Compositing MS and FR maps'):

        try:
            inundation_maps_list = inundation_maps_df.loc[ag,mosaic_attribute].tolist()
        except AttributeError:
            inundation_maps_list = [ inundation_maps_df.loc[ag,mosaic_attribute] ]

        ag_mosaic_output = __append_id_to_file_name(mosaic_output,ag)
        #try:
        mosaic_by_unit(inundation_maps_list,ag_mosaic_output,nodata,
                       workers=1,remove_inputs=remove_inputs,mask=mask,verbose=verbose)
        #except Exception as exc:
        #    print(ag,exc)
    

    # inundation maps
    inundation_maps_df.reset_index(drop=True)



def mosaic_by_unit(inundation_maps_list,mosaic_output,nodata=elev_raster_ndv,
                   workers=1,remove_inputs=False,mask=None,verbose=False):


    # overlap object instance
    overlap = OverlapWindowMerge( inundation_maps_list, (30, 30) )

    # mosaic
    #if verbose:
    #    print("Mosaicing ...")

    if mosaic_output is not None:
        if workers > 1:
            threaded = True
        else:
            threaded= False
        
        overlap.merge_rasters(mosaic_output, threaded=threaded, workers=workers,nodata=nodata)

        if mask:
            #if verbose:
            #    print("Masking ...")
            overlap.mask_mosaic(mosaic_output,mask,outfile=mosaic_output)
    
    if remove_inputs:
        #if verbose:
        #    print("Removing inputs ...")

        for inun_map in inundation_maps_list:
            if inun_map is not None:
                if os.path.isfile(inun_map):
                    os.remove(inun_map)


def __append_id_to_file_name(file_name,identifier):


    if file_name is not None:

        root,extension = os.path.splitext(file_name)

        if isinstance(identifier,list):
            for i in identifier:
                out_file_name = root + "_{}".format(i)
            out_file_name += extension
        else:
            out_file_name = root + "_{}".format(identifier) + extension

    else:
        out_file_name = None

    return(out_file_name)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mosaic GMS Inundation Rasters')
    parser.add_argument('-i','--map-file', help='List of file paths to inundation/depth maps to mosaic', required=True)
    parser.add_argument('-a','--mask', help='File path to vector polygon mask to clip mosaic too', required=False,default=None)
    parser.add_argument('-s','--subset', help='Subset units', required=False,default=None,type=str,nargs='+')
    parser.add_argument('-n','--nodata', help='Inundation Maps', required=False,default=elev_raster_ndv)
    parser.add_argument('-w','--workers', help='Number of Workers', required=False,default=4,type=int)
    parser.add_argument('-t','--mosaic-attribute', help='Mosaiced inundation Maps', required=False,default=None)
    parser.add_argument('-m','--mosaic-output', help='Mosaiced inundation Maps', required=False,default=None)
    parser.add_argument('-r','--remove-inputs', help='Remove original input inundation Maps', required=False,default=False,action='store_true')
    parser.add_argument('-v','--verbose', help='Remove original input inundation Maps', required=False,default=False,action='store_true')

    args = vars(parser.parse_args())
    
    Mosaic_inundation(**args)