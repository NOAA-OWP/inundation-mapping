# -*- coding: utf-8 -*-
"""
Created on Fri Jul 24 13:50:59 2020

@author: trevor.grout
"""
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio import features
import numpy as np
import geopandas as gpd
import os
import argparse

def fimx_to_fim3(catchments_path, raster_value_field, hand_raster_path, template_raster, out_hand_path = None, out_catchment_path = None):
    '''
    This function will produce a hand and catchment grid from fim1/fim2 for use in the fim3 inundation.py script. To accomplish this it:
        1) reprojects the hand raster to the template raster crs.
        2) It reprojects the catchment polygons to the template raster crs and then converts the polygons to a raster. The reprojected HAND raster properties are applied (extent, resolution)
        3) Performs the intersection of the two rasters (HAND/catchments) and applies NODATA if either dataset has NODATA
        4) Writes out the preprocessed HAND and Catchments raster if a path is specified.

    Parameters
    ----------
    catchments_path : STRING
        The path to the catchments vector data (assumes it is in a database).
    raster_value_field : STRING
        Attribute field in catchments layer whos values will be used for raster creation.
    hand_raster_path : STRING
        The path to the HAND raster dataset (ARC GRID is acceptable).
    template_raster : STRING
        Path to the template raster so that a CRS can be applied to output.
    out_hand_path : STRING, optional
        Path to the output HAND grid (Gtif format). The default is None.
    out_catchment_path : STRING, optional
        Path to the output catchment grid (Gtif format). The default is None.

    Returns
    -------
    hand_masked : Numpy Array
        Preprocessed HAND raster array.
    hand_profile : LIST
        Preprocessed HAND profile.
    catchment_masked : Numpy Array
        Preprocessed Catchment raster array.
    catchment_profile : LIST
        Preprocessed catchment raster profile.

    '''
    
    
    #Read in template raster as band object.
    reference = rasterio.open(template_raster)
    
    #Step 1: Convert HAND grid
    #Read in the hand raster     
    hand = rasterio.open(hand_raster_path)
    hand_arr = hand.read(1)
    #Determine the new transform and dimensions of reprojected raster (CRS = reference raster).
    new_transform, new_width, new_height = calculate_default_transform(hand.crs, reference.crs, hand.width, hand.height, *hand.bounds)
    #Define an empty array that is same dimensions as output by the "calculate_default_transform" command. 
    hand_proj = np.empty((new_height,new_width), dtype=np.float)    
    #Reproject to target dataset (resample method is bilinear due to elevation type data).
    hand_nodata_value = -2147483648
    reproject(hand_arr, 
              destination = hand_proj,
              src_transform = hand.transform, 
              src_crs = hand.crs,
              src_nodata = hand.nodata,
              dst_transform = new_transform, 
              dst_crs = reference.crs,
              dst_nodata = hand_nodata_value,
              dst_resolution = hand.res,
              resampling = Resampling.bilinear)
    #Update profile data type and no data value.
    hand_profile = reference.profile
    hand_profile.update(dtype = rasterio.float32)
    hand_profile.update(nodata = hand_nodata_value)
    hand_profile.update(width = new_width)
    hand_profile.update(height = new_height)
    hand_profile.update(transform = new_transform)
    
    #Step 2: Catchments to Polygons (same extent as the HAND raster)
    #Read in the catchment layer to geopandas dataframe and convert to same CRS as reference raster.
    gdbpath, layername = os.path.split(catchments_path)
    gdb_layer=gpd.read_file(gdbpath, driver='FileGDB', layer=layername)
    proj_gdb_layer = gdb_layer.to_crs(reference.crs)
    #Prepare vector data to be written to raster.
    shapes = list(zip(proj_gdb_layer['geometry'],proj_gdb_layer[raster_value_field].astype('int32')))   
    #Write vector data to raster image. Fill raster with zeros for areas that do not have data. We will set nodata to be zero later.
    catchment_proj = features.rasterize(((geometry, value) for geometry, value in shapes), fill = 0, out_shape=hand_proj.shape, transform=hand_profile['transform'], dtype = 'int32' )    
    #Save raster image to in-memory dataset. Reset dtype and nodata values.
    catchment_profile = hand_profile.copy()
    catchment_profile.update(dtype = 'int32')
    catchment_profile.update(nodata=0)
  
    #Step 3: Union of NODATA locations applied to both HAND and Catchment grids. 
    catchment_masked = np.where(np.logical_or(hand_proj == hand_profile['nodata'], catchment_proj == catchment_profile['nodata']), catchment_profile['nodata'],catchment_proj)
        #Assign NODATA to hand where both catchment and hand have NODATA else assign hand values.
    hand_masked = np.where(np.logical_or(hand_proj == hand_profile['nodata'], catchment_proj == catchment_profile['nodata']), hand_profile['nodata'],hand_proj)

    #Step 4: Write out hand and catchment rasters to file if path is specified
    if out_hand_path is not None:
        os.makedirs(os.path.split(out_hand_path)[0], exist_ok = True)        
        with rasterio.Env():
            with rasterio.open(out_hand_path, 'w', **hand_profile) as hnd_dst:
                hnd_dst.write(hand_masked.astype('float32'),1)
    if out_catchment_path is not None:
        os.makedirs(os.path.split(out_catchment_path)[0], exist_ok = True)        
        with rasterio.Env():
            with rasterio.open(out_catchment_path, 'w', **catchment_profile) as cat_dst:
                cat_dst.write(catchment_masked.astype('int32'),1)   
    
    return hand_masked, hand_profile, catchment_masked, catchment_profile

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Preprocess FIM 1 and FIM 2 HAND and Catchment grids to be compatible with FIM 3.')
    parser.add_argument('-c','--catchments-path', help = 'Path to catchments vector file', required = True)
    parser.add_argument('-f', '--raster-value-field', help = 'Attribute ID field from which raster values will be assigned. Typically this will be "HydroID" for FIM2 and "feature_ID" for fim 1.', required = True)
    parser.add_argument('-ha', '--hand-raster-path', help = 'Path to HAND raster (can be in ESRI GRID format)', required = True)
    parser.add_argument('-t', '--template-raster', help = 'Path to a template raster. Properties (CRS, resolution) of the template raster will be used to preprocess HAND and Catchments grids', required = True)
    parser.add_argument('-oh', '--out-hand-path', help = 'Path to the output HAND raster. Raster must be named "rem_clipped_zeroed_masked.tif', required = True)
    parser.add_argument('-oc', '--out-catchment-path', help = 'Path to the output Catchment raster. Raster must be named "gw_catchments_reaches_clipped_addedAttributes.tif"', required = True)
    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    #Run fimx to fim3 function.
    fimx_to_fim3(**args)

