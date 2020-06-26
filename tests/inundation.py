#!/usr/bin/env python3

import numpy as np
import pandas as pd
from tqdm import tqdm
from numba import njit, typeof, typed, types
#import concurrent.features 
import rasterio
import fiona
import shapely
from shapely.geometry import shape
from rasterio.mask import mask
from rasterio.io import DatasetReader,DatasetWriter
from rasterio.features import shapes,geometry_window,dataset_features
from rasterio.windows import transform,Window
from collections import OrderedDict
import argparse
import json


def inundate(rem,catchments,forecast,rating_curve,cross_walk,aoi=None,
             inundation_raster=None,inundation_polygon=None,depths=None,
             out_raster_profile=None,out_vector_profile=None):

    # make a catchment,stages numba dictionary
    catchmentStagesDict = __make_catchment_stages_dictionary(forecast,rating_curve,cross_walk)
    
    # input rem
    if isinstance(rem,str): 
        rem = rasterio.open(rem)
    elif isinstance(rem,DatasetReader):
        pass
    else:
        raise TypeError("Pass rasterio dataset or filepath for rem")

    # input catchments
    if isinstance(catchments,str):
        catchments = rasterio.open(catchments)
    elif isinstance(catchments,DatasetReader):
        pass
    else:
        raise TypeError("Pass rasterio dataset or filepath for catchments")

    # save desired profiles for outputs
    depths_profile = rem.profile
    inundation_profile = catchments.profile

    # update output profiles
    if isinstance(out_raster_profile,dict):
        depths_profile.update(**out_raster_profile)
        inundation_profile.update(**out_raster_profile)
    elif out_raster_profile is None:
        depths_profile.update(driver= 'GTiff', blockxsize=256, blockysize=256, tiled=True, compress='lzw')
        inundation_profile.update(driver= 'GTiff',blockxsize=256, blockysize=256, tiled=True, compress='lzw')
    else:
        raise TypeError("Pass dictionary for output raster profiles")

    # open output depths
    if isinstance(depths,str): 
        depths = rasterio.open(depths, "w", **depths_profile)
    elif isinstance(depths,DatasetWriter):
        pass
    elif depths is None:
        pass
    else:
        raise TypeError("Pass rasterio dataset, filepath for output depths, or None.")
    
    # open output inundation
    if isinstance(inundation_raster,str): 
        inundation_raster = rasterio.open(inundation_raster,"w",**inundation_profile)
    elif isinstance(inundation_raster,DatasetWriter):
        pass
    elif inundation_raster is None:
        pass
    else:
        raise TypeError("Pass rasterio dataset, filepath for output inundation raster, or None.")

    """
    # find get polygon of aoi
    with fiona.open('','r') as hucs:
        for huc in hucs:
            aoi = shape(huc['geometry'])
    """

    # make windows
    if aoi is None:
        rem_window = Window(col_off=0,row_off=0,width=rem.width,height=rem.height)
        catchments_window = Window(col_off=0,row_off=0,width=catchments.width,height=catchments.height)
    elif isinstance(aoi,shape):
        rem_window = geometry_window(rem,aoi)
        catchments_window = geometry_window(catchments,aoi)
    else:
        raise TypeError("Pass rasterio shape geometry object or None")
    
    # load arrays
    rem_array = rem.read(1,window=rem_window)
    catchments_array = catchments.read(1,window=catchments_window)
    
    # save desired array shape
    desired_shape = rem.shape

    # flatten
    rem_array = rem_array.ravel()
    catchments_array = catchments_array.ravel()

    # create flat outputs
    depths_array = rem_array.copy()
    inundation_array = catchments_array.copy()

    # reset output values
    depths_array[depths_array != depths_profile['nodata']] = 0
    inundation_array[inundation_array != inundation_profile['nodata']] = -1

    # make output arrays
    inundation_array,depths_array = __go_fast_inundation(rem_array,catchments_array,catchmentStagesDict,inundation_array,depths_array)

    # reshape output arrays
    inundation_array = inundation_array.reshape(desired_shape)
    depths_array = depths_array.reshape(desired_shape)
    
    # write out inundation and depth rasters
    if isinstance(inundation_raster,DatasetWriter):
        inundation_raster.write(inundation_array,indexes=1,window=catchments_window)
    if isinstance(depths,DatasetWriter):
        depths.write(depths_array,indexes=1,window=rem_window)

    # polygonize inundation
    if isinstance(inundation_polygon,str):
        
        # set output vector profile
        if out_vector_profile is None:
            out_vector_profile = {'crs' : rem.crs.wkt , 'driver' : 'GPKG'}

        # schema for polygons
        out_vector_profile['schema'] = {
                                        'geometry' : 'Polygon',
                                        'properties' : OrderedDict([('HydroID' , 'int')])
                                       }

        # create file
        inundation_polygon = fiona.open(inundation_polygon,'w',**out_vector_profile)

        # make generator for inundation polygons
        inundation_polygon_generator = shapes(inundation_array,mask=inundation_array>0,connectivity=8,transform=rem.transform)
        
        # generate records
        records = []
        for i,(g,h) in enumerate(inundation_polygon_generator):
            record = dict()
            record['geometry'] = g
            record['properties'] = {'HydroID' : int(h)}
            records += [record]

        # write out
        inundation_polygon.writerecords(records)

        # close file
        inundation_polygon.close()

    #executor.done()
    # close datasets
    rem.close()
    catchments.close()
    if isinstance(depths,DatasetWriter): depths.close()
    if isinstance(inundation_raster,DatasetWriter): inundation_raster.close()


@njit
def __go_fast_inundation(rem,catchments,catchmentStagesDict,inundation,depths):

    for i,(r,cm) in enumerate(zip(rem,catchments)):
        if cm in catchmentStagesDict:

            depth = catchmentStagesDict[cm] - r
            depths[i] = max(depth,0) # set negative depths to 0

            if depths[i] > 0 : # set positive depths to value of catchment in inundation
                inundation[i] = cm

    return(inundation,depths)


def __make_catchment_stages_dictionary(forecast_fileName,src_fileName,cross_walk_table_fileName):
    """ test """

    #print("Making catchment to stages numba dictionary")

    forecast = pd.read_csv(forecast_fileName, dtype={'feature_id' : int , 'discharge' : float})

    with open(src_fileName,'r') as f:
        src = json.load(f)

    cross_walk_table = pd.read_csv(cross_walk_table_fileName, dtype={'feature_id' : int , 'HydroID' : int})

    catchmentStagesDict = typed.Dict.empty(types.int32,types.float64)

    number_of_forecast_points = len(forecast)

    #for _,rows in tqdm(forecast.iterrows(),total=number_of_forecast_points):
    for _,rows in forecast.iterrows():
        discharge = rows['discharge']
        fid = int(rows['feature_id'])

        # discharge = rows[1]
        # fid = rows[0]
        matching_hydroIDs = cross_walk_table['HydroID'][cross_walk_table['feature_id'] == fid]

        for hid in matching_hydroIDs:

            stage_list = np.array(src[str(hid)]['stage_list'])
            q_list = np.array(src[str(hid)]['q_list'])
            indices_that_are_lower = list(q_list < discharge)

            # print(indices_that_are_lower)
            is_index_last = indices_that_are_lower[-1]

            if is_index_last:
                h = stage_list[-1]

                hid = types.int32(hid) ; h = types.float32(h)
                catchmentStagesDict[hid] = h

                continue

            index_of_lower = np.where(indices_that_are_lower)[0][-1]
            index_of_upper = index_of_lower + 1

            Q_lower = q_list[index_of_lower]
            h_lower = stage_list[index_of_lower]

            Q_upper = q_list[index_of_upper]
            h_upper = stage_list[index_of_upper]

            # linear interpolation
            h = h_lower + (discharge - Q_lower) * ((h_upper - h_lower) / (Q_upper - Q_lower))

            hid = types.int32(hid) ; h = types.float32(h)
            catchmentStagesDict[hid] = h

    return(catchmentStagesDict)


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM')
    parser.add_argument('-r','--rem', help='REM raster at job level or mosaic vrt', required=True)
    parser.add_argument('-c','--catchments',help='Catchments raster at job level or mosaic vrt',required=True)
    parser.add_argument('-f','--forecast',help='Forecast discharges in CMS as CSV file',required=True)
    parser.add_argument('-s','--rating-curve',help='SRC JSON file',required=True)
    parser.add_argument('-w','--crosswalk',help='Cross-walk table csv',required=True)
    parser.add_argument('-i','--inundation-raster',help='Inundation Raster output',required=False,default=None)
    parser.add_argument('-p','--inundation-polygon',help='Inundation polygon output',required=False,default=None)
    parser.add_argument('-d','--depths',help='Depths raster output',required=False,default=None)
    
    # extract to dictionary
    args = vars(parser.parse_args())
    
    # call function
    inundate( 
              rem = args['rem'], catchments = args['catchments'], forecast = args['forecast'],
              rating_curve = args['rating_curve'], cross_walk = args['crosswalk'],
              inundation_raster = args['inundation_raster'],
              inundation_polygon = args['inundation_polygon'], depths = args['depths']
            )

