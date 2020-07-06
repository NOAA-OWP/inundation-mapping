#!/usr/bin/env python3

import numpy as np
import pandas as pd
from numba import njit, typeof, typed, types
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor,as_completed
from subprocess import run
from os.path import splitext
import rasterio
import fiona
import shapely
from shapely.geometry import shape
from fiona.crs import to_string
from rasterio.errors import WindowError
from rasterio.mask import mask
from rasterio.io import DatasetReader,DatasetWriter
from rasterio.features import shapes,geometry_window,dataset_features
from rasterio.windows import transform,Window
from collections import OrderedDict
import argparse
import json


def inundate(rem,catchments,forecast,rating_curve,cross_walk,hucs=None,
             hucs_layerName=None,num_workers=1,inundation_raster=None,inundation_polygon=None,
             depths=None,out_raster_profile=None,out_vector_profile=None,aggregate=False,current_huc=None):
    """
    Run inundation on FIM 3.0 <= outputs at job-level scale or aggregated scale
    
    Generate depths raster, inundation raster, and inundation polygon from FIM3.0 <= outputs. Can use the FIM 3.0 outputs at it's native HUC level or the aggregated products. Be sure to pass a HUCs file to process at HUC levels if passing aggregated products. 
    
    Parameters
    ----------
    rem : str or rasterio.DatasetReader
        File path to or rasterio dataset reader of Relative Elevation Model raster. Must have the same CRS as catchments raster.
    catchments : str or rasterio.DatasetReader
        File path to or rasterio dataset reader of Catchments raster. Must have the same CRS as REM raster
    TBC
    
    Returns
    -------
    error_code : int
        Zero for successful completion and non-zero for failure. (Untested)

    Notes
    -----

    Examples
    --------

    """
    # check for num_workers
    num_workers = int(num_workers)
    assert num_workers >= 1, "Number of workers should be 1 or greater"
    if (num_workers > 1) & (hucs is None):
        raise AssertionError("Pass a HUCs file to batch process inundation mapping")

    # check that aggregate is only done for hucs mode
    aggregate = bool(aggregate)
    if hucs is None:
        assert (not aggregate), "Pass HUCs file if aggregation is desired"

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
    
    # check for matching number of bands and single band only
    assert rem.count == catchments.count == 1, "REM and catchments rasters are required to be single band only"

    # check for matching raster sizes
    assert (rem.width == catchments.width) & (rem.height == catchments.height), "REM and catchments rasters required same shape"
    
    # check for matching projections
    #assert rem.crs.to_proj4() == catchments.crs.to_proj4(), "REM and Catchment rasters require same CRS definitions"

    # check for matching bounds
    assert ( (rem.transform*(0,0)) == (catchments.transform*(0,0)) ) & ( (rem.transform* (rem.width,rem.height)) == (catchments.transform*(catchments.width,catchments.height)) ), "REM and catchments rasters require same upper left and lower right extents"

    # open hucs
    if isinstance(hucs,str):
        hucs = fiona.open(hucs,'r',layer=hucs_layerName)
    elif isinstance(hucs,fiona.Collection):
        pass
    else:
        raise TypeError("Pass fiona collection or filepath for hucs")

    # check for matching projections
    #assert to_string(hucs.crs) == rem.crs.to_proj4() == catchments.crs.to_proj4(), "REM, Catchment, and HUCS CRS definitions must match"

    # make a catchment,stages numba dictionary
    catchmentStagesDict = __make_catchment_stages_dictionary(forecast,rating_curve,cross_walk)
    
    # make windows generator
    window_gen = __make_windows_generator(rem,catchments,catchmentStagesDict,inundation_raster,inundation_polygon,
                                          depths,out_raster_profile,out_vector_profile,hucs=hucs,hucSet=current_huc)

    # start up thread pool
    executor = ThreadPoolExecutor(max_workers=num_workers)
    
    # submit jobs
    results = {executor.submit(__inundate_in_huc,*wg) : wg[6] for wg in window_gen}

    inundation_rasters = [] ; depth_rasters = [] ; inundation_polys = []
    for future in as_completed(results):
        try:
            future.result()
        except Exception as exc:
            print("Exception {} for {}".format(exc,results[future]))
        else:
            print("... {} complete".format(results[future]))
            inundation_rasters += [future.result()[0]]
            depth_rasters += [future.result()[1]]
            inundation_polys += [future.result()[2]]
    
    # power down pool
    executor.shutdown(wait=True)
    
    # optional aggregation
    if (aggregate) & (hucs is not None):
        # inun grid vrt
        if inundation_raster is not None:
            _ = run('gdalbuildvrt -q -overwrite {} {}'.format(splitext(inundation_raster)[0]+'.vrt'," ".join(inundation_rasters)),shell=True)
        # depths vrt
        if depths is not None:
            _ = run('gdalbuildvrt -q -overwrite {} {}'.format(splitext(depths)[0]+'.vrt'," ".join(depth_rasters)),shell=True)

        # concat inun poly
        if inundation_polygon is not None:
            _ = run('ogrmerge.py -o {} {} -f GPKG -single -overwrite_ds'.format(inundation_polygon," ".join(inundation_polys)),shell=True)

    # close datasets
    rem.close()
    catchments.close()

    return(0)

def __inundate_in_huc(rem_array,catchments_array,crs,window_transform,rem_profile,catchments_profile,hucCode,
                      catchmentStagesDict,depths,inundation_raster,inundation_polygon,
                      out_raster_profile,out_vector_profile):

    # verbose print
    if hucCode is not None:
        print("Mapping {}".format(hucCode))

    # save desired profiles for outputs
    depths_profile = rem_profile
    inundation_profile = catchments_profile

    # update output profiles from inputs
    if isinstance(out_raster_profile,dict):
        depths_profile.update(**out_raster_profile)
        inundation_profile.update(**out_raster_profile)
    elif out_raster_profile is None:
        depths_profile.update(driver= 'GTiff', blockxsize=256, blockysize=256, tiled=True, compress='lzw')
        inundation_profile.update(driver= 'GTiff',blockxsize=256, blockysize=256, tiled=True, compress='lzw')
    else:
        raise TypeError("Pass dictionary for output raster profiles")

    # update profiles with width and heights from array sizes
    depths_profile.update(height=rem_array.shape[0],width=rem_array.shape[1])
    inundation_profile.update(height=catchments_array.shape[0],width=catchments_array.shape[1])
    
    # update transforms of outputs with window transform
    depths_profile.update(transform=window_transform)
    inundation_profile.update(transform=window_transform)

    # open output depths
    if isinstance(depths,str): 
        depths = __append_huc_code_to_file_name(depths,hucCode)
        depths = rasterio.open(depths, "w", **depths_profile)
    elif isinstance(depths,DatasetWriter):
        pass
    elif depths is None:
        pass
    else:
        raise TypeError("Pass rasterio dataset, filepath for output depths, or None.")
    
    # open output inundation raster
    if isinstance(inundation_raster,str): 
        inundation_raster = __append_huc_code_to_file_name(inundation_raster,hucCode)
        inundation_raster = rasterio.open(inundation_raster,"w",**inundation_profile)
    elif isinstance(inundation_raster,DatasetWriter):
        pass
    elif inundation_raster is None:
        pass
    else:
        raise TypeError("Pass rasterio dataset, filepath for output inundation raster, or None.")

    # prepare output inundation polygons schema
    if inundation_polygon is not None:
        if out_vector_profile is None:
            out_vector_profile = {'crs' : crs , 'driver' : 'GPKG'}
    
        out_vector_profile['schema'] = {
                                         'geometry' : 'Polygon',
                                         'properties' : OrderedDict([('HydroID' , 'int')])
                                       }

        # open output inundation polygons
        if isinstance(inundation_polygon,str):
            inundation_polygon = __append_huc_code_to_file_name(inundation_polygon,hucCode)
            inundation_polygon = fiona.open(inundation_polygon,'w',**out_vector_profile)
        elif isinstance(inundation_polygon,fiona.Collection):
            pass
        else:
            raise TypeError("Pass fiona collection or file path as inundation_polygon")

    # save desired array shape
    desired_shape = rem_array.shape

    # flatten
    rem_array = rem_array.ravel()
    catchments_array = catchments_array.ravel()

    # create flat outputs
    depths_array = rem_array.copy()
    inundation_array = catchments_array.copy()

    # reset output values
    depths_array[depths_array != depths_profile['nodata']] = 0
    inundation_array[inundation_array != inundation_profile['nodata']] = inundation_array[inundation_array != inundation_profile['nodata']] * -1

    # make output arrays
    inundation_array,depths_array = __go_fast_mapping(rem_array,catchments_array,catchmentStagesDict,inundation_array,depths_array)
    
    # reshape output arrays
    inundation_array = inundation_array.reshape(desired_shape)
    depths_array = depths_array.reshape(desired_shape)

    # write out inundation and depth rasters
    if isinstance(inundation_raster,DatasetWriter):
        inundation_raster.write(inundation_array,indexes=1)
    if isinstance(depths,DatasetWriter):
        depths.write(depths_array,indexes=1)
    
    # polygonize inundation
    if isinstance(inundation_polygon,fiona.Collection):
        
        # make generator for inundation polygons
        inundation_polygon_generator = shapes(inundation_array,mask=inundation_array>0,connectivity=8,transform=window_transform)
        
        # generate records
        records = []
        for i,(g,h) in enumerate(inundation_polygon_generator):
            record = dict()
            record['geometry'] = g
            record['properties'] = {'HydroID' : int(h)}
            records += [record]

        # write out
        inundation_polygon.writerecords(records)
        
    if isinstance(depths,DatasetWriter): depths.close()
    if isinstance(inundation_raster,DatasetWriter): inundation_raster.close()
    if isinstance(inundation_polygon,fiona.Collection): inundation_polygon.close()
    
    # return file names of outputs for aggregation. Handle Nones
    try:
        ir_name = inundation_raster.name
    except AttributeError:
        ir_name = None
    
    try:
        d_name = depths.name
    except AttributeError:
        d_name = None

    try:
        ip_name = inundation_polygon.path
    except AttributeError:
        ip_name = None

    return(ir_name,d_name,ip_name)

@njit
def __go_fast_mapping(rem,catchments,catchmentStagesDict,inundation,depths):

    for i,(r,cm) in enumerate(zip(rem,catchments)):
        if cm in catchmentStagesDict:

            depth = catchmentStagesDict[cm] - r
            depths[i] = max(depth,0) # set negative depths to 0

            if depths[i] > 0: # set positive depths to positive
                inundation[i] *= -1
            #else: # set positive depths to value of positive catchment value
                #inundation[i] = cm

    return(inundation,depths)


def __make_windows_generator(rem,catchments,catchmentStagesDict,inundation_raster,inundation_polygon,
                             depths,out_raster_profile,out_vector_profile,hucs=None,hucSet=None):

    if hucs is not None:
        
        # get attribute name for HUC column
        for huc in hucs:
            for hucColName in huc['properties'].keys():
                if 'HUC' in hucColName:
                    hucSize = int(hucColName[-1])
                    break
            break
        
        # make windows
        for huc in hucs:
    
            if hucSet is not None:
                # temporary: will change with hydro-table introduction 
                if huc['properties'][hucColName][0:len(hucSet)] not in hucSet:
                    continue
            
            try:
                #window = geometry_window(rem,shape(huc['geometry']))
                rem_array,window_transform = mask(rem,shape(huc['geometry']),crop=True,indexes=1)
                catchments_array,_ = mask(catchments,shape(huc['geometry']),crop=True,indexes=1)
            except ValueError: # shape doesn't overlap raster
                continue # skip to next HUC

            hucCode = huc['properties'][hucColName]

            yield (rem_array,catchments_array,rem.crs.wkt,
                   window_transform,rem.profile,catchments.profile,hucCode,
                   catchmentStagesDict,depths,inundation_raster,
                   inundation_polygon,out_raster_profile,out_vector_profile)

    else:
        hucCode = None
        #window = Window(col_off=0,row_off=0,width=rem.width,height=rem.height)

        yield (rem.read(1),catchments.read(1),rem.crs.wkt,
               rem.transform,rem.profile,catchments.profile,hucCode,
               catchmentStagesDict,depths,inundation_raster,
               inundation_polygon,out_raster_profile,out_vector_profile)


def __append_huc_code_to_file_name(fileName,hucCode):

    if hucCode is None:
        return(fileName)

    base_file_path,extension = splitext(fileName)

    return("{}_{}{}".format(base_file_path,hucCode,extension))


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

                h = round(h,4)

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
            
            h = round(h,4)

            hid = types.int32(hid) ; h = types.float32(h)
            catchmentStagesDict[hid] = h

    return(catchmentStagesDict)


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Inundation mapping for FOSS FIM')
    parser.add_argument('-r','--rem', help='REM raster at job level or mosaic vrt. Must match catchments CRS.', required=True)
    parser.add_argument('-c','--catchments',help='Catchments raster at job level or mosaic VRT. Must match rem CRS.',required=True)
    parser.add_argument('-f','--forecast',help='Forecast discharges in CMS as CSV file',required=True)
    parser.add_argument('-s','--rating-curve',help='SRC JSON file',required=True)
    parser.add_argument('-w','--cross-walk',help='Cross-walk table csv',required=True)
    parser.add_argument('-u','--hucs',help='HUCs file to process at. Must match CRS of input rasters',required=False,default=None)
    parser.add_argument('-l','--hucs-layerName',help='Layer name in HUCs file to use',required=False,default=None)
    parser.add_argument('-n','--num-workers',help='Number of concurrent processes',required=False,default=1,type=int)
    parser.add_argument('-i','--inundation-raster',help='Inundation Raster output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-p','--inundation-polygon',help='Inundation polygon output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-d','--depths',help='Depths raster output. Only writes if designated.',required=False,default=None)
    parser.add_argument('-a','--aggregate',help='Aggregate outputs to VRT files',required=False,action='store_true')
    parser.add_argument('-t','--current-huc',help='May deprecate soon, likely temporary. Pass current HUC code',required=True,default=None)

    # extract to dictionary
    args = vars(parser.parse_args())
    
    # call function
    inundate(**args)

