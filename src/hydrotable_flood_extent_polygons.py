#!/usr/bin/env python3
'''
Generate flood extent polygons for the synthetic river rating curve (hydroTable).

Author: Yan Y. Liu <yanliu@ornl.gov>, Oak Ridge National Laboratory
Date: 2023-07-04

Example command:
/path/to/fim4/src/hydrotable_flood_extent_polygons.py -H hydroTable.csv -r rem_zeroed_masked.tif -c gw_catchments_reaches_filtered_addedAttributes.tif -o flood_extent_polygons.gpkg -j 4
'''
import argparse
import numpy as np
import numpy.ma as ma
import os, pathlib
from datetime import datetime
import pandas as pd
import shapely
import shapely.geometry
import shapely.errors
import shapely.ops
import fiona
import fiona.crs
import rasterio
import rasterio.mask
import rasterio.features
import rasterio.windows
import copy
import json
from collections import OrderedDict

from concurrent.futures import ProcessPoolExecutor, as_completed

def fix_polygons(g):
    '''fix invalid polygons and return a list of valid polygons'''
    g_list = []
    if not shapely.is_valid(g):
        g_fix = shapely.make_valid(g)
        g_fix_list = []
        if type(g_fix) == shapely.MultiPolygon:
            for gg in g_fix.geoms:
                g_fix_list.append(gg)
        elif type(g_fix) == shapely.Polygon:
            g_fix_list.append(g_fix)
        elif type(g_fix) == shapely.GeometryCollection:
            for gg in g_fix.geoms:
                if type(gg) == shapely.Polygon:
                    g_fix_list.append(gg)
                elif type(gg) == shapely.MultiPolygon:
                    for ggg in gg.geoms:
                        g_fix_list.append(ggg)
        # add valid fixed polygons
        for gg in g_fix_list:
            if shapely.is_valid(gg):
                g_list.append(gg)
    else: # always return a list of polygons
        if type(g) == shapely.MultiPolygon or type(g) == shapely.GeometryCollection:
            for gg in g.geoms:
                g_list.append(gg)
        else:
            g_list.append(g)
    
    return g_list

def flood_extent_polygonize(catchgrid, b_catch, b_flood_mask, hydroids):
    '''
    Polygonize flood extent.

    Parameters
    ----------
    catchgrid : Catchment raster dataset
    b_catch : Catchment raster band
    b_flood_mask : Inundation mask computed from HAND and flood stage
    hydroids : List of HydroIDs to check if catchment is in the hydrotable

    Returns
    -------
    o_data : Dictionary of flood extent polygons, keyed by HydroID
    '''

    extent_shp = rasterio.features.shapes(b_catch, mask=b_flood_mask,connectivity=8,transform=catchgrid.transform)
    poly_data = {}
    for i,(geom, hydroid) in enumerate(extent_shp):
        if hydroid <= 0 or hydroid not in hydroids:
            continue
        g = shapely.geometry.shape(geom)
        # make polygons valid
        g_list = fix_polygons(g)
        if len(g_list) == 0: # no valid polygons
            continue # should not happen
        # add to dict and group by HydroID
        if hydroid not in poly_data:
            poly_data[hydroid] = g_list
        else:
            poly_data[hydroid] += g_list
    # create multipolygon for each HydroID
    o_data = {}
    for hydroid in poly_data:
        #poly_data[hydroid] = shapely.ops.cascaded_union(poly_data[hydroid])
        mp = shapely.MultiPolygon(poly_data[hydroid])
        mp = mp.buffer(0) # merge adjacent polygons, returns a multipolygon or a polygon
        # polys = fix_polygons(mp) # assume buffer() always returns valid polygons
        if type(mp) == shapely.Polygon:
            mp = shapely.MultiPolygon([mp])
        o_data[int(hydroid)] = mp
    
    return o_data

def flood_extent_by_stage(f_hhand, f_catchgrid, stage, stage_index, o_filename):
    '''
    Compute flood extent polygons for a given stage. 
    Write the flood extent MultiPolygons to a temporary file.
    This function is the target function for parallel processing.

    Parameters
    ----------
    f_hhand : File path to the healed HAND raster file
    f_catchgrid : File path to the catchment raster file
    stage : Flood stage (1ft interval, 0m - 25m)
    stage_index : Index of the flood stage (0 - 83)
    o_filename : Output file path
    '''

    print('processing stage:', stage)
    catchgrid = rasterio.open(f_catchgrid)
    b_catch = catchgrid.read(1, masked=True)
    hhand = rasterio.open(f_hhand)
    b_hhand = hhand.read(1, masked=True)
    
    rows = []
    if stage_index == 0: # no flood, to show river surface
        stage += 0.01 # 10 mm
    b_flood_mask = ~b_catch.mask & ~b_hhand.mask & ((stage - b_hhand.data) >= 0)
    floodext = flood_extent_polygonize(catchgrid, b_catch, b_flood_mask, hydroids)
    for hydroid, mp in floodext.items():
        if not shapely.is_valid(mp):
            print('invalid multipolygon for HydroID:', hydroid, 'at stage:', stage)
        feature_id = d_hydrotable[d_hydrotable['HydroID']==hydroid].iloc[0]['feature_id']
        rows.append({
            'geometry': json.loads(shapely.to_geojson(mp)),
            'properties': OrderedDict([
                ('HydroID', hydroid),
                ('feature_id', feature_id),
                ('stage', stage),
                ('stage_index', stage_index),
            ])
        })
    
    extent_meta = {
        #'crs' : hand.crs ,
        'crs' : fiona.crs.CRS.from_epsg(hhand.crs.to_epsg()), # hhand.crs ,
        #'driver' : 'ESRI Shapefile',
        'driver' : 'GPKG', # to avoid int32 limitation in shapefile
        'schema': {
            'geometry' : 'MultiPolygon',
            'properties' : OrderedDict([
                ('HydroID' , 'int'),
                ('feature_id' , 'int'),
                ('stage' , 'float'),
                ('stage_index', 'int'),
            ])
        }
    }
    
    # write to file
    with fiona.open(o_filename, 'w', **extent_meta) as dst:
        dst.writerecords(rows)
    
    # close rasters
    hhand.close()
    catchgrid.close()

if __name__ == '__main__':

    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Generate flood extent polygons for the synthetic river rating curve (hydroTable).')
    parser.add_argument('-H','--hydrotable', help='file path to the rating curve table (hydroTable) in csv format, e.g., hydroTable.csv',
                        required=True)
    parser.add_argument('-r','--rem', help='file path to the healed HAND raster file, e.g., rem_zeroed_masked_healed.tif',
                        required=True)
    parser.add_argument('-c','--catchgrid', help='file path to the catchment raster file, e.g., gw_catchments_reaches_filtered_addedAttributes.tif',
                        required=True)
    parser.add_argument('-o',  '--output', help = 'file path to output GeoPackage', required = True)
    parser.add_argument('-t','--tmpdir', help='temporary directory to store intermediate files. Default: /tmp',
                        required=False, default='/tmp')
    parser.add_argument('-j','--num_job_workers', help='Number of processes to use',
                        required=False, default=1, type=int)

    args = vars(parser.parse_args())

    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # rename variable inputs
    f_hydrotable = args['hydrotable']
    f_hhand = args['rem']
    f_catchgrid = args['catchgrid']
    o_filename = args['output']
    tdir = args['tmpdir'] + '/flood_extent_polygons'
    num_job_workers = args['num_job_workers']

    # create tmpdir if not exist
    pathlib.Path(tdir).mkdir(parents=True, exist_ok=True)

    # read hydrotable
    d_hydrotable = pd.read_csv(f_hydrotable, usecols=['HydroID', 'feature_id', 'stage', 'discharge_cms'], dtype={'HydroID':np.int32, 'feature_id':np.int32, 'stage':np.float64, 'discharge_cms':np.float64})
    stages = np.sort(d_hydrotable[d_hydrotable['HydroID']==d_hydrotable.iloc[0]['HydroID']]['stage'].to_numpy())
    comids_h = np.unique(d_hydrotable['feature_id'].to_numpy())
    print('num of unique COMIDs in hydrotable:', comids_h.size)
    hydroids = np.unique(d_hydrotable['HydroID'].to_numpy())
    print('[ ' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ' ]' + ' [INFO] ' + 'loading hydrotable...')
    print('unique HydroIDs:', hydroids.size, 'unique feature_ids:', comids_h.size, 'num of stages:', stages.size)

    # compute flood extent polygons for each stage
    print('[ ' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ' ]' + ' [INFO] ' + 'computing flood extent polygons for each stage...')
    ## sequential code:
    # stages = stages[:4] # DEBUG
    # for stage_index in range(stages.shape[0]):
    #     t_filename = tdir + '/' + 'flood_extent_polygons_' + str(stage_index) + '.gpkg'
    #     flood_extent_by_stage(f_hhand, f_catchgrid, stage=stages[stage_index], stage_index=stage_index, o_filename=t_filename)

    ## parallel code:
    futures = []
    with ProcessPoolExecutor(max_workers=num_job_workers) as executor:
        for stage_index in range(stages.shape[0]):
            t_filename = tdir + '/' + 'flood_extent_polygons_' + str(stage_index) + '.gpkg'
            futures.append(executor.submit(flood_extent_by_stage, f_hhand=f_hhand, f_catchgrid=f_catchgrid, stage=stages[stage_index], stage_index=stage_index, o_filename=t_filename))
    for future in as_completed(futures):
        future.result()


    # merge stages into a single file
    print('[ ' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ' ]' + ' [INFO] ' + 'merging flood extent polygons into the output file...')
    i_filename = tdir + '/' + 'flood_extent_polygons_' + str(0) + '.gpkg'
    src = fiona.open(i_filename, 'r')
    extent_meta = copy.deepcopy(src.meta)
    src.close()

    with fiona.open(o_filename, 'w', **extent_meta) as dst:
        for stage_index in range(stages.shape[0]):
            i_filename = tdir + '/' + 'flood_extent_polygons_' + str(stage_index) + '.gpkg'
            with fiona.open(i_filename, 'r') as src:
                dst.writerecords(src)

    # remove temporary files
    print('[ ' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ' ]' + ' [INFO] ' + 'removing temporary files...')
    for stage_index in range(stages.shape[0]):
        i_filename = tdir + '/' + 'flood_extent_polygons_' + str(stage_index) + '.gpkg'
        os.remove(i_filename)

    print('[ ' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ' ]' + ' [INFO] ' + 'DONE!')