#!/usr/bin/env python3

from pygeohydro import WBD
import py3dep
import argparse
import csv
import pandas as pd
import dask
from tqdm.dask import TqdmCallback
from foss_fim.src.utils.shared_variables import PREP_PROJECTION
import geopandas as gpd


def get_sources_by_hucs(hucs,desired_crs=PREP_PROJECTION,output_file=None):

    # Parse HUCs from hucs.
    if isinstance(hucs,list):
        if len(hucs) == 1:
            try:
                with open(hucs[0]) as csv_file:  # Does not have to be CSV format.
                    hucs = [i[0] for i in csv.reader(csv_file)]
            except FileNotFoundError:
                hucs = hucs
        else:
                hucs = hucs
    elif isinstance(hucs,str):
        try:
            with open(hucs) as csv_file:  # Does not have to be CSV format.
                hucs = [i[0] for i in csv.reader(csv_file)]
        except FileNotFoundError:
            hucs = list(hucs)
        
    huc_length = [ len(h) for h in hucs ]
    huc_length = set(huc_length)

    if len(huc_length) > 1:
        raise ValueError("Pass equivalent length HUCs")

    huc_length = list(huc_length)[0]
    huc_column_name = f'huc{huc_length}'

    wbd = WBD(f'huc{huc_length}')
    wbd_df = wbd.byids(f'huc{huc_length}',hucs)

    wbd_df = wbd_df.to_crs(desired_crs)

    bounds_hucs = [(r['geometry'].bounds,r[huc_column_name]) for _,r in wbd_df.iterrows()]

    def __get_sources_for_dask(b,h):
        
        try:
            sources = py3dep.query_3dep_sources(b,crs=wbd_df.crs)
        except Exception as e:
            print(f'HUC {h}: {e}')
        else:
            return(sources)

    #source_polygons = [py3dep.query_3dep_sources(b,crs=wbd_df.crs) for b in bounds]
    compute_list = [dask.delayed(__get_sources_for_dask)(b,h) for b,h in bounds_hucs]

    with TqdmCallback(desc='Get data sources'):
        source_polygons = dask.compute(*compute_list)

    print('Creating dataframe with results')
    source_polygons = pd.concat(source_polygons).reset_index(drop=True)
    source_polygons = source_polygons.explode()
    source_polygons = source_polygons.reset_index(drop=True)
    source_polygons = source_polygons.loc[source_polygons.is_valid,:]
    source_polygons = source_polygons.to_crs(desired_crs)

    source_polygons = source_polygons.loc[:,['dem_res','geometry']]

    # clipping
    print('Clipping to WBDs')
    source_polygons = gpd.overlay(source_polygons,wbd_df,keep_geom_type=True,make_valid=True)
    source_polygons = source_polygons.explode(ignore_index=True,index_parts=True)
    source_polygons=source_polygons.dropna(axis=1)
    source_polygons = source_polygons.loc[source_polygons.is_valid,:]

    if output_file is not None:
        source_polygons.to_file(output_file,index=False,driver='GPKG')

    return(source_polygons)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Gets polygons of sources')
    parser.add_argument('-u','--hucs',help='HUCs consistent size',required=True,nargs='+')
    parser.add_argument('-c','--desired-crs',help='Desired CRS',required=False,default=PREP_PROJECTION)
    parser.add_argument('-o','--output-file',help='Output file name',required=False,default=None)

    args = vars(parser.parse_args())

    get_sources_by_hucs(**args)

