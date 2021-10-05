#!/usr/bin/env python3

from shapely.geometry import box
import pandas as pd
import geopandas as gpd
import argparse
from datetime import datetime


def find_hucs_of_bounding_boxes(bounding_boxes_file,wbd=None,projection_of_boxes='EPSG:4329',wbd_layer='WBDHU8',huc_output_file=None,forecast_output_file=None):


    # load bounding box file
    bounding_boxes = pd.read_csv(bounding_boxes_file,
                                 dtype={'minx':float,'miny':float,'maxx':float,'maxy':float})


    make_box_geom = lambda df : box(df['minx'],df['miny'],df['maxx'],df['maxy'])

    bounding_boxes['geometry'] = bounding_boxes.apply(make_box_geom,axis=1)

    bounding_boxes = gpd.GeoDataFrame(bounding_boxes,crs=projection_of_boxes)

    wbd_proj = gpd.read_file(wbd,layer=wbd_layer,rows=1).crs

    bounding_boxes = bounding_boxes.to_crs(wbd_proj)

    wbdcol_name = 'HUC'+wbd_layer[-1]

    get_intersections = lambda bbdf : gpd.read_file(wbd,layer=wbd_layer,mask=bbdf.geometry)[wbdcol_name]

    hucs = bounding_boxes.apply(get_intersections,axis=1)
        
    bounding_boxes.drop(columns=['geometry','minx','miny','maxx','maxy'],inplace=True)

    hucs_columns = hucs.columns
    bb_columns = bounding_boxes.columns
    bounding_boxes = hucs.join(bounding_boxes)
    bounding_boxes = pd.melt(bounding_boxes,id_vars=bb_columns,value_vars=hucs_columns,value_name='HUC8')
    bounding_boxes.drop(columns=['variable'],inplace=True)
    bounding_boxes.dropna(inplace=True)
    bounding_boxes.reset_index(drop=True,inplace=True)

    hucs_series = pd.Series(hucs.stack().reset_index(drop=True).unique())
    
    if huc_output_file is not None:
        hucs_series.to_csv(huc_output_file,sep='\n',index=False,header=False)
    
    if forecast_output_file is not None:
        bounding_boxes.to_csv(forecast_output_file,index=False,date_format='%Y-%m-%d %H:%M:%S%Z')
    
    return(hucs_series,bounding_boxes)


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Find hucs for bounding boxes')
    parser.add_argument('-b','--bounding-boxes-file', help='Bounding box file', required=True)
    parser.add_argument('-w','--wbd', help='WBD file', required=True)
    parser.add_argument('-o','--huc-output-file', help='Output file of HUCS', required=False,default=None)
    parser.add_argument('-f','--forecast-output-file', help='Forecast file', required=False,default=None)

    args=vars(parser.parse_args())

    find_hucs_of_bounding_boxes(**args)
