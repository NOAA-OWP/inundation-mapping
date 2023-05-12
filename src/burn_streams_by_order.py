#!/usr/bin/env python3

import argparse
from typing import Union
import numpy as np
import geopandas as gpd
import rasterio as rio
from rasterio import features
from agreedem import agreedem

def burn_streams_by_order(streams_vector:str, streams_order_attribute:str, dem:str, output_raster:str, workspace:str, buffer_dist:float, smooth_drop:float, sharp_drop:float, delete_intermediate_data:bool):
    """
    Burn NWM streams by order. Rasterizes each order independently then burns them into the DEM using AGREE methodology, takes the minimum elevation value in each cell.

    Parameters
    ----------
    streams_vector: str
        Vector file of features to be rasterized
    streams_order_attributes: str
        Vector attribute to be rasterized
    output_raster: str
        Filename to save rasterized vector
    dem : STR
        Elevation DEM (units assumed to be in meters). For example, see dem_meters.tif.
    output_raster : STR
        Path to output raster. For example, dem_burned.tif
    workspace : STR
        Path to workspace to save all intermediate files.
    buffer_dist : FLOAT
        AGREE stream buffer distance (in meters) on either side of stream.
    smooth_drop : FLOAT
        Smooth drop distance (in meters). Typically this has been 10m.
    sharp_drop : FLOAT
        Sharp drop distance (in meters). Typically this has been 1000m.
    delete_intermediate_data: BOOL
        If True all intermediate data is deleted, if False (default) no intermediate datasets are deleted.
    """

    if isinstance(streams_vector, str):
        streams_vector = gpd.read_file(streams_vector)

    orders = streams_vector[streams_order_attribute].unique()
    orders = np.sort(orders)

    for i, order in enumerate(orders):
        rasterize_by_stream_order(streams_vector, streams_order_attribute, order, dem, output_raster)

        print(f'Burning stream order {order}')
        agreedem(output_raster, dem, output_raster, workspace, buffer_dist, smooth_drop, sharp_drop, delete_intermediate_data)

        with rio.open(output_raster) as out:
            out_data = out.read(1)

        if i == 0:
            burned = out_data
        else:
            burned = np.minimum(burned, out_data)

    with rio.open(dem) as rst:
        meta = rst.meta.copy()

    with rio.open(output_raster, 'w', **meta) as out:
        out.write_band(1, burned)


def rasterize_by_stream_order(input_vector:gpd.GeoDataFrame, input_vector_attribute:str, input_vector_attribute_value:int, input_raster:str, output_raster:str=''):
    """
    Rasterizes NWM streams by order.

    Parameters
    ----------
    input_vector: geopandas.GeoDataFrame
        Vector file of features to be rasterized
    input_vector_attributes: str
        Vector attribute to be rasterized
    input_raster: str
        Raster with same properties (e.g., width and height) as the desired output
    output_raster: str
        Filename to save rasterized vector
    """

    if isinstance(input_vector, str):
        input_vector = gpd.read_file(input_vector)

    with rio.open(input_raster) as rst:
        meta = rst.meta.copy()
        input_ds = rst.read(1)
        input_shape = input_ds.shape

    del input_ds

    temp_vector = input_vector[input_vector[input_vector_attribute]==input_vector_attribute_value]

    burned = features.rasterize(shapes=((geom,int(value)) for geom, value in zip(temp_vector['geometry'], temp_vector[input_vector_attribute])), fill=0, out_shape=input_shape, transform=meta['transform'])

    if output_raster != '':
        with rio.open(output_raster, 'w', **meta) as out:
            out.write_band(1, burned)

    return burned


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rasterizes NWM streams by order')
    parser.add_argument('-v', '--streams-vector', help='Streams vector', type=str, required=True)
    parser.add_argument('-a', '--streams-order-attribute', help='Input vector attribute', type=str, required=False)
    parser.add_argument('-d', '--dem',  help = 'DEM raster in meters', required = True)
    parser.add_argument('-w', '--workspace', help = 'Workspace', required = True)
    parser.add_argument('-o',  '--output-raster', help = 'Path to output raster', type=str, required = True)
    parser.add_argument('-b',  '--buffer-dist', help = 'Buffer distance (m) on either side of channel', required = True)
    parser.add_argument('-sm', '--smooth-drop', help = 'Smooth drop (m)', required = True)
    parser.add_argument('-sh', '---sharp-drop', help = 'Sharp drop (m)', required = True)
    parser.add_argument('-t',  '--delete-intermediate-data',  help = 'Optional flag to delete intermediate datasets', action = 'store_true')

    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    args['buffer_dist'] = float(args['buffer_dist'])
    args['smooth_drop'] = float(args['smooth_drop'])
    args['sharp_drop'] =  float(args['sharp_drop'])
 
    #Run agreedem
    burn_streams_by_order(**args)
