#!/usr/bin/env python3

import argparse
import numpy as np
import geopandas as gpd
import rasterio as rio
from rasterio import features
from utils.shared_functions import mem_profile

@mem_profile
def rasterize_vector_attributes(input_vector:str, input_vector_attribute:str, input_raster:str, output_raster:str):
    """
    Rasterizes NWM streams by order. Rasterizes each order independently then takes the maximum value in each cell.

    Parameters
    ----------
    input_vector: str
        Vector file of features to be rasterized
    input_vector_attributes: str
        Vector attribute to be rasterized
    input_raster: str
        Raster with same properties (e.g., width and height) as the desired output
    output_raster: str
        Filename to save rasterized vector
    """

    input_vector = gpd.read_file(input_vector)

    with rio.open(input_raster) as rst:
        meta = rst.meta.copy()
        input_ds = rst.read(1)
        input_shape = input_ds.shape

    del input_ds

    orders = input_vector[input_vector_attribute].unique()
    orders = np.sort(orders)

    for i, order in enumerate(orders):
        vector_order = input_vector[input_vector[input_vector_attribute]==order]

        if i == 0:
            burned = features.rasterize(shapes=((geom,int(value)) for geom, value in zip(vector_order['geometry'], vector_order[input_vector_attribute])), fill=0, out_shape=input_shape, transform=meta['transform'])
        else:
            burned = np.maximum(burned, features.rasterize(shapes=((geom,int(value)) for geom, value in zip(vector_order['geometry'], vector_order[input_vector_attribute])), fill=0, out_shape=input_shape, transform=meta['transform']))

    with rio.open(output_raster, 'w', **meta) as out:
        out.write_band(1, burned)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rasterizes NWM streams by order')
    parser.add_argument('-v', '--input-vector', help='Input vector', type=str, required=True)
    parser.add_argument('-a', '--input-vector-attribute', help='Input vector attribute', type=str, required=True)
    parser.add_argument('-i', '--input-raster', help='Input raster', type=str, required=True)
    parser.add_argument('-o', '--output-raster', help='Output raster', type=str, required=True)

    args = vars(parser.parse_args())

    rasterize_vector_attributes(**args)