#!/usr/bin/env python3

'''
 Description: Mask raster layers using 'mainstems' stream buffer
'''

import sys
import os
import argparse
import geopandas as gpd
import rasterio.mask


@profile
def fr_to_ms_raster_mask(ms_buffer_dist, split_flows_filename, fdr_fr, dem_fr, slope_fr, fdr_ms_filename, dem_ms_filename, slope_ms_filename, str_pixel_fr, str_pixel_ms_filename):
    # create output layer names
    split_flows = gpd.read_file(split_flows_fileName)

    # Limit the rasters to the buffer distance around the draft streams.
    print ("Limiting rasters to buffer area ({} meters) around model streams".format(str(ms_buffer_dist)))

    split_flows_ms_buffer = split_flows.unary_union.buffer(ms_buffer_dist)

    print('Writing raster outputs ...')

    # Mask nhddem
    with rasterio.open(dem_fr) as src:
        out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open(os.path.join(os.path.dirname(dem_fr), dem_ms_filename), "w", **out_meta) as dest:
        dest.write(out_image)

    # Mask fdr
    with rasterio.open(fdr_fr) as src:
        out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open(os.path.join(os.path.dirname(fdr_fr), fdr_ms_filename), "w", **out_meta) as dest:
        dest.write(out_image)

    # Mask slope
    with rasterio.open(slope_fr) as src:
        out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open(os.path.join(os.path.dirname(slope_fr), slope_ms_filename), "w", **out_meta) as dest:
        dest.write(out_image)

    # Mask stream pixels
    with rasterio.open(str_pixel_fr) as src:
        out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open(os.path.join(os.path.dirname(str_pixel_fr), str_pixel_ms_filename), "w", **out_meta) as dest:
        dest.write(out_image)


if __name__ == '__main__':
    ms_buffer_dist = int(os.environ['ms_buffer_dist'])

    # Parse arguments.
    parser = argparse.ArgumentParser(description='fr_to_ms_raster_mask.py')
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename', required=True)
    parser.add_argument('-f', '--fdr-fr', help='fdr-fr', required=True)
    parser.add_argument('-d', '--dem-fr', help='dem-fr', required=True)
    parser.add_argument('-r', '--slope-fr', help='slope-fr', required=True)
    parser.add_argument('-m', '--fdr-ms-filename', help='fdr-ms-filename', required=True)
    parser.add_argument('-n', '--dem-ms-filename', help='dem-ms-filename', required=True)
    parser.add_argument('-o', '--slope-ms-filename', help='slope-ms-filename', required=True)
    parser.add_argument('-p', '--str-pixel-fr', help='str-pixel-fr', required=True)
    parser.add_argument('-q', '--str-pixel-ms-filename', help='str-pixel-ms-filename', required=True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    fr_to_ms_raster_mask(ms_buffer_dist, **args)
