#!/usr/bin/env python3

import argparse
import copy
import os
import pathlib
import sys
import tempfile

import numpy as np
import pandas as pd

# Import raster and vector function libraries
# from types import NoneType
from osgeo import gdal, ogr
from osgeo.gdalconst import *
from pandas import DataFrame
from pixel_counter_functions import (
    get_bridge_counts,
    get_levee_counts,
    get_mask_value_counts,
    get_nlcd_counts,
    get_nlcd_counts_inside_flood,
)


'''Created on 02/21/2022.
Written by:
Anuska Narayanan (The University of Alabama Department of Geography, anarayanan1@crimson.ua.edu;
Sophie Williams (The University of Alabama Department of Geography, scwilliams8@crimson.ua.edu; and
Brad Bates (NOAA, Lynker, and the National Water Center, bradford.bates@noaa.gov)

Derived from a Python version of a zonal statistics function written by Matthew Perry (@perrygeo).

Description: This script isolates the number of pixels per class of a raster within the outlines of
    one or more polygons and displays them in a table. It accomplishes this by rasterizing the vector file,
    masking out the desired areas of both rasters, and then summarizing them in a dataframe. It makes use
    of the gdal, numpy, and pandas function libraries.
Inputs: one raster file with at least one set of attributes; one vector file containing one or more polygon
    boundaries
Output: a dataframe table with rows displayed by each polygon within the vector file, and columns
    displaying the pixel count of each raster attribute class in the polygon
'''


# Set up error handler
gdal.PushErrorHandler('CPLQuietErrorHandler')


# Function to pologonize flood extent
def make_flood_extent_polygon(flood_extent):
    flood_extent_dataset = gdal.Open(flood_extent)
    cols = flood_extent_dataset.RasterXSize
    rows = flood_extent_dataset.RasterYSize

    # Get some metadata to filter out NaN values
    flood_extent_raster = flood_extent_dataset.GetRasterBand(1)
    noDataVal = flood_extent_raster.GetNoDataValue()  # no data value
    scaleFactor = flood_extent_raster.GetScale()  # scale factor

    # Assign flood_extent No Data Values to NaN
    flood_extent_array = flood_extent_dataset.GetRasterBand(1).ReadAsArray(0, 0, cols, rows).astype(np.float)
    flood_extent_array[flood_extent_array == int(noDataVal)] = np.nan
    flood_extent_array = flood_extent_array / scaleFactor

    # Assign flood_extent Negative Values to NaN
    flood_extent_nonzero_array = copy.copy(flood_extent_array)
    flood_extent_nonzero_array[flood_extent_array < 0] = np.nan

    # make temporary output file
    mem_drv = gdal.GetDriverByName('MEM')
    target = mem_drv.Create('temp_tif', cols, rows, 1, gdal.GDT_Float32)
    target.GetRasterBand(1).WriteArray(flood_extent_nonzero_array)

    # Add GeoTranform and Projection
    geotrans = flood_extent_dataset.GetGeoTransform()
    proj = flood_extent_dataset.GetProjection()
    target.SetGeoTransform(geotrans)
    target.SetProjection(proj)
    target.FlushCache()

    # set up inputs for converting flood extent raster to polygon
    band = target.GetRasterBand(1)
    band.ReadAsArray()

    outshape_location = tempfile.gettempdir()
    outshape_location_path = os.path.abspath(outshape_location)
    outShapefile = outshape_location_path + "/" + "polygonized.shp"
    driver = ogr.GetDriverByName("ESRI Shapefile")
    outDatasource = driver.CreateDataSource(outShapefile)
    outLayer = outDatasource.CreateLayer("buffalo", srs=None)

    # Add the DN field
    newField = ogr.FieldDefn('HydroID', ogr.OFTInteger)
    outLayer.CreateField(newField)

    # Polygonize
    gdal.Polygonize(band, None, outLayer, 0, [], callback=None)
    outDatasource.Destroy()
    # sourceRaster = None

    fullpath = os.path.abspath(outShapefile)
    print(fullpath)
    print(type(fullpath))

    return fullpath


# Function that transforms vector dataset to raster
def bbox_to_pixel_offsets(gt, bbox):
    originX = gt[0]
    originY = gt[3]
    pixel_width = gt[1]
    pixel_height = gt[5]
    x1 = int((bbox[0] - originX) / pixel_width)
    x2 = int((bbox[1] - originX) / pixel_width) + 1

    y1 = int((bbox[3] - originY) / pixel_height)
    y2 = int((bbox[2] - originY) / pixel_height) + 1

    xsize = x2 - x1
    ysize = y2 - y1
    return (x1, y1, xsize, ysize)


# Main function that determines zonal statistics of raster classes in a polygon area
def zonal_stats(vector_path, raster_path_dict, nodata_value=None, global_src_extent=False):
    stats = []

    # Loop through different raster paths in the raster_path_dict and
    # perform zonal statistics on the files.
    for layer in raster_path_dict:
        raster_path = raster_path_dict[layer]
        if raster_path == "":  # Only process if a raster path is provided
            continue
        if layer == 'flood_extent' and raster_path_dict["nlcd"] != "":
            vector_path = make_flood_extent_polygon(flood_extent)
            raster_path = raster_path_dict["nlcd"]

        # Opens raster file and sets path
        rds = gdal.Open(raster_path)

        assert rds
        rb = rds.GetRasterBand(1)
        rgt = rds.GetGeoTransform()

        if nodata_value:
            nodata_value = float(nodata_value)
            rb.SetNoDataValue(nodata_value)
        if vector_path == "":
            print('No vector path provided. Continuing to next layer.')
            continue
        if not os.path.exists(vector_path):
            print(f'{vector_path} does not exist. Continuing to next layer.')
            continue

        # Opens vector file and sets path

        try:
            vds = ogr.Open(vector_path)
            vlyr = vds.GetLayer(0)
        except Exception as e:
            print(repr(e))
            continue

        # Creates an in-memory numpy array of the source raster data covering
        #   the whole extent of the vector layer
        if global_src_extent:
            # use global source extent
            # useful only when disk IO or raster scanning inefficiencies are your limiting factor
            # advantage: reads raster data in one pass
            # disadvantage: large vector extents may have big memory requirements
            src_offset = bbox_to_pixel_offsets(rgt, vlyr.GetExtent())
            src_array = rb.ReadAsArray(*src_offset)

            # calculate new geotransform of the layer subset
            new_gt = (
                (rgt[0] + (src_offset[0] * rgt[1])),
                rgt[1],
                0.0,
                (rgt[3] + (src_offset[1] * rgt[5])),
                0.0,
                rgt[5],
            )

        mem_drv = ogr.GetDriverByName('Memory')
        driver = gdal.GetDriverByName('MEM')

        # Loop through vectors, as many as exist in file
        # Creates new list to contain their stats

        feat = vlyr.GetNextFeature()
        while feat is not None:
            if not global_src_extent:
                # use local source extent
                # fastest option when you have fast disks and well indexed raster (ie tiled Geotiff)
                # advantage: each feature uses the smallest raster chunk
                # disadvantage: lots of reads on the source raster
                src_offset = bbox_to_pixel_offsets(rgt, feat.geometry().GetEnvelope())
                src_array = rb.ReadAsArray(*src_offset)

                # calculate new geotransform of the feature subset
                new_gt = (
                    (rgt[0] + (src_offset[0] * rgt[1])),
                    rgt[1],
                    0.0,
                    (rgt[3] + (src_offset[1] * rgt[5])),
                    0.0,
                    rgt[5],
                )

            # Create a temporary vector layer in memory
            mem_ds = mem_drv.CreateDataSource('out')
            mem_layer = mem_ds.CreateLayer('poly', None, ogr.wkbPolygon)
            mem_layer.CreateFeature(feat.Clone())

            # Rasterize temporary vector layer
            rvds = driver.Create('', src_offset[2], src_offset[3], 1, gdal.GDT_Byte)
            rvds.SetGeoTransform(new_gt)
            gdal.RasterizeLayer(rvds, [1], mem_layer, burn_values=[1])
            rv_array = rvds.ReadAsArray()

            # Mask the source data array with our current feature and get statistics (pixel count)
            #   of masked areas
            # we take the logical_not to flip 0<->1 to get the correct mask effect
            # we also mask out nodata values explictly
            if src_array is None:
                feat = vlyr.GetNextFeature()
                continue
            masked = np.ma.MaskedArray(
                src_array, mask=np.logical_or(src_array == nodata_value, np.logical_not(rv_array))
            )

            # Call different counter functions depending on the raster's source.
            if layer == "nlcd":
                feature_stats = get_nlcd_counts(feat, masked)
            if layer == "agreement_raster":
                feature_stats = get_mask_value_counts(feat, masked)
            if layer == "levees":
                feature_stats = get_levee_counts(feat, masked)
            if layer == "bridges":
                feature_stats = get_bridge_counts(feat, masked)
            if layer == "flood_extent":
                feature_stats = get_nlcd_counts_inside_flood(feat, masked)

            stats.append(feature_stats)

            rvds = None
            mem_ds = None
            feat = vlyr.GetNextFeature()

    vds = None
    rds = None
    if stats != []:
        return stats
    else:
        return []


# Creates and prints dataframe containing desired statistics
if __name__ == "__main__":
    # opts = {'VECTOR': sys.argv[1:], 'RASTER': sys.argv[2:]}
    # stats = zonal_stats(opts['VECTOR'], opts['RASTER'])

    parser = argparse.ArgumentParser(
        description='Computes pixel counts for raster classes within a vector area.'
    )
    parser.add_argument('-v', '--vector', help='Path to vector file.', required=False, default="")
    parser.add_argument(
        '-n', '--nlcd', help='Path to National Land Cover Database raster file.', required=False, default=""
    )
    parser.add_argument('-l', '--levees', help='Path to levees raster file.', required=False, default="")
    parser.add_argument('-b', '--bridges', help='Path to bridges file.', required=False, default="")
    parser.add_argument('-f', '--flood_extent', help='Path to flood extent file.', required=False, default="")
    parser.add_argument('-c', '--csv', help='Path to export csv file.', required=True)
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    vector = args['vector']
    nlcd = args['nlcd']
    levees = args['levees']
    bridges = args['bridges']
    flood_extent = args['flood_extent']

    csv = args['csv']

    raster_path_dict = {'nlcd': nlcd, 'levees': levees, 'bridges': bridges, 'flood_extent': flood_extent}
    stats = zonal_stats(vector, raster_path_dict)

    # Export CSV
    df = pd.DataFrame(stats)
    result = df[(df >= 0).all(axis=1)]
    df2 = pd.DataFrame(result)
    df2.to_csv(csv, index=False)
