#!/usr/bin/env python3
# -*- coding: utf-8

# TODO standardize this script

import argparse

import rasterio as rio
from osgeo import osr


osr.UseExceptions()


"""
read fsize ncols nrows ndv xmin ymin xmax ymax cellsize_resx cellsize_resy
    <<< $(./getRasterInfoNative.py <raster.tif>)
"""


def GetExtent(gt, cols, rows):
    '''Return list of corner coordinates from a geotransform

    @type gt:   C{tuple/list}
    @param gt: geotransform
    @type cols:   C{int}
    @param cols: number of columns in the dataset
    @type rows:   C{int}
    @param rows: number of rows in the dataset
    @rtype:    C{[float,...,float]}
    @return:   coordinates of each corner
    '''
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = gt[0] + (px * gt[1]) + (py * gt[2])
            y = gt[3] + (px * gt[4]) + (py * gt[5])
            ext.append([x, y])
            # print x,y
        yarr.reverse()
    return ext


def ReprojectCoords(coords, src_srs, tgt_srs):
    '''Reproject a list of x,y coordinates.

    @type geom:     C{tuple/list}
    @param geom:    List of [[x,y],...[x,y]] coordinates
    @type src_srs:  C{osr.SpatialReference}
    @param src_srs: OSR SpatialReference object
    @type tgt_srs:  C{osr.SpatialReference}
    @param tgt_srs: OSR SpatialReference object
    @rtype:         C{tuple/list}
    @return:        List of transformed [[x,y],...[x,y]] coordinates
    '''
    trans_coords = []
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    for x, y in coords:
        x, y, z = transform.TransformPoint(x, y)
        trans_coords.append([x, y])
    return trans_coords


def get_raster_info(raster):
    # open dataset
    with rio.open(raster) as ds:
        meta = ds.meta

    # fsize = sizeof_fmt(os.path.getsize(sys.argv[1]))
    cols = meta.get("width")
    rows = meta.get("height")
    nodata = meta.get("nodata")

    gt = meta.get("transform")  # (1336251.3593209502, 10.0, 0.0, 657665.814333962, 0.0, -10.0)

    ext = GetExtent(gt.to_gdal(), cols, rows)

    lon1 = ext[0][0]
    lon2 = ext[2][0]
    lat1 = ext[2][1]
    lat2 = ext[0][1]

    # calculate cellsize
    resx = (ext[2][0] - ext[0][0]) / cols
    resy = (ext[0][1] - ext[2][1]) / rows

    # print out RasterInfos
    print(
        cols,
        rows,
        nodata,
        str(lon1),
        str(lat1),
        str(lon2),
        str(lat2),
        "{:.15f}".format(resx),
        "{:.15f}".format(resy),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get raster information")
    parser.add_argument("-r", "--raster", help="Raster file to get information from")

    args = parser.parse_args()

    get_raster_info(**vars(args))
