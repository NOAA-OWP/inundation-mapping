#!/usr/bin/env python3
# -*- coding: utf-8

# TODO standardize this script

import os.path
import sys

from osgeo import gdal, osr


gdal.UseExceptions()


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


# get file size function
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


# open dataset
ds = gdal.Open(sys.argv[1])
fsize = sizeof_fmt(os.path.getsize(sys.argv[1]))
cols = ds.RasterXSize
rows = ds.RasterYSize
nodata = ds.GetRasterBand(1).GetNoDataValue()
# stats = ds.GetRasterBand(1).GetStatistics(True, True)

gt = ds.GetGeoTransform()
ext = GetExtent(gt, cols, rows)

src_srs = osr.SpatialReference()
src_srs.ImportFromWkt(ds.GetProjection())
tgt_srs = src_srs.CloneGeogCS()

geo_ext = ReprojectCoords(ext, src_srs, tgt_srs)
lon1 = ext[0][0]
lon2 = ext[2][0]
lat1 = ext[2][1]
lat2 = ext[0][1]

# calculate cellsize
resx = (ext[2][0] - ext[0][0]) / cols
resy = (ext[0][1] - ext[2][1]) / rows

# print out RasterInfos
print(
    fsize,
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
# print stats[0],
# print stats[1],
# print stats[2],
# print stats[3],
# print "\"" + str(lat1) + "," + str(lon1) + "," + str(lat2) + "," + str(lon2) + "\""
# print str(lon1),
# print str(lat1),
# print str(lon2),
# print str(lat2),

# if resx < 0.01: # unit is degree
# 	print "%.15f"  % (resx),
# 	print "%.15f" % (resy)
# else:
# 	print "%.15f" % (resx),
# 	print "%.15f" % (resy)

# close dataset
ds = None
