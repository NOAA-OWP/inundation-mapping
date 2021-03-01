#!/usr/bin/env python3
# -*- coding: utf-8

from osgeo import gdal
import numpy as np
import osgeo.ogr
import osgeo.osr
import sys

import cProfile
from tqdm import tqdm
import geopandas as gpd
from shapely.geometry import Point
from raster import Raster
from utils.shared_functions import getDriver

"""
USAGE:
./reachID_grid_to_vector_points.py <flows_grid_IDs raster file> <flows_points vector file> <reachID or featureID>

"""

path = sys.argv[1]
outputFileName = sys.argv[2]
writeOption = sys.argv[3]

#r = gdal.Open(path)
#band = r.GetRasterBand(1)
boolean=Raster(path)

#(upper_left_x, x_size, x_rotation, upper_left_y, y_rotation, y_size) = r.GetGeoTransform()
(upper_left_x, x_size, x_rotation, upper_left_y, y_rotation, y_size) = boolean.gt

#a = band.ReadAsArray().astype(np.float)

# indices = np.nonzero(a != band.GetNoDataValue())
indices = np.nonzero(boolean.array >= 1)

# Init the shapefile stuff..
#srs = osgeo.osr.SpatialReference()
#srs.ImportFromWkt(r.GetProjection())

#driver = osgeo.ogr.GetDriverByName('GPKG')
#shapeData = driver.CreateDataSource(outputFileName)

#layer = shapeData.CreateLayer('ogr_pts', srs, osgeo.ogr.wkbPoint)
#layerDefinition = layer.GetLayerDefn()

#idField = osgeo.ogr.FieldDefn("id", osgeo.ogr.OFTInteger)
#layer.CreateField(idField)

id =[None] * len(indices[0]);points = [None]*len(indices[0])

# Iterate over the Numpy points..
i = 1
for y_index,x_index in tqdm(zip(*indices),total=len(indices[0])):
    x = x_index * x_size + upper_left_x + (x_size / 2) #add half the cell size
    y = y_index * y_size + upper_left_y + (y_size / 2) #to centre the point

    # get raster value
    #reachID = a[y_index,x_index]

    #point = osgeo.ogr.Geometry(osgeo.ogr.wkbPoint)
    #point.SetPoint(0, x, y)
    points[i-1] = Point(x,y)

    #feature = osgeo.ogr.Feature(layerDefinition)
    #feature.SetGeometry(point)
    #feature.SetFID(i)
    if writeOption == 'reachID':
        reachID = a[y_index,x_index]
        id[i-1] = reachID
	#feature.SetField("id",reachID)
    elif (writeOption == 'featureID') |( writeOption == 'pixelID'):
        #feature.SetField("id",i)
        id[i-1] = i
    #layer.CreateFeature(feature)

    i += 1

pointGDF = gpd.GeoDataFrame({'id' : id, 'geometry' : points},crs=boolean.proj,geometry='geometry')
pointGDF.to_file(outputFileName,driver=getDriver(outputFileName),index=False)

print("Complete")
#shapeData.Destroy()
