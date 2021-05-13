#!/usr/bin/env python3

import numpy as np
import osgeo.ogr
import osgeo.osr
import sys
from tqdm import tqdm
import geopandas as gpd
from shapely.geometry import Point
import rasterio
from utils.shared_functions import getDriver

"""
USAGE:
./reachID_grid_to_vector_points.py <flows_grid_IDs raster file> <flows_points vector file> <reachID or featureID>

"""

path = sys.argv[1]
outputFileName = sys.argv[2]
writeOption = sys.argv[3]

boolean = rasterio.open(path,'r')

(upper_left_x, x_size, x_rotation, upper_left_y, y_rotation, y_size) = boolean.get_transform()
indices = np.nonzero(boolean.read(1) >= 1)

id =[None] * len(indices[0]);points = [None]*len(indices[0])

# Iterate over the Numpy points..
i = 1
for y_index,x_index in tqdm(zip(*indices),total=len(indices[0])):
    x = x_index * x_size + upper_left_x + (x_size / 2) # add half the cell size
    y = y_index * y_size + upper_left_y + (y_size / 2) # to center the point
    points[i-1] = Point(x,y)

    if writeOption == 'reachID':
        reachID = a[y_index,x_index]
        id[i-1] = reachID

    elif (writeOption == 'featureID') |( writeOption == 'pixelID'):
        id[i-1] = i

    i += 1

pointGDF = gpd.GeoDataFrame({'id' : id, 'geometry' : points},crs=boolean.proj,geometry='geometry')
pointGDF.to_file(outputFileName,driver=getDriver(outputFileName),index=False)

print("Complete")
