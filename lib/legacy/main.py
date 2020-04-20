#!/usr/bin/env python3
# -*- coding: utf-8

import subprocess as sp
import os
import sys
import gdal
import ogr
import osr
import numpy as np
from raster import Raster
import geopandas as gpd
from shapely.geometry import Point


############ TO DO ###########
## Clipping: Consider making full mosaic vrt of DEM for country. Then use buffered polygons of SPU's to clip DEM and network
## Resegment NWM streams
## explore gdalwarp multi-threading feature
## NN zonal thalweg min's
## floodstack libraries
## rating curves

##############################
###### parameters ############
##############################
projectDirectory = os.path.join('/home','lqc','Documents','research','nwc','hand')
dataDirectory = os.path.join(projectDirectory,'data','test1')
originalStreamNetwork = os.path.join(dataDirectory,'flows.shp')
originalDEM = os.path.join(dataDirectory,'dem.tif')
waterbodies = os.path.join(dataDirectory,'waterbodies.shp')
dropZburnValue = 10000
streamIdentifierColumnString = 'COMID'
processors = 4
projection = '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'

################################################
#### reproject files ############################
################################################

## outputs
# reprojectedDEM = os.path.join(dataDirectory,"{}_projected.vrt".format(os.path.basename(originalDEM).split('.')[0]))
# reprojectedNetwork = os.path.join(dataDirectory,"{}_projected.shp".format(os.path.basename(originalStreamNetwork).split('.')[0]))

# reproject DEM
# gdalwarp -overwrite -r bilinear -t_srs '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs' -ot Float32 -of VRT -multi dem.tif dem_projected.vrt
# sp.call(['gdalwarp','-r','bilinear','-t_srs',projection,originalDEM,reprojectedDEM])

# ogr2ogr -overwrite flows_projected.gpkg -t_srs '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs' flows.shp
# sp.call(['ogr2ogr',reprojectedNetwork,originalStreamNetwork])

#######################################################
##### rasterize stream network: burning values ######
#######################################################

# burn COMID's
streamNetworkRaster_fileName = os.path.join(dataDirectory,'flows_grid_reaches.tif')
# if os.isfile(streamNetworkRaster_fileName): os.remove(streamNetworkRaster_fileName)
# gdal_rasterize -ot Int32 -l 'flows' -a 'COMID' -a_nodata 0 -init 0 -te -183340.502 602718.776 15036.923  837943.385 -ts $(gdal_size dem_projected.vrt) flows_projected.gpkg/flows.shp flows_grid_reaches.tif

# sp.call(['gdal_rasterize','-burn','2','-a',streamIdentifierColumnString,'-a_nodata',str(thalwegNDV),'-init',str(thalwegNDV),'-te',str(xmin),str(ymin),str(xmax),str(ymax),'-ts',str(xsize),str(ysize)])

# burn boolean stream grid
# streamNetworkRasterBoolean_fileName = os.path.join(dataDirectory,'flows_grid_boolean.tif')
# if os.isfile(streamNetworkRasterBoolean_fileName): os.remove(streamNetworkRasterBoolean_fileName)
# gdal_rasterize -ot Byte -burn 1 -init 0 -te -183340.502 602718.776 15036.923  837943.385 -ts $(gdal_size dem_projected.vrt) flows_projected.gpkg/flows.shp flows_grid_boolean.tif


#######################################################################
#### clip or extract or get indices for individual reaches files ######
#######################################################################



#######################################################################
######## generate vector points of thalweg pixel centroids ############
#######################################################################

"""
# read thalwegRaster
streamNetworkRaster = Raster(streamNetworkRaster_fileName)
ulx, xres, _, uly, _, yres = streamNetworkRaster.gt

# get unique HydroID's
uniqueHydroIDs = iter(np.unique(streamNetworkRaster.array[streamNetworkRaster.array != streamNetworkRaster.ndv]))

fieldName = 'id'
fieldType = ogr.OFTString
shpDriver = ogr.GetDriverByName("ESRI Shapefile")
srs = osr.SpatialReference()
srs.ImportFromWkt(projection)
idField = ogr.FieldDefn(fieldName, fieldType)

# for each reach, write a file that
for hydroID in uniqueHydroIDs:

	print(hydroID)

	# get indices of thalweg pixels
	thalwegRaster_indices = np.where(streamNetworkRaster.array == hydroID)
	thalwegRaster_rowIndices = thalwegRaster_indices[0]
	thalwegRaster_colIndices = thalwegRaster_indices[1]
	IDs = np.array(range(len(thalwegRaster_colIndices)))

	xCoords = ulx + (thalwegRaster_indices[1] * xres) + (xres/2)
	yCoords = uly + (thalwegRaster_indices[0] * yres) + (yres/2)

	# Input data
	# fieldValue = '1'
	outSHPfn = os.path.join(dataDirectory,'flows_vector_points_{}.shp'.format(hydroID))

	# Create the output shapefile
	if os.path.exists(outSHPfn):
		shpDriver.DeleteDataSource(outSHPfn)
	outDataSource = shpDriver.CreateDataSource(outSHPfn)

	outLayer = outDataSource.CreateLayer(outSHPfn, srs,geom_type=ogr.wkbPoint )

	outLayer.CreateField(idField)

	# print(xCoords,yCoords)
	id = 1
	for xCoord,yCoord in zip(xCoords,yCoords):
		#create point geometry
		point = ogr.Geometry(ogr.wkbPoint)
		point.AddPoint(xCoord,yCoord)

		# Create the feature and set values
		featureDefn = outLayer.GetLayerDefn()
		outFeature = ogr.Feature(featureDefn)
		outFeature.SetGeometry(point)
		outFeature.SetField(fieldName, id)
		outLayer.CreateFeature(outFeature)
		id += 1
	outFeature = None
	exit()
"""


#mpiexec -n ' + inputProc + ' flowdircond -p  -z   -zfdc

def grid_to_points(streamNetworkRaster_fileName,points):
	"""Takes boolean grid to points"""

	streamNetworkRaster = Raster(streamNetworkRaster_fileName)
	uniqueHydroIDs = iter(np.unique(streamNetworkRaster.array[streamNetworkRaster.array != streamNetworkRaster.ndv]))

	ulx, xres, _, uly, _, yres = streamNetworkRaster.gt

	for reach in uniqueHydroIDs:

		thalwegRaster_indices = np.where(streamNetworkRaster.array == reach)
		thalwegRaster_rowIndices = thalwegRaster_indices[0]
		thalwegRaster_colIndices = thalwegRaster_indices[1]

		xCoords = ulx + (thalwegRaster_indices[1] * xres) + (xres/2)
		yCoords = uly + (thalwegRaster_indices[0] * yres) + (yres/2)

		points = []
		for x,y in zip(xCoords,yCoords):
			points.append(Point([x,y]))



grid_to_points('flows_grid_reaches.vrt','points.shp')






#######################################################################
######################### drop thalweg points ########################
#######################################################################

## GET WBD POLYGON ##

## BUFFER WBD POLYGON ##

## REPROJECT DEM ##
# gdalwarp -multi -overwrite -r bilinear -t_srs '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs' -ot Float32 -of VRT -multi dem.tif dem_projected.vrt

## REPROJECT STREAMS ##
# ogr2ogr -overwrite flows_projected.gpkg -t_srs '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs' flows.shp

## REPROJECT BOTH WBD POLYGONS ##

## IDENTIFY INLETS ##

## IDENTIFY INLET POINTS ##

## RASTERIZE INLETS ##

## REPROJECT WEIGHTS ##
# gdalwarp -overwrite -r near -t_srs '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs' -ot Byte -of VRT -multi 120903-weights.tif weights_projected.vrt

## BURN REACH IDENTIFIERS ##
# gdal_rasterize -ot Int32 -l 'flows' -a 'COMID' -a_nodata 0 -init 0 -te -183340.502 602718.776 15036.923  837943.385 -ts $(gdal_size dem_projected.vrt) flows_projected.gpkg/flows.shp flows_grid_reaches.tif

## BURN REACH BOOLEAN (1 & 0) ##
# gdal_calc.py --overwrite -A flows_grid_reaches.tif --calc="A>0" --outfile="flows_grid_boolean.tif" --NoDataValue=0
# gdal_edit.py -unsetnodata flows_grid_boolean.tif

## BURN NEGATIVE ELEVATIONS STREAMS ##
# gdal_calc.py --overwrite -A dem_projected_clipped.vrt -B flows_grid_boolean.tif --calc="A-10000*B" --outfile="dem_burned.tif" --NoDataValue=-3.40282346638528898e+38

## PIT REMOVE DEM ##
# mpiexec -n 3 /usr/local/taudem/pitremove -z dem_burned.tif -fel dem_filled.tif

## D8 FLOW DIR ##
# mpiexec -n 3 /usr/local/taudem/d8flowdir -fel dem_filled.tif -p flowdir_d8.tif

## MASK D8 FLOW DIR FOR STREAMS ONLY ###
# gdal_calc.py --overwrite -A flowdir_d8.tif -B flows_grid_boolean.tif --calc="A/B" --outfile="flowdir_d8_flows.tif" --NoDataValue=0

## FLOW CONDITION STREAMS ##
# flowdircond -p flowdir_d8_flows.tif -z dem_projected_clipped.vrt -zfdc dem_reconditioned.tif
# mpiexec -n 3 flowdircond -p flowdir_d8_flows.tif -z dem_projected_clipped.vrt -zfdc dem_reconditioned.tif

######################################

##### ROUND 2 ##############

## PIT FILL ##
# mpiexec -n 3 /usr/local/taudem/pitremove -z dem_reconditioned.tif -fel dem_filled_2.tif

## DINF FLOW DIR ##
# mpiexec -n 3 /usr/local/taudem/dinfflowdir -fel dem_filled_2.tif -ang flowdir_dinf.tif -slp flowdir_dinf_slp.tif

## D8 FLOW DIR ##
# mpiexec -n 3 /usr/local/taudem/d8flowdir -fel dem_filled_2.tif -p flowdir_d8_2.tif

## SUSTITUTE PROPER THALWEG FLOWDIR ##
# gdal_calc.py --overwrite -A flows_grid_boolean.tif --calc="(~A)+2" --outfile="nonFlows_grid_boolean.tif"
# gdal_calc.py --overwrite -A nonFlows_grid_boolean.tif -B flowdir_d8_2.tif --calc="A*B" --outfile="flowdir_d8_filled.tif" --NoDataValue=0
# unset both nodata values
# gdal_calc.py --overwrite -A flowdir_d8_filled.tif -B flowdir_d8_flows.tif --calc="A+B" --outfile="flowdir_d8_filled_thalweg.tif" --NoDataValue=0
# reset nodatavalues

## ACCUMULATIONS FROM WEIGHTS ##
# mpiexec -n 3 /usr/local/taudem/aread8 -p flowdir_d8_2.tif -ad8 flowaccum_d8.tif -wg headwater_points_raster.tif -nc
# mpiexec -n 3 /usr/local/taudem/aread8 -p flowdir_d8_filled_thalweg.tif -ad8 flowaccum_d8_filled_thalweg.tif -wg headwater_points_raster.tif -nc

## THRESHOLD ACCUMULATIONS ##
# mpirun -n 3 threshold -ssa flowaccum_d8_filled_thalweg.tif -src streamPixels.tif -thresh 1

## DINF DISTANCE DOWN ##
# mpiexec -n 3 /usr/local/taudem/dinfdistdown -ang flowdir_dinf.tif  -fel dem_filled_2.tif -src streamPixels.tif -dd distDown_dinf.tif -m ave v
# mpiexec -n 3 /usr/local/taudem/dinfdistdown -ang flowdir_dinf.tif  -fel dem_reconditioned.tif -src streamPixels.tif -dd distDown_dinf.tif -m ave v

## CLIP DIST DOWN TO WBD ##


## WATERSHEDS ##
# mpiexec -n 3 streamnet -p flowdir_d8_2.tif  -fel dem_filled_2.tif -ad8 flowaccum_d8.tif -src streamPixels.tif -ord streamOrder.tif -tree treeFile.txt -coord coordFile.txt -w watershed.tif -net outputStreams.shp





"""

# make an attribute for HydroID

# for every HydroID make each thalweg pixel point a unique 'id' feature

# write out to vector points


#######################################################################
######################### drop thalweg points ########################
#######################################################################


negativeThalwegRaster_fileName = os.path.join(dataDirectory,'negativeThalwegRaster.tif')

# load reprojected dem
reprojectedDEM_buffer = gdal.Open(reprojectedDEM)
reprojectedDEM_array = reprojectedDEM_buffer.ReadAsArray()

# load thalwegRaster
thalwegRaster_buffer = gdal.Open(thalwegRaster)
thalwegRaster_array = thalwegRaster.ReadAsArray()

# drop all thalweg pixels with indices above by dropZburnValue
thalwegBoolean = thalwegRaster_array != thalwegNDV
negativeThalwegRaster = reprojectedDEM_array[thalwegBoolean] - dropZburnValue
reprojectedDEM_array[thalwegBoolean] = negativeThalwegRaster

# write to burnedDEM



#######################################################################
######################### pitfill #####################################
#######################################################################

pitfilledBurnedDEM = os.path.join(dataDirectory,'pitfilledBurnedDEM.tif')

# pitfill burnedDEM using pitremove
sp.cal(['mpiexec','-n',str(processors),'pitremove'])


#######################################################################
######################## Flow Directions ##############################
#######################################################################

d8FlowDirections = os.path.join(dataDirectory,'d8FlowDirections.tif')

# generate D8 flowdirections using d8flowdir
sp.cal(['mpiexec','-n',str(processors),'d8flowdir'])


## DO DINF TOO
sp.cal([mpiexec -n <number of processes> DinfDistDown -ang <angfile> -fel <felfile> -src <srcfile> [ -wg <weightfile>] -dd <ddfile> [ -m ave h] [ -nc]])

#######################################################################
######################## Thalweg Adjustments ###########################
#######################################################################

pitfilled_negativeThalwegRaster = os.path.join(dataDirectory,'pitfilled_negativeThalwegRaster.tif')

# load burned thalweg-only DEM
# negativeThalwegRaster = Raster(negativeThalwegRaster_fileName)

#############################
# FOR NN Thalweg adjusting
# input thalwegRaster
# get coordinates of thalweg raster and DEM
# create nearest neighbor map
# select min of NN zone
##################################

# pitfill thalweg only dem
sp.call(['mpiexec','-n',str(processors),'pitremove'])


# find readjustment factor by finding lowest headwater neighborhood pixel in original DEM
############# AHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH ###########################

# readjust thalweg-only dem by the factor above

# put thalweg-only DEM into original DEM


#######################################################################
############## Derive pixel based catchments per reach ################
#######################################################################

# read thalwegRaster
thalwegRaster_buffer = gdal.Open(thalwegRaster)
thalwegRaster_array = thalwegRaster.ReadAsArray()
ulx, xres, _, uly, _, yres = thalwegRaster.GetGeoTransform()

# get unique HydroID's
uniqueHydroIDs = np.unique(thalwegRaster_array[thalwegRaster_array != thalwegNDV])

# for each HydroID run gagewatershed so that each pixel ('id value') has it's own catchment
for hydroID in uniqueHydroIDs:
	sp.call(['mpiexec','-n',str(processors),'pitremove'])


# clip output of gagewatershed to extent of watershed????
# change precision of


#############################################################################
################# Calculate Relative Elevations & flood stack library #######
#############################################################################

# read thalwegRaster
# thalwegRaster_buffer = gdal.Open(thalwegRaster)
# thalwegRaster_array = thalwegRaster.ReadAsArray


uniqueHydroIDs = np.unique(thalwegRaster_array[thalwegRaster_array != thalwegNDV])

for hydroID in uniqueHydroIDs:

	# input pixelWatersheds

	# for each pixel
		# calculate vertical distance for each pixel catchment

	# bring negatives to zero

	### DO LATER ########
	# for each stageValue in flood stack library
		# threshold HAND raster
		# get boolean of extents of inundation with binary erosion
		# get centroid coordinates of extents
		# generate polygon with centroids of extents
		# write out polygon

	# write out HAND raster for the reach
"""











############################################################################







"""
# read thalwegRaster
streamNetworkRaster = Raster(streamNetworkRaster_fileName)
ulx, xres, _, uly, _, yres = streamNetworkRaster.gt

# get unique HydroID's
uniqueHydroIDs = iter(np.unique(streamNetworkRaster.array[streamNetworkRaster.array != streamNetworkRaster.ndv]))

fieldName = 'id'
fieldType = ogr.OFTString
shpDriver = ogr.GetDriverByName("ESRI Shapefile")
srs = osr.SpatialReference()
srs.ImportFromWkt(projection)
idField = ogr.FieldDefn(fieldName, fieldType)

# for each reach, write a file that
for hydroID in uniqueHydroIDs:

	print(hydroID)

	# get indices of thalweg pixels
	thalwegRaster_indices = np.where(streamNetworkRaster.array == hydroID)
	thalwegRaster_rowIndices = thalwegRaster_indices[0]
	thalwegRaster_colIndices = thalwegRaster_indices[1]
	IDs = np.array(range(len(thalwegRaster_colIndices)))

	xCoords = ulx + (thalwegRaster_indices[1] * xres) + (xres/2)
	yCoords = uly + (thalwegRaster_indices[0] * yres) + (yres/2)

	# Input data
	# fieldValue = '1'
	outSHPfn = os.path.join(dataDirectory,'flows_vector_points_{}.shp'.format(hydroID))

	# Create the output shapefile
	if os.path.exists(outSHPfn):
		shpDriver.DeleteDataSource(outSHPfn)
	outDataSource = shpDriver.CreateDataSource(outSHPfn)

	outLayer = outDataSource.CreateLayer(outSHPfn, srs,geom_type=ogr.wkbPoint )

	outLayer.CreateField(idField)

	# print(xCoords,yCoords)
	id = 1
	for xCoord,yCoord in zip(xCoords,yCoords):
		#create point geometry
		point = ogr.Geometry(ogr.wkbPoint)
		point.AddPoint(xCoord,yCoord)

		# Create the feature and set values
		featureDefn = outLayer.GetLayerDefn()
		outFeature = ogr.Feature(featureDefn)
		outFeature.SetGeometry(point)
		outFeature.SetField(fieldName, id)
		outLayer.CreateFeature(outFeature)
		id += 1
	outFeature = None
	exit()
"""
