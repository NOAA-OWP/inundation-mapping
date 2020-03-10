#!/usr/bin/env python3
# -*- coding: utf-8

from osgeo import ogr
import os
import json
import sys
from tqdm import tqdm
from glob import glob

channelPropsDir = sys.argv[1]
flowsFile = sys.argv[2]
outputFlows = sys.argv[3]

files = glob(os.path.join(channelPropsDir,'*.json'))

flows = flowsFile
driver = ogr.GetDriverByName("GPKG")
dataSource = driver.Open(flows, 0)
inLayer = dataSource.GetLayer()

# Create the output LayerS
outShapefile = os.path.join( os.path.split( flows )[0], outputFlows )
outDriver = ogr.GetDriverByName("GPKG")

# Remove output shapefile if it already exists
if os.path.exists(outShapefile):
    outDriver.DeleteDataSource(outShapefile)

# Create the output shapefile
outDataSource = outDriver.CreateDataSource(outShapefile)
out_lyr_name = os.path.splitext( os.path.split( outShapefile )[1] )[0]
outLayer = outDataSource.CreateLayer( out_lyr_name, srs = inLayer.GetSpatialRef(),geom_type=inLayer.GetLayerDefn().GetGeomType() )

# Add input Layer Fields to the output Layer
inLayerDefn = inLayer.GetLayerDefn()
for i in range(0, inLayerDefn.GetFieldCount()):
    fieldDefn = inLayerDefn.GetFieldDefn(i)
    # print(fieldDefn.GetNameRef())
    outLayer.CreateField(fieldDefn)

outLayer.CreateField(ogr.FieldDefn("HUC6", ogr.OFTInteger))


outLayerDefn = outLayer.GetLayerDefn()

id_to_index = dict()
for index in tqdm(range(1,inLayer.GetFeatureCount()+1)):
    id = inLayer.GetFeature(index).GetField(1)
    id_to_index[id] = index


for file in tqdm(files):
    huc6code = int(os.path.split(file)[1].split("_")[0])

    with open(file) as f:
        ids_keys = json.load(f).keys()
        channelProperties = set()
        for i in ids_keys:

            try:
                channelProperties.add(int(i))
            except ValueError:
                continue

    for count,id in enumerate(channelProperties):

        # newDataSource = driver.Open(flows, 0)
        # filteredInLayer = newDataSource.GetLayer()
        # print("Prefilter Count {}".format(filteredInLayer.GetFeatureCount()))
        # filteredInLayer.SetAttributeFilter("ID='{}'".format(id))
        # print("Post Filter {}".format(filteredInLayer.GetFeatureCount()))
        # Get the input Feature
        try:
            index = id_to_index[id]
        except KeyError:
            continue

        inFeature = inLayer.GetFeature(index)# Create output Feature
        outFeature = ogr.Feature(outLayerDefn)


        # print(outLayerDefn.GetFieldDefn(1).GetNameRef(),inFeature.GetField(1))

        # Add field values from input Layer
        for i in range(0, outLayerDefn.GetFieldCount()):
            # print(inFeature.GetField(i))
            outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), inFeature.GetField(i))

        outFeature.SetField('HUC6',huc6code)
        # Set geometry as centroid
        geom = inFeature.GetGeometryRef()
        # centroid = geom.Centroid()
        outFeature.SetGeometry(geom)

        # Add new feature to output Layer
        outLayer.CreateFeature(outFeature)

        inFeature = None
        outFeature = None


# Save and close DataSources
inDataSource = None
outDataSource = None
