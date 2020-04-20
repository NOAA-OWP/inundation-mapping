#!/usr/bin/env python3
# -*- coding: utf-8

import richdem as rd
import sys
import numpy as np
from osgeo import gdal


def SaveGDAL(filename, rda):
    """Save a GDAL file.
     Saves a RichDEM array to a data file in GeoTIFF format.
     If you need to do something more complicated, look at the source of this
     function.
     Args:
         filename (str):     Name of the raster file to be created
         rda      (rdarray): Data to save.
     Returns:
         No Return
    """
    if type(rda) is not rd.rdarray:
        raise Exception("A richdem.rdarray or numpy.ndarray is required!")

    # if not GDAL_AVAILABLE:
    #   raise Exception("richdem.SaveGDAL() requires GDAL.")

    driver    = gdal.GetDriverByName('GTiff')
    data_type = gdal.GDT_Float32 #TODO
    data_set  = driver.Create(filename, xsize=rda.shape[1], ysize=rda.shape[0], bands=1, eType=data_type,options=["BIGTIFF=YES"])
    data_set.SetGeoTransform(rda.geotransform)
    data_set.SetProjection(rda.projection)
    band = data_set.GetRasterBand(1)
    band.SetNoDataValue(rda.no_data)
    band.WriteArray(np.array(rda))
    for k,v in rda.metadata.items():
        data_set.SetMetadataItem(str(k),str(v))

if __name__ == "__main__":

    demFileName = sys.argv[1]
    finalDEMFileName = sys.argv[2]
    epsilonOption = bool(sys.argv[3])


    dem = rd.LoadGDAL(demFileName)

    # dem = dem.astype(np.float64)
    rd.FillDepressions(dem,epsilon=epsilonOption,in_place=True)
    # rd.ResolveFlats(dem,in_place=True)

    SaveGDAL(finalDEMFileName,dem)
