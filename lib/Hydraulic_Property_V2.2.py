# ##########################################################
#    FILENAME:   Hydraulic_Property_Calculation.py
#    VERSION:    2.1b
#    SINCE:      2016-08-09
#    AUTHOR:     Xing Zheng - zhengxing@utexas.edu
#    Description:This program is designed for evaluating
#    NHD Flowline Hydraulic Properties from HAND Raster.
#    The input should be (1) Catchment shapefile;
#                        (2) Flowline shapefile;
#                        (3) HAND raster generated
#                            from NHD-HAND.
# ##########################################################

import sys
from osgeo import gdal, ogr
import os
import numpy as np
import numpy.ma as ma
from math import sqrt
import shutil
import netCDF4 as NET
import tqdm

def Shapefile_Attribute_Reader(catchmentgpkg, flowlinegpkg):
    """Find the shared COMID/FEATURE between flowline feature class
    and catchment feature class and read some flowline attributes
    needed to calculate channel hydraulic properties
    """
    # Read catchment shapefile
    driver = ogr.GetDriverByName("GPKG")
    dataSource = driver.Open(catchmentgpkg, 0)
    layer = dataSource.GetLayer()
    # Get catchment FEATUREID list
    catchment_COMID = []
    for catchment in layer:
        catchment_COMID.append(catchment.GetField("FEATUREID"))
    # Get flowline COMID list, reach length, and slope
    flowline_COMID = []
    RiverLength_dic = {}
    Slope_dic = {}
    dataSource = driver.Open(flowlinegpkg, 0)
    layer = dataSource.GetLayer()
    for flowline in layer:
        flowline_COMID.append(flowline.GetField("COMID"))
        RiverLength_dic[str(flowline.GetField("COMID"))] = flowline.GetField("LENGTHKM")
        Slope_dic[str(flowline.GetField("COMID"))] = flowline.GetField("SLOPE")
    # Find the intersection between catchment FEATUREID set
    # and flowline COMID set
    COMIDlist = list(set(catchment_COMID).intersection(flowline_COMID))

    return COMIDlist, RiverLength_dic, Slope_dic


def HANDClipper(catchmentRaster,catchmentgpkg, flowlinegpkg, handtif,
                COMIDlist, RiverLength_dic, Slope_dic,
                Hmax, dh, roughness, range_sh, vari_comid,
                vari_length, vari_slope, vari_roughness,
                vari_cw, vari_sa, vari_wa, vari_wp, vari_ba,
                vari_hr, vari_volume, vari_discharge):
    """Create HAND Raster for every catchment in the study area
    """
    # Set output path
    catchmentFolder = os.path.join(os.path.dirname(os.path.abspath(handtif)), "Catchment")
    # Clean up existing folders and files
    if os.path.exists(catchmentFolder):
        shutil.rmtree(catchmentFolder)
    # Create output folder
    if not os.path.exists(catchmentFolder):
        os.mkdir(catchmentFolder)
    # Read catchment shapefile
    inDriver = ogr.GetDriverByName("GPKG")
    inDataSource = inDriver.Open(catchmentgpkg, 0)
    inLayer = inDataSource.GetLayer()
    spatialRef = inLayer.GetSpatialRef()
    j = 0

    # get feature indices
    if inLayer.GetFeature(0) is None:
        # gpkg files are 1-indexed
        featureRange = range(1, inLayer.GetFeatureCount()+1)
    else:
        # shp files are 0-indexed
        featureRange = range(0, inLayer.GetFeatureCount())

    for i in tqdm.tqdm(featureRange):

        # Get an input catchment feature
        inFeature = inLayer.GetFeature(i)
        COMID = inFeature.GetField('FeatureID')
        if COMID in COMIDlist:
            vari_comid[j] = COMID
            vari_length[j] = RiverLength_dic[str(COMID)]*1000
            vari_slope[j] = Slope_dic[str(COMID)]
            vari_roughness[j] = roughness
            # Set individual catchment shapefile path
            outShapefile = os.path.join(catchmentFolder, str(COMID)+".gpkg")
            outDriver = ogr.GetDriverByName("GPKG")
            # Remove output shapefile if it already exists
            if os.path.exists(outShapefile):
                outDriver.DeleteDataSource(outShapefile)
            # Create the output shapefile
            outDataSource = outDriver.CreateDataSource(outShapefile)
            outLayer = outDataSource.CreateLayer(str(COMID), spatialRef,
                                                 geom_type=ogr.wkbPolygon)
            # Add input layer fields to the output Layer
            inLayerDefn = inLayer.GetLayerDefn()
            for k in range(0, inLayerDefn.GetFieldCount()):
                fieldDefn = inLayerDefn.GetFieldDefn(k)
                outLayer.CreateField(fieldDefn)
            # Get the output layer's feature definition
            outLayerDefn = outLayer.GetLayerDefn()
            outFeature = ogr.Feature(outLayerDefn)
            # Add field values from input layer
            for m in range(0, outLayerDefn.GetFieldCount()):
                outFeature.SetField(outLayerDefn.GetFieldDefn(m).GetNameRef(),
                                    inFeature.GetField(m))
            # Get polygon geometry
            geom = inFeature.GetGeometryRef()
            outFeature.SetGeometry(geom)
            # Add new feature to output Layer
            outLayer.CreateFeature(outFeature)
            # Close output file
            outDataSource.Destroy()
            # Read single catchment shapefile
            infile = outShapefile
            inSource = inDriver.Open(infile, 0)
            inlayer = inSource.GetLayer()
            extent = inlayer.GetExtent()
            dtsdir = handtif

            # create a masked hand raster
            dts_masked_file = os.path.join(catchmentFolder,
                                        str(COMID) + "hand_masked.tif")
            if os.path.exists(dts_masked_file):
                os.remove(dts_masked_file)

            command_dd = "gdal_calc.py --quiet -A {0} -B {1} --calc='A*(B=={2})+((B!={2})*{3})' --NoDataValue={3} --outfile={4}".format(dtsdir,catchmentRaster,COMID,-32768,dts_masked_file)

            os.system(command_dd)

            # clip the masked raster to shapefile max extents
            dts_out_file = os.path.join(catchmentFolder,
                                        str(COMID) + "hand.tif")
            if os.path.exists(dts_out_file):
                os.remove(dts_out_file)

            # Clip HAND raster with single catchment polygon boundary
            command_dd = "gdalwarp -q -te " + str(extent[0]) + " " + \
                         str(extent[2]) + " " + str(extent[1]) + " " + \
                         str(extent[3]) + " -dstnodata -32768 " + dts_masked_file + \
                         " " + dts_out_file

            os.system(command_dd)

            if os.path.exists(dts_masked_file):
                os.remove(dts_masked_file)

            if os.path.exists(dts_out_file):
                # Calculate flood volume and water surface area
                Return = Volume_SA_Calculation(COMID, dts_out_file,
                                               dh, Hmax)
                Volume = np.asarray(Return[0])
                SAlist = np.asarray(Return[1])
                vari_volume[j] = Volume
                vari_sa[j] = SAlist
                Depth = np.arange(0, Hmax, dh)
                # Calculate channel top width, wet area,
                # wetted perimeter, and bed area
                if np.any(Volume):
                    RiverLength = RiverLength_dic[str(COMID)]
                    Return_Result = TW_WA_WP_BA_Calculation(Depth, RiverLength,
                                                            Volume, SAlist, dh)
                    TWlist = np.asarray(Return_Result[0])
                    WAlist = np.asarray(Return_Result[1])
                    WPlist = np.asarray(Return_Result[2])
                    BAlist = np.asarray(Return_Result[3])
                    vari_cw[j] = TWlist
                    vari_wa[j] = WAlist
                    vari_wp[j] = WPlist
                    vari_ba[j] = BAlist
                    # Calculate hydraulic radius and discharge
                    Slope = Slope_dic[str(COMID)]
                    Return_Result = HR_Q_Calculation(WAlist, WPlist,
                                                     Slope, roughness)
                    HRlist = Return_Result[0]
                    Qlist = Return_Result[1]
                    vari_hr[j] = HRlist
                    vari_discharge[j] = Qlist
            j += 1

    # remove output files if desired
    if os.path.exists(catchmentFolder):
        shutil.rmtree(catchmentFolder)


def Volume_SA_Calculation(COMID, HANDRaster, dh, Hmax):
    """Calculate flood volume and water surface area
    """
    # Read HAND raster
    dts_ds = gdal.Open(HANDRaster)
    band_dts = dts_ds.GetRasterBand(1)
    nodata_dts = band_dts.GetNoDataValue()
    array_dts = band_dts.ReadAsArray()
    arraydts = ma.masked_where(array_dts == nodata_dts, array_dts)
    Volumelist = []
    SAlist = []
    for H in np.arange(0, Hmax, dh):
        # Subtract stage height from hand raster
        dts_value = arraydts-H
        # Find dts<0
        dts_less_height = dts_value <= 0
        # Count number of cell has negative value
        count_cell = dts_less_height.sum()
        cell_height = dts_value[dts_less_height]*(-1)
        # Calculate flood volume for stage height H
        volume_in = 10*10*cell_height
        volume = volume_in.sum()
        # Calculate water surface area for stage height H
        SurfaceArea = count_cell*10*10
        if type(volume) is not np.float32:
            Volumelist.append(0.0)
            SAlist.append(0.0)
        else:
            Volumelist.append(volume)
            SAlist.append(SurfaceArea)
    ReturnResult = [Volumelist, SAlist]
    return ReturnResult


def TW_WA_WP_BA_Calculation(Depth, RiverLength, Volume, SAlist, dh):
    """Calculate channel top width, wet area, wetted perimeter, and
    bed area
    """
    Volume = Volume - Volume[0]
    DDepth = np.diff(Depth)
    TotalArea = Volume/RiverLength/1000
    TWlist = SAlist/RiverLength/1000
    WAlist = list(TotalArea)
    TWlist = list(TWlist)
    WPlist = []
    BAlist = []
    WetPerimeter = 0
    BedArea = 0
    for i in range(len(TWlist)):
        if i == 0:
            WetPerimeter = TWlist[i]
            WPlist.append(WetPerimeter)
        else:
            WetPerimeter += 2*sqrt(DDepth[i-1]*DDepth[i-1] +
                                   ((TWlist[i]-TWlist[i-1])/2)**2)
            WPlist.append(WetPerimeter)
        BedArea = WetPerimeter*RiverLength
        BAlist.append(BedArea)
    ReturnResult = [TWlist, WAlist, WPlist, BAlist]

    return ReturnResult


def HR_Q_Calculation(WAlist, WPlist, Slope, roughness):
    """ Calculate hydraulic radius and discharge
    """
    HRlist = []
    Qlist = []
    HRlist.append(0)
    for i in range(1, len(WAlist)):
        HydraulicRadius = WAlist[i]/WPlist[i]
        HRlist.append(HydraulicRadius)
    Qlist.append(0)
    for i in range(1, len(WAlist)):
        if Slope >= 0:
            Discharge = WAlist[i]*(HRlist[i]**(2.0/3))*sqrt(Slope)/roughness
            Qlist.append(Discharge)
        else:
            Qlist.append(0)
    ReturnResult = [HRlist, Qlist]

    return ReturnResult


def main():

    catchmentgpkg = str(sys.argv[2])
    flowlinegpkg = str(sys.argv[4])
    handtif = str(sys.argv[6])
    nefcdf_name = str(sys.argv[8])
    catchmentRaster = str(sys.argv[10])
    Hmax = 25
    dh = 0.3048
    roughness = 0.05
    COMIDlist, RiverLength_dic, Slope_dic = Shapefile_Attribute_Reader(catchmentgpkg, flowlinegpkg)
    netcdf_file = NET.Dataset(nefcdf_name,"w",format='NETCDF4')
    range_sh = int(np.ceil(Hmax/dh))
    dim_comid = netcdf_file.createDimension('COMID',len(COMIDlist))
    dim_sh = netcdf_file.createDimension('StageHeight',range_sh)
    vari_sh = netcdf_file.createVariable('StageHeight','f',dimensions=("StageHeight"))
    vari_sh.units = 'meters'
    vari_comid = netcdf_file.createVariable('COMID','i8',dimensions=("COMID"))
    vari_length = netcdf_file.createVariable('Length','f',dimensions=("COMID"))
    vari_length.units = 'meters'
    vari_slope = netcdf_file.createVariable('Slope','f',dimensions=("COMID"))
    vari_roughness = netcdf_file.createVariable('Roughness','f',dimensions=("COMID"))
    vari_cw = netcdf_file.createVariable('Width','f',dimensions=("COMID","StageHeight"))
    vari_cw.units = 'meters'
    vari_sa = netcdf_file.createVariable('SurfaceArea','f',dimensions=("COMID","StageHeight"))
    vari_sa.units = 'square meters'
    vari_wa = netcdf_file.createVariable('WetArea','f',dimensions=("COMID","StageHeight"))
    vari_wa.units = 'square meters'
    vari_wp = netcdf_file.createVariable('WettedPerimeter','f',dimensions=("COMID","StageHeight"))
    vari_wp.units = 'meters'
    vari_ba = netcdf_file.createVariable('BedArea','f',dimensions=("COMID","StageHeight"))
    vari_ba.units = 'square meters'
    vari_hr = netcdf_file.createVariable('HydraulicRadius','f',dimensions=("COMID","StageHeight"))
    vari_hr.units = 'meters'
    vari_volume = netcdf_file.createVariable('Volume','f',dimensions=("COMID","StageHeight"))
    vari_volume.units = 'cubic meters'
    vari_discharge = netcdf_file.createVariable('Discharge','f',dimensions=("COMID","StageHeight"))
    vari_discharge.units = 'cubic meters per second'
    for j in np.arange(0, Hmax, dh):
        vari_sh[int(np.round(j/dh))] = j
    HANDClipper(catchmentRaster,catchmentgpkg, flowlinegpkg, handtif,
                COMIDlist, RiverLength_dic, Slope_dic,
                Hmax, dh, roughness, range_sh, vari_comid,
                vari_length, vari_slope, vari_roughness,
                vari_cw, vari_sa, vari_wa, vari_wp, vari_ba,
                vari_hr, vari_volume, vari_discharge)
    netcdf_file.close()


if __name__ == "__main__":
    main()
