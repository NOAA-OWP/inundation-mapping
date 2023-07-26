#!/usr/bin/env python3

import rasterio
import pandas as pd
import geopandas as gpd
import numpy as np
from rasterstats import zonal_stats
from osgeo import gdal
from osgeo import gdal_array
from argparse import ArgumentParser
import os

def preprocessing_ehydro(tif, bathy_bounds, survey_gdb, output):
    """
    will need to read in tif from Q-GIS mesh to raster and bathy bounds from SurveyJob layer in survey geodatabase
    """
    with rasterio.open(tif) as bathy_ft:
        bathy_affine = bathy_ft.transform
        bathy_ft = bathy_ft.read(1)
    bathy_m = bathy_ft/3.28084
    bathy_gdal = gdal_array.OpenArray(bathy_m)

    # Read in shapefiles
    bathy_bounds = gpd.read_file(survey_gdb, layer = bathy_bounds)
    nwm_streams = gpd.read_file("/data/inputs/nwm_hydrofabric/nwm_flows.gpkg", mask = bathy_bounds)
    nwm_catchments = gpd.read_file("/data/inputs/nwm_hydrofabric/nwm_catchments.gpkg", mask = bathy_bounds)
    bathy_bounds = bathy_bounds.to_crs(nwm_streams.crs)

    # Find missing volume from depth tif
    zs_area = zonal_stats(nwm_catchments, bathy_m, stats = ["sum"], affine= bathy_affine, geojson_out = True)
    zs_area = gpd.GeoDataFrame.from_features(zs_area)
    zs_area = zs_area.set_crs(nwm_streams.crs)
    zs_area.rename(columns = {"sum":"missing_volume_m3"}, inplace = True)

    # Derive slope tif
    output_slope_tif = os.paht.join(os.path.dirname(tif), 'bathy_slope.tif')
    slope_tif = gdal.DEMProcessing(output_slope_tif, bathy_gdal , 'slope', format = 'GTiff')
    slope_tif = slope_tif.GetRasterBand(1).ReadAsArray()
    os.remove(output_slope_tif)
    slope_tif[np.where(slope_tif == -9999.)] = np.nan
    missing_bed_area = (1 / np.cos(slope_tif*np.pi/180)) - 1

    # Find missing bed area from slope tif
    zs_slope = zonal_stats(nwm_catchments, missing_bed_area, stats = ["sum"], affine= bathy_affine, geojson_out = True)
    zs_slope = gpd.GeoDataFrame.from_features(zs_slope)
    zs_slope.rename(columns = {"sum":"missing_bed_area_m2"}, inplace = True)

    # Clip streams to survey bounds and find new reach lengths
    nwm_streams_clip = nwm_streams.clip(bathy_bounds)
    nwm_streams_clip["Length"] = nwm_streams_clip.length

    # Merge data into nwm streams file and remove extremely small reaches
    bathy_nwm_streams = nwm_streams_clip.merge(zs_area[['missing_volume_m3', 'ID']], on='ID')
    bathy_nwm_streams = bathy_nwm_streams.merge(zs_slope[['missing_bed_area_m2', 'ID']], on='ID')
    river_length = bathy_nwm_streams["Length"].sum()
    min_length = river_length*0.05
    bathy_nwm_streams = bathy_nwm_streams[bathy_nwm_streams['Length'] > min_length]

    # Calculate wetted perimeter and cross sectional area
    bathy_nwm_streams['missing_xs_area_m2'] = bathy_nwm_streams['missing_volume_m3']/bathy_nwm_streams['Length']
    bathy_nwm_streams['missing_wet_perimeter_m'] = bathy_nwm_streams['missing_bed_area_m2']/bathy_nwm_streams['Length']

    # Add survey meta data
    bathy_nwm_streams['SurveyDateStamp'] = bathy_bounds.loc[0, 'SurveyDateStamp']
    bathy_nwm_streams['SurveyId'] = bathy_bounds.loc[0, 'SurveyId']
    bathy_nwm_streams['Sheet_Name'] = bathy_bounds.loc[0, 'Sheet_Name']

    # Export geopackage with bathymetry
    if os.path.exists(output):
        print(f"{output} already exists. Concatinating now...")
        existing_bathy_file = gpd.read_file(output)
        bathy_nwm_streams = pd.concat([existing_bathy_file, bathy_nwm_streams])
    bathy_nwm_streams.to_file(output, index = False)

if __name__ == '__main__':
    
    parser = ArgumentParser(description="Preprocessed Bathymetry")
    parser.add_argument('-tif','--tif', help='Survey Depth Raster', required=True,type=str)
    parser.add_argument('-survey_gdb','--survey_gdb', help='Survey Geodatabase Ex. AL_LP_EMS_20150809_CS_005_065_SORT.gbd', required=True,type=str)
    parser.add_argument('-bathy_bounds','--bathy_bounds', help='Survey Bounds Layer Ex. SurveyJob.gpkg', default = 'SurveyJob', required=False,type=str)
    parser.add_argument('-output','--output', help='output geopackage location', required=True,type=str)

    args = vars(parser.parse_args())

    tif = args['tif']
    survey_gdb = args['survey_gdb']
    bathy_bounds = args['bathy_bounds']
    output = args['output']

    preprocessing_ehydro(tif, bathy_bounds, survey_gdb, output)
    print("success :)")
