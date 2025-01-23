#!/usr/bin/env python3

import os
from argparse import ArgumentParser

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import whitebox
from rasterstats import zonal_stats


# Set wbt envs
wbt = whitebox.WhiteboxTools()
wbt.set_verbose_mode(False)
wbt.set_whitebox_dir(os.environ.get("WBT_PATH"))


"""
To use this script you will need to create a tif file from the eHydro depth survey tin file.
    To create depth tif:
        1. Download eHydro survey data from USACE Hydrographic Surveys and open the tin file in Q-GIS.
        2. Run rasterize mesh tool on tin file (make sure the tif and gdb file names are the same), e.g.
        processing.run("native:meshrasterize", {'INPUT':'ESRI_TIN:"C:/Users/riley.mcdermott/
        Documents/Bathymetry_PA/AL_LP_EMS_20150809_CS_005_065_SORT/AL_LP_EMS_20150809_CS_005_065_SORT_tin/
        tdenv9.adf"','DATASET_GROUPS':[0],'DATASET_TIME':{'type': 'static'},'EXTENT':None,'PIXEL_SIZE':1,
        'CRS_OUTPUT':QgsCoordinateReferenceSystem('EPSG:5070'),'OUTPUT':'C:/Users/riley.mcdermott/Documents
        /Bathymetry_PA/Allegheny_USACE_Depth_ft.tif'})
"""


def preprocessing_ehydro(tif, bathy_bounds, survey_gdb, output, min_depth_threshold):
    """Function for calculating missing reach averaged cross sectional
    area and wetted perimeter by NWM feature_id.

        Parameters
        ----------
        tif : str
            Path to depth tif file from rasterized USACE eHydro survey tin, e.g.
            "/data/inputs/bathymetry/HU_01_TRY_20211108_CS_5043_30.tif".
        bathy_bounds : str
            Name of survey extent vector layer within given geodatabase,
            defaults to 'SurveyJob'.
        survey_gdb : str
            Path to survey geodatabase containing vector survey extent layer, e.g.
            "/data/inputs/bathymetry/HU_01_TRY_20211108_CS_5043_30.gdb".
        output : str
            Path to output location for NWM flows geopackage, e.g.
            "/data/inputs/bathymetry/bathymetry_adjustment_data.gpkg".
        min_depth_threshold : int
            Highest value allowed for the minimum depth of the survey
            in feet (Checks for data referenced to a datum).
    """

    try:
        with rasterio.open(tif) as bathy_ft:
            bathy_profile = bathy_ft.profile
            bathy_affine = bathy_ft.transform
            bathy_ft = bathy_ft.read(1)

        bathy_ft[np.where(bathy_ft == -9999.0)] = np.nan
        bathy_ft[np.where(bathy_ft <= 0.0)] = 0.000001

        survey_min_depth = np.nanmin(bathy_ft)

        assert survey_min_depth < min_depth_threshold, (
            f"The minimum depth value of the survey is {survey_min_depth} which exceeds the minimum depth "
            "threshold. This may indicate depth values are based on a datum."
        )

        assert survey_min_depth > 0, (
            f"The minimum depth value of the survey is {survey_min_depth}, negative values may indicate a "
            "problem with the datum conversion or survey."
        )

        bathy_m = bathy_ft / 3.28084

        output_slope_tif = os.path.join(os.path.dirname(tif), 'bathy_slope.tif')
        bathy_temp = os.path.join(os.path.dirname(tif), 'bathy_m.tif')

        with rasterio.open(bathy_temp, 'w', **bathy_profile) as dst:
            dst.write(bathy_m, 1)

        # Read in shapefiles
        bathy_bounds = gpd.read_file(survey_gdb, layer=bathy_bounds, engine="pyogrio", use_arrow=True)
        nwm_streams = gpd.read_file("/data/inputs/nwm_hydrofabric/nwm_flows.gpkg", mask=bathy_bounds)
        nwm_catchments = gpd.read_file("/data/inputs/nwm_hydrofabric/nwm_catchments.gpkg", mask=bathy_bounds)
        bathy_bounds = bathy_bounds.to_crs(nwm_streams.crs)

        # Find missing volume from depth tif
        zs_area = zonal_stats(
            nwm_catchments, bathy_m, stats=["sum"], affine=bathy_affine, geojson_out=True, nodata=np.nan
        )
        zs_area = gpd.GeoDataFrame.from_features(zs_area)
        zs_area = zs_area.set_crs(nwm_streams.crs)
        zs_area = zs_area.rename(columns={"sum": "missing_volume_m3"})

        # print("------------------------------")
        # print(zs_area.ID)
        # print("------------------------------")

        # Derive slope tif
        wbt.slope(bathy_temp, output_slope_tif, units="degrees")

        with rasterio.open(output_slope_tif) as slope_tif:
            slope_tif = slope_tif.read(1)

        os.remove(output_slope_tif)
        slope_tif[np.where(slope_tif == -9999.0)] = np.nan
        missing_bed_area = (1 / np.cos(slope_tif * np.pi / 180)) - 1

        # Find missing bed area from slope tif
        zs_slope = zonal_stats(
            nwm_catchments,
            missing_bed_area,
            stats=["sum"],
            affine=bathy_affine,
            geojson_out=True,
            nodata=np.nan,
        )
        zs_slope = gpd.GeoDataFrame.from_features(zs_slope)
        zs_slope = zs_slope.rename(columns={"sum": "missing_bed_area_m2"})

        # Clip streams to survey bounds and find new reach lengths
        nwm_streams_clip = nwm_streams.clip(bathy_bounds)
        nwm_streams_clip["Length"] = nwm_streams_clip.length

        # Merge data into nwm streams file and remove extremely small reaches
        bathy_nwm_streams = nwm_streams_clip.merge(zs_area[['missing_volume_m3', 'ID']], on='ID')
        bathy_nwm_streams = bathy_nwm_streams.merge(zs_slope[['missing_bed_area_m2', 'ID']], on='ID')
        max_order = bathy_nwm_streams["order_"].max()
        bathy_nwm_streams = bathy_nwm_streams.loc[bathy_nwm_streams['order_'] >= (max_order - 1)]

        # Calculate wetted perimeter and cross sectional area
        bathy_nwm_streams['missing_xs_area_m2'] = (
            bathy_nwm_streams['missing_volume_m3'] / bathy_nwm_streams['Length']
        )
        bathy_nwm_streams['missing_wet_perimeter_m'] = (
            bathy_nwm_streams['missing_bed_area_m2'] / bathy_nwm_streams['Length']
        )

        # Add survey meta data
        time_stamp = bathy_bounds.loc[0, 'SurveyDateStamp']
        time_stamp_obj = str(time_stamp)

        bathy_nwm_streams['SurveyDateStamp'] = time_stamp_obj  # bathy_bounds.loc[0, 'SurveyDateStamp']
        bathy_nwm_streams['SurveyId'] = bathy_bounds.loc[0, 'SurveyId']
        bathy_nwm_streams['Sheet_Name'] = bathy_bounds.loc[0, 'Sheet_Name']
        bathy_nwm_streams["Bathymetry_source"] = 'USACE eHydro'

        # Export geopackage with bathymetry
        num_streams = len(bathy_nwm_streams)
        bathy_nwm_streams = bathy_nwm_streams.to_crs(epsg=5070)

        # schema = gpd.io.file.infer_schema(bathy_nwm_streams)
        # print(schema)
        # print("---------------------------")

        if os.path.exists(output):
            print(f"{output} already exists. Concatinating now...")
            existing_bathy_file = gpd.read_file(output, engine="pyogrio", use_arrow=True)
            bathy_nwm_streams = pd.concat([existing_bathy_file, bathy_nwm_streams])
        bathy_nwm_streams.to_file(output, index=False)
        print(f"Added {num_streams} new NWM features")
    except AssertionError as e:
        print(f"Error processing {tif}: {e}")


def process_directory(input_dir, output_dir, min_depth_threshold, bathy_bounds_layer='SurveyJob'):

    tif_files = [f for f in os.listdir(input_dir) if f.endswith('.tif')]

    for tif in tif_files:
        base_name = os.path.splitext(tif)[0]
        tif_path = os.path.join(input_dir, tif)
        gdb_path = os.path.join(input_dir, base_name + '.gdb')

        if not os.path.isdir(gdb_path):
            print(f"Matching GDB not found for {tif}. Skipping...")
            continue

        preprocessing_ehydro(tif_path, bathy_bounds_layer, gdb_path, output_dir, min_depth_threshold)


if __name__ == '__main__':
    parser = ArgumentParser(description="Preprocessed Bathymetry")
    parser.add_argument(
        '-input_dir', '--input_dir', help='Directory containing TIFF and GDB files', required=True, type=str
    )
    parser.add_argument(
        '-output_dir',
        '--output_dir',
        help='Directory to save output geopackage files',
        required=True,
        type=str,
    )
    parser.add_argument(
        '-min_depth_threshold',
        '--min_depth_threshold',
        help='minimum expected depth value',
        required=False,
        type=int,
        default=10,
    )
    parser.add_argument(
        '-bathy_bounds',
        '--bathy_bounds',
        help='Survey Bounds Layer Ex. SurveyJob.gpkg',
        default='SurveyJob',
        required=False,
        type=str,
    )

    args = vars(parser.parse_args())

    input_dir = args['input_dir']
    output_dir = args['output_dir']
    min_depth_threshold = args['min_depth_threshold']
    bathy_bounds = args['bathy_bounds']

    process_directory(input_dir, output_dir, min_depth_threshold, bathy_bounds)
    print("Batch processing complete :)")

    # Example usage
    """
    python3 /foss_fim/data/bathymetry/preprocess_bathymetry.py \
        -input_dir /fim-home/bathymetry_processing/surveys \
        -output_dir /fim-home/bathymetry_processing/combined.gpkg
    """
