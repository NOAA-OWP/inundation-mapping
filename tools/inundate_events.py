#!/usr/bin/env python3

from shapely.geometry import box
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
import argparse
from inundation import inundate
import os


def inundate_events(
    hydrofabric_dir, forecast_file, inundation_file, inundation_polygon=None, jobs=1
):
    forecast_df = pd.read_csv(
        forecast_file, infer_datetime_format=True, dtype={'huc': str}, parse_dates=['date_time']
    )

    # list_of_hucs_to_run = { '09020318','09020317' }
    # forecast_df = forecast_df.loc[ forecast_df.huc.isin(list_of_hucs_to_run),:]

    inputs = build_fim3_inputs(hydrofabric_dir, forecast_df, inundation_file, inundation_polygon)

    executor = ThreadPoolExecutor(max_workers=jobs)

    results = {
        executor.submit(inundate, **kwargs): (kwargs['hydro_table'], kwargs['forecast'])
        for kwargs in inputs
    }
    # rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=None,hucs_layerName=None,
    # subset_hucs=None,num_workers=1,aggregate=False,inundation_raster=None,inundation_polygon=None,
    # depths=None,out_raster_profile=None,out_vector_profile=None,quiet=False

    for future in tqdm(as_completed(results), total=len(forecast_df)):
        try:
            future.result()
        except Exception as exc:
            print(exc, results[future])

    executor.shutdown(wait=True)


def build_fim3_inputs(hydrofabric_dir, forecast_df, inundation_file=None, inundation_polygons=None):
    for idx, row in forecast_df.iterrows():
        huc = row['huc']
        rem = os.path.join(hydrofabric_dir, huc, 'rem_zeroed_masked.tif')
        catchments_raster = os.path.join(
            hydrofabric_dir, huc, 'gw_catchments_reaches_filtered_addedAttributes.tif'
        )
        catchment_poly = os.path.join(
            hydrofabric_dir, huc, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg'
        )
        hydrotable = os.path.join(hydrofabric_dir, huc, 'hydroTable.csv')

        # inundation raster, inundation poly add HUC,date,time to filename
        inundation_file_for_row = append_meta_data_to_file_names(inundation_file, row)
        inundation_poly_for_row = append_meta_data_to_file_names(inundation_polygons, row)

        kwargs = {
            'rem': rem,
            'catchments': catchments_raster,
            'catchment_poly': catchment_poly,
            'hydro_table': hydrotable,
            'mask_type': 'huc',
            'forecast': row['forecast_file'],
            'inundation_raster': inundation_file_for_row,
            'inundation_polygon': inundation_poly_for_row,
            'quiet': True,
        }

        yield (kwargs)


def append_meta_data_to_file_names(file_name, row):
    if file_name is None:
        return file_name

    base_file_path, extension = os.path.splitext(file_name)

    hucCode = row['huc']
    site_name = row['Name']
    date_time = (
        str(row['date_time'].year)
        + str(row['date_time'].month).zfill(2)
        + str(row['date_time'].day).zfill(2)
        + '_'
        + str(row['date_time'].hour).zfill(2)
        + str(row['date_time'].minute).zfill(2)
        + 'Z'
    )

    appended_file_path = f"{base_file_path}_{site_name}_{hucCode}_{date_time}{extension}"

    return appended_file_path


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='Find hucs for bounding boxes')
    parser.add_argument('-y', '--hydrofabric-dir', help='Bounding box file', required=True)
    parser.add_argument('-f', '--forecast-file', help='WBD file', required=True)
    parser.add_argument('-i', '--inundation-file', help='WBD file', required=False, default=None)
    parser.add_argument('-j', '--jobs', help='WBD file', required=False, default=None, type=int)

    args = vars(parser.parse_args())

    inundate_events(**args)
