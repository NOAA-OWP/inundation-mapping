#!/usr/bin/env python3

from shapely.geometry import box
import pandas as pd
import geopandas as gpd
import argparse
from datetime import datetime
from make_boxes_from_bounds import find_hucs_of_bounding_boxes
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm

##################################
##
## Likely Deprecated: File appears to be no longer used. Noticed Jan 16, 2023
## Might want to be kept for possible re-use at a later time?
##
##################################


def nesdis_preprocessing(
    bounding_boxes_file,
    wbd=None,
    projection_of_boxes='EPSG:4329',
    wbd_layer='WBDHU8',
    forecast_output_file=None,
    retrieve=True,
    workers=6,
    download_directory=None,
):
    _, bounding_boxes = find_hucs_of_bounding_boxes(
        bounding_boxes_file, wbd=wbd, projection_of_boxes=projection_of_boxes, wbd_layer=wbd_layer
    )

    # load bounding box file
    bounding_boxes['event_start'] = pd.to_datetime(bounding_boxes['event_start'], utc=True)
    bounding_boxes['event_end'] = pd.to_datetime(bounding_boxes['event_end'], utc=True)
    bounding_boxes['additional_date'] = pd.to_datetime(bounding_boxes['additional_date'], utc=True)

    bounding_boxes.reset_index(drop=True, inplace=True)

    wbdcol_name = 'HUC' + wbd_layer[-1]

    # expand dates
    datetime_indices = bounding_boxes.apply(
        lambda df: pd.date_range(
            df['event_start'], df['event_end'], closed=None, freq='D', tz='UTC'
        ),
        axis=1,
    )

    datetime_indices.name = 'date_time'
    datetime_indices = pd.DataFrame(datetime_indices)
    datetime_indices = datetime_indices.join(bounding_boxes[['Name', wbdcol_name]])

    # append columns to expanded dates
    forecast_df = pd.DataFrame()
    for idx, row in datetime_indices.iterrows():
        dt_df = row['date_time'].to_frame(index=False, name='date_time')

        row = row.drop('date_time')

        dt_df = dt_df.join(pd.concat([pd.DataFrame(row).T] * len(dt_df), ignore_index=True))

        forecast_df = pd.concat((forecast_df, dt_df), ignore_index=True)

    # add extra dry date
    additional_date_df = (
        forecast_df[['Name', wbdcol_name]]
        .merge(
            bounding_boxes[['additional_date', wbdcol_name]],
            left_on=wbdcol_name,
            right_on=wbdcol_name,
        )
        .drop_duplicates(ignore_index=True)
    )

    forecast_df = pd.concat(
        (forecast_df, additional_date_df.rename(columns={'additional_date': 'date_time'})),
        ignore_index=True,
    )

    forecast_df = forecast_df.sort_values(['Name', wbdcol_name], ignore_index=True)

    forecast_df['date_time'] = forecast_df.apply(
        lambda df: df['date_time'].replace(hour=18, minute=0), axis=1
    )

    forecast_df = forecast_df.rename(columns={wbdcol_name: 'huc'})

    forecast_df = construct_nwm_forecast_filenames_and_retrieve(
        forecast_df, download_directory, retrieve=retrieve, workers=workers
    )

    if forecast_output_file is not None:
        forecast_df.to_csv(forecast_output_file, index=False, date_format='%Y-%m-%d %H:%M:%S%Z')


def construct_nwm_forecast_filenames_and_retrieve(
    forecast_df, download_directory, retrieve=True, workers=1
):
    # make forecast file names for NWM and retrieve

    # construct url
    # url = f'{year}/{year}{month}{day}{time}.CHRTOUT_DOMAIN1.comp'

    make_url = (
        lambda df: "https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/model_output/"
        + str(df['date_time'].year)
        + '/'
        + str(df['date_time'].year)
        + str(df['date_time'].month).zfill(2)
        + str(df['date_time'].day).zfill(2)
        + str(df['date_time'].hour).zfill(2)
        + str(df['date_time'].minute).zfill(2)
        + ".CHRTOUT_DOMAIN1.comp"
    )

    def make_file_names(df, download_directory):
        # assumes that the last segment after the / represents the file name
        url = df['forecast_url']
        file_name_start_pos = url.rfind("/") + 1
        file_name = url[file_name_start_pos:]

        file_name = os.path.join(download_directory, file_name)

        return file_name

    urls = forecast_df.apply(make_url, axis=1)
    forecast_df['forecast_url'] = urls

    file_names = forecast_df.apply(lambda df: make_file_names(df, download_directory), axis=1)
    forecast_df['forecast_file'] = file_names

    if not retrieve:
        return forecast_df

    download_df = forecast_df[['forecast_url', 'forecast_file']].drop_duplicates()

    def download_url(url, file_name):
        r = requests.get(url, stream=True)
        if r.status_code == requests.codes.ok:
            with open(file_name, 'wb') as f:
                for data in r:
                    f.write(data)
        return url

    pool = ThreadPoolExecutor(max_workers=workers)

    results = {
        pool.submit(download_url, *(url, file_name)): (url, file_name)
        for idx, (url, file_name) in download_df.iterrows()
    }

    for future in tqdm(as_completed(results), total=len(download_df)):
        url, file_name = results[future]

        try:
            future.result()
        except Exception as exc:
            print('error', exc, url)

    pool.shutdown(wait=True)

    return forecast_df


if __name__ == '__main__':
    ##################################
    ##
    ## Likely Deprecated: File appears to be no longer used. Noticed Jan 16, 2023
    ## Might want to be kept for possible re-use at a later time?
    ##
    ##################################

    # parse arguments
    parser = argparse.ArgumentParser(description='Find hucs for bounding boxes')
    parser.add_argument('-b', '--bounding-boxes-file', help='Bounding box file', required=True)
    parser.add_argument('-w', '--wbd', help='WBD file', required=True)
    parser.add_argument(
        '-f', '--forecast-output-file', help='Forecast file', required=False, default=None
    )
    parser.add_argument(
        '-r', '--retrieve', help='Forecast file', required=False, default=False, action='store_true'
    )
    parser.add_argument(
        '-j', '--workers', help='Forecast file', required=False, default=1, type=int
    )
    parser.add_argument(
        '-d', '--download_directory', help='Forecast file', required=False, default=1
    )

    args = vars(parser.parse_args())

    nesdis_preprocessing(**args)
