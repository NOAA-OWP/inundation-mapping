#!/usr/bin/env python3

import argparse

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from utils.shared_functions import getDriver


gpd.options.io_engine = "pyogrio"


def find_hucs_of_bounding_boxes(
    bounding_boxes_file,
    wbd=None,
    projection_of_boxes='EPSG:4329',
    wbd_layer='WBDHU8',
    huc_output_file=None,
    forecast_output_file=None,
    bounding_boxes_outfile=None,
):
    # load bounding box file
    bounding_boxes = pd.read_csv(
        bounding_boxes_file, dtype={'minx': float, 'miny': float, 'maxx': float, 'maxy': float}, comment='#'
    )

    bounding_boxes['geometry'] = bounding_boxes.apply(
        lambda df: box(df['minx'], df['miny'], df['maxx'], df['maxy']), axis=1
    )

    bounding_boxes = gpd.GeoDataFrame(bounding_boxes, crs=projection_of_boxes)

    wbd_proj = gpd.read_file(wbd, layer=wbd_layer, rows=1).crs

    bounding_boxes = bounding_boxes.to_crs(wbd_proj)

    if bounding_boxes_outfile is not None:
        bounding_boxes.to_file(
            bounding_boxes_outfile, driver=getDriver(bounding_boxes_outfile), index=False, engine='fiona'
        )

    wbdcol_name = 'HUC' + wbd_layer[-1]

    hucs = bounding_boxes.apply(
        lambda bbdf: gpd.read_file(wbd, layer=wbd_layer, mask=bbdf.geometry)[wbdcol_name], axis=1
    )

    bounding_boxes = bounding_boxes.drop(columns=['geometry', 'minx', 'miny', 'maxx', 'maxy'])

    hucs_columns = hucs.columns
    bb_columns = bounding_boxes.columns
    bounding_boxes = hucs.join(bounding_boxes)
    bounding_boxes = pd.melt(bounding_boxes, id_vars=bb_columns, value_vars=hucs_columns, value_name='HUC8')
    bounding_boxes = bounding_boxes.drop(columns=['variable'])
    bounding_boxes = bounding_boxes.dropna()
    bounding_boxes = bounding_boxes.reset_index(drop=True)

    hucs_series = pd.Series(hucs.stack().reset_index(drop=True).unique())

    if huc_output_file is not None:
        hucs_series.to_csv(huc_output_file, sep='\n', index=False, header=False)

    if forecast_output_file is not None:
        bounding_boxes.to_csv(forecast_output_file, index=False, date_format='%Y-%m-%d %H:%M:%S%Z')

    return (hucs_series, bounding_boxes)


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='Find hucs for bounding boxes')
    parser.add_argument('-b', '--bounding-boxes-file', help='Bounding box file', required=True)
    parser.add_argument('-w', '--wbd', help='WBD file', required=True)
    parser.add_argument('-p', '--projection-of-boxes', help='Projection', required=False, default='EPSG:4329')
    parser.add_argument('-o', '--huc-output-file', help='Output file of HUCS', required=False, default=None)
    parser.add_argument('-f', '--forecast-output-file', help='Forecast file', required=False, default=None)
    parser.add_argument(
        '-u', '--bounding-boxes-outfile', help='Bounding boxes outfile', required=False, default=None
    )

    args = vars(parser.parse_args())

    find_hucs_of_bounding_boxes(**args)
