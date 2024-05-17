#!/usr/bin/env python3

import argparse
import os

import fiona
import fiona._env
import fiona.env
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio._env
from osgeo import gdal


# gpd.options.io_engine = "pyogrio"


def create_flow_forecast_file(
    huc,
    ble_geodatabase,
    nwm_geodatabase,
    output_parent_dir,
    ble_xs_layer_name='XS',
    nwm_stream_layer_name='nwm_streams',
    nwm_feature_id_field='ID',
):
    '''
    This function will create a forecast flow file using ble data.
    It will import the ble XS layer and will intersect it with a nwm streams layer.
    It will the perform an intersection with the BLE XS layer and the nwm river layer.
    It will then calculate the median flow for the 100 and 500 year events for each nwm segment and convert flow from CFS to CMS.
    Individual forecast files (.csv) will be created  with two columns (nwm segment ID and Flow (CMS)) for the 100 year and the 500 year event.
    Flow field names are set to the default field names within the BLE submittal.
    As differing versions of the NWM river layer have different names for the ID field,
    this field will need to be specified as an argument to the function.
    Output flow forecast files will be automatically named to be compatible with current FIM code,
    and will be written out in specific folder structures.

    Parameters
    ----------
    ble_geodatabase : STRING
        Path to BLE geodatabase.
    nwm_geodatabase : STRING
        Path to nwm geodatabase.
    output_parent_dir : STRING
        Output parent directory of output. Flow files will be output to subdirectories within parent directory.
    ble_xs_layer_name : STRING
       The cross section layer in the ble geodatabase to be imported. Default is 'XS' (sometimes it is 'XS_1D')
    ble_huc_layer_name : STRING
       The huc layer in the ble geodatabase.  Default is 'S_HUC_Ar' (sometimes it is 'S_HUC_ar' )
    nwm_stream_layer_name : STRING
       The stream centerline layer name (or partial layer name) for the NWM geodatabase.  Default is 'RouteLink_FL_2020_04_07'.
    nwm_feature_id_field : STRING
       The feature id of the nwm segments.  Default is 'ID' (applicable if nwmv2.1 is used)
    Returns
    -------
    None.

    '''

    print(" ******************************************")
    # print(locals())

    # gdal.SetConfigOption('GDAL_DATA', rasterio._env.get_gdal_data())
    # gdal.SetConfigOption('CPL_LOG', '/dev/null')
    # gdal.SetConfigOption('CPL_DEBUG', 'OFF')

    def fill_missing_flows(forecast: pd.DataFrame, nwm_river_layer: pd.DataFrame):
        """
        This function will fill in missing flows in the forecast dataframe.
        It will do this by finding segments that don't intersect with the forecast dataframe, and
        then finding the upstream and downstream segments.
        It will then assign the average of the two to the missing segment.

        Parameters
        ----------
        forecast: pandas.DataFrame
            Dataframe containing forecast flows.
        nwm_river_layer: pandas.DataFrame
            Dataframe containing NWM river layer.

        Returns
        -------
        forecast: pandas.DataFrame
            Dataframe containing forecast flows with missing flows filled in.
        """
        n = 1
        while True:
            # Merge flows with nwm_river_layer. This will be used to find upstream and downstream flow.
            merged = nwm_river_layer.merge(forecast, left_on='ID', right_on='feature_id', how='left')

            nonintersects = merged[merged['discharge'].isna()]

            # print(f'Iteration {n}. {len(nonintersects)} segments remaining.')

            updated = False
            for i, row in nonintersects.iterrows():
                new_flow = []

                # Find sum of upstream flow(s)
                upstreams = merged.loc[merged['to'] == row['ID']]
                # if all upstream segments have flows
                if len(upstreams) > 0 and not upstreams['discharge'].isnull().values.any():
                    new_flow.append(upstreams['discharge'].sum())

                # Find downstream flow(s)
                downstream = merged.loc[merged['ID'] == row['to']]

                if len(downstream) > 0:
                    downstream_upstreams = merged.loc[merged['to'] == int(downstream['ID'].iloc[0])]

                    # all segments have flow except for the one being filled in
                    if (
                        downstream['discharge'].isnull().sum()
                        + downstream_upstreams['discharge'].isnull().sum()
                    ) == 1:
                        new_flow.append(
                            downstream['discharge'].iloc[0] - downstream_upstreams['discharge'].sum()
                        )

                # Add row data to forecast
                if len(new_flow) > 0:
                    forecast = pd.concat(
                        [
                            forecast,
                            pd.DataFrame(
                                {'feature_id': row['ID'], 'discharge': np.mean(new_flow)}, index=[0]
                            ),
                        ]
                    ).reset_index(drop=True)
                    updated = True

            if not updated:
                break
            else:
                n += 1

        forecast = forecast[forecast['discharge'] > 0]

        return forecast

    # Read the ble xs layer into a geopandas dataframe.
    layers = fiona.listlayers(ble_geodatabase)
    if ble_xs_layer_name not in layers:
        ble_xs_layer_name = 'XS_1D'

    xs_layer = gpd.read_file(ble_geodatabase, layer=ble_xs_layer_name)

    # Read in the NWM stream layer into a geopandas dataframe using the bounding box option based on the extents of the BLE XS layer.
    nwm_river_layer = gpd.read_file(nwm_geodatabase, mask=xs_layer, layer=nwm_stream_layer_name)

    # Make sure xs_layer is in same projection as nwm_river_layer.
    xs_layer_proj = xs_layer.to_crs(nwm_river_layer.crs)

    # Perform an intersection of the BLE layers and the NWM layers, using the keep_geom_type set to False produces a point output.
    intersection = gpd.overlay(xs_layer_proj, nwm_river_layer, how='intersection', keep_geom_type=False)

    ## Create the flow forecast files
    # Define fields containing flow (typically these won't change for BLE)
    flow_fields = ['E_Q_01PCT', 'E_Q_0_2PCT']

    # Define return period associated with flow_fields (in same order as flow_fields). These will also serve as subdirectory names.
    return_period = ['100yr', '500yr']

    # Conversion factor from CFS to CMS
    dischargeMultiplier = 0.3048**3

    # Write individual flow csv files
    for i, flow in enumerate(flow_fields):
        # Write dataframe with just ID and single flow event
        forecast = intersection[[nwm_feature_id_field, flow]]

        # Rename field names and re-define datatypes
        forecast = forecast.rename(columns={nwm_feature_id_field: 'feature_id', flow: 'discharge'})
        forecast = forecast.astype({'feature_id': int, 'discharge': float})

        # Calculate median flow per feature id
        forecast = forecast.groupby('feature_id').median()
        forecast = forecast.reset_index(level=0)

        # Convert CFS to CMS
        forecast['discharge'] = forecast['discharge'] * dischargeMultiplier

        # Assign flow to segments missing flows
        forecast = fill_missing_flows(forecast, nwm_river_layer)

        # Set paths and write file
        output_dir = os.path.join(output_parent_dir, huc)
        dir_of_csv = os.path.join(output_dir, return_period[i])
        os.makedirs(dir_of_csv, exist_ok=True)
        path_to_csv = os.path.join(dir_of_csv, "ble_huc_{}_flows_{}.csv".format(huc, return_period[i]))
        forecast.to_csv(path_to_csv, index=False)

    return


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Produce forecast flow files from BLE datasets')
    parser.add_argument(
        '-b',
        '--ble-geodatabase',
        help='BLE geodatabase (.gdb file extension).                               \
        Will look for layer with "XS" in name. It is assumed the 100 year flow field is "E_Q_01PCT"                           \
        and the 500 year flow field is "E_Q_0_2_PCT" as these are the default field names.',
        required=True,
    )
    parser.add_argument(
        '-n', '--nwm-geodatabase', help='NWM geodatabase (.gdb file extension).', required=True
    )
    parser.add_argument(
        '-o',
        '--output-parent-dir',
        help='Output directory where forecast files will be stored.              \
        Two subdirectories are created (100yr and 500yr) and in each subdirectory a forecast file is written',
        required=True,
    )
    parser.add_argument(
        '-xs',
        '--ble-xs-layer-name',
        help='BLE cross section layer.                                          \
        Default layer is "XS" (sometimes it is "XS_1D").',
        required=False,
        default='XS',
    )
    parser.add_argument('-hu', '--huc', help='HUC', required=True)
    parser.add_argument(
        '-l',
        '--nwm-stream-layer-name',
        help='NWM streams layer. Default layer is "nwm_streams")',
        required=False,
        default='nwm_streams',
    )
    parser.add_argument(
        '-f',
        '--nwm-feature-id-field',
        help='id field for nwm streams.                                       \
        Not required if NWM v2.1 is used (default id field is "ID")',
        required=False,
        default='ID',
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
    # Run create_flow_forecast_file
    create_flow_forecast_file(**args)
