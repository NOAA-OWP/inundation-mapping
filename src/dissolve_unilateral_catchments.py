#!/usr/bin/env python3

import argparse
import logging

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point
from shapely.ops import split


logging.getLogger('shapely.geos').setLevel(logging.CRITICAL)


def combine_catchments(ids: list, layers: list, field: str = "HydroID") -> gpd.GeoDataFrame:
    """
    Combine the HydroIDs in ids into a single feature. Returns the original layer with the combined features replacing the original HydroIDs.

    Parameters
    ----------
        ids: list
            List of catchment IDs
        layers: list
            List of catchment layers

    Returns
    -------
        gpd.GeoDataFrame
            Combined GeoDataFrame of features
    """
    if isinstance(layers, gpd.GeoDataFrame):
        layers = [layers]

    if not isinstance(ids, list):
        raise ValueError("ids must be a list")
    if not isinstance(layers, list):
        raise ValueError("layers must be a list")
    if not all(isinstance(layer, gpd.GeoDataFrame) for layer in layers):
        raise ValueError("All layers must be GeoDataFrames")

    out = []
    for layer in layers:
        other_features = layer[~layer[field].isin(ids)]

        combined_features = layer[layer[field].isin(ids)]

        layer_columns = layer.columns

        # Manage attributes that aren't summed
        if 'NextDownID' in layer_columns:
            NextDownID = combined_features['NextDownID'].iloc[-1]
        if 'S0' in layer_columns:
            s0 = combined_features['S0'].mean()
        # if 'LakeID' in layer_columns:
        #     LakeID = combined_features['LakeID'].max()
        if 'From_Node' in layer_columns:
            from_node = combined_features['From_Node'].iloc[0]
        if 'To_Node' in layer_columns:
            to_node = combined_features['To_Node'].iloc[-1]

        combined_features = combined_features.dissolve(aggfunc='sum')

        combined_features[field] = ids[0]

        if 'NextDownID' in layer_columns:
            combined_features['NextDownID'] = NextDownID
        if 'S0' in layer_columns:
            combined_features['S0'] = s0
        # if 'LakeID' in layer_columns:
        #     combined_features['LakeID'] = LakeID
        if 'From_Node' in layer_columns:
            combined_features['From_Node'] = from_node
        if 'To_Node' in layer_columns:
            combined_features['To_Node'] = to_node

        out.append(pd.concat([other_features, combined_features], ignore_index=True))

    return out


def dissolve_unilateral_catchments(catchments: str, reaches: str, catchments_out: str, reaches_out: str):
    """
    Dissolves catchments where adjacent catchments are "unilateral", i.e., they only capture one side of the floodplain. This function dissolves adjacent catchments if they have missing the floodplain on opposite sides of the river.

    Parameters
    ----------
        catchments: str
            Filename of catchments to be read in
        reaches: str
            Filename of reaches to be read in
        catchments_out: str
            Filename of catchments to be written out
        reaches_out: str
            Filename of reaches to be written out

    Returns
    -------
        None
    """

    catchments = gpd.read_file(catchments)
    reaches = gpd.read_file(reaches)

    catchments['HydroID'] = catchments['HydroID'].astype(str)
    reaches = reaches.astype({'HydroID': str, 'NextDownID': str, 'LakeID': int})

    catchments_copy = catchments.copy()
    reaches_copy = reaches.copy()

    catchments = catchments[catchments['HydroID'].isin(reaches['HydroID'])]

    data = []
    reach_data = []

    # Ignore catchments that are in, above, or below lakes
    lake_hydroids = reaches[reaches['LakeID'] > 0]['HydroID']
    lake_hydroids_above = reaches[reaches['HydroID'].isin(lake_hydroids)]['NextDownID']
    lake_hydroids_below = reaches[reaches['NextDownID'].isin(lake_hydroids)]['HydroID']

    lake_hydroids = pd.concat([lake_hydroids, lake_hydroids_above, lake_hydroids_below]).drop_duplicates()
    lake_hydroids = lake_hydroids[~lake_hydroids.isin(catchments['HydroID'])]

    if not lake_hydroids.empty:
        catchments = catchments[~catchments['HydroID'].isin(list(lake_hydroids))]

    for catchment in catchments.itertuples():
        hydroid = catchment.HydroID

        reach = reaches[reaches['HydroID'] == hydroid]
        reach = reach.clip(catchment.geometry)

        reach_exploded = reach.explode(index_parts=False)
        if len(reach_exploded) > 1:
            reach_exploded = reach_exploded[
                reach_exploded.geometry.length == reach_exploded.geometry.length.max()
            ]
        reach = reach_exploded

        upstream_reach = reaches[reaches['NextDownID'] == hydroid]

        linestring = reach.iloc[0].geometry
        reach_length = linestring.length

        # Extend linestring to the upstream point
        if not upstream_reach.empty:
            linestring_coords = list(linestring.coords)

            upstream_linestring = upstream_reach.iloc[0].geometry
            upstream_linestring_coords = list(upstream_linestring.coords)

            linestring = LineString([Point(upstream_linestring_coords[-2])] + linestring_coords)

        catchments_split = split(catchment.geometry, linestring)

        reach_buffered_left_geometry = linestring.buffer(1, single_sided=True)
        reach_buffered_right_geometry = linestring.buffer(-1, single_sided=True)

        for i, catchment_split in enumerate(catchments_split.geoms):
            catchment_split_gs = gpd.GeoSeries(catchment_split)
            catchment_split_gs = catchment_split_gs.set_crs(catchments.crs)

            geometry = catchment_split.intersection(reach_buffered_left_geometry)
            if geometry.area > 0:
                side = 'left'
            else:
                geometry = catchment_split.intersection(reach_buffered_right_geometry)
                side = 'right'

            data.append([hydroid, side, catchment_split, catchment_split.area])

        reach_data.append([hydroid, reach_length])

    reach_df = pd.DataFrame(reach_data, columns=['HydroID', 'length'])

    data = gpd.GeoDataFrame(
        data,
        columns=['HydroID', 'side', 'catchment_split', 'area'],
        geometry='catchment_split',
        crs=catchments.crs,
    )

    area_total = data.groupby('HydroID')['area'].sum().reset_index()

    data_dissolved = data.dissolve(by=['HydroID', 'side'], aggfunc='sum', as_index=False)

    data_dissolved = data_dissolved.merge(area_total, on='HydroID', how='left', suffixes=('', '_total'))
    data_dissolved = data_dissolved.merge(reach_df, on='HydroID', how='left')

    data_dissolved['area_prop'] = data_dissolved['area'] / data_dissolved['area_total']
    data_dissolved['length_prop'] = data_dissolved['area'] / data_dissolved['length']

    data_left = data_dissolved[data_dissolved['side'] == 'left']
    data_right = data_dissolved[data_dissolved['side'] == 'right']

    # data_left.to_file('/outputs/split_catchments/catchments_split_left.gpkg', driver='GPKG')
    # data_right.to_file('/outputs/split_catchments/catchments_split_right.gpkg', driver='GPKG')

    temp_left = data_left[
        ((data_left['area_prop'] < 0.1) & (data_left['area_total'] < 1000000))
        | (data_left['length_prop'] < 250)
    ].sort_values(by='area_prop')
    temp_right = data_right[
        ((data_right['area_prop'] < 0.1) & (data_right['area_total'] < 1000000))
        | (data_right['length_prop'] < 250)
    ].sort_values(by='area_prop')

    # Loop through the left and right dataframes to find the upstream and downstream reaches
    # and their respective length proportions
    for i in temp_left.itertuples():
        reach_id = i.HydroID
        # Find the length_prop of the upstream reach
        upstream_id = reaches[reaches['NextDownID'] == i.HydroID]['HydroID'].values[0]
        downstream_id = reaches[
            reaches['HydroID'] == reaches[reaches['HydroID'] == i.HydroID]['NextDownID'].values[0]
        ]['HydroID'].values[0]

        upstream_reach = temp_right[temp_right['HydroID'] == upstream_id]
        upstream_length_prop = upstream_reach['area_prop'].values[0] if not upstream_reach.empty else np.nan
        downstream_reach = temp_right[temp_right['HydroID'] == downstream_id]
        downstream_length_prop = (
            downstream_reach['area_prop'].values[0] if not downstream_reach.empty else np.nan
        )

        # if upstream_length_prop and downstream_length_prop are both NaN, skip this iteration
        if pd.isna(upstream_length_prop) and pd.isna(downstream_length_prop):
            continue

        # Find the upstream or downstream reach with the lowest length_prop
        if upstream_length_prop < downstream_length_prop:
            # Use the upstream reach
            id = upstream_id
            next_id = reach_id

            # Remove the upstream reach from temp_right
            temp_right = temp_right[temp_right['HydroID'] != upstream_id]
        else:
            # Use the downstream reach
            id = reach_id
            next_id = downstream_id

            # Remove the downstream reach from temp_right
            temp_right = temp_right[temp_right['HydroID'] != downstream_id]

        # Combine the upstream and downstream reaches into a single catchment
        catchments_copy, reaches_copy = combine_catchments(
            [id, next_id], [catchments_copy, reaches_copy], field='HydroID'
        )

    # catchments_copy.to_file('/outputs/split_catchments/catchments_combined_area.gpkg', driver='GPKG')
    # reaches_copy.to_file('/outputs/split_catchments/reaches_combined_area.gpkg', driver='GPKG')

    catchments_copy.to_file(catchments_out, driver='GPKG')
    reaches_copy.to_file(reaches_out, driver='GPKG')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Assess bilateral catchments.")
    parser.add_argument('-c', '--catchments', type=str, help='Path to catchments file')
    parser.add_argument('-r', '--reaches', type=str, help='Path to reaches file')
    parser.add_argument('-co', '--catchments-out', type=str, help='Path to output catchments file')
    parser.add_argument('-ro', '--reaches-out', type=str, help='Path to output reaches file')
    args = parser.parse_args()

    dissolve_unilateral_catchments(**vars(args))
