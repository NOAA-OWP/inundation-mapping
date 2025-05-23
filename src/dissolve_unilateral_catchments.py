#!/usr/bin/env python3

import argparse
import logging
import os
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point
from shapely.ops import split


logging.getLogger('shapely.geos').setLevel(logging.CRITICAL)
warnings.filterwarnings("ignore", category=RuntimeWarning)


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

        if combined_features.empty:
            out.append(other_features)
            continue

        layer_columns = layer.columns

        # Manage attributes that aren't summed
        if 'NextDownID' in layer_columns:
            NextDownID = combined_features['NextDownID'].iloc[-1]
        if 'S0' in layer_columns:
            s0 = combined_features['S0'].mean()
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
        if 'From_Node' in layer_columns:
            combined_features['From_Node'] = from_node
        if 'To_Node' in layer_columns:
            combined_features['To_Node'] = to_node

        out.append(pd.concat([other_features, combined_features], ignore_index=True))

    return out


def find_and_combine_sequences(df, data_dissolved, reaches, catchments_copy, reaches_copy):
    """
    Find and combine sequences of adjacent reaches with missing floodplain on opposite sides.
    """
    skip = 0
    for row in df.itertuples():
        print(f'{row.HydroID}, skip: {skip}, count: {row.count}')
        if skip > 0:
            skip -= 1
            print(f'\tSkipping {row.HydroID}, skip: {skip}')
            continue

        # Get areas of the first reach
        area_left = data_dissolved[
            (data_dissolved['HydroID'] == row.HydroID) & (data_dissolved['side'] == 'left')
        ]['area'].values[0]
        area_right = data_dissolved[
            (data_dissolved['HydroID'] == row.HydroID) & (data_dissolved['side'] == 'right')
        ]['area'].values[0]

        # Get ID of the next reach
        next_id = reaches.loc[reaches['HydroID'] == row.HydroID, 'NextDownID'].item()

        ids_to_combine = [row.HydroID, next_id]

        # Compute combined area ratio
        next_area_left = data_dissolved[
            (data_dissolved['HydroID'] == next_id) & (data_dissolved['side'] == 'left')
        ]['area'].values[0]
        next_area_right = data_dissolved[
            (data_dissolved['HydroID'] == next_id) & (data_dissolved['side'] == 'right')
        ]['area'].values[0]

        area_left += next_area_left
        area_right += next_area_right
        area_total = row.area_total + next_area_left + next_area_right

        area_left_prop = (area_left + next_area_left) / area_total
        area_right_prop = (area_right + next_area_right) / area_total

        area_prop = min(area_left_prop, area_right_prop)

        for i in range(row.count):
            print(f'\t\trow.count: {row.count}, skip: {skip}, i: {i}')
            next_id = reaches.loc[reaches['HydroID'] == next_id, 'NextDownID'].item()

            # Compute combined area ratio
            next_area_left = data_dissolved[
                (data_dissolved['HydroID'] == next_id) & (data_dissolved['side'] == 'left')
            ]['area'].values[0]
            next_area_right = data_dissolved[
                (data_dissolved['HydroID'] == next_id) & (data_dissolved['side'] == 'right')
            ]['area'].values[0]

            next_area_total = next_area_left + next_area_right

            next_area_left_prop = (area_left + next_area_left) / (area_total + next_area_total)
            next_area_right_prop = (area_right + next_area_right) / (area_total + next_area_total)

            next_area_prop = min(next_area_left_prop, next_area_right_prop)

            # Add next catchment if catchments are more balanced (bilateral)
            skip += 1
            print(f'\tnext_area_prop: {next_area_prop}, area_prop: {area_prop}, skip: {skip}')
            if (next_area_prop > area_prop) or (area_total < 500000):
                ids_to_combine.append(next_id)
                area_left += next_area_left
                area_right += next_area_right
                area_total += next_area_total
                area_prop = next_area_prop

                print(f'\tDissolving {ids_to_combine}')

            else:
                print(f'\tStop dissolving {ids_to_combine} at {next_id}')
                break

        catchments_copy, reaches_copy = combine_catchments(
            ids_to_combine, [catchments_copy, reaches_copy], field='HydroID'
        )

    return catchments_copy, reaches_copy


def dissolve_unilateral_catchments(
    catchments_filename: str, reaches_filename: str, catchments_out: str, reaches_out: str
):
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

    catchments_layername = 'catchments'
    reaches_layername = os.path.splitext(os.path.basename(reaches_filename))[0]

    catchments = gpd.read_file(catchments_filename, layer=catchments_layername)
    reaches = gpd.read_file(reaches_filename, layer=reaches_layername)

    catchments['HydroID'] = catchments['HydroID'].astype(str)
    reaches = reaches.astype({'HydroID': str, 'NextDownID': str, 'LakeID': int})

    catchments_copy = catchments.copy()
    reaches_copy = reaches.copy()

    catchments = catchments[catchments['HydroID'].isin(reaches['HydroID'])]

    # Get HydroIDs in upstream to downstream order
    hydroids_ordered = [reaches[~reaches['HydroID'].isin(reaches['NextDownID'])]['HydroID'].values[0]]
    for i in range(len(reaches) - 1):
        hydroid = hydroids_ordered[i]
        next_down_id = reaches[reaches['HydroID'] == hydroid]['NextDownID'].values[0]
        if next_down_id not in hydroids_ordered:
            hydroids_ordered.append(next_down_id)

    hydroids_ordered = {hydroid: i for i, hydroid in enumerate(hydroids_ordered)}

    reaches['upstream_id'] = reaches['HydroID'].apply(
        lambda x: (
            reaches[reaches['NextDownID'] == x]['HydroID'].values[0]
            if not reaches[reaches['NextDownID'] == x].empty
            else None
        )
    )

    data = []
    reach_data = []

    # Ignore catchments that are in, above, or below lakes
    lake_hydroids = reaches[reaches['LakeID'] > 0]['HydroID']
    if not lake_hydroids.empty:
        lake_hydroids_above = reaches[reaches['HydroID'].isin(lake_hydroids)]['NextDownID']
        lake_hydroids_below = reaches[reaches['NextDownID'].isin(lake_hydroids)]['HydroID']

        lake_hydroids = pd.concat([lake_hydroids, lake_hydroids_above, lake_hydroids_below]).drop_duplicates()
        lake_hydroids = lake_hydroids[~lake_hydroids.isin(catchments['HydroID'])]

        catchments = catchments[~catchments['HydroID'].isin(list(lake_hydroids))]
        reaches = reaches[~reaches['HydroID'].isin(list(lake_hydroids))]

    for catchment in catchments.itertuples():
        hydroid = catchment.HydroID

        reach = reaches[reaches['HydroID'] == hydroid]
        reach = reach.clip(catchment.geometry)

        if reach.empty:
            continue

        reach_exploded = reach.explode(index_parts=False)
        if len(reach_exploded) > 1:
            reach_exploded = reach_exploded[
                reach_exploded.geometry.length == reach_exploded.geometry.length.max()
            ]
        reach = reach_exploded

        upstream_reach = reaches[reaches['NextDownID'] == hydroid]
        upstream_reach_exploded = upstream_reach.explode(index_parts=False)
        if len(upstream_reach_exploded) > 1:
            upstream_reach_exploded = upstream_reach_exploded[
                upstream_reach_exploded.geometry.length == upstream_reach_exploded.geometry.length.max()
            ]
        upstream_reach = upstream_reach_exploded

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

    # data_left.to_file('/outputs/v4.7.4.0/11070203/branches/2093000005/catchments_split_left.gpkg', driver='GPKG')
    # data_right.to_file('/outputs/v4.7.4.0/11070203/branches/2093000005/catchments_split_right.gpkg', driver='GPKG')

    temp_left = data_left[
        ((data_left['area_prop'] < 0.1) & (data_left['area_total'] < 1000000))
        | (data_left['length_prop'] < 250)
    ]
    temp_right = data_right[
        ((data_right['area_prop'] < 0.1) & (data_right['area_total'] < 1000000))
        | (data_right['length_prop'] < 250)
    ]

    temp_left_ids = list(temp_left['HydroID'])
    temp_right_ids = list(temp_right['HydroID'])

    # Starting from the top of the HUC, find the reaches that are in successive temp_left and temp_right
    candidate_list = []
    for i, id in enumerate(hydroids_ordered):
        if id in temp_left_ids:
            next_id = reaches.loc[reaches['HydroID'] == id, 'NextDownID'].item()
            if next_id in temp_right_ids:
                candidate_list.append([i, id, next_id, 'left'])
        if id in temp_right_ids:
            next_id = reaches.loc[reaches['HydroID'] == id, 'NextDownID'].item()
            if next_id in temp_left_ids:
                candidate_list.append([i, id, next_id, 'right'])

    candidate_df = pd.DataFrame(candidate_list, columns=['i', 'HydroID', 'NextDownID', 'side'])
    # candidate_ids = list(candidate_df['HydroID'])

    # Loop through candidate_df and find candidate HydroIDs where the next down is the opposite side
    sequences_even = []
    sequences_odd = []
    for row in candidate_df.itertuples():
        # if row.NextDownID in candidate_ids and [row.side] != list(
        #     candidate_df[candidate_df['HydroID'] == row.NextDownID]['side']
        # ):
        position = hydroids_ordered[row.HydroID]

        # Separate sides
        if (position % 2 == 0 and row.side == 'left') or (position % 2 != 0 and row.side == 'right'):
            sequences_even.append([row.HydroID, position, row.side])
        else:
            sequences_odd.append([row.HydroID, position, row.side])

    sequences_even_df = pd.DataFrame(sequences_even, columns=['HydroID', 'position', 'side'])
    sequences_odd_df = pd.DataFrame(sequences_odd, columns=['HydroID', 'position', 'side'])

    sequences_even_df['diff'] = -sequences_even_df['position'].diff(-1).fillna(0).astype(int)
    sequences_odd_df['diff'] = -sequences_odd_df['position'].diff(-1).fillna(0).astype(int)

    sequences_even_df['group'] = sequences_even_df['diff'].where(sequences_even_df['diff'] == 1, 0)
    sequences_odd_df['group'] = sequences_odd_df['diff'].where(sequences_odd_df['diff'] == 1, 0)

    y = sequences_even_df['group']
    sequences_even_df['count'] = y * (y.groupby((y != y.shift()).cumsum()).cumcount(ascending=False) + 1)
    z = sequences_odd_df['group']
    sequences_odd_df['count'] = z * (z.groupby((z != z.shift()).cumsum()).cumcount(ascending=False) + 1)

    # Get area from data_dissolved based on HydroID and side
    sequences_even_df = sequences_even_df.merge(
        data_dissolved[['HydroID', 'side', 'area', 'length', 'area_total', 'area_prop']],
        on=['HydroID', 'side'],
        how='left',
    )
    sequences_odd_df = sequences_odd_df.merge(
        data_dissolved[['HydroID', 'side', 'area', 'length', 'area_total', 'area_prop']],
        on=['HydroID', 'side'],
        how='left',
    )

    catchments_copy, reaches_copy = find_and_combine_sequences(
        sequences_even_df, data_dissolved, reaches, catchments_copy, reaches_copy
    )
    catchments_copy, reaches_copy = find_and_combine_sequences(
        sequences_odd_df, data_dissolved, reaches, catchments_copy, reaches_copy
    )

    catchments_copy.to_file(catchments_out, layer=catchments_layername, driver='GPKG', mode='w')
    reaches_copy.to_file(reaches_out, layer=reaches_layername, driver='GPKG', mode='w')

    # catchments_copy.to_file(catchments_filename, layer=catchments_layername, driver='GPKG')
    # reaches_copy.to_file(reaches_filename, layer=reaches_layername, driver='GPKG')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Assess bilateral catchments.")
    parser.add_argument('-c', '--catchments-filename', type=str, help='Path to catchments file')
    parser.add_argument('-r', '--reaches-filename', type=str, help='Path to reaches file')
    parser.add_argument('-co', '--catchments-out', type=str, help='Path to output catchments file')
    parser.add_argument('-ro', '--reaches-out', type=str, help='Path to output reaches file')
    args = parser.parse_args()

    dissolve_unilateral_catchments(**vars(args))
