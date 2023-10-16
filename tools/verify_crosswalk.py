#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd


def verify_crosswalk(input_flows_fileName, input_nwmflows_fileName, output_table_fileName):
    # Crosswalk check
    # fh.vprint('Checking for crosswalks between NWM and DEM-derived flowlines', verbose)

    flows = gpd.read_file(input_flows_fileName)
    nwm_streams = gpd.read_file(input_nwmflows_fileName)

    # Compute the number of intersections between the NWM and DEM-derived flowlines
    streams = nwm_streams
    xwalks = []
    intersects = flows.sjoin(streams)

    for idx in intersects.index:
        flows_idx = intersects.loc[intersects.index == idx, 'HydroID'].unique()

        if type(intersects.loc[idx, 'ID']) == np.int64:
            streams_idxs = [intersects.loc[idx, 'ID']]
        else:
            streams_idxs = intersects.loc[idx, 'ID'].unique()

        for flows_id in flows_idx:
            for streams_idx in streams_idxs:
                intersect = gpd.overlay(
                    flows[flows['HydroID'] == flows_id],
                    nwm_streams[nwm_streams['ID'] == streams_idx],
                    keep_geom_type=False,
                )

                if len(intersect) == 0:
                    intersect_points = 0
                    feature_id = flows.loc[flows['HydroID'] == flows_id, 'feature_id']
                elif intersect.geometry[0].geom_type == 'Point':
                    intersect_points = 1
                    feature_id = flows.loc[flows['HydroID'] == flows_id, 'feature_id']
                else:
                    intersect_points = len(intersect.geometry[0].geoms)
                    feature_id = int(flows.loc[flows['HydroID'] == flows_id, 'feature_id'].iloc[0])

                xwalks.append([flows_id, feature_id, streams_idx, intersect_points])

                print(f'Found {intersect_points} intersections for {flows_id} and {streams_idx}')

    # Get the maximum number of intersections for each flowline
    xwalks = pd.DataFrame(xwalks, columns=['HydroID', 'feature_id', 'ID', 'intersect_points'])
    xwalks['feature_id'] = xwalks['feature_id'].astype(int)

    xwalks['match'] = xwalks['feature_id'] == xwalks['ID']

    xwalks_groupby = xwalks[['HydroID', 'intersect_points']].groupby('HydroID').max()

    xwalks = xwalks.merge(xwalks_groupby, on='HydroID', how='left')
    xwalks['max'] = xwalks['intersect_points_x'] == xwalks['intersect_points_y']

    xwalks['crosswalk'] = (xwalks['match'] is True) & (xwalks['max'] is True)

    xwalks = xwalks.drop_duplicates()

    xwalks = xwalks.sort_values(by=['HydroID', 'intersect_points_x', 'ID'], ascending=False)

    xwalks.to_csv(output_table_fileName, index=False)

    return flows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Crosswalk for MS/FR/GMS networks; calculate synthetic rating curves; update short rating curves'
    )
    parser.add_argument('-a', '--input-flows-fileName', help='DEM derived streams', required=True)
    parser.add_argument('-b', '--input-nwmflows-fileName', help='Subset NWM burnlines', required=True)
    parser.add_argument('-c', '--output-table-fileName', help='Output table filename', required=True)

    args = vars(parser.parse_args())

    verify_crosswalk(**args)
