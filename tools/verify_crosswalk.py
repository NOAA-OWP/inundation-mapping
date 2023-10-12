#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd


def verify_crosswalk(input_flows_fileName, input_nwmflows_fileName, output_table_fileName):
    """
    Verify the crosswalk between the NWM and DEM-derived flowlines.

    Parameters
    ----------
    input_flows_fileName : str
        DEM-derived flowlines
    input_nwmflows_fileName : str
        NWM flowlines
    crosswalk_fileName : str
        Crosswalk table filename
    """

    nwm_streams = gpd.read_file(input_nwmflows_fileName)
    flows = gpd.read_file(input_flows_fileName)

    # Crosswalk check
    # fh.vprint('Checking for crosswalks between NWM and DEM-derived flowlines', verbose)

    # Compute the number of intersections between the NWM and DEM-derived flowlines
    streams = nwm_streams
    xwalks = []
    intersects = flows.sjoin(streams)
    intersects['HydroID'] = intersects['HydroID'].astype(int)
    intersects['feature_id_right'] = intersects['feature_id_right'].astype(int)

    for idx in intersects.index:
        flows_idx = int(intersects.loc[idx, 'HydroID'].unique())

        streams_idxs = intersects.loc[idx, 'feature_id_right'].unique()

        for streams_idx in streams_idxs:
            intersect = gpd.overlay(
                flows[flows['HydroID'] == flows_idx],
                nwm_streams[nwm_streams['feature_id'] == streams_idx],
                keep_geom_type=False,
            )

            if intersect.geometry[0].geom_type == 'Point':
                intersect_points = 1
            else:
                intersect_points = len(intersect.geometry[0].geoms)

            xwalks.append(
                [
                    flows_idx,
                    int(flows.loc[flows['HydroID'] == flows_idx, 'feature_id'].iloc[0]),
                    streams_idx,
                    intersect_points,
                ]
            )

            print(f'Found {intersect_points} intersections for {flows_idx} and {streams_idx}')

    # Get the maximum number of intersections for each flowline
    xwalks = pd.DataFrame(xwalks, columns=['HydroID', 'feature_id', 'feature_id_right', 'intersect_points'])
    xwalks = xwalks.drop_duplicates()
    xwalks['match'] = xwalks[1] == xwalks[2]

    xwalks_groupby = xwalks[[0, 3]].groupby(0).max()

    xwalks = xwalks.merge(xwalks_groupby, on=0)
    xwalks['max'] = xwalks['3_x'] == xwalks['3_y']

    xwalks['crosswalk'] = np.where(xwalks['match'] == xwalks['max'], True, False)

    # Save the crosswalk table
    xwalks.to_csv(output_table_fileName, index=False)

    return xwalks


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Crosswalk for MS/FR/GMS networks; calculate synthetic rating curves; update short rating curves'
    )
    parser.add_argument('-a', '--input-flows-fileName', help='DEM derived streams', required=True)
    parser.add_argument('-b', '--input-nwmflows-fileName', help='Subset NWM burnlines', required=True)
    parser.add_argument('-c', '--output-table-fileName', help='Output table filename', required=True)

    args = vars(parser.parse_args())

    verify_crosswalk(**args)
