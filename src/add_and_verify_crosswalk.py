#!/usr/bin/env python3

import numpy as np
import geopandas as gpd
from rasterstats import zonal_stats
import argparse
import sys
from utils.shared_functions import mem_profile, getDriver

# Feb 17, 2023
# We want to explore using FR methodology as branch zero


@mem_profile
def add_and_verify_crosswalk(
    input_catchments_fileName,
    input_flows_fileName,
    input_huc_fileName,
    input_nwmflows_fileName,
    input_nwmcatras_fileName,
    input_nwmcat_fileName,
    crosswalk_fileName,
    output_catchments_fileName,
    output_flows_fileName,
    extent,
    min_catchment_area,
    min_stream_length,
):
    input_catchments = gpd.read_file(input_catchments_fileName)
    input_flows = gpd.read_file(input_flows_fileName)
    input_huc = gpd.read_file(input_huc_fileName)
    input_nwmflows = gpd.read_file(input_nwmflows_fileName)
    min_catchment_area = float(min_catchment_area)  # 0.25#
    min_stream_length = float(min_stream_length)  # 0.5#

    if extent == 'FR':
        ## crosswalk using majority catchment method

        # calculate majority catchments
        majority_calc = zonal_stats(
            input_catchments, input_nwmcatras_fileName, stats=['majority'], geojson_out=True
        )
        input_majorities = gpd.GeoDataFrame.from_features(majority_calc)
        input_majorities = input_majorities.rename(columns={'majority': 'feature_id'})

        input_majorities = input_majorities[:][input_majorities['feature_id'].notna()]
        if input_majorities.feature_id.dtype != 'int':
            input_majorities.feature_id = input_majorities.feature_id.astype(int)
        if input_majorities.HydroID.dtype != 'int':
            input_majorities.HydroID = input_majorities.HydroID.astype(int)

        if len(input_majorities) < 1:
            print('No relevant streams within HUC boundaries.')
            sys.exit(0)
        else:
            input_majorities.to_file(crosswalk_fileName, index=False)

        input_nwmflows = input_nwmflows.rename(columns={'ID': 'feature_id'})
        if input_nwmflows.feature_id.dtype != 'int':
            input_nwmflows.feature_id = input_nwmflows.feature_id.astype(int)
        relevant_input_nwmflows = input_nwmflows[
            input_nwmflows['feature_id'].isin(input_majorities['feature_id'])
        ]
        relevant_input_nwmflows = relevant_input_nwmflows.filter(items=['feature_id', 'order_'])

        if input_catchments.HydroID.dtype != 'int':
            input_catchments.HydroID = input_catchments.HydroID.astype(int)
        output_catchments = input_catchments.merge(input_majorities[['HydroID', 'feature_id']], on='HydroID')
        output_catchments = output_catchments.merge(
            relevant_input_nwmflows[['order_', 'feature_id']], on='feature_id'
        )

        if input_flows.HydroID.dtype != 'int':
            input_flows.HydroID = input_flows.HydroID.astype(int)
        output_flows = input_flows.merge(input_majorities[['HydroID', 'feature_id']], on='HydroID')
        if output_flows.HydroID.dtype != 'int':
            output_flows.HydroID = output_flows.HydroID.astype(int)
        output_flows = output_flows.merge(relevant_input_nwmflows[['order_', 'feature_id']], on='feature_id')
        output_flows = output_flows.merge(
            output_catchments.filter(items=['HydroID', 'areasqkm']), on='HydroID'
        )

    elif (extent == 'MS') | (extent == 'GMS'):
        ## crosswalk using stream segment midpoint method
        input_nwmcat = gpd.read_file(input_nwmcat_fileName, mask=input_huc)

        # only reduce nwm catchments to mainstems if running mainstems
        if extent == 'MS':
            input_nwmcat = input_nwmcat.loc[input_nwmcat.mainstem == 1]

        input_nwmcat = input_nwmcat.rename(columns={'ID': 'feature_id'})
        if input_nwmcat.feature_id.dtype != 'int':
            input_nwmcat.feature_id = input_nwmcat.feature_id.astype(int)
        input_nwmcat = input_nwmcat.set_index('feature_id')

        input_nwmflows = input_nwmflows.rename(columns={'ID': 'feature_id'})
        if input_nwmflows.feature_id.dtype != 'int':
            input_nwmflows.feature_id = input_nwmflows.feature_id.astype(int)

        # Get stream midpoint
        stream_midpoint = []
        hydroID = []
        for i, lineString in enumerate(input_flows.geometry):
            hydroID = hydroID + [input_flows.loc[i, 'HydroID']]
            stream_midpoint = stream_midpoint + [lineString.interpolate(0.5, normalized=True)]

        input_flows_midpoint = gpd.GeoDataFrame(
            {'HydroID': hydroID, 'geometry': stream_midpoint}, crs=input_flows.crs, geometry='geometry'
        )
        input_flows_midpoint = input_flows_midpoint.set_index('HydroID')

        # Create crosswalk
        crosswalk = gpd.sjoin(
            input_flows_midpoint, input_nwmcat, how='left', predicate='within'
        ).reset_index()
        crosswalk = crosswalk.rename(columns={'index_right': 'feature_id'})

        # fill in missing ms
        crosswalk_missing = crosswalk.loc[crosswalk.feature_id.isna()]
        for index, stream in crosswalk_missing.iterrows():
            # find closest nwm catchment by distance
            distances = [stream.geometry.distance(poly) for poly in input_nwmcat.geometry]
            min_dist = min(distances)
            nwmcat_index = distances.index(min_dist)

            # update crosswalk
            crosswalk.loc[crosswalk.HydroID == stream.HydroID, 'feature_id'] = input_nwmcat.iloc[
                nwmcat_index
            ].name
            crosswalk.loc[crosswalk.HydroID == stream.HydroID, 'AreaSqKM'] = input_nwmcat.iloc[
                nwmcat_index
            ].AreaSqKM
            crosswalk.loc[crosswalk.HydroID == stream.HydroID, 'Shape_Length'] = input_nwmcat.iloc[
                nwmcat_index
            ].Shape_Length
            crosswalk.loc[crosswalk.HydroID == stream.HydroID, 'Shape_Area'] = input_nwmcat.iloc[
                nwmcat_index
            ].Shape_Area

        crosswalk = crosswalk.filter(items=['HydroID', 'feature_id'])
        crosswalk = crosswalk.merge(input_nwmflows[['feature_id', 'order_']], on='feature_id')

        if len(crosswalk) < 1:
            print('No relevant streams within HUC boundaries.')
            sys.exit(0)
        else:
            crosswalk.to_csv(crosswalk_fileName, index=False)

        if input_catchments.HydroID.dtype != 'int':
            input_catchments.HydroID = input_catchments.HydroID.astype(int)
        output_catchments = input_catchments.merge(crosswalk, on='HydroID')

        if input_flows.HydroID.dtype != 'int':
            input_flows.HydroID = input_flows.HydroID.astype(int)
        output_flows = input_flows.merge(crosswalk, on='HydroID')

        # added for GMS. Consider adding filter_catchments_and_add_attributes.py to run_by_branch.sh
        if 'areasqkm' not in output_catchments.columns:
            output_catchments['areasqkm'] = output_catchments.geometry.area / (1000**2)

        output_flows = output_flows.merge(
            output_catchments.filter(items=['HydroID', 'areasqkm']), on='HydroID'
        )

        # write out
        output_catchments.to_file(
            output_catchments_fileName, driver=getDriver(output_catchments_fileName), index=False
        )

    if output_flows.NextDownID.dtype != 'int':
        output_flows.NextDownID = output_flows.NextDownID.astype(int)

    output_flows = verify_crosswalk(output_flows, input_nwmflows)

    output_flows.to_file(output_flows_fileName, index=False)


def verify_crosswalk(flows, nwm_streams):
    # Crosswalk check
    # fh.vprint('Checking for crosswalks between NWM and DEM-derived flowlines', verbose)

    # Compute the number of intersections between the NWM and DEM-derived flowlines
    streams = nwm_streams
    n_intersects = 0
    xwalks = []
    intersects = flows.sjoin(streams)
    for idx in intersects.index:
        if type(intersects.loc[idx, 'HydroID']) == np.int64:
            flows_idx = intersects.loc[idx, 'HydroID']
        else:
            flows_idx = int(intersects.loc[idx, 'HydroID'].unique())

        if type(intersects.loc[idx, 'feature_id_right']) == np.int64:
            streams_idxs = [intersects.loc[idx, 'feature_id_right']]
        else:
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

            if intersect_points > n_intersects:
                n_intersects = intersect_points

            if n_intersects > 0:
                xwalks.append((flows_idx, streams_idx, intersect_points))

                print(f'Found {intersect_points} intersections for {flows_idx} and {streams_idx}')

    # Get the maximum number of intersections for each flowline
    xwalks = np.array(xwalks)

    return flows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Crosswalk for MS/FR/GMS networks; calculate synthetic rating curves; update short rating curves'
    )
    parser.add_argument('-d', '--input-catchments-fileName', help='DEM derived catchments', required=True)
    parser.add_argument('-a', '--input-flows-fileName', help='DEM derived streams', required=True)
    parser.add_argument('-w', '--input-huc-fileName', help='HUC8 boundary', required=True)
    parser.add_argument('-b', '--input-nwmflows-fileName', help='Subset NWM burnlines', required=True)
    parser.add_argument('-y', '--input-nwmcatras-fileName', help='NWM catchment raster', required=False)
    parser.add_argument('-z', '--input-nwmcat-fileName', help='NWM catchment polygon', required=True)
    parser.add_argument('-c', '--crosswalk-fileName', help='Crosswalk table filename', required=True)
    parser.add_argument(
        '-l', '--output-catchments-fileName', help='Subset crosswalked catchments', required=True
    )
    parser.add_argument('-p', '--extent', help='GMS only for now', default='GMS', required=False)
    parser.add_argument('-e', '--min-catchment-area', help='Minimum catchment area', required=True)
    parser.add_argument('-g', '--min-stream-length', help='Minimum stream length', required=True)
    parser.add_argument('-f', '--output-flows-fileName', help='Subset crosswalked streams', required=True)

    args = vars(parser.parse_args())

    add_and_verify_crosswalk(**args)
