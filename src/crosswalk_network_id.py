#!/usr/bin/env python3

import argparse
import sys

import geopandas as gpd


# from rasterstats import zonal_stats


def conflate_nwm_feature_ids(
    input_flows_fileName,
    input_flows_id_attribute,
    input_catchments_fileName,
    method,
    input_huc_fileName,
    input_nwmflows_fileName,
    input_nwmflows_levelPath_fileName,
    input_nwmcat_fileName,
    input_nwmcatras_fileName,
    output_flows_fileName,
    output_flows_levelPath_fileName,
    output_catchments_fileName,
    crosswalk_fileName,
):
    """
    Conflate NWM stream IDs with NHD stream IDs

    Parameters
    ----------
    nwm_fileName : str
        Path to NWM feature class
    input_flows_fileName : str
        Path to NHD streams feature class
    """

    # def _majority_catchment(input_flows, input_catchments, input_nwmcatras_fileName):
    #     ## crosswalk using majority catchment method

    #     # calculate majority catchments
    #     majority_calc = zonal_stats(
    #         input_catchments, input_nwmcatras_fileName, stats=['majority'], geojson_out=True
    #     )
    #     input_majorities = gpd.GeoDataFrame.from_features(majority_calc)
    #     input_majorities = input_majorities.rename(columns={'majority': 'feature_id'})

    #     input_majorities = input_majorities[:][input_majorities['feature_id'].notna()]
    #     if input_majorities.feature_id.dtype != 'int':
    #         input_majorities.feature_id = input_majorities.feature_id.astype(int)
    #     if input_majorities.HydroID.dtype != 'int':
    #         input_majorities.HydroID = input_majorities.HydroID.astype(int)

    #     input_nwmflows = input_nwmflows.rename(columns={'ID': 'feature_id'})
    #     if input_nwmflows.feature_id.dtype != 'int':
    #         input_nwmflows.feature_id = input_nwmflows.feature_id.astype(int)
    #     relevant_input_nwmflows = input_nwmflows[
    #         input_nwmflows['feature_id'].isin(input_majorities['feature_id'])
    #     ]
    #     relevant_input_nwmflows = relevant_input_nwmflows.filter(items=['feature_id', 'order_'])

    #     if input_catchments.HydroID.dtype != 'int':
    #         input_catchments.HydroID = input_catchments.HydroID.astype(int)
    #     output_catchments = input_catchments.merge(input_majorities[['HydroID', 'feature_id']], on='HydroID')
    #     output_catchments = output_catchments.merge(
    #         relevant_input_nwmflows[['order_', 'feature_id']], on='feature_id'
    #     )

    #     if input_flows.HydroID.dtype != 'int':
    #         input_flows.HydroID = input_flows.HydroID.astype(int)
    #     output_flows = input_flows.merge(input_majorities[['HydroID', 'feature_id']], on='HydroID')
    #     if output_flows.HydroID.dtype != 'int':
    #         output_flows.HydroID = output_flows.HydroID.astype(int)
    #     output_flows = output_flows.merge(relevant_input_nwmflows[['order_', 'feature_id']], on='feature_id')
    #     output_flows = output_flows.merge(
    #         output_catchments.filter(items=['HydroID', 'areasqkm']), on='HydroID'
    #     )

    #     return output_flows#, output_catchments

    def _segment_midpoint(
        input_flows, input_flows_id_attribute, input_nwmflows, input_nwmcat, crosswalk_fileName, extent='GMS'
    ):
        ## crosswalk using stream segment midpoint method

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

        if input_flows_id_attribute != 'HydroID':
            input_flows = input_flows.rename(columns={input_flows_id_attribute: 'HydroID'})

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
        crosswalk = crosswalk.rename(columns={"index_right": "feature_id"})

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
            print("No relevant streams within HUC boundaries.")
            sys.exit(0)

        else:
            crosswalk.to_csv(crosswalk_fileName)

        # if input_catchments.HydroID.dtype != 'int':
        #     input_catchments.HydroID = input_catchments.HydroID.astype(int)
        # output_catchments = input_catchments.merge(crosswalk, on='HydroID')

        if input_flows.HydroID.dtype != 'int':
            input_flows.HydroID = input_flows.HydroID.astype(int)
        output_flows = input_flows.merge(crosswalk, on='HydroID')

        # # added for GMS. Consider adding filter_catchments_and_add_attributes.py to run_by_branch.sh
        # if 'areasqkm' not in output_catchments.columns:
        #     output_catchments['areasqkm'] = output_catchments.geometry.area / (1000**2)

        # output_flows = output_flows.merge(
        #     output_catchments.filter(items=['HydroID', 'areasqkm']), on='HydroID'
        # )

        return output_flows, crosswalk

    input_flows = gpd.read_file(input_flows_fileName)
    # input_catchments = gpd.read_file(input_catchments_fileName)

    if method == 'segment-midpoint' and input_nwmcat_fileName is not None:
        input_flows = gpd.read_file(input_flows_fileName)
        input_nwmflows = gpd.read_file(input_nwmflows_fileName)

        input_huc = gpd.read_file(input_huc_fileName)
        input_nwmcat = gpd.read_file(input_nwmcat_fileName, mask=input_huc)

        output_flows, crosswalk = _segment_midpoint(
            input_flows, input_flows_id_attribute, input_nwmflows, input_nwmcat, crosswalk_fileName
        )

    # elif method == 'majority catchment' and input_nwmcatras_fileName is not None:
    #     output_flows, output_catchments = _majority_catchment(input_flows, input_catchments, input_nwmcatras_fileName)

    else:
        print("Invalid inputs for conflate_network_attributes.py")
        sys.exit(0)

    output_flows.to_file(output_flows_fileName)
    # output_catchments.to_file(output_catchments_fileName)
    crosswalk.to_csv(crosswalk_fileName)

    # Filter levelpaths
    input_nwmflows_levelPath = gpd.read_file(input_nwmflows_levelPath_fileName)
    output_flows_levelPath = output_flows[output_flows['StreamOrde'] > 2]
    output_flows_levelPath = output_flows_levelPath[
        output_flows_levelPath['feature_id'].isin(input_nwmflows_levelPath['ID'])
    ]

    output_flows_levelPath.to_file(output_flows_levelPath_fileName)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-if', '--input-flows-fileName', type=str, required=True, help='Path to input feature class'
    )
    parser.add_argument(
        '-id', '--input-flows-id-attribute', type=str, required=True, help='Path to input feature class'
    )
    parser.add_argument(
        '-ic', '--input-catchments-fileName', type=str, required=True, help='Path to catchment feature class'
    )
    parser.add_argument('-method', '--method', type=str, required=True, help='Crosswalking method to use')
    parser.add_argument(
        '-huc', '--input-huc-fileName', type=str, required=True, help='Path to HUC feature class'
    )
    parser.add_argument(
        '-nwm', '--input-nwmflows-fileName', type=str, required=True, help='Path to NWM streams feature class'
    )
    parser.add_argument(
        '-lp',
        '--input-nwmflows-levelPath-fileName',
        type=str,
        required=True,
        help='Path to NWM level path feature class',
    )
    parser.add_argument(
        '-cat', '--input-nwmcat-fileName', type=str, required=False, help='Path to NWM feature class'
    )
    parser.add_argument(
        '-ras', '--input-nwmcatras-fileName', type=str, required=False, help='Path to NWM raster class'
    )
    parser.add_argument(
        '-of', '--output-flows-fileName', type=str, required=True, help='Path to output flows feature class'
    )
    parser.add_argument(
        '-ol',
        '--output-flows-levelPath-fileName',
        type=str,
        required=True,
        help='Path to output flows level path feature class',
    )
    parser.add_argument(
        '-oc',
        '--output-catchments-fileName',
        type=str,
        required=True,
        help='Path to output catchments feature class',
    )
    parser.add_argument('-cw', '--crosswalk-fileName', type=str, required=True, help='Path to crosswalk file')

    args = vars(parser.parse_args())

    conflate_nwm_feature_ids(**args)
