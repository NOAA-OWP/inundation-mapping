#!/usr/bin/env python3

import argparse
import math
import os

import geopandas as gpd
import pandas as pd
import shapely


def main(
    catchments_filename: str,
    streams_filename: str,
    id_attribute: str,
    from_attribute: str,
    to_attribute: str,
    out_filename: str,
    sink_value=None,
):
    """
    Conflate catchments with network

    Parameters
    ----------
    catchments_filename : str
        Catchments filename
    streams_filename : str
        Streams filename
    id_attribute : str
        Name of ID attribute
    from_attribute : str
        Name of 'from' attribute
    to_attribute : str
        Name of 'to' attribute
    out_filename : str
        Output filename
    sink_value : int
        Sink value
    """

    # Check if input files exist
    assert os.path.exists(catchments_filename), f"Catchments file {catchments_filename} not found"
    assert os.path.exists(streams_filename), f"Streams file {streams_filename} not found"

    # Load data
    catchments = gpd.read_file(catchments_filename)  # NHDPlusID (or GridCode)
    streams = gpd.read_file(streams_filename)  # NHDPlusID (or GridCode) [FromNode, ToNode]

    assert id_attribute in catchments.columns, f"Attribute {id_attribute} not found in catchments"
    assert from_attribute in streams.columns, f"Attribute {from_attribute} not found in streams"
    assert to_attribute in streams.columns, f"Attribute {to_attribute} not found in streams"

    def _get_upstream_area(
        id: int, streams, catchments, headwater_ids, out_catchments: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Recursively find upstream segments and assign ID to confluence DEM reach starting with outlets

        Parameters
        ----------
        upstream_id : int
            Link number
        streams : GeoDataFrame
            Target network
        confluence_ids : list
            List of confluence IDs

        Returns
        -------
        GeoDataFrame
            Target network with upstream IDs
        """

        catchment_geoms = []

        # Get IDs of upstream segments
        upstream_ids = streams.loc[streams[to_attribute] == id, id_attribute]

        if len(upstream_ids) > 0:
            for i, upstream_id in enumerate(upstream_ids):
                # Get feature geometry
                catchment_geom = catchments.loc[catchments[id_attribute] == upstream_id, 'geometry']

                if upstream_id in headwater_ids:
                    if len(catchment_geom) > 0:  # To avoid empty geometries (streams without catchments)
                        # Add headwater catchment to out_catchments
                        out_catchments = pd.concat(
                            [
                                out_catchments,
                                gpd.GeoDataFrame(
                                    [
                                        {
                                            id_attribute: upstream_id,
                                            'downstream_id': id,
                                            'geometry': catchment_geom.values[0],
                                        }
                                    ]
                                ),
                            ]
                        )

                        # Save headwater catchment geometry for dissolving
                        catchment_geoms.append(catchment_geom)

                else:
                    # Recursively find upstream segments
                    out_catchments, upstream_catchment_geoms = _get_upstream_area(
                        upstream_id, streams, catchments, headwater_ids, out_catchments
                    )

                    if len(upstream_catchment_geoms) > 0:
                        catchment_geoms.extend(upstream_catchment_geoms)

            # Add downstream catchment to dissolved catchments
            # Get feature geometry
            catchment_geom = catchments.loc[catchments[id_attribute] == id, 'geometry']

            if len(catchment_geom) > 0:
                catchment_geoms.append(catchment_geom)

            if len(catchment_geoms) > 0:
                catchment_geometry = shapely.unary_union(catchment_geoms)

                downstream_id = streams.loc[streams[id_attribute] == id, to_attribute].values[0]

                # Add dissolved catchment to out_catchments
                out_catchments = pd.concat(
                    [
                        out_catchments,
                        gpd.GeoDataFrame(
                            [
                                {
                                    id_attribute: id,
                                    'downstream_id': downstream_id,
                                    'geometry': catchment_geometry,
                                }
                            ]
                        ),
                    ]
                )

        return out_catchments, catchment_geoms

    headwater_ids = streams.loc[~streams[from_attribute].isin(streams[to_attribute]), id_attribute].values

    # Starting with outlets
    out_catchments = gpd.GeoDataFrame(columns=[id_attribute, 'downstream_id', 'geometry'])
    if sink_value is None:
        for upstream_id in streams.loc[~streams['to'].isin(streams['ID']), 'ID']:
            # Recursively find upstream segments
            out_catchments, _ = _get_upstream_area(
                upstream_id, streams, catchments, headwater_ids, out_catchments
            )

    else:
        for upstream_id in streams.loc[streams['to'] == sink_value, 'ID']:
            # Recursively find upstream segments
            out_catchments, _ = _get_upstream_area(
                upstream_id, streams, catchments, headwater_ids, out_catchments
            )

    # Save output
    out_catchments.to_file(out_filename, crs=catchments.crs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Conflate NWM feature_ids to NHDPlus network')
    parser.add_argument('-catch', '--catchments-filename', help='Catchments filename')
    parser.add_argument('-stream', '--streams-filename', help='Streams filename')
    parser.add_argument('-id', '--id-attribute', help='Name of ID attribute')
    parser.add_argument('-to', '--to-attribute', help='Name of to attribute')
    parser.add_argument('-from', '--from-attribute', help='Name of from attribute')
    parser.add_argument('-sink', '--sink-value', help='Sink value', default=None, required=False)
    parser.add_argument('-out', '--out-filename', help='Output filename')

    args = vars(parser.parse_args())

    main(**args)
