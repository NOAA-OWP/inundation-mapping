#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import pandas as pd
import shapely


gpd.options.io_engine = "pyogrio"


def make_upstream_watersheds(
    catchments_filename: str,
    streams_filename: str,
    id_attribute: str,
    from_attribute: str,
    to_attribute: str,
    out_filename: str,
    wbd_buffered=None,
):
    """
    Make upstream watersheds for each stream segment (reach) in a stream network

    Parameters
    ----------
    catchments_filename : str
        Catchments filename. Assumes 'NHD' or 'NWM' in filename.
    streams_filename : str
        Streams filename
    id_attribute : str
        Name of ID attribute
    from_attribute : str
        Name of 'from' attribute
    to_attribute : str
        Name of to_attribute attribute
    out_filename : str
        Output filename
    wbd_buffered : str or None
        WBD buffered polygon layer filename
    """

    def _get_upstream_catchment(
        id: int, streams, catchments, headwater_ids, out_catchments: gpd.GeoDataFrame
    ) -> tuple[gpd.GeoDataFrame, list]:
        """
        Recursively find upstream segments

        Parameters
        ----------
        id : int
            Link number
        streams : GeoDataFrame
            Target network
        catchments : GeoDataFrame
            Catchments
        headwater_ids : list
            List of headwater IDs
        out_catchments : GeoDataFrame
            Output catchments

        Returns
        -------
        gpd.GeoDataFrame
            Output catchments
        list
            List of catchment geometries
        """

        catchment_geoms = []

        # Get IDs of upstream segments
        upstream_ids = streams.loc[streams[to_attribute] == id, from_attribute]

        if len(upstream_ids) > 0:
            for i, upstream_id in enumerate(upstream_ids):
                # Get feature geometry
                catchment_id = streams.loc[streams[from_attribute] == upstream_id, id_attribute].values[0]
                catchment_geom = catchments.loc[catchments[id_attribute] == catchment_id, 'geometry']

                if upstream_id in headwater_ids:
                    if len(catchment_geom) > 0:  # To avoid empty geometries (streams without catchments)
                        # Add headwater catchment to out_catchments
                        out_catchments = pd.concat(
                            [
                                out_catchments,
                                gpd.GeoDataFrame(
                                    [
                                        {
                                            'id': catchment_id,
                                            'from': upstream_id,
                                            'to': id,
                                            'geometry': catchment_geom.values[0],
                                        }
                                    ],
                                    crs=catchments.crs,
                                ),
                            ]
                        )

                        # Save headwater catchment geometry for dissolving
                        catchment_geoms.append(catchment_geom)

                else:
                    # Recursively find upstream segments
                    out_catchments, upstream_catchment_geoms = _get_upstream_catchment(
                        upstream_id, streams, catchments, headwater_ids, out_catchments
                    )

                    if len(upstream_catchment_geoms) > 0:
                        catchment_geoms.extend(upstream_catchment_geoms)

            # Add downstream catchment to dissolved catchments
            # Get feature geometry
            catchment_id = streams.loc[streams[from_attribute] == id, id_attribute].values[0]
            catchment_geom = catchments.loc[catchments[id_attribute] == catchment_id, 'geometry']

            if len(catchment_geom) > 0:
                catchment_geoms.append(catchment_geom)

            if len(catchment_geoms) > 0:
                catchment_geometry = shapely.unary_union(catchment_geoms)

                downstream_id = streams.loc[streams[from_attribute] == id, to_attribute].values[0]

                # Add dissolved catchment to out_catchments
                out_catchments = pd.concat(
                    [
                        out_catchments,
                        gpd.GeoDataFrame(
                            [
                                {
                                    'id': catchment_id,
                                    'from': id,
                                    'to': downstream_id,
                                    'geometry': catchment_geometry,
                                }
                            ],
                            crs=catchments.crs,
                        ),
                    ]
                )

        return out_catchments, catchment_geoms

    if 'nwm' in catchments_filename.lower():
        # Check if input files exist
        assert os.path.exists(streams_filename), f"Streams file {streams_filename} not found"
        assert os.path.exists(catchments_filename), f"Catchments file {catchments_filename} not found"

        # Load data
        catchments = gpd.read_file(catchments_filename)  # NHDPlusID (or GridCode)
        streams = gpd.read_file(streams_filename)  # NHDPlusID (or GridCode) [FromNode, ToNode]

    elif 'nhd' in catchments_filename.lower():
        # Check if input files exist
        streams_file = os.path.split(streams_filename)
        catchments_file = os.path.split(catchments_filename)

        assert os.path.exists(wbd_buffered), f"WBD buffered file {wbd_buffered} not found"
        assert os.path.exists(streams_file[0]), f"Streams file {streams_file[0]} not found"
        assert os.path.exists(catchments_file[0]), f"Catchments file {catchments_file[0]} not found"

        # Clip NHDPlus catchments and streams to WBD
        wbd_buffered = gpd.read_file(wbd_buffered)

        streams = gpd.read_file(streams_file[0], layer=streams_file[1], mask=wbd_buffered, engine='fiona')

        # Need to extract catchments
        catchments = gpd.read_file(
            catchments_file[0], layer=catchments_file[1], mask=wbd_buffered, engine='fiona'
        )

    else:
        raise ValueError(f"NWM or NHD not found in catchments_filename {catchments_filename}")

    headwater_ids = streams.loc[~streams[from_attribute].isin(streams[to_attribute]), from_attribute].values

    # Create empty GeoDataFrame for saving results
    out_catchments = gpd.GeoDataFrame(columns=['id', 'from', 'to', 'geometry'])
    out_catchments.crs = catchments.crs

    # Starting with outlets
    for upstream_id in streams.loc[~streams[to_attribute].isin(streams[from_attribute]), from_attribute]:
        # Recursively find upstream segments
        out_catchments, _ = _get_upstream_catchment(
            upstream_id, streams, catchments, headwater_ids, out_catchments
        )

    # Save output
    out_catchments.to_file(out_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Conflate NWM feature_ids to NHDPlus network')
    parser.add_argument('-catch', '--catchments-filename', help='Catchments filename')
    parser.add_argument('-stream', '--streams-filename', help='Streams filename')
    parser.add_argument('-id', '--id-attribute', help='Name of ID attribute')
    parser.add_argument('-to', '--to-attribute', help='Name of to attribute')
    parser.add_argument('-from', '--from-attribute', help='Name of from attribute')
    parser.add_argument(
        '-wbd', '--wbd-buffered', help='WBD buffered polygon layer filename', default=None, required=False
    )
    parser.add_argument('-out', '--out-filename', help='Output filename')

    args = vars(parser.parse_args())

    make_upstream_watersheds(**args)
