#!/usr/bin/env python3

import argparse
import math
import os

import geopandas as gpd


def conflate_networks(
    headwater_points_fileName,
    # levelpath_points_fileName,
    reference_network_fileName,
    target_network_fileName,
    out_network_fileName,
):
    """
    Conflate headwater and levelpath points to reference and target networks

    Parameters
    ----------
    headwater_points_fileName : str
        Headwater points filename
    # levelpath_points_fileName : str
    #     Levelpath points filename
    reference_network_fileName : str
        Reference network filename
    target_network_fileName : str
        Target network filename
    out_network_fileName : str
        Out network filename

    Returns
    -------
    None
    """

    # Check if input files exist
    assert os.path.exists(
        headwater_points_fileName
    ), f"Headwater points file {headwater_points_fileName} not found"
    # assert os.path.exists(
    #     levelpath_points_fileName
    # ), f"Levelpath points file {levelpath_points_fileName} not found"
    assert os.path.exists(
        reference_network_fileName
    ), f"Reference network file {reference_network_fileName} not found"
    assert os.path.exists(target_network_fileName), f"Target network file {target_network_fileName} not found"

    # Load data
    headwater_points = gpd.read_file(headwater_points_fileName)
    # levelpath_points = gpd.read_file(levelpath_points_fileName)
    reference_network = gpd.read_file(reference_network_fileName, columns=["ID", "geometry"])
    target_network = gpd.read_file(target_network_fileName)

    reference_network = reference_network.rename(columns={"ID": "feature_id"})

    # Join ID from reference network to headwater point (NOTE: snap to NHDPlus?)
    headwater_points = gpd.sjoin_nearest(headwater_points, reference_network, max_distance=100)
    headwater_points = headwater_points.drop(columns=["index_right"])
    out_network = gpd.sjoin_nearest(target_network, headwater_points, how="left", max_distance=10)
    reference_network['feature_id'] = reference_network['feature_id'].astype(int)
    reference_network['to'] = reference_network['to'].astype(int)

    # Find confluence IDs
    confluence_ids = reference_network.groupby("to").size()
    confluence_ids = list(confluence_ids[confluence_ids > 1].index)

    def _find_downstream_confluence_id(
        id: int, confluence_ids: list, reference_network: gpd.GeoDataFrame
    ) -> int:
        """
        Recursively find downstream confluence ID

        Parameters
        ----------
        id : int
            Link number
        confluence_ids : list
            List of confluence IDs
        reference_network : GeoDataFrame
            Reference network

        Returns
        -------
        int
            Downstream confluence ID
        """

        # Get IDs of downstream segments
        downstream_id = int(reference_network.loc[reference_network['feature_id'] == id, 'to'].iloc[0])

        # Find downstream confluence ID
        if downstream_id in confluence_ids:
            downstream_confluence_id = downstream_id
        else:
            # Recursively find downstream confluence ID
            downstream_confluence_id = _find_downstream_confluence_id(
                downstream_id, confluence_ids, reference_network
            )

        return downstream_confluence_id

    def _add_upstream_ids(
        id: int, confluence_ids: list, target_network: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Recursively find upstream segments and assign ID to confluence DEM reach starting with outlets

        Parameters
        ----------
        upstream_id : int
            Link number
        target_network : GeoDataFrame
            Target network
        confluence_ids : list
            List of confluence IDs

        Returns
        -------
        GeoDataFrame
            Target network with upstream IDs
        """

        print('id', id)

        if id == 722:
            pass

        # Get IDs of upstream segments
        upstream_ids = target_network.loc[target_network['DSLINKNO'] == id, 'LINKNO']

        # Assign ID to confluence DEM reach starting at upstream_id
        upstream_id = None
        if len(upstream_ids) > 0:
            feature_ids = []
            for upstream_id in upstream_ids:
                print('\tupstream_id', upstream_id)

                upstream_feature_id = target_network.loc[
                    target_network['LINKNO'] == upstream_id, 'feature_id'
                ].item()

                print('\tupstream_feature_id', upstream_feature_id)

                if not math.isnan(upstream_feature_id):
                    upstream_feature_id = int(upstream_feature_id)
                    feature_id = reference_network.loc[
                        reference_network['feature_id'] == upstream_feature_id, 'to'
                    ].item()
                    if feature_id not in confluence_ids:
                        feature_id = _find_downstream_confluence_id(
                            feature_id, confluence_ids, reference_network
                        )

                    print('\tfeature_id', feature_id)
                    feature_ids.append(feature_id)

                else:
                    # Recursively find upstream segments
                    target_network = _add_upstream_ids(upstream_id, confluence_ids, target_network)

                    up_fid = target_network.loc[target_network['LINKNO'] == upstream_id, 'feature_id'].item()
                    feature_id = reference_network.loc[reference_network['feature_id'] == up_fid, 'to'].item()

            if len(feature_ids) > 0:
                # feature_ids = [x for x in feature_ids if x in confluence_ids]
                feature_id = max(feature_ids, key=feature_ids.count)
                target_network.loc[target_network['LINKNO'] == id, 'feature_id'] = feature_id
                print(f'--> Assigned {feature_id} to {id}\n')

        return target_network

    # Assign ID to confluence DEM reach starting with outlets
    for upstream_id in out_network.loc[out_network['DSLINKNO'] == -1, 'LINKNO']:
        # Recursively find upstream segments
        out_network = _add_upstream_ids(upstream_id, confluence_ids, out_network)

    out_network.to_file(out_network_fileName)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Conflate networks")
    parser.add_argument(
        "-hp", "--headwater-points-fileName", help="Headwater points filename", type=str, required=True
    )
    # parser.add_argument(
    #     "-lp", "--levelpath-points-fileName", help="Levelpath points filename", type=str, required=True
    # )
    parser.add_argument(
        "-rn", "--reference-network-fileName", help="Reference network filename", type=str, required=True
    )
    parser.add_argument(
        "-tn", "--target-network-fileName", help="Target network filename", type=str, required=True
    )
    parser.add_argument("-on", "--out-network-fileName", help="Out network filename", type=str, required=True)

    args = vars(parser.parse_args())

    conflate_networks(**args)
