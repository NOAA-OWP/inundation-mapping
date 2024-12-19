#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd


gpd.options.io_engine = "pyogrio"


def evaluate_crosswalk(
    input_flows_fileName: str,
    input_nwmflows_fileName: str,
    input_nwm_headwaters_fileName: str,
    output_table_fileName: str,
    huc: str,
    branch: str,
):
    """
    Tool to check the accuracy of crosswalked attributes using two methods: counting the number of intersections between two stream representations and network, which checks the upstream and downstream connectivity of each stream segment.

    Parameters
    ----------
    input_flows_fileName : str
        Path to DEM derived streams
    input_nwmflows_fileName : str
        Path to subset NWM burnlines
    input_nwm_headwaters_fileName : str
        Path to subset NWM headwaters
    output_table_fileName : str
        Path to output table filename
    huc : str
        HUC ID
    branch : str
        Branch ID

    Returns
    -------
    results : pandas.DataFrame

    Usage
    -----
    python evaluate_crosswalk.py -a <input_flows_fileName> -b <input_nwmflows_fileName> -d <input_nwm_headwaters_fileName> -c <output_table_fileName> -u <huc> -z <branch>
    """

    intersections_results = _evaluate_crosswalk_intersections(input_flows_fileName, input_nwmflows_fileName)

    intersections_correct = intersections_results['crosswalk'].sum()
    intersections_total = len(intersections_results)
    intersections_summary = intersections_correct / intersections_total

    network_results = _evaluate_crosswalk_network(
        input_flows_fileName, input_nwmflows_fileName, input_nwm_headwaters_fileName
    )

    network_results = network_results[network_results['status'] >= 0]
    network_correct = len(network_results[network_results['status'] == 0])
    network_total = len(network_results)
    network_summary = network_correct / network_total

    results = pd.DataFrame(
        data={
            'huc': [huc, huc],
            'branch': [branch, branch],
            'method': ['intersections', 'network'],
            'correct': [intersections_correct, network_correct],
            'total': [intersections_total, network_total],
            'proportion': [intersections_summary, network_summary],
        }
    )

    results.to_csv(output_table_fileName, index=False)

    return results


def _evaluate_crosswalk_intersections(input_flows_fileName: str, input_nwmflows_fileName: str):
    """
    Computes the number of intersections between the NWM and DEM-derived flowlines

    Parameters
    ----------
    input_flows_fileName : str
        Path to DEM derived streams
    input_nwmflows_fileName : str
        Path to subset NWM burnlines

    Returns
    -------
    pandas.DataFrame
    """

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

        if isinstance(intersects.loc[idx, 'ID'], np.int64):
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

    # Get the maximum number of intersections for each flowline
    xwalks = pd.DataFrame(xwalks, columns=['HydroID', 'feature_id', 'ID', 'intersect_points'])
    xwalks['feature_id'] = xwalks['feature_id'].astype(int)

    xwalks['match'] = xwalks['feature_id'] == xwalks['ID']

    xwalks_groupby = xwalks[['HydroID', 'intersect_points']].groupby('HydroID').max()

    xwalks = xwalks.merge(xwalks_groupby, on='HydroID', how='left')
    xwalks['max'] = xwalks['intersect_points_x'] == xwalks['intersect_points_y']

    xwalks['crosswalk'] = xwalks['match'] == xwalks['max']

    return xwalks


def _evaluate_crosswalk_network(
    input_flows_fileName: str, input_nwmflows_fileName: str, input_nwm_headwaters_fileName: str
):
    """
    Compares the upstream and downstream connectivity of each stream segment

    Parameters
    ----------
    input_flows_fileName : str
        Path to DEM derived streams
    input_nwmflows_fileName : str
        Path to subset NWM burnlines
    input_nwm_headwaters_fileName : str
        Path to subset NWM headwaters
    output_table_fileName : str
        Path to output table filename

    Returns
    -------
    pandas.DataFrame
    """

    # Check for crosswalks between NWM and DEM-derived flowlines
    # fh.vprint('Checking for crosswalks between NWM and DEM-derived flowlines', verbose)

    flows = gpd.read_file(input_flows_fileName)
    flows['HydroID'] = flows['HydroID'].astype(int)
    nwm_streams = gpd.read_file(input_nwmflows_fileName)
    nwm_streams = nwm_streams.rename(columns={'ID': 'feature_id'})
    nwm_headwaters = gpd.read_file(input_nwm_headwaters_fileName)

    streams_outlets = nwm_streams.loc[~nwm_streams.to.isin(nwm_streams.feature_id), 'feature_id']
    flows_outlets = flows.loc[~flows['NextDownID'].isin(flows['HydroID']), 'HydroID']

    nwm_streams_headwaters_list = ~nwm_streams['feature_id'].isin(nwm_streams['to'])
    # flows_headwaters_list = ~flows['LINKNO'].isin(flows['DSLINKNO'])
    flows_headwaters_list = ~flows['HydroID'].isin(flows['NextDownID'])

    nwm_streams_headwaters = nwm_streams[nwm_streams_headwaters_list]
    flows_headwaters = flows[flows_headwaters_list]

    del flows_headwaters_list, nwm_streams_headwaters_list

    # Map headwater points to DEM-derived reaches
    flows_headwaters = flows_headwaters.sjoin_nearest(nwm_headwaters)
    flows_headwaters = flows_headwaters[['HydroID', 'ID']]
    nwm_streams_headwaters = nwm_streams_headwaters.sjoin_nearest(nwm_headwaters)

    del nwm_headwaters

    nwm_streams_headwaters = nwm_streams_headwaters[['feature_id', 'ID']]

    def _hydroid_to_feature_id(df, hydroid, hydroid_attr, feature_id_attr):
        return df.loc[df[hydroid_attr] == hydroid, feature_id_attr]

    def _get_upstream_data(data, data_headwaters, data_dict, hydroid, hydroid_attr, nextdownid_attr):
        # Find upstream segments
        data_dict[hydroid] = list(data.loc[data[nextdownid_attr] == hydroid, hydroid_attr].values)

        for hydroid in data_dict[hydroid]:
            if hydroid in data_headwaters[hydroid_attr].values:
                data_dict[hydroid] = data_headwaters.loc[
                    data_headwaters[hydroid_attr] == hydroid, 'ID'
                ].values[0]
            else:
                data_dict = _get_upstream_data(
                    data, data_headwaters, data_dict, hydroid, hydroid_attr, nextdownid_attr
                )

        return data_dict

    flows_dict = dict()
    streams_dict = dict()

    # Compare hash tables
    for hydroid in flows_outlets:
        flows_dict = _get_upstream_data(flows, flows_headwaters, flows_dict, hydroid, 'HydroID', 'NextDownID')

    del flows_outlets, flows_headwaters

    for feature_id in streams_outlets:
        streams_dict = _get_upstream_data(
            nwm_streams, nwm_streams_headwaters, streams_dict, feature_id, 'feature_id', 'to'
        )

    del nwm_streams, nwm_streams_headwaters, streams_outlets

    results = []
    for flow in flows_dict:
        fid = _hydroid_to_feature_id(flows, flow, 'HydroID', 'feature_id').iloc[0]
        upstream_hid = flows_dict[flow]

        upstream_fids = []
        nwm_fids = streams_dict[fid]
        out_list = [flow, fid, upstream_fids, nwm_fids]

        if not isinstance(upstream_hid, np.int64):
            if len(upstream_hid) > 0:
                for i in upstream_hid:
                    # Find upstream feature_id(s)
                    temp_fid = int(_hydroid_to_feature_id(flows, i, 'HydroID', 'feature_id').iloc[0])

                    if isinstance(temp_fid, list):
                        upstream_fids.append(temp_fid[0])
                    else:
                        upstream_fids.append(temp_fid)

                out_list = [flow, fid, upstream_fids, nwm_fids]
                if isinstance(nwm_fids, np.int64):
                    nwm_fids = [nwm_fids]

                if fid in upstream_fids:
                    # Skip duplicate feature_ids
                    out_list.append(-1)
                elif set(upstream_fids) == set(nwm_fids):
                    # 0: Crosswalk is correct
                    out_list.append(0)
                else:
                    # 1: Crosswalk is incorrect
                    out_list.append(1)
            else:
                # 2: Crosswalk is empty
                out_list.append(2)
        else:
            # 3: if upstream is a headwater point
            out_list.append(3)

        results.append(out_list)

    results = pd.DataFrame(
        data=results, columns=['HydroID', 'feature_id', 'upstream_fids', 'upstream_nwm_fids', 'status']
    )

    del flows_dict, streams_dict, flows

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tool to check crosswalk accuracy')
    parser.add_argument('-a', '--input-flows-fileName', help='DEM derived streams', type=str, required=True)
    parser.add_argument(
        '-b', '--input-nwmflows-fileName', help='Subset NWM burnlines', type=str, required=True
    )
    parser.add_argument(
        '-d', '--input-nwm-headwaters-fileName', help='Subset NWM headwaters', type=str, required=True
    )
    parser.add_argument(
        '-c', '--output-table-fileName', help='Output table filename', type=str, required=True
    )
    parser.add_argument('-u', '--huc', help='HUC ID', type=str, required=True)
    parser.add_argument('-z', '--branch', help='Branch ID', type=str, required=True)

    args = vars(parser.parse_args())

    evaluate_crosswalk(**args)
