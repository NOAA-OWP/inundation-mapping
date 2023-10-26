#!/usr/bin/env python3

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd


def verify_crosswalk(
    input_flows_fileName, input_nwmflows_fileName, input_nwm_headwaters_fileName, output_table_fileName
):
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

    # Map headwater points to DEM-derived reaches
    flows_headwaters = flows_headwaters.sjoin_nearest(nwm_headwaters)
    flows_headwaters = flows_headwaters[['HydroID', 'ID']]
    nwm_streams_headwaters = nwm_streams_headwaters.sjoin_nearest(nwm_headwaters)
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

    for feature_id in streams_outlets:
        streams_dict = _get_upstream_data(
            nwm_streams, nwm_streams_headwaters, streams_dict, feature_id, 'feature_id', 'to'
        )

    results = []
    for flow in flows_dict:
        fid = _hydroid_to_feature_id(flows, flow, 'HydroID', 'feature_id').iloc[0]
        upstream_fid = flows_dict[flow]

        upstream_fids = []
        nwm_fids = streams_dict[fid]
        out_list = [flow, fid, upstream_fids, nwm_fids]

        if type(upstream_fid) != np.int64:
            if len(upstream_fid) > 0:
                for i in upstream_fid:
                    # Find upstream feature_id(s)
                    temp_ids = streams_dict[
                        int(_hydroid_to_feature_id(flows, i, 'HydroID', 'feature_id').iloc[0])
                    ]
                    if type(temp_ids) == list:
                        upstream_fids.append(temp_ids[0])
                    else:
                        upstream_fids.append(temp_ids)

                out_list = [flow, fid, upstream_fids, nwm_fids]
                if type(nwm_fids) == np.int64:
                    nwm_fids = [nwm_fids]
                if upstream_fids == nwm_fids:
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
        results, columns=['HydroID', 'feature_id', 'upstream_fids', 'upstream_nwm_fids', 'status']
    )
    results.to_csv(output_table_fileName, index=False)

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Crosswalk for MS/FR/GMS networks; calculate synthetic rating curves; update short rating curves'
    )
    parser.add_argument('-a', '--input-flows-fileName', help='DEM derived streams', required=True)
    parser.add_argument('-b', '--input-nwmflows-fileName', help='Subset NWM burnlines', required=True)
    parser.add_argument('-d', '--input-nwm-headwaters-fileName', help='Subset NWM headwaters', required=True)
    parser.add_argument('-c', '--output-table-fileName', help='Output table filename', required=True)

    args = vars(parser.parse_args())

    verify_crosswalk(**args)
