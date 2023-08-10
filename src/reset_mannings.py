#!/usr/bin/env python3

import pandas as pd
import numpy as np
import argparse
import os
from glob import iglob, glob
from stream_branches import StreamNetwork


def Reset_mannings(hydrofabric_dir, mannings_value, overwrite_files=False):
    (
        src_table_filePaths,
        hydro_table_filePaths,
        stream_network_filePaths,
    ) = make_file_paths_for_inputs(hydrofabric_dir)

    single_stream_network = len(stream_network_filePaths) == 1

    if single_stream_network:
        stream_network = StreamNetwork.from_file(stream_network_filePaths[0])

    for i, (srcFP, hydFP) in enumerate(zip(src_table_filePaths, hydro_table_filePaths)):
        src_table = load_src_table(srcFP)
        hydro_table = load_hydro_table(hydFP)

        src_table, hydro_table = reset_mannings_for_a_processing_unit(
            src_table, hydro_table, mannings_value
        )

        if not single_stream_network:
            stream_network = StreamNetwork.from_file(stream_network_filePaths[i])

        small_segments = identify_small_reaches(
            stream_network, min_catchment_area=None, min_stream_length=None
        )
        src_table, hydro_table = replace_discharges_of_small_segments(
            small_segments, src_table, hydro_table
        )

        if overwrite_files:
            src_table.to_csv(srcFP, index=False)
            hydro_table.to_csv(hydFP, index=False)

        # yield(src_table, hydro_table)


def load_hydro_table(hydro_table_filePath):
    hydro_table = pd.read_csv(
        hydro_table_filePath,
        dtype={
            'HydroID': str,
            'feature_id': str,
            'stage': float,
            'discharge_cms': float,
            'HUC': str,
            'LakeID': str,
        },
    )

    return hydro_table


def load_src_table(src_table_filePath):
    src_table = pd.read_csv(
        src_table_filePath,
        dtype={
            'HydroID': str,
            'feature_id': str,
            'stage': float,
            'discharge_cms': float,
            'HUC': str,
            'LakeID': str,
        },
    )

    return src_table


def make_file_paths_for_inputs(hydrofabric_dir):
    src_table_filePath_to_glob = os.path.join(hydrofabric_dir, '**', 'src_full_crosswalked*.csv')
    hydro_table_filePath_to_glob = os.path.join(hydrofabric_dir, '**', 'hydroTable*.csv')
    stream_network_filePath_to_glob = os.path.join(
        hydrofabric_dir, '**', 'demDerived_reaches_split_filtered_addedAttributes_crosswalked*.gpkg'
    )

    src_table_filePaths = iglob(src_table_filePath_to_glob, recursive=True)
    hydro_table_filePaths = iglob(hydro_table_filePath_to_glob, recursive=True)
    stream_network_filePaths = glob(stream_network_filePath_to_glob, recursive=True)

    return (src_table_filePaths, hydro_table_filePaths, stream_network_filePaths)


def reset_mannings_for_a_processing_unit(src_table, hydro_table, mannings_value):
    src_table = override_mannings(src_table, mannings_value)

    src_table = calculate_discharge(src_table)

    hydro_table["discharge_cms"] = src_table["Discharge (m3s-1)"]

    return (src_table, hydro_table)


def override_mannings(table, mannings_value, mannings_attribute="ManningN"):
    table[mannings_attribute] = mannings_value

    return table


def calculate_discharge(src_table):
    src_table['Discharge (m3s-1)'] = (
        src_table['WetArea (m2)']
        * pow(src_table['HydraulicRadius (m)'], 2.0 / 3)
        * pow(src_table['SLOPE'], 0.5)
        / src_table['ManningN']
    )

    # set zero stage values to zero discharge
    src_table.loc[src_table['Stage'] == 0, ['Discharge (m3s-1)']] = 0

    return src_table


def identify_small_reaches(stream_network, min_catchment_area=None, min_stream_length=None):
    # Adjust short model reach rating curves
    sml_segs = pd.DataFrame()

    if min_catchment_area is None:
        min_catchment_area = float(os.environ['min_catchment_area'])  # 0.25#

    if min_stream_length is None:
        min_stream_length = float(os.environ['min_stream_length'])  # 0.5#

    # replace small segment geometry with neighboring stream
    for stream_index in stream_network.index:
        if (
            stream_network["areasqkm"][stream_index] < min_catchment_area
            and stream_network["LengthKm"][stream_index] < min_stream_length
            and stream_network["LakeID"][stream_index] < 0
        ):
            short_id = stream_network['HydroID'][stream_index]
            to_node = stream_network['To_Node'][stream_index]
            from_node = stream_network['From_Node'][stream_index]

            # multiple upstream segments
            if len(stream_network.loc[stream_network['NextDownID'] == short_id]['HydroID']) > 1:
                max_order = max(
                    stream_network.loc[stream_network['NextDownID'] == short_id]['order_']
                )  # drainage area would be better than stream order but we would need to calculate

                if (
                    len(
                        stream_network.loc[
                            (stream_network['NextDownID'] == short_id)
                            & (stream_network['order_'] == max_order)
                        ]['HydroID']
                    )
                    == 1
                ):
                    update_id = stream_network.loc[
                        (stream_network['NextDownID'] == short_id)
                        & (stream_network['order_'] == max_order)
                    ]['HydroID'].item()

                else:
                    update_id = stream_network.loc[
                        (stream_network['NextDownID'] == short_id)
                        & (stream_network['order_'] == max_order)
                    ]['HydroID'].values[
                        0
                    ]  # get the first one (same stream order, without drainage area info it is hard to know which is the main channel)

            # single upstream segments
            elif len(stream_network.loc[stream_network['NextDownID'] == short_id]['HydroID']) == 1:
                update_id = stream_network.loc[stream_network.To_Node == from_node][
                    'HydroID'
                ].item()

            # no upstream segments; multiple downstream segments
            elif len(stream_network.loc[stream_network.From_Node == to_node]['HydroID']) > 1:
                max_order = max(
                    stream_network.loc[stream_network.From_Node == to_node]['HydroID']['order_']
                )  # drainage area would be better than stream order but we would need to calculate

                if (
                    len(
                        stream_network.loc[
                            (stream_network['NextDownID'] == short_id)
                            & (stream_network['order_'] == max_order)
                        ]['HydroID']
                    )
                    == 1
                ):
                    update_id = stream_network.loc[
                        (stream_network.From_Node == to_node)
                        & (stream_network['order_'] == max_order)
                    ]['HydroID'].item()

                else:
                    update_id = stream_network.loc[
                        (stream_network.From_Node == to_node)
                        & (stream_network['order_'] == max_order)
                    ]['HydroID'].values[
                        0
                    ]  # get the first one (same stream order, without drainage area info it is hard to know which is the main channel)

            # no upstream segments; single downstream segment
            elif len(stream_network.loc[stream_network.From_Node == to_node]['HydroID']) == 1:
                update_id = stream_network.loc[stream_network.From_Node == to_node][
                    'HydroID'
                ].item()

            else:
                update_id = stream_network.loc[stream_network.HydroID == short_id]['HydroID'].item()

            str_order = stream_network.loc[stream_network.HydroID == short_id]['order_'].item()
            sml_segs = pd.concat(
                [sml_segs, {'short_id': short_id, 'update_id': update_id, 'str_order': str_order}],
                ignore_index=True,
            )

    # print("Number of short reaches [{} < {} and {} < {}] = {}".format("areasqkm", min_catchment_area, "LengthKm", min_stream_length, len(sml_segs)))

    return sml_segs


def replace_discharges_of_small_segments(sml_segs, src_table, hydro_table):
    # update rating curves
    if len(sml_segs) == 0:
        return (src_table, hydro_table)

    # sml_segs.to_csv(small_segments_filename,index=False)
    # print("Update rating curves for short reaches.")

    for index, segment in sml_segs.iterrows():
        short_id = segment[0]
        update_id = segment[1]
        new_values = src_table.loc[src_table['HydroID'] == update_id][
            ['Stage', 'Discharge (m3s-1)']
        ]

        for src_index, src_stage in new_values.iterrows():
            src_table.loc[
                (src_table['HydroID'] == short_id) & (src_table['Stage'] == src_stage[0]),
                ['Discharge (m3s-1)'],
            ] = src_stage[1]

    hydro_table["discharge_cms"] = src_table["Discharge (m3s-1)"]

    return (src_table, hydro_table)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Overwrites mannings n values and recomputes discharge values for SRCs and Hydro-Tables'
    )
    parser.add_argument('-y', '--hydrofabric-dir', help='Hydrofabric directory', required=True)
    parser.add_argument(
        '-n', '--mannings-value', help='Mannings N value to use', required=True, type=float
    )
    parser.add_argument(
        '-o',
        '--overwrite-files',
        help='Overwrites original files if used',
        required=False,
        default=False,
        action='store_true',
    )

    args = vars(parser.parse_args())

    Reset_mannings(**args)
