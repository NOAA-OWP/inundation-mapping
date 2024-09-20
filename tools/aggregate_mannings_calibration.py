#!/usr/bin/env python3

import argparse
import csv
import json
import os

import pandas as pd

from utils.shared_functions import compute_stats_from_contingency_table


def aggregate_parameter_sets(huc_list_path, calibration_stat_folder, summary_file, mannings_json):
    outfolder = os.path.dirname(summary_file)
    aggregate_output_dir = os.path.join(outfolder, 'aggregate_metrics')

    if not os.path.exists(aggregate_output_dir):
        os.makedirs(aggregate_output_dir)

    mannings_summary_table = pd.DataFrame(
        columns=['metric', 'value', 'stream_order', 'mannings_n', 'huc', 'interval']
    )

    with open(huc_list_path) as f:
        huc_list = [huc.rstrip() for huc in f]

    for huc in huc_list:
        branch_dir = os.path.join(
            'data',
            'test_cases',
            str(huc) + '_ble',
            'performance_archive',
            'development_versions',
            calibration_stat_folder,
        )
        for stream_order in os.listdir(branch_dir):
            stream_order_dir = os.path.join(branch_dir, stream_order)
            for mannings_value in os.listdir(stream_order_dir):
                mannings_value_dir = os.path.join(stream_order_dir, mannings_value)
                for flood_recurrence in os.listdir(mannings_value_dir):
                    flood_recurrence_dir = os.path.join(mannings_value_dir, flood_recurrence)
                    total_area_stats = pd.read_csv(
                        os.path.join(flood_recurrence_dir, 'total_area_stats.csv'), index_col=0
                    )
                    total_area_stats = total_area_stats.loc[
                        [
                            'true_positives_count',
                            'true_negatives_count',
                            'false_positives_count',
                            'false_negatives_count',
                            'masked_count',
                            'cell_area_m2',
                            'CSI',
                        ],
                        :,
                    ]
                    total_area_stats = total_area_stats.reset_index()
                    total_area_stats_table = pd.DataFrame(
                        {
                            'metric': total_area_stats.iloc[:, 0],
                            'value': total_area_stats.iloc[:, 1],
                            'stream_order': stream_order,
                            'mannings_n': mannings_value,
                            'huc': huc,
                            'interval': flood_recurrence,
                        }
                    )
                    mannings_summary_table = pd.concat(
                        [mannings_summary_table, total_area_stats_table], ignore_index=True
                    )

    mannings_summary_table.to_csv(summary_file, index=False)

    ## calculate optimal parameter set

    true_positives, true_negatives, false_positives, false_negatives, cell_area, masked_count = (
        0,
        0,
        0,
        0,
        0,
        0,
    )

    list_to_write = [
        ['metric', 'value', 'stream_order', 'mannings_value', 'return_interval']
    ]  # Initialize header.
    for stream_order in mannings_summary_table.stream_order.unique():
        for return_interval in mannings_summary_table.interval.unique():
            for mannings_value in mannings_summary_table.mannings_n.unique():
                true_positives = mannings_summary_table.loc[
                    (mannings_summary_table['interval'] == return_interval)
                    & (mannings_summary_table['stream_order'] == stream_order)
                    & (mannings_summary_table['mannings_n'] == mannings_value)
                    & (mannings_summary_table['metric'] == 'true_positives_count'),
                    'value',
                ].sum()
                true_negatives = mannings_summary_table.loc[
                    (mannings_summary_table['interval'] == return_interval)
                    & (mannings_summary_table['stream_order'] == stream_order)
                    & (mannings_summary_table['mannings_n'] == mannings_value)
                    & (mannings_summary_table['metric'] == 'true_negatives_count'),
                    'value',
                ].sum()
                false_positives = mannings_summary_table.loc[
                    (mannings_summary_table['interval'] == return_interval)
                    & (mannings_summary_table['stream_order'] == stream_order)
                    & (mannings_summary_table['mannings_n'] == mannings_value)
                    & (mannings_summary_table['metric'] == 'false_positives_count'),
                    'value',
                ].sum()
                false_negatives = mannings_summary_table.loc[
                    (mannings_summary_table['interval'] == return_interval)
                    & (mannings_summary_table['stream_order'] == stream_order)
                    & (mannings_summary_table['mannings_n'] == mannings_value)
                    & (mannings_summary_table['metric'] == 'false_negatives_count'),
                    'value',
                ].sum()
                masked_count = mannings_summary_table.loc[
                    (mannings_summary_table['interval'] == return_interval)
                    & (mannings_summary_table['stream_order'] == stream_order)
                    & (mannings_summary_table['mannings_n'] == mannings_value)
                    & (mannings_summary_table['metric'] == 'masked_count'),
                    'value',
                ].sum()

                cell_area = mannings_summary_table.loc[
                    (mannings_summary_table['interval'] == return_interval)
                    & (mannings_summary_table['stream_order'] == stream_order)
                    & (mannings_summary_table['mannings_n'] == mannings_value)
                    & (mannings_summary_table['metric'] == 'cell_area_m2'),
                    'value',
                ].sum()

                # Pass all sums to shared function to calculate metrics.
                stats_dict = compute_stats_from_contingency_table(
                    true_negatives,
                    false_negatives,
                    false_positives,
                    true_positives,
                    cell_area=cell_area,
                    masked_count=masked_count,
                )

                for stat in stats_dict:
                    list_to_write.append(
                        [stat, stats_dict[stat], stream_order, mannings_value, return_interval]
                    )

    # Map path to output directory for aggregate metrics.
    output_file = os.path.join(
        aggregate_output_dir, 'aggregate_metrics_mannings_calibration_by_streamorder.csv'
    )

    with open(output_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(list_to_write)

    print("Finished aggregating metrics over " + str(len(huc_list)) + " test cases.")

    print('Writing optimal mannings parameter set')

    manning_dict = {}
    list_to_write_pd = pd.read_csv(output_file)
    for stream_order in list_to_write_pd.stream_order.unique():
        interval_100 = list_to_write_pd.loc[
            (list_to_write_pd['stream_order'] == stream_order)
            & (list_to_write_pd['metric'] == 'CSI')
            & (list_to_write_pd['return_interval'] == '100yr'),
            'value',
        ].max()
        interval_500 = list_to_write_pd.loc[
            (list_to_write_pd['stream_order'] == stream_order)
            & (list_to_write_pd['metric'] == 'CSI')
            & (list_to_write_pd['return_interval'] == '500yr'),
            'value',
        ].max()
        mannings_100yr = list_to_write_pd.loc[
            (list_to_write_pd['stream_order'] == stream_order)
            & (list_to_write_pd['metric'] == 'CSI')
            & (list_to_write_pd['return_interval'] == '100yr')
            & (list_to_write_pd['value'] == interval_100),
            'mannings_value',
        ]
        mannings_500yr = list_to_write_pd.loc[
            (list_to_write_pd['stream_order'] == stream_order)
            & (list_to_write_pd['metric'] == 'CSI')
            & (list_to_write_pd['return_interval'] == '500yr')
            & (list_to_write_pd['value'] == interval_500),
            'mannings_value',
        ]
        if (len(mannings_100yr) == 1) & (len(mannings_500yr) == 1):
            if mannings_100yr.iloc[0] == mannings_500yr.iloc[0]:
                manning_dict[str(stream_order)] = mannings_100yr.iloc[0]
            else:
                print(
                    '100yr and 500yr optimal mannings vary by '
                    + str(round(abs(mannings_100yr.iloc[0] - mannings_500yr.iloc[0]), 2))
                    + " for stream order "
                    + str(stream_order)
                )
                print('Selecting optimal mannings n for 100yr event')
                manning_dict[str(stream_order)] = mannings_100yr.iloc[0]
        elif (len(mannings_100yr) > 1) or (len(mannings_500yr) > 1):
            print('multiple values achieve optimal results ' + " for stream order " + str(stream_order))
            print('Selecting optimal mannings n for 100yr event')
            manning_dict[str(stream_order)] = mannings_100yr.iloc[0]

    for n in range(1, 15):
        if str(n) not in manning_dict:
            manning_dict[str(n)] = 0.06

    with open(mannings_json, "w") as outfile:
        json.dump(manning_dict, outfile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Aggregates the evaluation statistics of all mannings calibration runs'
    )
    parser.add_argument('-l', '--huc-list-path', help='csv list of HUCs to aggregate', required=True)
    parser.add_argument('-c', '--calibration-stat-folder', help='eval stat column name', required=True)
    parser.add_argument(
        '-f', '--summary-file', help='output file with aggregate mannings calibration stats', required=True
    )
    parser.add_argument(
        '-e',
        '--mannings-json',
        help='file path for optimal mannings n parameter set json',
        required=False,
        default="/foss_fim/config/mannings_calibrated.json",
    )

    args = vars(parser.parse_args())

    huc_list_path = args['huc_list_path']
    calibration_stat_folder = args['calibration_stat_folder']
    summary_file = args['summary_file']
    mannings_json = args['mannings_json']

    aggregate_parameter_sets(huc_list_path, calibration_stat_folder, summary_file, mannings_json)
