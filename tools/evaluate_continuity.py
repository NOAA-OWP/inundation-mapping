#!/usr/bin/env python3

import argparse

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


gpd.options.io_engine = "pyogrio"


def evaluate_continuity(
    stream_network_file, forecast_file, stream_network_outfile=None, confluences_only=False, plot_file=None
):
    stream_network = gpd.read_file(stream_network_file)
    forecast = pd.read_csv(forecast_file)

    stream_network = stream_network.merge(forecast, how='left', left_on='feature_id', right_on='feature_id')

    stream_network['discharge'] = stream_network['discharge'].fillna(0)

    toNodes = set(stream_network['To_Node'].tolist())

    # upstream dictionary and confluences
    upstream_dictionary, confluences = {}, set()
    for idx, reach in stream_network.iterrows():
        fromNode = reach['From_Node']
        hydroId = reach['HydroID']

        if fromNode in toNodes:
            upstream_indices = stream_network['To_Node'] == fromNode
            upstream_discharges = np.array(stream_network.loc[upstream_indices, 'discharge'].tolist())
            upstream_dictionary[hydroId] = upstream_discharges

            isconfluence = len(upstream_discharges) > 1
            if isconfluence:
                confluences.add(hydroId)

    # filter out non-confluences
    if confluences_only:
        hydroIDs = stream_network['HydroID'].tolist()
        confluence_bool = np.array([True if h in confluences else False for h in hydroIDs])
        stream_network = stream_network.loc[confluence_bool, :]

    actual_discharges, expected_discharges = [], []
    expected_dischages_dict = dict()
    for idx, reach in stream_network.iterrows():
        hydroId = reach['HydroID']

        try:
            upstream_discharges = upstream_dictionary[hydroId]
        except KeyError:
            expected_dischages_dict[hydroId] = 0
            continue

        actual_discharges += [reach['discharge']]
        expected_discharges += [np.sum(upstream_discharges)]
        expected_dischages_dict[hydroId] = np.sum(upstream_discharges)

    actual_discharges, expected_discharges = np.array(actual_discharges), np.array(expected_discharges)

    # add to stream_network
    expected_discharges_df = pd.DataFrame.from_dict(
        expected_dischages_dict, orient='index', columns=['expected_discharges']
    )
    stream_network = stream_network.merge(
        expected_discharges_df, left_on='HydroID', right_index=True, how='left'
    )

    number_of_reaches = len(stream_network)
    SMAPE = smape(actual_discharges, expected_discharges)
    diff = actual_discharges - expected_discharges

    print(
        "Number of No Flow Reaches: {} out of {}".format(
            (stream_network['discharge'] == 0).sum(), number_of_reaches
        )
    )
    print("SMAPE = {}%".format(SMAPE[0]))
    print("Diff (<0) = {}".format(np.sum(diff < 0)))
    print("Diff (>0) = {}".format(np.sum(diff > 0)))
    print("Diff (=0) = {}".format(np.sum(diff == 0)))
    print("Diff (>-10,<10) = {}".format(np.sum(np.logical_or(diff > -10, diff < 10))))
    print("Median diff: {}".format(np.nanmedian(diff)))
    print("Mean diff: {}".format(np.nanmean(diff)))

    if confluences_only:
        nbins = 50
        xlim = (-2000, 2000)
        ylim = (0, 50)
        title = 'Discharge Errors (CMS) At Confluence Reaches'
    else:
        nbins = 500
        xlim = (-450, 450)
        ylim = (0, 60)
        title = 'Discharge Errors (CMS)'

    fig = plt.figure(1)
    ax = plt.subplot(111)
    n, bins, patches = ax.hist(diff, nbins, facecolor='blue', alpha=0.4)
    try:
        plt.xlim(xlim)
        plt.ylim(ylim)
    except UnboundLocalError:
        pass
    plt.title(title)
    plt.xlabel('Discharge Errors (CMS) = actual - expected')
    plt.ylabel('Count (Peak not show)')

    if plot_file is not None:
        fig.savefig(plot_file)

    if stream_network_outfile is not None:
        stream_network.to_file(stream_network_outfile, index=False, driver='GPKG', engine='fiona')

    return stream_network


def smape(predicted, actual):
    assert len(predicted) == len(actual), "Predicted and actual need to have same length"

    sape = 100 * (np.abs(predicted - actual) / (np.abs(predicted) + np.abs(actual)))

    return (np.nanmean(sape), sape)


if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Evaluating continuity')
    parser.add_argument('-s', '--stream-network-file', help='Stream Network', required=True)
    parser.add_argument('-f', '--forecast-file', help='Forecast File', required=True)
    parser.add_argument(
        '-o', '--stream-network-outfile', help='Stream Network Outfile', required=False, default=None
    )
    parser.add_argument(
        '-c',
        '--confluences-only',
        help='Only at confluence reaches',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument('-p', '--plot-file', help='Plot File', required=False, default=None)

    args = vars(parser.parse_args())

    evaluate_continuity(**args)
