#!/usr/bin/env python3

import argparse
import multiprocessing as mp
import os
import sys
import time
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob
from typing import Generator, Tuple

import dask
import numpy as np
import pandas as pd
import xarray as xr
from lmoments3 import distr
from numba import njit
from scipy import stats
from tqdm import tqdm


@njit(fastmath=True)
def LNSE(q_flow: np.array, q_flow_pred: np.array, q_mean: float, length: int) -> float:
    """Calculate the log nash-sutcliffe efficiency (LNSE)

    Parameters
    ----------
    q_flow : np.array
        Percentile values of retrospective flow from NWM
    q_flow_pred : np.array
        Percentile values of distribution fit randomized values
    q_mean : float
        Mean value of the retrospective flow
    length : int
        Length of the percentile values of retrospective flow

    Returns
    -------
    float
        LNSE (log nash-sutcliffe efficiency)
    """

    flow_og, flow_avg = 0.0, 0.0
    for idx in range(length):
        flow_og += (np.log(q_flow[idx]) - np.log(q_flow_pred[idx])) ** 2
        flow_avg += (np.log(q_flow[idx]) - np.log(q_mean)) ** 2

    if flow_avg == 0:
        return 0
    else:
        return 1 - (flow_og / flow_avg)


def get_score(
    sorted_flows: np.array, lmoment_distribution: Generator, scipy_distribution: Generator
) -> Tuple[str, float, dict]:
    """Get LNSE score given distributions

    Parameters
    ----------
    sorted_flows : np.array
        Sorted retrospective flows
    lmoment_distribution : Generator
        Linear moments distribution to fit sorted flows
    scipy_distribution : Generator
        Scipy statistical distribution to use linear moment parameters

    Returns
    -------
    Tuple[str, float, dict]
        Name of distribution, LNSE score, and distribution parameters

    """

    # Catch if distribution does not converge
    try:
        params = lmoment_distribution.lmom_fit(sorted_flows)
    except Exception:
        return lmoment_distribution.name, np.nan, {'params': 'N/a'}

    try:
        frozen_distribution = scipy_distribution(**params)
        # Test once to avoid many tries within the loop
        frozen_distribution.rvs(len(sorted_flows))

    # Catch if parameters are not valid
    except ValueError:
        return lmoment_distribution.name, np.nan, {'params': 'N/a'}

    # Get percentiles of retrospective flow and the mean
    percentiles = np.arange(5, 101, 5)
    q_flow = np.array([np.percentile(sorted_flows, x) for x in percentiles])
    q_mean = np.nanmean(q_flow)

    if q_mean <= 0:
        print('Mean flow is less than or equal to zero')
        return lmoment_distribution.name, np.nan, {'params': 'N/a'}

    # Recreate flow duration curve, sort values, and get percentiles
    vs = frozen_distribution.rvs(len(sorted_flows))
    sort_vs = np.sort(vs)
    q_predicted = np.array([np.percentile(sort_vs, x) for x in percentiles])

    # Filter out all values that are less than or equal to zero (avoid inf values in log)
    gt_zero_mask = (q_flow > 0) & (q_predicted > 0)
    q_flow_filt = q_flow[gt_zero_mask]
    q_predicted = q_predicted[gt_zero_mask]

    # Calculate LNSE
    lnse = LNSE(q_flow_filt, q_predicted, q_mean, len(q_flow_filt))

    return lmoment_distribution.name, lnse, params


def fit_distributions(index: int, num_flows: int, output_file_name: str, reccurence_flows_file: str = None):
    """Fit probability distributions for recreating flow duration curve for NWM retrospective flows

    Parameters
    ---------
    index : int
        Index of flows to gather from NWM retrospective based on number of flows
    num_flows : int
        How many flows to gather from NWM retrospective
    output_file_name : str
        Name of tile to save the DataFrame
    reccurence_flows_file : str, default=None
        Path to the reccurence flow netcdf file

    """

    # Distributions
    distributions = [
        [distr.exp, stats.expon],
        [distr.gam, stats.gamma],
        [distr.gev, stats.genextreme],
        [distr.gpa, stats.genpareto],
        [distr.gum, stats.gumbel_r],
        [distr.kap, stats.kappa4],
        [distr.pe3, stats.pearson3],
        [distr.nor, stats.norm],
        [distr.wei, stats.weibull_min],
    ]

    # Get reccurence interval dataset if path provided
    reccurence = xr.open_dataset(reccurence_flows_file) if reccurence_flows_file is not None else None

    if not os.path.exists(output_file_name):

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # Open NWM retrospective dataset

            ds = xr.open_zarr(
                'https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/CONUS/zarr/chrtout.zarr',
                consolidated=True,
            )

            # Get flows (maybe large amounts necessitating the dask config below)
            with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                st = (
                    ds["streamflow"]
                    .isel(feature_id=slice(index * num_flows, (index + 1) * num_flows))
                    .compute()
                )

            # For each feature_id
            dfs = []
            for i in tqdm(range(st.shape[1])):
                feat = int(st[:, i].coords['feature_id'].values)

                # Get daily mean flows
                if reccurence is not None:
                    flows = np.sort(
                        np.hstack(
                            [
                                st[:, i].resample({'time': "1D"}).mean(skipna=True),
                                reccurence.sel({'feature_id': feat}).to_array()[:-1],
                            ]
                        )
                    )
                else:
                    flows = np.sort(st[:, i].resample({'time': "1D"}).mean(skipna=True).dropna('time'))

                # Run parameter fit and calculate score for each distribution
                feature_ids, names, scores, parameters, max_flows = [], [], [], [], []
                for dist in distributions:
                    feature_ids.append(feat)

                    try:
                        max_flows.append(np.max(flows))
                        values = get_score(flows, *dist)
                    except ValueError:
                        max_flows.append(np.nan)
                        values = dist[0].name, np.nan, {'params': 'N/a'}

                    names.append(values[0])
                    scores.append(values[1])
                    parameters.append(str(dict(values[2])))

                dfs.append(
                    pd.DataFrame(
                        {
                            'feature_id': feature_ids,
                            'distribtution_name': names,
                            'LNSE': scores,
                            'parameters': parameters,
                            'max_flow': max_flows,
                        }
                    )
                )

            # Concatenate DataFrames and output file
            df = pd.concat(dfs)
            df.to_csv(output_file_name, index=False)


def progress_bar_handler(executor_dict, verbose, desc):
    """Show progress of operation

    Parameters
    ----------
    executor_dict: dict
        Keys as futures and HUC ids as values
    verbose: bool
        Whether to print more progress
    desc: str
        Description of the process
    """

    for future in tqdm(
        as_completed(executor_dict), total=len(executor_dict), disable=(not verbose), desc=desc
    ):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))


def run_linear_moment_fit(
    output_directory: str, output_name: str, num_flows: int, num_jobs: int, reccurence_flows_file: str = None
):
    """Driver for processing fit of probability distributions

    Parameters
    ----------
    output_directory : str
        Directory to output the DataFrame
    output_name : str
        Name of the output file
    num_flows : int
        Number of flows to process
    num_jobs : int
        Number of jobs to run concurrently
    reccurence_flows_file : str, optional
        Reccurence flow NetCDF file

    """

    steps = 2776738 // num_flows

    print('Begin fitting probability distributions')
    print(time.localtime())

    # Loop through all split indices
    with ProcessPoolExecutor(max_workers=num_jobs) as executor:
        executor_dict = {}
        for index in range(steps):

            output_file_name = os.path.join(
                output_directory, f"{output_name.split('.')[0]}{str(index)}.{output_name.split('.')[1]}"
            )
            try:
                future = executor.submit(
                    fit_distributions,
                    index=index,
                    num_flows=num_flows,
                    output_file_name=output_file_name,
                    reccurence_flows_file=reccurence_flows_file,
                )
                executor_dict[future] = index

            except Exception as ex:
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)

        progress_bar_handler(executor_dict, True, f"Running linear moment fit with {num_jobs} workers")

    concat_df = pd.concat(
        [
            pd.read_csv(param_file)
            for param_file in glob(os.path.join(output_directory, f"{output_name.split('.')[0]}*"))
        ]
    )

    concat_df.to_csv(os.path.join(output_directory, output_name), index=False)

    print('End fitting probability distributions')
    print(time.localtime())


if __name__ == '__main__':

    """
    Example Usage:

    python probabilistic_distribution_parameters.py
    -o "../prob_dist_test"
    -n "params.csv"
    -f 8000
    -j 1

    NOTE: If the file name
    """

    # run_linear_moment_fit(
    #     "../prob_dist_test", "tester7.csv", 100, None
    # )

    # Parse arguments
    parser = argparse.ArgumentParser(description="Fit probability distributions to flow duration curves. ")

    parser.add_argument(
        "-o", "--output_directory", help="REQUIRED: Must be an existing directory", required=True
    )

    parser.add_argument("-n", "--output_name", help='REQUIRED: Name to save each DataFrame', required=True)

    parser.add_argument(
        "-f",
        "--num_flows",
        type=int,
        help="REQUIRED: Number of flows to process per iteration (for memory purposes)",
        required=True,
    )

    parser.add_argument(
        "-j", "--num_jobs", type=int, help="REQUIRED: Number of jobs to run concurrently", required=True
    )

    parser.add_argument(
        "-r",
        "--reccurence_flows_file",
        help="OPTIONAL: Reccurence flow NetCDF file to include in flow duration curves",
        required=False,
    )

    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        if not os.path.exists(args["output_directory"]):
            raise "Directory not found"

        run_linear_moment_fit(**args)

    except Exception:
        print("The following error has occured:\n", traceback.format_exc())
