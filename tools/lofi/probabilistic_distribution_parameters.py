#!/usr/bin/env python3

import argparse
import logging
import os
import time
import traceback
import warnings
from datetime import datetime
from glob import glob
from typing import Generator, List, Tuple, Union

import dask
import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from dask.distributed import Client, Lock, as_completed
from lmoments3 import distr
from numba import njit
from scipy import stats
from shared_functions import FIM_Helpers as fh
from tqdm import tqdm


NWM_V3_ZARR_URL = 'https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/CONUS/zarr/chrtout.zarr'


def __setup_logger(output_folder_path):

    start_time = datetime.now()

    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"probabilistic_distribution_parameters-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('netCDF4').setLevel(logging.WARNING)
    logging.getLogger('numcodecs').setLevel(logging.WARNING)

    logging.info(f'Started : {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")
    return start_time


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
        logging.info('Mean flow is less than or equal to zero')
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


def fit_distributions(
    index: int,
    num_flows: int,
    output_file_name: str,
    lock: dask.distributed.Lock,
    stream_ids: List = None,
    recurrence_flows_file: str = None,
):
    """Fit probability distributions for recreating flow duration curve for NWM retrospective flows

    Parameters
    ---------
    index : int
        Index of flows to gather from NWM retrospective based on number of flows
    num_flows : int
        How many flows to gather from NWM retrospective
    output_file_name : str
        Name of tile to save the DataFrame
    lock : Lock
        Mechanism to avoid ServerDisconnected errors
    stream_ids : List, default=None
        List of stream ids to process
    recurrence_flows_file : str, default=None
        Path to the recurrence flows NetCDF file

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

    # Get recurrence interval dataset if path provided
    recurrence = xr.open_dataset(recurrence_flows_file) if recurrence_flows_file is not None else None

    if not os.path.exists(output_file_name):

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            def get_streamflows(
                lock: Lock, index: int, num_flows: int, count: int, stream_ids: List = None
            ) -> Union[xr.Dataset, xr.DataArray]:
                """
                Get streamflows from service with 5 attempts

                Parameters
                ----------
                lock : Lock
                    Mechanism to avoid ServerDisconnected errors
                index : int
                    Index of the dataset to take streamflows from
                num_flows : int
                    Number of flows to select from dataset
                count : int
                    Number of connections to NWM retrospective dataset
                stream_ids : List, default=None
                    List of stream ids to process
                """

                try:

                    with lock:
                        ds = xr.open_zarr(NWM_V3_ZARR_URL, consolidated=True)

                    with dask.config.set(**{'array.slicing.split_large_chunks': True}):

                        if stream_ids is not None:
                            st = ds["streamflow"].sel(feature_id=stream_ids).compute()
                        else:
                            st = (
                                ds["streamflow"]
                                .isel(feature_id=slice(index * num_flows, (index + 1) * num_flows))
                                .compute()
                            )

                        return st

                except Exception:

                    if count < 5:
                        count += 1
                        time.sleep(1)
                        return get_streamflows(
                            lock=lock, index=index, num_flows=num_flows, count=count, stream_ids=stream_ids
                        )
                    else:
                        logging.WARNING(f"Could not connect to NWM Retrosepctive Dataset for index {index}")
                        return None

            # Open NWM retrospective dataset
            connection_count = 0

            st = get_streamflows(
                lock=lock, index=index, num_flows=num_flows, count=connection_count, stream_ids=stream_ids
            )

            # Exit operation but allow for other processes to run if return code is 1
            if st is None:
                return None

            # For each feature_id
            dfs = []
            for i in (pbar3 := tqdm(range(st.shape[1]))):
                pbar3.set_description(f"Running feature {i}")
                feat = int(st[:, i].coords['feature_id'].values)

                try:
                    # Get daily mean flows
                    if recurrence is not None:
                        fls = st[:, i].resample({'time': "1D"}).mean(skipna=True)
                        rec_fls = recurrence.sel({'feature_id': feat}).to_array()[:-3]
                        flows = np.sort(np.hstack([fls, rec_fls[rec_fls > np.max(fls)]]))
                    else:
                        flows = np.sort(st[:, i].resample({'time': "1D"}).mean(skipna=True).dropna('time'))
                except Exception:
                    return None

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


def run_linear_moment_fit(
    output_directory: str,
    output_name: str,
    num_flows: int,
    stream_file: str = None,
    recurrence_flows_file: str = None,
    num_jobs: int = None,
    threads_per_worker: int = None,
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
    stream_file : str, default=None
        Path to file with stream ids (CSV or Geopackage)
    recurrence_flows_file : str, default=None
        Path to the recurrence flows NetCDF file
    num_jobs : int
        Number of jobs to run concurrently
    threads_per_worker : int, optional
        Number of threads to run per a worker
    """

    start_time = __setup_logger(output_directory)

    # If arguments are none Dask will automatically resolve
    client = Client(threads_per_worker=threads_per_worker, n_workers=num_jobs, silence_logs=logging.ERROR)
    num_jobs = num_jobs if num_jobs else len(client.scheduler_info()['workers'])

    # Get stream IDs if file is passed
    if stream_file:
        stream_df = (
            gpd.read_file(stream_file) if stream_file.split('.')[-1] == 'gpkg' else pd.read_csv(stream_file)
        )
        if 'ID' not in stream_df.columns:
            raise ValueError('ID column not in stream file')
        steps = stream_df.shape[0] // num_flows
        stream_ids = np.array_split(stream_df['ID'].values, steps)

    else:
        steps = 2776738 // num_flows
        stream_ids = None

    lock = Lock()

    # Run batches of size given available workers
    num_batches = int(steps / num_jobs)
    logging.info(f'Number of Batches: {num_batches} \n')

    for batch_idx in (pbar := tqdm(range(num_batches))):
        pbar.set_description(f"Running Batch {batch_idx+1}")
        batch_start_time = datetime.now()
        logging.info(
            f'Running Batch {batch_idx + 1} of {num_batches} : {batch_start_time.strftime("%m/%d/%Y %H:%M:%S")}'
        )

        lazy_results = []
        for job_idx in range(num_jobs):
            run_idx = job_idx + (num_jobs * batch_idx)
            output_file_name = os.path.join(
                output_directory, f"{output_name.split('.')[0]}{str(run_idx)}" f".{output_name.split('.')[1]}"
            )
            lazy_results.append(
                dask.delayed(fit_distributions)(
                    run_idx, num_flows, output_file_name, lock, stream_ids, recurrence_flows_file
                )
            )

        results = []
        working_futures = client.compute(lazy_results)
        ac = as_completed(working_futures)

        for job_idx, fut in (pbar2 := tqdm(enumerate(ac))):
            pbar2.set_description(f"Running Job {job_idx + 1}")
            logging.info(f"Running Job {job_idx + 1}")

            res = fut.result()
            results.append(res)

        batch_end_time = datetime.now()
        logging.info(f'Completed Batch {batch_idx + 1}: {batch_end_time.strftime("%m/%d/%Y %H:%M:%S")}')
        logging.info(f"Batch Run {fh.print_date_time_duration(batch_start_time, batch_end_time)}")
        logging.info(f"Current Processing {fh.print_date_time_duration(start_time, batch_end_time)} \n")

    # Concatenate all param files in to one
    concat_df = pd.concat(
        [
            pd.read_csv(param_file)
            for param_file in glob(os.path.join(output_directory, f"{output_name.split('.')[0]}[0-9]*"))
        ]
    )

    concat_df.to_csv(os.path.join(output_directory, output_name), index=False)

    end_time = datetime.now()
    logging.info("Completed Probabilistic Distribution Parameter Fit")
    logging.info(f'Completed : {end_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info(f"Batch Run {fh.print_date_time_duration(start_time, end_time)}")


if __name__ == '__main__':

    """
    Example Usage:

    python probabilistic_distribution_parameters.py
    -o "../prob_dist_test"
    -n "params.csv"
    -f 800

    NOTE: If the file name
    """

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
        "-s",
        "--stream_file",
        help="OPTIONAL: File to get stream ids to run distribution for CSV or Geopackage",
        required=False,
    )

    parser.add_argument(
        "-r",
        "--recurrence_flows_file",
        help="OPTIONAL: Recurrence flows NetCDF file to include in flow duration curves",
        required=False,
    )

    parser.add_argument(
        "-j", "--num_jobs", type=int, help="OPTIONAL: Number of jobs to run concurrently", required=False
    )

    parser.add_argument(
        "-t",
        "--threads_per_worker",
        type=int,
        help="OPTIONAL: Number of threads to run per a job/worker",
        required=False,
    )

    args = vars(parser.parse_args())

    if not os.path.exists(args["output_directory"]):
        try:
            logging.info("Creating non-existent output directory")
            os.makedirs(args["output_directory"])
        except Exception as e:
            raise e("Unable to find directory")

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        run_linear_moment_fit(**args)

    except Exception:
        logging.ERROR("The following error has occurred:\n", traceback.format_exc())
