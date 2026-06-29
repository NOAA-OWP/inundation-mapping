#!/usr/bin/env python3

import argparse
import logging
import os
import random
import sys
import time
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
from inundation import inundate
from tqdm import tqdm

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import s3_or_local_isfile, s3_or_local_path_exists, setup_file_logger


# Suppress only FutureWarnings
# TODO: Jun 2026: This is a temp fix as gval is what is issuing this
# A new gval is already ready to plug into fix this. We can remove it later.
warnings.simplefilter(action='ignore', category=FutureWarning)


def Inundate_gms(
    hydrofabric_dir: str,
    forecast_file_path: str,
    hucs: Union[List[str], str] = None,
    num_threads: Optional[int] = 1,
    hydro_table_df: Optional[Union[str, pd.DataFrame]] = None,
    inundation_raster: Optional[str] = None,
    depths_raster: Optional[str] = None,
    verbose: Optional[bool] = False,
    output_fileNames: Optional[str] = None,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = True,
    show_progress_bar: Optional[bool] = False,
) -> pd.DataFrame:
    """
    Run inundation using the Generalized Mainstem methodology

    hydrofabric_dir : str
        Directory with flood inundation mapping outputs
    forecast: Union[str, pd.DataFrame]
        Data with streamflow associated with feature id
    hucs: List[str] or string, default = None but will error if empty
        List of hucs to process GMS
    num_threads: Optional[int], default = 1
        Number of threads to run in parallel
    hydro_table_df: Optional[Union[str, pd.DataFrame]], default = None
        Hydro table path or DataFrame
    inundation_raster : str
        Name of inundation extent raster
    depths_raster : str
        Name of depth raster
    verbose: Optional[bool], default = False
        Whether to qsilence output or not
    output_fileNames: Optional[str], default = None
        Name of file to output filenames from gms inundation routine
    precalb_option: Optional[bool], default = False
        Whether to use precalb discharge in hydrotable
    windowed: Optional[bool], default = False
        Whether to use window memory optimization
    show_progress_bar : Optional[bool], default=False

    Returns
    -------
    pd.DataFrame
        Output filenames from gms inundation routine

    """

    # input handling
    if isinstance(hucs, list):
        if len(hucs) == 0:
            raise ValueError("hucs (list or string) can not be empty")
        # validate that huc list is valid
        try:
            _ = (i for i in hucs)
        except TypeError:
            raise ValueError("hucs argument must be an iterable")

    if isinstance(hucs, str):
        if hucs == "":
            raise ValueError("hucs (list or string) can not be empty")
        hucs = [hucs]

    if verbose and show_progress_bar:
        logging.info(
            f"--- Starting Inundate_gms for {forecast_file_path} based on {hydrofabric_dir} with {num_threads} workers"
        )
    else:
        logging.debug(
            f"--- Starting Inundate_gms for {forecast_file_path} based on {hydrofabric_dir} with {num_threads} workers"
        )

    # June 2026:
    # Most scripts that call this function use an ProcessPoolExecutor. When it first starts, they all hit this
    # function at the same time. Putting a random time sleeper helps manage that a little lowering
    # resource needs a little and network bottlenecks, especially if they are all hitting one hucs files at one time.
    # random between 0 and 3 seconds.
    time.sleep(random.randint(0, 3))

    # load fim inputs
    hucs_branches_all = pd.read_csv(
        os.path.join(hydrofabric_dir, "fim_inputs.csv"), header=None, dtype={0: str, 1: str}
    )

    hucs = set(hucs)
    huc_indices = hucs_branches_all.loc[:, 0].isin(hucs)
    hucs_branches = hucs_branches_all.loc[huc_indices, :]

    # get number of branches
    # number_of_branches = len(hucs_branches)

    # make inundate generator
    # Jun 2026: generators do not play well with threadpoolexecutors
    # Changed to an array of dicionaries
    inundate_input_args = __inundate_gms_generator(
        hucs_branches,
        hydrofabric_dir,
        inundation_raster,
        depths_raster,
        forecast_file_path,
        hydro_table_df,
        verbose=verbose,
        windowed=windowed,
        precalb_option=precalb_option,
    )

    # logging.debug(f"back from __inundate_gms_generator for {hucs} with number of branches
    #   of {len(hucs_branches)}")

    # logging.debug("++++++++++++++++")
    # logging.debug(f"Number of branches is {branch_ids}")
    # logging.debug(f"Number of hucs is {hucCodes}")
    # logging.debug("first rec only.")
    # logging.debug(next(inundate_input_generator))
    # logging.debug("Copy the generator to a list and see what it looks like")
    # temp_list = list(inundate_input_generator)
    # logging.debug(f"Count of list is {len(temp_list)}")
    # logging.debug("First rec from the list")
    # logging.debug(temp_list[0])
    # logging.debug("++++++++++++++++")

    # Note: Some scripts such as run_test_case, use a processpoolexecutor
    # it is highly discourage but possible to use a processpoolexecutor inside a
    # processpoolexecutor, which is why this is a ThreadPoolExecutor

    inun_data_list = []  # list of dictionaries
    try:
        # We could upgrade to creating an event and queue system passed into each thread to stop
        # catestrophic errors quicker, but it can be messy
        futures = {}
        with ThreadPoolExecutor(max_workers=num_threads) as executor:

            # Using tqdm manually instead of part of as_completed, we have more
            # control over future results and exceptions
            # and using tqdm with a threadexecutor with as_complete stops
            # the thread from truly operating in a multi threading mode
            pbar = tqdm(
                total=len(inundate_input_args),
                desc=f"Inundating branches with {num_threads} workers",
                disable=(not show_progress_bar),
            )

            for inp in inundate_input_args:
                future = executor.submit(inundate, **inp)
                future_id = f"{inp['huc']}-{inp['branch_id']}"
                futures[future] = future_id

            for future in as_completed(futures):

                future_id = futures[future]  # huc-branchid ie) 12090301-1700000043
                # logging.debug(f"index {idx}: {hucCode} - {branch_id}")
                try:
                    if future.cancelled():  # for keyboard CTRL-C's generally
                        continue

                    if future.exception() is not None:
                        raise future.exception()  # re-raise it

                    if future.result() is not None:
                        inun_data_list.append(future.result())

                except Exception as exc:

                    context = f"{sys._getframe().f_code.co_name} -- {future_id}"
                    logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
                    logging.critical(f"Error: {context} : {exc}")
                    logging.critical("Thread pool shutting down")

                    print(
                        "Process pool shutting down. This may take a while depending on how many jobs."
                        " Jobs currently in progress will need to complete for this can fully shut down.",
                        flush=True,
                    )
                    print("", flush=True)

                    # Note: You can not sys.exit from executors directly
                    # all processes inside the ThreadPools tasks can be aborted
                    # but it is very messy and not really necessary
                    pbar.close()  # aborts the progress bar
                    executor.shutdown(wait=True, cancel_futures=True)  # yes.. need wait True for MT
                    raise exc  # yes.. reraise

            if pbar and show_progress_bar:
                pbar.update(1)

    except Exception as ex:
        logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
        logging.critical(f"Error while inundating based on {forecast_file_path}")
        logging.critical(traceback.format_exc())
        # Note: you can not use sys.exit in ProcessPools.
        raise ex  # yes.. reraise, so we can shut inudation down.

    # make filename dataframe
    if len(inun_data_list) != 0:
        output_fileNames_df = pd.DataFrame(inun_data_list)

        if output_fileNames is not None and output_fileNames != "":
            output_fileNames_df.to_csv(output_fileNames, index=False)
            logging.info(f"Inundation file data saved to {output_fileNames}")

        return output_fileNames_df
    else:
        return None


def __inundate_gms_generator(
    hucs_branches: pd.DataFrame,
    hydrofabric_dir: str,
    inundation_raster: str,
    depths_raster: str,
    forecast_file_path: str,
    hydro_table_df: Union[str, pd.DataFrame],
    verbose: Optional[bool] = False,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
):
    """
    Generator for use in parallelizing inundation

    Parameters
    ----------
    hucs_branches : pd.DataFrame
        DataFrame containing huc8 and branch ids
    hydrofabric_dir : str
        Directory with flood inundation mapping outputs
    inundation_raster : str
        Name of inundation extent raster
    depths_raster : str
        Name of depth raster
    forecast : Union[str, pd.DataFrame]
        Dataset with streamflow associated with feature id
    hydro_table_df: Union[str, pd.DataFrame]
        Hydrotable DataFrame.
    verbose: Optional[bool], default = False
        Whether to silence output or not
    precalb_option: Optional[bool], default = False
        Whether to use precalb discharge in hydrotable
    windowed: Optional[bool], default = False
        Whether to use window memory optimization

    """
    # Iterate over branches
    # logging.debug(f"Loading inundate gms generator for {hydrofabric_dir}")

    inundation_inputs = []

    for ___, row in hucs_branches.iterrows():
        huc = str(row[0])
        branch_id = str(row[1])

        huc_dir = os.path.join(hydrofabric_dir, huc)
        branch_dir = os.path.join(huc_dir, "branches", branch_id)

        # logging.debug(f" __inundate_gms_generator for {branch_dir}")

        rem_file_name = f"rem_zeroed_masked_{branch_id}.tif"
        rem_branch_path = os.path.join(branch_dir, rem_file_name)

        catchments_file_name = f"gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif"
        catchments_file_path = os.path.join(branch_dir, catchments_file_name)

        if isinstance(hydro_table_df, pd.DataFrame):
            hydro_table_all = hydro_table_df.set_index(["HUC", "feature_id", "HydroID"], inplace=False)
            hydro_table_branch = hydro_table_all.loc[hydro_table_all["branch_id"] == int(branch_id)]
        elif isinstance(hydro_table_df, str):
            hydro_table_branch = hydro_table_df.format(branch_id)
        else:
            dtype = {
                "HUC": str,
                "branch_id": int,
                "feature_id": str,
                "HydroID": str,
                "stage": float,
                "precalb_discharge_cms": float,
                "discharge_cms": float,
                "LakeID": int,
            }

            # TODO: Jun 2026: we no longer produce a feather file, full non HV parquet coming soon
            # if (
            #     s3_or_local_path_exists(os.path.join(huc_dir, "hydrotable.feather"))
            #     and precalb_option == False
            # ):  # Quicker reads
            #     hydro_table_huc = os.path.join(huc_dir, "hydrotable.feather")
            #     hydro_table_all = pd.read_feather(hydro_table_huc)
            # elif s3_or_local_path_exists(os.path.join(huc_dir, "hydrotable.csv")):

            # TODO: Jun 2026: Does the s3 path part still work in light of the Jun 2026 update?
            if s3_or_local_path_exists(os.path.join(huc_dir, "hydrotable.csv")):
                hydro_table_huc = os.path.join(huc_dir, "hydrotable.csv")
                # FIM versions > 4.3.5 use an aggregated hydrotable file rather than individual branch hydrotables
                htable_req_cols = [
                    "HUC",
                    "branch_id",
                    "feature_id",
                    "HydroID",
                    "stage",
                    "precalb_discharge_cms",
                    "discharge_cms",
                    "LakeID",
                ]
                hydro_table_all = pd.read_csv(hydro_table_huc, dtype=dtype, usecols=htable_req_cols)
            else:
                hydro_table_huc = None

            if precalb_option:
                if "precalb_discharge_cms" not in hydro_table_all.columns:
                    raise ValueError("Missing expected column 'precalb_discharge_cms' in hydrotable.")
                missing_count = hydro_table_all["precalb_discharge_cms"].isna().sum()
                if missing_count > 0:
                    hydro_table_all["precalb_discharge_cms"].fillna(
                        hydro_table_all["discharge_cms"], inplace=True
                    )

            if hydro_table_huc is not None and s3_or_local_isfile(hydro_table_huc):
                hydro_table_all.set_index(["HUC", "feature_id", "HydroID"], inplace=True)
                hydro_table_branch = hydro_table_all.loc[hydro_table_all["branch_id"] == int(branch_id)]
            else:
                # Earlier FIM4 versions only have branch level hydrotables
                hydro_table_branch = os.path.join(branch_dir, f"hydroTable_{branch_id}.csv")

        xwalked_file_name = f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg"
        catchments_poly_path = os.path.join(branch_dir, xwalked_file_name)

        # branch output
        # Some other functions that call in here already added a huc, so only add it if not yet there
        if (inundation_raster is not None) and (huc not in inundation_raster):
            inundation_branch_raster_path = fh.append_id_to_file_name(inundation_raster, [huc, branch_id])
        else:
            inundation_branch_raster_path = fh.append_id_to_file_name(inundation_raster, branch_id)

        if (depths_raster is not None) and (huc not in depths_raster):
            depths_branch_raster_path = fh.append_id_to_file_name(depths_raster, [huc, branch_id])
        else:
            depths_branch_raster_path = fh.append_id_to_file_name(depths_raster, branch_id)

        # identifiers
        # identifiers = (huc, branch_id)

        # inundate input
        # Jun 2026: See notes in inundate about masking now n/a
        inundate_input = {
            "huc": huc,
            "branch_id": branch_id,
            "rem_branch_path": rem_branch_path,
            "catchments_file_path": catchments_file_path,
            "catchments_poly_path": catchments_poly_path,
            "hydro_table": hydro_table_branch,
            "forecast_file_path": forecast_file_path,
            "mask_type": "filter",
            "inundation_branch_raster_path": inundation_branch_raster_path,
            "depths_branch_raster_path": depths_branch_raster_path,
            "verbose": verbose,
            "precalb_option": precalb_option,
            "windowed": windowed,
        }
        inundation_inputs.append(inundate_input)

    return inundation_inputs


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Inundate FIM")
    parser.add_argument(
        "-y", "--hydrofabric-dir", help="Directory path to FIM hydrofabric by processing unit", required=True
    )
    parser.add_argument(
        "-u", "--hucs", help="List of HUCS to run", required=False, default=None, type=str, nargs="+"
    )
    parser.add_argument(
        "-f", "--forecast-file-path", help="Forecast discharges in CMS as CSV file", required=True
    )
    parser.add_argument(
        "-i",
        "--inundation-raster",
        help="Inundation Raster output. Only writes if designated.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-d",
        "--depths-raster",
        help="Depths raster output. Only writes if designated. Appends HUC code in batch mode.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-l", "--log-file", help="Log-file to store level-path exceptions", required=False, default=None
    )
    parser.add_argument(
        "-o",
        "--output-fileNames",
        help="Output CSV file with filenames for inundation rasters, inundation polygons, and depth rasters",
        required=False,
        default=None,
    )
    parser.add_argument("-w", "--num-workers", help="Number of Workers", required=False, default=1)
    parser.add_argument(
        "-vr", "--verbose", help="Verbose printing", required=False, default=False, action="store_true"
    )
    parser.add_argument(
        "-sp",
        "--show-progress-bar",
        help="Show tqdm progress bar",
        required=False,
        default=True,
        action="store_false",
    )

    Inundate_gms(**vars(parser.parse_args()))
