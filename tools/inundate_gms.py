#!/usr/bin/env python3

import argparse
import logging
import os
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple, Union

import pandas as pd
from inundation import inundate
from tqdm import tqdm

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import (
    NoForecastFound,
    hydroTableHasOnlyLakes,
    s3_or_local_isfile,
    s3_or_local_path_exists,
)

logging.getLogger('numba').setLevel(logging.WARNING)

# Commented out some args that we no longer valid or not used by any scripts
def Inundate_gms(
    hydrofabric_dir: str,
    forecast_file_path: str,
    hucs: List[str],  # Not optional, but can be a list of one huc
    num_threads: Optional[int] = 1,
    # Most use a path to HUC HT, interpolates leaves it empty, no one sends in a dataframe
    hydro_table_path: Optional[str] = None,
    # inundation_raster_path is never used as a final file, just used as a base file name to append
    # intermedary files will processing. In mosiack, it uses this true file name for the final mosaicked file
    inundation_raster_path: Optional[str] = None,
    depths_raster_path: Optional[str] = None,  # Used by interpolate_water_surface
    verbose: Optional[bool] = False,
    # log_file: Optional[str] = None,
    # output_fileNames (renaming it) was not be used by any scripts, but
    # will open the option to save the dataframe of huc8, branchs and raster paths in case
    # something wants it later. Renamed from output_fileNames to inundation_results_file_path
    inundation_results_file_path: Optional[str] = None,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
    # multi_process: Optional[bool] = False,
    show_progress_bar: Optional[bool] = False,
) -> pd.DataFrame:
    """

    hydrofabric_dir : str
        Directory with flood inundation mapping outputs
    forecast: Union[str, pd.DataFrame]
        Data with streamflow associated with feature id
    num_workers: Optional[int], default = 1
        Number of threads to useNumber of processes to run in parallel
    hydro_table_df: Optional[Union[str, pd.DataFrame]], default = None
        Hydro table path or DataFrame
    hucs: List[str]]
        List of hucs to process GMS
    inundation_raster : str
        Name of inundation extent raster
    inundation_polygon: Optional[str], default = None
        Name of inundation polygon vector
    depths_raster : str
        Name of depth raster
    verbose: Optional[bool], default = False
        Whether to silence output or not
    log_file: Optional[str], default = None
        Name of file to log output
    inundation_results_file_path: Optional[str], default = None
        Name of file to output filenames from inundation routine
    precalb_option: Optional[bool], default = False
        Whether to use precalb discharge in hydrotable
    windowed: Optional[bool], default = False
        Whether to use window memory optimization
    multi_process: Optional[bool], default = False
        Whether to use process pool, otherwise use thread pool

    Returns
    -------
    pd.DataFrame
        Output filenames from inundation routine

    """

    # print("++++++++++++++++++++++++++++++++++++++++++++++++")
    # print("+++ starting iundate_gms")
    # print(locals())

    # input handling
    if isinstance(hucs, list):
        if len(hucs) == 0:
            raise ValueError(f"hucs (list or string) can not be empty: [{hucs}]")
        # validate that huc list is valid
        try:
            _ = (i for i in hucs)
        except TypeError:
            raise ValueError(f"hucs argument must be an iterable: [{hucs}]")

    if isinstance(hucs, str):
        if hucs == "":
            raise ValueError(f"hucs (list or string) can not be empty: [{hucs}]")
        hucs = [hucs]

    if not os.path.exists(forecast_file_path):
        raise ValueError(f"forecast file does not exist [{hucs}]")

    if verbose and show_progress_bar:
        logging.info(
            f"--- Starting Inundate_gms for {forecast_file_path} based on {hydrofabric_dir} with {num_threads} workers"
        )
    else:
        logging.debug(f"--- Starting Inundate_gms for {forecast_file_path} based on {hydrofabric_dir}")

    # June 2026:
    # Most scripts that call this function use an ProcessPoolExecutor. When it first starts, they all hit this
    # function at the same time. Putting a random time sleeper helps manage that a little lowering
    # resource needs a little and network bottlenecks, especially if they are all hitting one hucs files at one time.
    # random between 0 and 3 seconds.
    time.sleep(random.randint(0, 3))

    # log file
    # if log_file is not None:
    #     if os.path.exists(log_file):
    #         os.remove(log_file)

    #     if verbose:
    #         with open(log_file, 'a') as f:
    #             f.write("HUC8,BranchID,Exception")

    # load fim inputs (hand level)
    hucs_branches = pd.read_csv(
        os.path.join(hydrofabric_dir, "fim_inputs.csv"), header=None, dtype={0: str, 1: str}
    )

    hucs = set(hucs)
    huc_indices = hucs_branches.loc[:, 0].isin(hucs)
    hucs_branches = hucs_branches.loc[huc_indices, :]

    # -----------------------------------
    # get number of branches
    # number_of_branches = len(hucs_branches)

    # make inundate generator
    # Aug 2026: Theadprocesspools no longer like lazy loaded generator
    inundate_input_args = __inundate_gms_generator(
        hucs_branches,
        hydrofabric_dir,
        inundation_raster_path,
        depths_raster_path,
        forecast_file_path,
        hydro_table_path,
        verbose=verbose,
        windowed=windowed,
        precalb_option=precalb_option,
    )

    # print(".........................................")
    # print("and branches are...")
    # print(hucs_branches)

    # # start up process pool
    # # better results with Process pool
    # if multi_process is True:
    #     executor = ProcessPoolExecutor(max_workers=num_threads)
    # else:

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

                # creating a future_id, will use huc-branchid
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
        # Yes.. I don't really have a good identifier to help with context, but this is better than nothing
        # We do not want to add the list of hucs as it might be huge, depending on what scripts is calling
        # this.
        logging.critical(f"Error while inundating based on {forecast_file_path}")
        logging.critical(traceback.format_exc())
        # Note: you can not use sys.exit in ProcessPools.
        raise ex  # yes.. reraise, so we can shut inudation down.

    # make filename dataframe
    if len(inun_data_list) != 0:
        raster_paths_df = pd.DataFrame(inun_data_list)

        if inundation_results_file_path is not None and inundation_results_file_path != "":
            if not os.path.isdir(os.path.dirname(inundation_results_file_path)):
                os.makedirs(os.path.dirname(inundation_results_file_path))

            raster_paths_df.to_csv(inundation_results_file_path, index=False)
            logging.info(f"Inundation raster mapping data saved to {inundation_results_file_path}")

        return raster_paths_df
    else:
        return None

    # collect output filenames
    # inundation_raster_fileNames = [None] * number_of_branches

    # # inundation.py.__inundate_in_huc never returned a poly, it was hardcoded to None
    # inundation_polygon_fileNames = [None] * number_of_branches
    # depths_raster_fileNames = [None] * number_of_branches
    # hucCodes = [None] * number_of_branches
    # branch_ids = [None] * number_of_branches

    # # Note: rasterio opened files are never truly thread safe. But, most of our tools are processed
    # # one branch at a time. Except synthesize_test_cases which has its own processpool so there could
    # # be collisions there, but it has a random sleep timer to help.

    # executor_generator = {executor.submit(inundate, **inp): ids for inp, ids in inundate_input_generator}
    # idx = 0
    # for future in tqdm(
    #     as_completed(executor_generator),
    #     total=len(executor_generator),
    #     desc=f"Inundating branches with {num_threads} workers",
    #     disable=(not verbose),
    # ):
    #     hucCode, branch_id = executor_generator[future]

    #     try:
    #         future.result()

    #     except NoForecastFound as exc:
    #         if log_file is not None:
    #             print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
    #         elif verbose:
    #             print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

    #     except hydroTableHasOnlyLakes as exc:
    #         if log_file is not None:
    #             print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
    #         elif verbose:
    #             print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

    #     except Exception as exc:
    #         traceback.print_exc(file=sys.stdout)
    #         if log_file is not None:
    #             print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
    #         else:
    #             print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")
    #     else:
    #         hucCodes[idx] = hucCode
    #         branch_ids[idx] = branch_id

    #         try:
    #             inundation_raster_fileNames[idx] = future.result()[0][0]
    #         except TypeError:
    #             pass

    #         try:
    #             depths_raster_fileNames[idx] = future.result()[1][0]
    #         except TypeError:
    #             pass

    #         # inundation.py.__inundate_in_huc never returned a poly, it was hardcoded to None
    #         # try:
    #         #     inundation_polygon_fileNames[idx] = future.result()[2][0]
    #         # except TypeError:
    #         #     pass

    #         idx += 1

    # # power down pool
    # executor.shutdown(wait=True)

    # make filename dataframe
    # inundation.py.__inundate_in_huc never returned a poly, it was hardcoded to None
    # output_fileNames_df = pd.DataFrame(
    #     {
    #         "huc8": hucCodes,
    #         "branchID": branch_ids,
    #         "inundation_rasters": inundation_raster_fileNames,
    #         "depths_rasters": depths_raster_fileNames,
    #         # "inundation_polygons": inundation_polygon_fileNames,
    #     }
    # )

    # if output_fileNames is not None:
    #     output_fileNames_df.to_csv(output_fileNames, index=False)

    # return output_fileNames_df


# July 2026: No longer a true generator as our adjusted Threadpool does not like lazy loading anymore
def __inundate_gms_generator(
    hucs_branches: pd.DataFrame,
    hydrofabric_dir: str,
    inundation_raster_path: str,
    depths_raster_path: str,
    forecast_file_path: str,
    hydro_table_path: str,
    verbose: bool = False,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
    # ) -> Tuple[dict, List[str]]:
) -> List[dict]:
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
    forecast : str
        Dataset with streamflow associated with feature id
    hydro_table_path: str
        Hydrotable DataFrame.
    verbose: Optional[bool], default = False
        Whether to silence output or not
    precalb_option: Optional[bool], default = False
        Whether to use precalb discharge in hydrotable
    windowed: Optional[bool], default = False
        Whether to use window memory optimization

    Returns
    -------
    An list of dictionaries
        Data inputs for inundate gms and the respective branch ids

    """
    inundation_inputs = []

    # Iterate over branches
    for ___, row in hucs_branches.iterrows():
        huc = str(row[0])
        branch_id = str(row[1])

        huc_dir = os.path.join(hydrofabric_dir, huc)
        branch_dir = os.path.join(huc_dir, "branches", branch_id)

        rem_file_name = f"rem_zeroed_masked_{branch_id}.tif"
        rem_branch_path = os.path.join(branch_dir, rem_file_name)

        catchments_file_name = f"gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif"
        catchments_branch_path = os.path.join(branch_dir, catchments_file_name)

        # if isinstance(hydro_table_path, pd.DataFrame):
        #     hydro_table_all = hydro_table_path.set_index(["HUC", "feature_id", "HydroID"], inplace=False)
        #     hydro_table_branch_df = hydro_table_all.loc[hydro_table_all["branch_id"] == int(branch_id)]
        # elif isinstance(hydro_table_path, str):
        if isinstance(hydro_table_path, str):
            hydro_table_branch_df = hydro_table_path.format(branch_id)
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

            # TODO: Jul 2026: Change to parquet, but watch for required columns and indexes
            if (
                s3_or_local_path_exists(os.path.join(huc_dir, "hydrotable.feather"))
                and precalb_option == False
            ):  # Quicker reads
                hydro_table_huc = os.path.join(huc_dir, "hydrotable.feather")
                hydro_table_all = pd.read_feather(hydro_table_huc)
            elif s3_or_local_path_exists(os.path.join(huc_dir, "hydrotable.csv")):
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
                # hydro_table_huc = None
                # we always load a branch HT or filter a HUC or other HT
                raise Exception(f"[{huc}:{branch_id}] - HUC level Hydrotable can not be found.")

            if precalb_option:
                if "precalb_discharge_cms" not in hydro_table_all.columns:
                    raise ValueError("Missing expected column 'precalb_discharge_cms' in hydrotable.")
                missing_count = hydro_table_all["precalb_discharge_cms"].isna().sum()
                if missing_count > 0:
                    # Yes.. write back to itself as it affect memory pointers.
                    # We are moving away from inplace args.
                    hydro_table_all = hydro_table_all["precalb_discharge_cms"].fillna(
                        hydro_table_all["discharge_cms"]
                    )

            # if hydro_table_huc is not None and s3_or_local_isfile(hydro_table_huc):
            # Yes.. write back to itself as it affect memory pointers.
            # We are moving away from inplace args.

            hydro_table_all.set_index(["HUC", "feature_id", "HydroID"])
            hydro_table_branch_df = hydro_table_all.loc[hydro_table_all["branch_id"] == int(branch_id)]
            # else:
            # Aug 1, 2026:  Change to purely HUC level and not branch level
            # Earlier FIM4 versions only have branch level hydrotables
            # hydro_table_branch_df = os.path.join(branch_dir, f"hydroTable_{branch_id}.csv")

        xwalked_file_name = f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg"
        catchment_poly_path = os.path.join(branch_dir, xwalked_file_name)

        # branch output
        # Some other functions that call in here already added a huc, so only add it if not yet there

        if (inundation_raster_path is not None) and (huc not in inundation_raster_path):
            inundation_raster_path = fh.append_id_to_file_name(inundation_raster_path, [huc, branch_id])
        else:
            inundation_raster_path = fh.append_id_to_file_name(inundation_raster_path, branch_id)

        if (depths_raster_path is not None) and (huc not in depths_raster_path):
            depths_raster_path = fh.append_id_to_file_name(depths_raster_path, [huc, branch_id])
        else:
            depths_raster_path = fh.append_id_to_file_name(depths_raster_path, branch_id)

        # identifiers
        # identifiers = (huc, branch_id)

        # inundate input
        inundate_input = {
            "huc": huc,
            "branch_id": branch_id,
            "rem_branch_path": rem_branch_path,
            "catchments_branch_path": catchments_branch_path,
            "catchment_poly_path": catchment_poly_path,
            "hydro_table_branch_df": hydro_table_branch_df,
            "forecast_file_path": forecast_file_path,
            #            "mask_type": "filter",
            #             "hucs": None,
            #             "hucs_layerName": None,
            #             "subset_hucs": None,
            #             "num_workers": 1,
            #             "aggregate": False,
            "inundation_raster_path": inundation_raster_path,
            "depths_raster_path": depths_raster_path,
            "verbose": verbose,
            "precalb_option": precalb_option,
            "windowed": windowed,
        }
        inundation_inputs.append(inundate_input)

    return inundation_inputs


if __name__ == "__main__":

    # TODO: July 2026: The args from command line here need to be upgraded. I ran short of time
    # while updating inundation files.
    # Need some samples here if we rebuild this part.

    # parse arguments
    parser = argparse.ArgumentParser(description="Inundate FIM")
    parser.add_argument(
        "-y", "--hydrofabric_dir", help="Directory path to FIM hydrofabric by processing unit", required=True
    )
    parser.add_argument(
        "-u", "--hucs", help="List of HUCS to run", required=False, default=None, type=str, nargs="+"
    )
    parser.add_argument("-f", "--forecast", help="Forecast discharges in CMS as CSV file", required=True)
    parser.add_argument(
        "-i",
        "--inundation-raster",
        help="Inundation Raster output. Only writes if designated.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-p",
        "--inundation-polygon",
        help="Inundation polygon output. Only writes if designated.",
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
        "-v", "--verbose", help="Verbose printing", required=False, default=None, action="store_true"
    )

    Inundate_gms(**vars(parser.parse_args()))
