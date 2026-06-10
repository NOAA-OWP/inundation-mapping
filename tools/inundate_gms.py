#!/usr/bin/env python3

import argparse
import os
import logging
import sys
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple, Union

import pandas as pd
from inundation import NoForecastFound, hydroTableHasOnlyLakes, inundate
from tqdm import tqdm

# Suppress only FutureWarnings
# TODO: Jun 2026: This is a temp fix as gval is what is issuing this
# A new gval is already ready to plug into fix this. We can remove it later.
warnings.simplefilter(action='ignore', category=FutureWarning)


from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import s3_or_local_isfile, s3_or_local_path_exists


def Inundate_gms(
    hydrofabric_dir: str,
    forecast: Union[str, pd.DataFrame],
    num_workers: Optional[int] = 1,
    hydro_table_df: Optional[Union[str, pd.DataFrame]] = None,
    hucs: Optional[List[str]] = None,
    inundation_raster: Optional[str] = None,
    depths_raster: Optional[str] = None,
    verbose: Optional[bool] = False,
    log_file: Optional[str] = None,
    output_fileNames: Optional[str] = None,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
    multi_process: Optional[bool] = False,
    debug: Optional[bool] = False,
) -> pd.DataFrame:
    """
    Run inundation using the Generalized Mainstem methodology

    hydrofabric_dir : str
        Directory with flood inundation mapping outputs
    forecast: Union[str, pd.DataFrame]
        Data with streamflow associated with feature id
    num_workers: Optional[int], default = 1
        Number of threads to useNumber of processes to run in parallel
    hydro_table_df: Optional[Union[str, pd.DataFrame]], default = None
        Hydro table path or DataFrame
    hucs: Optional[List[str]], default = None
        List of hucs to process GMS
    inundation_raster : str
        Name of inundation extent raster
    inundation_polygon: Optional[str], default = None
        Name of inundation polygon vector
    depths_raster : str
        Name of depth raster
    verbose: Optional[bool], default = False
        Whether to qsilence output or not
    log_file: Optional[str], default = None
        Name of file to log output
    output_fileNames: Optional[str], default = None
        Name of file to output filenames from gms inundation routine
    precalb_option: Optional[bool], default = False
        Whether to use precalb discharge in hydrotable
    windowed: Optional[bool], default = False
        Whether to use window memory optimization
    multi_process: Optional[bool], default = False
        Whether to use process pool, otherwise use thread pool

    Returns
    -------
    pd.DataFrame
        Output filenames from gms inundation routine

    """
    # input handling
    if hucs is not None:
        try:
            _ = (i for i in hucs)
        except TypeError:
            raise ValueError("hucs argument must be an iterable")

    if isinstance(hucs, str):
        hucs = [hucs]

    num_workers = int(num_workers)

    # log file
    # if log_file is not None and log_file != "":
    #     # if os.path.exists(log_file):
    #     #     os.remove(log_file)

    #     if verbose:
    #         with open(log_file, 'a') as f:
    #             f.write("HUC8,BranchID,Exception")

    if debug:
        logging.debug(f"Starting Inundate_gms for {hucs}")

    # load fim inputs
    hucs_branches = pd.read_csv(
        os.path.join(hydrofabric_dir, "fim_inputs.csv"), header=None, dtype={0: str, 1: str}
    )

    if hucs is not None:
        hucs = set(hucs)
        huc_indices = hucs_branches.loc[:, 0].isin(hucs)
        hucs_branches = hucs_branches.loc[huc_indices, :]

    # get number of branches
    number_of_branches = len(hucs_branches)

    # make inundate generator
    inundate_input_generator = __inundate_gms_generator(
        hucs_branches,
        hydrofabric_dir,
        inundation_raster,
        depths_raster,
        forecast,
        hydro_table_df,
        verbose=False,
        windowed=windowed,
        precalb_option=precalb_option,
        debug=debug
    )

    if debug:
        logging.debug(f"back from __inundate_gms_generator for {hucs} with number of branches of {len(hucs_branches)}")

    # TODO: May 2026: This shoul be put to our more normal Processpool or threadpool.
    # We need the try catch to manage catestropic failes that we want to forward.
    # As is, this will just keep processing all branches even with exceptions.
    # Also see notes below about stopping catestropic branch fails.

    # collect output filenames
    inundation_raster_fileNames = [None] * number_of_branches
    inundation_polygon_fileNames = [None] * number_of_branches
    depths_raster_fileNames = [None] * number_of_branches
    hucCodes = [None] * number_of_branches
    branch_ids = [None] * number_of_branches

    try:
        with ProcessPoolExecutor(max_workers=num_workers, max_tasks_per_child=num_workers) as executor:

            # TODO: really should have a "with" to manage scope
            executor_generator = {executor.submit(inundate, **inp): ids for inp, ids in inundate_input_generator}

            idx = 0
            for future in tqdm(
                as_completed(executor_generator),
                total=len(executor_generator),
                desc=f"Inundating branches with {num_workers} workers",
                disable=(not verbose),
            ):

                hucCode, branch_id = executor_generator[future]

                try:
                    future.result()

                except NoForecastFound as exc:
                    # TODO: Jun 2026: Now that we are adding logging, do we need this log file test?
                    if log_file is not None and log_file != "":
                        # print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
                        # , file=open(log_file, "a")
                        logging.warning(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")
                    elif debug:
                        # print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")
                        logging.warning(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

                except hydroTableHasOnlyLakes as exc:
                    # # TODO: Jun 2026: Now that we are adding logging, do we need this log file test?
                    # if log_file is not None and log_file != "":
                    #     print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
                    # elif verbose:
                    #     print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

                    if log_file is not None and log_file != "":
                        # print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
                        # , file=open(log_file, "a")
                        logging.warning(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")
                    elif debug:
                        # print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")
                        logging.warning(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

                except Exception as exc:
                    # TODO: Jun 2026: Now that we are adding logging, do we need this log file test?
                    if log_file is not None and log_file != "":
                        print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
                        # print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

                    logging.critical(f"Critical Error: {hucCode},{branch_id},{exc.__class__.__name__}")
                    logging.critical(traceback.format_exc())
                    
                    # Note: You can not sys.exit from a ProcessPoolExecutor directly
                    # all processes inside the ProcessPoolExecutor can not be aborted
                    # but you can shutdown and stop the executor from creating more.
                    # The trick is recongizing that each child process can throw an
                    # ThreadPoolExecutors can abort treads in process.
                    executor.shutdown(
                        wait=False, cancel_futures=True
                    )  # tells the ProcessPoolExecutor to stop accepting new tasks. Even cancel the running tasks as soon as possible                

                    raise exc  # yes.. reraise
                
                else: # excutes only if the try was successful
                    hucCodes[idx] = hucCode
                    branch_ids[idx] = branch_id

                    try:
                        inundation_raster_fileNames[idx] = future.result()[0][0]
                    except TypeError:
                        pass
                    try:
                        depths_raster_fileNames[idx] = future.result()[1][0]
                    except TypeError:
                        pass
                    try:
                        inundation_polygon_fileNames[idx] = future.result()[2][0]
                    except TypeError:
                        pass
                    idx += 1

            # # power down pool
            # if future.running():
            #     executor.shutdown(wait=True)

    except Exception as ex:
        msg = f"Critical Error: {hucs},{exc.__class__.__name__}"
        # TODO: Jun 2026: Now that we are adding logging, do we need this log file test?
        if log_file is not None and log_file != "":
            print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
        logging.critical(msg)
        logging.critical(traceback.format_exc())
        raise ex  # yes.. reraise

    # make filename dataframe
    output_fileNames_df = pd.DataFrame(
        {
            "huc8": hucCodes,
            "branchID": branch_ids,
            "inundation_rasters": inundation_raster_fileNames,
            "depths_rasters": depths_raster_fileNames,
            "inundation_polygons": inundation_polygon_fileNames,
        }
    )

    if output_fileNames is not None:
        output_fileNames_df.to_csv(output_fileNames, index=False)

    return output_fileNames_df


def __inundate_gms_generator(
    hucs_branches: pd.DataFrame,
    hydrofabric_dir: str,
    inundation_raster: str,
    depths_raster: str,
    forecast: Union[str, pd.DataFrame],
    hydro_table_df: Union[str, pd.DataFrame],
    verbose: Optional[bool] = False,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
    debug: Optional[bool] = False,
) -> Tuple[dict, List[str]]:
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
    debug: Optional[bool], default=False
        If True, extra logging lines will print to file only.
    Returns
    -------
    Tuple[dict, List[str]]
        Data inputs for inundate gms and the respective branch ids

    """
    # Iterate over branches
    if debug:
        logging.debug(f"In __inundate_gms_generator for {hydrofabric_dir}")

    for idx, row in hucs_branches.iterrows():
        huc = str(row[0])
        branch_id = str(row[1])

        huc_dir = os.path.join(hydrofabric_dir, huc)
        branch_dir = os.path.join(huc_dir, "branches", branch_id)

        if debug:
            logging.debug(f" __inundate_gms_generator for {branch_dir}")

        rem_file_name = f"rem_zeroed_masked_{branch_id}.tif"
        rem_branch = os.path.join(branch_dir, rem_file_name)

        catchments_file_name = f"gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif"
        catchments_branch = os.path.join(branch_dir, catchments_file_name)

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
        catchment_poly = os.path.join(branch_dir, xwalked_file_name)

        # branch output
        # Some other functions that call in here already added a huc, so only add it if not yet there
        if (inundation_raster is not None) and (huc not in inundation_raster):
            inundation_branch_raster = fh.append_id_to_file_name(inundation_raster, [huc, branch_id])
        else:
            inundation_branch_raster = fh.append_id_to_file_name(inundation_raster, branch_id)

        if (depths_raster is not None) and (huc not in depths_raster):
            depths_branch_raster = fh.append_id_to_file_name(depths_raster, [huc, branch_id])
        else:
            depths_branch_raster = fh.append_id_to_file_name(depths_raster, branch_id)

        # identifiers
        identifiers = (huc, branch_id)

        # inundate input
        inundate_input = {
            "rem": rem_branch,
            "catchments": catchments_branch,
            "catchment_poly": catchment_poly,
            "hydro_table": hydro_table_branch,
            "forecast": forecast,
            "mask_type": "filter",
            "hucs": None,
            "hucs_layerName": None,
            "subset_hucs": None,
            "num_workers": 1,
            "aggregate": False,
            "inundation_raster": inundation_branch_raster,
            "depths": depths_branch_raster,
            "quiet": not verbose,
            "precalb_option": precalb_option,
            "windowed": windowed,
            "debug": debug,
        }

        yield inundate_input, identifiers


if __name__ == "__main__":

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
