#!/usr/bin/env python3

import argparse
import logging
import os
import random
import time
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
from inundation import inundate

from src.utils.shared_functions import FIM_Helpers as fh


# Suppress only FutureWarnings
# TODO: Jun 2026: This is a temp fix as gval is what is issuing this
# A new gval is already ready to plug into fix this. We can remove it later.
warnings.simplefilter(action='ignore', category=FutureWarning)


# This can process one huc at a time and all of its branches. It does use threading inside
# of it to speed up processing the branches. It does not have tqdm and you will need to create
# it on the outside. See synthesize_test_case for examples.
def Inundate_gms(
    hydrofabric_dir: str,
    flow_file_path: str,
    huc: str,
    hydro_table_path: Optional[str] = None,  # When None, loads branch level Hydrotables
    num_threads: Optional[int] = 1,
    inundation_raster_path: Optional[str] = None,
    depths_raster: Optional[str] = None,
    verbose: Optional[bool] = False,
    output_fileNames: Optional[str] = None,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = True
) -> pd.DataFrame:
    """
    Run inundation using the Generalized Mainstem methodology

    hydrofabric_dir : str
        Directory with flood inundation mapping outputs
    flow_file_path : str
        Path to flow file to be used for inundation. Feature_ids in flow_file should be present in supplied HUC.
    huc: str
    hydro_table_path: Optional[str], default = None
        Hydro table path or None.              
    num_threads: Optional[int], default = 1
        Number of threads to run in parallel
    hydro_table_df: Optional[Union[str, pd.DataFrame]], default = None
        Hydro table path or DataFrame (gets a hydrotable from each branch in the hand folder)
        If not supplied will default to loading each branch hydrotable
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
        
    Returns
    -------
    pd.DataFrame
        Output filenames from gms inundation routine

    """

    if huc is None or huc == "":
        raise Exception("huc is either None or empty")

    # June 2026:
    # Most scripts that call this function use an ProcessPoolExecutor. When it first starts, they all hit this
    # function at the same time. Putting a random time sleeper helps manage that a little lowering
    # resource needs a little and network bottlenecks, especially if they are all hitting one hucs files at one time.
    # random between 0 and 3 seconds.
    time.sleep(random.randint(0, 3))

    # logger = logging.getLogger()
    # logger.info("This log went directly to the file (synchronously).")

    # --- STEP 2: Intercept and convert it dynamically ---
    # Pass None for root logger, or pass a specific string name like 'my_app'
    # log_listener = convert_logger_to_async(logger_name=None)

    # --- STEP 3: Run your ThreadPoolExecutor safely ---
    # logger.info("This log and all future worker logs now use the fast queue!")

    if verbose:
        logging.info(
            f"--- Starting Inundate_gms for {flow_file_path} based on {huc} - {hydrofabric_dir}"
        )
    else:
        logging.debug(
            f"--- Starting Inundate_gms for {flow_file_path} based on {huc} - {hydrofabric_dir}"
        )

    # load fim inputs
    # We load against this as it will not include branches that failed.
    hucs_branches_all = pd.read_csv(
        os.path.join(hydrofabric_dir, "fim_inputs.csv"), header=None, dtype=str, names=["huc", "branch_id"]
    )

    hucs_branches = hucs_branches_all.loc[hucs_branches_all['huc'] == huc]

    # make inundate generator
    # Jun 2026: generators do not play well with threadpoolexecutors as it does not like lazy loaders
    # Changed to an array of dicionaries and also helps me with counts.
    inundate_input_args = __inundate_gms_generator(
        hucs_branches=hucs_branches,
        hydrofabric_dir=hydrofabric_dir,
        inundation_raster_path=inundation_raster_path,
        depths_raster=depths_raster,
        flow_file_path=flow_file_path,
        hydro_table_path=hydro_table_path,
        verbose=verbose,
        precalb_option=precalb_option,
        windowed=windowed
    )

    num_inputs_args = len(inundate_input_args)  # each arg is a huc and branch

    msg = f"Starting threading {num_inputs_args} branches against {flow_file_path} using {num_threads} workers"
    if verbose:
        logging.info(msg)
    else:
        logging.debug(msg)

    inun_data_list = []  # list of dictionaries
    try:
        # Keep only a bounded number of in-flight futures so large branch counts do not
        # accumulate an ever-growing set of pending tasks and their associated payloads.
        with ThreadPoolExecutor(max_workers=num_threads) as executor:

            # Some mp functions might throw an exception, which means it may not get to as_completed
            futures_dict = [executor.submit(inundate, **arg) for arg in inundate_input_args]

            # It is ok to let the others finish even if one is an exception
            for future in as_completed(futures_dict):
                try:
                    if future.cancelled():  # for keyboard CTRL-C's generally
                        return

                    if future.exception() is not None:
                        raise future.exception()  # re-raise it

                    result = future.result()
                    if result is not None:  # Yes, we can legitimately get a None
                        inun_data_list.append(result)

                except Exception as exc:
                    print(
                        "Thread pool shutting down. This may take a while depending on how many jobs."
                        " Jobs currently in progress will need to complete for this can fully shut down.",
                        flush=True,
                    )
                    # Note: You can not sys.exit from executors directly
                    # all processes inside the ThreadPools tasks can be aborted
                    # but it is very messy and not really necessary
                    # pbar.close()  # aborts the progress bar
                    # executor.shutdown(wait=True, cancel_futures=True)

                    raise exc  # yes.. reraise

                # # Use a lambda to unpack the dictionary keys into keyword arguments
                # # Note: Must use a map and not submit becuase it is related to when each dictionaries args
                # # are called. While you use a generator, you won't want to as you want them to unpack now
                # # and here and not inside the inundate function
                # # Perfect.. these are processign in sets of num_threads(ie 10), each group of 10 starts together
                # # and ends together. Clean and fast.
                # results  = executor.map(lambda d: inundate(**d), batch)            
                
                # for result in results:
                # # Check if the returned item is an instance of an Exception
                #     if isinstance(result, Exception):
                #         pool_errors.append(result)
                #         pbar.set_postfix(errors=len(pool_errors))  # Update error count on the bar
                #     else:
                #         huc, branch_id, inun_data = result
                #         if inun_data is not None:  # and it is ok if it does come back None
                #             inun_data_list.append(inun_data)            

            # while True:
            #     try:
            #         # Manually pull items to catch exceptions individually
            #         result = next(iterator)
            #         huc, branch_id, inun_data = result
            #         inun_data_list.append(inun_data)
            #         # print(f"hi.. back from result for {huc}-{branch_id}")
            #     except StopIteration:
            #         break  # The generator is empty, exit loop
            #     except Exception:
            #         raise  # Just re-raise, but having this helps clear memory

                # The loop continues to the next item automatically

            # def _handle_completed_future(future):
            #     future_id = pending_futures[future]
            #     try:
            #         if future.cancelled():  # for keyboard CTRL-C's generally
            #             return

            #         if future.exception() is not None:
            #             raise future.exception()  # re-raise it

            #         result = future.result()
            #         if result is not None:  # Yes, we can legitimately get a None
            #             inun_data_list.append(result)

            #     except Exception as exc:
            #         context = f"{sys._getframe().f_code.co_name} -- {future_id}"
            #         logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
            #         logging.critical(f"Error: {context} : {exc}")
            #         in_error = True

            #         print(
            #             "Thread pool shutting down. This may take a while depending on how many jobs."
            #             " Jobs currently in progress will need to complete for this can fully shut down.",
            #             flush=True,
            #         )

            #         # Note: You can not sys.exit from executors directly
            #         # all processes inside the ThreadPools tasks can be aborted
            #         # but it is very messy and not really necessary
            #         # pbar.close()  # aborts the progress bar
            #         # executor.shutdown(wait=True, cancel_futures=True)
            #         raise exc  # yes.. reraise


            #     finally:
            #         pending_futures.pop(future, None)
            #         # pbar.update(1)
            #         del future

            #     return in_error

                # pending_futures = {}
                # for inp in inundate_input_args:
                #     future = executor.submit(inundate, **inp)
                #     future_id = f"{inp['huc']}-{inp['branch_id']}"
                #     pending_futures[future] = future_id

                #     if len(pending_futures) >= max(1, num_threads):
                #         done_futures, _ = wait(pending_futures, return_when=FIRST_COMPLETED)
                #         for done_future in done_futures:
                #             _handle_completed_future(done_future)

            # while pending_futures:
            #     done_futures, _ = wait(pending_futures, return_when=FIRST_COMPLETED)
            #     for done_future in done_futures:
            #        in_error = _handle_completed_future(done_future)

            # if pbar:
            #     pbar.close()

    except Exception as ex:
        logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
        logging.critical(f"Error while inundating based on {flow_file_path}")
        logging.critical(traceback.format_exc())
        # Note: you can not use sys.exit in ProcessPools.
        raise ex  # yes.. reraise, so we can shut inudation down.


    # # --- STEP 4: Shutdown cleanup ---
    # if log_listener:
    #     log_listener.stop()


    # make filename dataframe
    # Jun 2026: At this point, only one set of rec based on huc are returned, but the df can be concat
    # later
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
    inundation_raster_path: str,
    depths_raster: str,
    flow_file_path: str,
    hydro_table_path: Optional[str] = None,  # When None, loads branch level Hydrotables later
    verbose: Optional[bool] = False,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
):

    """
    Generator for use in parallelizing inundation
    Note: We can not use a true generator with the yield command as Threadpoolexecutors dont like them.

    Parameters
    ----------
    hucs_branches : pd.DataFrame
        DataFrame containing huc8 and branch ids
    hydrofabric_dir : str
        Directory with flood inundation mapping outputs
    inundation_raster_path : str
        Name of inundation extent raster
    depths_raster : str
        Name of depth raster
    flow_file_path : str
        Path to file with streamflow associated with feature id
    verbose: Optional[bool], default = False
        Whether to silence output or not
    precalb_option: Optional[bool], default = False
        Whether to use precalb discharge in hydrotable
    windowed: Optional[bool], default = False
        Whether to use window memory optimization

    Returns
    --------------
    A list of dictionaries, each mapped to the exact arg in the inundation function.


    """
    # Iterate over branches
    # logging.debug(f"Loading inundate gms generator for {hydrofabric_dir}")
    inundation_input_args = []
    
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

    for ___, row in hucs_branches.iterrows():
        huc = str(row[0])
        branch_id = str(row[1])
        huc_dir = os.path.join(hydrofabric_dir, huc)
        branch_dir = os.path.join(huc_dir, "branches", branch_id)

        if hydro_table_path is None:  # then load the branch one
            hydro_table_path = os.path.join(branch_dir, f"hydroTable_{branch_id}.csv")

        # logging.debug(f" __inundate_gms_generator for {branch_dir}")

        rem_file_name = f"rem_zeroed_masked_{branch_id}.tif"
        rem_branch_path = os.path.join(branch_dir, rem_file_name)

        catchments_file_name = f"gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif"
        catchments_file_path = os.path.join(branch_dir, catchments_file_name)

        # And we now default to loading the branch hydrotable, pre-calib or not
        # Jun 2026: Moved to inundate function. As we no longer have a loaded hydrotable df
        # We always load them in inundate function chain

        # if precalb_option:
        #     if "precalb_discharge_cms" not in hydro_table_all.columns:
        #         raise ValueError("Missing expected column 'precalb_discharge_cms' in hydrotable.")
        #     missing_count = hydro_table_all["precalb_discharge_cms"].isna().sum()
        #     if missing_count > 0:
        #         hydro_table_all["precalb_discharge_cms"].fillna(
        #             hydro_table_all["discharge_cms"], inplace=True
        #         )
        #     hydro_table_branch = os.path.join(branch_dir, f"hydroTable_{branch_id}.csv")

        xwalked_file_name = f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg"
        catchments_poly_path = os.path.join(branch_dir, xwalked_file_name)

        # branch output
        # Some other functions that call in here already added a huc, so only add it if not yet there
        if (inundation_raster_path is not None) and (huc not in inundation_raster_path):
            inundation_branch_raster_path = fh.append_id_to_file_name(inundation_raster_path, [huc, branch_id])
        else:
            inundation_branch_raster_path = fh.append_id_to_file_name(inundation_raster_path, branch_id)

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
            "hydro_table": hydro_table_path,
            "flow_file_path": flow_file_path,
            "inundation_branch_raster_path": inundation_branch_raster_path,
            "depths_branch_raster_path": depths_branch_raster_path,
            "verbose": verbose,
            "precalb_option": precalb_option,
            "windowed": windowed,
        }
        # yield inundate_input
        inundation_input_args.append(inundate_input)
    return inundation_input_args


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Inundate FIM")
    parser.add_argument(
        "-y", "--hydrofabric-dir", help="Directory path to FIM hydrofabric by processing unit", required=True
    )
    parser.add_argument(
        "-u", "--huc", help="One and exactly one", required=True, default="", type=str
    )
    parser.add_argument(
        "-f", "--flow-file-path", help="Forecast discharges in CMS as CSV file", required=True
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

    Inundate_gms(**vars(parser.parse_args()))
