import argparse
import errno
import logging
import os
import traceback
from timeit import default_timer as timer
from typing import List, Optional, Union

import pandas as pd
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import s3_or_local_path_exists
from src.utils.shared_variables import elev_raster_ndv

# It now uses MultiThread versus MultiProc
# Jun 2026:
# The original log_file was used as a simple file io log file saving on demand
# This can now only handle one huc at a time. Add your own iterator, processpool and/or tqdm
# before calling this. See Synthesize_test_case.py -> run_test_case.py - alpha_test
# Also, hydrotables are no longer passed in as each branch will have its own hydrotable

# This function now optionally supports a ProcessPool system and optional TQDM at this level.
# instead of a threadpool inside inundate_gms which is prone to become overloaded and is now
# using exclusively threads. It now designed to process only one huc at time for performance and memory
# isues.

# Now this function will handle MP and TQDM optionally if required and manage single calls to
# inundate_gms one at a time, but concat the final mosiacked image here if required.
# Various fields have been dropped that were invalid or not used downstream, or were not in use
# Some of them such as iundation_polygon_path did not work anyways.
def produce_mosaicked_inundation(
    hydrofabric_dir: str,
    hucs: Union[str, List[str]],
    flow_file_path: str,
    hydro_table_path: Optional[str] = None,
    inundation_raster_path: Optional[str] = None,
    # inundation_polygon_path: Optional[str] = None,  # June 2026, was not in use and did not work
    depths_raster_path: Optional[str] = None,
    map_filename: Optional[str] = None,
    remove_intermediate: Optional[bool] = True,
    verbose: Optional[bool] = False,
    is_mosaic_for_branches: Optional[bool] = False,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = True,
    nodata: Optional[int] = elev_raster_ndv,
    use_process_pool: Optional[bool] = False,
    show_progress_bar: Optional[bool] = False,
    num_pool_workers: Optional[int] = 1,
    num_threads: Optional[int] = 1,
):

    """
    # Jun 2026: Temp disabled and appears to have not been in use or even work.
    # Note: As befoe, Flow files must have feature_id, and discharge column.
    # hydrotable paths optionally be added, but if not provided, the system will use the huc list and its
    # branch hydrotables. If you do submit your own hydrotable, ensure it has a "HUC" column along with
    # other standard fields used for inundation.
    

    # Old notes Pre June 2026
    #       This function calls Inundate_gms and Mosaic_inundation to produce inundation maps.
    #       Possible outputs include inundation rasters encoded by HydroID (negative HydroID for dry and positive
    #       HydroID for wet), polygons depicting extent, and depth rasters. The function requires a flow file
    #       organized by NWM feature_id and discharge in cms. "feature_id" and "discharge" columns MUST be present
    #       in the flow file.

    # 

    Parameters
    ----------
    hydrofabric_dir : str
        Path to hydrofabric directory where FIM outputs were written by fim_pipeline
    hucs : list or str
        The HUC(s) for which to produce mosaicked inundation files.
    flow_file_path : str
        Path to flow file to be used for inundation. Feature_ids in flow_file should be present in supplied HUC.
    hydro_table_path : Optional[str], default = None
        Path to the synthetic rating curve table
    inundation_raster_path : Optional[str], default=None
        Full path to output inundation raster (encoded by positive and negative HydroIDs).
    inundation_polygon_path : Optional[str], default=None
        Full path to output inundation polygon
    depths_raster_path : Optional[str], default = None
        Full path to output depths_raster. Pixel values will be in meters
    map_filename : Optional[str], default = None
        If not None saves the mapfiles to a csv file
    remove_intermediate : Optional[bool], default=True
        Option to keep intermediate files.
    verbose : Optional[bool], default=False
        Print verbose messages to screen. Not tested.
    is_mosaic_for_branches : Optional[Bool], default=False
        Whether the mosaic routine is for branches
    num_threads : Optional[int], default=1
        Number of threads to process
    precalb_option : Optional[bool], default=False
        Whether to use precalb discharge in hydrotable. If True, will use precalb_discharge_cms column
    windowed : Optional[bool], default=False
        Memory conscious creation of inundation and depth datasets
    nodata : Optional[int], default=elev_raster_ndv
        Nodata to pass to the mosaic_inundation function
    """

    # logging.debug(f"num_workers is {num_workers} and show_progress_bar is {show_progress_bar}")
    # Check that inundation_raster or depths_raster is supplied
    if (inundation_raster_path is None and inundation_raster_path == "") and (
        depths_raster_path is None and depths_raster_path == ""
    ):
        raise ValueError("Must supply either inundation_raster path or depths_raster path.")

    mosaic_file_path = ""

    try:
        # Check that output directory exists. Notify user that output directory will be created if not.
        for output_file_path in [inundation_raster_path, inundation_polygon_path, depths_raster_path]:
            if output_file_path is None:
                continue
            parent_dir = os.path.split(output_file_path)[0]
            if not os.path.exists(parent_dir):
                msg = f"Parent directory for {os.path.split(output_file_path)[1]} does not exist."
                "The parent directory will be produced."
                if verbose:
                    logging.info(msg)
                # logging.debug(msg)
                os.makedirs(parent_dir, exist_ok=True)
            # TODO: Jun 2026: Do we want to remove it to clean it?

        # Check that hydrofabric_dir exists
        if not s3_or_local_path_exists(hydrofabric_dir):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), hydrofabric_dir)

        # Check that huc folder exists in the hydrofabric_dir.
        if not s3_or_local_path_exists(os.path.join(hydrofabric_dir, huc)):
            raise FileNotFoundError(
                (errno.ENOENT, os.strerror(errno.ENOENT), os.path.join(hydrofabric_dir, huc))
            )

        # Check that flow file exists
        if not os.path.exists(flow_file_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file_path)

        # Jun 2026: Now that we are using threads, the cpu limits are no longer appliable
        # We mostly want to watch the network performance monitor to set a good value here
        # Check job numbers and raise error if necessary
        # total_cpus_available = os.cpu_count() - 1
        # if num_workers > total_cpus_available:
        #     raise ValueError(
        #         "The number of workers (-w), {}, "
        #         "exceeds your machine's available CPU count minus one ({}). "
        #         "Please lower the num_workers.".format(num_workers, total_cpus_available)
        #     )

        map_file_df = Inundate_gms(
            hydrofabric_dir=hydrofabric_dir,
            flow_file_path=flow_file_path,
            hydro_table_path=hydro_table_path,
            huc=huc,
            num_threads=num_threads,
            inundation_raster_path=inundation_raster_path,
            depths_raster=depths_raster_path,
            verbose=verbose,
            precalb_option=precalb_option,
            windowed=windowed
        )

        if map_file_df is None or len(map_file_df) == 0:
            raise Exception("Map file df came back Inundate_gms as None or Empty")

        # Write map file if designated (optional)
        if map_filename is not None and map_filename != "":
            os.makedirs(os.path.dirname(map_filename), exist_ok=True)
            logging.debug(f"Writing map file to {map_filename}")
            map_file_df.to_csv(map_filename, index=False)

        if verbose:
            logging.info(f"Mosaicking extent... for {flow_file_path}")
        else:
            logging.debug(f"Mosaicking extent... for {flow_file_path}")

        # TODO: Jun 2026: Does this really want depth_rasters first?
        for mosaic_attribute in ["depths_rasters", "inundation_rasters"]:
            mosaic_output = None
            if mosaic_attribute == "inundation_rasters":
                if inundation_raster_path is not None:
                    mosaic_output = inundation_raster_path
            elif mosaic_attribute == "depths_rasters":
                if depths_raster_path is not None:
                    mosaic_output = depths_raster_path

            if mosaic_output is not None:
                # Call Mosaic_inundation
                mosaic_file_path = Mosaic_inundation(
                    map_file_df.copy(),
                    mosaic_attribute=mosaic_attribute,
                    mosaic_output=mosaic_output,
                    nodata=nodata,
                    remove_inputs=remove_intermediate,
                    verbose=verbose,
                    is_mosaic_for_branches=is_mosaic_for_branches,
                    inundation_polygon=inundation_polygon_path
                )

        # Note: if a logging system has not been setup, default logging goes to screen
        if verbose:
            logging.info(f"Mosaicking extent complete. Saved to {mosaic_file_path}")
        else:
            logging.debug(f"Mosaicking extent complete. Saved to {mosaic_file_path}")

    except Exception as ex:
        logging.critical("++++++++++++++++++++++++++++++++++++++++++++++++")
        logging.critical(f"Error producing mosiacked inundation for {huc}")
        logging.critical(traceback.format_exc())
        raise ex

    return mosaic_file_path


if __name__ == "__main__":

    # TODO: Jun 2026:  Check if this still works
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Helpful utility to produce mosaicked inundation extents (raster and poly) and depths."
    )
    parser.add_argument(
        "-y",
        "--hydrofabric-dir",
        help="Directory path to FIM hydrofabric by processing unit.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-u", "--huc", help="REQUIRED: a valid huc id", required=True, default="", type=str, nargs="+"
    )
    parser.add_argument(
        "-f",
        "--flow-file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument(
        "-i",
        "--inundation-raster-path",
        help="Inundation raster output.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-p",
        "--inundation-polygon-path",
        help="Inundation polygon output. Only writes if designated.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-d",
        "--depths-raster-path",
        help="Depths raster output. Only writes if designated. Appends HUC code in batch mode.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-m",
        "--map-filename",
        help="Path to write output map file CSV (optional). Default is None.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-a",
        "--unit-attribute-name",
        help='Name of attribute column in map_file. Default is "huc8".',
        required=False,
        default="huc8",
        type=str,
    )
    parser.add_argument(
        "-w", "--num-workers", help="Number of worker threads.", required=False, default=1, type=int
    )
    parser.add_argument(
        "-r",
        "--remove-intermediate",
        help="Remove intermediate products, i.e. individual branch inundation.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-vr",
        "--verbose",
        help="Verbose printing. Not tested.",
        required=False,
        default=False,
        action="store_true",
    )

    start = timer()

    # Extract to dictionary and run
    produce_mosaicked_inundation(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
