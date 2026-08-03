import argparse
import errno
import logging
import os

# import shutil
from timeit import default_timer as timer
from typing import List, Optional, Union

import pandas as pd
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import s3_or_local_path_exists
from src.utils.shared_variables import elev_raster_ndv


# This function is for inundation rasters and not depth rasters. interpolate_water_surface.py is the only
# tool that uses depth rasters and calls directly to Inundate_gms
# Aug 2026: masking system commented out. See notes at mosiac_iundation.py -> mask_mosiac function
def produce_mosaicked_inundation(
    hydrofabric_dir: str,
    hucs: Union[str, List[str]],
    flow_file_path: str,
    # This name is used and adjusted to make different huc / branch rasters, and file name
    # is the final mosaicked file name. It is also used in inundation as a base file name
    # appended to add branch ids, etc to create the inundation mp df for the final mosaic
    output_raster_path: str,  # The final mosaicked output raster file path
    hydro_table_path: Optional[str] = None,
    # inundation_polygon: Optional[str] = None,  # July 2026: Not in use. The only thing using a depth raster
    # is interpolate_water_surface.py and that jumps in a inundate_gms and not here.
    # depths_raster_path: Optional[str] = None,
    # map_filename: Optional[str] = None
    # will open the option to save the dataframe of huc8, branchs and raster paths in case
    # something wants it later. Renamed from map_filename to inundation_mapping_file_path
    # is actually saved in inundate_gms.py. The orig_mapfile was saving the same dataframe results.
    # I put it in inundate_gms.py as not all scripts come through here.
    inundation_mapping_file_path: Optional[str] = None,
    #     Aug 2026: masking system commented out. See notes at mosiac_iundation.py -> mask_mosiac function
    #     mask_path: Optional[str] = None,
    # unit_attribute_name: Optional[str] = "huc8",  # all scripts used the value of huc8
    # There are only threads from here downstream. MP no longer available. It is the user's responsility if they
    # use MP before getting here (not all do) to ensure over usign system resources.
    # Note: You will find that you can use more threads then cpu's, so the limit does not apply.
    # num_workers: Optional[int] = 1,
    # Aug 2026: Nothing is using it but keep it for now in case someone wants it
    # for debugging
    remove_intermediate: Optional[bool] = True,
    verbose: Optional[bool] = False,
    is_mosaic_for_branches: Optional[bool] = False,
    num_threads: Optional[int] = 1,
    precalb_option: Optional[bool] = False,
    windowed: Optional[bool] = False,
    # log_file: Optional[str] = None,  # each calling script should have its own logging now
    nodata: Optional[int] = elev_raster_ndv,
    # gms_multi_process: Optional[bool] = False,
):
    """
        # Jun 2026: Many of the args above either do not apply anymore, where never used or errored based on
        # certain conditions.
        # Note: As before, Flow files must have feature_id, and discharge column.
        # hydrotable paths optionally be added, but if not provided, the system will use the huc list and its
        # branch hydrotables. If you do submit your own hydrotable, ensure it has a "HUC" column along with
        # other standard fields used for inundation.

        # For log_file, all code from here down stream will use standard python.logging calls. If a logger
        # has been set up, it will honor it. If not, it will just go to screen.

        # Old notes Pre June 2026
        #       This function calls Inundate_gms and Mosaic_inundation to produce inundation maps.
        #       Possible outputs include inundation rasters encoded by HydroID (negative HydroID for dry and positive
        #       HydroID for wet), polygons depicting extent, and depth rasters. The function requires a flow file
        #       organized by NWM feature_id and discharge in cms. "feature_id" and "discharge" columns MUST be present
        #       in the flow file.

        # NOTE: Aug 1, 2026: Consider dropping the depth raster path in favour of just inundation path.
        # downstream, the only difference is that the nodata becomes 0 when true inundation, but no data
        # but it is overridden in inundation.py anyways (see nodata = np.int16... (appx line 207))

        Parameters
        ----------
        hydrofabric_dir : str
            Path to hydrofabric directory where FIM outputs were written by fim_pipeline
        hucs : str
            The HUC for which to produce mosaicked inundation files.
        flow_file_path : str
            Path to flow file to be used for inundation. Feature_ids in flow_file should be present in supplied HUC.
        hydro_table_path : Optional[str], default = None
            Path to the synthetic rating curve table
        output_raster_path : Optional[str], default=None
            Full path to output inundation raster (encoded by positive and negative HydroIDs).
    #     inundation_polygon : Optional[str], default=None
    #        Full path to output inundation polygon
    #   depths_raster_path : Optional[str], default = None
    #         Full path to output depths_raster. Pixel values will be in meters
         inundation_mapping_file_path : Optional[str], default = None
            If not None saves the df of inundation and raster files created to a csv file
    #     mask_path : Optional[str], default = None
    #        The inclusive mask for the final mosaicked datasets
    #    unit_attribute_name : Optional[str], default="huc8"
    #        The name of the processing unit
    #    num_workers : Optional[int]:
    #        Number of parallel processes to run.
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
    #    log_file : Optional[str], default=None
    #        File path for log file
        nodata : Optional[int], default=elev_raster_ndv
            Nodata to pass to the mosaic_inundation function
    #    gms_multi_process : Optional[bool], default=False
    #        Use processes for parallel processing instead of threads
    """

    print("++++++++++++++++++++++++++++++++++++++++++++++++")
    logging.info("Starting produce_mosaicked_inundation")
    # print(locals())

    # Check that inundation_raster or depths_raster is supplied
    if not output_raster_path:
        raise ValueError("Must supply an output nundation_raster file path")

    # Pre-create the folder paths that the inundation raster will need
    raster_path_dir = os.path.dirname(output_raster_path)
    os.makedirs(raster_path_dir, exist_ok=True)

    # Check that hydrofabric_dir exists -- July 2026: Does S3 even work? It is not being used anywhere
    if not s3_or_local_path_exists(hydrofabric_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), hydrofabric_dir)

    # If the "hucs" argument is really one huc, convert it to a list
    if type(hucs) is str:
        hucs = [hucs]

    # Check that huc folder exists in the hydrofabric_dir.
    for huc in hucs:
        if not s3_or_local_path_exists(os.path.join(hydrofabric_dir, huc)):
            raise FileNotFoundError(
                (errno.ENOENT, os.strerror(errno.ENOENT), os.path.join(hydrofabric_dir, huc))
            )

    # Check that flow file exists
    if not isinstance(flow_file_path, pd.DataFrame) and not os.path.exists(flow_file_path):
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

    # Call Inundate_gms
    # inundation_raster_path is used as a base file name for processing per branch
    raster_paths_df = Inundate_gms(
        hydrofabric_dir=hydrofabric_dir,
        forecast_file_path=flow_file_path,
        hydro_table_path=hydro_table_path,
        num_threads=num_threads,
        hucs=hucs,
        inundation_raster_path=output_raster_path,
        # depth_rasters - Not used any a scripts from here, only used via apps directly calling inundate_gms.py
        # depths_raster_path=depths_raster,
        verbose=verbose,
        precalb_option=precalb_option,
        windowed=windowed,
        # log_file=log_file,
        # multi_process=gms_multi_process,
        inundation_results_file_path=inundation_mapping_file_path,
    )

    # Write map file if designated (resolved in inundate_gms (was duplicate))
    # if map_filename is not None:
    #     if not os.path.isdir(os.path.dirname(map_filename)):
    #         os.makedirs(os.path.dirname(map_filename))

    #     map_file.to_csv(map_filename, index=False)

    logging.info(f"Mosaicking extent... - [{hucs}]")

    logging.debug(raster_paths_df)

    #     raise Exception(f"Aborting hucs = {hucs} -  just before mosiac attempt")

    # Call Mosaic_inundation
    # All tools passed in either the entire large wbd.gpkg or the value of None
    # so masking had no value and is being disabled. Should speed it up not to
    # have to keep loading the wbd.gpkg

    # Aug, 2026: This always became the value of "inundation_rasters" in all scenerios
    # commented it out
    # for mosaic_attribute in ["depths_rasters", "inundation_rasters"]:
    #     mosaic_output = None
    #     if mosaic_attribute == "inundation_rasters":
    #         if inundation_raster is not None:
    #             mosaic_output = inundation_raster
    #     elif mosaic_attribute == "depths_rasters":
    #         if depths_raster is not None:
    #             mosaic_output = depths_raster

    #     if mosaic_output is not None:

    mosaic_file_path = Mosaic_inundation(
        raster_paths_df.copy(),
        output_mosaic_path=output_raster_path,
        mosaic_attribute="inundation_rasters",  # has to be inundation_rasters for this code path
        # mask_path=mask_path,
        unit_attribute_name="huc8",  # has to always be huc8 for this code pathing
        nodata=nodata,
        remove_inputs=remove_intermediate,
        verbose=verbose,
        is_mosaic_for_branches=is_mosaic_for_branches,
        # inundation_polygon=inundation_polygon,
        num_threads=num_threads,  # likely broke as part of the write_window error
    )

    # fh.vprint("Mosaicking complete.", verbose)
    if verbose:
        logging.info("Mosaicking complete.")
    else:
        logging.debug("Mosaicking complete.")

    return mosaic_file_path


if __name__ == "__main__":

    # July 2026: This does not appear to have worked for quite a while, but not sure.
    # Removed some args that are no longer valid.

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
        "-u", "--hucs", help="List of HUCS to run", required=True, default="", type=str, nargs="+"
    )
    parser.add_argument(
        "-f",
        "--flow-file-path",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument(
        "-i",
        "--output-inundation-raster-path",
        help="Inundation raster output.",
        required=False,
        default=None,
        type=str,
    )
    # parser.add_argument(
    #     "-p",
    #     "--inundation-polygon",
    #     help="Inundation polygon output. Only writes if designated.",
    #     required=False,
    #     default=None,
    #     type=str,
    # )
    # parser.add_argument(
    #     "-d",
    #     "--depths-raster",
    #     help="Depths raster output. Only writes if designated. Appends HUC code in batch mode.",
    #     required=False,
    #     default=None,
    #     type=str,
    # )
    # parser.add_argument(
    #     "-m",
    #     "--map-filename",
    #     help="Path to write output map file CSV (optional). Default is None.",
    #     required=False,
    #     default=None,
    #     type=str,
    # )
    parser.add_argument(
        "-k", "--mask-path", help="Name of mask file.", required=False, default=None, type=str
    )
    # parser.add_argument(
    #     "-a",
    #     "--unit_attribute_name",
    #     help='Name of attribute column in map_file. Default is "huc8".',
    #     required=False,
    #     default="huc8",
    #     type=str,
    # )
    # parser.add_argument("-w", "--num-workers", help="Number of workers.", required=False, default=1, type=int)
    parser.add_argument(
        "-r",
        "--remove-intermediate",
        help="Remove intermediate products, i.e. individual branch inundation.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-v",
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
