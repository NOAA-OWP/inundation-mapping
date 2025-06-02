import argparse
import errno
import os
from timeit import default_timer as timer
from typing import List, Optional, Union

import pandas as pd
from inundate_gms import Inundate_gms
from mosaic_inundation import Mosaic_inundation

from utils.shared_functions import FIM_Helpers as fh
from utils.shared_variables import elev_raster_ndv


def produce_mosaicked_inundation(
    hydrofabric_dir: str,
    hucs: Union[str, List[str]],
    flow_file: str,
    hydro_table_df: Optional[str] = None,
    inundation_raster: Optional[str] = None,
    inundation_polygon: Optional[str] = None,
    depths_raster: Optional[str] = None,
    map_filename: Optional[str] = None,
    mask: Optional[str] = None,
    unit_attribute_name: Optional[str] = "huc8",
    num_workers: Optional[int] = 1,
    remove_intermediate: Optional[bool] = True,
    verbose: Optional[bool] = False,
    is_mosaic_for_branches: Optional[bool] = False,
    num_threads: Optional[int] = 1,
    windowed: Optional[bool] = False,
    log_file: Optional[str] = None,
    nodata: Optional[int] = elev_raster_ndv,
):
    """
    This function calls Inundate_gms and Mosaic_inundation to produce inundation maps.
    Possible outputs include inundation rasters encoded by HydroID (negative HydroID for dry and positive
    HydroID for wet), polygons depicting extent, and depth rasters. The function requires a flow file
    organized by NWM feature_id and discharge in cms. "feature_id" and "discharge" columns MUST be present
    in the flow file.

    Parameters
    ----------
    hydrofabric_dir : str
        Path to hydrofabric directory where FIM outputs were written by fim_pipeline
    hucs : str
        The HUC for which to produce mosaicked inundation files.
    flow_file : str
        Path to flow file to be used for inundation. Feature_ids in flow_file should be present in supplied HUC.
    hydro_table_df : Optional[str], default = None
        Path to the synthetic rating curve table
    inundation_raster : Optional[str], default=None
        Full path to output inundation raster (encoded by positive and negative HydroIDs).
    inundation_polygon : Optional[str], default=None
        Full path to output inundation polygon
    depths_raster : Optional[str], default = None
        Full path to output depths_raster. Pixel values will be in meters
    map_filename : Optional[str], default = None
        If not None saves the mapfiles to a csv file
    mask : Optional[str], default = None
        The inclusive mask for the final mosaicked datasets
    unit_attribute_name : Optional[str], default="huc8"
        The name of the processing unit
    num_workers : Optional[int]:
        Number of parallel processes to run.
    remove_intermediate : Optional[bool], default=True
        Option to keep intermediate files.
    verbose : Optional[bool], default=False
        Print verbose messages to screen. Not tested.
    is_mosaic_for_branches : Optional[Bool], default=False
        Whether the mosaic routine is for branches
    num_threads : Optional[int], default=1
        Number of threads to process
    windowed : Optional[bool], default=False
        Memory conscious creation of inundation and depth datasets
    log_file : Optional[str], default=None
        File path for log file
    nodata : Optional[int], default=elev_raster_ndv
        Nodata to pass to the mosaic_inundation function
    """

    # Check that inundation_raster or depths_raster is supplied
    if inundation_raster is None and depths_raster is None:
        raise ValueError("Must supply either inundation_raster or depths_raster.")

    # Check that output directory exists. Notify user that output directory will be created if not.
    for output_file in [inundation_raster, inundation_polygon, depths_raster]:
        if output_file is None:
            continue
        parent_dir = os.path.split(output_file)[0]
        if not os.path.exists(parent_dir):
            fh.vprint(
                "Parent directory for "
                + os.path.split(output_file)[1]
                + " does not exist. The parent directory will be produced.",
                verbose,
            )
            os.makedirs(parent_dir)

    # Check that hydrofabric_dir exists
    if not os.path.exists(hydrofabric_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), hydrofabric_dir)

    # If the "hucs" argument is really one huc, convert it to a list
    if type(hucs) is str:
        hucs = [hucs]

    # Check that huc folder exists in the hydrofabric_dir.
    for huc in hucs:
        if not os.path.exists(os.path.join(hydrofabric_dir, huc)):
            raise FileNotFoundError(
                (errno.ENOENT, os.strerror(errno.ENOENT), os.path.join(hydrofabric_dir, huc))
            )

    # Check that flow file exists
    if not isinstance(flow_file, pd.DataFrame) and not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    # Check job numbers and raise error if necessary
    total_cpus_available = os.cpu_count() - 1
    if num_workers > total_cpus_available:
        raise ValueError(
            "The number of workers (-w), {}, "
            "exceeds your machine's available CPU count minus one ({}). "
            "Please lower the num_workers.".format(num_workers, total_cpus_available)
        )

    # Call Inundate_gms
    map_file = Inundate_gms(
        hydrofabric_dir=hydrofabric_dir,
        forecast=flow_file,
        hydro_table_df=hydro_table_df,
        num_workers=num_threads,
        hucs=hucs,
        inundation_raster=inundation_raster,
        depths_raster=depths_raster,
        verbose=verbose,
        windowed=windowed,
        log_file=log_file,
    )

    # Write map file if designated
    if map_filename is not None:
        if not os.path.isdir(os.path.dirname(map_filename)):
            os.makedirs(os.path.dirname(map_filename))

        map_file.to_csv(map_filename, index=False)

    fh.vprint("Mosaicking extent...", verbose)

    for mosaic_attribute in ["depths_rasters", "inundation_rasters"]:
        mosaic_output = None
        if mosaic_attribute == "inundation_rasters":
            if inundation_raster is not None:
                mosaic_output = inundation_raster
        elif mosaic_attribute == "depths_rasters":
            if depths_raster is not None:
                mosaic_output = depths_raster

        if mosaic_output is not None:

            # Call Mosaic_inundation
            mosaic_file_path = Mosaic_inundation(
                map_file.copy(),
                mosaic_attribute=mosaic_attribute,
                mosaic_output=mosaic_output,
                mask=mask,
                unit_attribute_name=unit_attribute_name,
                nodata=nodata,
                remove_inputs=remove_intermediate,
                verbose=verbose,
                is_mosaic_for_branches=is_mosaic_for_branches,
                inundation_polygon=inundation_polygon,
                workers=num_threads,
            )

    fh.vprint("Mosaicking complete.", verbose)

    return mosaic_file_path


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Helpful utility to produce mosaicked inundation extents (raster and poly) and depths."
    )
    parser.add_argument(
        "-y",
        "--hydrofabric_dir",
        help="Directory path to FIM hydrofabric by processing unit.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-u", "--hucs", help="List of HUCS to run", required=True, default="", type=str, nargs="+"
    )
    parser.add_argument(
        "-f",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
        type=str,
    )
    parser.add_argument(
        "-i", "--inundation-raster", help="Inundation raster output.", required=False, default=None, type=str
    )
    parser.add_argument(
        "-p",
        "--inundation-polygon",
        help="Inundation polygon output. Only writes if designated.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-d",
        "--depths-raster",
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
    parser.add_argument("-k", "--mask", help="Name of mask file.", required=False, default=None, type=str)
    parser.add_argument(
        "-a",
        "--unit_attribute_name",
        help='Name of attribute column in map_file. Default is "huc8".',
        required=False,
        default="huc8",
        type=str,
    )
    parser.add_argument("-w", "--num-workers", help="Number of workers.", required=False, default=1, type=int)
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
