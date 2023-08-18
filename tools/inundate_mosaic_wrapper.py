import os, sys
import argparse
import errno
import rasterio
from rasterio.features import shapes
import geopandas as gpd
import numpy as np
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from timeit import default_timer as timer

sys.path.append("/foss_fim/tools")
from mosaic_inundation import Mosaic_inundation, mosaic_final_inundation_extent_to_poly
from inundate_gms import Inundate_gms


def inundate_and_mosaic(
    hydrofabric_dir,
    huc,
    flow_file,
    inundation_raster,
    depths_raster,
    log_file,
    output_fileNames,
    num_workers,
    remove_intermediate,
    verbose,
    mosaic_attribute,
    huc_dir,
):
    """
    Helper function. Refer to produce_mosaicked_inundation for arg descriptions.
    """

    map_file = Inundate_gms(
        hydrofabric_dir=hydrofabric_dir,
        forecast=flow_file,
        num_workers=num_workers,
        hucs=huc,
        inundation_raster=inundation_raster,
        inundation_polygon=None,
        depths_raster=depths_raster,
        verbose=verbose,
        log_file=None,
        output_fileNames=None,
    )

    print("Mosaicking extent for " + huc + "...")
    # Call Mosaic_inundation
    Mosaic_inundation(
        map_file,
        mosaic_attribute=mosaic_attribute,
        mosaic_output=inundation_raster if depths_raster == None else depths_raster,
        mask=os.path.join(huc_dir, "wbd.gpkg"),
        unit_attribute_name="huc8",
        nodata=-9999,
        workers=1,
        remove_inputs=remove_intermediate,
        subset=None,
        verbose=verbose,
    )


def produce_mosaicked_inundation(
    hydrofabric_dir,
    huc,
    flow_file,
    inundation_raster,
    inundation_polygon,
    depths_raster,
    log_file,
    output_fileNames,
    num_workers,
    remove_intermediate,
    verbose,
):
    """
    This function calls Inundate_gms and Mosaic_inundation to produce inundation maps. Possible outputs include inundation rasters
    encoded by HydroID (negative HydroID for dry and positive HydroID for wet), polygons depicting extent, and depth rasters. The
    function requires a flow file organized by NWM feature_id and discharge in cms. "feature_id" and "discharge" columns MUST be
    present in the flow file.

    Args:
        hydrofabric_dir (str): Directory path to hydrofabric directory where FIM outputs were written by fim_pipeline.
        huc (str): The HUC for which to produce mosaicked inundation files.
        flow_file (str): Directory path to flow file to be used for inundation. feature_ids in flow_file should be present in supplied HUC.
        inundation_raster (str): Full path to output inundation raster (encoded by positive and negative HydroIDs).
        inuntation_polygon (str): Full path to output inundation polygon. Optional.
        depths_raster (str): Full path to output depths_raster. Pixel values will be in meters. Optional.
        log_file (str): Full path to log file to write logs to. Optional. Not tested.
        output_fileNames (str): Full path to CSV containing paths to output file names. Optional. Not tested.
        num_workers (int): Number of parallel jobs to run.
        keep_intermediate (bool): Option to keep intermediate files. Not tested.
        verbose (bool): Print verbose messages to screen. Not tested.

    """

    # Check that output directory exists. Notify user that output directory will be created if not.
    for output_file in [inundation_raster, inundation_polygon, depths_raster]:
        if output_file == None:
            continue
        parent_dir = os.path.split(output_file)[0]
        if not os.path.exists(parent_dir):
            print(
                "Parent directory for "
                + os.path.split(output_file)[1]
                + " does not exist. The parent directory will be produced."
            )
            os.makedirs(parent_dir)

    # Check that hydrofabric_dir exists
    if not os.path.exists(hydrofabric_dir):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), hydrofabric_dir
        )

    # Check that huc folder exists in the hydrofabric_dir.
    if not os.path.exists(os.path.join(hydrofabric_dir, huc)):
        raise FileNotFoundError(
            (
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                os.path.join(hydrofabric_dir, huc),
            )
        )

    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    # Check job numbers and raise error if necessary
    total_cpus_available = os.cpu_count() - 1
    if num_workers > total_cpus_available:
        raise ValueError(
            "The number of workers (-w), {}, "
            "exceeds your machine's available CPU count minus one ({}). "
            "Please lower the num_workers.".format(num_workers, total_cpus_available)
        )

    huc_dir = os.path.join(hydrofabric_dir, huc)
    print("Running inundate for " + huc + "...")
    # Call Inundate_gms
    fn_inundation_raster, fn_depths_raster, mosaic_attribute = (
        inundation_raster,
        None,
        "inundation_rasters",
    )

    inundate_and_mosaic(
        hydrofabric_dir,
        huc,
        flow_file,
        fn_inundation_raster,
        fn_depths_raster,
        log_file,
        output_fileNames,
        num_workers,
        remove_intermediate,
        verbose,
        mosaic_attribute,
        huc_dir,
    )

    # Produce depths if instructed
    if depths_raster != None:
        print("Computing depths for " + huc + "...")
        fn_inundation_raster, fn_depths_raster, mosaic_attribute = (
            None,
            depths_raster,
            "depths_rasters",
        )

        inundate_and_mosaic(
            hydrofabric_dir,
            huc,
            flow_file,
            fn_inundation_raster,
            fn_depths_raster,
            log_file,
            output_fileNames,
            num_workers,
            remove_intermediate,
            verbose,
            mosaic_attribute,
            huc_dir,
        )
    else:
        pass

    if inundation_polygon != None:
        mosaic_final_inundation_extent_to_poly(inundation_raster, inundation_polygon)

    print("Mosaicking complete.")


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
    )
    parser.add_argument(
        "-u",
        "--huc",
        help="List of HUCS to run",
        required=True,
        default="",
        type=str,
    )
    parser.add_argument(
        "-f",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
    )
    parser.add_argument(
        "-i",
        "--inundation-raster",
        help="Inundation raster output.",
        required=True,
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
        "-l",
        "--log-file",
        help="Log-file to store level-path exceptions. Not tested.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-o",
        "--output-fileNames",
        help="Output CSV file with filenames for inundation rasters, inundation polygons, and depth rasters. Not tested.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-w",
        "--num-workers",
        help="Number of workers.",
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        "-r",
        "--remove-intermediate",
        help="Keep intermediate products, i.e. individual branch inundation.",
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
