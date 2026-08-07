import argparse
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import features


def catchment_boundary_errors(
    hydrofabric_dir, huc, inundation_file, output, inundation_type, min_error_length
):
    """
    This function compares inundation extent to catchment extents to identify catchment boundary
    issues. The output of this function is a geopackage of lines that identifys sections of inundation with catchment
    boundary issues present.

    Args:
        hydrofabric_dir (str):  Path to hydrofabric directory where FIM outputs were written by
                                fim_pipeline.
        huc (str):              The HUC for which to check for catchment boundary issues.
        inundation_file (str):  Full path to inundation file(s). Raster (encoded by positive and negative HydroIDs)
                                or vector.
        output (str):           Path to output location for catchment boundary geopackage.
        inundation_type (str):  Type of input inundation. Either 'tif' or 'gpkg'. Default tif.
        min_error_length (int): Minimum length for output error lines. Default 100 meters.
    """

    print(f'Processing HUC: {huc} now.')
    huc_st = timer()
    # Get branch names for input HUC
    dirs = [x[1] for x in os.walk(f"{hydrofabric_dir}/{huc}/branches")][0]

    # Merge all catchment geopackages into one file
    catchments = gpd.GeoDataFrame()
    for d in dirs:
        branch_catchments = gpd.read_parquet(
            f"{hydrofabric_dir}/{huc}/branches/{d}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_{d}.parquet",
            columns=["HydroID", "feature_id", "geometry"],
            engine="pyogrio",
        )
        branch_catchments = branch_catchments.assign(branch_id=d)
        catchments = pd.concat([catchments, branch_catchments], ignore_index=True)
    # print(f"Catchment Merge Completed in {round((timer() - huc_st)/60, 2)} minutes.")

    ## Read HUC Inundation
    # print("Reading inundation")
    if inundation_type == "tif":
        # Vectorize inundation
        # print("Inundation file: ", inundation_file)
        with rasterio.open(inundation_file) as src:
            affine = src.transform
            band = src.read(1)
            band = np.where(band > 0, 1, 0).astype("int16")
            results = (
                {'properties': {'raster_val': v}, 'geometry': s}
                for s, v in features.shapes(band, mask=(band == 1), transform=affine)
            )
        inund_poly = gpd.GeoDataFrame.from_features(results)

    if inundation_type == "gpkg":
        inund_poly = gpd.read_file(inundation_file, engine="pyogrio")
        # For Hydrovis Outputs
        # catchments = catchments.to_crs(crs="EPSG:3857")
        # inund_poly = gpd.read_file(
        #     inundation_file, columns=["hydro_id", "feature_id", "huc8", "geometry"], engine="pyogrio"
        # )
        # inund_poly = inund_poly.loc[inund_poly["huc8"] == int(huc)]
        inund_poly = inund_poly.dissolve()

    # Get boundary lines for inundation
    inundation_boundary = inund_poly.boundary
    inundation_boundary = inundation_boundary.set_crs(catchments.boundary.crs, allow_override=True)

    # Save boundary lines to geodataframe
    inundation_boundary_df = gpd.GeoDataFrame(geometry=inundation_boundary.normalize())
    catchment_boundary_df = catchments.assign(geometry=catchments.boundary.normalize())

    # Find intersecting lines
    # print("find intersections")
    intersect = gpd.overlay(
        catchment_boundary_df[["geometry"]],
        inundation_boundary_df[["geometry"]],
        how='intersection',
        keep_geom_type=False,
    )
    intersections = intersect.loc[intersect.geom_type.isin(['LineString', 'MultiLineString'])]

    # Check that there are intersections
    if len(intersections) < 1:
        print(f"No catchment boundary issues detected in HUC: {huc}")
    else:
        # print("process intersections")
        # Dissolve geometries into one large multilinestring
        intersections = intersections.dissolve()

        # Merge overlapping lines and explode to idividual linestrings
        intersections = intersections.assign(geometry=intersections.line_merge(directed=True))
        intersections_explode = intersections.explode(ignore_index=True, index_parts=False)

        # Drop line segments smaller than min_error_length
        error_lines_final = intersections_explode.loc[
            intersections_explode["geometry"].length >= min_error_length
        ]
        num_catchment_boundary_lines = len(error_lines_final)

        # Link errors to HydroID, feature_id, and branch_id
        spatial_join = gpd.sjoin(
            error_lines_final,
            catchment_boundary_df[["HydroID", "feature_id", "branch_id", "geometry"]],
            how="inner",
            predicate="intersects",
        ).reset_index()
        aggregate = (
            spatial_join[["index", "HydroID", "feature_id", "branch_id"]]
            .groupby(by="index")
            .agg(lambda x: list(set(x)))
        )
        error_lines_final = pd.merge(error_lines_final.reset_index(), aggregate, on="index", how="left").drop(
            columns="index"
        )

        print(
            f'Finished processing HUC: {huc}. Number of catchment boundary issues identified: {num_catchment_boundary_lines}.'
        )
        root, ext = os.path.splitext(output)
        output = f"{root}_{huc}.parquet"

        if os.path.exists(output):
            print(f"{output} already exists. Concatinating now...")
            existing_error_lines = gpd.read_parquet(output)
            error_lines_final = pd.concat([existing_error_lines, error_lines_final])
        error_lines_final.to_file(output, index=False)
        print(f"HUC: {huc} Completed in {round((timer() - huc_st)/60, 2)} minutes.")


def multi_process_catchment_boundaries(
    hydrofabric_dir, hucs, inundation_dir, output, inundation_type, min_error_length, job_number
):
    """
    Function for multiprocessing HUCs to identify catchment boundaries for a larger HUC subset efficiently.

    Args:
        hydrofabric_dir (str):  Path to hydrofabric directory where FIM outputs were written by
                                fim_pipeline.
        huc (str):              The HUC for which to check for catchment boundary issues.
        inundation_file (str):  Full path to directory contatining inundation file(s). Raster (encoded by positive
                                and negative HydroIDs) or vector. HUC number must be in file name.
        output (str):           Path to output location for catchment boundary geopackage.
        inundation_type (str):  Type of input inundation. Either 'tif' or 'gpkg'. Default tif.
        min_error_length (int): Minimum length for output error lines. Default 100 meters.
        job_number (int):       Number of concurrent jobs to use.

    Example Usage:
    python /foss_fim/tools/identify_catchment_boundary.py -y /data/previous_fim/hand_4_8_7_2/
    -u 12090109 12090110 12090203 -i /user/Documents/inundation_dir/
    -t tif -o /user/Documents/test_catch_bound/output_catchment_boundary_issue_file.gpkg -jh 5

    """
    with ProcessPoolExecutor(max_workers=job_number) as executor:
        executor_dict = {}
        if not os.path.exists(os.path.dirname(output)):
            print(f"Output directory {os.path.dirname(output)} does not exist. Making directory now...")
            os.makedirs(os.path.dirname(output))
        inundation_files = os.listdir(f"{inundation_dir}")
        if os.path.exists(str(hucs[0])) == True:
            huc_list = pd.read_csv(hucs[0], dtype=str, header=None)
            hucs = list(huc_list[0].unique())
        for huc in hucs:
            file = [x for x in inundation_files if huc in x and x.endswith(f".{inundation_type}")][0]
            inundation_file = f"{inundation_dir}/{file}"
            catchment_boundary_args = {
                "hydrofabric_dir": hydrofabric_dir,
                "huc": huc,
                "inundation_file": inundation_file,
                "inundation_type": inundation_type,
                "output": output,
                "min_error_length": min_error_length,
            }
            try:
                future = executor.submit(catchment_boundary_errors, **catchment_boundary_args)
                executor_dict[future] = huc
            except Exception as ex:
                print(f"*** {ex}")
                traceback.print_exc()
                sys.exit(1)


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Helpful utility to identify catchment boundary errors.")

    parser.add_argument(
        "-y",
        "--hydrofabric_dir",
        help="Directory path to FIM hydrofabric by processing unit.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-u",
        "--hucs",
        help="List of HUCs to run, space delimited or path to .csv file.",
        required=True,
        default="",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "-i",
        "--inundation_dir",
        help="Path to directory containing an inundation file for each HUC run. File name must include the HUC number",
        required=True,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Path to output geopackage location and base filename.",
        required=True,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-t",
        "--inundation_type",
        help="Inundation file type, either tif or gpkg",
        required=False,
        default="tif",
        type=str,
    )
    parser.add_argument(
        '-min',
        '--min_error_length',
        help='Minimum length for output error lines. Default is 100 meters.',
        required=False,
        type=int,
        default=100,
    )
    parser.add_argument(
        '-jh',
        '--job_number',
        help='Number of jobs to use to process input HUCs.',
        required=False,
        type=int,
        default=1,
    )
    start = timer()

    # catchment_boundary_errors(**vars(parser.parse_args()))
    multi_process_catchment_boundaries(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
