import argparse
import geopandas as gpd
import os
import numpy as np
import pandas as pd
import rasterio
import sys
import traceback

from timeit import default_timer as timer
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
from rasterio import features
from shapely.geometry import shape

def catchment_boundary_errors(    
    hydrofabric_dir,
    hucs,
    inundation_raster,
    output,
    number_of_jobs
):
    """
    This function compares output inundation raster extent to catchment extents to identify catchment boundary
    issues. The output of this function is a shapefile of lines that identify areas of inundation with catchment 
    boundary issues present.

    Args:
        hydrofabric_dir (str):    Path to hydrofabric directory where FIM outputs were written by
                                    fim_pipeline.
        hucs (str):                The HUC for which to check for catchment boundary issues.
        inundation_raster (str):  Full path to output inundation raster
                                    (encoded by positive and negative HydroIDs).
        output (str):             Path to output location for catchment boundary geopackage.
        number_of_jobs (int):     Number of parallel jobs to run.
                                   
    """
    
    # Validation
    total_cpus_available = os.cpu_count() - 1
    if number_of_jobs > total_cpus_available:
        raise ValueError(
            f'The number of jobs provided: {number_of_jobs} ,'
            ' exceeds your machine\'s available CPU count minus one.'
            ' Please lower the number of jobs'
            ' value accordingly.'
        )
    intersect_lines_final = pd.DataFrame()

    with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
        executor_dict = {}

        for huc in hucs:
            huc_feature_args = {
                'hydrofabric_dir': hydrofabric_dir,
                'huc': huc,
                'inundation_raster': inundation_raster,
                'output': output,
            }

            try:
                future = executor.submit(identify_per_huc, **huc_feature_args)
                executor_dict[future] = huc
            except Exception as ex:
                summary = traceback.StackSummary.extract(traceback.walk_stack(None))
                print(f"*** {ex}")
                print(''.join(summary.format()))

                sys.exit(1)

            for future_result in futures.as_completed(executor_dict):
                if future_result is not None:
                    boundary_line_df = future_result.result()
                    if boundary_line_df is None:
                        print('Process failed.')
                    else:
                        intersect_lines_final = pd.concat([intersect_lines_final, boundary_line_df])
    # Export final file                   
    intersect_lines_final.to_file(output, index=False)

    # for huc in hucs:
def identify_per_huc(hydrofabric_dir, huc, inundation_raster, output):
    print(f'Processing HUC:{huc} now.')
    # Get branch names for input HUC
    dirs = [x[1] for x in os.walk(f"{hydrofabric_dir}/{huc}/branches")] [0]

    # merge all catchment geopackages into one file
    catchments = gpd.GeoDataFrame()
    for d in dirs:
        catch = gpd.read_file(f"{hydrofabric_dir}/{huc}/branches/{d}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_{d}.gpkg")
        catchments = pd.concat([catchments, catch], ignore_index = True)

    # Vectorize inundation 
    with rasterio.open(inundation_raster) as src:
        affine = src.transform
        band = src.read(1)
        band[np.where(band <= 0)] = 0
        band[np.where(band > 0)] = 1
        results = (
        {'properties': {'raster_val': v}, 'geometry': s}
        for i, (s, v) 
        in enumerate(
            features.shapes(band, mask=None, transform=affine)))
    
    # Save features to geodataframe and select inundated pixels
    geoms = list(results)
    inund_poly = gpd.GeoDataFrame.from_features(geoms)
    inund_poly = inund_poly.loc[inund_poly['raster_val'] == 1.0]

    # Get boundary lines for catchments and inundation
    catchment_boundary = catchments.boundary
    inundation_boundary = inund_poly.boundary
    inundation_boundary = inundation_boundary.set_crs(catchment_boundary.crs)

    # Save boundary lines to geodataframe
    inundation_boundary_df = gpd.GeoDataFrame(geometry = inundation_boundary)
    catchment_boundary_df = gpd.GeoDataFrame(geometry = catchment_boundary)

    # Dissolve geomtries into one multilinestring
    catchment_boundary_dissolve = catchment_boundary_df.dissolve()
    inundation_boundary_dissolve = inundation_boundary_df.dissolve()

    # Find intersection of inundation and catchments
    intersect = gpd.GeoDataFrame()
    intersect = gpd.GeoDataFrame(geometry = inundation_boundary_dissolve.intersection(catchment_boundary_dissolve))
        
    intersect_exp = intersect.explode(ignore_index = True)
    intersect_lines = intersect_exp.loc[intersect_exp.geom_type=='LineString']
    num_catchment_boundary_lines = len(intersect_lines)

    print(f'Finished processing huc: {huc}. Number of boundary line issues identified: {num_catchment_boundary_lines}.')

    return intersect_lines

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
        "-u", "--hucs", help="List of HUCs to run", required=True, default="", type=str, nargs="+"
    )
    parser.add_argument(
        "-i", "--inundation-raster", help="Inundation raster output.", required=True, default=None, type=str
    )
    parser.add_argument(
        "-o", "--output", help="Output geopackage location.", required=True, default=None, type=str
    )
    parser.add_argument(
        '-j',
        '--number_of_jobs',
        help='OPTIONAL: number of cores/processes (default=4). This is a memory intensive '
        'script, and the multiprocessing will crash if too many CPUs are used. It is recommended to provide '
        'half the amount of available CPUs.',
        type=int,
        required=False,
        default=4,
    )

    start = timer()

    catchment_boundary_errors(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
