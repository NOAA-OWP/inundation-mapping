import argparse
import os
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import features
from shapely import geometry, ops
from shapely.geometry import shape


def catchment_boundary_errors(hydrofabric_dir, huc, inundation_raster, output, min_error_length):
    """
    This function compares output inundation raster extent to catchment extents to identify catchment boundary
    issues. The output of this function is a geopackage of lines that identifys sections of inundation with catchment
    boundary issues present.

    Args:
        hydrofabric_dir (str):    Path to hydrofabric directory where FIM outputs were written by
                                    fim_pipeline.
        huc (str):                The HUC for which to check for catchment boundary issues.
        inundation_raster (str):  Full path to inundation raster
                                    (encoded by positive and negative HydroIDs).
        output (str):             Path to output location for catchment boundary geopackage.
        min_error_length (int):   Minimum length for output error lines. Default 100 meters.
    """

    huc = huc[0]
    print(f'Processing HUC:{huc} now.')
    # Get branch names for input HUC
    dirs = [x[1] for x in os.walk(f"{hydrofabric_dir}/{huc}/branches")][0]

    # Merge all catchment geopackages into one file
    catchments = gpd.GeoDataFrame()
    for d in dirs:
        catch = gpd.read_file(
            f"{hydrofabric_dir}/{huc}/branches/{d}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_{d}.gpkg"
        )
        catch = catch.assign(branch_id=d)
        catchments = pd.concat([catchments, catch], ignore_index=True)

    # Vectorize inundation
    with rasterio.open(inundation_raster) as src:
        affine = src.transform
        band = src.read(1)
        band[np.where(band <= 0)] = 0
        band[np.where(band > 0)] = 1
        results = (
            {'properties': {'raster_val': v}, 'geometry': s}
            for i, (s, v) in enumerate(features.shapes(band, mask=None, transform=affine))
        )

    # Save features to geodataframe and select inundated pixels
    geoms = list(results)
    inund_poly = gpd.GeoDataFrame.from_features(geoms)
    inund_poly = inund_poly.loc[inund_poly['raster_val'] == 1.0]

    # Get boundary lines for inundation
    inundation_boundary = inund_poly.boundary
    inundation_boundary = inundation_boundary.set_crs(catchments.boundary.crs)

    # Save boundary lines to geodataframe
    inundation_boundary_df = gpd.GeoDataFrame(geometry=inundation_boundary)
    catchment_boundary_df = catchments.assign(geometry=catchments.boundary)

    # Explode geomtries into many individual linestrings
    catchment_boundary_explode = catchment_boundary_df.explode(ignore_index=True)
    inundation_boundary_explode = inundation_boundary_df.explode(ignore_index=True)

    # Find where catchment boundary and inundation boundary intersect (catchment boundary errors)
    intersect = inundation_boundary_explode.overlay(
        catchment_boundary_explode, how='intersection', keep_geom_type=True
    )
    error_lines = gpd.GeoDataFrame()
    for i in intersect['branch_id'].unique():
        branch_df = intersect.loc[intersect['branch_id'] == f'{i}']
        branch_df = branch_df.explode(index_parts=True)
        branch_df = branch_df.dissolve(by=['HydroID', 'feature_id'], as_index=False)

        for index, row in branch_df.iterrows():
            dissolved_lines = branch_df.iloc[index, 2]
            if isinstance(dissolved_lines, geometry.multilinestring.MultiLineString):
                merged_lines = ops.linemerge(dissolved_lines)
                branch_df.loc[branch_df['geometry'] == dissolved_lines, 'geometry'] = merged_lines
        error_lines = pd.concat([error_lines, branch_df])
    error_lines_explode = error_lines.explode(index_parts=False)

    # Link HydroID and feature_id to error lines
    hydroid_join = gpd.GeoDataFrame()
    for i in catchments['branch_id'].unique():
        catchments1 = catchments.loc[catchments['branch_id'] == f'{i}']
        ip = inund_poly.set_crs(catchments1.crs)
        poly_intersect = ip.overlay(catchments1, how='intersection', keep_geom_type=True)

        branch_explode = error_lines_explode.loc[error_lines_explode['branch_id'] == f'{i}']

        poly_join = poly_intersect[['HydroID', 'feature_id', 'branch_id', 'geometry']]
        branch_join = branch_explode[['HydroID', 'feature_id', 'branch_id', 'geometry']]

        feature_join = branch_join.overlay(poly_join, how='intersection', keep_geom_type=False)
        lines_joined = feature_join.loc[feature_join.geom_type.isin(['LineString', 'MultiLineString'])]

        lines_drop = lines_joined.drop(columns=['HydroID_1', 'feature_id_1', 'branch_id_1'])
        rename_attributes = lines_drop.drop_duplicates().rename(
            columns={'HydroID_2': 'HydroID', 'feature_id_2': 'feature_id', 'branch_id_2': 'branch_id'}
        )
        hydroid_join = pd.concat([hydroid_join, rename_attributes])

    # Filter remaining lines by length
    hydroid_join_len = hydroid_join.assign(Length=hydroid_join.length)
    error_lines_final = hydroid_join_len.loc[hydroid_join_len['Length'] >= min_error_length].copy()
    num_catchment_boundary_lines = len(error_lines_final)

    if os.path.exists(output):
        print(f"{output} already exists. Concatinating now...")
        existing_error_lines = gpd.read_file(output, engine="pyogrio", use_arrow=True)
        error_lines_final = pd.concat([existing_error_lines, error_lines_final])
    error_lines_final.to_file(output, driver="GPKG", index=False)

    print(
        f'Finished processing huc: {huc}. Number of boundary line issues identified: {num_catchment_boundary_lines}.'
    )


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
    parser.add_argument("-u", "--huc", help="HUC to run", required=True, default="", type=str, nargs="+")
    parser.add_argument(
        "-i", "--inundation-raster", help="Inundation raster output.", required=True, default=None, type=str
    )
    parser.add_argument(
        "-o", "--output", help="Output geopackage location.", required=True, default=None, type=str
    )
    parser.add_argument(
        '-min',
        '--min_error_length',
        help='Minimum length for output error lines. Default is 100 meters.',
        required=False,
        type=int,
        default=100,
    )

    start = timer()

    catchment_boundary_errors(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
