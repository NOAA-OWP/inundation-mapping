import argparse
import os
from timeit import default_timer as timer

import rasterio
from rasterio import features
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import shape


def catchment_boundary_errors(    
    hydrofabric_dir,
    hucs,
    inundation_raster,
    output
):
    """
    This function compares output inundation raster extent to catchment extents to identify catchment boundary
    issues. The output of this function is a shapefile of lines that identify areas of inundation with catchment 
    boundary issues present.

    Args:
        hydrofabric_dir (str):    Path to hydrofabric directory where FIM outputs were written by
                                    fim_pipeline.
        huc (str):               The HUC for which to check for catchment boundary issues.
        inundation_raster (str):  Full path to output inundation raster
                                    (encoded by positive and negative HydroIDs).
        output (str):             Path to output location for catchment boundary geopackage.
                                   
    """
    for huc in hucs:
        # Get branch names for input HUC
        dirs = [x[1] for x in os.walk(f"{hydrofabric_dir}/{huc}/branches")] [0]

        # merge all catchment geopackages into one file
        catchments = gpd.GeoDataFrame()
        for d in dirs:
            catch = gpd.read_file(f"{hydrofabric_dir}/{huc}/branches/{d}/gw_catchments_reaches_filtered_addedAttributes_crosswalked_{d}.gpkg")
            catchments = pd.concat([catchments, catch], ignore_index = True)

        # vectorize inundation 
        with rasterio.open(inundation_raster) as src:
            affine = src.transform
            band = src.read(1)
            band[np.where(band <= 0)] = 0
            band[np.where(band > 0)] = 1
            
        # read geometries from raster and save to geodataframe
        inund_poly = gpd.GeoDataFrame()
        for shp, val in features.shapes(band, transform = affine):
            if val == 1.0:
                temp = {'val': val, 'geometry' : gpd.GeoSeries(shape(shp))}
                temp = gpd.GeoDataFrame(temp)
                inund_poly = pd.concat([inund_poly,temp], ignore_index = True)

        # Get boundary lines for catchments and inundation
        cb = catchments.boundary
        ib = inund_poly.boundary
        ib = ib.set_crs(cb.crs)

        # Explode geometries to only keep single linestrings
        cb = cb.explode(ignore_index = True)
        ib = ib.explode(ignore_index = True)

        # Find intersection of inundation and catchments
        intersect = gpd.GeoDataFrame()
        ib_df = gpd.GeoDataFrame(geometry = ib)
        for i in cb:
            ib_df = ib_df.assign(intersects = ib.intersects(i))
            ib_df_1 = ib_df.loc[ib_df['intersects'] == True]
            ib_df_2 = gpd.GeoDataFrame(geometry = ib_df_1.intersection(i))
            intersect = pd.concat([intersect, ib_df_2])
            
        intersect_exp = intersect.explode(ignore_index = True)

        intersect_lines = intersect_exp.loc[intersect_exp.geom_type=='LineString']
        num_errors = len(intersect_lines)
        if os.path.exists(output):
            print(f"{output} already exists. Concatinating now...")
            existing_intersect_lines = gpd.read_file(output, engine="pyogrio", use_arrow=True)
            intersect_lines = pd.concat([existing_intersect_lines, intersect_lines])
        intersect_lines.to_file(output, index=False)
        print(f"Identified {num_errors} catchment boundary issues within given huc.")

        return output

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

    start = timer()

    catchment_boundary_errors(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
