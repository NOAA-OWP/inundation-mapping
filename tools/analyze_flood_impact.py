#!/usr/bin/env python3

import argparse
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio
from rasterio import features as riofeatures


def analyze_flood_impact(inundation_tif, structures_gpkg, roads_gpkg, output_gpkg):
    # Load vector files
    flood_extent = vectorize(inundation_tif)
    structures = gpd.read_file(structures_gpkg)
    roads = gpd.read_file(roads_gpkg)

    # Ensure all data are in the same CRS
    structures = structures.to_crs(flood_extent.crs)
    roads = roads.to_crs(flood_extent.crs)

    # Find intersecting structures and create gdf
    impacted_structures = gpd.GeoDataFrame(gpd.sjoin(structures, flood_extent, how='inner', predicate='intersects'), crs = flood_extent.crs)
    impacted_structures['isImpacted'] = True
    impacted_structures['fid'] = impacted_structures['fid'].astype('int64')

    # Find intersecting roads and create gdf
    impacted_roads = gpd.GeoDataFrame(gpd.sjoin(roads, flood_extent, how='inner', predicate='intersects'), crs = flood_extent.crs)
    impacted_roads['isImpacted'] = True
    impacted_roads['fid'] = impacted_roads['fid'].astype('int64')


    # Save the combined data to new layers in a GeoPackage file
    impacted_structures.to_file(output_gpkg, layer='structures', driver="GPKG")
    impacted_roads.to_file(output_gpkg, layer='roads', driver="GPKG")
    flood_extent.to_file(output_gpkg, layer='inundation', index= False)

    print(f"Structures and roads with impact attribute saved to {output_gpkg}.")

    # Total impacted infrastructure
    total_structures_impact= len(impacted_structures)
    total_road_impact = len(impacted_roads)
    
    print(f" There are {total_structures_impact} structures impacted by this flood and {total_road_impact} roads impacted by this flood.")


def vectorize(inundation_tif):
    with rasterio.open(inundation_tif) as fim_rast:
        fim_nodata = fim_rast.profile['nodata']
        fim_transform = fim_rast.transform
        fim_crs = fim_rast.crs
        fim = fim_rast.read(1).astype(np.float32)

    # Create binary raster
    fim[np.where(fim == fim_nodata)] = np.nan
    fim[np.where(fim <= 0)] = np.nan
    fim[np.where(fim > 0)] = 1

    # Vectorize
    results = (
        {"properties": {"extent": 1}, "geometry": s}
        for i, (s, v) in enumerate(
            riofeatures.shapes(fim, mask=fim > 0, transform=fim_transform, connectivity=8)
        )
    )

    # Convert list of shapes to polygon, then dissolve
    extent_poly = gpd.GeoDataFrame.from_features(list(results), crs=fim_crs)
    extent_poly_diss = extent_poly.dissolve()

    return extent_poly_diss


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Analyze flood impact on structures and roads.")
    parser.add_argument('-i', '--inundation', required=True, help="Path to the inundation TIF file.")
    parser.add_argument('-s', '--structures', required=True, help="Path to the structures vector file.")
    parser.add_argument('-rd', '--roads', required=True, help="Path to the roads vector file.")
    parser.add_argument('-o', '--output', required=True, help="Path to the output vector file (GeoPackage).")

    args = vars(parser.parse_args())

    analyze_flood_impact(args['inundation'], args['structures'], args['roads'], args['output'])