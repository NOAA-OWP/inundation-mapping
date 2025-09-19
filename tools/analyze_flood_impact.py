#!/usr/bin/env python3

import argparse
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import features as riofeatures


def analyze_flood_impact(
    benchmark_inundation_tif, test_inundation_tif, model_domain_shp, structures_gpkg, roads_gpkg, output_gpkg
):
    """
    Assesses the impact of a flood on road and building vector files. Counts how many roads and structures a benchmark and test
    flood extent intersect and calculates CSI.

    Data information:
    - Building vector files can be accessed from the FEMA Geospatial Rescource Center USA Structures webpage here:
        https://gis-fema.hub.arcgis.com/pages/usa-structures.
    - Road vector files are provided by state DOT GIS services. Ex. Texas DOT data can be found here:
        https://gis-txdot.opendata.arcgis.com/

    Parameters
    ----------
    benchmark_inundation_tif : str
        Input path for benchmark inundation raster.
    test_inundation_tif : str
        Input path for test inundation raster.
    model_domain_shp : str
        Input path for the model domain vector file. Domain file should match the extent and CRS of the benchmark inundation file.
    structures_gpkg : str
        Input path for the structures vector file. File must have an OBJECTID field identifying individual structures.
    roads_gpkg : str
        Input path for the roads vector file. File must have an OBJECTID field identifying individual roads.


    Outputs
    -------
    output_gpkg: str
        Output path for the geopackage vector file that contains the following:
            - test inundation: test inundation extent vector file, clipped to the model domain.
            - test impacted structures: structures that intersect the test inundation extent.
            - test impacted roads: roads that intersect the test inundation extent.
            - benchmark inundation: benchmark inundation extent.
            - benchmark impacted structures: structures that intersect the benchmark inundation extent.
            - benchmark impacted roads: roads that intersect the test inundation extent.

    This function will also print CSI and print the number of impacted roads and structures for each inundation extent.
    """
    # Load vector files
    flood_extent_bench = vectorize(benchmark_inundation_tif)
    flood_extent_test_whole = vectorize(test_inundation_tif)
    domain = gpd.read_file(model_domain_shp)
    structures = gpd.read_file(structures_gpkg)
    roads = gpd.read_file(roads_gpkg)

    # Clip the test extent to the model domain
    flood_extent_test = gpd.clip(flood_extent_test_whole, domain)

    # Ensure all data are in the same CRS
    structures = structures.to_crs(flood_extent_test.crs)
    roads = roads.to_crs(flood_extent_test.crs)
    flood_extent_bench = flood_extent_bench.to_crs(flood_extent_test.crs)

    # Find intersecting structures/roads and create gdf for benchmark and test
    impacted_structures_bench = impacted(structures, flood_extent_bench)
    impacted_structures_test = impacted(structures, flood_extent_test)
    impacted_roads_bench = impacted(roads, flood_extent_bench)
    impacted_roads_test = impacted(roads, flood_extent_test)

    # Calculate CSI

    # TP: features in both benchmark and test
    true_positives_structures = impacted_structures_bench.merge(
        impacted_structures_test, how='left', on='OBJECTID', suffixes=('_benchmark', '_test')
    )
    true_positives_roads = impacted_roads_bench.merge(
        impacted_roads_test, how='left', on='OBJECTID', suffixes=('_benchmark', '_test')
    )

    # FN: features in benchmark but not in test
    false_neg_structures = impacted_structures_bench[
        ~impacted_structures_bench['OBJECTID'].isin(impacted_structures_test['OBJECTID'])
    ]
    false_neg_roads = impacted_roads_bench[
        ~impacted_roads_bench['OBJECTID'].isin(impacted_roads_test['OBJECTID'])
    ]

    # FP: features in test but not in benchmark
    false_pos_structures = impacted_structures_test[
        ~impacted_structures_test['OBJECTID'].isin(impacted_structures_bench['OBJECTID'])
    ]
    false_pos_roads = impacted_roads_test[
        ~impacted_roads_test['OBJECTID'].isin(impacted_roads_bench['OBJECTID'])
    ]

    # Calculation
    TP = len(true_positives_structures) + len(true_positives_roads)
    FN = len(false_neg_structures) + len(false_neg_roads)
    FP = len(false_pos_structures) + len(false_pos_roads)
    CSI = TP / (TP + FN + FP)

    # Save the combined data to new layers in a GeoPackage file
    impacted_structures_bench.to_file(output_gpkg, layer='benchmark impacted structures', driver="GPKG")
    impacted_roads_bench.to_file(output_gpkg, layer='benchmark impacted roads', driver="GPKG")
    impacted_structures_test.to_file(output_gpkg, layer='test impacted structures', driver="GPKG")
    impacted_roads_test.to_file(output_gpkg, layer='test impacted roads', driver="GPKG")
    flood_extent_bench.to_file(output_gpkg, layer='benchmark inundation', index=False)
    flood_extent_test.to_file(output_gpkg, layer='test inundation', index=False)

    print(f"Structures and roads with impact attribute for benchmark and test data saved to {output_gpkg}.")

    # Total impacted infrastructure
    total_structures_test_impact = len(impacted_structures_test)
    total_road_test_impact = len(impacted_roads_test)
    total_structures_bench_impact = len(impacted_structures_bench)
    total_road_bench_impact = len(impacted_roads_bench)

    print(
        f" Benchmark: {total_structures_bench_impact} structures impacted by this flood and {total_road_bench_impact} roads impacted by this flood."
    )
    print(
        f" Test: {total_structures_test_impact} structures impacted by this flood and {total_road_test_impact} roads impacted by this flood."
    )
    print(f'Critical Success Index: {CSI}')


def vectorize(inundation_tif):
    """
    Converts inundation raster into a vector file and returns it.

    Parameters
    ----------
    inundation_tif : str
        Input path for inundation raster.

    Returns
    ----------
    extent_poly_diss :
        Inundation vector
    """
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


def impacted(features_gpkg, inundation_tif):
    """
    Finds features that intersect with an inundation raster (impacted features) and returns the impacted features.

    Parameters
    ----------
    features_gpkg : str
        Input path for feature vector (ex. roads or structures).
    inundation_tif : str
        Input path for inundation raster.

    Returns
    --------
    impacted_features :
        Vector features that intersect with the inundation extent.
    """
    # Find intersecting features with inundation
    impacted_features = gpd.GeoDataFrame(
        gpd.sjoin(features_gpkg, inundation_tif, how='inner', predicate='intersects'), crs=inundation_tif.crs
    )

    impacted_features['isImpacted'] = True
    return impacted_features


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="""Assesses the impact of a flood on road and building vector files. Counts how many roads and structures a
benchmark and test flood extent intersect and calculates CSI.

Sample usage:
python3  analyze_flood_impact.py -b home/user/benchmark_inundation.tif -t home/user/test_inundation.tif \
    -d home/user/model_domain.shp -s home/user/structures_vector.gpkg -rd home/user/roads_vector.gpkg \
    -o home/user/impacted_roads_and_structures_output.gpkg
"""
    )
    # Parse arguments
    parser.add_argument(
        '-b', '--benchmark_inundation', required=True, help="Path to the benchmark inundation TIF file."
    )
    parser.add_argument(
        '-t', '--test_inundation', required=True, help="Path to the test inundation TIF file."
    )
    parser.add_argument(
        '-d',
        '--domain',
        required=True,
        help="Path to the model domain vector file. Domain file should match the extent and CRS of the benchmark inundation file.",
    )
    parser.add_argument(
        '-s',
        '--structures',
        required=True,
        help="Path to the structures vector file. File must have an OBJECTID field identifying individual structures.",
    )
    parser.add_argument(
        '-rd',
        '--roads',
        required=True,
        help="Path to the roads vector file. File must have an OBJECTID field identifying individual roads.",
    )
    parser.add_argument('-o', '--output', required=True, help="Path to the output vector file (GeoPackage).")

    args = vars(parser.parse_args())

    start = timer()

    analyze_flood_impact(
        args['benchmark_inundation'],
        args['test_inundation'],
        args['domain'],
        args['structures'],
        args['roads'],
        args['output'],
    )

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
