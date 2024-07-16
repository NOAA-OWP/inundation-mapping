#!/usr/bin/env python3

import geopandas as gpd
import argparse

def analyze_flood_impact(inundation_tif, structures_gpkg, roads_gpkg, output_gpkg):
    # Load vector files
    flood_extent = vectorize(inundation_tif)
    structures = gpd.read_file(structures_gpkg)
    roads = gpd.read_file(roads_gpkg)
    
    # Ensure all data are in the same CRS
    structures = structures.to_crs(flood_extent.crs)
    roads = roads.to_crs(flood_extent.crs)
    
    # Find intersecting structures
    impacted_structures = gpd.sjoin(structures, flood_extent, how= 'inner', predicate='intersects')
    impacted_structures['isImpacted'] = True
    
    # Find non-intersecting structures
    non_impacted_structures = structures[~structures.index.isin(impacted_structures.index)]
    non_impacted_structures['isImpacted'] = False
    
    # Combine impacted and non-impacted structures
    all_structures = impacted_structures.append(non_impacted_structures)
    
    # Find intersecting roads
    impacted_roads = gpd.sjoin(roads, flood_extent, how= 'inner', predicate='intersects')
    impacted_roads['isImpacted'] = True
    
    # Find non-intersecting roads
    non_impacted_roads = roads[~roads.index.isin(impacted_roads.index)]
    non_impacted_roads['isImpacted'] = False
    
    # Combine impacted and non-impacted roads
    all_roads = impacted_roads.append(non_impacted_roads)
    
    # Save the combined data to new layers in a GeoPackage file
    all_structures.to_file(output_gpkg, layer='structures', driver="GPKG")
    all_roads.to_file(output_gpkg, layer='roads', driver="GPKG")
    
    print(f"Structures and roads with impact attribute saved to {output_gpkg}")

def vectorize(inundation_tif, output_gpkg):
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
            riofeatures.shapes(
                fim,
                mask=fim > 0,
                transform=fim_transform,
                connectivity=8
            )
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

    args = parser.parse_args()
    
    analyze_flood_impact(args.inundation, args.structures, args.roads, args.output)
