#!/usr/bin/env python3

import argparse

import geopandas as gpd
import pandas as pd
import rasterio as rio
from rasterstats import zonal_stats


# Calculate minimum REM values in each catchment
def calculate_min_rem_catchment_values(catchments_gpkg, catchment_tif, rem_raster, out_raster):

    # Read catchments GeoPackage
    if isinstance(catchments_gpkg, str):
        catchments_gpkg = gpd.read_file(catchments_gpkg)

    if not isinstance(catchments_gpkg, gpd.GeoDataFrame):
        raise ValueError("catchments_gpkg must be a file path or a GeoDataFrame")

    # Read raster profile to get nodata value
    with rio.open(rem_raster) as rem_src:
        rem_raster_data = rem_src.read(1)
        rem_profile = rem_src.profile
        rem_nodata = rem_profile['nodata']

    # Get max hand values for each catchment
    stats = zonal_stats(
        catchments_gpkg['geometry'],
        rem_raster,
        stats="min",
        nodata=rem_nodata,
        all_touched=True,
        geojson_out=True,
    )

    gdf = gpd.GeoDataFrame.from_features(stats)
    gdf = pd.merge(catchments_gpkg, gdf, left_index=True, right_index=True)

    # Read catchment raster and replace values with min hand values from gdf
    with rio.open(catchment_tif) as catchment_src:
        catchment_raster = catchment_src.read(1)
        catchment_profile = catchment_src.profile
        catchment_nodata = catchment_profile['nodata']

    out_raster_data = rem_raster_data.copy()
    for _, row in gdf.iterrows():
        min_rem_value = row['min']
        if pd.isna(min_rem_value):
            continue
        out_raster_data[(catchment_raster == row['HydroID']) & (catchment_raster != catchment_nodata)] = (
            min_rem_value
        )

    out_raster_data = rem_raster_data - out_raster_data
    out_raster_data[out_raster_data < 0] = 0  # Ensure non-negative values
    out_raster_data[catchment_raster == catchment_nodata] = rem_nodata
    out_raster_data[rem_raster_data == rem_nodata] = rem_nodata

    with rio.open(out_raster, "w", **rem_profile) as out_src:
        out_src.write(out_raster_data, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate minimum REM values in each catchment")
    parser.add_argument("-c", "--catchments_gpkg", required=True, help="Path to catchments GeoPackage file")
    parser.add_argument("-t", "--catchment_tif", required=True, help="Path to catchment raster file")
    parser.add_argument("-r", "--rem_raster", required=True, help="Path to REM raster file")
    parser.add_argument("-o", "--out_raster", required=True, help="Path to output raster file")
    # parser.add_argument("-m", "--min_raster", help="Path to minimum raster file", required=False)

    args = parser.parse_args()

    calculate_min_rem_catchment_values(**vars(args))
