#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio


def reachID_grid_to_vector_points_in_memory(
    raster_dataset: rasterio.DatasetReader, id_field_name: str = "featureID", nodata_val: float = None
) -> gpd.GeoDataFrame:
    """Converts non-NoData raster grid pixel centroids to a GeoDataFrame in-memory.

    Parameters
    ----------
    raster_dataset : rasterio.DatasetReader
        Open rasterio dataset object in RAM.
    id_field_name : str
        Attribute column name for the pixel grid values (default: 'featureID').
    nodata_val : float, optional
        Override NoData value. If None, uses raster_dataset.nodata.

    Returns
    -------
    gpd.GeoDataFrame
        Point features corresponding to pixel centroids with raster IDs.
    """
    data = raster_dataset.read(1)

    if nodata_val is None:
        nodata_val = raster_dataset.nodata

    # Filter out NoData pixels
    if nodata_val is not None:
        valid_mask = data != nodata_val
    else:
        valid_mask = ~np.isnan(data)

    if not np.any(valid_mask):
        print("Warning: No valid pixel values found in grid.")
        return gpd.GeoDataFrame(columns=[id_field_name, "geometry"], crs=raster_dataset.crs)

    # Extract pixel row/col indices for valid data
    rows, cols = np.where(valid_mask)
    pixel_values = data[rows, cols].astype(np.int64)

    # Calculate real-world centroid coordinates using affine transform
    xs, ys = rasterio.transform.xy(raster_dataset.transform, rows, cols, offset="center")

    # Build GeoDataFrame directly in RAM using vectorized Points
    points_gdf = gpd.GeoDataFrame(
        {id_field_name: pixel_values}, geometry=gpd.points_from_xy(xs, ys), crs=raster_dataset.crs
    )

    return points_gdf


def reachID_grid_to_vector_points(input_raster_path: str, id_field_name: str, output_gpkg_path: str) -> None:
    """File I/O wrapper around the in-memory centroid vectorizer."""
    print(f"Loading {input_raster_path} into RAM...")
    with rasterio.open(input_raster_path) as src:
        gdf = reachID_grid_to_vector_points_in_memory(raster_dataset=src, id_field_name=id_field_name)

    print(f"Writing {len(gdf)} points to {output_gpkg_path}...")
    out_path = Path(output_gpkg_path)
    if out_path.exists():
        out_path.unlink()

    gdf.to_file(out_path, driver="GPKG", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert reach/feature ID grid centroids to vector points in-memory."
    )
    parser.add_argument("-r", "--input-raster", required=True, help="Input raster file path")
    parser.add_argument("-i", "--id-field", default="featureID", help="Output ID attribute column name")
    parser.add_argument("-p", "--output-gpkg", required=True, help="Output vector GeoPackage path")

    args = parser.parse_args()
    reachID_grid_to_vector_points(
        input_raster_path=args.input_raster, id_field_name=args.id_field, output_gpkg_path=args.output_gpkg
    )
