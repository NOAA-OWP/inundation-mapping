#!/usr/bin/env python3

import argparse
from contextlib import nullcontext
from pathlib import Path
from typing import Union

import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import Point

from utils.io import write_geodataframe
from utils.shared_variables import PREP_PROJECTION


gpd.options.io_engine = "pyogrio"


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

    # Filter out NoData pixels and background cells (< 1)
    if nodata_val is not None:
        valid_mask = (data != nodata_val) & (data >= 1)
    else:
        valid_mask = (~np.isnan(data)) & (data >= 1)

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


def convert_grid_cells_to_points(
    raster: Union[str, Path, rasterio.io.DatasetReader],
    index_option: str,
    output_points_filename: Union[str, Path, bool] = False,
) -> gpd.GeoDataFrame:
    """Vectorized compatibility wrapper converting raster stream cells (>= 1) to point vector centroids."""
    if isinstance(raster, (str, Path)):
        ctx = rasterio.open(raster, "r")
    elif isinstance(raster, rasterio.io.DatasetReader):
        ctx = nullcontext(raster)
    else:
        raise TypeError("Pass raster dataset handle or filepath string")

    with ctx as src:
        crs = src.crs if src.crs is not None else PREP_PROJECTION
        arr = src.read(1)

        y_indices, x_indices = np.nonzero(arr >= 1)
        total_pts = len(y_indices)

        if total_pts == 0:
            point_gdf = gpd.GeoDataFrame(columns=["id", "geometry"], crs=crs)
        else:
            xs, ys = rasterio.transform.xy(src.transform, y_indices, x_indices, offset="center")
            geoms = [Point(x, y) for x, y in zip(xs, ys)]

            if index_option in ("featureID", "pixelID"):
                pt_ids = np.arange(1, total_pts + 1, dtype=np.int64)
            elif index_option == "reachID":
                reach_vals = arr[y_indices, x_indices].astype(np.int64)
                i_counter = np.arange(1, total_pts + 1, dtype=np.int64)
                pt_ids = reach_vals * 10000 + i_counter
            else:
                raise ValueError(
                    f"Invalid index_option '{index_option}'. Must be reachID, featureID, or pixelID."
                )

            point_gdf = gpd.GeoDataFrame({"id": pt_ids, "geometry": geoms}, crs=crs)

    if output_points_filename:
        write_geodataframe(point_gdf, str(output_points_filename), index=False)

    return point_gdf


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
    parser = argparse.ArgumentParser(description="Convert reach/feature ID grid centroids to vector points.")
    parser.add_argument("-r", "--input-raster", required=True, help="Input raster file path")
    parser.add_argument(
        "-i", "--id-field", default="featureID", help="Output ID attribute column name or index option"
    )
    parser.add_argument("-p", "--output-gpkg", required=True, help="Output vector layer file path")

    args = parser.parse_args()

    if args.id_field in ["reachID", "pixelID"]:
        convert_grid_cells_to_points(
            raster=args.input_raster, index_option=args.id_field, output_points_filename=args.output_gpkg
        )
    else:
        reachID_grid_to_vector_points(
            input_raster_path=args.input_raster,
            id_field_name=args.id_field,
            output_gpkg_path=args.output_gpkg,
        )
