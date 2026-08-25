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
    raster: Union[str, Path, rasterio.io.DatasetReader],
    index_option: str = "featureID",
    output_points_filename: Union[str, Path, bool] = False,
    raster_dataset: rasterio.io.DatasetReader = None,
    id_field_name: str = None,
) -> gpd.GeoDataFrame:
    """Converts non-zero raster grid cells (>= 1) directly to a spatial point GeoDataFrame.

    Supports both file paths and active Rasterio DatasetReader handles.
    """
    # Map legacy kwargs if passed from older pipeline calls
    if raster_dataset is not None:
        raster = raster_dataset
    if id_field_name is not None:
        index_option = id_field_name

    if isinstance(raster, (str, Path)):
        ctx = rasterio.open(raster, "r")
    elif isinstance(raster, rasterio.io.DatasetReader):
        ctx = nullcontext(raster)
    else:
        raise TypeError("Pass a raster dataset handle or file path.")

    with ctx as src:
        arr = src.read(1)
        transform = src.transform
        crs = src.crs if src.crs is not None else PREP_PROJECTION

        # Find 0-based row (y) and col (x) indices of valid stream cells (>= 1)
        y_indices, x_indices = np.nonzero(arr >= 1)
        total_pts = len(y_indices)

        if total_pts == 0:
            point_gdf = gpd.GeoDataFrame(columns=["id", "geometry"], crs=crs)
            if output_points_filename:
                write_geodataframe(point_gdf, str(output_points_filename), index=False)
            return point_gdf

        # 1. Compute spatial X, Y cell centroid coordinates (vectorized)
        xs, ys = rasterio.transform.xy(transform, y_indices, x_indices, offset="center")
        geoms = [Point(x, y) for x, y in zip(xs, ys)]

        # 2. Compute point IDs matching index options
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

    # Construct GeoDataFrame
    gdf = gpd.GeoDataFrame({"id": pt_ids, "geometry": geoms}, crs=crs)

    if output_points_filename:
        write_geodataframe(gdf, str(output_points_filename), index=False)

    return gdf


# Vectorized compatibility aliases for callers using either function name
convert_grid_cells_to_points = reachID_grid_to_vector_points_in_memory


def reachID_grid_to_vector_points(input_raster_path: str, id_field_name: str, output_gpkg_path: str) -> None:
    """File I/O wrapper calling the in-memory centroid vectorizer."""
    reachID_grid_to_vector_points_in_memory(
        raster=input_raster_path, index_option=id_field_name, output_points_filename=output_gpkg_path
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converts raster stream cells to points.")
    parser.add_argument("-r", "--raster", dest="raster", help="Raster file path", required=True, type=str)
    parser.add_argument(
        "-i",
        "--index-option",
        dest="index_option",
        help="Indexing option or ID attribute column name",
        required=False,
        default="featureID",
        type=str,
    )
    parser.add_argument(
        "-p",
        "--output-points-filename",
        dest="output_points_filename",
        help="Output points layer file path",
        required=False,
        default=False,
    )

    args = parser.parse_args()

    reachID_grid_to_vector_points_in_memory(
        raster=args.raster, index_option=args.index_option, output_points_filename=args.output_points_filename
    )
