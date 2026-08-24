#!/usr/bin/env python3

import argparse
import sys

import geopandas as gpd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape

from utils.io import write_geodataframe


def polygonize_in_memory(
    input_raster: str, field_name: str = "HydroID", connectivity: int = 8, output_file: str = None
) -> gpd.GeoDataFrame:
    """Polygonizes a raster dataset into a GeoDataFrame directly in RAM."""
    with rasterio.open(input_raster) as src:
        raster_array = src.read(1)
        transform = src.transform
        crs = src.crs
        nodata = src.nodata

    mask = (raster_array != nodata) if nodata is not None else None

    shape_generator = shapes(raster_array, mask=mask, transform=transform, connectivity=connectivity)

    geometries = []
    values = []
    for geom, value in shape_generator:
        geometries.append(shape(geom))
        values.append(int(value))

    gdf = gpd.GeoDataFrame({field_name: values}, geometry=geometries, crs=crs)

    if output_file:
        write_geodataframe(gdf, output_file, index=False)

    return gdf


def polygonize_raster(
    input_raster: str,
    output_file: str,
    field_name: str = "HydroID",
    connectivity: int = 8,
    quiet: bool = False,
) -> None:
    """Polygonizes a raster file and writes the resulting vector file to disk using write_geodataframe.

    Parameters
    ----------
    input_raster: str
        Input raster filename
    output_file: str
        Output vector filename (Parquet/GPKG)
    field_name: str
        Name of field containing values (default: 'HydroID')
    connectivity: int
        Number of cells in connectivity (4 or 8)
    quiet: bool
        Toggle for verbosity
    """
    if not quiet:
        print(f"Reading input raster: {input_raster}...")

    try:
        with rasterio.open(input_raster) as src:
            raster_array = src.read(1)
            transform = src.transform
            crs = src.crs
            nodata = src.nodata
    except Exception as e:
        print(f"Error reading raster: {e}", file=sys.stderr)
        sys.exit(1)

    if nodata is not None:
        mask = raster_array != nodata
    else:
        mask = None

    if not quiet:
        print(f"Extracting polygons ({connectivity}-way connectivity)...")

    try:
        shape_generator = shapes(raster_array, mask=mask, transform=transform, connectivity=connectivity)

        geometries = []
        values = []
        for geom, value in shape_generator:
            geometries.append(shape(geom))
            values.append(int(value))

    except Exception as e:
        print(f"Error during polygonization: {e}", file=sys.stderr)
        sys.exit(1)

    if not quiet:
        print(f"Creating GeoDataFrame and saving to {output_file}...")

    try:
        gdf = gpd.GeoDataFrame({field_name: values}, geometry=geometries, crs=crs)
        write_geodataframe(gdf, output_file, index=False)

        if not quiet:
            print("Done successfully!")

    except Exception as e:
        print(f"Error writing output: {e}", file=sys.stderr)
        sys.exit(1)


# Backward compatibility alias
polygonize = polygonize_raster


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Python equivalent of gdal_polygonize converting a raster directly to vector output."
    )

    parser.add_argument("input_raster", help="Path to input raster file (.tif)")
    parser.add_argument("output_file", help="Path to output vector file")
    parser.add_argument(
        "field_name",
        nargs="?",
        default="HydroID",
        help="Name of the attribute column to create from raster values",
    )

    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress progress outputs")
    parser.add_argument(
        "-8",
        dest="connectivity",
        action="store_const",
        const=8,
        default=4,
        help="Use 8-connectedness instead of 4-connectedness",
    )

    args = parser.parse_args()

    polygonize_raster(**vars(args))
