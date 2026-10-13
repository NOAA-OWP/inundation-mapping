#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape

from utils.io import write_geodataframe


def polygonize_raster(
    input_raster: str, output_file: str, field_name: str = "DN", connectivity: int = 8, quiet: bool = False
):
    """Convert a raster file into polygon geometries saved to GeoParquet."""
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


def polygonize_extent(dem_files, dem_domain_file, file_logger, sf=None, fh=None):
    """
    Create a polygon of 3DEP domain from individual HUC DEMs which are then dissolved into a single polygon.
    """
    msg = f" - Polygonizing -- {dem_domain_file} - Started (be patient, it can take a while)"
    if sf:
        sf.l_print(msg, file_logger, "info")

    start_time = datetime.now(timezone.utc)
    if sf:
        sf.l_print(
            f"Polygonation start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info"
        )

    if len(dem_files) == 0:
        raise Exception("There are no DEMs to polygonize")

    dem_files = sorted(list(set(dem_files)))
    gdfs = []

    for dem_file in dem_files:
        if sf:
            sf.l_print(f"Polygonizing: {dem_file}", file_logger, "info")

        edge_tif = f"{os.path.splitext(dem_file)[0]}_edge.tif"
        edge_parquet = f"{os.path.splitext(edge_tif)[0]}.parquet"

        if not os.path.exists(edge_tif):
            subprocess.run(
                [
                    "gdal_calc.py",
                    "-A",
                    dem_file,
                    f"--outfile={edge_tif}",
                    "--calc=where(A > -900, 1, 0)",
                    "--co",
                    "BIGTIFF=YES",
                    "--co",
                    "NUM_THREADS=ALL_CPUS",
                    "--co",
                    "TILED=YES",
                    "--co",
                    "COMPRESS=LZW",
                    "--co",
                    "SPARSE_OK=TRUE",
                    "--type=Byte",
                    "--quiet",
                ],
                check=True,
            )

        # Matched to local polygonize_raster function name
        polygonize_raster(
            input_raster=edge_tif, output_file=edge_parquet, field_name="DN", connectivity=8, quiet=True
        )

        gdf = gpd.read_parquet(edge_parquet)
        gdfs.append(gdf)

        if os.path.exists(edge_tif):
            os.remove(edge_tif)
        if os.path.exists(edge_parquet):
            os.remove(edge_parquet)

    if gdfs:
        dem_parquets = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    else:
        dem_parquets = gpd.GeoDataFrame()

    dem_parquets["DN"] = 1
    dem_dissolved = dem_parquets.dissolve(by="DN")

    os.makedirs(os.path.dirname(os.path.abspath(dem_domain_file)), exist_ok=True)
    write_geodataframe(dem_dissolved, dem_domain_file)

    if sf:
        if not os.path.exists(dem_domain_file):
            sf.l_print(f" - Polygonizing -- {dem_domain_file} - Failed", file_logger, "error")
        else:
            sf.l_print(f" - Polygonizing -- {dem_domain_file} - Complete", file_logger, "info")

        end_time = datetime.now(timezone.utc)
        sf.l_print(f"Polygonization end time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")
        if fh:
            sf.l_print(
                fh.print_date_time_duration(start_time, end_time, print_dur_msg=False), file_logger, "info"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Python equivalent of gdal_polygonize converting a raster directly to GeoParquet."
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
