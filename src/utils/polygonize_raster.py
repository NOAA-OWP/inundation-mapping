#!/usr/bin/env python3
import argparse
import concurrent.futures
import glob
import os
import sys
from datetime import datetime, timezone

import geopandas as gpd
import numpy as np
import rasterio
import scipy.ndimage
import shapely
from rasterio.features import shapes
from shapely.geometry import shape

from utils.io import write_geodataframe


# =====================================================================
# UNCHANGED: Retained for external script compatibility & CLI usage
# =====================================================================
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


# =====================================================================
# In-memory, multi-core exact 10m HUC polygonization helpers
# =====================================================================
def _extract_exact_huc_geometry(dem_file: str):
    """
    Extracts the 1:1 pixel polygon domain for a single HUC8 DEM in-memory,
    matching legacy gdal_calc + polygonize_raster behavior without disk I/O.
    """
    try:
        with rasterio.open(dem_file) as src:
            arr = src.read(1)
            nodata_val = src.nodata if src.nodata is not None else -9999.0

            # Match dev's gdal_calc: where(A > -900, 1, 0)
            binary_arr = np.where((arr > -900) & (arr != nodata_val), 1, 0).astype(np.uint8)

            if not np.any(binary_arr == 1):
                return None

            # Omit `mask=` so shapes() segments solid 0/1 regions cleanly (avoids bowtie hangs)
            geoms = [
                shape(geom)
                for geom, val in shapes(binary_arr, transform=src.transform, connectivity=8)
                if val == 1
            ]

            if not geoms:
                return None

            # unary_union safely handles any 8-connected diagonal corner touches
            return shapely.unary_union(geoms)

    except Exception as e:
        print(f"Error processing {dem_file}: {e}", file=sys.stderr)
        return None


def _hierarchical_union(geometries, batch_size: int = 32):
    """
    Hierarchically merges polygons in batches using unary_union
    to safely resolve overlaps while maintaining low memory.
    """
    current_geoms = [g for g in geometries if g is not None and not g.is_empty]

    while len(current_geoms) > 1:
        batches = [current_geoms[i : i + batch_size] for i in range(0, len(current_geoms), batch_size)]
        next_level = []
        for batch in batches:
            merged = shapely.unary_union(batch)
            next_level.append(merged)
        current_geoms = next_level

    result = current_geoms[0] if current_geoms else shapely.GeometryCollection()

    # Ensure OGC validity without altering pixel coordinates or exploding vertices
    if not result.is_valid:
        result = shapely.make_valid(result)

    return result


def polygonize_extent(dem_files, dem_domain_file, number_of_jobs=1, file_logger=None, sf=None, fh=None):
    """
    Create a polygon of 3DEP domain from individual HUC DEMs at exact 10m resolution.
    Runs in-memory across multiple CPU cores, then hierarchically dissolves geometries.
    """
    msg = f" - Polygonizing -- {dem_domain_file} - Started (10m exact in-memory)"
    if sf and file_logger:
        sf.l_print(msg, file_logger, "info")
    else:
        print(msg)

    start_time = datetime.now(timezone.utc)
    if sf and file_logger:
        sf.l_print(
            f"Polygonization start time: {start_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info"
        )

    if len(dem_files) == 0:
        raise ValueError("There are no DEMs to polygonize")

    dem_files = sorted(list(set(dem_files)))

    with rasterio.open(dem_files[0]) as ref:
        target_crs = ref.crs

    max_workers = max(1, min((os.cpu_count() or 1) - 2, number_of_jobs))
    if sf and file_logger:
        sf.l_print(f"Extracting 10m pixel boundaries using {max_workers} processes...", file_logger, "info")
    else:
        print(f"Extracting 10m pixel boundaries across {len(dem_files)} DEMs using {max_workers} workers...")

    huc_geometries = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for poly in executor.map(_extract_exact_huc_geometry, dem_files):
            if poly is not None and not poly.is_empty:
                huc_geometries.append(poly)

    if not huc_geometries:
        raise RuntimeError("No valid geometries could be extracted from input DEMs")

    if sf and file_logger:
        sf.l_print(f"Hierarchically dissolving {len(huc_geometries)} HUC8 boundaries...", file_logger, "info")
    else:
        print(f"Dissolving {len(huc_geometries)} boundaries...")

    dissolved_geom = _hierarchical_union(huc_geometries, batch_size=32)

    dem_dissolved = gpd.GeoDataFrame({"DN": [1]}, geometry=[dissolved_geom], crs=target_crs)

    os.makedirs(os.path.dirname(os.path.abspath(dem_domain_file)), exist_ok=True)
    write_geodataframe(dem_dissolved, dem_domain_file)

    if sf and file_logger:
        status_msg = "Complete" if os.path.exists(dem_domain_file) else "Failed"
        level = "info" if status_msg == "Complete" else "error"
        sf.l_print(f" - Polygonizing -- {dem_domain_file} - {status_msg}", file_logger, level)

        end_time = datetime.now(timezone.utc)
        sf.l_print(f"Polygonization end time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}", file_logger, "info")
        if fh:
            sf.l_print(
                fh.print_date_time_duration(start_time, end_time, print_dur_msg=False), file_logger, "info"
            )
    else:
        print(f"Saved domain boundary to {dem_domain_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract polygon boundaries from a raster.")

    # Positional arguments matching original interface
    parser.add_argument("-i", "--input_file", type=str, required=True, help="Input raster to polygonize.")
    parser.add_argument(
        "-o",
        "--output_file",
        type=str,
        required=True,
        help="Path to output vector file (.parquet, .gpkg, .shp, etc.)",
    )
    parser.add_argument(
        "-f",
        "--field_name",
        type=str,
        required=False,
        default="HydroID",
        help="Name of the attribute column to create from raster values (default: HydroID)",
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

    # Standard single-raster polygonize mode
    polygonize_raster(
        input_raster=args.input_file,
        output_file=args.output_file,
        field_name=args.field_name,
        connectivity=args.connectivity,
        quiet=args.quiet,
    )
