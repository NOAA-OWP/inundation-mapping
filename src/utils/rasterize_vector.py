#!/usr/bin/env python3
"""
src/utils/rasterize_vector.py
------------------------------
Optimized helper function for rasterizing vector features (GeoDataFrames or files)
matching a template raster's transform, dimensions, CRS, and nodata settings.
"""

import argparse
from pathlib import Path
from typing import Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from rasterio.features import rasterize


def rasterize_vector(
    vector_path_or_gdf: Union[str, Path, gpd.GeoDataFrame],
    template_raster_path: str,
    output_raster_path: str = None,
    attribute: str = None,
    burn_value: Union[int, float] = None,
    init_value: Union[int, float] = 0,
    dtype: np.dtype = None,
) -> tuple[np.ndarray, dict]:
    """Rasterizes vector geometries (GeoDataFrame or file) to match a template raster."""

    # 1. Load input vector layer
    if isinstance(vector_path_or_gdf, (str, Path)):
        if not Path(vector_path_or_gdf).is_file():
            raise FileNotFoundError(f"Vector file not found: {vector_path_or_gdf}")
        gdf = gpd.read_file(vector_path_or_gdf)
    elif isinstance(vector_path_or_gdf, gpd.GeoDataFrame):
        gdf = vector_path_or_gdf
    else:
        raise TypeError("vector_path_or_gdf must be a file path or GeoDataFrame.")

    # 2. Read template raster metadata
    with rio.open(template_raster_path) as tmpl:
        profile = tmpl.profile.copy()
        transform = tmpl.transform
        crs = tmpl.crs
        out_shape = (tmpl.height, tmpl.width)
        bounds = tmpl.bounds

    # Handle empty GeoDataFrames early
    if gdf.empty:
        out_dtype = dtype or (np.float32 if isinstance(init_value, float) else np.int32)
        out_arr = np.full(out_shape, fill_value=init_value, dtype=out_dtype)
        profile.update(dtype=out_dtype, count=1)
        if output_raster_path:
            Path(output_raster_path).parent.mkdir(parents=True, exist_ok=True)
            with rio.open(output_raster_path, "w", **profile) as dst:
                dst.write(out_arr, 1)
        return out_arr, profile

    # 3. Align Coordinate Reference Systems (CRS) only when necessary
    if gdf.crs != crs and gdf.crs is not None and crs is not None:
        gdf = gdf.to_crs(crs)

    # 4. Fast Spatial Bounding Box Pre-Filter
    try:
        gdf = gdf.cx[bounds.left : bounds.right, bounds.bottom : bounds.top]
    except Exception:
        pass  # Fall back to full dataset if spatial index fails

    if gdf.empty:
        out_dtype = dtype or (np.float32 if isinstance(init_value, float) else np.int32)
        out_arr = np.full(out_shape, fill_value=init_value, dtype=out_dtype)
        profile.update(dtype=out_dtype, count=1)
        if output_raster_path:
            Path(output_raster_path).parent.mkdir(parents=True, exist_ok=True)
            with rio.open(output_raster_path, "w", **profile) as dst:
                dst.write(out_arr, 1)
        return out_arr, profile

    # 5. Determine Target Data Type and Construct Generator
    if attribute:
        if attribute not in gdf.columns:
            raise KeyError(f"Attribute column '{attribute}' not found in vector features.")

        # Infer type directly from Pandas dtype
        col_dtype = gdf[attribute].dtype
        if dtype:
            out_dtype = dtype
        elif np.issubdtype(col_dtype, np.floating):
            out_dtype = np.float32
        else:
            out_dtype = np.int32

        # Memory-efficient generator (streams tuples into C without allocating lists)
        shapes = (
            (geom, val)
            for geom, val in zip(gdf.geometry, gdf[attribute])
            if geom is not None and not geom.is_empty and pd.notnull(val)
        )
    elif burn_value is not None:
        out_dtype = dtype or (np.float32 if isinstance(burn_value, (float, np.floating)) else np.int32)
        shapes = ((geom, burn_value) for geom in gdf.geometry if geom is not None and not geom.is_empty)
    else:
        raise ValueError("Either 'attribute' or 'burn_value' must be specified.")

    # 6. Execute in-memory C-vectorized rasterization
    out_arr = rasterize(
        shapes=shapes,
        out_shape=out_shape,
        fill=init_value,
        transform=transform,
        all_touched=False,
        default_value=burn_value if burn_value is not None else 1,
        dtype=out_dtype,
    )

    # 7. Update profile metadata
    profile.update(dtype=out_dtype, count=1)

    # 8. Persist to disk if requested
    if output_raster_path:
        Path(output_raster_path).parent.mkdir(parents=True, exist_ok=True)
        with rio.open(output_raster_path, "w", **profile) as dst:
            dst.write(out_arr, 1)

    return out_arr, profile


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rasterize vector layer using template raster")
    parser.add_argument("-v", "--vector", required=True, help="Input vector path (.gpkg, .shp)")
    parser.add_argument("-t", "--template", required=True, help="Template raster path (.tif)")
    parser.add_argument("-o", "--output", required=True, help="Output raster path (.tif)")
    parser.add_argument("-a", "--attribute", required=False, help="Attribute column to burn")
    parser.add_argument("-b", "--burn-value", type=float, required=False, help="Fixed scalar burn value")
    parser.add_argument("-i", "--init-value", type=float, default=0, help="Initial background fill value")

    args = parser.parse_args()

    rasterize_vector(
        vector_path_or_gdf=args.vector,
        template_raster_path=args.template,
        output_raster_path=args.output,
        attribute=args.attribute,
        burn_value=args.burn_value,
        init_value=args.init_value,
    )
