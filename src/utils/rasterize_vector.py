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


gpd.options.io_engine = "pyogrio"


def rasterize_vector(
    vector_path_or_gdf: Union[str, Path, gpd.GeoDataFrame],
    template_raster_path: str,
    output_raster_path: str = None,
    attribute: str = None,
    burn_value: Union[int, float] = None,
    init_value: Union[int, float] = 0,
    nodata: Union[int, float] = None,
    dtype: np.dtype = None,
) -> tuple[np.ndarray, dict]:
    """
    Rasterizes vector geometries (GeoDataFrame or file) to match a template raster's profile.

    Parameters
    ----------
    vector_path_or_gdf : Union[str, Path, gpd.GeoDataFrame]
        Input vector source.
    template_raster_path : str
        Path to template raster providing spatial dimensions, transform, CRS, and bounds.
    output_raster_path : str, optional
        Path to save generated GeoTIFF on disk.
    attribute : str, optional
        Vector column name to burn into raster cells (e.g., 'HydroID').
    burn_value : Union[int, float], optional
        Constant scalar burn value if attribute column is not specified.
    init_value : Union[int, float], default 0
        Initial background fill value used during array initialization.
    nodata : Union[int, float], optional
        NoData value registered in the raster metadata. Defaults to `init_value` if None.
    dtype : np.dtype, optional
        Target numpy array and GDAL band data type (e.g., np.int32, np.float32).

    Returns
    -------
    tuple[np.ndarray, dict]
        The output raster array and rasterio metadata profile.
    """

    # Register metadata NoData tag (defaulting to init_value if not explicitly set)
    nodata_val = nodata if nodata is not None else init_value

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

    # Auto-select HydroID_Index if attribute and burn_value are omitted
    if attribute is None and burn_value is None:
        if "HydroID_Index" in gdf.columns:
            attribute = "HydroID_Index"
        elif "HydroID" in gdf.columns:
            attribute = "HydroID"

    # Handle empty GeoDataFrames early
    if gdf.empty:
        out_dtype = dtype or (
            np.float32
            if isinstance(init_value, float)
            else (np.int16 if attribute == "HydroID_Index" else np.int32)
        )
        out_arr = np.full(out_shape, fill_value=init_value, dtype=out_dtype)
        profile.update(dtype=out_dtype, count=1, nodata=nodata_val)
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
        out_dtype = dtype or (
            np.float32
            if isinstance(init_value, float)
            else (np.int16 if attribute == "HydroID_Index" else np.int32)
        )
        out_arr = np.full(out_shape, fill_value=init_value, dtype=out_dtype)
        profile.update(dtype=out_dtype, count=1, nodata=nodata_val)
        if output_raster_path:
            Path(output_raster_path).parent.mkdir(parents=True, exist_ok=True)
            with rio.open(output_raster_path, "w", **profile) as dst:
                dst.write(out_arr, 1)
        return out_arr, profile

    # 5. Determine Target Data Type and Construct Generator
    if attribute:
        if attribute not in gdf.columns:
            raise KeyError(f"Attribute column '{attribute}' not found in vector features.")

        col_dtype = gdf[attribute].dtype

        # Explicit dtype parameter takes highest precedence
        if dtype:
            out_dtype = dtype
        elif attribute == "HydroID_Index":
            out_dtype = np.int16
        elif np.issubdtype(col_dtype, np.floating):
            out_dtype = np.float32
        else:
            out_dtype = np.int32

        # Stream geometry/attribute tuples
        shapes = (
            (geom, int(val) if np.issubdtype(out_dtype, np.integer) else val)
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

    # 7. Update profile metadata with explicit data type and NoData registration
    profile.update(dtype=out_dtype, count=1, nodata=nodata_val)

    # 8. Persist to disk if requested
    if output_raster_path:
        Path(output_raster_path).parent.mkdir(parents=True, exist_ok=True)
        with rio.open(output_raster_path, "w", **profile) as dst:
            dst.write(out_arr, 1)

    return out_arr, profile


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rasterize vector layer using template raster")
    parser.add_argument("-v", "--vector", required=True, help="Input vector path (.gpkg, .parquet, .shp)")
    parser.add_argument("-t", "--template", required=True, help="Template raster path (.tif)")
    parser.add_argument("-o", "--output", required=True, help="Output raster path (.tif)")
    parser.add_argument(
        "-a", "--attribute", required=False, default=None, help="Attribute column to burn (e.g., HydroID)"
    )
    parser.add_argument("-b", "--burn-value", type=float, required=False, help="Fixed scalar burn value")
    parser.add_argument("-i", "--init-value", type=float, default=0, help="Initial background fill value")
    parser.add_argument(
        "-n",
        "--nodata",
        type=float,
        required=False,
        default=None,
        help="Explicit metadata NoData value (defaults to init-value if unspecified)",
    )
    parser.add_argument(
        "-d",
        "--dtype",
        required=False,
        default=None,
        help="Explicit output numpy dtype (e.g., int16, int32, float32)",
    )

    args = parser.parse_args()

    target_dtype = np.dtype(args.dtype) if args.dtype else None

    rasterize_vector(
        vector_path_or_gdf=args.vector,
        template_raster_path=args.template,
        output_raster_path=args.output,
        attribute=args.attribute,
        burn_value=args.burn_value,
        init_value=args.init_value,
        nodata=args.nodata,
        dtype=target_dtype,
    )
