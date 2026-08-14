from pathlib import Path

import geopandas as gpd
import pandas as pd


def write_geodataframe(gdf: gpd.GeoDataFrame, filename: str | Path, *args, **kwargs) -> None:
    """Write a GeoDataFrame using the appropriate GeoPandas I/O method."""
    filepath = Path(filename) if isinstance(filename, (str, Path)) else None

    has_layer = "layer" in kwargs and kwargs["layer"] is not None

    driver_arg = kwargs.get("driver")
    driver = driver_arg.lower() if isinstance(driver_arg, str) else None

    ext = filepath.suffix.lower() if filepath else ""

    # ROUTE 1: Parquet (ONLY when no 'layer' is passed and format requests Parquet)
    if not has_layer and (driver == "parquet" or (driver is None and ext == ".parquet")):
        EXCLUDED_PARQUET_KWARGS = {"driver", "layer", "schema", "mode", "engine", "overwrite"}
        parquet_kwargs = {k: v for k, v in kwargs.items() if k not in EXCLUDED_PARQUET_KWARGS}

        # Set GeoParquet & PyArrow writing options
        parquet_kwargs.setdefault("write_covering_bbox", True)
        parquet_kwargs.setdefault("compression", "zstd")
        parquet_kwargs.setdefault("write_page_index", True)
        parquet_kwargs.setdefault("write_page_checksum", True)

        if not gdf.empty and gdf.active_geometry_name is not None and not gdf.geometry.is_empty.all():
            is_default_index = isinstance(gdf.index, pd.RangeIndex)

            gdf = gdf.sort_values(by="geometry", ascending=True)

            if is_default_index and kwargs.get("index") is not True:
                gdf = gdf.reset_index(drop=True)

        gdf.to_parquet(filename, **parquet_kwargs)

    # ROUTE 2: All Vector File Formats (GPKG, Shapefile, GeoJSON, GDB, etc.)
    else:
        out_kwargs = kwargs.copy()

        # If writing a GeoPackage, apply Fiona defaults
        if driver in ("gpkg", "geopackage") or (driver is None and ext == ".gpkg"):
            out_kwargs.setdefault("engine", "fiona")

        gdf.to_file(filename, *args, **out_kwargs)
