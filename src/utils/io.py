from pathlib import Path

import geopandas as gpd


def write_geodataframe(
    gdf: gpd.GeoDataFrame, filename: str | Path, *args, ignore_index: bool = False, **kwargs
) -> None:
    """Write a GeoDataFrame using the appropriate GeoPandas I/O method."""

    filepath = Path(filename) if isinstance(filename, (str, Path)) else None

    has_layer = "layer" in kwargs and kwargs["layer"] is not None

    driver_arg = kwargs.get("driver")
    driver = driver_arg.lower() if isinstance(driver_arg, str) else None

    ext = filepath.suffix.lower() if filepath else ""

    # Route 1: Parquet -> Hilbert sort + to_parquet()
    if not has_layer and (driver == "parquet" or (driver is None and ext == ".parquet")):

        EXCLUDED_PARQUET_KWARGS = {"driver", "layer", "schema", "mode", "engine", "overwrite"}

        parquet_kwargs = {k: v for k, v in kwargs.items() if k not in EXCLUDED_PARQUET_KWARGS}

        # Set GeoParquet & PyArrow writing options
        parquet_kwargs.setdefault("write_covering_bbox", True)
        parquet_kwargs.setdefault("compression", "zstd")
        parquet_kwargs.setdefault("write_page_index", True)
        parquet_kwargs.setdefault("write_page_checksum", True)

        if not gdf.empty and gdf.active_geometry_name is not None and not gdf.geometry.is_empty.all():
            # Sort spatially on Hilbert curve
            gdf = gdf.sort_values(by="geometry", ascending=True)

            if gdf.index.name is None:
                gdf = gdf.reset_index(drop=True)

        # Any write error is surfaced to the caller.
        gdf.to_parquet(filename, **parquet_kwargs)

    # Route 2: GeoPackage -> to_file()
    elif driver in ("gpkg", "geopackage") or (driver is None and ext == ".gpkg"):
        gpkg_kwargs = kwargs.copy()
        gpkg_kwargs.setdefault("driver", "GPKG")
        gpkg_kwargs.setdefault("engine", "fiona")

        gdf.to_file(filename, *args, **gpkg_kwargs)

    # Route 3: All other formats
    else:
        gdf.to_file(filename, *args, **kwargs)
