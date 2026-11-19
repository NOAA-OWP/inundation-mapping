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


# We can use the file size to adjust GDAL Cache size on a file by file basis which can
# help performance quite a bit
def calculate_gdal_cache_mb(
    file_path: Path,
    min_mb: int = 128,
    max_mb: int = 512,
    expansion_factor: float = 1.0,
) -> int:
    """Estimate uncompressed raster footprint and clamp GDAL_CACHEMAX safely."""

    '''
    (2 meaning "n" value, number of mpiexec / taudem n jobs)
    • For a 5–15 MiB compressed HUC-8: 10 MiB × 2 = 20 MB → clamps up to 128 MB 
       (plenty to hold the entire raster + outputs in RAM).
    • For a 100 MiB compressed HUC-8: 100 MiB × 2 = 200 MB → allocates 200 MB.
    • For a 400+ MiB compressed HUC-8: 400 MiB × 2 = 800 MB → allocates 512 (max) MB
       (and caps at 512 MB for anything over 206 GiB compressed with 2 jobs).

    (n value of 1)
    • Anything below 256 MiB in size x 1 = min 128 MB 
    • For a 700 MiB (Alaska) in size x 1 = max 512
    '''

    compressed_mb = file_path.stat().st_size / (1024 * 1024)
    estimated_uncompressed_mb = int(compressed_mb * expansion_factor)
    return max(min_mb, min(max_mb, estimated_uncompressed_mb))