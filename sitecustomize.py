from pathlib import Path

import geopandas as gpd


# Store original references to prevent infinite recursion
_original_to_parquet = gpd.GeoDataFrame.to_parquet
_original_to_file = gpd.GeoDataFrame.to_file
_original_read_file = gpd.read_file
_original_read_parquet = gpd.read_parquet


# ---------------------------------------------------------------------------
# Write Helpers
# ---------------------------------------------------------------------------
def _hilbert_sorted_to_parquet(self: gpd.GeoDataFrame, path, **kwargs) -> None:
    """Writes GeoParquet sorted natively by Hilbert curve."""
    has_valid_geometry = False

    try:
        if not self.empty and self.geometry is not None and not self.geometry.is_empty.all():
            has_valid_geometry = True
    except (AttributeError, ValueError):
        has_valid_geometry = False

    if has_valid_geometry:
        self = self.sort_values(by="geometry", ascending=True).reset_index(drop=True)

    _original_to_parquet(self, path, **kwargs)


def _smart_to_file(self: gpd.GeoDataFrame, filename, *args, **kwargs) -> None:
    """Smart writer: follows native GeoPandas priority, determining driver from extension when 'layer' is given."""
    filepath = Path(filename) if isinstance(filename, (str, Path)) else None

    # 1. Check if a layer argument is present
    has_layer = "layer" in kwargs and kwargs["layer"] is not None

    # 2. Extract and normalize driver if explicitly provided
    driver_arg = kwargs.get("driver")
    driver = driver_arg.lower() if isinstance(driver_arg, str) else None

    # 3. Extract file extension
    ext = filepath.suffix.lower() if filepath else ""

    # Route 1: Parquet (ONLY when no 'layer' is passed and driver/extension requests Parquet)
    if not has_layer and (driver == "parquet" or (driver is None and ext == ".parquet")):
        parquet_kwargs = {
            k: v for k, v in kwargs.items() if k not in ("driver", "layer", "schema", "mode", "engine")
        }
        _hilbert_sorted_to_parquet(self, filename, **parquet_kwargs)

    # Route 2: GeoPackage (.gpkg extension OR explicit driver="gpkg")
    elif driver in ("gpkg", "geopackage") or (driver is None and ext == ".gpkg"):
        gpkg_kwargs = kwargs.copy()
        gpkg_kwargs.setdefault("driver", "GPKG")
        gpkg_kwargs.setdefault("engine", "fiona")
        _original_to_file(self, filename, *args, **gpkg_kwargs)

    # Route 3: All other extensions/drivers (.sqlite, .gdb, .shp)
    else:
        _original_to_file(self, filename, *args, **kwargs)


# ---------------------------------------------------------------------------
# Read Helpers
# ---------------------------------------------------------------------------
def _smart_read_file(filename, *args, **kwargs) -> gpd.GeoDataFrame:
    """Smart reader: transparently routes .parquet extension paths to gpd.read_parquet()."""
    filepath = Path(filename) if isinstance(filename, (str, Path)) else None
    ext = filepath.suffix.lower() if filepath else ""

    # Check for explicit driver argument if passed to read_file
    driver_arg = kwargs.get("driver")
    driver = driver_arg.lower() if isinstance(driver_arg, str) else None

    # Route 1: Parquet file -> Use read_parquet
    if driver == "parquet" or (driver is None and ext == ".parquet"):
        # Strip GDAL/Fiona specific kwargs that read_parquet doesn't accept
        parquet_read_kwargs = {
            k: v for k, v in kwargs.items() if k not in ("driver", "layer", "engine", "rows", "bbox", "mask")
        }
        return _original_read_parquet(filename, **parquet_read_kwargs)

    # Route 2: All other spatial formats -> Standard gpd.read_file()
    return _original_read_file(filename, *args, **kwargs)


# ---------------------------------------------------------------------------
# Global Overrides (Interpreter Boot)
# ---------------------------------------------------------------------------
gpd.GeoDataFrame.to_parquet = _hilbert_sorted_to_parquet
gpd.GeoDataFrame.to_file = _smart_to_file
gpd.read_file = _smart_read_file
