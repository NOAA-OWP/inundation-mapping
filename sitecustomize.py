from pathlib import Path

import geopandas as gpd


_original_to_parquet = gpd.GeoDataFrame.to_parquet
_original_to_file = gpd.GeoDataFrame.to_file


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
    """Follows native GeoPandas priority, determining driver from file extension when 'layer' is given."""
    filepath = Path(filename) if isinstance(filename, (str, Path)) else None

    # 1. Check if a layer argument is present
    has_layer = "layer" in kwargs and kwargs["layer"] is not None

    # 2. Extract and normalize driver if explicitly provided
    driver_arg = kwargs.get("driver")
    driver = driver_arg.lower() if isinstance(driver_arg, str) else None

    # 3. Extract file extension
    ext = filepath.suffix.lower() if filepath else ""

    # --- ROUTE 1: Parquet (ONLY when no 'layer' is passed and driver/extension requests Parquet) ---
    if not has_layer and (driver == "parquet" or (driver is None and ext == ".parquet")):
        parquet_kwargs = {
            k: v for k, v in kwargs.items() if k not in ("driver", "layer", "schema", "mode", "engine")
        }
        _hilbert_sorted_to_parquet(self, filename, **parquet_kwargs)

    # --- ROUTE 2: GeoPackage (.gpkg extension OR explicit driver="gpkg") ---
    elif driver in ("gpkg", "geopackage") or (driver is None and ext == ".gpkg"):
        gpkg_kwargs = kwargs.copy()
        gpkg_kwargs.setdefault("driver", "GPKG")
        gpkg_kwargs.setdefault("engine", "fiona")
        _original_to_file(self, filename, *args, **gpkg_kwargs)

    # --- ROUTE 3: All other extensions/drivers (e.g., .sqlite, .gdb, .shp) ---
    else:
        _original_to_file(self, filename, *args, **kwargs)


# Patch globally on Python startup
gpd.GeoDataFrame.to_parquet = _hilbert_sorted_to_parquet
gpd.GeoDataFrame.to_file = _smart_to_file
