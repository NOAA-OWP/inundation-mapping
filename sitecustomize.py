import multiprocessing as mp
from pathlib import Path

import geopandas as gpd


# ---------------------------------------------------------------------------
# 1. PREVENT SEGFAULTS: Force clean process spawning for multiprocessing
# ---------------------------------------------------------------------------
try:
    # 'spawn' creates clean child processes without inheriting dangerous C-pointers
    mp.set_start_method("spawn", force=True)
except RuntimeError:
    pass

# Store original method references
_original_to_parquet = gpd.GeoDataFrame.to_parquet
_original_to_file = gpd.GeoDataFrame.to_file
# _original_read_file = gpd.read_file
# _original_read_parquet = gpd.read_parquet


# ---------------------------------------------------------------------------
# 2. Write Helpers
# ---------------------------------------------------------------------------
def _hilbert_sorted_to_parquet(self: gpd.GeoDataFrame, path, **kwargs) -> None:
    """Writes GeoParquet sorted natively by Hilbert curve via PyArrow."""
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
    """Smart writer: routes .parquet to PyArrow and .gpkg to fast C-vectorized Pyogrio."""
    filepath = Path(filename) if isinstance(filename, (str, Path)) else None

    has_layer = "layer" in kwargs and kwargs["layer"] is not None
    driver_arg = kwargs.get("driver")
    driver = driver_arg.lower() if isinstance(driver_arg, str) else None
    ext = filepath.suffix.lower() if filepath else ""

    # Route 1: Parquet -> PyArrow + Hilbert Sort
    if not has_layer and (driver == "parquet" or (driver is None and ext == ".parquet")):
        parquet_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ("driver", "layer", "schema", "mode", "engine", "overwrite")
        }

        try:
            _hilbert_sorted_to_parquet(self, filename, **parquet_kwargs)
        except Exception as e:
            print(f'ERROR: {e}\n')

    # Route 2: GeoPackage -> Fast Pyogrio Engine (Default in GeoPandas 1.x)
    elif driver in ("gpkg", "geopackage") or (driver is None and ext == ".gpkg"):
        gpkg_kwargs = kwargs.copy()
        gpkg_kwargs.setdefault("driver", "GPKG")
        # Uses pyogrio natively (no engine='fiona' fallback needed)
        _original_to_file(self, filename, *args, **gpkg_kwargs)

    # Route 3: Other Formats (.shp, .sqlite, etc.)
    else:
        _original_to_file(self, filename, *args, **kwargs)


# # ---------------------------------------------------------------------------
# # 3. Read Helpers
# # ---------------------------------------------------------------------------
# def _smart_read_file(filename, *args, **kwargs) -> gpd.GeoDataFrame:
#     """Smart reader: routes .parquet to read_parquet, and .gpkg to fast Pyogrio read_file."""
#     filepath = Path(filename) if isinstance(filename, (str, Path)) else None
#     ext = filepath.suffix.lower() if filepath else ""

#     driver_arg = kwargs.get("driver")
#     driver = driver_arg.lower() if isinstance(driver_arg, str) else None

#     if driver == "parquet" or (driver is None and ext == ".parquet"):
#         parquet_read_kwargs = {
#             k: v for k, v in kwargs.items()
#             if k not in ("driver", "layer", "engine", "rows", "bbox", "mask")
#         }
#         return _original_read_parquet(filename, **parquet_read_kwargs)

#     # Uses native Pyogrio reader (fastest)
#     return _original_read_file(filename, *args, **kwargs)


# ---------------------------------------------------------------------------
# 4. Global Overrides
# ---------------------------------------------------------------------------
gpd.GeoDataFrame.to_parquet = _hilbert_sorted_to_parquet
gpd.GeoDataFrame.to_file = _smart_to_file
# gpd.read_file = _smart_read_file
