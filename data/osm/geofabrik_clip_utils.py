"""
Shared per-state-parquet-to-HUC clipping helpers used by both data/roads/make_osm_roads_per_huc.py
and data/bridges/make_osm_bridges_per_huc.py. Not domain-specific to either roads or bridges —
kept here in data/osm/ (alongside pull_osm.py, which produces the per-state parquets these
functions operate on) rather than in either domain folder, so neither domain script depends
on the other.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
from shapely.geometry import box


def compute_state_parquet_bounds_4326(parquet_files: List[Path]) -> dict[Path, tuple]:
    """Load each state parquet once and return its bounding box in EPSG:4326, skipping empty files."""
    bounds: dict[Path, tuple] = {}
    for pq_path in parquet_files:
        gdf_tmp = gpd.read_parquet(pq_path)
        if not gdf_tmp.empty:
            bounds[pq_path] = tuple(gdf_tmp.to_crs("EPSG:4326").total_bounds)
    return bounds


def find_overlapping_parquets(wbd_path: Path, parquet_bounds_4326: dict[Path, tuple]) -> List[Path]:
    """Return the state parquet paths whose EPSG:4326 bbox intersects the HUC's wbd boundary bbox."""
    huc_bounds = gpd.read_file(wbd_path).to_crs("EPSG:4326").total_bounds
    huc_box = box(*huc_bounds)
    return [pq_path for pq_path, pb in parquet_bounds_4326.items() if box(*pb).intersects(huc_box)]


def clip_state_parquets_to_huc(
    parquet_paths: List[Path], huc_gdf: gpd.GeoDataFrame, dedupe_col: str | None = "osmid"
) -> gpd.GeoDataFrame | None:
    """
    Read each state parquet, clip to the HUC boundary (reprojecting the boundary to
    match each parquet's CRS), concatenate the results, and drop duplicate OSM ways
    that appear in more than one state parquet at a border. Returns None if nothing
    intersects the HUC.
    """
    all_parts: List[gpd.GeoDataFrame] = []
    for pq_path in parquet_paths:
        gdf = gpd.read_parquet(pq_path)
        if gdf.empty:
            continue

        huc_in_crs = huc_gdf.to_crs(gdf.crs)

        # Coarse cx[] filter before the more expensive gpd.clip call.
        minx, miny, maxx, maxy = huc_in_crs.total_bounds
        gdf = gdf.cx[minx:maxx, miny:maxy]
        if gdf.empty:
            continue

        clipped = gpd.clip(gdf, huc_in_crs, keep_geom_type=True)
        if not clipped.empty:
            all_parts.append(clipped)

    if not all_parts:
        return None

    merged = gpd.GeoDataFrame(
        pd.concat(all_parts, ignore_index=True), geometry="geometry", crs=all_parts[0].crs
    )

    if dedupe_col and dedupe_col in merged.columns:
        merged = merged.drop_duplicates(subset=[dedupe_col])

    return merged
