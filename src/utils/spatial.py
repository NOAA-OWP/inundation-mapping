"""
src/utils/spatial.py

Optimized spatial analysis helper functions for GeoPandas and Shapely geometries.
Provides high-performance spatial join (sjoin), clipping, and bounding-box filtering.
"""

from typing import Optional, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import base


def sjoin(
    left_df: gpd.GeoDataFrame,
    right_df: gpd.GeoDataFrame,
    how: str = "inner",
    predicate: str = "intersects",
    lsuffix: str = "left",
    rsuffix: str = "right",
) -> gpd.GeoDataFrame:
    """Optimized wrapper around GeoPandas spatial join using R-tree spatial indexing.

    Ensures matching coordinate reference systems (CRS) before joining.
    """
    if left_df.empty or right_df.empty:
        return gpd.GeoDataFrame(columns=left_df.columns, crs=left_df.crs)

    # Ensure CRS alignment
    if left_df.crs != right_df.crs and left_df.crs is not None and right_df.crs is not None:
        right_df = right_df.to_crs(left_df.crs)

    # Perform spatial join using GeoPandas native sjoin
    joined = gpd.sjoin(left_df, right_df, how=how, predicate=predicate, lsuffix=lsuffix, rsuffix=rsuffix)

    return joined


def clip(
    gdf: gpd.GeoDataFrame,
    mask: Union[gpd.GeoDataFrame, gpd.GeoSeries, base.BaseGeometry],
    keep_geom_type: bool = False,
) -> gpd.GeoDataFrame:
    """Optimized spatial clipping of a GeoDataFrame by a masking geometry or layer.

    Filters bounding boxes first using spatial indexing prior to exact geometric intersection
    to maximize vector processing performance.
    """
    if gdf.empty:
        return gdf.copy()

    # Convert single Shapely geometry or GeoSeries to GeoDataFrame if necessary
    if isinstance(mask, base.BaseGeometry):
        mask_gdf = gpd.GeoDataFrame(geometry=[mask], crs=gdf.crs)
    elif isinstance(mask, gpd.GeoSeries):
        mask_gdf = gpd.GeoDataFrame(geometry=mask, crs=gdf.crs)
    else:
        mask_gdf = mask

    # Align CRS if necessary
    if gdf.crs != mask_gdf.crs and gdf.crs is not None and mask_gdf.crs is not None:
        mask_gdf = mask_gdf.to_crs(gdf.crs)

    # Use GeoPandas clip implementation
    clipped = gpd.clip(gdf, mask_gdf, keep_geom_type=keep_geom_type)

    return clipped


def bbox_intersects(gdf: gpd.GeoDataFrame, bbox: tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    """Quickly filters a GeoDataFrame to features whose bounding box intersects the given extent tuple (minx, miny, maxx, maxy)."""
    if gdf.empty:
        return gdf.copy()

    spatial_index = gdf.sindex
    if spatial_index is None:
        return gdf.cx[bbox[0] : bbox[2], bbox[1] : bbox[3]]

    possible_matches_index = list(spatial_index.intersection(bbox))
    return gdf.iloc[possible_matches_index].copy()
