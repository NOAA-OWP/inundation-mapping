#!/usr/bin/env python3

import os
import re
from os.path import join

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point
from shapely.ops import linemerge, substring


RIPPLE_DIR = "/outputs/"
RIPPLE_DOMAIN_GPKG = "ripple_domains.gpkg"
RIPPLE_WHITELIST_TABLE = "ripple_feature_list_20260310_huc_considered_delivered.csv"
RIPPLE_COLLECTIONS_DIR = "/outputs/nwm_ripple_streams/"

TARGET_CRS = "EPSG:5070"

# 20% of each domain's estimated average width
# This will be used for creating the domain buffer
# EDGE_TOLERANCE_WIDTH_FRACTION = 0.20
EDGE_TOLERANCE_M = 300

MIN_COMPONENT_AREA_FRACTION = 0.20

DOWNSTREAM_FRACTION = 0.50

HEADWATER_DOWNSTREAM_COVERAGE_THRESHOLD = 0.50
NOT_HEADWATER_COVERAGE_THRESHOLD = 0.60


def read_whitelist(ripple_dir, whitelist_file):
    whitelist_df = pd.read_csv(join(ripple_dir, whitelist_file), dtype={"huc": str})

    whitelist_df = whitelist_df[whitelist_df["is_valid_huc_considered"] == True].copy()

    whitelist_cols = ["feature_id", "huc", "model_id", "collection_id", "library_path"]

    return whitelist_df[whitelist_cols].reset_index(drop=True)


def create_collection_model_ids(whitelist_df):
    collection_model_ids = (
        whitelist_df[["collection_id", "model_id"]].drop_duplicates(keep="first").reset_index(drop=True)
    )

    collection_model_ids["model_indicator"] = collection_model_ids.apply(
        lambda row: f"/{row['collection_id']}/source_models/{row['model_id']}/", axis=1
    )

    return collection_model_ids


def create_whitelist_domain(ripple_dir, domain_gpkg, collection_model_ids):

    # Please note that all feature_ids in a single ripple model is on whitelist.
    # Because we excluded the whole model if there is one blacklisted FID in it.
    domain_gdf = gpd.read_file(join(ripple_dir, domain_gpkg))

    domain_gdf["model_indicator"] = domain_gdf["source_path"].str.extract(
        r"collections(\/.*\/source_models\/[^\/]+\/)"
    )

    # Finding the whitelist model domain using model_indicator
    domain_whitelist_gdf = domain_gdf.merge(collection_model_ids, on="model_indicator", how="inner")

    domain_whitelist_gdf = domain_whitelist_gdf.drop(columns=["model_name", "model_indicator"])

    domain_cols = ["model_id", "collection_id", "project_title", "version", "source_path", "geometry"]

    domain_whitelist_gdf = gpd.GeoDataFrame(
        domain_whitelist_gdf[domain_cols], geometry="geometry", crs=domain_gdf.crs
    )

    domain_whitelist_gdf["geometry"] = domain_whitelist_gdf["geometry"].make_valid()
    domain_whitelist_gdf["geometry"] = domain_whitelist_gdf["geometry"].buffer(0)

    return domain_whitelist_gdf


def read_ripple_streams(whitelist_df, ripple_collections_dir, collection_slice=None):
    collection_ids = sorted(
        whitelist_df["collection_id"].drop_duplicates(keep="first").reset_index(drop=True)
    )

    if collection_slice is not None:
        collection_ids = collection_ids[collection_slice]

    stream_gdfs = []

    for cid in collection_ids:
        huc = "".join(re.findall(r"\d", cid))

        stream_path = os.path.join(
            ripple_collections_dir, cid, f"ripple_reaches_order_sourcemodels_{huc}.gpkg"
        )
        # print(stream_path)

        if not os.path.exists(stream_path):
            print(f"{cid} does not have ripple_reaches_order_sourcemodels.gpkg")
            continue

        stream_gdf = gpd.read_file(stream_path)
        stream_gdf = stream_gdf[["feature_id", "order_", "nwm_to_id", "geometry"]]
        stream_gdfs.append(stream_gdf)

    if not stream_gdfs:
        return gpd.GeoDataFrame(columns=["feature_id", "order_", "nwm_to_id", "geometry"])

    streams_gdf = gpd.GeoDataFrame(
        pd.concat(stream_gdfs, ignore_index=True), geometry="geometry", crs=stream_gdfs[0].crs
    )

    return streams_gdf


def create_save_whitelist_streams(whitelist_df, streams_gdf, ripple_dir):
    whitelist_streams_df = whitelist_df.merge(streams_gdf, on="feature_id", how="left")

    whitelist_streams_gdf = gpd.GeoDataFrame(
        whitelist_streams_df.drop_duplicates(keep="first").reset_index(drop=True),
        geometry="geometry",
        crs=streams_gdf.crs,
    )

    whitelist_streams_gdf.to_file(join(ripple_dir, "whitelist_ripple_nwm_streams.gpkg"), driver="GPKG")
    # return whitelist_streams_gdf


# Break a Polygon, MultiPolygon, or geometry collection into individual polygon pieces
def polygon_components(geom):
    if geom is None or geom.is_empty:
        return []

    if geom.geom_type == "Polygon":
        return [geom]

    if geom.geom_type == "MultiPolygon":
        return list(geom.geoms)

    if hasattr(geom, "geoms"):
        components = []

        for part in geom.geoms:
            components.extend(polygon_components(part))

        return components

    return []


# Pick the largest polygon component by area
def main_domain_component(geom):
    components = polygon_components(geom)

    if not components:
        return None

    return max(components, key=lambda component: component.area)


# Group domains by collection_id + model_id; union each model’s geometry,
# keep only the largest connected polygon, and record diagnostics.
# Create main river polygon + small disconnected island polygon
def keep_main_collection_domain_components(domain_whitelist_gdf):  # , min_component_area_fraction
    main_domain_rows = []

    for _, group in domain_whitelist_gdf.groupby("collection_id"):  # DOMAIN_GROUP_COLS
        group_geometry = group.geometry.union_all()
        components = polygon_components(group_geometry)

        if not components:
            continue

        main_geometry = max(components, key=lambda component: component.area)
        main_area_m2 = main_geometry.area

        retained_components = [
            component
            for component in components
            if component.area >= main_area_m2 * MIN_COMPONENT_AREA_FRACTION
        ]

        retained_geometry = gpd.GeoSeries(retained_components, crs=domain_whitelist_gdf.crs).union_all()

        original_area_m2 = group_geometry.area
        retained_area_m2 = retained_geometry.area

        row = group.iloc[0].copy()
        row["geometry"] = retained_geometry
        row["domain_component_count"] = len(components)
        row["retained_domain_component_count"] = len(retained_components)
        row["disconnected_domain_area_m2"] = max(original_area_m2 - retained_area_m2, 0.0)
        row["main_domain_area_fraction"] = main_area_m2 / original_area_m2 if original_area_m2 > 0 else 0.0
        row["retained_domain_area_fraction"] = (
            retained_area_m2 / original_area_m2 if original_area_m2 > 0 else 0.0
        )

        main_domain_rows.append(row)

    if not main_domain_rows:
        return gpd.GeoDataFrame(
            columns=[
                *domain_whitelist_gdf.columns,
                "domain_component_count",
                "retained_domain_component_count",
                "disconnected_domain_area_m2",
                "main_domain_area_fraction",
                "retained_domain_area_fraction",
            ],
            geometry="geometry",
            crs=domain_whitelist_gdf.crs,
        )

    return gpd.GeoDataFrame(main_domain_rows, geometry="geometry", crs=domain_whitelist_gdf.crs).reset_index(
        drop=True
    )


def create_save_whitelist_merged_domain(domain_whitelist_gdf, ripple_dir):

    domain_whitelist_gdf = domain_whitelist_gdf.to_crs(TARGET_CRS).copy()
    # Main river polygon + small disconnected island polygon
    domain_whitelist_gdf = keep_main_collection_domain_components(domain_whitelist_gdf)

    domain_whitelist_gdf.to_file(
        join(ripple_dir, "whitelist_ripple_model_domain_main_component.gpkg"), driver="GPKG"
    )

    disconnected_domain_count = (domain_whitelist_gdf["domain_component_count"] > 1).sum()
    disconnected_area_m2 = domain_whitelist_gdf["disconnected_domain_area_m2"].sum()

    print(
        "Removed disconnected domain components from "
        f"{disconnected_domain_count} model domains "
        f"({disconnected_area_m2:.1f} square meters)."
    )

    # domain_whitelist_gdf["domain_width_m"] = domain_whitelist_gdf.geometry.apply(
    #     estimate_ripple_domain_width_m
    # )
    # domain_whitelist_gdf["edge_tolerance_m"] = (
    #     domain_whitelist_gdf["domain_width_m"] * EDGE_TOLERANCE_WIDTH_FRACTION
    # )
    # domain_whitelist_gdf["geometry_buffered"] = domain_whitelist_gdf.apply(
    #     lambda row: row.geometry.buffer(row["edge_tolerance_m"]),
    #     axis=1,
    # )
    domain_whitelist_gdf["edge_tolerance_m"] = EDGE_TOLERANCE_M

    domain_whitelist_gdf["geometry_buffered"] = domain_whitelist_gdf.geometry.buffer(EDGE_TOLERANCE_M)

    domain_union = domain_whitelist_gdf.geometry.union_all()
    domain_union_buffered = domain_whitelist_gdf["geometry_buffered"].union_all()

    merged_domain_whitelist_gdf = gpd.GeoDataFrame(
        geometry=[domain_union], crs=domain_whitelist_gdf.crs
    ).reset_index(drop=True)

    merged_domain_whitelist_gdf["geometry_buffered"] = domain_union_buffered

    merged_domain_whitelist_gdf.drop(columns=["geometry_buffered"]).to_file(
        join(ripple_dir, "merged_domain_whitelist.gpkg"), driver="GPKG"
    )

    merged_domain_buffered_gdf = gpd.GeoDataFrame(
        geometry=[domain_union_buffered], crs=domain_whitelist_gdf.crs
    )
    merged_domain_buffered_gdf.to_file(
        join(ripple_dir, "merged_domain_whitelist_buffered.gpkg"), driver="GPKG"
    )

    return merged_domain_whitelist_gdf


def as_single_linestring(geom):
    if geom is None or geom.is_empty:
        return None

    if geom.geom_type == "LineString":
        return geom

    if geom.geom_type == "MultiLineString":
        merged = linemerge(geom)

        if merged.geom_type == "LineString":
            return merged

        return max(merged.geoms, key=lambda part: part.length)

    return None


def topology_bridge_mask(candidates, included_col="included", max_bridge_reaches=3):

    included_ids = set(candidates.loc[candidates[included_col], "feature_id"].dropna())

    downstream_by_feature_id = (
        candidates.dropna(subset=["feature_id"])
        .drop_duplicates(subset="feature_id")
        .set_index("feature_id")["nwm_to_id"]
        .to_dict()
    )

    candidate_ids = set(candidates["feature_id"].dropna())
    bridge_ids = set()

    for upstream_included_id in included_ids:
        gap_ids = []
        seen_ids = {upstream_included_id}
        current_id = downstream_by_feature_id.get(upstream_included_id)

        for _ in range(max_bridge_reaches):
            if pd.isna(current_id) or current_id not in candidate_ids:
                break

            if current_id in seen_ids:
                break

            seen_ids.add(current_id)

            if current_id in included_ids:
                break

            gap_ids.append(current_id)
            next_id = downstream_by_feature_id.get(current_id)

            if pd.isna(next_id):
                break

            if next_id in included_ids:
                bridge_ids.update(gap_ids)
                break

            current_id = next_id

    return (~candidates[included_col]) & candidates["feature_id"].isin(bridge_ids)


def downstream_domain_metrics(geom, domain_union_buffered):  # domain_union

    line = as_single_linestring(geom)

    if line is None or line.length == 0:
        return pd.Series(
            {
                "stream_length_m": 0.0,
                "downstream_tail_length_m": 0.0,
                "downstream_tail_covered_length_m": 0.0,
                "downstream_tail_frac_inside_buffered": 0.0,
                "downstream_endpoint_covered_buffered": False,
                "inside_length_m_buffered": 0.0,
                "frac_inside_buffered": 0.0,
            }
        )

    stream_length_m = line.length

    # headwaters at least 50% of downstream tail of stream is inside the domain
    # and downstream_endpoint covered by the domain.
    downstream_start_m = stream_length_m * (1 - DOWNSTREAM_FRACTION)
    downstream_tail = substring(line, downstream_start_m, stream_length_m)

    downstream_tail_length_m = downstream_tail.length
    downstream_tail_covered_length_m = downstream_tail.intersection(domain_union_buffered).length

    downstream_tail_frac_inside_buffered = (
        downstream_tail_covered_length_m / downstream_tail_length_m if downstream_tail_length_m > 0 else 0.0
    )

    downstream_endpoint = Point(line.coords[-1])
    downstream_endpoint_covered_buffered = domain_union_buffered.covers(downstream_endpoint)

    # For not-headwater streams, at least 60% of stream is inside the domain
    # and downstream_endpoint covered by the domain.
    inside_geom_buffered = line.intersection(domain_union_buffered)
    inside_length_m_buffered = inside_geom_buffered.length

    frac_inside_buffered = inside_length_m_buffered / stream_length_m if stream_length_m > 0 else 0.0

    return pd.Series(
        {
            "stream_length_m": stream_length_m,
            "downstream_tail_length_m": downstream_tail_length_m,
            "downstream_tail_covered_length_m": downstream_tail_covered_length_m,
            "downstream_tail_frac_inside_buffered": downstream_tail_frac_inside_buffered,
            "downstream_endpoint_covered_buffered": downstream_endpoint_covered_buffered,
            "inside_length_m_buffered": inside_length_m_buffered,
            "frac_inside_buffered": frac_inside_buffered,
        }
    )


def select_valid_streams(streams_gdf, merged_domain_whitelist_gdf):

    merged_domain_whitelist_gdf = merged_domain_whitelist_gdf.to_crs(TARGET_CRS)
    streams_gdf = streams_gdf.to_crs(TARGET_CRS)

    # Streams that are completely whitin the whitelist ripple domain
    streams_within_gdf = gpd.sjoin(
        streams_gdf, merged_domain_whitelist_gdf, how="inner", predicate="within"
    ).drop(columns=["index_right"])

    streams_within_gdf.to_file(join(RIPPLE_DIR, "white_streams_within.gpkg"), driver="GPKG")

    within_feature_ids = set(streams_within_gdf["feature_id"])
    within_count = len(within_feature_ids)

    # domain_union = merged_domain_whitelist_gdf.geometry.iloc[0]
    domain_union_buffered = merged_domain_whitelist_gdf["geometry_buffered"].iloc[0]
    # domain_union_buffered = domain_union.buffer(EDGE_TOLERANCE_M)

    candidates = gpd.sjoin(
        streams_gdf, merged_domain_whitelist_gdf, how="inner", predicate="intersects"
    ).drop(columns=["index_right"])

    candidates = candidates.drop_duplicates(subset="feature_id").reset_index(drop=True)

    metrics = candidates.geometry.apply(lambda geom: downstream_domain_metrics(geom, domain_union_buffered))

    candidates_metrix_df = pd.concat([candidates, metrics], axis=1)

    downstream_target_ids = set(streams_gdf["nwm_to_id"].dropna())
    candidates_metrix_df["headwater_stream"] = ~candidates_metrix_df["feature_id"].isin(downstream_target_ids)
    candidates_metrix_df["not_headwater_stream"] = ~candidates_metrix_df["headwater_stream"]

    candidates_metrix_df["strictly_within_domain"] = candidates_metrix_df["feature_id"].isin(
        within_feature_ids
    )

    candidates_metrix_df["headwater_downstream_buffered_covered"] = (
        (candidates_metrix_df["headwater_stream"])
        & (
            candidates_metrix_df["downstream_tail_frac_inside_buffered"]
            >= HEADWATER_DOWNSTREAM_COVERAGE_THRESHOLD
        )
        & candidates_metrix_df["downstream_endpoint_covered_buffered"]
    )

    candidates_metrix_df["not_headwater_buffered_covered"] = (
        (candidates_metrix_df["not_headwater_stream"])
        & (
            candidates_metrix_df["frac_inside_buffered"]
            >= NOT_HEADWATER_COVERAGE_THRESHOLD
            # candidates_metrix_df["downstream_tail_frac_inside_buffered"]
            # >= HEADWATER_DOWNSTREAM_COVERAGE_THRESHOLD
        )
        & candidates_metrix_df["downstream_endpoint_covered_buffered"]
    )

    candidates_metrix_df["included"] = (
        candidates_metrix_df["strictly_within_domain"]
        | candidates_metrix_df["headwater_downstream_buffered_covered"]
        | candidates_metrix_df["not_headwater_buffered_covered"]
    )

    candidates_metrix_df["topology_bridge"] = topology_bridge_mask(candidates_metrix_df)

    candidates_metrix_df["included"] = (
        candidates_metrix_df["included"] | candidates_metrix_df["topology_bridge"]
    )

    candidates_metrix_df["included_by"] = np.select(
        [
            candidates_metrix_df["strictly_within_domain"],
            candidates_metrix_df["headwater_downstream_buffered_covered"],
            candidates_metrix_df["not_headwater_buffered_covered"],
            candidates_metrix_df["topology_bridge"],
        ],
        [
            "within",
            ("headwater_downstream_buffered_covered" f">={HEADWATER_DOWNSTREAM_COVERAGE_THRESHOLD:.0%}"),
            ("not_headwater_buffered_covered" f">={NOT_HEADWATER_COVERAGE_THRESHOLD:.0%}"),
            "topology_bridge_between_included_reaches",
        ],
        default="excluded",
    )

    included_streams_gdf = gpd.GeoDataFrame(
        candidates_metrix_df[candidates_metrix_df["included"]].copy(),
        geometry="geometry",
        crs=candidates_metrix_df.crs,
    )

    return included_streams_gdf, candidates_metrix_df


def save_outputs(included_streams_gdf, candidates_metrix_df, ripple_dir):

    whitelist_df = read_whitelist(RIPPLE_DIR, RIPPLE_WHITELIST_TABLE)

    collection_model_ids = create_collection_model_ids(whitelist_df)

    domain_whitelist_gdf = create_whitelist_domain(
        RIPPLE_DIR,
        RIPPLE_DOMAIN_GPKG,
        collection_model_ids,
    )

    domain_whitelist_gdf.to_file(
        join(RIPPLE_DIR, "whitelist_ripple_model_domain.gpkg"),
        driver="GPKG",
    )

    streams_gdf = read_ripple_streams(
        whitelist_df,
        RIPPLE_COLLECTIONS_DIR,
        collection_slice=None # slice(92, 94),
    )
    streams_gdf.to_file(
        join(RIPPLE_DIR, "all_nwm_streams.gpkg"),
        driver="GPKG",
    )

    create_save_whitelist_streams(whitelist_df, streams_gdf, RIPPLE_DIR)

    merged_domain_whitelist_gdf = create_save_whitelist_merged_domain(
        domain_whitelist_gdf,
        RIPPLE_DIR,
    )

    included_streams_gdf, candidates_metrix_df = select_valid_streams(
        streams_gdf, merged_domain_whitelist_gdf
    )

    included_streams_gdf = included_streams_gdf.drop(
        columns=[
            "geometry_buffered",
            "stream_length_m",
            "downstream_tail_length_m",
            "downstream_tail_covered_length_m",
            "downstream_tail_frac_inside_buffered",
            "downstream_endpoint_covered_buffered",
            "inside_length_m_buffered",
            "frac_inside_buffered",
            "strictly_within_domain",
            "headwater_stream",
            "not_headwater_stream",
            "headwater_downstream_buffered_covered",
            "not_headwater_buffered_covered",
            "topology_bridge",
            "included",
        ],
        # errors="ignore",
    )
    included_streams_gdf.to_file(
        join(RIPPLE_DIR, "nwm_streams_WITHIN_DOWNSTREAM_GAP_whitelisted_rippledomain_union.gpkg"),
        driver="GPKG",
    )

    print(
        f"candidates_metrix_df intersecting: {candidates_metrix_df['feature_id'].nunique()}, "
        f"'within' count: {within_count}, "
        f"included by headwater downstream rule: "
        f"{candidates_metrix_df['headwater_downstream_buffered_covered'].sum()}, "
        f"included by not-headwater coverage rule: "
        f"{candidates_metrix_df['not_headwater_buffered_covered'].sum()}, "
        f"included by topology bridge: {candidates_metrix_df['topology_bridge'].sum()}, "
        f"total included: {included_streams_gdf['feature_id'].nunique()}"
    )

    # candidates_metrix_df[diagnostic_cols].sort_values("feature_id").to_csv(
    #     join(
    #         ripple_dir,
    #         "nwm_streams_WITHIN_OR_DOWNSTREAM_whitelisted_rippledomain_union.csv",
    #     ),
    #     index=False,
    # )

    # included_feature_ids = included_streams_gdf["feature_id"]

    # gap_df = whitelist_df[
    #     ~whitelist_df["feature_id"].isin(included_feature_ids)
    # ].copy()

    # gap_df.to_csv(
    #     join(ripple_dir, "whitelist_ripple_nwm_streams_GAP_within_or_downstream.csv"),
    #     index=False,
    # )


def main():
    

    save_outputs(included_streams_gdf, candidates_metrix_df, whitelist_df, RIPPLE_DIR)


if __name__ == "__main__":
    main()
