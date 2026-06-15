#!/usr/bin/env python3

import argparse
import os
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from math import ceil
from os.path import join

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point
from shapely.ops import linemerge, substring


TARGET_CRS = "EPSG:5070"

# EDGE_TOLERANCE_WIDTH_FRACTION = 0.20
EDGE_TOLERANCE_M = 300

# # Keep the largest 90% of each collection's disconnected domain components by area.
# RETAIN_COMPONENT_COUNT_FRACTION = 1.00  # 0.90
# Keep only those disconnected domain components that cover more than 1 NWM stream.
MAX_STREAMS_FOR_COMPONENT_EXCLUSION = 1  # 2

# At least 50% of downstream tail of headwaters stream is inside the domain.
# and downstream_endpoint covered by the domain.
DOWNSTREAM_FRACTION = 0.50
HEADWATER_DOWNSTREAM_COVERAGE_THRESHOLD = 0.50
# At least 60% of middle stream is inside the domain and downstream_endpoint covered by the domain.
NOT_HEADWATER_COVERAGE_THRESHOLD = 0.60

_WORKER_DOMAIN_UNION_BUFFERED = None

WHITELIST_COLS = [
    "huc",
    "feature_id",
    "nwm_to_id",
    "order_",
    "collection_id",
    "model_id",
    "db_path",
    "is_blacklisted",
    "is_bridge",
    "huc_valid",
    "is_valid",
]  # , "library_path"


ripple_whitelist_table = 'ripple_feature_id_whitelist_conus.csv'
ripple_dir = '/outputs/'
ripple_domain_gpkg = 'ripple_domains.gpkg'
ripple_collections_dir = '/outputs/test_blacklist_metrics/output_metrics_codex_final_dupHuc/'


# -----------------------------------------------------------------------------
def create_huc_validated_whitelist(ripple_dir, ripple_whitelist_table, whitelist_cols):

    whitelist_df = pd.read_csv(join(ripple_dir, ripple_whitelist_table), dtype={"huc": str})

    # Detect valid duplicate feature-ids across more than one HUC.
    valid_huc_counts = whitelist_df[whitelist_df["is_valid"]].groupby("feature_id")["huc"].nunique()
    whitelist_df["duplicate_valid"] = whitelist_df["is_valid"] & (
        whitelist_df["feature_id"].map(valid_huc_counts).fillna(0) > 1
    )

    hucs_df = whitelist_df.loc[whitelist_df["duplicate_valid"], "huc"].drop_duplicates()

    # Read nwm_stream_gpkg
    src_dir = os.getenv('srcDir')
    load_dotenv(os.path.join(src_dir, 'bash_variables.env'))
    pre_clip_huc_dir = os.getenv('pre_clip_huc_dir')

    nwms_dfs = []
    for huc in hucs_df:
        nwm_stream_gpkg = os.path.join(pre_clip_huc_dir, huc, 'nwm_subset_streams.gpkg')
        if os.path.isfile(nwm_stream_gpkg):
            nwms_gdf = gpd.read_file(nwm_stream_gpkg)  # , dtype={"huc": str}
            nwms_gdf = nwms_gdf.rename(columns={"ID": "feature_id"})
            nwms_dfs.append(nwms_gdf[["feature_id"]].drop_duplicates().assign(huc=str(huc), in_huc=True))

    whitelist_df["huc_valid"] = whitelist_df["is_valid"]

    if nwms_dfs:
        combined_ht = pd.concat(nwms_dfs, ignore_index=True).drop_duplicates(["feature_id", "huc"])

        merged_df = whitelist_df.merge(combined_ht, on=["feature_id", "huc"], how="left")

        in_huc = merged_df["in_huc"].eq(True)

        whitelist_df.loc[merged_df["duplicate_valid"] & ~in_huc, "huc_valid"] = False

    whitelist_df["is_valid_original"] = whitelist_df["is_valid"]
    whitelist_df["is_valid"] = whitelist_df["huc_valid"]

    cols = [col for col in whitelist_df.columns if col != "is_valid"] + ["is_valid"]
    whitelist_df = whitelist_df[cols]

    if 'huc' in whitelist_df.columns:
        whitelist_df['huc'] = whitelist_df['huc'].astype('string')
    today = date.today().strftime("%Y%m%d")
    whitelist_df.to_csv(join(ripple_dir, f'ripple_feature_list_{today}.csv'), index=False)

    whitelist_df_complete = whitelist_df[whitelist_cols].copy()
    whitelist_df = whitelist_df[whitelist_df["is_valid"] == True].copy()

    return whitelist_df[whitelist_cols].reset_index(drop=True), whitelist_df_complete


# -----------------------------------------------------------------------------
def create_collection_model_ids(whitelist_df):
    collection_model_ids = (
        whitelist_df[["collection_id", "model_id"]].drop_duplicates(keep="first").reset_index(drop=True)
    )

    collection_model_ids["model_indicator"] = collection_model_ids.apply(
        lambda row: f"/{row['collection_id']}/source_models/{row['model_id']}/", axis=1
    )

    return collection_model_ids


# -----------------------------------------------------------------------------
def create_whitelist_domain(ripple_dir, ripple_domain_gpkg, collection_model_ids):

    # Please note that all feature_ids in a single ripple model is on whitelist.
    # Because we excluded the whole model if there is one blacklisted FID in it.
    domain_gdf = gpd.read_file(join(ripple_dir, ripple_domain_gpkg))

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

    today = date.today().strftime("%Y%m%d")
    domain_whitelist_gdf.to_file(
        join(ripple_dir, f"whitelist_ripple_model_domain_{today}.gpkg"), driver="GPKG"
    )

    return domain_whitelist_gdf


# -----------------------------------------------------------------------------
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
        stream_cols = ["feature_id", "order_", "nwm_to_id", "geometry"]
        stream_gdf = stream_gdf[stream_cols]
        stream_gdf["collection_id"] = cid
        stream_gdfs.append(stream_gdf)

    if not stream_gdfs:
        return gpd.GeoDataFrame(columns=["feature_id", "order_", "nwm_to_id", "collection_id", "geometry"])

    streams_gdf = gpd.GeoDataFrame(
        pd.concat(stream_gdfs, ignore_index=True), geometry="geometry", crs=stream_gdfs[0].crs
    )

    return streams_gdf


# -----------------------------------------------------------------------------
def create_save_whitelist_streams(whitelist_df, streams_gdf, ripple_dir):
    whitelist_streams_df = whitelist_df.merge(streams_gdf, on="feature_id", how="left")

    whitelist_streams_gdf = gpd.GeoDataFrame(
        whitelist_streams_df.drop_duplicates(keep="first").reset_index(drop=True),
        geometry="geometry",
        crs=streams_gdf.crs,
    )

    today = date.today().strftime("%Y%m%d")
    whitelist_streams_gdf.to_file(
        join(ripple_dir, f"whitelist_ripple_nwm_streams_{today}.gpkg"), driver="GPKG"
    )
    # return whitelist_streams_gdf


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Pick the largest polygon component by area
def main_domain_component(geom):
    components = polygon_components(geom)

    if not components:
        return None

    return max(components, key=lambda component: component.area)


# -----------------------------------------------------------------------------
def component_intersecting_feature_count(component, streams_gdf):
    if streams_gdf.empty:
        return 0

    intersecting_streams = streams_gdf[streams_gdf.intersects(component)]

    return intersecting_streams["feature_id"].nunique()


# -----------------------------------------------------------------------------
# Pick the polygon component that at least includes 2 streams
def exclude_components_with_few_streams(components, streams_gdf):
    retained_components = []
    excluded_component_count_by_stream_rule = 0

    for component in components:
        intersecting_feature_count = component_intersecting_feature_count(component, streams_gdf)

        if intersecting_feature_count <= MAX_STREAMS_FOR_COMPONENT_EXCLUSION:
            excluded_component_count_by_stream_rule += 1
            continue

        retained_components.append(component)

    return retained_components, excluded_component_count_by_stream_rule


# -----------------------------------------------------------------------------
# Group domains by collection_id; union each collection's geometry,
# keep the components covering at least 2 streams, and record diagnostics.
def keep_main_collection_domain_components(domain_whitelist_gdf, streams_gdf):

    main_domain_rows = []

    # for cid, group in domain_debug.groupby("collection_id", dropna=False):
    for _, group in domain_whitelist_gdf.groupby("collection_id"):  # DOMAIN_GROUP_COLS
        group_geometry = group.geometry.union_all()
        components = polygon_components(group_geometry)
        #     print(
        #         cid,
        #         "rows:", len(group),
        #         "union type:", group_geometry.geom_type,
        #         "is empty:", group_geometry.is_empty,
        #         "components:", len(components),
        #         "areas:", sorted([c.area for c in components], reverse=True)[:5],
        #     )

        if not components:
            # print(
            #     f"WARNING: collection_id={group['collection_id'].iloc[0]} "
            #     f"has no polygon components after union. "
            #     f"geom_type={group_geometry.geom_type}, is_empty={group_geometry.is_empty}"
            # )
            continue

        components_by_area = sorted(components, key=lambda component: component.area, reverse=True)
        ## Keep the largest 90% of each collection's disconnected domain components by area.
        # retained_component_count = max(1, ceil(len(components_by_area) * RETAIN_COMPONENT_COUNT_FRACTION))

        main_geometry = components_by_area[0]
        main_area_m2 = main_geometry.area

        # retained_components = components_by_area[:retained_component_count
        retained_components = components_by_area

        # Keep those disconnected domain components that cover at least 2 NWM stream.
        collection_id = group["collection_id"].iloc[0]
        collection_streams_gdf = streams_gdf[streams_gdf["collection_id"] == collection_id]

        retained_components, stream_rule_excluded_count = exclude_components_with_few_streams(
            retained_components, collection_streams_gdf
        )

        retained_geometry = gpd.GeoSeries(retained_components, crs=domain_whitelist_gdf.crs).union_all()

        original_area_m2 = group_geometry.area
        retained_area_m2 = retained_geometry.area

        excluded_component_count = len(components) - len(retained_components)

        print(
            f"collection_id={collection_id} "
            f"excluded {stream_rule_excluded_count} retained domain components "
            f"because they intersected <= {MAX_STREAMS_FOR_COMPONENT_EXCLUSION} feature_ids."
        )

        row = group.iloc[0].copy()
        row["geometry"] = retained_geometry
        row["domain_component_count"] = len(components)
        row["retained_domain_component_count"] = len(retained_components)
        row["excluded_domain_component_count"] = excluded_component_count
        row["disconnected_domain_area_m2"] = max(original_area_m2 - retained_area_m2, 0.0)
        row["main_domain_area_fraction"] = main_area_m2 / original_area_m2 if original_area_m2 > 0 else 0.0
        row["retained_domain_area_fraction"] = (
            retained_area_m2 / original_area_m2 if original_area_m2 > 0 else 0.0
        )
        row["stream_rule_excluded_component_count"] = stream_rule_excluded_count

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


# -----------------------------------------------------------------------------
def create_save_whitelist_merged_domain(domain_whitelist_gdf, streams_gdf, ripple_dir):

    domain_whitelist_gdf = domain_whitelist_gdf.to_crs(TARGET_CRS).copy()
    streams_gdf = streams_gdf.to_crs(TARGET_CRS).copy()
    # Main river polygon + small disconnected island polygon
    domain_whitelist_gdf = keep_main_collection_domain_components(domain_whitelist_gdf, streams_gdf)

    today = date.today().strftime("%Y%m%d")
    domain_whitelist_gdf.to_file(
        join(ripple_dir, f"whitelist_ripple_model_domain_main_component_{today}.gpkg"), driver="GPKG"
    )

    disconnected_domain_count = (domain_whitelist_gdf["domain_component_count"] > 1).sum()
    disconnected_area_m2 = domain_whitelist_gdf["disconnected_domain_area_m2"].sum()

    print(
        "Removed disconnected domain components from "
        f"{disconnected_domain_count} model domains "
        f"({disconnected_area_m2:.1f} square meters)."
    )

    domain_whitelist_gdf["edge_tolerance_m"] = EDGE_TOLERANCE_M

    domain_whitelist_gdf["geometry_buffered"] = domain_whitelist_gdf.geometry.buffer(EDGE_TOLERANCE_M)

    domain_union = domain_whitelist_gdf.geometry.union_all()
    domain_union_buffered = domain_whitelist_gdf["geometry_buffered"].union_all()

    merged_domain_whitelist_gdf = gpd.GeoDataFrame(
        geometry=[domain_union], crs=domain_whitelist_gdf.crs
    ).reset_index(drop=True)

    merged_domain_whitelist_gdf["geometry_buffered"] = domain_union_buffered

    merged_domain_whitelist_gdf.drop(columns=["geometry_buffered"]).to_file(
        join(ripple_dir, f"merged_domain_whitelist_2streams0h_{today}.gpkg"), driver="GPKG"
    )

    merged_domain_buffered_gdf = gpd.GeoDataFrame(
        geometry=[domain_union_buffered], crs=domain_whitelist_gdf.crs
    )
    merged_domain_buffered_gdf.to_file(
        join(ripple_dir, f"merged_domain_whitelist_buffered_2streams0h_{today}.gpkg"), driver="GPKG"
    )

    return merged_domain_whitelist_gdf


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
def _init_domain_metrics_worker(domain_union_buffered):
    global _WORKER_DOMAIN_UNION_BUFFERED
    _WORKER_DOMAIN_UNION_BUFFERED = domain_union_buffered


# -----------------------------------------------------------------------------
def _downstream_domain_metrics_worker(geom):
    return downstream_domain_metrics(geom, _WORKER_DOMAIN_UNION_BUFFERED).to_dict()


# -----------------------------------------------------------------------------
def compute_downstream_domain_metrics_parallel(geometries, domain_union_buffered, n_workers, chunksize):
    geometries = list(geometries)

    if n_workers is None:
        n_workers = max((os.cpu_count() or 2) - 1, 1)

    if n_workers <= 1 or len(geometries) == 0:
        records = [downstream_domain_metrics(geom, domain_union_buffered).to_dict() for geom in geometries]
    else:
        with ProcessPoolExecutor(
            max_workers=n_workers, initializer=_init_domain_metrics_worker, initargs=(domain_union_buffered,)
        ) as executor:
            records = list(executor.map(_downstream_domain_metrics_worker, geometries, chunksize=chunksize))

    return pd.DataFrame.from_records(records)


# -----------------------------------------------------------------------------
def select_valid_streams(streams_gdf, merged_domain_whitelist_2streams0h_gdf, n_workers, chunksize):

    merged_domain_whitelist_2streams0h_gdf = merged_domain_whitelist_2streams0h_gdf.to_crs(TARGET_CRS)
    streams_gdf = streams_gdf.to_crs(TARGET_CRS)

    # Streams that are completely within the whitelist ripple domain
    streams_within_gdf = gpd.sjoin(
        streams_gdf, merged_domain_whitelist_2streams0h_gdf, how="inner", predicate="within"
    ).drop(columns=["index_right"])

    within_feature_ids = set(streams_within_gdf["feature_id"])
    within_count = len(within_feature_ids)

    domain_union_buffered = merged_domain_whitelist_2streams0h_gdf["geometry_buffered"].iloc[0]

    # Candidate whitelisted streams that have any intersection
    # with domain components covers at least 2 streams (2streams0h)
    candidates = gpd.sjoin(
        streams_gdf, merged_domain_whitelist_2streams0h_gdf, how="inner", predicate="intersects"
    ).drop(columns=["index_right"])

    candidates = candidates.drop_duplicates(subset="feature_id").reset_index(drop=True)

    print(
        "Processing stream-domain metrics for collection_ids:",
        sorted(candidates["collection_id"].dropna().unique()),
    )

    # Compute downstream_domain_metrics of any whitelisted domain components
    metrics = compute_downstream_domain_metrics_parallel(
        candidates.geometry, domain_union_buffered, n_workers=n_workers, chunksize=chunksize
    )

    candidates_metrics_df = pd.concat([candidates, metrics], axis=1)

    downstream_target_ids = set(streams_gdf["nwm_to_id"].dropna())
    candidates_metrics_df["headwater_stream"] = ~candidates_metrics_df["feature_id"].isin(
        downstream_target_ids
    )
    candidates_metrics_df["not_headwater_stream"] = ~candidates_metrics_df["headwater_stream"]

    candidates_metrics_df["strictly_within_domain"] = candidates_metrics_df["feature_id"].isin(
        within_feature_ids
    )

    candidates_metrics_df["headwater_downstream_buffered_covered"] = (
        candidates_metrics_df["headwater_stream"]
        & (
            candidates_metrics_df["downstream_tail_frac_inside_buffered"]
            >= HEADWATER_DOWNSTREAM_COVERAGE_THRESHOLD
        )
        & candidates_metrics_df["downstream_endpoint_covered_buffered"]
    )

    candidates_metrics_df["not_headwater_buffered_covered"] = (
        candidates_metrics_df["not_headwater_stream"]
        & (candidates_metrics_df["frac_inside_buffered"] >= NOT_HEADWATER_COVERAGE_THRESHOLD)
        & candidates_metrics_df["downstream_endpoint_covered_buffered"]
    )

    candidates_metrics_df["included"] = (
        candidates_metrics_df["strictly_within_domain"]
        | candidates_metrics_df["headwater_downstream_buffered_covered"]
        | candidates_metrics_df["not_headwater_buffered_covered"]
    )

    candidates_metrics_df["topology_bridge"] = topology_bridge_mask(candidates_metrics_df)

    candidates_metrics_df["included"] = (
        candidates_metrics_df["included"] | candidates_metrics_df["topology_bridge"]
    )

    candidates_metrics_df["included_by"] = np.select(
        [
            candidates_metrics_df["strictly_within_domain"],
            candidates_metrics_df["headwater_downstream_buffered_covered"],
            candidates_metrics_df["not_headwater_buffered_covered"],
            candidates_metrics_df["topology_bridge"],
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
        candidates_metrics_df[candidates_metrics_df["included"]].copy(),
        geometry="geometry",
        crs=candidates_metrics_df.crs,
    )

    return included_streams_gdf, candidates_metrics_df, within_count


# -----------------------------------------------------------------------------
def process_streams_save_outputs(
    ripple_dir, ripple_whitelist_table, ripple_domain_gpkg, ripple_collections_dir, n_workers, chunksize
):
    whitelist_cols = WHITELIST_COLS
    whitelist_df, whitelist_df_complete = create_huc_validated_whitelist(
        ripple_dir, ripple_whitelist_table, whitelist_cols
    )

    collection_model_ids = create_collection_model_ids(whitelist_df)

    domain_whitelist_gdf = create_whitelist_domain(ripple_dir, ripple_domain_gpkg, collection_model_ids)

    today = date.today().strftime("%Y%m%d")
    domain_whitelist_gdf.to_file(
        join(ripple_dir, f"whitelist_ripple_model_domain_{today}.gpkg"), driver="GPKG"
    )

    streams_gdf = read_ripple_streams(
        whitelist_df, ripple_collections_dir, collection_slice=None  # slice(92, 94),
    )

    create_save_whitelist_streams(whitelist_df, streams_gdf, ripple_dir)

    merged_domain_whitelist_2streams0h_gdf = create_save_whitelist_merged_domain(
        domain_whitelist_gdf, streams_gdf, ripple_dir
    )

    included_streams_gdf, candidates_metrics_df, within_count = select_valid_streams(
        streams_gdf, merged_domain_whitelist_2streams0h_gdf, n_workers=n_workers, chunksize=chunksize
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
        errors="ignore",
    )
    included_streams_gdf.to_file(
        join(ripple_dir, f"whitelisted_nwm_streams_within_downstreamCovered_GapExcl_{today}.gpkg"),
        driver="GPKG",
    )
    included_streams_gdf.sort_values("feature_id").drop(columns=["geometry"]).to_csv(
        join(ripple_dir, f"whitelisted_nwm_streams_within_downstreamCovered_GapExcl_{today}.csv"), index=False
    )

    print(
        f"Total number of the streams intersecting the whitelist domain: {candidates_metrics_df['feature_id'].nunique()}, "
        f"'within' count: {within_count}, "
        f"included by headwater downstream rule: "
        f"{candidates_metrics_df['headwater_downstream_buffered_covered'].sum()}, "
        f"included by not-headwater coverage rule: "
        f"{candidates_metrics_df['not_headwater_buffered_covered'].sum()}, "
        f"included by topology bridge: {candidates_metrics_df['topology_bridge'].sum()}, "
        f"total included: {included_streams_gdf['feature_id'].nunique()}"
    )

    whitelist_original_to_merge = whitelist_df_complete[whitelist_cols]
    included_streams_gdf["is_gap"] = False
    on_cols = ["feature_id", "order_", "nwm_to_id", "collection_id"]
    whitelist_final_df = whitelist_original_to_merge.merge(
        included_streams_gdf.drop(columns=["geometry"]), on=on_cols, how="left"
    )
    whitelist_final_df.loc[whitelist_final_df["is_gap"].ne(False).fillna(True), "is_gap"] = True
    whitelist_final_df.loc[whitelist_final_df["is_gap"].eq(True), "is_valid"] = False
    whitelist_final_df = whitelist_final_df[
        [col for col in whitelist_final_df.columns if col != "is_valid"] + ["is_valid"]
    ]
    whitelist_final_df.sort_values("feature_id").to_csv(
        join(ripple_dir, f"whitelist_ripple_feature_ids_{today}.csv"), index=False
    )
    # included_feature_ids = included_streams_gdf["feature_id"]
    # gap_df = whitelist_df[~whitelist_df["feature_id"].isin(included_feature_ids)].copy()
    # gap_df.to_csv(join(ripple_dir, "whitelist_ripple_nwm_streams_GAP_excluded.csv"), index=False)


if __name__ == "__main__":

    """
    Examples of usage:

    # RIPPLE_DIR = "/outputs/"
    # RIPPLE_DOMAIN_GPKG = "ripple_domains.gpkg"
    # RIPPLE_WHITELIST_TABLE = "ripple_feature_list_20260310_huc_considered_delivered.csv"
    # RIPPLE_COLLECTIONS_DIR = "/outputs/nwm_ripple_streams/" or "/data/ripple/ripple_20260211_merged/ripple_metrics/"

    python data/ripple/remove_blacklisted_streams_and_ripple_model_domain_gaps.py \
        -rd /outputs/ \
        -dg ripple_domains.gpkg \
        -wl ripple_feature_list_20260310_huc_considered_delivered.csv \
        -rc /outputs/nwm_ripple_streams/ \
        -j 8 \
        -cs 500

    """

    parser = argparse.ArgumentParser(
        description="Remove blacklisted streams and identify valid Ripple streams using domain coverage rules."
    )
    parser.add_argument("-rd", "--ripple-dir", required=True, type=str, help="Ripple output directory")
    parser.add_argument(
        "-wl",
        "--ripple-whitelist-table",
        required=True,
        type=str,
        help=(
            "A CSV file containing a list of all NWM/Ripple streams maked as whitelist/blacklist."
            "should be saved in the ripple_dir"
        ),
    )
    parser.add_argument(
        "-dg",
        "--ripple-domain-gpkg",
        required=True,
        type=str,
        help="ripple_domain_gpkg; should be saved in the ripple_dir",
    )
    parser.add_argument(
        "-rc",
        "--ripple-collections-dir",
        required=True,
        type=str,
        help="ripple_collections_dir contains ripple_reaches_order_sourcemodels_huc.gpkg",
    )
    parser.add_argument(
        "-j",
        "--n-workers",
        type=int,
        default=None,
        help=(
            "Number of worker processes for downstream domain metrics. "
            "Default: CPU count minus 1. Use 1 to disable multiprocessing."
        ),
    )
    parser.add_argument(
        "-cs",
        "--chunksize",
        type=int,
        default=100,
        help="Number of geometries sent to each worker per batch. Default: 100.",
    )

    args = vars(parser.parse_args())

    ripple_dir = args["ripple_dir"]
    ripple_whitelist_table = args["ripple_whitelist_table"]
    ripple_domain_gpkg = args["ripple_domain_gpkg"]
    ripple_collections_dir = args["ripple_collections_dir"]
    n_workers = args["n_workers"]
    chunksize = args["chunksize"]

    process_streams_save_outputs(
        ripple_dir, ripple_whitelist_table, ripple_domain_gpkg, ripple_collections_dir, n_workers, chunksize
    )
