#!/usr/bin/env python3

import argparse
import os
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from math import ceil
from os.path import join

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point
from shapely.ops import linemerge, substring


TARGET_CRS = "EPSG:5070"
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")

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

DOMAIN_COLS = ["collection_id", "model_id", "project_title", "version"]

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
    "library_path",
    "is_library_path_valid",
    "is_duplicated",
    "huc_valid",
    "is_valid",
]

ripple_whitelist_table = 'ripple_feature_list_20260721.csv'
ripple_metrics_dir = '/outputs/blacklist_metrics_test_20260720/'
ripple_analysis_dir = '/outputs/blacklist_metrics_test_20260720/'
ripple_domain_gpkg = 'ripple_domains.gpkg'


# -----------------------------------------------------------------------------
def create_huc_validated_whitelist(ripple_analysis_dir, ripple_whitelist_table, whitelist_cols):

    whitelist_df = pd.read_csv(join(ripple_analysis_dir, ripple_whitelist_table), dtype={"huc": str})

    invalid_non_bridge_conds = (
        (whitelist_df["is_valid"] == True)
        & (whitelist_df["is_bridge"] == False)
        & (whitelist_df["is_library_path_valid"] == False)
    )
    whitelist_df.loc[invalid_non_bridge_conds, "is_valid"] = False

    # Detect valid duplicate feature-ids across more than one HUC
    valid_huc_counts = whitelist_df[whitelist_df["is_valid"]].groupby("feature_id")["huc"].nunique()
    whitelist_df["is_duplicated"] = whitelist_df["is_valid"] & (
        whitelist_df["feature_id"].map(valid_huc_counts).fillna(0) > 1
    )

    hucs_df = whitelist_df.loc[whitelist_df["is_duplicated"], "huc"].drop_duplicates()

    # Read nwm_stream_gpkg to validate HUCs
    src_dir = os.getenv('srcDir')
    load_dotenv(os.path.join(src_dir, 'bash_variables.env'))
    pre_clip_huc_dir = os.getenv('pre_clip_huc_dir')

    nwms_dfs = []
    for huc in hucs_df:
        nwm_stream_gpkg = os.path.join(pre_clip_huc_dir, huc, 'nwm_subset_streams.gpkg')
        wbd_gpkg = os.path.join(pre_clip_huc_dir, huc, 'wbd.gpkg')

        if os.path.isfile(nwm_stream_gpkg):
            nwms_gdf = gpd.read_file(nwm_stream_gpkg)  # , dtype={"huc": str}
            nwms_gdf = nwms_gdf.rename(columns={"ID": "feature_id"})

            nwms_gdf = nwms_gdf[["feature_id", "geometry"]].drop_duplicates("feature_id")
            nwms_gdf["huc"] = str(huc)
            nwms_gdf["in_huc"] = True

            if os.path.isfile(wbd_gpkg):
                wbd_gdf = gpd.read_file(wbd_gpkg)

                if nwms_gdf.crs != TARGET_CRS:
                    nwms_projected = nwms_gdf.to_crs(TARGET_CRS)
                else:
                    nwms_projected = nwms_gdf.copy()

                if wbd_gdf.crs != TARGET_CRS:
                    wbd_projected = wbd_gdf.to_crs(TARGET_CRS)
                else:
                    wbd_projected = wbd_gdf.copy()

                huc_boundary = wbd_projected.geometry.make_valid().union_all()

                nwms_projected["huc_overlap_m"] = nwms_projected.geometry.apply(
                    lambda geom: (
                        geom.intersection(huc_boundary).length
                        if geom is not None and not geom.is_empty
                        else 0.0
                    )
                )

                nwms_gdf["huc_overlap_m"] = nwms_projected["huc_overlap_m"].values
            else:
                nwms_gdf["huc_overlap_m"] = np.nan

            nwms_dfs.append(nwms_gdf[["feature_id", "huc", "in_huc", "huc_overlap_m"]])

    whitelist_df["huc_valid"] = whitelist_df["is_valid"]

    if nwms_dfs:
        combined_ht = pd.concat(nwms_dfs, ignore_index=True).drop_duplicates(["feature_id", "huc"])

        merged_df = whitelist_df.merge(combined_ht, on=["feature_id", "huc"], how="left")

        in_huc = merged_df["in_huc"].eq(True)

        whitelist_df.loc[merged_df["is_duplicated"] & ~in_huc, "huc_valid"] = False

        # For feature_ids still valid in multiple HUCs, keep only the HUC whose WBD
        # contains the largest stream length.
        merged_df["huc_valid_after_nwm_check"] = whitelist_df["huc_valid"].values

        duplicated_after_nwm_check = (
            merged_df["is_valid"]
            & merged_df["huc_valid_after_nwm_check"]
            & (
                merged_df[merged_df["is_valid"] & merged_df["huc_valid_after_nwm_check"]]
                .groupby("feature_id")["huc"]
                .transform("nunique")
                > 1
            )
        )

        overlap_candidates = merged_df[duplicated_after_nwm_check].copy()

        if not overlap_candidates.empty:
            overlap_candidates["huc_overlap_m"] = overlap_candidates["huc_overlap_m"].fillna(0.0)

            best_huc_idx = overlap_candidates.groupby("feature_id")["huc_overlap_m"].idxmax()

            keep_pairs = set(
                zip(
                    overlap_candidates.loc[best_huc_idx, "feature_id"],
                    overlap_candidates.loc[best_huc_idx, "huc"],
                )
            )

            remove_by_wbd = duplicated_after_nwm_check & ~merged_df.apply(
                lambda row: (row["feature_id"], row["huc"]) in keep_pairs, axis=1
            )

            whitelist_df.loc[remove_by_wbd, "huc_valid"] = False

    whitelist_df["is_valid_original"] = whitelist_df["is_valid"]
    whitelist_df["is_valid"] = whitelist_df["huc_valid"]

    cols = [col for col in whitelist_df.columns if col != "is_valid"] + ["is_valid"]
    whitelist_df = whitelist_df[cols]

    if 'huc' in whitelist_df.columns:
        whitelist_df['huc'] = whitelist_df['huc'].astype('string')
    today = RUN_TIMESTAMP
    whitelist_df.to_csv(
        join(ripple_analysis_dir, f'ripple_feature_ids_whitelist_pregap_{today}.csv'), index=False
    )

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
def create_whitelist_domain(ripple_analysis_dir, ripple_domain_gpkg, collection_model_ids):

    # Please note that all feature_ids in a single ripple model is on whitelist.
    # Because we excluded the whole model if there is one blacklisted FID in it.
    domain_gdf = gpd.read_file(join(ripple_analysis_dir, ripple_domain_gpkg))

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

    today = RUN_TIMESTAMP
    domain_whitelist_gdf.to_file(
        join(ripple_analysis_dir, f"whitelist_ripple_model_domain_pregap_{today}.gpkg"), driver="GPKG"
    )

    return domain_whitelist_gdf


# -----------------------------------------------------------------------------
def read_ripple_streams(whitelist_df, ripple_metrics_dir, collection_slice=None):

    collection_ids = sorted(
        whitelist_df["collection_id"].drop_duplicates(keep="first").reset_index(drop=True)
    )

    if collection_slice is not None:
        collection_ids = collection_ids[collection_slice]

    stream_gdfs = []

    for cid in collection_ids:
        huc = "".join(re.findall(r"\d", cid))

        stream_path = os.path.join(ripple_metrics_dir, cid, f"ripple_reaches_order_sourcemodels_{huc}.gpkg")
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
def create_save_whitelist_streams(whitelist_df, streams_gdf, ripple_analysis_dir):
    whitelist_streams_df = whitelist_df.merge(streams_gdf, on="feature_id", how="left")

    whitelist_streams_gdf = gpd.GeoDataFrame(
        whitelist_streams_df.drop_duplicates(keep="first").reset_index(drop=True),
        geometry="geometry",
        crs=streams_gdf.crs,
    )

    today = RUN_TIMESTAMP
    whitelist_streams_gdf.to_file(
        join(ripple_analysis_dir, f"whitelist_ripple_nwm_streams_pregap_{today}.gpkg"), driver="GPKG"
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
def create_save_whitelist_merged_domain(domain_whitelist_gdf, streams_gdf, ripple_analysis_dir):

    domain_whitelist_gdf = domain_whitelist_gdf.to_crs(TARGET_CRS).copy()
    streams_gdf = streams_gdf.to_crs(TARGET_CRS).copy()
    # Main river polygon + small disconnected island polygon
    domain_whitelist_gdf = keep_main_collection_domain_components(domain_whitelist_gdf, streams_gdf)

    today = RUN_TIMESTAMP
    domain_whitelist_gdf.to_file(
        join(ripple_analysis_dir, f"whitelist_ripple_model_domain_main_component_{today}.gpkg"), driver="GPKG"
    )
    domain_whitelist_gdf[[*DOMAIN_COLS, "geometry"]].to_file(
        join(ripple_analysis_dir, f"whitelist_ripple_model_domain_2streams0h_postgap_{today}.gpkg"),
        driver="GPKG",
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
        join(ripple_analysis_dir, f"merged_domain_whitelist_2streams0h_{today}.gpkg"), driver="GPKG"
    )

    merged_domain_buffered_gdf = gpd.GeoDataFrame(
        geometry=[domain_union_buffered], crs=domain_whitelist_gdf.crs
    )
    merged_domain_buffered_gdf.to_file(
        join(ripple_analysis_dir, f"merged_domain_whitelist_buffered_2streams0h_{today}.gpkg"), driver="GPKG"
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
def select_valid_streams(
    streams_gdf, merged_domain_whitelist_2streams0h_gdf, whitelist_feature_ids, n_workers, chunksize
):

    merged_domain_whitelist_2streams0h_gdf = merged_domain_whitelist_2streams0h_gdf.to_crs(TARGET_CRS)
    streams_gdf = streams_gdf.to_crs(TARGET_CRS)
    whitelist_streams_gdf = streams_gdf[streams_gdf["feature_id"].isin(whitelist_feature_ids)].copy()

    # Streams that are completely within the whitelist ripple domain
    streams_within_gdf = gpd.sjoin(
        whitelist_streams_gdf, merged_domain_whitelist_2streams0h_gdf, how="inner", predicate="within"
    ).drop(columns=["index_right"])

    within_feature_ids = set(streams_within_gdf["feature_id"])
    within_count = len(within_feature_ids)

    domain_union_buffered = merged_domain_whitelist_2streams0h_gdf["geometry_buffered"].iloc[0]

    # Candidate whitelisted streams that have any intersection
    # with domain components covers at least 2 streams (2streams0h)
    candidates = gpd.sjoin(
        whitelist_streams_gdf, merged_domain_whitelist_2streams0h_gdf, how="inner", predicate="intersects"
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
def select_fully_overlapping_domain_polygons(domain_whitelist_gdf, merged_domain_whitelist_2streams0h_gdf):
    """Restore source-domain rows and attributes to the retained merged geometry."""

    if domain_whitelist_gdf.crs is None or merged_domain_whitelist_2streams0h_gdf.crs is None:
        raise ValueError("Both domain GeoDataFrames must have a defined CRS.")

    domain_for_overlay = domain_whitelist_gdf.to_crs(merged_domain_whitelist_2streams0h_gdf.crs).copy()
    domain_for_overlay["geometry"] = domain_for_overlay.geometry.make_valid()

    merged_geometries = merged_domain_whitelist_2streams0h_gdf.geometry.dropna().make_valid()

    if merged_geometries.empty:
        whitelist_domain_final_gdf = domain_whitelist_gdf.iloc[0:0].copy()
    else:
        merged_union_gdf = gpd.GeoDataFrame(
            geometry=[merged_geometries.union_all()], crs=merged_domain_whitelist_2streams0h_gdf.crs
        )

        whitelist_domain_final_gdf = gpd.overlay(
            domain_for_overlay, merged_union_gdf, how="intersection", keep_geom_type=True
        )
        whitelist_domain_final_gdf = whitelist_domain_final_gdf.to_crs(domain_whitelist_gdf.crs).reset_index(
            drop=True
        )

    today = RUN_TIMESTAMP
    whitelist_domain_final_gdf.to_file(
        join(ripple_analysis_dir, f"whitelist_domain_final_{today}.gpkg"), driver="GPKG"
    )

    return whitelist_domain_final_gdf


# -----------------------------------------------------------------------------
def flag_too_long_streams(
    whitelist_final_df,
    streams_gdf,
    whitelist_domain_final_gdf,
    candidates_metrics_df,
    coverage_threshold=0.95,
):
    """Flag valid streams covered collectively, but not individually, by multiple models."""

    if not 0 < coverage_threshold <= 1:
        raise ValueError("coverage_threshold must be greater than 0 and no greater than 1.")

    required_whitelist_columns = {"feature_id", "is_bridge", "is_valid"}
    required_stream_columns = {"feature_id", "geometry"}
    required_domain_columns = {"collection_id", "model_id", "geometry"}
    required_metrics_columns = {"feature_id", "included_by", "not_headwater_stream", "topology_bridge"}
    missing_whitelist_columns = required_whitelist_columns.difference(whitelist_final_df.columns)
    missing_stream_columns = required_stream_columns.difference(streams_gdf.columns)
    missing_domain_columns = required_domain_columns.difference(whitelist_domain_final_gdf.columns)
    missing_metrics_columns = required_metrics_columns.difference(candidates_metrics_df.columns)

    if missing_whitelist_columns:
        raise ValueError(f"whitelist_final_df is missing columns: {sorted(missing_whitelist_columns)}")
    if missing_stream_columns:
        raise ValueError(f"streams_gdf is missing columns: {sorted(missing_stream_columns)}")
    if missing_domain_columns:
        raise ValueError(f"whitelist_domain_final_gdf is missing columns: {sorted(missing_domain_columns)}")
    if missing_metrics_columns:
        raise ValueError(f"candidates_metrics_df is missing columns: {sorted(missing_metrics_columns)}")

    whitelist_final_df = whitelist_final_df.copy()
    whitelist_final_df["too_long"] = False
    valid_feature_ids = set(whitelist_final_df.loc[whitelist_final_df["is_valid"].eq(True), "feature_id"])
    not_headwater_feature_ids = set(
        candidates_metrics_df.loc[candidates_metrics_df["not_headwater_stream"].eq(True), "feature_id"]
    )
    excluded_feature_ids = set(
        candidates_metrics_df.loc[
            candidates_metrics_df["included_by"].eq("within")
            | candidates_metrics_df["topology_bridge"].eq(True),
            "feature_id",
        ]
    )
    excluded_feature_ids.update(
        whitelist_final_df.loc[whitelist_final_df["is_bridge"].eq(True), "feature_id"]
    )
    valid_feature_ids &= not_headwater_feature_ids
    valid_feature_ids -= excluded_feature_ids

    valid_streams_gdf = (
        streams_gdf.loc[streams_gdf["feature_id"].isin(valid_feature_ids), ["feature_id", "geometry"]]
        .drop_duplicates(subset="feature_id")
        .copy()
    )

    if valid_streams_gdf.empty or whitelist_domain_final_gdf.empty:
        return whitelist_final_df

    valid_streams_gdf = valid_streams_gdf.to_crs(TARGET_CRS)
    valid_streams_gdf["geometry"] = valid_streams_gdf.geometry.apply(as_single_linestring)
    valid_streams_gdf = valid_streams_gdf[
        valid_streams_gdf.geometry.notna() & ~valid_streams_gdf.geometry.is_empty
    ].copy()
    valid_streams_gdf["stream_length_m"] = valid_streams_gdf.geometry.length
    valid_streams_gdf = valid_streams_gdf[valid_streams_gdf["stream_length_m"] > 0].copy()

    if valid_streams_gdf.empty:
        return whitelist_final_df

    model_domains_gdf = whitelist_domain_final_gdf.to_crs(TARGET_CRS).copy()
    model_domains_gdf["geometry"] = model_domains_gdf.geometry.make_valid()
    model_domains_gdf = model_domains_gdf.dissolve(
        by=["collection_id", "model_id"], as_index=False, dropna=False
    ).reset_index(drop=True)

    stream_model_pairs = gpd.sjoin(
        valid_streams_gdf,
        model_domains_gdf[["collection_id", "model_id", "geometry"]],
        how="inner",
        predicate="intersects",
    ).reset_index(drop=True)

    if stream_model_pairs.empty:
        return whitelist_final_df

    matched_model_geometry = gpd.GeoSeries(
        model_domains_gdf.geometry.iloc[stream_model_pairs["index_right"].to_numpy()].to_numpy(),
        index=stream_model_pairs.index,
        crs=TARGET_CRS,
    )
    covered_geometry = stream_model_pairs.geometry.intersection(matched_model_geometry, align=False)
    stream_model_pairs["covered_length_m"] = covered_geometry.length
    stream_model_pairs = stream_model_pairs[stream_model_pairs["covered_length_m"] > 0].copy()

    if stream_model_pairs.empty:
        return whitelist_final_df

    stream_model_pairs["individual_coverage"] = (
        stream_model_pairs["covered_length_m"] / stream_model_pairs["stream_length_m"]
    )
    coverage_by_feature = stream_model_pairs.groupby("feature_id").agg(
        model_count=("model_id", "size"),
        maximum_individual_coverage=("individual_coverage", "max"),
        stream_length_m=("stream_length_m", "first"),
    )

    covered_pieces_gdf = gpd.GeoDataFrame(
        stream_model_pairs[["feature_id"]].copy(),
        geometry=covered_geometry.loc[stream_model_pairs.index],
        crs=TARGET_CRS,
    )
    combined_covered_length = covered_pieces_gdf.dissolve(by="feature_id").geometry.length
    coverage_by_feature["combined_coverage"] = (
        combined_covered_length / coverage_by_feature["stream_length_m"]
    )

    too_long_feature_ids = coverage_by_feature.index[
        (coverage_by_feature["model_count"] >= 2)
        & (coverage_by_feature["combined_coverage"] >= coverage_threshold)
        & (coverage_by_feature["maximum_individual_coverage"] < coverage_threshold)
    ]

    whitelist_final_df.loc[
        whitelist_final_df["is_valid"].eq(True) & whitelist_final_df["feature_id"].isin(too_long_feature_ids),
        "too_long",
    ] = True

    return whitelist_final_df


# -----------------------------------------------------------------------------
def process_streams_save_outputs(
    ripple_analysis_dir, ripple_whitelist_table, ripple_domain_gpkg, ripple_metrics_dir, n_workers, chunksize
):
    whitelist_cols = WHITELIST_COLS
    whitelist_df, whitelist_df_complete = create_huc_validated_whitelist(
        ripple_analysis_dir, ripple_whitelist_table, whitelist_cols
    )

    collection_model_ids = create_collection_model_ids(whitelist_df)

    domain_whitelist_gdf = create_whitelist_domain(
        ripple_analysis_dir, ripple_domain_gpkg, collection_model_ids
    )

    today = RUN_TIMESTAMP
    domain_whitelist_gdf.to_file(
        join(ripple_analysis_dir, f"whitelist_ripple_model_domain_{today}.gpkg"), driver="GPKG"
    )

    streams_gdf = read_ripple_streams(
        whitelist_df, ripple_metrics_dir, collection_slice=None  # slice(92, 94),
    )

    create_save_whitelist_streams(whitelist_df, streams_gdf, ripple_analysis_dir)

    merged_domain_whitelist_2streams0h_gdf = create_save_whitelist_merged_domain(
        domain_whitelist_gdf, streams_gdf, ripple_analysis_dir
    )

    included_streams_gdf, candidates_metrics_df, within_count = select_valid_streams(
        streams_gdf,
        merged_domain_whitelist_2streams0h_gdf,
        whitelist_feature_ids=set(whitelist_df["feature_id"]),
        n_workers=n_workers,
        chunksize=chunksize,
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
        join(ripple_analysis_dir, f"whitelisted_nwm_streams_within_downstreamCovered_GapExcl_{today}.gpkg"),
        driver="GPKG",
    )
    included_streams_gdf.sort_values("feature_id").drop(columns=["geometry"]).to_csv(
        join(ripple_analysis_dir, f"whitelisted_nwm_streams_within_downstreamCovered_GapExcl_{today}.csv"),
        index=False,
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

    whitelist_final_df.loc[whitelist_final_df["feature_id"].duplicated(keep=False), "is_duplicated"] = True
    final_valid_override1 = (
        whitelist_final_df["is_blacklisted"].eq(False)
        & whitelist_final_df["is_library_path_valid"].eq(True)
        & whitelist_final_df["is_duplicated"].eq(True)
        & whitelist_final_df["huc_valid"].eq(True)
        & whitelist_final_df["is_gap"].eq(True)
        & whitelist_final_df["is_valid"].eq(False)
    )
    whitelist_final_df.loc[final_valid_override1, "is_valid"] = True
    final_valid_override2 = (
        whitelist_final_df["is_blacklisted"].eq(False)
        & whitelist_final_df["is_library_path_valid"].eq(False)
        & whitelist_final_df["is_bridge"].eq(True)
        & whitelist_final_df["is_duplicated"].eq(True)
        & whitelist_final_df["huc_valid"].eq(True)
        & whitelist_final_df["is_gap"].eq(True)
        & whitelist_final_df["is_valid"].eq(False)
    )
    whitelist_final_df.loc[final_valid_override2, "is_valid"] = True

    whitelist_domain_final_gdf = select_fully_overlapping_domain_polygons(
        domain_whitelist_gdf, merged_domain_whitelist_2streams0h_gdf
    )
    whitelist_final_df = flag_too_long_streams(
        whitelist_final_df, streams_gdf, whitelist_domain_final_gdf, candidates_metrics_df
    )

    whitelist_final_df.sort_values("feature_id").to_csv(
        join(ripple_analysis_dir, f"whitelist_ripple_feature_ids_final_{today}.csv"), index=False
    )


if __name__ == "__main__":

    """
    Examples of usage:

    # ripple_analysis_dir = "/outputs/"
    # RIPPLE_DOMAIN_GPKG = "ripple_domains.gpkg"
    # RIPPLE_WHITELIST_TABLE = "ripple_feature_list_20260310_huc_considered_delivered.csv"
    # ripple_metrics_dir = "/outputs/nwm_ripple_streams/" or "/data/ripple/ripple_20260211_merged/ripple_metrics/"

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
            "should be saved in the ripple_analysis_dir"
        ),
    )
    parser.add_argument(
        "-dg",
        "--ripple-domain-gpkg",
        required=True,
        type=str,
        help="ripple_domain_gpkg; should be saved in the ripple_analysis_dir",
    )
    parser.add_argument(
        "-rc",
        "--ripple-collections-dir",
        required=True,
        type=str,
        help="ripple_metrics_dir contains ripple_reaches_order_sourcemodels_huc.gpkg",
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

    ripple_analysis_dir = args["ripple_analysis_dir"]
    ripple_whitelist_table = args["ripple_whitelist_table"]
    ripple_domain_gpkg = args["ripple_domain_gpkg"]
    ripple_metrics_dir = args["ripple_metrics_dir"]
    n_workers = args["n_workers"]
    chunksize = args["chunksize"]

    process_streams_save_outputs(
        ripple_analysis_dir,
        ripple_whitelist_table,
        ripple_domain_gpkg,
        ripple_metrics_dir,
        n_workers,
        chunksize,
    )
