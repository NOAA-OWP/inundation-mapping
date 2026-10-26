#!/usr/bin/env python3
"""Evaluate non-blacklisted Ripple streams using collection rasters and model polygons."""

import argparse
from contextlib import ExitStack
from datetime import datetime
from os.path import basename, dirname, join
from zoneinfo import ZoneInfo

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import shapes
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT
from shapely import covers, line_merge, prepare
from shapely.geometry import shape
from shapely.ops import substring


# Reference connectivity and HUC assignments; all coverage lengths use metres.
reference_hydrofabric = (
    "/efs-drives/fim-dev-efs/fim-data/inputs/reference_hydrofabric/" "reference_fabric_v2_3_flowpaths.gpkg"
)
TARGET_CRS = "EPSG:5070"
RUN_TIMESTAMP = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d_%H%M%S")

# Rebuild eligibility from this whitelist so previous gap flags do not exclude domains.
DEFAULT_OUTPUT_DIR = "/home/rdp-user/outputs/whitelist_ripple_streams_and_domain"
DEFAULT_WHITELIST = "ripple_feature_ids_whitelist_final_20261022_115950_no_path.csv"
DEFAULT_DOMAINS = "ripple_domains.gpkg"
DOMAIN_COLS = ["model_id", "collection_id", "project_title", "version", "source_path", "geometry"]


def save_gpkg(gdf, output_dir, name):
    """Write a GeoPackage using the shared run timestamp."""
    gdf.to_file(join(output_dir, f"{name}_{RUN_TIMESTAMP}.gpkg"), driver="GPKG")


def add_huc_valid(whitelist):
    """Allow duplicated streams when none of their assigned HUCs match the reference."""
    whitelist["duplicated"] = whitelist["feature_id"].duplicated(keep=False)
    reference = gpd.read_file(
        reference_hydrofabric,
        layer="reference_fabric_flowpaths",
        columns=["flowpath_id", "reachcode"],
        ignore_geometry=True,
        engine="pyogrio",
    )

    # Recheck the reference on each run so earlier exceptions do not hide HUC mismatches.
    whitelist["huc"] = whitelist["huc"].str.zfill(8)
    reference["huc"] = reference["reachcode"].str[:8]
    reference_pairs = reference.set_index(["flowpath_id", "huc"]).index
    whitelist["huc_valid"] = whitelist.set_index(["feature_id", "huc"]).index.isin(reference_pairs)

    # Accept all rows of a duplicated stream only when none has a matching reference HUC.
    any_huc_valid = whitelist.groupby("feature_id")["huc_valid"].transform("any")
    whitelist["long_river_huc_valid"] = whitelist["duplicated"] & ~any_huc_valid
    whitelist.loc[whitelist["long_river_huc_valid"], "huc_valid"] = True

    # A mismatched HUC excludes a row only when its feature ID occurs more than once.
    whitelist.loc[whitelist["duplicated"] & ~whitelist["huc_valid"], "valid"] = False
    columns = whitelist.columns.drop(["duplicated", "long_river_huc_valid"]).tolist()
    columns.insert(columns.index("huc_valid"), "duplicated")
    columns.insert(columns.index("huc_valid") + 1, "long_river_huc_valid")
    return whitelist[columns]


# def find_too_short_bridge(whitelist):
#     """Flag short missing-library bridges and invalidate those failing the length rule."""
#     reference = gpd.read_file(
#         reference_hydrofabric,
#         layer="reference_fabric_flowpaths",
#         columns=["flowpath_id", "flowpath_toid", "lengthkm"],
#         ignore_geometry=True,
#         engine="pyogrio",
#     ).set_index("flowpath_id")

#     # Use 50% of the longest immediate neighbor; reference lengths are in kilometres.
#     upstream = reference.groupby("flowpath_toid")["lengthkm"].max()
#     downstream = reference["flowpath_toid"].map(reference["lengthkm"])
#     longest_neighbor = pd.concat([upstream, downstream], axis=1).max(axis=1)
#     short_enough = whitelist["feature_id"].map(reference["lengthkm"]) < (
#         0.50 * whitelist["feature_id"].map(longest_neighbor)
#     )
#     whitelist["short_bridge"] = False
#     whitelist.loc[
#         ~whitelist["library_path_valid"] & whitelist["bridge"] & short_enough, "short_bridge"
#     ] = True
#     whitelist.loc[
#         whitelist["bridge"] & ~whitelist["library_path_valid"] & ~whitelist["short_bridge"], "valid"
#     ] = False

#     # Keep the new flag immediately after bridge, including on repeated runs.
#     columns = whitelist.columns.drop("short_bridge").tolist()
#     columns.insert(columns.index("bridge") + 1, "short_bridge")
#     return whitelist[columns]


def create_collection_model_ids(whitelist_df):
    """Build the source-path identifiers for models with valid feature rows."""
    models = whitelist_df[["collection_id", "model_id"]].drop_duplicates().reset_index(drop=True)
    models["model_indicator"] = (
        "/" + models["collection_id"].astype(str) + "/source_models/" + models["model_id"].astype(str) + "/"
    )
    return models


def create_whitelist_domain(ripple_analysis_dir, ripple_domain_gpkg, collection_model_ids):
    """Select and repair domain polygons belonging to the whitelisted models in memory."""
    domains = gpd.read_file(join(ripple_analysis_dir, ripple_domain_gpkg))
    domains["model_indicator"] = domains["source_path"].str.extract(
        r"collections(\/.*\/source_models\/[^\/]+\/)"
    )
    domains = domains.merge(collection_model_ids, on="model_indicator", how="inner")[DOMAIN_COLS]
    domains["geometry"] = domains.geometry.make_valid().buffer(0)
    return domains


def merge_valid_stream_domains(whitelist_df, streams_gdf):
    """Yield (collection_id, raster array, profile) without saving any files.

    Only valid rows with valid library paths contribute domains. Collections with
    no such rows are omitted. Process each yielded mosaic before requesting the
    next one to avoid retaining all collection rasters in memory.
    """
    # Keep available domains for valid streams, reading each library folder once.
    valid = whitelist_df.loc[
        whitelist_df["valid"] & whitelist_df["library_path_valid"], ["collection_id", "library_path"]
    ].drop_duplicates()
    s3_root = "s3://hydrovis-ti-deployment-us-east-1/fim/ripple/"

    # Collections without reference geometry cannot establish coverage and remain gaps.
    stream_groups = dict(
        tuple(streams_gdf.dropna(subset=["geometry"]).to_crs(TARGET_CRS).groupby("collection_id"))
    )
    valid = valid.loc[valid["collection_id"].isin(stream_groups)]
    for collection_id, streams in valid.groupby("collection_id"):
        # Read S3 rasters directly and project them to EPSG:5070 in memory.
        with ExitStack() as stack:
            domains = []
            for library_path in streams["library_path"]:
                path = s3_root + library_path.rstrip("/") + "/domain.tif"
                source = stack.enter_context(rasterio.open(path))
                domains.append(stack.enter_context(WarpedVRT(source, crs=TARGET_CRS)))

            # Crop to collection streams while keeping the full mosaic's pixel grid.
            left = min(domain.bounds.left for domain in domains)
            right = max(domain.bounds.right for domain in domains)
            bottom = min(domain.bounds.bottom for domain in domains)
            top = max(domain.bounds.top for domain in domains)
            xres, yres = domains[0].res
            width, height = round((right - left) / xres), round((top - bottom) / yres)
            west, south, east, north = stream_groups[collection_id].total_bounds
            col0 = max(0, int(np.floor((west - left) / xres)) - 2)
            row0 = max(0, int(np.floor((top - north) / yres)) - 2)
            col1 = min(width, int(np.ceil((east - left) / xres)) + 2)
            row1 = min(height, int(np.ceil((top - south) / yres)) + 2)
            bounds = (left + col0 * xres, top - row1 * yres, left + col1 * xres, top - row0 * yres)
            mosaic, transform = merge(domains, bounds=bounds, method="first", nodata=domains[0].nodata)
            profile = domains[0].profile.copy()
            profile.update(driver="GTiff", height=mosaic.shape[1], width=mosaic.shape[2], transform=transform)

        # Close the S3 datasets before returning this collection's merged domain.
        yield collection_id, mosaic, profile
        del mosaic


def find_stream_domain_gaps(whitelist_df, streams_gdf):
    """Flag non-blacklisted streams with less than 99% coverage by their collection domain.

    Check every non-blacklisted row, including rows already invalid for another reason.
    Only blacklisted rows are left unevaluated (NA). Domains still come from valid streams.
    Stream geometries must include collection_id and feature_id. No files are saved,
    and valid is unchanged.
    """
    result = whitelist_df.copy()
    result["gap"] = pd.Series(pd.NA, index=result.index, dtype="boolean")
    candidates = result["blacklisted"].eq(False)
    # Non-blacklisted streams remain gaps when their collection has no available domain.
    result.loc[candidates, "gap"] = True

    # Match each stream within its own collection and measure lengths in metres.
    keys = ["collection_id", "feature_id"]
    streams = streams_gdf.to_crs(TARGET_CRS).drop_duplicates(keys).set_index(keys)
    for collection_id, mosaic, profile in merge_valid_stream_domains(result, streams_gdf):
        # Domain pixels can be zero: coverage means non-NoData, not a positive value.
        covered_pixels = mosaic[0] != profile["nodata"]
        polygons = [
            shape(geometry)
            for geometry, _ in shapes(
                covered_pixels.astype("uint8"), mask=covered_pixels, transform=profile["transform"]
            )
        ]
        domain = gpd.GeoSeries(polygons, crs=TARGET_CRS).union_all()

        # Include invalid rows and streams without their own domain; neighbors may cover them.
        rows = result.loc[candidates & result["collection_id"].eq(collection_id)]
        stream_keys = pd.MultiIndex.from_frame(rows[keys])
        geometries = streams.loc[stream_keys, "geometry"]
        covered_fraction = geometries.intersection(domain).length / geometries.length
        result.loc[rows.index, "gap"] = (~covered_fraction.ge(0.99)).to_numpy()
        del mosaic

    return result


def invalidate_gap_streams(whitelist_df):
    """Invalidate remaining gaps after raster, headwater, and own-model coverage checks."""
    result = whitelist_df.copy()
    result.loc[result["gap"].eq(True), "valid"] = False
    return result


def downstream_headwater_segments(streams_gdf):
    """Return the downstream-most 40% of each stream, indexed like the input GeoDataFrame.

    outlet_X/outlet_Y are hydrofabric outlet coordinates in the input geometry's CRS.
    Identify the downstream endpoint from that outlet, rather than assuming that a
    merged line's final coordinate is downstream. All segment lengths use EPSG:5070.
    """
    # Joining connected parts gives one measurable stream without bridging gaps between disconnected parts.
    lines = gpd.GeoSeries(
        line_merge(streams_gdf.to_crs(TARGET_CRS).geometry.array), index=streams_gdf.index, crs=TARGET_CRS
    )
    outlets = gpd.GeoSeries(
        gpd.points_from_xy(streams_gdf["outlet_X"], streams_gdf["outlet_Y"]),
        index=streams_gdf.index,
        crs=streams_gdf.crs,
    ).to_crs(TARGET_CRS)
    start_distance = lines.interpolate(0).distance(outlets)
    end_distance = lines.interpolate(1, normalized=True).distance(outlets)
    downstream_at_start = start_distance.lt(end_distance)

    # Missing/disconnected geometry, zero length, or an ambiguous outlet cannot establish downstream coverage.
    measurable = lines.geom_type.eq("LineString") & lines.length.gt(0) & start_distance.ne(end_distance)
    measurable &= start_distance.notna() & end_distance.notna()
    segments = gpd.GeoSeries(None, index=streams_gdf.index, crs=TARGET_CRS)
    segments.loc[measurable] = [
        substring(line, 0, 0.40, normalized=True) if at_start else substring(line, 0.60, 1, normalized=True)
        for line, at_start in zip(lines.loc[measurable], downstream_at_start.loc[measurable])
    ]
    return segments


def relax_headwater_domain_gaps(whitelist_df, streams_gdf):
    """Clear headwater gaps only when the collection domain covers the entire downstream 40%.

    A headwater exists in the reference fabric and has no incoming flowpath there.
    The covered stretch must reach the outlet without a gap; middle/upstream coverage does not qualify.
    Multiple valid-stream rasters can jointly cover that stretch within the collection. Preserve
    all other gap flags and input validity; return the table without saving.
    headwater marks reference headwaters that had gap=True when this function started.
    """
    result = whitelist_df.copy()
    reference = gpd.read_file(
        reference_hydrofabric,
        layer="reference_fabric_flowpaths",
        columns=["flowpath_id", "flowpath_toid"],
        ignore_geometry=True,
        engine="pyogrio",
    )

    # Use the entire reference network: no flowpath may drain to a headwater's ID.
    headwater_ids = reference.loc[~reference["flowpath_id"].isin(reference["flowpath_toid"]), "flowpath_id"]
    # Keep this flag True even when the 40% rule clears the row's gap below.
    result["headwater"] = (
        result["blacklisted"].eq(False) & result["gap"].eq(True) & result["feature_id"].isin(headwater_ids)
    ).fillna(False)
    candidates = result["headwater"]
    collections = result.loc[candidates, "collection_id"].unique()
    keys = ["collection_id", "feature_id"]
    streams = streams_gdf.drop_duplicates(keys).set_index(keys)
    headwater_keys = pd.MultiIndex.from_frame(result.loc[candidates, keys])
    downstream = downstream_headwater_segments(streams.loc[streams.index.isin(headwater_keys)])

    # Keep domains from ALL valid streams in each selected collection, including non-headwaters.
    collection_rows = result.loc[result["collection_id"].isin(collections)]
    for collection_id, mosaic, profile in merge_valid_stream_domains(collection_rows, streams_gdf):
        covered_pixels = mosaic[0] != profile["nodata"]
        polygons = [
            shape(geometry)
            for geometry, _ in shapes(
                covered_pixels.astype("uint8"), mask=covered_pixels, transform=profile["transform"]
            )
        ]
        domain = gpd.GeoSeries(polygons, crs=TARGET_CRS).union_all()

        # Cover the whole downstream segment, including its endpoint and boundary, with no buffer or tolerance.
        # Testing this segment rejects middle-only coverage and disconnected patches totaling 40% or more.
        rows = result.loc[candidates & result["collection_id"].eq(collection_id)]
        stream_keys = pd.MultiIndex.from_frame(rows[keys])
        segments = downstream.reindex(stream_keys)
        covered = covers(domain, segments.array) & segments.length.gt(0).to_numpy()
        result.loc[rows.index[covered], "gap"] = False
        del mosaic

    # Gap flags do not restore rows excluded by another validity rule.
    return result


def clear_gaps_covered_by_own_model(whitelist_df, streams_gdf, ripple_domains_path):
    """Clear own-model gaps at 99%, or fully covered downstream 40% segments for headwaters.

    Match both collection_id and model_id to source_path. Non-headwaters need their own model.
    A headwater's own model and other already-valid models may jointly cover its downstream segment.
    Preserve input validity so the caller can invalidate only the final gaps after all coverage checks.
    """
    result = whitelist_df.copy()
    candidates = result["blacklisted"].eq(False) & result["gap"].eq(True)
    keys = ["collection_id", "model_id"]
    domains = gpd.read_file(ripple_domains_path).to_crs(TARGET_CRS)
    domains[keys] = domains["source_path"].str.extract(r"/collections/([^/]+)/source_models/([^/]+)/")

    # Keep each candidate's own model separate for the non-headwater check and its headwater contribution.
    domains = domains.merge(result.loc[candidates, keys].drop_duplicates(), on=keys, how="inner")
    domains["geometry"] = domains.geometry.make_valid()
    model_domains = domains.dissolve(by=keys).geometry
    candidate_indices = {key: group.index for key, group in result.loc[candidates].groupby(keys, sort=False)}
    stream_keys = ["collection_id", "feature_id"]
    streams = streams_gdf.to_crs(TARGET_CRS).drop_duplicates(stream_keys).set_index(stream_keys)

    for key, domain in model_domains.items():
        rows = result.loc[candidate_indices.get(key, [])]
        geometries = streams.loc[pd.MultiIndex.from_frame(rows[stream_keys]), "geometry"]
        covered_fraction = geometries.intersection(domain).length / geometries.length
        # Finish non-headwaters first so models accepted by this 99% check can support headwaters below.
        covered = covered_fraction.ge(0.99).to_numpy() & ~rows["headwater"].to_numpy(dtype=bool)
        result.loc[rows.index[covered], "gap"] = False

    # Build a fixed supporting footprint from rows already passing all exclusions and coverage checks.
    # Checking bridges here prevents a rejected missing-library bridge from supplying supporting coverage.
    # These local validity changes select supporting models only; the returned table's valid is unchanged.
    supporting = apply_manual_exclusions(result)
    supporting.loc[~supporting["gap"].eq(False).fillna(False), "valid"] = False
    supporting = flag_bridge_domain_gaps(supporting, streams_gdf, ripple_domains_path)
    valid_domains = save_valid_model_domains(
        supporting,
        streams_gdf,
        dirname(ripple_domains_path),
        basename(ripple_domains_path),
        save_output=False,
    )
    supporting_domain = valid_domains.geometry.union_all()
    prepare(supporting_domain)

    # Do not let new headwater acceptances support one another during this pass.
    # Other-model support can come from any collection; the candidate's own model still matches both IDs.
    rows = result.loc[candidates & result["headwater"]]
    stream_rows = streams_gdf.drop_duplicates(stream_keys).set_index(stream_keys)
    selected_keys = pd.MultiIndex.from_frame(rows[stream_keys])
    downstream = downstream_headwater_segments(stream_rows.loc[stream_rows.index.isin(selected_keys)])
    segments = downstream.reindex(selected_keys).reset_index(drop=True)
    own_domains = model_domains.reindex(pd.MultiIndex.from_frame(rows[keys])).reset_index(drop=True)
    uncovered = segments.difference(own_domains)

    # The own model may cover only the middle if valid models cover every missing piece towards the outlet.
    # A missing own model can qualify only when supporting models cover the entire downstream segment.
    covered = covers(supporting_domain, segments.array) | uncovered.is_empty.to_numpy()
    covered |= covers(supporting_domain, uncovered.array)
    covered &= segments.length.gt(0).to_numpy()
    result.loc[rows.index[covered], "gap"] = False

    return result


def flag_bridge_domain_gaps(whitelist_df, streams_gdf, ripple_domains_path):
    """Flag selected missing-library bridges with less than 99% own-model coverage.

    Check valid, non-headwater bridges that passed the raster gap check. Preserve
    existing flags on other rows; candidates without model polygons remain flagged.
    This separate polygon check invalidates flagged rows without changing gap.
    """
    result = whitelist_df.copy()
    candidates = (
        result["bridge"].eq(True)
        # & result["short_bridge"].eq(False)
        & result["library_path_valid"].eq(False)
        & result["gap"].eq(False)
        & result["headwater"].eq(False)
        & result["valid"].eq(True)
    ).fillna(False)
    result["bridge_domain_gap"] = (
        result.get("bridge_domain_gap", pd.Series(False, index=result.index)).fillna(False).astype(bool)
    )
    result.loc[candidates, "bridge_domain_gap"] = True

    # Match collection and model together; neighboring models cannot supply coverage.
    keys = ["collection_id", "model_id"]
    domains = gpd.read_file(ripple_domains_path).to_crs(TARGET_CRS)
    domains[keys] = domains["source_path"].str.extract(r"/collections/([^/]+)/source_models/([^/]+)/")
    domains = domains.merge(result.loc[candidates, keys].drop_duplicates(), on=keys, how="inner")
    domains["geometry"] = domains.geometry.make_valid()
    candidate_indices = {key: group.index for key, group in result.loc[candidates].groupby(keys, sort=False)}
    stream_keys = ["collection_id", "feature_id"]
    streams = streams_gdf.to_crs(TARGET_CRS).drop_duplicates(stream_keys).set_index(stream_keys)

    # Compare full reference-stream length with all polygon pieces of its own model.
    for key, domain in domains.dissolve(by=keys).geometry.items():
        rows = result.loc[candidate_indices.get(key, [])]
        geometries = streams.loc[pd.MultiIndex.from_frame(rows[stream_keys]), "geometry"]
        coverage = geometries.intersection(domain).length / geometries.length
        result.loc[rows.index, "bridge_domain_gap"] = (~coverage.ge(0.99)).to_numpy()

    # Reject bridges that lack sufficient own-model coverage; keep validity last.
    result.loc[result["bridge_domain_gap"], "valid"] = False
    return result[[column for column in result.columns if column != "valid"] + ["valid"]]


def apply_manual_exclusions(whitelist_df):
    """Exclude feature IDs and named models within the specified HUCs or collections."""
    result = whitelist_df.copy()
    # Add additional feature IDs to this list to exclude them from every collection.
    excluded_feature_ids = [
        1260353,
        1260133,  # needs ripple to be run with lower flows
        6149009,
        6149635,
        5576197,
        418327,
        482624,
        1440395,
        1439649,
        7240649,
        7240439,
        703990,
        21044228,
        21047294,
    ]

    # Add model exclusions here: normalized name, scope column, and HUCs or collections to exclude.
    # Write names in lowercase without separators; matching also includes prefixes and reach suffixes.
    excluded_models = [
        ("arkansasriver", "huc", ["11020005", "11020009"]),
        ("styxriver", "collection_id", ["mip_03140106"]),
    ]

    # Ignore capitalization and separators in the dataset's model names.
    model_names = result["model_id"].str.lower().str.replace(r"[^a-z0-9]", "", regex=True)
    excluded = result["feature_id"].isin(excluded_feature_ids)
    for model_name, scope_column, scope_values in excluded_models:
        matching_model = model_names.str.contains(model_name, regex=False, na=False)
        matching_scope = result[scope_column].astype(str).isin(scope_values)
        excluded |= matching_model & matching_scope

    # Invalidate every matching row while preserving exclusions already applied to other rows.
    result.loc[excluded, "valid"] = False
    return result


def save_streams_by_validity(whitelist_df, output_dir):
    """Export valid and invalid rows with all whitelist and reference flowpath attributes."""
    feature_ids = ",".join(str(int(value)) for value in whitelist_df["feature_id"].unique())
    reference = gpd.read_file(
        reference_hydrofabric, layer="reference_fabric_flowpaths", where=f"flowpath_id IN ({feature_ids})"
    ).to_crs(TARGET_CRS)
    # Preserve Boolean fields when missing reference streams introduce null values during the join.
    reference = reference.astype({column: "boolean" for column in reference.select_dtypes(include="bool")})

    # Retain collection-specific rows even when the same feature occurs more than once.
    # IDs absent from the hydrofabric retain their attributes with null geometry.
    streams = whitelist_df.merge(
        reference,
        left_on="feature_id",
        right_on="flowpath_id",
        how="left",
        validate="many_to_one",
        suffixes=("", "_reference"),
    )
    streams = gpd.GeoDataFrame(streams, geometry="geometry", crs=reference.crs)

    # Keep valid last in each attribute table and use the same timestamp for both files.
    columns = [column for column in streams.columns if column not in ["geometry", "valid"]]
    streams = streams[columns + ["valid", "geometry"]]
    paths = []
    for valid, label in [(True, "valid"), (False, "invalid")]:
        path = join(output_dir, f"ripple_whitelist_streams_{label}_{RUN_TIMESTAMP}.gpkg")
        streams.loc[streams["valid"].eq(valid)].to_file(
            path, layer=f"streams_{label}", driver="GPKG", index=False
        )
        paths.append(path)
    return paths


def save_valid_model_domains(
    whitelist_df, streams_gdf, ripple_analysis_dir, ripple_domain_gpkg, save_output=True
):
    """Keep valid-model parts intersecting valid streams; optionally save GeoPackage and GeoParquet."""
    valid_rows = whitelist_df.loc[whitelist_df["valid"].eq(True)]
    valid_models = create_collection_model_ids(valid_rows)
    domains = create_whitelist_domain(ripple_analysis_dir, ripple_domain_gpkg, valid_models)
    # Manually exclude only the OLIVER_BRANCH model from this collection.
    domains = domains.query("model_id != 'OLIVER_BRANCH' or collection_id != 'ble_12070103_Navasota'").copy()

    # Split multipart models into disconnected polygon parts, retaining each part's model attributes.
    # A valid stream in one part must not keep separate islands that intersect no valid stream.
    domains = domains.explode(ignore_index=True)

    # Any currently valid reference stream can retain a part, including one from another model/collection.
    # Reuse the reference geometry already loaded by main and align its CRS with the model polygons.
    valid_streams = streams_gdf.loc[streams_gdf["feature_id"].isin(valid_rows["feature_id"])]
    valid_streams = valid_streams.drop_duplicates("feature_id").to_crs(domains.crs)

    # Keep each intersecting part once even if several streams cross it; boundary contact also counts.
    # Keep the entire polygon without clipping or buffering, so connected branches stay intact.
    polygon_indices = domains.sindex.query(valid_streams.geometry, predicate="intersects")[1]
    domains = domains.iloc[np.unique(polygon_indices)].reset_index(drop=True)
    if save_output:
        save_gpkg(domains, ripple_analysis_dir, "ripple_model_domain_whitelist_valid")
        # Save the same attributes, geometry, and CRS as GeoParquet using the shared timestamp.
        domains.to_parquet(
            join(ripple_analysis_dir, f"ripple_model_domain_whitelist_valid_{RUN_TIMESTAMP}.parq"),
            index=False,
        )
    return domains


def merge_valid_model_domains(valid_domains, output_dir, save_output=True):
    """Merge overlapping valid domains into separate polygon parts and optionally save them."""
    # Remove internal boundaries wherever valid domains overlap or share an edge.
    merged = gpd.GeoDataFrame(geometry=[valid_domains.geometry.union_all()], crs=valid_domains.crs).explode(
        ignore_index=True
    )

    # Each separate polygon gets its own ID because it can contain several source models.
    merged.insert(0, "domain_id", range(1, len(merged) + 1))
    if save_output:
        save_gpkg(merged, output_dir, "ripple_model_domain_whitelist_valid_merged")
    return merged


def restore_streams_covered_by_valid_merged_domain(whitelist_df, streams_gdf, merged_domains):
    """Flag fully covered invalid rows and restore those excluded only by either gap rule."""
    result = whitelist_df.copy()
    invalid = result["valid"].eq(False)
    column = "fully_covered_by_valid_domain"
    result[column] = False

    # Check the entire stream against the combined valid footprint, including its boundary.
    # Preparing the polygon speeds up repeated coverage checks without changing its shape.
    domain = merged_domains.to_crs(TARGET_CRS).geometry.union_all()
    prepare(domain)
    keys = ["collection_id", "feature_id"]
    streams = streams_gdf.to_crs(TARGET_CRS).drop_duplicates(keys).set_index(keys)
    geometries = streams.reindex(pd.MultiIndex.from_frame(result.loc[invalid, keys])).geometry
    result.loc[invalid, column] = covers(domain, geometries.array) & geometries.length.gt(0).to_numpy()

    # Preserve every non-gap exclusion, including manually excluded IDs and Arkansas River models.
    eligible = result.copy()
    eligible["valid"] = (
        ~result["blacklisted"]
        & (result["library_path_valid"] | result["bridge"])
        & ~(result["duplicated"] & ~result["huc_valid"])
    )
    eligible = apply_manual_exclusions(eligible)
    gap_excluded = result["gap"].eq(True) | result["bridge_domain_gap"].eq(True)
    restored = invalid & result[column] & gap_excluded & eligible["valid"]
    result.loc[restored, "valid"] = True

    # Keep the original gap flags for review and place the coverage flag just before valid.
    columns = result.columns.drop(column).tolist()
    columns.insert(columns.index("valid"), column)
    return result[columns]


def main():
    """Apply restoration and the final neighbor rule before saving the six final outputs."""
    global reference_hydrofabric

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-rd", "--ripple-dir", default=DEFAULT_OUTPUT_DIR, help="Input and output directory")
    parser.add_argument(
        "-wl",
        "--ripple-whitelist-table",
        default=DEFAULT_WHITELIST,
        help="Whitelist with blacklist and library/bridge flags; relative to ripple-dir or absolute",
    )
    parser.add_argument(
        "-dg",
        "--ripple-domain-gpkg",
        default=DEFAULT_DOMAINS,
        help="Source-model polygons with source_path; relative to ripple-dir or absolute",
    )
    parser.add_argument(
        "--reference-hydrofabric", default=reference_hydrofabric, help="Reference flowpath GeoPackage"
    )
    args = parser.parse_args()
    reference_hydrofabric = args.reference_hydrofabric

    # Preserve all input attributes while rebuilding eligibility for this run.
    whitelist = pd.read_csv(join(args.ripple_dir, args.ripple_whitelist_table), dtype={"huc": str})
    # Accept existing CSV headers and use the shorter column names throughout the workflow.
    whitelist = whitelist.rename(columns=lambda column: column.removeprefix("is_"))
    # Rebuild eligibility so rows rejected by an older HUC or gap rule are reconsidered.
    whitelist["valid"] = ~whitelist["blacklisted"] & (whitelist["library_path_valid"] | whitelist["bridge"])
    whitelist["bridge_domain_gap"] = False
    whitelist["no_domain"] = ~whitelist["library_path_valid"]
    whitelist = add_huc_valid(whitelist)
    # Read every feature from the reference, including newly eligible HUC mismatches.
    # Include outlet coordinates to locate the downstream end independently of the line's vertex order.
    # Keep flowpath_toid for the final upstream/downstream validity rule.
    feature_ids = ",".join(str(int(value)) for value in whitelist["feature_id"].unique())
    reference = gpd.read_file(
        reference_hydrofabric,
        layer="reference_fabric_flowpaths",
        columns=["flowpath_id", "flowpath_toid", "outlet_X", "outlet_Y"],
        where=f"flowpath_id IN ({feature_ids})",
        engine="pyogrio",
    ).rename(columns={"flowpath_id": "feature_id"})
    streams = gpd.GeoDataFrame(
        whitelist[["collection_id", "feature_id"]].merge(reference, on="feature_id", how="left"),
        geometry="geometry",
        crs=reference.crs,
    )

    # Check all non-blacklisted streams without restoring other exclusions, then invalidate gaps.
    whitelist = find_stream_domain_gaps(whitelist, streams)
    whitelist = relax_headwater_domain_gaps(whitelist, streams)
    whitelist = clear_gaps_covered_by_own_model(
        whitelist, streams, join(args.ripple_dir, args.ripple_domain_gpkg)
    )
    whitelist = apply_manual_exclusions(whitelist)
    whitelist = flag_bridge_domain_gaps(whitelist, streams, join(args.ripple_dir, args.ripple_domain_gpkg))
    whitelist = invalidate_gap_streams(whitelist)

    # Build the currently valid footprint in memory so restoration cannot use its own rejected models.
    valid_domains = save_valid_model_domains(
        whitelist, streams, args.ripple_dir, args.ripple_domain_gpkg, save_output=False
    )
    merged_domains = merge_valid_model_domains(valid_domains, args.ripple_dir, save_output=False)

    # Restore once before the final neighbor rule; keep the original gap flags for review.
    whitelist = restore_streams_covered_by_valid_merged_domain(whitelist, streams, merged_domains)

    # Require valid immediate neighbors on both sides, using valid feature IDs across all collections.
    # Any valid incoming reach counts as upstream support; absent neighbors provide no valid support.
    downstream_ids = whitelist["feature_id"].map(reference.set_index("feature_id")["flowpath_toid"])
    # Repeat after removals so no retained row relies on a neighbor that this rule just invalidated.
    while True:
        valid_ids = whitelist.loc[whitelist["valid"].eq(True), "feature_id"]
        valid_upstream = reference.loc[reference["feature_id"].isin(valid_ids), "flowpath_toid"]
        unsupported = (
            whitelist["valid"].eq(True)
            & whitelist["library_path_valid"].eq(False)
            & (~whitelist["feature_id"].isin(valid_upstream) | ~downstream_ids.isin(valid_ids))
        )
        if not unsupported.any():
            break
        whitelist.loc[unsupported, "valid"] = False
    # Applied only to a new CSV on 2026-10-26; existing GeoPackage/GeoParquet outputs were not refreshed.
    # A future full run applies this rule before rebuilding all outputs below.

    # Rebuild both domain outputs from final valid rows, including newly restored streams' models.
    valid_domains = save_valid_model_domains(whitelist, streams, args.ripple_dir, args.ripple_domain_gpkg)
    merge_valid_model_domains(valid_domains, args.ripple_dir)

    # Keep the complete table, including invalid rows, in a new timestamped CSV.
    output = join(args.ripple_dir, f"ripple_feature_ids_whitelist_final_{RUN_TIMESTAMP}_no_path.csv")
    whitelist.to_csv(output, index=False, mode="x")
    save_streams_by_validity(whitelist, args.ripple_dir)


if __name__ == "__main__":
    main()
