#!/usr/bin/env python3

import argparse
import json
import os
import sys
import warnings
from collections import Counter
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from osgeo import gdal, ogr, osr
from rasterio.mask import mask
from shapely import ops
from shapely.geometry import Point

from utils.io import write_geodataframe
from utils.polygonize_raster import polygonize_raster


warnings.simplefilter(action="ignore", category=FutureWarning)


def catch_catchment_size_outliers(catchment_pixels_geom):
    """Identifies whether catchment size contains statistical outliers."""
    unique_values = np.unique(catchment_pixels_geom)
    value_counts = Counter(catchment_pixels_geom.ravel())

    vals, counts = [], []
    for value in unique_values:
        vals.append(value)
        counts.append(value_counts[value])

    catchments_array = np.array(list(zip(vals, counts)), dtype=[("catchment_id", int), ("counts", int)])
    catchments_df = pd.DataFrame(catchments_array)
    catchments_df = catchments_df[catchments_df["catchment_id"] > 0]

    if catchments_df.empty:
        return False, []

    mean_counts = catchments_df["counts"].mean()
    std_dev_counts = catchments_df["counts"].std()
    threshold = 1 * std_dev_counts

    catchments_df["outlier"] = abs(catchments_df["counts"] - mean_counts) > threshold
    num_outlier = catchments_df["outlier"].sum()

    if num_outlier == 0:
        print("No outliers detected in catchment size.")
        flagged_catchment = False
    else:
        print(f"{num_outlier} outlier catchment(s) found in catchment size.")
        flagged_catchment = True

    catchments_df["outlier"] = catchments_df["outlier"].astype("string")
    outlier_catchment_ids = catchments_df[catchments_df["outlier"] == "True"]["catchment_id"].tolist()

    return flagged_catchment, outlier_catchment_ids


def get_raster_value(point, src_dataset, catchment_pixels_geom):
    """Queries pixel value from an in-memory raster array given point geometry."""
    row, col = src_dataset.index(point.geometry.x, point.geometry.y)
    if 0 <= row < catchment_pixels_geom.shape[0] and 0 <= col < catchment_pixels_geom.shape[1]:
        return catchment_pixels_geom[row, col]
    return -9999


def check_if_outlet(last_point_geom, outlier_catchment_ids, src_dataset, catchment_pixels_geom):
    """Determines whether the outlier catchment is positioned at the branch outlet."""
    last_point_geom["catchment_id"] = last_point_geom.apply(
        lambda pt: get_raster_value(pt, src_dataset, catchment_pixels_geom), axis=1
    )
    outlet_flag = last_point_geom["catchment_id"].isin(outlier_catchment_ids).any()
    outlet_catchment_id = last_point_geom["catchment_id"]
    return outlet_flag, outlet_catchment_id


def snap_and_trim_splitflow(outlet_point, flows):
    """Trims and snaps flowlines to outlet point in-memory."""
    if len(flows.index) == 1:
        flow = flows.copy()
    else:
        near_flows = []
        for _, point in outlet_point.iterrows():
            nearest_line = flows.loc[flows.distance(point["geometry"]).idxmin()]
            near_flows.append(nearest_line)

        near_flows_gdf = gpd.GeoDataFrame(near_flows, crs=flows.crs)

        if len(near_flows_gdf) == 1:
            flow = near_flows_gdf
        else:
            last_node = near_flows_gdf["From_Node"].max()
            flow = near_flows_gdf[near_flows_gdf["From_Node"] == last_node].copy()

    toMetersConversion = 1e-3
    initial_length_km = flow.geometry.length.iloc[0] * toMetersConversion

    if flow.index != outlet_point.index:
        flow = flow.reset_index(drop=True)
        outlet_point = outlet_point.reset_index(drop=True)

    outlet_point["geometry"] = flow.interpolate(flow.project(outlet_point))
    outlet_point_buffer = outlet_point.iloc[0]["geometry"].buffer(1)
    split_lines = ops.split(flow.iloc[0]["geometry"], outlet_point_buffer)

    split_lines_df = pd.DataFrame(
        {
            "split_lines_indices": list(range(len(split_lines.geoms))),
            "geometry": list(split_lines.geoms),
            "len_flow": [g.length for g in split_lines.geoms],
        }
    )

    longest_split_line_df = split_lines_df[split_lines_df.len_flow == split_lines_df.len_flow.max()]
    longest_split_line_gdf = gpd.GeoDataFrame(
        longest_split_line_df, geometry=longest_split_line_df["geometry"], crs=flows.crs
    )

    flow_geometry = longest_split_line_gdf.iloc[0]["geometry"]

    if len(flows) > 1:
        flows.loc[flows["NextDownID"] == "-1", "geometry"] = flow_geometry
    else:
        flows["geometry"] = flow_geometry

    return flows, initial_length_km


def calculate_length_and_slope(flows, dem_dataset, slope_min):
    """Recalculates slope and length using an active rasterio DEM dataset in RAM."""
    flow = flows[flows["NextDownID"] == "-1"] if len(flows) > 1 else flows

    start_point = flow.geometry.iloc[0].coords[0]
    end_point = flow.geometry.iloc[0].coords[-1]

    start_elev, end_elev = [i[0] for i in rasterio.sample.sample_gen(dem_dataset, [start_point, end_point])]

    slope = float(abs(start_elev - end_elev) / flow.length)
    if slope < slope_min:
        slope = slope_min

    toMetersConversion = 1e-3
    LengthKm = flow.geometry.length * toMetersConversion

    if len(flows) > 1:
        flows.loc[flows["NextDownID"] == "-1", "S0"] = slope
        flows.loc[flows["NextDownID"] == "-1", "LengthKm"] = LengthKm
    else:
        flows["S0"] = slope
        flows["LengthKm"] = LengthKm

    return flows, LengthKm


def polygonize_array_in_memory(arr, transform, crs_wkt, field_name="HydroID") -> gpd.GeoDataFrame:
    """Converts a 2D numpy raster array to a GeoDataFrame entirely in RAM via GDAL C++ driver."""
    drv = gdal.GetDriverByName("MEM")
    ds = drv.Create("", arr.shape[1], arr.shape[0], 1, gdal.GDT_Int32)
    ds.SetGeoTransform(transform.to_gdal())
    ds.SetProjection(crs_wkt)

    band = ds.GetRasterBand(1)
    band.WriteArray(arr)
    band.SetNoDataValue(0)

    ogr_drv = ogr.GetDriverByName("Memory")
    ogr_ds = ogr_drv.CreateDataSource("memData")

    srs = osr.SpatialReference()
    srs.ImportFromWkt(crs_wkt)

    layer = ogr_ds.CreateLayer("catchments", srs=srs, geom_type=ogr.wkbPolygon)
    fld = ogr.FieldDefn(field_name, ogr.OFTInteger)
    layer.CreateField(fld)

    gdal.Polygonize(band, band.GetMaskBand(), layer, 0, ["8CONNECTED=8"], callback=None)

    geoms, vals = [], []
    for feat in layer:
        geom_wkt = feat.GetGeometryRef().ExportToWkt()
        from shapely.wkt import loads

        geoms.append(loads(geom_wkt))
        vals.append(feat.GetField(field_name))

    ds = None
    ogr_ds = None

    return gpd.GeoDataFrame({field_name: vals}, geometry=geoms, crs=crs_wkt)


def mask_array_to_boundary(raster_dataset, boundary_gdf):
    """Masks an in-memory raster dataset using boundary geometries."""
    boundary_json = [json.loads(boundary_gdf.to_json())["features"][0]["geometry"]]
    masked_data, _ = mask(raster_dataset, boundary_json, crop=False)
    return masked_data[0, :, :]


def mitigate_branch_outlet_backpool_in_memory(
    catchment_pixels_ds: rasterio.DatasetReader,
    catchment_reaches_ds: rasterio.DatasetReader,
    split_flows_gdf: gpd.GeoDataFrame,
    split_points_gdf: gpd.GeoDataFrame,
    nwm_streams_gdf: gpd.GeoDataFrame,
    dem_dataset: rasterio.DatasetReader,
    branch_dir: str = None,
    slope_min: float = 0.00001,
    calculate_stats: bool = False,
    dry_run: bool = False,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, np.ndarray, np.ndarray]:
    """In-memory execution engine for branch outlet backpool mitigation.

    Returns: (output_flows_gdf, output_points_gdf, masked_catchment_reaches_arr, masked_catchment_pixels_arr)
    """
    nwm_streams = nwm_streams_gdf.explode(index_parts=True)

    if "levpa_id" not in nwm_streams.columns:
        print("Will not test for outlet backpool problem in branch zero.")
        return split_flows_gdf, split_points_gdf, catchment_reaches_ds.read(1), catchment_pixels_ds.read(1)

    print("\nNon-branch zero, testing backpool criteria in RAM...")

    catchment_pixels_geom = catchment_pixels_ds.read(1)
    split_flows_geom = split_flows_gdf.copy()
    split_points_geom = split_points_gdf.copy()

    split_flows_last_geom = split_flows_geom[split_flows_geom["NextDownID"] == "-1"].copy()
    one_neg1_nextdownid = len(split_flows_last_geom.index) == 1

    if not one_neg1_nextdownid:
        print("WARNING: Incorrect number of segments with NextDownID = -1. Skipping mitigation.")
        return split_flows_gdf, split_points_gdf, catchment_reaches_ds.read(1), catchment_pixels_geom

    flagged_catchment, outlier_catchment_ids = catch_catchment_size_outliers(catchment_pixels_geom)

    if flagged_catchment:
        last_point = split_flows_last_geom["geometry"].apply(lambda line: Point(line.coords[-1]))
        last_point_geom = gpd.GeoDataFrame(last_point, columns=["geometry"], crs=split_flows_geom.crs)
        outlet_flag, outlet_catchment_id = check_if_outlet(
            last_point_geom, outlier_catchment_ids, catchment_pixels_ds, catchment_pixels_geom
        )
    else:
        outlet_flag = False

    if not outlet_flag:
        print("Incorrectly-large outlet pixel catchment was NOT detected.")
        return split_flows_gdf, split_points_gdf, catchment_reaches_ds.read(1), catchment_pixels_geom

    print("Incorrectly-large outlet pixel catchment detected. Trimming line points in RAM...")

    split_flows_last_geom["num_coordinates"] = split_flows_last_geom["geometry"].apply(
        lambda x: len(x.coords) if x.geom_type == "LineString" else 0
    )

    if split_flows_last_geom["num_coordinates"].iloc[0] < 3:
        if len(split_flows_geom.index) > 1:
            node_2tl = split_flows_last_geom["From_Node"].iloc[0]
            split_flows_2tl_geom = split_flows_geom[split_flows_geom["To_Node"] == node_2tl]
            pt_3tl = split_flows_2tl_geom["geometry"].apply(lambda line: Point(line.coords[-1]))
            trim_flowlines_proceed = True
        else:
            print("Geom length < 3 coords and no 2nd-to-last geom. Skipping mitigation.")
            trim_flowlines_proceed = False
    else:
        pt_3tl = split_flows_last_geom["geometry"].apply(lambda line: Point(line.coords[-3]))
        trim_flowlines_proceed = True

    if not trim_flowlines_proceed:
        return split_flows_gdf, split_points_gdf, catchment_reaches_ds.read(1), catchment_pixels_geom

    pt_3tl_geom = gpd.GeoDataFrame(pt_3tl, columns=["geometry"], crs=split_flows_geom.crs)
    pt_3tl_geom["catchment_id"] = pt_3tl_geom.apply(
        lambda pt: get_raster_value(pt, catchment_pixels_ds, catchment_pixels_geom), axis=1
    )

    trimmed_flows, initial_length_km = snap_and_trim_splitflow(pt_3tl_geom, split_flows_geom)
    buffer = trimmed_flows.buffer(10).geometry.union_all()
    split_points_filtered_geom = split_points_geom[split_points_geom.geometry.within(buffer)].copy()

    output_flows, new_length_km = calculate_length_and_slope(trimmed_flows, dem_dataset, slope_min)

    # In-memory GDAL polygonization of pixel catchments
    cp_poly_geom = polygonize_array_in_memory(
        catchment_pixels_geom,
        catchment_pixels_ds.transform,
        catchment_pixels_ds.crs.to_wkt(),
        field_name="HydroID",
    )

    outlet_catch_val = outlet_catchment_id.iloc[0]
    cp_poly_filt_geom = cp_poly_geom[cp_poly_geom["HydroID"] != outlet_catch_val]
    cp_new_boundary_geom = cp_poly_filt_geom.dissolve()

    masked_reaches_arr = catchment_reaches_ds.read(1)
    masked_pixels_arr = catchment_pixels_geom

    if not dry_run:
        masked_reaches_arr = mask_array_to_boundary(catchment_reaches_ds, cp_new_boundary_geom)
        masked_pixels_arr = mask_array_to_boundary(catchment_pixels_ds, cp_new_boundary_geom)

    if calculate_stats and branch_dir:
        catchment_pixels_old_boundary_geom = cp_poly_geom.dissolve()
        old_boundary_area = catchment_pixels_old_boundary_geom.area.iloc[0]
        new_boundary_area = cp_new_boundary_geom.area.iloc[0]

        boundary_area_km_diff = float(old_boundary_area - new_boundary_area)
        boundary_area_percent_diff = float((boundary_area_km_diff / old_boundary_area) * 100)
        flowlength_km_diff = float(initial_length_km - new_length_km)

        backpool_stats_df = pd.DataFrame(
            {
                "flowlength_km_diff": [flowlength_km_diff],
                "area_km_diff": [boundary_area_km_diff],
                "area_percent_diff": [boundary_area_percent_diff],
            }
        )
        stats_path = Path(branch_dir) / "backpool_stats.csv"
        backpool_stats_df.to_csv(stats_path, index=False)
        print(f"Saved backpool stats to {stats_path}")

    return output_flows, split_points_filtered_geom, masked_reaches_arr, masked_pixels_arr


def mitigate_branch_outlet_backpool(
    branch_dir,
    catchment_pixels_filename,
    catchment_pixels_polygonized_filename,
    catchment_reaches_filename,
    split_flows_filename,
    split_points_filename,
    nwm_streams_filename,
    dem_filename,
    slope_min,
    calculate_stats,
    dry_run,
):
    """File I/O wrapper calling the in-memory backpool mitigation engine."""
    if str(split_flows_filename).endswith(".parquet"):
        split_flows_gdf = gpd.read_parquet(split_flows_filename)
    else:
        split_flows_gdf = gpd.read_file(split_flows_filename, engine="fiona")

    split_points_gdf = gpd.read_file(split_points_filename)

    if str(nwm_streams_filename).endswith(".parquet"):
        nwm_streams_gdf = gpd.read_parquet(nwm_streams_filename)
    else:
        nwm_streams_gdf = gpd.read_file(nwm_streams_filename, engine="fiona")

    with (
        rasterio.open(catchment_pixels_filename) as cp_ds,
        rasterio.open(catchment_reaches_filename) as cr_ds,
        rasterio.open(dem_filename) as dem_ds,
    ):
        out_flows, out_pts, masked_cr, masked_cp = mitigate_branch_outlet_backpool_in_memory(
            catchment_pixels_ds=cp_ds,
            catchment_reaches_ds=cr_ds,
            split_flows_gdf=split_flows_gdf,
            split_points_gdf=split_points_gdf,
            nwm_streams_gdf=nwm_streams_gdf,
            dem_dataset=dem_ds,
            branch_dir=branch_dir,
            slope_min=slope_min,
            calculate_stats=calculate_stats,
            dry_run=dry_run,
        )

        if not dry_run:
            for path, arr, ds in [
                (catchment_reaches_filename, masked_cr, cr_ds),
                (catchment_pixels_filename, masked_cp, cp_ds),
            ]:
                profile = ds.profile.copy()
                profile.update(BIGTIFF="YES")
                with rasterio.open(path, "w", **profile) as dst:
                    dst.write(arr, 1)

            write_geodataframe(out_flows, split_flows_filename, index=False)
            write_geodataframe(out_pts, split_points_filename, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect and mitigate branch outlet backpools issue.")
    parser.add_argument("-b", "--branch-dir", help="branch directory", required=True)
    parser.add_argument("-cp", "--catchment-pixels-filename", help="catchment-pixels-filename", required=True)
    parser.add_argument(
        "-cpp",
        "--catchment-pixels-polygonized-filename",
        help="catchment-pixels-polygonized-filename",
        required=True,
    )
    parser.add_argument(
        "-cr", "--catchment-reaches-filename", help="catchment-reaches-filename", required=True
    )
    parser.add_argument("-s", "--split-flows-filename", help="split-flows-filename", required=True)
    parser.add_argument("-p", "--split-points-filename", help="split-points-filename", required=True)
    parser.add_argument("-n", "--nwm-streams-filename", help="nwm-streams-filename", required=True)
    parser.add_argument("-d", "--dem-filename", help="dem-filename", required=True)
    parser.add_argument("-t", "--slope-min", help="Minimum slope", required=True)
    parser.add_argument(
        "--calculate-stats", help="Optional flag to calculate stats", required=False, action="store_true"
    )
    parser.add_argument(
        "--dry-run", help="Optional flag to run without changing files.", required=False, action="store_true"
    )

    args = vars(parser.parse_args())
    args["slope_min"] = float(args["slope_min"])
    args["calculate_stats"] = bool(args["calculate_stats"])

    mitigate_branch_outlet_backpool(**args)
