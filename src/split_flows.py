#!/usr/bin/env python3

import argparse
import os
import sys
from collections import OrderedDict
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from shapely import ops, wkt
from shapely.geometry import LineString, Point
from shapely.ops import split as shapely_ops_split

import build_stream_traversal
from utils.fim_enums import FIM_exit_codes
from utils.io import write_geodataframe
from utils.shared_variables import FIM_ID
from utils.spatial import sjoin


def snap_and_trim_flow(snapped_point, flows):
    """Snaps flowline to DEM flows and trims segment in-memory."""
    if len(flows) > 1:
        sjoin_nearest = gpd.sjoin_nearest(snapped_point, flows, max_distance=100)
        if sjoin_nearest.empty:
            return flows

        if len(sjoin_nearest) > 1:
            sjoin_nearest = sjoin_nearest[sjoin_nearest["LINKNO"].isin(sjoin_nearest["DSLINKNO"])]

        nearest_index = int(sjoin_nearest["LINKNO"].iloc[0])
        flow = flows[flows["LINKNO"] == nearest_index].copy()
        flow.index = [0]
    else:
        flow = flows.copy()
        nearest_index = None

    snapped_point["geometry"] = flow.interpolate(flow.project(snapped_point.geometry))[0]

    trimmed_line = shapely_ops_split(flow.iloc[0]["geometry"], snapped_point.iloc[0]["geometry"].buffer(1))

    last_line_segment = pd.DataFrame(
        {"id": ["first"], "geometry": [trimmed_line.geoms[len(trimmed_line.geoms) - 1].wkt]}
    )
    last_line_segment["geometry"] = last_line_segment["geometry"].apply(wkt.loads)
    last_line_segment_geodataframe = gpd.GeoDataFrame(last_line_segment, crs=flow.crs)

    flow_geometry = last_line_segment_geodataframe.iloc[0]["geometry"]

    if nearest_index is not None:
        flows.loc[flows["LINKNO"] == nearest_index, "geometry"] = flow_geometry
    else:
        flows["geometry"] = flow_geometry

    return flows


def split_flows_in_memory(
    flows_gdf: gpd.GeoDataFrame,
    dem_dataset: rasterio.DatasetReader,
    wbd8_gdf: gpd.GeoDataFrame,
    nwm_streams_gdf: gpd.GeoDataFrame,
    lakes_gdf: gpd.GeoDataFrame = None,
    max_length: float = 2000.0,
    slope_min: float = 0.001,
    lakes_buffer_input: float = 20.0,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Executes full stream splitting logic entirely in-memory.

    Returns (split_flows_gdf, split_points_gdf)
    """
    toMetersConversion = 1e-3
    flows_crs = flows_gdf.crs

    if len(flows_gdf) == 0:
        print("No relevant streams within HUC boundaries.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    wbd8 = wbd8_gdf.filter(items=[FIM_ID, "geometry"]).set_index(FIM_ID)

    flows = flows_gdf.explode(index_parts=False).to_crs(wbd8.crs)

    # 1. Trim DEM streams to NWM branch terminus
    nwm_streams = nwm_streams_gdf.explode(index_parts=True)

    if "levpa_id" in nwm_streams.columns:
        if len(nwm_streams) > 1:
            linestring_geo = ops.linemerge(nwm_streams.dissolve(by="levpa_id").iloc[0]["geometry"])
        else:
            linestring_geo = nwm_streams.iloc[0]["geometry"]

        if linestring_geo.geom_type == "MultiLineString":
            linestring_geo = linestring_geo.geoms[-1]

        last = Point(linestring_geo.coords[-1])
        snapped_point = gpd.GeoDataFrame([{"ID": "terminal", "geometry": last}], crs=nwm_streams.crs)
        flows = snap_and_trim_flow(snapped_point, flows)
    else:
        nwm_streams_terminal = nwm_streams[nwm_streams["to"] == 0]
        if not nwm_streams_terminal.empty:
            for _, row in nwm_streams_terminal.iterrows():
                last = Point(row["geometry"].coords[-1])
                snapped_point = gpd.GeoDataFrame([{"ID": "terminal", "geometry": last}], crs=nwm_streams.crs)
                flows = snap_and_trim_flow(snapped_point, flows)

    # 2. Split stream segments at HUC8 boundaries
    flows = (
        gpd.overlay(flows, wbd8, how="union", keep_geom_type=True)
        .explode(index_parts=True)
        .reset_index(drop=True)
    )
    flows = flows[~flows.is_empty]

    if len(flows) == 0:
        print("No relevant streams within HUC boundaries.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    # 3. Split at waterbodies (lakes)
    lake_id_column = None
    lakes_buffer = None
    if lakes_gdf is not None and len(lakes_gdf) > 0 and len(flows) > 0:
        if "newID" in lakes_gdf.columns:
            lake_id_column = "newID"
        elif "wb_id" in lakes_gdf.columns:
            lake_id_column = "wb_id"
        elif "LakeID" in lakes_gdf.columns:
            lake_id_column = "LakeID"
        else:
            print("No 'newID' or 'wb_id' column found in lake file")
            sys.exit(1)

        lakes = lakes_gdf.filter(items=[lake_id_column, "geometry"]).set_index(lake_id_column)
        flows = (
            gpd.overlay(flows, lakes, how="union", keep_geom_type=True)
            .explode(index_parts=True)
            .reset_index(drop=True)
        )
        lakes_buffer = lakes.copy()
        lakes_buffer["geometry"] = lakes.buffer(lakes_buffer_input)

    flows = flows.loc[~flows.is_empty, :]
    if len(flows) == 0:
        print("No relevant streams within HUC boundaries.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    # 4. Length-splitting and DEM Slope calculation (in-memory)
    split_flows_geoms = []
    slopes = []

    for _, lineString in enumerate(flows.geometry):
        lineString = LineString(lineString.coords[::-1])
        if lineString.length == 0:
            continue

        if lineString.length < max_length:
            split_flows_geoms.append(lineString)
            line_points = [point for point in zip(*lineString.coords.xy)]
            start_elev, end_elev = [
                i[0] for i in rasterio.sample.sample_gen(dem_dataset, [line_points[0], line_points[-1]])
            ]
            slope = float(abs(start_elev - end_elev) / lineString.length)
            slopes.append(max(slope, slope_min))
            continue

        splitLength = lineString.length / np.ceil(lineString.length / max_length)
        cumulative_line = []
        last_point = []
        last_point_in_entire_lineString = list(zip(*lineString.coords.xy))[-1]

        for point in zip(*lineString.coords.xy):
            cumulative_line.append(point)
            if last_point:
                cumulative_line = [last_point] + cumulative_line
            elif len(cumulative_line) == 1:
                continue

            if LineString(cumulative_line).length >= splitLength:
                splitLineString = LineString(cumulative_line)
                split_flows_geoms.append(splitLineString)

                start_elev, end_elev = [
                    i[0]
                    for i in rasterio.sample.sample_gen(
                        dem_dataset, [cumulative_line[0], cumulative_line[-1]]
                    )
                ]
                slope = float(abs(start_elev - end_elev) / splitLineString.length)
                slopes.append(max(slope, slope_min))

                last_point = cumulative_line[-1]
                if last_point == last_point_in_entire_lineString:
                    continue
                cumulative_line = []

        if cumulative_line:
            splitLineString = LineString(cumulative_line)
            split_flows_geoms.append(splitLineString)
            start_elev, end_elev = [
                i[0]
                for i in rasterio.sample.sample_gen(dem_dataset, [cumulative_line[0], cumulative_line[-1]])
            ]
            slope = float(abs(start_elev - end_elev) / splitLineString.length)
            slopes.append(max(slope, slope_min))

    split_flows_gdf = gpd.GeoDataFrame(
        {"S0": slopes, "geometry": split_flows_geoms}, crs=flows_crs, geometry="geometry"
    )
    split_flows_gdf["LengthKm"] = split_flows_gdf.geometry.length * toMetersConversion

    if lakes_buffer is not None:
        split_flows_gdf = sjoin(split_flows_gdf, lakes_buffer, how="left", predicate="within")
        split_flows_gdf = split_flows_gdf.rename(columns={lake_id_column: "LakeID"}).fillna(-999)
    else:
        split_flows_gdf["LakeID"] = -999

    split_flows_gdf = split_flows_gdf.drop_duplicates()

    # 5. Build Traversal Network Attributes
    addattributes = build_stream_traversal.build_stream_traversal_columns()
    tResults = addattributes.execute(split_flows_gdf, wbd8, "HydroID")

    if tResults[0] == "OK":
        split_flows_gdf = tResults[1]
    else:
        print("Error: Could not add network attributes to stream segments")

    split_flows_gdf = split_flows_gdf.query("From_Node != To_Node").copy()

    # 6. Generate Split Points Layer in-memory
    split_points = OrderedDict()
    for _, segment in split_flows_gdf.iterrows():
        lineString = segment.geometry
        for point in zip(*lineString.coords.xy):
            if point in split_points:
                if segment.NextDownID != split_points[point]:
                    split_points[point] = segment["HydroID"]
            else:
                split_points[point] = segment["HydroID"]

    hydroIDs_points = list(split_points.values())
    point_geoms = [Point(*point) for point in split_points.keys()]

    split_points_gdf = gpd.GeoDataFrame(
        {"id": hydroIDs_points, "geometry": point_geoms}, crs=flows_crs, geometry="geometry"
    )

    return split_flows_gdf, split_points_gdf


def split_flows(
    flows_filename,
    dem_filename,
    split_flows_filename,
    split_points_filename,
    wbd8_clp_filename,
    lakes_filename,
    nwm_streams_filename,
    max_length,
    slope_min,
    lakes_buffer_input,
):
    """File I/O wrapper calling the in-memory splitting engine with support for Parquet/Fiona inputs."""
    print("Loading data into RAM...")
    flows_gdf = gpd.read_file(flows_filename, engine="fiona")
    wbd8_gdf = gpd.read_file(wbd8_clp_filename, engine="fiona")

    if str(nwm_streams_filename).endswith(".parquet"):
        nwm_streams_gdf = gpd.read_parquet(nwm_streams_filename)
    else:
        nwm_streams_gdf = gpd.read_file(nwm_streams_filename, engine="fiona")

    lakes_gdf = None
    if Path(lakes_filename).exists():
        lakes_gdf = gpd.read_file(lakes_filename, engine="fiona")

    with rasterio.open(dem_filename, "r") as dem_dataset:
        split_flows_gdf, split_points_gdf = split_flows_in_memory(
            flows_gdf=flows_gdf,
            dem_dataset=dem_dataset,
            wbd8_gdf=wbd8_gdf,
            nwm_streams_gdf=nwm_streams_gdf,
            lakes_gdf=lakes_gdf,
            max_length=max_length,
            slope_min=slope_min,
            lakes_buffer_input=lakes_buffer_input,
        )

    print("Writing output files...")
    if len(split_flows_gdf) == 0:
        print("There are no flowlines after stream order filtering.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)

    write_geodataframe(split_flows_gdf, split_flows_filename, index=False)

    if len(split_points_gdf) == 0:
        raise Exception("No points exist.")
    write_geodataframe(split_points_gdf, split_points_filename, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="split_flows.py")
    parser.add_argument("-f", "--flows-filename", help="flows-filename", required=True)
    parser.add_argument("-d", "--dem-filename", help="dem-filename", required=True)
    parser.add_argument("-s", "--split-flows-filename", help="split-flows-filename", required=True)
    parser.add_argument("-p", "--split-points-filename", help="split-points-filename", required=True)
    parser.add_argument("-w", "--wbd8-clp-filename", help="wbd8-clp-filename", required=True)
    parser.add_argument("-l", "--lakes-filename", help="lakes-filename", required=True)
    parser.add_argument("-n", "--nwm-streams-filename", help="nwm-streams-filename", required=True)
    parser.add_argument("-m", "--max-length", help="Maximum split distance (meters)", required=True)
    parser.add_argument("-t", "--slope-min", help="Minimum slope", required=True)
    parser.add_argument("-b", "--lakes-buffer-input", help="Lakes buffer distance (meters)", required=True)

    args = vars(parser.parse_args())
    args["max_length"] = float(args["max_length"])
    args["slope_min"] = float(args["slope_min"])
    args["lakes_buffer_input"] = float(args["lakes_buffer_input"])

    split_flows(**args)
