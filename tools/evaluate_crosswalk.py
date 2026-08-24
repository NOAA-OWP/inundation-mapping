#!/usr/bin/env python3

import argparse
import os
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


def _evaluate_crosswalk_intersections(
    flows_gdf: gpd.GeoDataFrame, nwm_streams_gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Computes the number of intersections between NWM and DEM-derived flowlines."""
    flows = flows_gdf.copy()
    nwm_streams = nwm_streams_gdf.copy()

    intersects = flows.sjoin(nwm_streams)
    xwalks = []

    for idx in intersects.index:
        flows_idx = intersects.loc[intersects.index == idx, "HydroID"].unique()

        if isinstance(intersects.loc[idx, "ID"], (np.int64, int)):
            streams_idxs = [intersects.loc[idx, "ID"]]
        else:
            streams_idxs = intersects.loc[idx, "ID"].unique()

        for flows_id in flows_idx:
            for streams_idx in streams_idxs:
                intersect = gpd.overlay(
                    flows[flows["HydroID"] == flows_id],
                    nwm_streams[nwm_streams["ID"] == streams_idx],
                    keep_geom_type=False,
                )

                if len(intersect) == 0:
                    intersect_points = 0
                    feature_id = flows.loc[flows["HydroID"] == flows_id, "feature_id"]
                elif intersect.geometry[0].geom_type == "Point":
                    intersect_points = 1
                    feature_id = flows.loc[flows["HydroID"] == flows_id, "feature_id"]
                else:
                    intersect_points = len(intersect.geometry[0].geoms)
                    feature_id = int(flows.loc[flows["HydroID"] == flows_id, "feature_id"].iloc[0])

                xwalks.append([flows_id, feature_id, streams_idx, intersect_points])

    xwalks_df = pd.DataFrame(xwalks, columns=["HydroID", "feature_id", "ID", "intersect_points"])
    if not xwalks_df.empty:
        xwalks_df["feature_id"] = xwalks_df["feature_id"].astype(int)
        xwalks_df["match"] = xwalks_df["feature_id"] == xwalks_df["ID"]

        xwalks_groupby = xwalks_df[["HydroID", "intersect_points"]].groupby("HydroID").max()
        xwalks_df = xwalks_df.merge(xwalks_groupby, on="HydroID", how="left")
        xwalks_df["max"] = xwalks_df["intersect_points_x"] == xwalks_df["intersect_points_y"]
        xwalks_df["crosswalk"] = xwalks_df["match"] == xwalks_df["max"]

    return xwalks_df


def _evaluate_crosswalk_network(
    flows_gdf: gpd.GeoDataFrame, nwm_streams_gdf: gpd.GeoDataFrame, headwaters_gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Compares upstream and downstream network connectivity of stream segments."""
    flows = flows_gdf.copy()
    flows["HydroID"] = flows["HydroID"].astype(int)

    nwm_streams = nwm_streams_gdf.rename(columns={"ID": "feature_id"}).copy()
    nwm_headwaters = headwaters_gdf.copy()

    streams_outlets = nwm_streams.loc[~nwm_streams["to"].isin(nwm_streams["feature_id"]), "feature_id"]
    flows_outlets = flows.loc[~flows["NextDownID"].isin(flows["HydroID"]), "HydroID"]

    nwm_streams_headwaters_list = ~nwm_streams["feature_id"].isin(nwm_streams["to"])
    flows_headwaters_list = ~flows["HydroID"].isin(flows["NextDownID"])

    nwm_streams_headwaters = nwm_streams[nwm_streams_headwaters_list]
    flows_headwaters = flows[flows_headwaters_list]

    flows_headwaters = flows_headwaters.sjoin_nearest(nwm_headwaters)
    flows_headwaters = flows_headwaters[["HydroID", "ID"]]
    nwm_streams_headwaters = nwm_streams_headwaters.sjoin_nearest(nwm_headwaters)
    nwm_streams_headwaters = nwm_streams_headwaters[["feature_id", "ID"]]

    def _hydroid_to_feature_id(df, hydroid, hydroid_attr, feature_id_attr):
        return df.loc[df[hydroid_attr] == hydroid, feature_id_attr]

    def _get_upstream_data(data, data_headwaters, data_dict, hydroid, hydroid_attr, nextdownid_attr):
        data_dict[hydroid] = list(data.loc[data[nextdownid_attr] == hydroid, hydroid_attr].values)
        for hid in data_dict[hydroid]:
            if hid in data_headwaters[hydroid_attr].values:
                data_dict[hid] = data_headwaters.loc[data_headwaters[hydroid_attr] == hid, "ID"].values[0]
            else:
                data_dict = _get_upstream_data(
                    data, data_headwaters, data_dict, hid, hydroid_attr, nextdownid_attr
                )
        return data_dict

    flows_dict, streams_dict = {}, {}
    for hydroid in flows_outlets:
        flows_dict = _get_upstream_data(flows, flows_headwaters, flows_dict, hydroid, "HydroID", "NextDownID")

    for feature_id in streams_outlets:
        streams_dict = _get_upstream_data(
            nwm_streams, nwm_streams_headwaters, streams_dict, feature_id, "feature_id", "to"
        )

    results = []
    for flow in flows_dict:
        fid = _hydroid_to_feature_id(flows, flow, "HydroID", "feature_id").iloc[0]
        upstream_hid = flows_dict[flow]

        upstream_fids = []
        nwm_fids = streams_dict.get(fid, [])
        out_list = [flow, fid, upstream_fids, nwm_fids]

        if not isinstance(upstream_hid, (np.int64, int)):
            if len(upstream_hid) > 0:
                for i in upstream_hid:
                    temp_fid = int(_hydroid_to_feature_id(flows, i, "HydroID", "feature_id").iloc[0])
                    if isinstance(temp_fid, list):
                        upstream_fids.append(temp_fid[0])
                    else:
                        upstream_fids.append(temp_fid)

                out_list = [flow, fid, upstream_fids, nwm_fids]
                if isinstance(nwm_fids, (np.int64, int)):
                    nwm_fids = [nwm_fids]

                if fid in upstream_fids:
                    out_list.append(-1)
                elif set(upstream_fids) == set(nwm_fids):
                    out_list.append(0)
                else:
                    out_list.append(1)
            else:
                out_list.append(2)
        else:
            out_list.append(3)

        results.append(out_list)

    return pd.DataFrame(
        data=results, columns=["HydroID", "feature_id", "upstream_fids", "upstream_nwm_fids", "status"]
    )


def evaluate_crosswalk_in_memory(
    dem_reaches_gdf: gpd.GeoDataFrame,
    nwm_streams_gdf: gpd.GeoDataFrame,
    headwaters_gdf: gpd.GeoDataFrame = None,
    huc_unit: str = None,
    branch_id: str = None,
    output_csv_path: str = None,
) -> pd.DataFrame:
    """Evaluates crosswalk mapping and connectivity accuracy in RAM."""
    if dem_reaches_gdf.empty or nwm_streams_gdf.empty:
        print("Empty streams passed to evaluate_crosswalk_in_memory.")
        return pd.DataFrame()

    xwalks = _evaluate_crosswalk_intersections(dem_reaches_gdf, nwm_streams_gdf)

    intersections_total = len(xwalks)
    intersections_correct = len(xwalks[xwalks["crosswalk"] == True]) if not xwalks.empty else 0
    intersections_summary = (intersections_correct / intersections_total) if intersections_total > 0 else 0.0

    if headwaters_gdf is not None and not headwaters_gdf.empty:
        network = _evaluate_crosswalk_network(dem_reaches_gdf, nwm_streams_gdf, headwaters_gdf)
        network_total = len(network)
        network_correct = len(network[network["status"] == 0]) if not network.empty else 0
        network_summary = (network_correct / network_total) if network_total > 0 else 0.0
    else:
        network_total = 0
        network_correct = 0
        network_summary = 0.0

    results = pd.DataFrame(
        data={
            "huc": [huc_unit, huc_unit],
            "branch": [branch_id, branch_id],
            "method": ["intersections", "network"],
            "correct": [intersections_correct, network_correct],
            "total": [intersections_total, network_total],
            "proportion": [intersections_summary, network_summary],
        }
    )

    if output_csv_path:
        results.to_csv(output_csv_path, index=False)

    return results


def evaluate_crosswalk(
    dem_reaches_path: str,
    nwm_streams_path: str,
    output_csv: str,
    headwaters_path: str = None,
    huc_unit: str = None,
    branch_id: str = None,
) -> None:
    """File I/O CLI wrapper supporting Parquet and GPKG stream inputs."""
    if str(dem_reaches_path).endswith(".parquet"):
        reaches_gdf = gpd.read_parquet(dem_reaches_path)
    else:
        reaches_gdf = (
            gpd.read_file(dem_reaches_path) if Path(dem_reaches_path).is_file() else gpd.GeoDataFrame()
        )

    if str(nwm_streams_path).endswith(".parquet"):
        nwm_gdf = gpd.read_parquet(nwm_streams_path)
    else:
        nwm_gdf = gpd.read_file(nwm_streams_path) if Path(nwm_streams_path).is_file() else gpd.GeoDataFrame()

    if headwaters_path and Path(headwaters_path).is_file():
        if str(headwaters_path).endswith(".parquet"):
            hw_gdf = gpd.read_parquet(headwaters_path)
        else:
            hw_gdf = gpd.read_file(headwaters_path)
    else:
        hw_gdf = None

    evaluate_crosswalk_in_memory(
        dem_reaches_gdf=reaches_gdf,
        nwm_streams_gdf=nwm_gdf,
        headwaters_gdf=hw_gdf,
        huc_unit=huc_unit,
        branch_id=branch_id,
        output_csv_path=output_csv,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tool to check crosswalk accuracy")
    parser.add_argument(
        "-a", "--dem-reaches", dest="dem_reaches", required=True, help="DEM reaches file path"
    )
    parser.add_argument(
        "-b", "--nwm-streams", dest="nwm_streams", required=True, help="NWM streams file path"
    )
    parser.add_argument(
        "-c", "--output-csv", dest="output_csv", required=True, help="Output summary CSV path"
    )
    parser.add_argument(
        "-d", "--headwaters", dest="headwaters", required=False, default=None, help="NWM headwaters file path"
    )
    parser.add_argument("-u", "--huc-unit", dest="huc_unit", required=False, default=None, help="HUC unit ID")
    parser.add_argument("-z", "--branch-id", dest="branch_id", required=False, default=None, help="Branch ID")

    args = vars(parser.parse_args())
    evaluate_crosswalk(
        dem_reaches_path=args["dem_reaches"],
        nwm_streams_path=args["nwm_streams"],
        output_csv=args["output_csv"],
        headwaters_path=args["headwaters"],
        huc_unit=args["huc_unit"],
        branch_id=args["branch_id"],
    )
