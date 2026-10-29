#!/usr/bin/env python3

"""
Preprocess NHDPlus V2.1 Pacific Island (PI) vector datasets (streams, lakes, catchments).
"""

import argparse
import os
import sys

import geopandas as gpd

from data.derive_headwaters import findHeadWaterPoints
from src.utils.io import write_geodataframe


def preprocess_nhdplus_pi(
    nhd_path: str, nhdplus_path: str, target_folder: str, target_crs_number: str, region: str
):
    """Processes stream network, attributes, lakes, catchments, and headwaters."""
    target_name = f"{region}_{target_crs_number}"
    os.makedirs(target_folder, exist_ok=True)

    nhd_flowline = os.path.join(nhd_path, "NHDFlowline.shp")
    if not os.path.exists(nhd_flowline):
        sys.exit(f"NHDFlowline file {nhd_flowline} does not exist.")

    print(f"[{region}] Loading flowlines and projecting to EPSG:{target_crs_number}...")
    flowpaths = gpd.read_file(nhd_flowline)
    flowpaths["geometry"] = flowpaths.geometry.force_2d()
    flowpaths = flowpaths.to_crs(epsg=target_crs_number)

    # Ingest attribute DBFs and merge topological connectivity
    plus_flow_dbf = gpd.read_file(f"{nhdplus_path}/NHDPlusAttributes/PlusFlow.dbf")
    plus_vaa_dbf = gpd.read_file(f"{nhdplus_path}/NHDPlusAttributes/PlusFlowlineVAA.dbf")

    flowpaths = flowpaths.merge(
        plus_flow_dbf[["FROMCOMID", "TOCOMID"]], left_on="ComID", right_on="FROMCOMID", how="left"
    )
    flowpaths = flowpaths.merge(plus_vaa_dbf[["ComID", "StreamOrde"]], on="ComID", how="left")
    flowpaths = flowpaths.rename(columns={"ComID": "ID", "TOCOMID": "to", "StreamOrde": "order_"})

    # Coastline handling: reroute flow into sink (to = 0) and remove coastline features
    coastline_ids = flowpaths[flowpaths["FCode"] == 56600]["ID"].tolist()
    flowpaths.loc[flowpaths["to"].isin(coastline_ids), "to"] = 0
    flowpaths = flowpaths[flowpaths["FCode"] != 56600].copy()

    # Derive headwater points
    print(f"[{region}] Deriving headwater points...")
    headwater_points = findHeadWaterPoints(flowpaths)
    write_geodataframe(
        headwater_points,
        os.path.join(target_folder, f"NHDFlowline_headwaters_{target_name}.gpkg"),
        driver="GPKG",
    )

    # Process and join waterbodies
    nhd_waterbody = os.path.join(nhd_path, "NHDWaterbody.shp")
    if os.path.exists(nhd_waterbody):
        print(f"[{region}] Processing waterbodies...")
        waterbodies = gpd.read_file(nhd_waterbody)
        waterbodies = waterbodies.rename(columns={"ComID": "LakeID"})
        waterbodies = waterbodies.to_crs(epsg=target_crs_number)
        waterbodies = waterbodies[["LakeID", "geometry"]]
        write_geodataframe(
            waterbodies, os.path.join(target_folder, f"NHDWaterbody_{target_name}.gpkg"), driver="GPKG"
        )

        flowpaths = flowpaths.sjoin(waterbodies, how="left", predicate="intersects")
        flowpaths = flowpaths.rename(columns={"LakeID": "Lake"})
        flowpaths["Lake"] = flowpaths["Lake"].fillna(-9999).astype(int)

        if "index_right" in flowpaths.columns:
            flowpaths = flowpaths.drop(columns=["index_right"])
        flowpaths = flowpaths.drop_duplicates(subset=["ID"])
    else:
        flowpaths["Lake"] = -9999

    write_geodataframe(
        flowpaths, os.path.join(target_folder, f"NHDFlowline_{target_name}.gpkg"), driver="GPKG"
    )

    # Process catchments
    nhd_catchment = os.path.join(nhdplus_path, "NHDPlusCatchment", "Catchment.shp")
    if os.path.exists(nhd_catchment):
        print(f"[{region}] Processing catchments...")
        catchments = gpd.read_file(nhd_catchment)
        catchments = catchments.rename(columns={"FeatureID": "ID"})
        catchments = catchments.to_crs(epsg=target_crs_number)
        write_geodataframe(
            catchments, os.path.join(target_folder, f"NHDCatchment_{target_name}.gpkg"), driver="GPKG"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess NHDPlus Pacific Island hydrography vector layers."
    )
    parser.add_argument("-n", "--nhd_path", type=str, required=True, help="Path to NHDSnapshot/Hydrography")
    parser.add_argument(
        "-p", "--nhdplus_path", type=str, required=True, help="Path to root NHDPlusPI/NHDPlusXX"
    )
    parser.add_argument(
        "-t", "--target_folder", type=str, required=True, help="Directory to save output GPKGs"
    )
    parser.add_argument("-c", "--target_crs_number", type=str, required=True, help="Target EPSG code")
    parser.add_argument("-r", "--region", type=str, required=True, help="Region name")

    args = parser.parse_args()
    preprocess_nhdplus_pi(**vars(args))


if __name__ == "__main__":
    main()
