#!/usr/bin/env python3

import argparse
import json
import sys

import geopandas as gpd
import numpy as np
import pandas as pd
from numpy import unique

from utils.fim_enums import FIM_exit_codes
from utils.shared_functions import getDriver
from utils.shared_variables import FIM_ID


# Define acceptable slope range
SLOPE_MIN = 9.999e-7
SLOPE_MAX = 0.5


def add_crosswalk_in_memory(
    input_catchments: gpd.GeoDataFrame,
    input_flows: gpd.GeoDataFrame,
    input_src_base: pd.DataFrame,
    input_huc: gpd.GeoDataFrame,
    input_nwmflows: gpd.GeoDataFrame,
    iris_df: pd.DataFrame,
    hfab_slopes_df: pd.DataFrame,
    mannings_n: float,
    min_catchment_area: float,
    min_stream_length: float,
    huc_id: str,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict, pd.DataFrame]:
    """In-memory execution engine for add_crosswalk.

    Returns:
    --------
    tuple: (
        output_catchments,
        output_flows,
        output_src,
        output_crosswalk,
        output_hydro_table,
        output_src_json,
        sml_segs
    )
    """
    min_catchment_area = float(min_catchment_area)
    min_stream_length = float(min_stream_length)

    input_catchments = input_catchments.dissolve(by="HydroID").reset_index()

    # Rename ID column
    input_nwmflows = input_nwmflows.rename(columns={"ID": "feature_id"})
    if input_nwmflows.feature_id.dtype != "int":
        input_nwmflows.feature_id = input_nwmflows.feature_id.astype(int)

    # Merge new HFAB slopes data with NWM flows
    input_nwmflows = input_nwmflows.merge(hfab_slopes_df, on="feature_id", how="left")

    # Merge IRIS-SWORD slope data with NWM flows
    input_nwmflows = input_nwmflows.merge(
        iris_df[["feature_id", "SLOPE_IRIS_SWORD"]], on="feature_id", how="left"
    )
    input_nwmflows = input_nwmflows.set_index("feature_id")

    # Get stream midpoint
    stream_midpoint = []
    hydroID = []
    for i, lineString in enumerate(input_flows.geometry):
        hydroID = hydroID + [input_flows.loc[i, "HydroID"]]
        stream_midpoint = stream_midpoint + [lineString.interpolate(0.5, normalized=True)]

    input_flows_midpoint = gpd.GeoDataFrame(
        {"HydroID": hydroID, "geometry": stream_midpoint}, crs=input_flows.crs, geometry="geometry"
    )
    input_flows_midpoint = input_flows_midpoint.set_index("HydroID")

    # Create crosswalk
    crosswalk = gpd.sjoin_nearest(
        input_flows_midpoint, input_nwmflows, how="left", distance_col="distance"
    ).reset_index()
    crosswalk = crosswalk.rename(columns={"index_right": "feature_id"})

    crosswalk.loc[crosswalk["distance"] > 100.0, "feature_id"] = pd.NA

    crosswalk = crosswalk.filter(items=["HydroID", "feature_id", "distance"])
    crosswalk = crosswalk.merge(input_nwmflows[["order_", "SLOPE_HFAB", "SLOPE_IRIS_SWORD"]], on="feature_id")

    del input_nwmflows

    if crosswalk.empty:
        print("No relevant streams within HUC boundaries.")
        sys.exit(FIM_exit_codes.NO_VALID_CROSSWALKS.value)

    if input_catchments.HydroID.dtype != "int":
        input_catchments.HydroID = input_catchments.HydroID.astype(int)
    output_catchments = input_catchments.merge(crosswalk, on="HydroID")

    del input_catchments

    if output_catchments.empty:
        print("No valid catchments remain.")
        sys.exit(FIM_exit_codes.NO_VALID_CROSSWALKS.value)

    if input_flows.HydroID.dtype != "int":
        input_flows.HydroID = input_flows.HydroID.astype(int)
    output_flows = input_flows.merge(crosswalk, on="HydroID")

    del input_flows

    # Ensure lowercase 'areasqkm' on catchments (matching original line 97)
    if "areasqkm" not in output_catchments.columns:
        if "Areasqkm" in output_catchments.columns:
            output_catchments = output_catchments.rename(columns={"Areasqkm": "areasqkm"})
        else:
            output_catchments["areasqkm"] = output_catchments.geometry.area / (1000**2)

    # Original line 99 merge, but removing 'areasqkm' from flows first if already present
    # to prevent pyogrio field name collisions on save
    if "areasqkm" in output_flows.columns:
        output_flows = output_flows.drop(columns=["areasqkm"])
    if "Areasqkm" in output_flows.columns:
        output_flows = output_flows.drop(columns=["Areasqkm"])

    output_flows = output_flows.merge(output_catchments.filter(items=["HydroID", "areasqkm"]), on="HydroID")

    output_flows = output_flows.drop_duplicates(subset="HydroID")

    output_flows["ManningN"] = mannings_n

    if output_flows.NextDownID.dtype != "int":
        output_flows.NextDownID = output_flows.NextDownID.astype(int)

    # Adjust short model reach rating curves
    print("Adjusting model reach rating curves")
    sml_segs_rows = []

    # Replace small segment geometry with neighboring stream
    for stream_index in output_flows.index:
        row = output_flows.loc[stream_index]
        if row["areasqkm"] < min_catchment_area and row["LengthKm"] < min_stream_length and row["LakeID"] < 0:
            short_id = row["HydroID"]
            to_node = row["To_Node"]
            from_node = row["From_Node"]

            # Multiple upstream segments
            if len(output_flows.loc[output_flows["NextDownID"] == short_id]["HydroID"]) > 1:
                try:
                    max_order = max(output_flows.loc[output_flows["NextDownID"] == short_id]["order_"])
                except Exception as e:
                    print(
                        f"short_id: {short_id} cannot calculate max stream order for "
                        f"multiple upstream segments scenario. \n Exception: \n {repr(e)} \n"
                    )

                if (
                    len(
                        output_flows.loc[
                            (output_flows["NextDownID"] == short_id) & (output_flows["order_"] == max_order)
                        ]["HydroID"]
                    )
                    == 1
                ):
                    update_id = output_flows.loc[
                        (output_flows["NextDownID"] == short_id) & (output_flows["order_"] == max_order)
                    ]["HydroID"].item()
                else:
                    update_id = output_flows.loc[
                        (output_flows["NextDownID"] == short_id) & (output_flows["order_"] == max_order)
                    ]["HydroID"].values[0]

            # Single upstream segments
            elif len(output_flows.loc[output_flows["NextDownID"] == short_id]["HydroID"]) == 1:
                update_id = output_flows.loc[output_flows.To_Node == from_node]["HydroID"].item()

            # No upstream segments; multiple downstream segments
            elif len(output_flows.loc[output_flows.From_Node == to_node]["HydroID"]) > 1:
                try:
                    max_order = max(output_flows.loc[output_flows.From_Node == to_node]["order_"])
                except Exception as e:
                    print(
                        f"To Node {to_node} cannot calculate max stream order for no upstream segments; "
                        "multiple downstream segments scenario. "
                        f"Exception \n {repr(e)} \n"
                    )

                if (
                    len(
                        output_flows.loc[
                            (output_flows["NextDownID"] == short_id) & (output_flows["order_"] == max_order)
                        ]["HydroID"]
                    )
                    == 1
                ):
                    update_id = output_flows.loc[
                        (output_flows.From_Node == to_node) & (output_flows["order_"] == max_order)
                    ]["HydroID"].item()
                elif output_flows.loc[(output_flows.From_Node == to_node), "order_"].max() > max_order:
                    update_id = output_flows.loc[
                        (output_flows.From_Node == to_node)
                        & (
                            output_flows["order_"]
                            == output_flows.loc[(output_flows.From_Node == to_node), "order_"].max()
                        )
                    ]["HydroID"].values[0]
                else:
                    if max_order in output_flows.loc[output_flows.From_Node == to_node, "order_"].values:
                        update_id = output_flows.loc[
                            (output_flows.From_Node == to_node) & (output_flows["order_"] == max_order)
                        ]["HydroID"].values[0]
                    else:
                        update_id = output_flows.loc[
                            (output_flows.From_Node == to_node)
                            & (
                                output_flows["order_"]
                                == output_flows.loc[output_flows.From_Node == to_node, "order_"].max()
                            )
                        ]["HydroID"].values[0]

            # No upstream segments; single downstream segment
            elif len(output_flows.loc[output_flows.From_Node == to_node]["HydroID"]) == 1:
                update_id = output_flows.loc[output_flows.From_Node == to_node]["HydroID"].item()

            else:
                update_id = output_flows[output_flows.HydroID == short_id]["HydroID"].iloc[0]

            output_order = output_flows.loc[output_flows.HydroID == short_id]["order_"]
            if len(output_order) == 1:
                str_order = output_order.item()
            else:
                str_order = output_order.max()
            sml_segs_rows.append({"short_id": short_id, "update_id": update_id, "str_order": str_order})

    sml_segs = pd.DataFrame(sml_segs_rows, columns=["short_id", "update_id", "str_order"])

    print(
        f"Number of short reaches [areasqkm < {min_catchment_area} and LengthKm < {min_stream_length}] = "
        f"{len(sml_segs)}"
    )

    # Process src_base
    input_src_base = input_src_base.copy()
    input_src_base = input_src_base.rename(columns={" SLOPE": "SLOPE_RISE_RUN"})

    if input_src_base.CatchId.dtype != "int":
        input_src_base.CatchId = input_src_base.CatchId.astype(int)

    input_src_base = input_src_base.merge(
        output_flows[["ManningN", "HydroID", "NextDownID", "order_", "SLOPE_HFAB", "SLOPE_IRIS_SWORD"]],
        left_on="CatchId",
        right_on="HydroID",
    )

    input_src_base["SLOPE_HFAB"] = pd.to_numeric(input_src_base["SLOPE_HFAB"], errors="coerce")
    input_src_base["SLOPE_IRIS_SWORD"] = pd.to_numeric(input_src_base["SLOPE_IRIS_SWORD"], errors="coerce")
    input_src_base["SLOPE_RISE_RUN"] = pd.to_numeric(input_src_base["SLOPE_RISE_RUN"], errors="coerce")
    input_src_base["SLOPE"] = (
        input_src_base["SLOPE_HFAB"]
        .combine_first(input_src_base["SLOPE_IRIS_SWORD"])
        .combine_first(input_src_base["SLOPE_RISE_RUN"])
    )

    input_src_base["SLOPE"] = input_src_base["SLOPE"].apply(lambda x: float(f"{x:.3e}"))

    input_src_base = input_src_base.rename(columns=lambda x: x.strip(" "))
    input_src_base = input_src_base.apply(pd.to_numeric, **{"errors": "coerce"})
    input_src_base["TopWidth (m)"] = input_src_base["SurfaceArea (m2)"] / input_src_base["LENGTHKM"] / 1000
    input_src_base["WettedPerimeter (m)"] = input_src_base["BedArea (m2)"] / input_src_base["LENGTHKM"] / 1000
    input_src_base["WetArea (m2)"] = input_src_base["Volume (m3)"] / input_src_base["LENGTHKM"] / 1000
    input_src_base["HydraulicRadius (m)"] = (
        input_src_base["WetArea (m2)"] / input_src_base["WettedPerimeter (m)"]
    )
    input_src_base["HydraulicRadius (m)"] = input_src_base["HydraulicRadius (m)"].fillna(0)
    input_src_base["Discharge (m3s-1)"] = (
        input_src_base["WetArea (m2)"]
        * pow(input_src_base["HydraulicRadius (m)"], 2.0 / 3)
        * pow(input_src_base["SLOPE"], 0.5)
        / input_src_base["ManningN"]
    )

    input_src_base.loc[input_src_base["Stage"] == 0, ["Discharge (m3s-1)"]] = 0
    input_src_base["Bathymetry_source"] = pd.NA

    output_src = input_src_base.drop(columns=["CatchId"]).copy()

    del input_src_base

    if output_src.HydroID.dtype != "int":
        output_src.HydroID = output_src.HydroID.astype(int)

    # Update rating curves
    if len(sml_segs) > 0:
        print("Update rating curves for short reaches.")

        if huc_id.startswith("19"):
            print("Update rating curves for short reaches in Alaska.")
            new_values = output_src[output_src["HydroID"].isin(sml_segs["update_id"])][
                ["HydroID", "Stage", "Discharge (m3s-1)"]
            ]

            sml_segs_with_values = sml_segs.merge(
                new_values, left_on="update_id", right_on="HydroID", suffixes=("", "_new")
            )
            sml_segs_with_values = sml_segs_with_values[["short_id", "Stage", "Discharge (m3s-1)"]]
            merged_output_src = output_src.merge(
                sml_segs_with_values[["short_id", "Stage", "Discharge (m3s-1)"]],
                left_on=["HydroID", "Stage"],
                right_on=["short_id", "Stage"],
                suffixes=("", "_df2"),
            )
            merged_output_src = merged_output_src[["HydroID", "Stage", "Discharge (m3s-1)_df2"]]
            output_src = pd.merge(output_src, merged_output_src, on=["HydroID", "Stage"], how="left")

            del merged_output_src

            output_src["Discharge (m3s-1)"] = output_src["Discharge (m3s-1)_df2"].fillna(
                output_src["Discharge (m3s-1)"]
            )
            output_src = output_src.drop(columns=["Discharge (m3s-1)_df2"])
        else:
            for index, segment in sml_segs.iterrows():
                short_id = segment["short_id"]
                update_id = segment["update_id"]
                new_values = output_src.loc[output_src["HydroID"] == update_id][
                    ["Stage", "Discharge (m3s-1)"]
                ]

                for src_index, src_stage in new_values.iterrows():
                    output_src.loc[
                        (output_src["HydroID"] == short_id) & (output_src["Stage"] == src_stage["Stage"]),
                        ["Discharge (m3s-1)"],
                    ] = src_stage["Discharge (m3s-1)"]

    output_src = output_src.merge(crosswalk[["HydroID", "feature_id"]], on="HydroID")

    output_src["default_SLOPE"] = output_src["SLOPE"]
    output_src["default_ManningN"] = output_src["ManningN"]

    del crosswalk

    output_crosswalk = output_src[["HydroID", "feature_id"]]
    output_crosswalk = output_crosswalk.drop_duplicates(ignore_index=True)

    # Make hydroTable
    output_hydro_table = output_src.loc[
        :,
        [
            "HydroID",
            "feature_id",
            "NextDownID",
            "order_",
            "Number of Cells",
            "SurfaceArea (m2)",
            "BedArea (m2)",
            "TopWidth (m)",
            "LENGTHKM",
            "AREASQKM",
            "WettedPerimeter (m)",
            "HydraulicRadius (m)",
            "WetArea (m2)",
            "Volume (m3)",
            "SLOPE_HFAB",
            "SLOPE_IRIS_SWORD",
            "SLOPE_RISE_RUN",
            "SLOPE",
            "ManningN",
            "Stage",
            "Discharge (m3s-1)",
        ],
    ]
    output_hydro_table.rename(columns={"Stage": "stage", "Discharge (m3s-1)": "discharge_cms"}, inplace=True)

    # Set placeholder variables
    output_hydro_table["default_discharge_cms"] = output_src["Discharge (m3s-1)"]
    output_hydro_table["default_Volume (m3)"] = output_src["Volume (m3)"]
    output_hydro_table["default_WetArea (m2)"] = output_src["WetArea (m2)"]
    output_hydro_table["default_HydraulicRadius (m)"] = output_src["HydraulicRadius (m)"]
    output_hydro_table["default_ManningN"] = output_src["ManningN"]
    output_hydro_table["Bathymetry_source"] = pd.NA
    output_hydro_table["subdiv_applied"] = False
    output_hydro_table["overbank_n"] = pd.NA
    output_hydro_table["channel_n"] = pd.NA
    output_hydro_table["subdiv_discharge_cms"] = pd.NA
    output_hydro_table["calb_applied"] = False
    output_hydro_table["last_updated"] = pd.NA
    output_hydro_table["submitter"] = pd.NA
    output_hydro_table["obs_source"] = pd.NA
    output_hydro_table["precalb_discharge_cms"] = pd.NA
    output_hydro_table["calb_coef_usgs"] = pd.NA
    output_hydro_table["calb_coef_ras2fim"] = pd.NA
    output_hydro_table["calb_coef_spatial"] = pd.NA
    output_hydro_table["calb_coef_final"] = pd.NA

    if output_hydro_table.HydroID.dtype != "str":
        output_hydro_table.HydroID = output_hydro_table.HydroID.astype(str)

    output_hydro_table["HydroID Int16"] = output_hydro_table["HydroID"].apply(lambda x: str(int(x[4:])))
    output_hydro_table[FIM_ID] = output_hydro_table.loc[:, "HydroID"].apply(lambda x: str(x)[0:4])

    if input_huc[FIM_ID].dtype != "str":
        input_huc[FIM_ID] = input_huc[FIM_ID].astype(str)
    output_hydro_table = output_hydro_table.merge(input_huc.loc[:, [FIM_ID, "HUC8"]], how="left", on=FIM_ID)

    if output_flows.HydroID.dtype != "str":
        output_flows.HydroID = output_flows.HydroID.astype(str)
    output_hydro_table = output_hydro_table.merge(
        output_flows.loc[:, ["HydroID", "LakeID"]], how="left", on="HydroID"
    )
    output_hydro_table["LakeID"] = output_hydro_table["LakeID"].astype(int)
    output_hydro_table = output_hydro_table.rename(columns={"HUC8": "HUC"})
    if output_hydro_table.HUC.dtype != "str":
        output_hydro_table.HUC = output_hydro_table.HUC.astype(str)

    output_hydro_table = output_hydro_table.drop(columns=FIM_ID)
    if output_hydro_table.feature_id.dtype != "int":
        output_hydro_table.feature_id = output_hydro_table.feature_id.astype(int)
    if output_hydro_table.feature_id.dtype != "str":
        output_hydro_table.feature_id = output_hydro_table.feature_id.astype(str)

    # Make src json
    output_src_json = dict()
    hydroID_list = unique(output_src["HydroID"])

    for hid in hydroID_list:
        indices_of_hid = output_src["HydroID"] == hid
        stage_list = output_src["Stage"][indices_of_hid].astype(float)
        q_list = output_src["Discharge (m3s-1)"][indices_of_hid].astype(float)

        stage_list = stage_list.tolist()
        q_list = q_list.tolist()

        output_src_json[str(hid)] = {"q_list": q_list, "stage_list": stage_list}

    return (
        output_catchments,
        output_flows,
        output_src,
        output_crosswalk,
        output_hydro_table,
        output_src_json,
        sml_segs,
    )


def add_crosswalk(
    input_catchments_fileName,
    input_flows_fileName,
    input_srcbase_fileName,
    output_catchments_fileName,
    output_flows_fileName,
    output_src_fileName,
    output_src_json_fileName,
    output_crosswalk_fileName,
    output_hydro_table_fileName,
    input_huc_fileName,
    input_nwmflows_fileName,
    mannings_n,
    small_segments_filename,
    min_catchment_area,
    min_stream_length,
    huc_id,
    iris_sword_slope,
    hfab_slope_fileName,
):
    """Disk I/O wrapper for add_crosswalk."""
    input_catchments = gpd.read_file(input_catchments_fileName, engine="fiona")
    input_flows = gpd.read_file(input_flows_fileName, engine="fiona")
    input_huc = gpd.read_file(input_huc_fileName, engine="fiona")
    input_nwmflows = gpd.read_file(input_nwmflows_fileName, engine="fiona")
    input_src_base = pd.read_csv(input_srcbase_fileName, dtype=object)

    iris_df = pd.read_parquet(iris_sword_slope).rename(
        columns={"slope_iris_sword": "SLOPE_IRIS_SWORD", "id": "feature_id"}
    )
    hfab_slopes_df = pd.read_parquet(hfab_slope_fileName)[["id", "slope_m_per_m"]].rename(
        columns={"id": "feature_id", "slope_m_per_m": "SLOPE_HFAB"}
    )

    (
        output_catchments,
        output_flows,
        output_src,
        output_crosswalk,
        output_hydro_table,
        output_src_json,
        sml_segs,
    ) = add_crosswalk_in_memory(
        input_catchments=input_catchments,
        input_flows=input_flows,
        input_src_base=input_src_base,
        input_huc=input_huc,
        input_nwmflows=input_nwmflows,
        iris_df=iris_df,
        hfab_slopes_df=hfab_slopes_df,
        mannings_n=mannings_n,
        min_catchment_area=min_catchment_area,
        min_stream_length=min_stream_length,
        huc_id=huc_id,
    )

    if len(sml_segs) > 0:
        sml_segs.to_csv(small_segments_filename, index=False)

    # Write out
    output_catchments.to_file(
        output_catchments_fileName, driver=getDriver(output_catchments_fileName), index=False
    )
    output_flows.to_file(output_flows_fileName, driver=getDriver(output_flows_fileName), index=False)
    output_src.to_csv(output_src_fileName, index=False)
    output_crosswalk.to_csv(output_crosswalk_fileName, index=False)
    output_hydro_table.to_csv(output_hydro_table_fileName, index=False)

    with open(output_src_json_fileName, "w") as f:
        json.dump(output_src_json, f, sort_keys=True, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Crosswalk for MS/FR/GMS networks; calculate synthetic rating curves; update short rating curves"
    )
    parser.add_argument("-d", "--input-catchments-fileName", help="DEM derived catchments", required=True)
    parser.add_argument("-a", "--input-flows-fileName", help="DEM derived streams", required=True)
    parser.add_argument(
        "-s", "--input-srcbase-fileName", help="Base synthetic rating curve table", required=True
    )
    parser.add_argument(
        "-l", "--output-catchments-fileName", help="Subset crosswalked catchments", required=True
    )
    parser.add_argument("-f", "--output-flows-fileName", help="Subset crosswalked streams", required=True)
    parser.add_argument(
        "-r", "--output-src-fileName", help="Output crosswalked synthetic rating curve table", required=True
    )
    parser.add_argument(
        "-j", "--output-src-json-fileName", help="Output synthetic rating curve json", required=True
    )
    parser.add_argument("-x", "--output-crosswalk-fileName", help="Crosswalk table", required=True)
    parser.add_argument("-t", "--output-hydro-table-fileName", help="Hydrotable", required=True)
    parser.add_argument("-w", "--input-huc-fileName", help="HUC8 boundary", required=True)
    parser.add_argument("-b", "--input-nwmflows-fileName", help="Subest NWM burnlines", required=True)
    parser.add_argument(
        "-m",
        "--mannings-n",
        help="Mannings n. Accepts single parameter set or list of parameter set in calibration mode. Currently input as csv.",
        required=True,
    )
    parser.add_argument("-u", "--huc-id", help="HUC ID", required=True)
    parser.add_argument(
        "-k", "--small-segments-filename", help="output list of short segments", required=True
    )
    parser.add_argument("-e", "--min-catchment-area", help="Minimum catchment area", required=True)
    parser.add_argument("-g", "--min-stream-length", help="Minimum stream length", required=True)
    parser.add_argument(
        "-i", "--iris-sword-slope", help="Channel slope data from IRIS-SWORD database", required=True
    )
    parser.add_argument(
        "-p",
        "--hfab-slope-fileName",
        help="Parquet file containing hydrofabric ransac slope values",
        required=True,
    )

    args = vars(parser.parse_args())

    add_crosswalk(**args)
