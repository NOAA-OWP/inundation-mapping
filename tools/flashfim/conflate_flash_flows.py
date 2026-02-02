import argparse
import os
from time import perf_counter as timer

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats


def smooth_level_path(lp_order_flows):
    """
    This function removes extreme outlier flow values by interpolating any value that is less then 5%
    of the median flow value within the range of three reaches upstream and downstream.
    """

    lp_order_flows = lp_order_flows.sort_values(by="hydroseq")
    # Select any id that the minimum value is less than 5% of the median of three upstream to three downstream
    lp_order_flows['median'] = lp_order_flows['discharge'].rolling(7, min_periods=1, center=True).median()
    lp_order_flows['threshold'] = lp_order_flows['discharge'] < (lp_order_flows['median'] * 0.05)

    lp_order_flows.loc[lp_order_flows['threshold'] == True, 'discharge'] = np.nan
    lp_order_flows['discharge'] = (
        lp_order_flows['discharge'].interpolate(method='linear').drop(columns="threshold")
    )
    lp_order_flows = lp_order_flows.drop(columns=["threshold"])

    return lp_order_flows


def low_order_confluence_check(lp_order_group):
    """
    This function checks the reaches upstream of a confluence for mis-assignment of the higher order
    downstream flow to the lower order confluence. If the 4 reaches upstream of a confluence have flow
    values 90% or greater than the downstream value they are reassigned to the next highest flow pixels.
    """

    if len(lp_order_group) > 0:
        lp_order_group = lp_order_group.sort_values(by="hydroseq", ascending=False)
        pre_confluence_flows = lp_order_group.iloc[-4:, :]
        confluence_q = pre_confluence_flows.iloc[-1].to_q

        pre_confluence_flows = pre_confluence_flows.assign(
            threshold=pre_confluence_flows["discharge"] / confluence_q > 0.9
        )
        pre_confluence_ids = pre_confluence_flows.loc[pre_confluence_flows["threshold"] == True][
            "flowpath_id"
        ]
        lp_order_group.loc[lp_order_group["flowpath_id"].isin(pre_confluence_ids), "fixed_q"] = -9999
        lp_order_group.loc[~lp_order_group["flowpath_id"].isin(pre_confluence_ids), "fixed_q"] = (
            lp_order_group["discharge"]
        )

    return lp_order_group


def flash_flow_conflation(model, huc_flows, output, timestep, min_order):
    """
    This function conflates the flow predicted by a FLASH model to hydrofabric flow paths and exports a flow file
    for use in FIM generation.
    """

    if timestep == "latest":
        url = f"https://mrms.ncep.noaa.gov/2D/FLASH/{model}_MAXSTREAMFLOW/MRMS_FLASH_{model}_MAXSTREAMFLOW.latest.grib2.gz"
    else:
        yr = timestep.split("-")[0][:4]
        mo = timestep.split("-")[0][4:6]
        day = timestep.split("-")[0][6:]
        url = f"https://mtarchive.geol.iastate.edu/{yr}/{mo}/{day}/mrms/ncep/FLASH/{model}_MAXSTREAMFLOW/{model}_MAXSTREAMFLOW_00.00_{timestep}.grib2.gz"

    flash_raster_url = f"/vsigzip//vsicurl/{url}"

    # Buffer stream lines
    huc_flows = huc_flows.loc[huc_flows["streamorder"] >= min_order]
    huc_flows_buffer = huc_flows.assign(geometry=huc_flows.buffer(500, cap_style="square"))[
        ["flowpath_id", "flowpath_toid", "lengthkm", "streamorder", "mainstemlp", "hydroseq", "geometry"]
    ]

    # Calculate zonal stats for each order of magnitude
    ranges = [[10000, 100000], [1000, 10000], [100, 1000], [10, 100], [1, 10], [0, 1]]
    huc_flows_rs = huc_flows_buffer

    with rasterio.open(flash_raster_url) as src:
        band = src.read(1)
        affine = src.transform
        src_crs = src.crs
        huc_flows_buffer = huc_flows_buffer.to_crs(src_crs)

        for r_min, r_max in ranges:
            reclass = np.where(np.logical_and(band > r_min, band < r_max), band, np.nan)

            # Raster Stats Using all touched cells within the buffer
            raster_stats_buf = zonal_stats(
                huc_flows_buffer,
                reclass,
                affine=affine,
                stats=["mean", "sum", "count"],
                all_touched=True,
                geojson_out=True,
            )

            rsb_df = gpd.GeoDataFrame.from_features(raster_stats_buf)[
                ["flowpath_id", "mean", "count"]
            ].astype(float)
            huc_flows_rs = pd.merge(huc_flows_rs, rsb_df, on="flowpath_id", suffixes=("", f"_{r_min}"))
    huc_flows_rs = huc_flows_rs.rename(columns={"mean": "mean_10000", "count": "count_10000"}).drop(
        columns="geometry"
    )

    # Select category with the highest pixel count, if two have the same pick the higher of the two
    huc_flows_rs["max_pixels"] = huc_flows_rs[
        ['count_10000', 'count_1000', 'count_100', 'count_10', 'count_1', 'count_0']
    ].idxmax(axis=1)

    # Set column to the maximum flow out of all pixels
    huc_flows_rs["max_flow"] = (
        huc_flows_rs[['mean_10000', 'mean_1000', 'mean_100', 'mean_10', 'mean_1', 'mean_0']]
        .max(axis=1)
        .astype(float)
    )

    # Set column to discharge associated with the max # of pixels & remove excess columns
    huc_flows_rs["max_pixels"] = huc_flows_rs.apply(
        lambda row: row[f"mean_{row.max_pixels.split('_')[1]}"], axis=1
    )
    huc_flows_rs = huc_flows_rs.drop(
        columns=['count_10000', 'count_1000', 'count_100', 'count_10', 'count_1', 'count_0']
    )

    # For high stream orders override to largest flow
    huc_flows_rs.loc[huc_flows_rs["streamorder"] <= 2, "discharge"] = huc_flows_rs["max_pixels"]
    huc_flows_rs.loc[huc_flows_rs["streamorder"] > 2, "discharge"] = huc_flows_rs["max_flow"]

    # Apply smoothing to get rid of outliers
    huc_flows_out = huc_flows_rs.groupby(by=["mainstemlp", "streamorder"], group_keys=False).apply(
        smooth_level_path
    )

    # Apply confluence check to fix misaligned flows - Set confluence errors to -9999
    map_index = huc_flows_out.set_index("flowpath_id")["discharge"]
    huc_flows_out["to_q"] = huc_flows_out["flowpath_toid"].map(map_index)
    huc_flows_conf = (
        huc_flows_out.groupby(by=["mainstemlp", "streamorder"], group_keys=False)
        .apply(low_order_confluence_check)
        .drop(columns=["to_q"])
    )

    # Replace confluence errors with the next highest value
    huc_flows_conf.loc[huc_flows_conf["fixed_q"] == -9999, "fixed_q"] = huc_flows_conf.loc[
        huc_flows_conf["fixed_q"] == -9999
    ][['mean_10000', 'mean_1000', 'mean_100', 'mean_10', 'mean_1', 'mean_0']].apply(
        lambda row: row.sort_values(ascending=False).iloc[1], axis=1
    )

    # Check to make sure next no values are outliers if so set to nan
    huc_flows_conf.loc[huc_flows_conf["fixed_q"] < (huc_flows_conf["median"] * 0.01), "fixed_q"] = np.nan

    # huc_flows_conf = huc_flows_conf.sort_values(by = "hydroseq", ascending = False).reset_index()
    huc_flows_conf["final_q"] = (
        huc_flows_conf.groupby(by=["mainstemlp", "streamorder"])["fixed_q"]
        .rolling(5, min_periods=1, center=True, win_type="triang")
        .mean()
        .reset_index(level=["mainstemlp", "streamorder"], drop=True)
    )

    export_flow = huc_flows_conf[["flowpath_id", "final_q"]].rename(
        columns={"flowpath_id": "feature_id", "final_q": "discharge"}
    )
    export_flow["feature_id"] = export_flow["feature_id"].astype(int)

    output_path = f"{os.path.splitext(output)[0]}_{model}{os.path.splitext(output)[1]}"
    export_flow.to_csv(output_path, index=False)


def conflate_all_models(hucs, huc_file, hydrofabric_file, output, timestep, min_order):
    """
    Function for conflating the FLASH output flow values to the Hydrofabric reference flowlines to obtain a flow file
    for each model (CREST, SAC-SMA, and Hydrophobic) to use to generate HAND FIM. This Function requires a list of hucs
    to run and a path to export flow files for each of the three models.

    Args:

        hucs (str): HUCs to extract FLASH flow for.
        huc_file (str): Path to HUC geopackage, either full CONUS or a subset.
        hydrofabric_file (str): Path to hydrofabric flowpaths geopackage. Must be reference hydrofabric v3.0 or newer.
        output (str): Path and base name to output flow files. Ex. "/user/Documents/flow_file.csv"
        timestep (str): Timestep to pull data from. Pulls either "latest" or archived data using a specific timestep
                        with the format YYYYMMDD-HHMMSS. Ex. 20250704-083000
        min_order (str): Minimum stream order to use in conflating flow.

    Example Usage:
    python /foss_fim/tools/flashfim/conflate_flash_flows.py -u 12100201 12090201 -b /user/Documents/HUC8_CONUS.gpkg
    -hf /user/Documents/hydrofabric_reference_2_3.gpkg -o /user/Documents/latest_flow.csv -t 20250704-083000 -m 3

    """
    # Check that input paths exist
    if os.path.exists(huc_file) == False:
        raise Exception(f"Input HUC file {huc_file} does not exist.")

    if os.path.exists(hydrofabric_file) == False:
        raise Exception(f"Input hydrofabric file:{hydrofabric_file} does not exist.")

    if os.path.exists(os.path.dirname(output)) == False:
        os.makedirs(os.path.dirname(output), exist_ok=True)

    # Subset hydrofabric to HUCs of interest
    huc8 = gpd.read_file(huc_file, engine="pyogrio")
    huc8 = huc8.loc[huc8["HUC8"].astype(str).isin(hucs)]
    huc_flows = gpd.read_file(hydrofabric_file, mask=huc8.geometry, engine="pyogrio")

    # Conflate Flows
    flash_flow_conflation(
        model="CREST", timestep=timestep, huc_flows=huc_flows, output=output, min_order=min_order
    )
    flash_flow_conflation(
        model="SAC", timestep=timestep, huc_flows=huc_flows, output=output, min_order=min_order
    )
    flash_flow_conflation(
        model="HP", timestep=timestep, huc_flows=huc_flows, output=output, min_order=min_order
    )


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Tool to conflate flow from FLASH raster to NWM flowlines")

    parser.add_argument(
        "-u", "--hucs", help="List of HUCs to run", required=True, default="", type=str, nargs="+"
    )
    parser.add_argument(
        "-b",
        "--huc-file",
        help="Path to HUC geopackage, CONUS scale or subset.",
        required=True,
        default="",
        type=str,
    )
    parser.add_argument(
        "-hf",
        "--hydrofabric-file",
        help="Path to hydrofabric flowpaths geopackage. Must be reference hydrofabric v3.0 or newer.",
        required=True,
        default="",
        type=str,
    )

    parser.add_argument("-o", "--output", help="Output flow file.", required=True, default=None, type=str)
    parser.add_argument(
        "-t",
        "--timestep",
        help="Timestep to pull FLASH data for in 10 minute intervals and UTC time. Defaults to latest. Ex. 20250704-083000 or YYYYMMDD-HHMMSS",
        required=False,
        default="latest",
        type=str,
    )
    parser.add_argument(
        '-m',
        '--min-order',
        help='Minimum size streamorder to consider when conflating flow. Defaults to 2.',
        required=False,
        type=int,
        default=2,
    )

    start = timer()

    conflate_all_models(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
