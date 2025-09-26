import argparse
import os
from timeit import default_timer as timer

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats


def smooth_level_path(lp_order_flows):

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
    if len(lp_order_group) > 0:
        lp_order_group = lp_order_group.sort_values(by="hydroseq", ascending=False)
        pre_confluence_flows = lp_order_group.iloc[-4:, :]
        confluence_q = pre_confluence_flows.iloc[-1].to_q

        pre_confluence_flows = pre_confluence_flows.assign(
            threshold=pre_confluence_flows["discharge"] / confluence_q > 0.9
        )
        pre_confluence_ids = pre_confluence_flows.loc[pre_confluence_flows["threshold"] == True]["id"]
        lp_order_group.loc[lp_order_group["id"].isin(pre_confluence_ids), "fixed_q"] = -9999
        lp_order_group.loc[~lp_order_group["id"].isin(pre_confluence_ids), "fixed_q"] = lp_order_group[
            "discharge"
        ]

    return lp_order_group


def flash_flow_conflation(model, huc_flows, output, timestep, min_order):
    """
    This function conflates the flow predicted by FLASH outputs to the Reference Hydrofabric version 2.2 to enable the
    generation of FIM for each forecast.
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
    huc_flows = huc_flows.loc[huc_flows["streamorde"] >= min_order]
    huc_flows_buffer = huc_flows.assign(geometry=huc_flows.buffer(500, cap_style="square"))[
        ["id", "toid", "lengthkm", "streamorde", "mainstemlp", "hydroseq", "geometry"]
    ]

    # Calculate zonal stats for each order of magnitude
    ranges = [[10000, 100000], [1000, 10000], [100, 1000], [10, 100], [1, 10], [0, 1]]
    huc_flows_rs = huc_flows_buffer

    for r_min, r_max in ranges:
        with rasterio.open(flash_raster_url) as src:
            band = src.read(1)
            reclass = np.where(np.logical_and(band > r_min, band < r_max), band, np.nan)
            affine = src.transform

            src_crs = src.crs
            huc_flows_buffer = huc_flows_buffer.to_crs(src_crs)

            # Raster Stats Using all touched cells within the buffer
            raster_stats_buf = zonal_stats(
                huc_flows_buffer,
                reclass,
                affine=affine,
                stats=["mean", "sum", "count"],
                all_touched=True,
                geojson_out=True,
            )

            rsb_df = gpd.GeoDataFrame.from_features(raster_stats_buf)[["id", "mean", "count"]].astype(float)
            huc_flows_rs = pd.merge(huc_flows_rs, rsb_df, on="id", suffixes=("", f"_{r_min}"))
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
    huc_flows_rs.loc[huc_flows_rs["streamorde"] <= 2, "discharge"] = huc_flows_rs["max_pixels"]
    huc_flows_rs.loc[huc_flows_rs["streamorde"] > 2, "discharge"] = huc_flows_rs["max_flow"]

    # Apply smoothing to get rid of outliers
    huc_flows_out = huc_flows_rs.groupby(by=["mainstemlp", "streamorde"], group_keys=False).apply(
        smooth_level_path
    )

    # Apply confluence check to fix misaligned flows - Set confluence errors to -9999
    map_index = huc_flows_out.set_index('id')["discharge"]
    huc_flows_out["to_q"] = huc_flows_out["toid"].map(map_index)
    huc_flows_conf = (
        huc_flows_out.groupby(by=["mainstemlp", "streamorde"], group_keys=False)
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
        huc_flows_conf.groupby(by=["mainstemlp", "streamorde"])["fixed_q"]
        .rolling(5, min_periods=1, center=True, win_type="triang")
        .mean()
        .reset_index(level=["mainstemlp", "streamorde"], drop=True)
    )

    export_flow = huc_flows_conf[["id", "final_q"]].rename(
        columns={"id": "feature_id", "final_q": "discharge"}
    )
    export_flow["feature_id"] = export_flow["feature_id"].astype(int)

    output_path = f"{os.path.splitext(output)[0]}_{model}{os.path.splitext(output)[1]}"
    export_flow.to_csv(output_path, index=False)


def conflate_all_models(hucs, output, timestep, min_order):
    huc8 = gpd.read_file("/data/inputs/wbd/WBD_National_HUC8_EPSG_5070_HAND_domain.gpkg", engine="pyogrio")
    huc8 = huc8.loc[huc8["HUC8"].astype(str).isin(hucs)]
    huc_flows = gpd.read_file(
        "/rdp-user/Documents/hydrofabric/conus_reference.gpkg",
        layer="flowpaths",
        mask=huc8.geometry,
        engine="pyogrio",
    )
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

    parser.add_argument("-u", "--hucs", help="HUCs to run", required=True, default="", type=str, nargs="+")

    parser.add_argument("-o", "--output", help="Output flow file.", required=True, default=None, type=str)
    parser.add_argument(
        "-t",
        "--timestep",
        help="Timestep to pull FLASH data for in UTC time. Defaults to latest. Ex. 20250704-080000 or YYYYMMDD-HHMMSS",
        required=False,
        default="latest",
        type=str,
    )
    parser.add_argument(
        '-m', '--min-order', help='Minimum size streamorder to consider.', required=False, type=int, default=2
    )

    start = timer()

    conflate_all_models(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
