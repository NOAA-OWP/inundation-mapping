#!/usr/bin/env python3
import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tools_shared_functions import (
    aggregate_wbd_hucs,
    flow_data,
    get_metadata,
    get_nwm_segs,
    get_thresholds,
    mainstem_nwm_segs,
)

sys.path.append("/foss_fim/src")
from utils.shared_variables import VIZ_PROJECTION

EVALUATED_SITES_CSV = r"/data/inputs/ahps_sites/evaluated_ahps_sites.csv"


def get_env_paths():
    load_dotenv()
    # import variables from .env file
    API_BASE_URL = os.getenv("API_BASE_URL")
    WBD_LAYER = os.getenv("WBD_LAYER")
    return API_BASE_URL, WBD_LAYER


def generate_catfim_flows(workspace, nwm_us_search, nwm_ds_search):
    """
    This will create static flow files for all nws_lids and save to the
    workspace directory with the following format:
    huc code
        nws_lid_code
            threshold (action/minor/moderate/major if they exist/are defined by WRDS)
                flow file (ahps_{lid code}_huc_{huc 8 code}_flows_{threshold}.csv)

    This will use the WRDS API to get the nwm segments as well as the flow
    values for each threshold at each nws_lid and then create the necessary
    flow file to use for inundation mapping.

    Parameters
    ----------
    workspace : STR
        Location where output flow files will exist.
    nwm_us_search : STR
        Upstream distance (in miles) for walking up NWM network.
    nwm_ds_search : STR
        Downstream distance (in miles) for walking down NWM network.
    wbd_path : STR
        Location of HUC geospatial data (geopackage).

    Returns
    -------
    None.

    """

    all_start = time.time()
    # Define workspace and wbd_path as a pathlib Path. Convert search distances to integer.
    workspace = Path(workspace)
    nwm_us_search = int(nwm_us_search)
    nwm_ds_search = int(nwm_ds_search)
    metadata_url = f"{API_BASE_URL}/metadata"
    threshold_url = f"{API_BASE_URL}/nws_threshold"
    ###################################################################

    # Create workspace
    workspace.mkdir(parents=True, exist_ok=True)

    print("Retrieving metadata...")
    # Get metadata for 'CONUS'
    conus_list, conus_dataframe = get_metadata(
        metadata_url,
        select_by="nws_lid",
        selector=["all"],
        must_include="nws_data.rfc_forecast_point",
        upstream_trace_distance=nwm_us_search,
        downstream_trace_distance=nwm_ds_search,
    )

    # Get metadata for Islands
    islands_list, islands_dataframe = get_metadata(
        metadata_url,
        select_by="state",
        selector=["HI", "PR"],
        must_include=None,
        upstream_trace_distance=nwm_us_search,
        downstream_trace_distance=nwm_ds_search,
    )

    # Append the dataframes and lists
    all_lists = conus_list + islands_list

    print("Determining HUC using WBD layer...")
    # Assign HUCs to all sites using a spatial join of the FIM 3 HUC layer.
    # Get a dictionary of hucs (key) and sites (values) as well as a GeoDataFrame
    # of all sites used later in script.
    huc_dictionary, out_gdf = aggregate_wbd_hucs(
        metadata_list=all_lists, wbd_huc8_path=WBD_LAYER
    )

    # Get all possible mainstem segments
    print("Getting list of mainstem segments")
    # Import list of evaluated sites
    print(EVALUATED_SITES_CSV)
    print(os.path.exists(EVALUATED_SITES_CSV))
    list_of_sites = pd.read_csv(EVALUATED_SITES_CSV)["Total_List"].to_list()
    # The entire routine to get mainstems is hardcoded in this function.
    ms_segs = mainstem_nwm_segs(metadata_url, list_of_sites)

    # Loop through each huc unit, first define message variable and flood categories.
    all_messages = []
    flood_categories = ["action", "minor", "moderate", "major", "record"]
    for huc in huc_dictionary:
        print(f"Iterating through {huc}")
        # Get list of nws_lids
        nws_lids = huc_dictionary[huc]
        # Loop through each lid in list to create flow file
        for lid in nws_lids:
            # Convert lid to lower case
            lid = lid.lower()
            # Get stages and flows for each threshold from the WRDS API. Priority given to USGS calculated flows.
            stages, flows = get_thresholds(
                threshold_url=threshold_url,
                select_by="nws_lid",
                selector=lid,
                threshold="all",
            )
            # Check if stages are supplied, if not write message and exit.
            if all(stages.get(category, None) == None for category in flood_categories):
                message = f"{lid}:missing threshold stages"
                all_messages.append(message)
                continue
            # Check if calculated flows are supplied, if not write message and exit.
            if all(flows.get(category, None) == None for category in flood_categories):
                message = f"{lid}:missing calculated flows"
                all_messages.append(message)
                continue

            # find lid metadata from master list of metadata dictionaries (line 66).
            metadata = next(
                (
                    item
                    for item in all_lists
                    if item["identifiers"]["nws_lid"] == lid.upper()
                ),
                False,
            )

            # Get mainstem segments of LID by intersecting LID segments with known mainstem segments.
            segments = get_nwm_segs(metadata)
            site_ms_segs = set(segments).intersection(ms_segs)
            segments = list(site_ms_segs)
            # if no segments, write message and exit out
            if not segments:
                print(f"{lid} no segments")
                message = f"{lid}:missing nwm segments"
                all_messages.append(message)
                continue
            # For each flood category
            for category in flood_categories:
                # Get the flow
                flow = flows[category]
                # If there is a valid flow value, write a flow file.
                if flow:
                    # round flow to nearest hundredth
                    flow = round(flow, 2)
                    # Create the guts of the flow file.
                    flow_info = flow_data(segments, flow)
                    # Define destination path and create folders
                    output_file = (
                        workspace
                        / huc
                        / lid
                        / category
                        / (f"ahps_{lid}_huc_{huc}_flows_{category}.csv")
                    )
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    # Write flow file to file
                    flow_info.to_csv(output_file, index=False)
                else:
                    message = f"{lid}:{category} is missing calculated flow"
                    all_messages.append(message)

            # Get various attributes of the site.
            lat = float(metadata["usgs_preferred"]["latitude"])
            lon = float(metadata["usgs_preferred"]["longitude"])
            wfo = metadata["nws_data"]["wfo"]
            rfc = metadata["nws_data"]["rfc"]
            state = metadata["nws_data"]["state"]
            county = metadata["nws_data"]["county"]
            name = metadata["nws_data"]["name"]
            flow_units = flows["units"]
            flow_source = flows["source"]
            stage_units = stages["units"]
            stage_source = stages["source"]
            wrds_timestamp = stages["wrds_timestamp"]
            nrldb_timestamp = metadata["nrldb_timestamp"]
            nwis_timestamp = metadata["nwis_timestamp"]

            # Create a csv with same information as shapefile but with each threshold as new record.
            csv_df = pd.DataFrame()
            for threshold in flood_categories:
                line_df = pd.DataFrame(
                    {
                        "nws_lid": [lid],
                        "name": name,
                        "WFO": wfo,
                        "rfc": rfc,
                        "huc": [huc],
                        "state": state,
                        "county": county,
                        "magnitude": threshold,
                        "q": flows[threshold],
                        "q_uni": flows["units"],
                        "q_src": flow_source,
                        "stage": stages[threshold],
                        "stage_uni": stages["units"],
                        "s_src": stage_source,
                        "wrds_time": wrds_timestamp,
                        "nrldb_time": nrldb_timestamp,
                        "nwis_time": nwis_timestamp,
                        "lat": [lat],
                        "lon": [lon],
                    }
                )
                csv_df = csv_df.append(line_df)
            # Round flow and stage columns to 2 decimal places.
            csv_df = csv_df.round({"q": 2, "stage": 2})

            # If a site folder exists (ie a flow file was written) save files containing site attributes.
            output_dir = workspace / huc / lid
            if output_dir.exists():
                # Export DataFrame to csv containing attributes
                csv_df.to_csv(output_dir / f"{lid}_attributes.csv", index=False)
            else:
                message = f"{lid}:missing all calculated flows"
                all_messages.append(message)

    print("wrapping up...")
    # Recursively find all *_attributes csv files and append
    csv_files = list(workspace.rglob("*_attributes.csv"))
    all_csv_df = pd.DataFrame()
    for csv in csv_files:
        # Huc has to be read in as string to preserve leading zeros.
        temp_df = pd.read_csv(csv, dtype={"huc": str})
        all_csv_df = all_csv_df.append(temp_df, ignore_index=True)
    # Write to file
    all_csv_df.to_csv(workspace / "nws_lid_attributes.csv", index=False)

    # This section populates a shapefile of all potential sites and details
    # whether it was mapped or not (mapped field) and if not, why (status field).

    # Preprocess the out_gdf GeoDataFrame. Reproject and reformat fields.
    viz_out_gdf = out_gdf.to_crs(VIZ_PROJECTION)
    viz_out_gdf.rename(
        columns={
            "identifiers_nwm_feature_id": "nwm_seg",
            "identifiers_nws_lid": "nws_lid",
            "identifiers_usgs_site_code": "usgs_gage",
        },
        inplace=True,
    )
    viz_out_gdf["nws_lid"] = viz_out_gdf["nws_lid"].str.lower()

    # Using list of csv_files, populate DataFrame of all nws_lids that had
    # a flow file produced and denote with "mapped" column.
    nws_lids = [file.stem.split("_attributes")[0] for file in csv_files]
    lids_df = pd.DataFrame(nws_lids, columns=["nws_lid"])
    lids_df["mapped"] = "yes"

    # Identify what lids were mapped by merging with lids_df. Populate
    #'mapped' column with 'No' if sites did not map.
    viz_out_gdf = viz_out_gdf.merge(lids_df, how="left", on="nws_lid")
    viz_out_gdf["mapped"] = viz_out_gdf["mapped"].fillna("no")

    # Write messages to DataFrame, split into columns, aggregate messages.
    messages_df = pd.DataFrame(all_messages, columns=["message"])
    messages_df = (
        messages_df["message"]
        .str.split(":", n=1, expand=True)
        .rename(columns={0: "nws_lid", 1: "status"})
    )
    status_df = (
        messages_df.groupby(["nws_lid"])["status"].apply(", ".join).reset_index()
    )

    # Join messages to populate status field to candidate sites. Assign
    # status for null fields.
    viz_out_gdf = viz_out_gdf.merge(status_df, how="left", on="nws_lid")
    viz_out_gdf["status"] = viz_out_gdf["status"].fillna(
        "all calculated flows available"
    )

    # Filter out columns and write out to file
    viz_out_gdf = viz_out_gdf.filter(
        ["nws_lid", "usgs_gage", "nwm_seg", "HUC8", "mapped", "status", "geometry"]
    )
    viz_out_gdf.to_file(workspace / "nws_lid_flows_sites.shp")

    # time operation
    all_end = time.time()
    print(f"total time is {round((all_end - all_start)/60),1} minutes")


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Create forecast files for all nws_lid sites"
    )
    parser.add_argument(
        "-w",
        "--workspace",
        help="Workspace where all data will be stored.",
        required=True,
    )
    parser.add_argument(
        "-u",
        "--nwm_us_search",
        help="Walk upstream on NWM network this many miles",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--nwm_ds_search",
        help="Walk downstream on NWM network this many miles",
        required=True,
    )
    args = vars(parser.parse_args())

    # Run get_env_paths and static_flow_lids
    API_BASE_URL, WBD_LAYER = get_env_paths()
    generate_catfim_flows(**args)
