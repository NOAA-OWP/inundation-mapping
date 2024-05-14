"""
what is the purpose, example usage, link to the PR

"""

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio


# from rasterio import plot as rioplot


# This will make jupyter display all columns of the hydroTable
pd.options.display.max_columns = None


# ------------------------------------------------------
def find_missing_fim_cells_for1huc(fim_dir, huc8_num):

    fim_dir = Path(fim_dir)
    branch0_dir = Path(fim_dir, huc8_num, "branches", "0")
    hydroTable_csv = Path(branch0_dir, "hydroTable_0.csv")
    hydroTable = pd.read_csv(hydroTable_csv)

    rem_tif = Path(branch0_dir, "rem_zeroed_masked_0.tif")
    catchments_tif = Path(branch0_dir, "gw_catchments_reaches_filtered_addedAttributes_0.tif")

    with rasterio.open(catchments_tif) as catchments:
        catchments = catchments.read(1)

    stream_orders = list(hydroTable["order_"].drop_duplicates(keep='first'))
    stream_orders = sorted(stream_orders)

    analysis_data_ordr = []
    for ordr in stream_orders:

        branch0_streams = hydroTable[(hydroTable["order_"] == ordr)]
        num_streams_order = len(branch0_streams)
        branch0_hydroids = list(branch0_streams['HydroID'].drop_duplicates(keep='first'))

        with rasterio.open(rem_tif) as rem:
            rem = rem.read(1)

        # Filter the REM to the catchments of interest
        rem = np.where(~np.isin(catchments, branch0_hydroids), np.nan, rem)

        # Filter the REM again to only the 0 values
        rem_zeros = rem.copy()
        rem_zeros[np.where(rem_zeros != 0)] = np.nan

        cells_remaining = np.count_nonzero(~np.isnan(rem_zeros))
        print("")
        print(f"Analyzing Branch0, {ordr}-order streams FIM in HUC8 {huc8_num}")
        print("")
        print(f"Actual count of cells remaining: {cells_remaining}")

        non_zero_count = (catchments != 0).sum()
        percentage_b0_rem0 = round(100 * (cells_remaining / non_zero_count), 4)
        print("")
        print(
            f"Actual percentage of branch0, {ordr}-order stream cells that have not inundated: {percentage_b0_rem0}%"
        )

        # Finding branch0 hydroIDs that do not have zero rem (never inundate, notches)
        target_hydroids = []
        for hydroid in branch0_hydroids:

            rem_hydroid = rem.copy()
            hydroid_ls = [hydroid]
            rem_hydroid = np.where(~np.isin(catchments, hydroid_ls), np.nan, rem_hydroid)

            rem_hydroid_nonnan = rem_hydroid[~np.isnan(rem_hydroid)]
            cond = min(rem_hydroid_nonnan)

            if cond > 0:
                target_hydroids.append(hydroid)

        print("")
        print(
            f"{len(target_hydroids)} streams between {ordr}-order streams are thalwag notch, including hydroIDs {target_hydroids}"
        )

        analysis = [ordr, target_hydroids, num_streams_order, cells_remaining, percentage_b0_rem0]

        analysis_data_ordr.append(analysis)

    return analysis_data_ordr


# ------------------------------------------------------
def analysis_missing_fim_cells(huc8_dir):

    list_subdir = os.listdir(huc8_dir)
    dir_path_ls = [os.path.join(huc8_dir, subdir) for subdir in list_subdir]

    missing_fim_data = []
    for path1 in dir_path_ls:

        list_subdir = os.listdir(path1)
        huc8 = [stg for stg in list_subdir if stg.isdigit()][0]
        missing_fim = find_missing_fim_cells_for1huc(path1, huc8)

        missing_fim_data.append([huc8, missing_fim])

    # Number of thalwag_notch_streams grouped by stream orders
    thalwag_notch_ord = []
    for ord1 in range(6):
        thalwag_notch = []
        for subls in missing_fim_data:
            if len(subls[1]) >= (ord1 + 1):
                thalwag_notch.append(len(subls[1][ord1][1]))
        thalwag_notch_ord.append(sum(thalwag_notch))

    # Number of streams grouped by stream orders
    streams_ord = []
    for ord1 in range(6):
        streams = []
        for subls in missing_fim_data:
            if len(subls[1]) >= (ord1 + 1):
                streams.append(subls[1][ord1][2])
        streams_ord.append(sum(streams))

    return thalwag_notch_ord, streams_ord


# fim_dir = "/home/rdp-user/outputs/healded_fim_hca_01080107_if2/"
# fim_dir_str = Path(fim_dir)
# huc8_num = "01080107"
huc8_dir = "/efs-drives/fim-dev-efs/fim-home/heidi.safa/outputs/healed-fim-removing-hcas-analysis/"
thalwag_notch_ord, streams_ord = analysis_missing_fim_cells(huc8_dir)

stream_orders = ["1st order", "2nd order", "3rd order", "4th order", "5th order", "6th order"]
percentage_notches = [
    round(100 * (thalwag_notch_ord[i] / streams_ord[i]), 4) for i in range(len(streams_ord))
]

path2savefig = (
    "/efs-drives/fim-dev-efs/fim-home/heidi.safa/outputs/path2savefig_hh_rhcas/Thalwag_Notch_Streams_perc.png"
)

# creating the bar plot
fig = plt.figure(figsize=(20, 10))
colors = [(135, 206, 255)]
plt.bar(stream_orders, percentage_notches, color=colors[0])

plt.xlabel("Stream Orders", fontsize=20)
plt.xticks(fontsize=20)
plt.ylabel("Percentage of Thalwag Notch Streams (%)", fontsize=18)
plt.yticks(fontsize=20)
plt.title("Percentage of catchments that never been Inundated", fontsize=20)
plt.show()
plt.savefig(path2savefig)

if __name__ == "__main__":

    # Parse arguments.
    parser = argparse.ArgumentParser(description="Analysis for missing FIM cells.")
    parser.add_argument(
        "-r",
        dest="fim_dir_str",
        help="Path to directory storing FIM outputs. Type = string",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-w", dest="huc8_num", help="HUC8 that is being analyzsd. Type = string", required=True, type=str
    )
    parser.add_argument(
        "-o", dest="dir_fig", help="Path to save rem_zero. Type = string", required=True, type=str
    )

    # Assign variables from arguments.
    args = vars(parser.parse_args())

    fim_dir = args["fim_dir_str"]
    huc8_num = args["huc8_num"]
    dir_fig = args["dir_fig"]

    if not os.path.exists(fim_dir):
        print("FIM directory: " + fim_dir + " does not exist.")
        quit

    if not os.path.exists(dir_fig):
        os.mkdir(dir_fig)

    analysis_missing_fim_cells(fim_dir, huc8_num, dir_fig)
