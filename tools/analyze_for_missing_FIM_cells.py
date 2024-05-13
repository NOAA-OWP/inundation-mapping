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
from rasterio import plot as rioplot


# This will make jupyter display all columns of the hydroTable
pd.options.display.max_columns = None

# -------------------------------------------
# fim_dir_str = "/home/rdp-user/outputs/healded_fim_hca_01080107_if2/"
# fim_dir_str = Path(fim_dir_str)
# huc8_num = "01080107"


def find_missing_fim_cells(fim_dir_str, huc8_num, dir_fig):

    fim_dir = Path(fim_dir_str)
    branch0_dir = Path(fim_dir, huc8_num, "branches", "0")
    hydroTable_csv = Path(branch0_dir, "hydroTable_0.csv")
    hydroTable = pd.read_csv(hydroTable_csv)

    branch0_streams = hydroTable[(hydroTable["order_"] == 1) | (hydroTable["order_"] == 2)]
    branch0_hydroids = list(branch0_streams['HydroID'].drop_duplicates(keep='first'))

    rem_tif = Path(branch0_dir, "rem_zeroed_masked_0.tif")
    catchments_tif = Path(branch0_dir, "gw_catchments_reaches_filtered_addedAttributes_0.tif")

    with rasterio.open(catchments_tif) as catchments:
        catchments = catchments.read(1)

    with rasterio.open(rem_tif) as rem:
        # rem_nodata = rem.profile['nodata']
        # rem_extent = rioplot.plotting_extent(rem)
        # rem_transform = rem.transform
        # rem_crs = rem.crs
        rem = rem.read(1)

    # Filter the REM to the catchments of interest
    rem = np.where(~np.isin(catchments, branch0_hydroids), np.nan, rem)

    # Filter the REM again to only the 0 values
    rem_zeros = rem.copy()
    rem_zeros[np.where(rem_zeros != 0)] = np.nan

    cells_remaining = np.count_nonzero(~np.isnan(rem_zeros))
    print(f"Actual count of cells remaining: {cells_remaining}")

    non_zero_count = (catchments != 0).sum()
    percentage_b0_rem0 = cells_remaining / non_zero_count
    print(f"Actual percentage of branch0 stream cells that have not inundated: {percentage_b0_rem0}")

    # Finding branch0 hydroIDs that do not have zero rem (never inundate)
    target_hydroids = []
    for hydroid in branch0_hydroids:

        rem_hydroid = rem.copy()
        hydroid_ls = [hydroid]
        rem_hydroid = np.where(~np.isin(catchments, hydroid_ls), np.nan, rem_hydroid)

        rem_hydroid_nonnan = rem_hydroid[~np.isnan(rem_hydroid)]
        cond = min(rem_hydroid_nonnan)

        if cond > 0:
            target_hydroids.append(hydroid)

    print(
        f"{len(target_hydroids)} catchments in HUC {huc8_num} do not inundate, including HUC8s {target_hydroids}"
    )

    """
    # # finding branch0 hydroIDs that have zero rem
    # catchments_rem0 = catchments.copy()
    # catchments_rem0 = np.where(np.isnan(rem_zeros), np.nan, catchments_rem0)

    # catchments_rem0_ls = catchments_rem0[~np.isnan(catchments_rem0)].tolist()
    # hydroids_rem0_ls = list(set(catchments_rem0_ls))
    # hydroids_rem0 = [int(x) for x in hydroids_rem0_ls]

    # # finding inundated branch0 rem and hydroids
    # rem_inund = rem.copy()
    # rem_inund[np.where(rem_inund <= 0)] = np.nan

    # catchments_inund = catchments.copy()
    # catchments_inund = np.where(np.isnan(rem_inund), np.nan, catchments_inund)

    # catchments_inund_ls = catchments_inund[~np.isnan(catchments_inund)].tolist()
    # hydroids_inund_ls = list(set(catchments_inund_ls))
    # hydroids_inund = [int(x) for x in hydroids_inund_ls]

    # hydroIds_branch0_noinund = len(hydroids_inund) - len(list(set(hydroids_inund) & set(hydroids_rem0)))

    # Simple Plotting
    # reaches_gpkg = Path(branch0_dir, "demDerived_reaches_split_filtered_addedAttributes_crosswalked_0.gpkg")
    # reaches = gpd.read_file(reaches_gpkg)
    # plot_bounds = reaches.loc[reaches.HydroID.astype(int) == hydroid].bounds.iloc[0]

    # Plot the zero REM cells
    fig = plt.figure(figsize=(12, 7))
    ax = fig.subplots(1, 1)

    # reaches.plot(ax=ax, color='brown')
    im = ax.imshow(rem_zeros, cmap='bwr', extent=rem_extent, interpolation='none')

    path_fig = Path(dir_fig, 'rem_zeros.png')
    plt.savefig(im, path_fig)

    """

    return [target_hydroids, cells_remaining, percentage_b0_rem0]


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

    find_missing_fim_cells(fim_dir, huc8_num, dir_fig)
