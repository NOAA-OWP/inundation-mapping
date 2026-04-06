import argparse
import os
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import rasterio


def optimized_flash_flow_conflation(lookup_table, timestep, output):
    """
    Function for conflating the FLASH output flow values to the Hydrofabric feature_ids to obtain a flow file
    for each model (CREST, SAC-SMA, and Hydrophobic) to use to generate HAND FIM. This Function requires a lookup table
    of the index of pixels related to each feature_id and a timestep.

    Args:
        lookup_table (str): Lookup table defining FLASH pixel index for each feature_id.
        timestep (str): Timestep to pull data from. Pulls either "latest" or archived data using a specific timestep
                        with the format YYYYMMDD-HHMMSS. Ex. 20250704-083000
        output (str): Path and base name to output flow files. Ex. "/user/Documents/flow_file.csv"


    Example Usage:
    python /foss_fim/tools/flashfim/optimized_flash_conflation.py -l /user/Documents/flash_lookup_table_final.csv
    -o /user/Documents/latest_flow.csv -t 20250704-083000

    """

    if isinstance(lookup_table, (str, os.PathLike)):
        lookup_table = pd.read_csv(lookup_table)
    elif not isinstance(lookup_table, pd.DataFrame):
        raise ValueError(
            "The lookup_table must be a pandas DataFrame or the path to a CSV file that can be opened as a pandas DataFrame."
        )

    lookup_table = lookup_table.dropna(subset=["coordinates"])

    for model in ["CREST", "SAC", "HP"]:
        if timestep == "latest":
            url = f"https://mrms.ncep.noaa.gov/2D/FLASH/{model}_MAXSTREAMFLOW/MRMS_FLASH_{model}_MAXSTREAMFLOW.latest.grib2.gz"
        else:
            yr = timestep.split("-")[0][:4]
            mo = timestep.split("-")[0][4:6]
            day = timestep.split("-")[0][6:]
            url = f"https://mtarchive.geol.iastate.edu/{yr}/{mo}/{day}/mrms/ncep/FLASH/{model}_MAXSTREAMFLOW/{model}_MAXSTREAMFLOW_00.00_{timestep}.grib2.gz"

        flash_raster_url = f"/vsigzip//vsicurl/{url}"

        with rasterio.open(flash_raster_url) as src:
            band = src.read(1)

        rows = lookup_table["row_idx"].astype(int).values
        cols = lookup_table["col_idx"].astype(int).values

        lookup_table["discharge"] = band[rows, cols]

        output_path = f"{os.path.splitext(output)[0]}_{timestep}_{model}{os.path.splitext(output)[1]}"

        lookup_table["feature_id"] = lookup_table["feature_id"].astype("int64")
        lookup_table[["feature_id", "discharge"]].to_csv(output_path, index=False)


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Tool to conflate flow from FLASH raster to NWM flowlines")

    parser.add_argument(
        '-l',
        '--lookup_table',
        help='Lookup table defining FLASH pixel index for each feature_id.',
        required=True,
        default=None,
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


start = timer()

optimized_flash_flow_conflation(**vars(parser.parse_args()))

print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
