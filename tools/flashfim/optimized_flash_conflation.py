import argparse
import datetime
import os
import pathlib
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import rasterio
from rasterio.env import Env
from rasterio.session import AWSSession


def optimized_flash_flow_conflation(model, lookup_table, domain, timestep):
    """
    Function for conflating the FLASH output flow values to the Hydrofabric feature_ids to obtain a flash flows DataFrame
    for a given model (CREST, SAC-SMA, and Hydrophobic) that can be used to generate HAND FIM. This Function requires 
    a lookup table DataFrame of the index of pixels related to each feature_id and a timestep.

    Args:
        model (str): The model for which to produce a flows DataFrame
        lookup_table (pd.DataFrame): lookup_table (pandas.DataFrame): A pandas DataFrame with feature_id as index and 
                        the following columns: row_idx, col_idx, and area_scale. Available as CSV in the FIM S3 bucket
                        ~/inputs/flash_fim/flash_lookup_table_final.csv
        domain (str): Domain of FLASH model, i.e. "CONUS", "CARIB", "HAWAII", or "GUAM
        timestep (str): Timestep to pull data from. Pulls either "latest" or archived data using a specific timestep
                        with the format YYYYMMDD-HHMMSS. Ex. 20250704-083000
    """
    domain = domain.upper()

    if timestep == "latest":
        print(f" Pulling latest {model} data")
        if domain == "CONUS":
            url = f"/vsigzip//vsicurl/https://mrms.ncep.noaa.gov/2D/FLASH/{model}_MAXSTREAMFLOW/MRMS_FLASH_{model}_MAXSTREAMFLOW.latest.grib2.gz"
        else:
            url = f"/vsigzip//vsicurl/https://mrms.ncep.noaa.gov/2D/{domain}/FLASH_{model}_MAXSTREAMFLOW/MRMS_FLASH_{model}_MAXSTREAMFLOW.latest.grib2.gz"

        with rasterio.open(url) as src:
            band = src.read(1)

    elif domain == "CONUS":  # will ONLY work for CONUS
        time = datetime.datetime.strptime(timestep, "%Y%m%d-%H%M%S")
        url = f"/vsigzip//vsicurl/https://mtarchive.geol.iastate.edu/{time.year}/{time.strftime('%m')}/{time.strftime('%d')}/mrms/ncep/FLASH/{model}_MAXSTREAMFLOW/{model}_MAXSTREAMFLOW_00.00_{timestep}.grib2.gz"
        print(f" Accessing {model} CONUS archive")
        with rasterio.open(url) as src:
            band = src.read(1)  # Read Dataset

    else:  # Archive for all other domains
        time = datetime.datetime.strptime(timestep, "%Y%m%d-%H%M%S")
        s3_url = f'/vsigzip//vsis3/noaa-mrms-pds/{domain}/FLASH_{model}_MAXSTREAMFLOW_00.00/{time.strftime("%Y%m%d")}/MRMS_FLASH_{model}_MAXSTREAMFLOW_00.00_{timestep}.grib2.gz'
        print(f" Accessing {model} oCONUS archive")
        # Explicitly create an anonymous session
        session = AWSSession(aws_unsigned=True)

        with Env(session=session):
            with rasterio.open(s3_url) as src:
                band = src.read(1)  # Read Dataset

    rows = lookup_table["row_idx"].values
    cols = lookup_table["col_idx"].values

    lookup_table["discharge"] = band[rows, cols]

    if domain in ["GUAM", "CARIB", "HAWAII"]:
        print(" Scaling oCONUS flows")
        # For oCONUS domains Scale Q by area factor
        lookup_table["discharge"] = lookup_table["discharge"] * lookup_table["area_scale"]

    return lookup_table[["discharge"]]

if __name__ == "__main__":
    # Example Usage:
    # python /foss_fim/tools/flashfim/optimized_flash_conflation.py -l /inputs/flash_fim/flash_lookup_table_final.csv -d CONUS
    # -o /user/Documents/latest_flow.csv -t 20250704-083000

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
    parser.add_argument(
        '-d',
        '--domain',
        help='Domain of model, i.e. "CONUS", "CARIB", "HAWAII", or "GUAM".',
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

    args = parser.parse_args()

    if not os.path.exists(args.lookup_table):
        raise ValueError(
            "File does not exist. The lookup_table must be a pandas DataFrame or the path to a CSV file that can be opened as a pandas DataFrame."
        )

    # Read in lookup table
    lookup_table = pd.read_csv(
        args.lookup_table,
        usecols=["feature_id", "row_idx", "col_idx", "area_scale"],
        dtype={'feature_id': 'Int64', 'row_idx': 'Int64', 'col_idx': 'Int64'}
        index_col="feature_id"
    ).dropna(subset=["row_idx", "col_idx"])

    # Check for output directory
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    for model in ["CREST", "SAC", "HP"]:
        output_df = optimized_flash_flow_conflation(model, lookup_table, args.domain, args.timestep)
        output_path = output.with_stem(f"{output.stem}_{args.timestep}_{model}")
        output_df.to_csv(output_path)

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
