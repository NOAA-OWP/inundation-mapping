from __future__ import annotations

import argparse
import logging
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import requests
from dotenv import load_dotenv

from src.utils.shared_functions import to_hilbert_parquet


# Website that shows the official "latest per state"
PAGE_URL = "https://disasters.geoplatform.gov/USA_Structures/"

srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')

DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
GUAM_CRS = os.getenv('GUAM_CRS')
AMERICAN_SAMOA_CRS = os.getenv('AMERICAN_SAMOA_CRS')


def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"get_fema_buildings-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)

    logging.info(f'Started : {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


def target_crs_for_state(state: str) -> str:
    state = state.upper()
    if state == "AK":
        return ALASKA_CRS
    if state == "GU":
        return GUAM_CRS
    if state == "AS":
        return AMERICAN_SAMOA_CRS
    return DEFAULT_FIM_PROJECTION_CRS


def pull_gdb_files(gdb_dir, selected_states):
    gdb_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Fetching webpage...")
    html = requests.get(PAGE_URL, timeout=60).text

    zip_urls = sorted(set(re.findall(r"https://fema-femadata\.s3\.amazonaws\.com/[^\"]+\.zip", html)))
    logging.info(f"Found {len(zip_urls)} ZIP files listed on website.")

    for url in zip_urls:
        fname = url.split("/")[-1]
        match = re.search(r"([A-Z]{2})\.zip$", fname, flags=re.IGNORECASE)
        if not match:
            logging.warning(f"Skipping unrecognized ZIP naming pattern: {fname}")
            continue
        state = match.group(1).upper()
        if selected_states and state not in selected_states:
            continue

        zip_path = gdb_dir / fname

        logging.info(f"Downloading: {fname}")
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        extract_dir = gdb_dir / zip_path.stem

        logging.info(f"Unzipping to: {extract_dir}")
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        if zip_path.exists():
            zip_path.unlink()

    logging.info(f"FEMA buildings GDB files saved in: {gdb_dir.resolve()}")


def convert_gdb_to_parquet(gdb_dir, parquet_dir, selected_states):
    logging.info("Started converting states gdb files into parquet files...")
    parquet_dir.mkdir(parents=True, exist_ok=True)

    gdb_paths = sorted(p for p in gdb_dir.rglob("*.gdb") if p.is_dir() and "__MACOSX" not in p.parts)
    if not gdb_paths:
        raise RuntimeError(f"No .gdb folders found under {gdb_dir}")

    logging.info(f"Found {len(gdb_paths)} GDB folders to convert to parquet.")

    for gdb in gdb_paths:
        state = gdb.stem.split("_")[0].upper()
        if selected_states and state not in selected_states:
            continue
        logging.info(f"[{state}] {gdb}")

        layers = gpd.list_layers(gdb)
        if len(layers) != 1:
            raise RuntimeError(f"[{state}] Expected exactly 1 layer in {gdb}, found {len(layers)}")

        layer_name = layers["name"].iloc[0]
        gdf = gpd.read_file(gdb, layer=layer_name)

        if gdf.empty:
            raise RuntimeError(f"[{state}] Layer is empty: {gdb} | {layer_name}")
        if gdf.crs is None:
            raise RuntimeError(f"[{state}] Missing CRS: {gdb}")

        tgt_crs = target_crs_for_state(state)
        if str(gdf.crs) != tgt_crs:
            gdf = gdf.to_crs(tgt_crs)

        out_path = parquet_dir / f"{state}_structures.parquet"
        logging.info(f"[{state}] Writing -> {out_path}  (CRS={tgt_crs})")
        to_hilbert_parquet(gdf, out_path, index=False, compression="zstd", row_group_size=250_000)

    logging.info(f"Done. Outputs in: {parquet_dir.resolve()}")


def get_fema_buildings(output_dir: str, state: str = "") -> None:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    __setup_logger(output_root)
    start_time = datetime.now(timezone.utc)

    gdb_dir = output_root / "states_gdb"
    parquet_dir = output_root / "states_parquet"
    selected_states = {s.upper() for s in state.split()} if state else None

    logging.info("Started FEMA buildings download/conversion.")
    if selected_states:
        logging.info(f"Selected states: {sorted(selected_states)}")
    else:
        logging.info("Selected states: all states listed on FEMA page")

    pull_gdb_files(gdb_dir, selected_states)
    convert_gdb_to_parquet(gdb_dir, parquet_dir, selected_states)
    end_time = datetime.now(timezone.utc)
    time_duration = end_time - start_time
    logging.info("Completed FEMA buildings download/conversion.")
    logging.info(f"TOTAL RUN TIME: {str(time_duration).split('.')[0]}")


if __name__ == "__main__":
    # Example:
    # python foss_fim/data/buildings/get_fema_buildings.py \
    #     -o data/inputs/fema/buildings/20260306/ \
    #     -s 'IL'
    parser = argparse.ArgumentParser(
        description="Download FEMA building GDBs and convert to per-state parquet."
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        help="REQUIRED: root output folder. Uses states_gdb/ and states_parquet/ subfolders.",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--state",
        help="OPTIONAL: space-delimited list of states/territories in quotes (e.g., 'TX CA').",
        required=False,
        default="",
    )

    args = vars(parser.parse_args())
    get_fema_buildings(**args)
