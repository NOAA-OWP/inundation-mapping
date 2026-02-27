from __future__ import annotations

import argparse
import re
import os
import zipfile
from pathlib import Path

import geopandas as gpd
import requests
from dotenv import load_dotenv

# Website that shows the official "latest per state"
PAGE_URL = "https://disasters.geoplatform.gov/USA_Structures/"

srcDir = os.getenv('srcDir')
load_dotenv(f'{srcDir}/bash_variables.env')

DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
GUAM_CRS = os.getenv('GUAM_CRS')
AMERICAN_SAMOA_CRS = os.getenv('AMERICAN_SAMOA_CRS')


def target_crs_for_state(state: str) -> str:
    state = state.upper()
    if state == "AK":
        return ALASKA_CRS
    if state == "GU":
        return GUAM_CRS
    if state == "AS":
        return AMERICAN_SAMOA_CRS
    return DEFAULT_FIM_PROJECTION_CRS


def pull_gdb_files(gdb_dir):
    gdb_dir.mkdir(parents=True, exist_ok=True)

    print("Fetching webpage...")
    html = requests.get(PAGE_URL, timeout=60).text

    zip_urls = sorted(set(re.findall(r"https://fema-femadata\.s3\.amazonaws\.com/[^\"]+\.zip", html)))
    print(f"Found {len(zip_urls)} ZIP files listed on website.")

    for url in zip_urls:
        fname = url.split("/")[-1]
        zip_path = gdb_dir / fname

        print(f"Downloading: {fname}")
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        extract_dir = gdb_dir / zip_path.stem

        print(f"Unzipping to: {extract_dir}")
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        if zip_path.exists():
            zip_path.unlink()

    print(f"FEMA buildings GDB files saved in: {gdb_dir.resolve()}")


def convert_gdb_to_parquet(gdb_dir, parquet_dir):
    print(f"Started converting states gdb files into parquet files...")
    parquet_dir.mkdir(parents=True, exist_ok=True)

    gdb_paths = sorted(p for p in gdb_dir.rglob("*.gdb") if p.is_dir() and "__MACOSX" not in p.parts)
    if not gdb_paths:
        raise RuntimeError(f"No .gdb folders found under {gdb_dir}")

    print(f"Found {len(gdb_paths)} GDB folders to convert to parquet.")

    for gdb in gdb_paths:
        state = gdb.stem.split("_")[0].upper()
        print(f"\n[{state}] {gdb}")

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
        print(f"[{state}] Writing -> {out_path}  (CRS={tgt_crs})")
        gdf.to_parquet(out_path, index=False, compression="zstd", row_group_size=250_000)


    print("\nDone. Outputs in:", parquet_dir.resolve())


def get_fema_buildings(output_dir: str) -> None:
    gdb_dir = Path(output_dir) / "states_gdb"
    parquet_dir = Path(output_dir) / "states_parquet"

    pull_gdb_files(gdb_dir)
    convert_gdb_to_parquet(gdb_dir, parquet_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download FEMA building GDBs and convert to per-state parquet.")
    parser.add_argument(
        "-o",
        "--output_dir",
        help="REQUIRED: root output folder. Uses states_gdb/ and states_parquet/ subfolders.",
        required=True,
    )

    args = vars(parser.parse_args())
    get_fema_buildings(**args)
