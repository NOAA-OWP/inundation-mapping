#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

import geopandas as gpd

from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS


gpd.options.io_engine = "pyogrio"

# NOTE:
# Sep 2024. This file might be deprecated as no code calls it


def clip_wbd_to_dem_domain(dem: str, wbd_in: str, wbd_out: str, huc_level: int):
    """
    Clips Watershed Boundary Dataset (WBD) to DEM domain

    Parameters
    ----------
    dem: str
        Path to DEM domain file
    wbd_in: str
        Path to WBD file input
    wbd_out: str
        Path to WBD file output
    huc_level: int
        HUC level
    """

    # Erase area outside 3DEP domain
    if Path(wbd_in).is_file() and Path(dem).is_file():
        layer = f'WBDHU{huc_level}'

        # Read input files
        wbd = gpd.read_file(wbd_in, layer=layer)
        dem_domain = gpd.read_file(dem)

        wbd = gpd.clip(wbd, dem_domain)

        # Write output file
        wbd.to_file(wbd_out, layer=layer, crs=DEFAULT_FIM_PROJECTION_CRS, driver='GPKG', engine='fiona')


if __name__ == '__main__':

    # Example:
    # preprocess_wbd.py -d /data/inputs/3dep_dems/10m_5070/20240916//HUC6_dem_domain.gpkg
    #  -w /data/inputs/wbd/WBD_National_EPSG_5070.gpkg
    #  -o /data/inputs/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg
    #  -l 8

    # WATCH FOR Alaska as well.  During the 3dep download of 20240916, it did not include
    # Alaska. That one is in data/inputs/3dep_dems/10m_South_Alaska/20240912/

    parser = argparse.ArgumentParser(description='Clip WBD to DEM domain')
    parser.add_argument('-d', '--dem', help='Path to DEM', type=str, required=True)
    parser.add_argument('-w', '--wbd-in', help='Input WBD filename', type=str, required=True)
    parser.add_argument('-o', '--wbd-out', help='Output WBD filename', type=str, required=True)
    parser.add_argument('-l', '--huc-level', help='HUC level', type=int, required=True)

    args = vars(parser.parse_args())

    clip_wbd_to_dem_domain(**args)
