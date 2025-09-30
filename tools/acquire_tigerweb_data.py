#!/usr/bin/env python3

"""
Acquires US Census TIGERweb data.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
import urllib
import zipfile
from pathlib import Path

import geopandas as gpd
from pyproj import CRS

from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS


WRITE_KWARGS = {'index': False}


def Acquire_tigerweb_data(
    source: str,
    year: str = "2020",
    target_crs: str | int | CRS = DEFAULT_FIM_PROJECTION_CRS,
    write_path: str | Path | None = None,
    write_kwargs: dict | None = WRITE_KWARGS,
) -> gpd.GeoDataFrame:
    """
    Acquires US Census TIGERweb data.

    Specifically, the Core Based Statistical Areas (CBSA), Combined Statistical Areas (CSA), Metropolitan Divisions (MD), and Urban Areas - Corrected (UA_C).

    Parameters
    ----------
    source : str
        The source of the data. Options are "CBSA", "CSA", "MD", "UA_C".

        CBSA = Core Based Statistical Area
        CSA = Combined Statistical Area
        MD = Metropolitan Division
        UA_C = Urban Areas - Corrected

        Please see refrerences for more information.
    year : str, default = "2020"
        The year of the data.
    target_crs : str or int or CRS, default = foss_fim.src.utils.shared_variables.from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS
        The target coordinate reference system. Use None to not reproject the data.
    write_path : str or Path or None, default = None
        The path to write the data to. Set to None to not write the data.
    write_kwargs : dict or None, default = WRITE_KWARGS
        Keyword arguments to pass to GeoDataFrame.to_file(). Only used if write_path is not None.

    References
    ----------
    [1] [US Census Cartograhic Boundaries](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2020.html#list-tab-1883739534)
    """
    url_prefix_dict = {"2020": "https://www2.census.gov/geo/tiger/GENZ2020/shp/"}

    archive_name_dict = {
        "CBSA": "cb_2020_us_cbsa_500k.zip",
        "CSA": "cb_2020_us_csa_500k.zip",
        "MD": "cb_2020_us_metdiv_500k.zip",
        "UA_C": "cb_2020_us_ua20_corrected_500k.zip",
    }

    archive_name = archive_name_dict[source]
    file_name = os.path.splitext(archive_name)[0] + '.shp'  # swap out extension

    url_prefix = url_prefix_dict[year]
    source_url = urllib.parse.urljoin(url_prefix, archive_name)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_zip_path = os.path.join(tmp, "tmp.zip")
        urllib.request.urlretrieve(source_url, tmp_zip_path)

        with zipfile.ZipFile(tmp_zip_path, "r") as zip_ref:
            zip_ref.extractall(tmp)

        gdf = gpd.read_file(os.path.join(tmp, file_name))

    '''
    # fiona.errors.DriverError: '/vsizip//vsicurl/https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_cbsa_500k.zip' does not exist in the file system,
    # and is not recognized as a supported dataset name.
    # working locally, but not on AWS
    gdal_virtual_file_path = "/vsizip//vsicurl/" + source_url
    gdf = gpd.read_file(gdal_virtual_file_path)
    '''

    # reproject the crs
    if target_crs:
        gdf = gdf.to_crs(target_crs)

    # write the data if write_path is not None
    if write_path:
        if write_kwargs is None:
            write_kwargs = {}
        gdf.to_file(write_path, **write_kwargs)

    return gdf


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Acquires US Census TIGERweb data.")

    parser.add_argument(
        "--source",
        "-s",
        type=str,
        required=True,
        help="The source of the data. Options are 'CBSA', 'CSA', 'MD', 'UA_C'.",
        choices=["CBSA", "CSA", "MD", "UA_C"],
    )

    parser.add_argument(
        "--year", "-y", type=str, required=False, default="2020", help="The year of the data."
    )

    parser.add_argument(
        "--target_crs",
        "-t",
        type=str,
        required=False,
        default=DEFAULT_FIM_PROJECTION_CRS,
        help="The target coordinate reference system. Exclude to not reproject the data.",
    )

    parser.add_argument("--write_path", "-w", type=str, required=False, help="The path to write the data to.")

    parser.add_argument(
        "--write_kwargs",
        "-k",
        type=json.loads,
        required=False,
        help="Keyword arguments to pass to GeoDataFrame.to_file().",
    )

    kwargs = vars(parser.parse_args())

    gdf = Acquire_tigerweb_data(**kwargs)
