#!/usr/bin/env python3

"""
Acquires US Census TIGERweb data and converts city names to HUCs list.

Command Used:

dir=/data/misc/lidar; /foss_fim/tools/convert_city_names_to_huc8s.py -s UA_C -c $dir/ngwpc_PI1_lidar_urban_areas_curated.lst -f NAME20 -b $dir/ngwpc_PI1_lidar_hucs.gpkg -a $dir/ngwpc_PI1_lidar_add_hucs.lst -d $dir/ngwpc_PI1_lidar_drop_hucs.lst -hw /data/inputs/huc_lists/ngwpc_PI1_lidar_hucs.lst
"""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Iterable, Tuple

import geopandas as gpd
import pandas as pd
from acquire_tigerweb_data import Acquire_tigerweb_data
from pyproj import CRS

from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS


INPUT_DIR = os.environ.get('inputsDir')

WBD_FILE_PATH = os.path.join(INPUT_DIR, "wbd", "WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg")
HUC_LIST_DIR = os.path.join(INPUT_DIR, "huc_lists")
HUC_LIST = os.path.join(HUC_LIST_DIR, "ngwpc_PI1_lidar_huc8.lst")


def Acquire_city_boundaries_and_convert_city_names_to_HUCs(
    source: str,
    cities: Iterable[str] | str | Path,
    wbd: str | Path | gpd.GeoDataFrame = WBD_FILE_PATH,
    huc_level: int | str = 8,
    year: str = "2020",
    target_crs: str | int | CRS = DEFAULT_FIM_PROJECTION_CRS,
    city_name_field: str = "NAME",
    predicate: str = "intersects",
    additional_hucs: Iterable[str] | str | Path | None = None,
    drop_hucs: Iterable[str] | str | Path | None = None,
    wbd_write_path: str | Path | None = None,
    wbd_write_kwargs: dict | None = None,
    huc_list_write_path: str | Path | None = HUC_LIST,
) -> Tuple[gpd.GeoDataFrame, list]:
    """
    Acquires US Census TIGERweb data and converts city names to HUCs list.

    Returns a GeoDataFrame of the WBDs and a list of HUCs. Also, optionally writes the WBDs and HUC list to file.

    Parameters
    ----------
    source : str
        The source of the data. Options are "CBSA", "CSA", "MD", "UA_C". See `acquire_tigerweb_data.py` for more information.
    cities : Iterable of str or str or Path
        The cities to convert to HUCs. Can be an iterable of city names, a path to a line-deliminted text file with city names, or a single city name. Assumes city, state format.
    wbd : str or Path or gpd.GeoDataFrame, default = WBD_FILE_PATH
        The path to the WBD vector file,  or a GeoDataFrame of the WBD data.
    huc_level : int or str, default = 8
        The HUC level to use. Options are 8.
    year : str, default = "2020"
        The year of the data. See `acquire_tigerweb_data.py` for more information.
    target_crs : str or int or CRS, default = utils.shared_variables.DEFAULT_FIM_PROJECTION_CRS
        The target coordinate reference system. Set to None to not reproject the data. See `acquire_tigerweb_data.py` for more information.
    city_name_field : str, default = "NAME"
        The name of the field in GeoDataFrame that contains the city names.
    predicate : str, default = "intersects"
        The spatial predicate to use to get the geometry of the city. Options are in geopandas.sindex.valid_query_predicates and listed here as 'overlaps', 'covers', 'covered_by', 'contains_properly', 'touches', 'intersects', 'contains', 'crosses', None, and 'within'.
    additional_hucs : Iterable of str or str or Path, default = None
        The additional HUCs to add to the HUC list. Can be an iterable of HUC names, a path to a line-deliminted text file with HUC names, or a single HUC name.
    drop_hucs : Iterable of str or str or Path or None, default = None
        The HUCs to drop from the HUC list. Can be an iterable of HUC names, a path to a line-deliminted text file with HUC names, or a single HUC name.
    wbd_write_path : str or Path or None, default = None
        The path to write the WBD data to. Set to None to not write the data.
    wbd_write_kwargs : dict or None, default = None
        Keyword arguments to pass to GeoDataFrame.to_file(). Only used if write_path is not None.
    huc_list_write_path : str or Path or None, default = HUC_LIST
        The path to write the HUC8 list to.

    Returns
    -------
    gpd.GeoDataFrame
        The GeoDataFrame of the matched cities and their HUCs.
    list
        The list of HUCs.

    Raises
    ------
    TypeError
        If wbd is not a GeoDataFrame, str, or Path.
        If additional_hucs is not a GeoDataFrame, str, or Path.
        If drop_hucs is not a GeoDataFrame, str, or Path.

    References
    ----------
    .. [1] `US Census Cartograhic Boundaries <https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2020.html#list-tab-1883739534>`_
    """

    # acquire the data
    census_boundaries = Acquire_tigerweb_data(source, year, target_crs)

    # get the cities list
    try:
        # reads the file successfully if cities is a str or Path, note that the file is city, state format
        cities_list = pd.read_fwf(cities, header=None, lineterminator='\n', index_col=False, comment='#')[
            0
        ].to_list()
    except FileNotFoundError:
        if isinstance(cities, str) | isinstance(cities, Path):
            cities_list = [cities]
        else:
            cities_list = cities

    # get census_boundaries for the cities
    census_boundaries = census_boundaries[census_boundaries[city_name_field].isin(cities_list)].reset_index(
        drop=True
    )

    # get the HUC level layer name
    layer_name = f"WBDHU{huc_level}"

    # get the wbd data
    if isinstance(wbd, str) or isinstance(wbd, Path):
        wbd = gpd.read_file(wbd, layer=layer_name)
    elif isinstance(wbd, gpd.GeoDataFrame):
        pass
    else:
        raise TypeError("wbd argument must be a GeoDataFrame, str, or Path.")

    # get the HUC field name
    huc_field_name = f"HUC{huc_level}"

    # use spatial join to create huc list
    huc_list = (
        wbd.sjoin(census_boundaries, how="inner", predicate=predicate)
        .loc[:, huc_field_name]
        .drop_duplicates()
        .reset_index(drop=True)
        .to_list()
    )

    # append the additional HUCs to wbd_subset
    if additional_hucs:
        try:
            # reads the file successfully if additional_hucs is a str or Path
            additional_hucs = pd.read_fwf(
                additional_hucs, header=None, lineterminator='\n', index_col=False, comment='#', dtype=str
            )[0].to_list()
        except FileNotFoundError:
            if isinstance(additional_hucs, str) | isinstance(additional_hucs, Path):
                additional_hucs = [additional_hucs]
            else:
                additional_hucs = additional_hucs

        # append the additional HUCs to wbd_subset
        huc_list += additional_hucs
        huc_list = list(set(huc_list))

    # remove the drop HUCs from wbd_subset
    if drop_hucs:
        try:
            # reads the file successfully if drop_hucs is a str or Path
            drop_hucs = pd.read_fwf(
                drop_hucs, header=None, lineterminator='\n', index_col=False, comment='#', dtype=str
            )[0].to_list()
        except FileNotFoundError:
            if isinstance(drop_hucs, str) | isinstance(drop_hucs, Path):
                drop_hucs = [drop_hucs]
            else:
                drop_hucs = drop_hucs

        # remove the drop HUCs from wbd_subset
        huc_list = list(set(huc_list) - set(drop_hucs))

    # remove duplicated HUCs
    wbd_subset = wbd.loc[wbd[huc_field_name].isin(huc_list)].reset_index(drop=True)

    # write wbd_subset to file
    if wbd_write_path:
        if wbd_write_kwargs is None:
            wbd_write_kwargs = {}
        wbd_subset.to_file(wbd_write_path, **wbd_write_kwargs)

    # write huc_list to file
    if huc_list_write_path:
        pd.Series(huc_list).to_csv(huc_list_write_path, sep='\n', header=False, index=False)

    return wbd_subset, huc_list


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
        "--cities",
        "-c",
        type=str,
        required=True,
        help="The cities to convert to HUCs. Can be an iterable of city names, a path to a line-deliminted text file with city names, or a single city name.",
    )

    parser.add_argument(
        "--wbd",
        "-b ",
        type=str,
        required=False,
        default=WBD_FILE_PATH,
        help="The path to the WBD vector file.",
    )

    parser.add_argument(
        "--huc_level",
        "-l",
        type=int,
        required=False,
        default=8,
        help="The HUC level to use. Options are 8.",
        choices=[8],
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

    parser.add_argument(
        "--city_name_field",
        "-f",
        type=str,
        required=False,
        default="NAME",
        help="The name of the field in GeoDataFrame that contains the city names.",
    )

    parser.add_argument(
        "--predicate",
        "-p",
        type=str,
        required=False,
        default="intersects",
        help="The spatial predicate to use to get the geometry of the city. Options are in geopandas.sindex.valid_query_parameters and listed here as 'overlaps', 'covers', 'covered_by', 'contains_properly', 'touches', 'intersects', 'contains', 'crosses', None, and 'within'",
        choices=[
            'overlaps',
            'covers',
            'covered_by',
            'contains_properly',
            'touches',
            'intersects',
            'contains',
            'crosses',
            None,
            'within',
        ],
    )

    parser.add_argument(
        "--additional_hucs",
        "-a",
        type=str,
        required=False,
        default=None,
        help="The additional HUCs to add to the HUC list. Can be a path to a line-deliminted text file with HUC names or a single HUC name.",
    )

    parser.add_argument(
        "--drop_hucs",
        "-d",
        type=str,
        required=False,
        default=None,
        help="The HUCs to drop from the HUC list. Can be a path to a line-deliminted text file with HUC names or a single HUC name.",
    )

    parser.add_argument(
        "--wbd_write_path",
        "-b",
        type=str,
        required=False,
        default=None,
        help="The path to write the WBD data to.",
    )

    parser.add_argument(
        "--wbd_write_kwargs",
        "-wk",
        type=json.loads,
        required=False,
        default=None,
        help="Keyword arguments to pass to GeoDataFrame.to_file(). Only used if write_path is not None.",
    )

    parser.add_argument(
        "--huc_list_write_path", "-hw", type=str, required=False, help="The path to write the HUC8 list to."
    )

    kwargs = vars(parser.parse_args())

    wbd, huc_list = Acquire_city_boundaries_and_convert_city_names_to_HUCs(**kwargs)
