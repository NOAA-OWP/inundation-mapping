#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Create 3DEP tile index.

TODO:
    - implement logging

Command Line Usage:
    date_yyymmdd=<YYYYMMDD>; res=<1 or 3> /foss_fim/data/usgs/create_3dep_tile_index.py -r ${res} -t /data/inputs/3dep_dems/lidar_tile_index/usgs_rocky_3dep_${r}m_tile_index_${date_yyymmdd}.gpkg
"""

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List

import geopandas as gpd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyproj import CRS
from tqdm import tqdm


srcDir = os.getenv('srcDir')
load_dotenv(os.path.join(srcDir, 'bash_variables.env'))
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')

# base url rocky
BASE_1m_URL = 'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/1m/Projects/'
BASE_3m_URL = 'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/19/IMG/'


def retry_request(url: str, max_retries: int = 10, sleep_time: int = 3) -> requests.Response:
    """
    Retry a request to the given URL a maximum of max_retries times.
    """

    for i in range(max_retries):
        try:
            # TODO: cache with aiohttp
            # async with CachedSession(cache=SQLiteBackend('demo_cache')) as session:
            # await session.get(url)
            response = requests.get(url)
            if response.status_code != 200:
                # print(f"Failed : '{url}'")
                time.sleep(sleep_time)
                continue
            return response

        except requests.exceptions.ConnectionError:
            # print(f"Failed : '{url}'")
            time.sleep(sleep_time)
            continue

    if i == (max_retries - 1):
        raise requests.exceptions.ConnectionError(f"Failed to connect to '{url}' after {max_retries} retries")
    elif response.status_code != 200:
        raise requests.exceptions.ConnectionError(
            f"Failed to connect to '{url}' with status code {response.status_code}"
        )


def get_1m_project_urls(base_url: str) -> List[str]:
    """
    Fetch all project folder URLs from the 1m 3DEP base URL
    """
    response = retry_request(base_url)

    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a')

    project_urls = []
    for link in links:
        href = link.get('href')
        if href and '/' in href and not href.startswith('/') and ('https://www.' in href) and (href != '../'):
            project_url = base_url + href
            project_urls.append(project_url)

    return project_urls


def get_1m_tile_urls(project_urls: List[str]) -> List[str]:
    """
    Fetch all .tif file URLs from the TIFF folders in the given project URLs
    """

    tile_urls = []

    for project_url in tqdm(project_urls, desc="Fetching 3DEP 1m tile URLs by project"):
        # Fetch the content of each folder
        try:
            response = retry_request(project_url)
        except Exception as e:
            print(f"Failed to fetch {project_url}")
            print(f"*** {e}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')

        # Check if TIFF folder exists
        tiff_folder_link = next((link for link in links if 'TIFF' in link.get('href', '')), None)
        if not tiff_folder_link:
            print(f"No TIFF folder found in {project_url}")
            continue

        # Fetch the content of the TIFF folder
        tiff_dir_contents = project_url + tiff_folder_link.get('href')
        try:
            response = retry_request(tiff_dir_contents)
        except Exception as e:
            print(f"Failed to fetch {tiff_dir_contents}")
            print(f"*** {e}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')

        # List all .tif files
        for link in links:
            href = link.get('href')
            if href and href.endswith('.tif'):
                tiff_file_url = '/vsicurl/' + tiff_dir_contents + href
                tile_urls.append(tiff_file_url)

    return tile_urls


def get_3m_tile_urls(base_url: str) -> List[str]:
    """
    Gather the URLs for all the 3m 3DEP tiles.
    """

    print(f"Fetching 3DEP 3m tile URLs from {base_url}...")
    response = retry_request(base_url)

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all <a> tags, which contain the links
    links = soup.find_all('a')

    # Filter and modify the URLs
    modified_urls = []
    for link in links:
        href = link.get('href')
        if href and href.endswith('.zip'):
            # Construct the full URL
            full_url = base_url + href
            base_name = href.split('.')[0]  # Extract the base name from the URL
            modified_url = f"/vsizip//vsicurl/{full_url}/{base_name}.img"
            modified_urls.append(modified_url)

    return modified_urls


def create_3dep_tile_index(
    tile_index_fn: str | Path,
    tile_index_lyr: str | None = None,
    resolution: int = 1,
    crs: str | CRS = DEFAULT_FIM_PROJECTION_CRS,
    overwrite: bool = False,
    driver: str = 'GPKG',
) -> Path:
    """
    Create 3DEP tile index.
    """

    # handle retry and overwrite check logic
    if os.path.exists(tile_index_fn):
        if overwrite:
            os.remove(tile_index_fn)
        else:
            answer = input(
                'Are you sure you want to overwrite the existing 3DEP DEM tile index (yes/y/no/n): '
            )
            if (answer.lower() == 'y') | (answer.lower() == 'yes'):
                os.remove(tile_index_fn)
            else:
                print('Exiting ... file already exists and overwrite not confirmed.')
                exit(0)

    # make tile list
    if resolution == 1:
        project_urls = get_1m_project_urls(BASE_1m_URL)
        tile_urls_list = get_1m_tile_urls(project_urls)
    elif resolution == 3:
        tile_urls_list = get_3m_tile_urls(BASE_3m_URL)
    else:
        raise ValueError("Resolution must be 1 or 3")

    print(f"Collected {len(tile_urls_list)} tile URLs for {resolution}m tiles")

    # convert crs to string
    if isinstance(crs, CRS):
        crs = crs.to_string()

    # layer name is None
    if tile_index_lyr is None:
        tile_index_lyr = os.path.basename(tile_index_fn).split('.')[0]

    # make directory if it doesn't exist
    tile_index_fn_dir = os.path.dirname(tile_index_fn)
    os.makedirs(tile_index_fn_dir, exist_ok=True)

    # enforce string type
    tile_index_fn = str(tile_index_fn)

    # write tempfile with tile list as line-delimited list
    with tempfile.NamedTemporaryFile(mode='w') as f_tmp:

        # write tile list
        for tile_url in tile_urls_list:
            f_tmp.write(f"{tile_url}\n")

        # get file path of tempfile
        f_tmp_path = f_tmp.name

        # subprocess
        subprocess.run(
            [
                'gdaltindex',
                '-f',
                f'{driver}',
                '-write_absolute_path',
                '-t_srs',
                crs,
                '-lyr_name',
                tile_index_lyr,
                tile_index_fn,
                '--optfile',
                f_tmp_path,
            ],
            check=True,
        )

    # add dem resolution field
    tile_index = gpd.read_file(tile_index_fn, layer=tile_index_lyr)
    tile_index['dem_resolution'] = resolution
    tile_index.to_file(tile_index_fn, layer=tile_index_lyr, driver=driver, index=False)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Create 3DEP tile index.')

    parser.add_argument('-t', '--tile-index-fn', help='Tile index filename', required=True, type=str)

    parser.add_argument('-l', '--tile-index-lyr', help='Tile index layer', required=False, default=None)

    parser.add_argument(
        '-r',
        '--resolution',
        help='Resolution. Valid values are 1 or 3 which are units of meters. Default is 1.',
        required=False,
        type=int,
        default=1,
        choices=[1, 3],
    )

    parser.add_argument(
        '-c',
        '--crs',
        help='Coordinate reference system',
        required=False,
        type=str,
        default=DEFAULT_FIM_PROJECTION_CRS,
    )

    parser.add_argument(
        '-o', '--overwrite', help='Overwrite existing tile index', required=False, action='store_true'
    )

    kwargs = vars(parser.parse_args())

    # Create 3DEP tile index.
    create_3dep_tile_index(**kwargs)
