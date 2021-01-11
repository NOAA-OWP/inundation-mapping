#!/usr/bin/env python3

import os
from os.path import splitext

def getDriver(fileName):

    driverDictionary = {'.gpkg' : 'GPKG','.geojson' : 'GeoJSON','.shp' : 'ESRI Shapefile'}
    driver = driverDictionary[splitext(fileName)[1]]

    return(driver)

def pull_file(url, full_pulled_filepath):
    """
    This helper function pulls a file and saves it to a specified path.

    Args:
        url (str): The full URL to the file to download.
        full_pulled_filepath (str): The full system path where the downloaded file will be saved.
    """
    import urllib.request

    print("Pulling " + url)
    urllib.request.urlretrieve(url, full_pulled_filepath)


def delete_file(file_path):
    """
    This helper function deletes a file.

    Args:
        file_path (str): System path to a file to be deleted.
    """

    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass


def run_system_command(args):
    """
    This helper function takes a system command and runs it. This function is designed for use
    in multiprocessing.

    Args:
        args (list): A single-item list, the first and only item being a system command string.
    """

    # Parse system command.
    command = args[0]

    # Run system command.
    os.system(command)


def subset_wbd_gpkg(wbd_gpkg, multilayer_wbd_geopackage):

    import geopandas as gp
    from utils.shared_variables import CONUS_STATE_LIST, PREP_PROJECTION

    print("Subsetting " + wbd_gpkg + "...")
    # Read geopackage into dataframe.
    wbd = gp.read_file(wbd_gpkg)
    gdf = gp.GeoDataFrame(wbd)

    for index, row in gdf.iterrows():
        state = row["STATES"]
        if state != None:  # Some polygons are empty in the STATES field.
            keep_flag = False  # Default to Fault, i.e. to delete the polygon.
            if state in CONUS_STATE_LIST:
                keep_flag = True
            # Only split if multiple states present. More efficient this way.
            elif len(state) > 2:
                for wbd_state in state.split(","):  # Some polygons have multiple states, separated by a comma.
                    if wbd_state in CONUS_STATE_LIST:  # Check each polygon to make sure it's state abbrev name is allowed.
                        keep_flag = True
                        break
            if not keep_flag:
                gdf.drop(index, inplace=True)  # Delete from dataframe.

    # Overwrite geopackage.
    layer_name = os.path.split(wbd_gpkg)[1].strip('.gpkg')
    gdf.crs = PREP_PROJECTION
    gdf.to_file(multilayer_wbd_geopackage, layer=layer_name,driver='GPKG',index=False)
