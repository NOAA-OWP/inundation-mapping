#!/usr/bin/env python3

import os
from os.path import splitext
import fiona
import rasterio
import numpy as np
from rasterio.warp import calculate_default_transform, reproject, Resampling
from pyproj.crs import CRS

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


def get_fossid_from_huc8(huc8_id,foss_id_attribute='fossid',
                         hucs=os.path.join(os.environ['inputDataDir'],'wbd','WBD_National.gpkg'),
                         hucs_layerName=None):

    hucs = fiona.open(hucs,'r',layer=hucs_layerName)

    for huc in hucs:
        if huc['properties']['HUC8'] == huc8_id:
            return(huc['properties'][foss_id_attribute])


def update_raster_profile(args):

    elev_cm_filename   = args[0]
    elev_m_filename    = args[1]
    projection         = args[2]
    nodata_val         = args[3]
    blocksize          = args[4]
    keep_intermediate  = args[5]
    overwrite  = args[6]

    if os.path.exists(elev_m_filename) & overwrite:
        os.remove(elev_m_filename)
    elif not os.path.exists(elev_m_filename):
        pass
    else:
        print(f"Skipping {elev_m_filename}. Use overwrite option.")
        return

    if isinstance(blocksize, int):
        pass
    elif isinstance(blocksize,str):
        blocksize = int(blocksize)
    elif isinstance(blocksize,float):
        blocksize = int(blocksize)
    else:
        raise TypeError("Pass integer for blocksize")

    assert elev_cm_filename.endswith('.tif'), "input raster needs to be a tif"

    # Update nodata value and convert from cm to meters
    dem_cm = rasterio.open(elev_cm_filename)
    
    no_data = dem_cm.nodata
    
    dem_m_profile = dem_cm.profile.copy()
    dem_m_profile.update(driver='GTiff',tiled=True,nodata=nodata_val,
                         blockxsize=blocksize, blockysize=blocksize,
                         dtype='float32',crs=projection,compress='lzw',interleave='band')

    dest = rasterio.open(elev_m_filename, "w", **dem_m_profile, BIGTIFF='YES')

    for idx,window in dem_cm.block_windows(1):
        data = dem_cm.read(1,window=window)

        # wrote out output of this line as the same variable.
        data = np.where(data == int(no_data), nodata_val, (data/100).astype(rasterio.float32))

    # removed this line to avoid having two array copies of data. Kills memory usage
    #del data

        dest.write(data, indexes = 1, window=window)


    # not necessary
    #del dem_m

    dem_cm.close()
    dest.close()
    
    if keep_intermediate == False:
        os.remove(elev_cm_filename)
    
    return(elev_m_filename)

'''
This function isn't currently used but is the preferred method for
reprojecting elevation grids.

Several USGS elev_cm.tifs have the crs value in their profile stored as the string "CRS.from_epsg(26904)"
instead of the actual output of that command.

Rasterio fails to properly read the crs but using gdal retrieves the correct projection.
Until this issue is resolved use the reproject_dem function in reproject_dem.py instead.
reproject_dem is not stored in the shared_functions.py because rasterio and
gdal bindings are not entirely compatible: https://rasterio.readthedocs.io/en/latest/topics/switch.html

'''

def reproject_raster(input_raster_name,reprojection,blocksize=None,reprojected_raster_name=None):

    if blocksize is not None:
        if isinstance(blocksize, int):
            pass
        elif isinstance(blocksize,str):
            blocksize = int(blocksize)
        elif isinstance(blocksize,float):
            blocksize = int(blocksize)
        else:
            raise TypeError("Pass integer for blocksize")
    else:
        blocksize = 256

    assert input_raster_name.endswith('.tif'), "input raster needs to be a tif"

    reprojection = rasterio.crs.CRS.from_string(reprojection)

    with rasterio.open(input_raster_name) as src:

        # Check projection
        if src.crs.to_string() != reprojection:
            if src.crs.to_string().startswith('EPSG'):
                epsg = src.crs.to_epsg()
                proj_crs = CRS.from_epsg(epsg)
                rio_crs = rasterio.crs.CRS.from_user_input(proj_crs).to_string()
            else:
                rio_crs = src.crs.to_string()

            print(f"{input_raster_name} not projected")
            print(f"Reprojecting from {rio_crs} to {reprojection}")

            transform, width, height = calculate_default_transform(
                src.crs, reprojection, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': reprojection,
                'transform': transform,
                'width': width,
                'height': height,
                'compress': 'lzw'
            })

            if reprojected_raster_name is None:
                reprojected_raster_name = input_raster_name

            assert reprojected_raster_name.endswith('.tif'), "output raster needs to be a tif"

            with rasterio.open(reprojected_raster_name, 'w', **kwargs, tiled=True, blockxsize=blocksize, blockysize=blocksize, BIGTIFF='YES') as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=rio_crs,
                    dst_transform=transform,
                    dst_crs=reprojection.to_string(),
                    resampling=Resampling.nearest)
                del dst
        del src

def mem_profile(func):
    def wrapper(*args, **kwargs):
        if (os.environ.get('mem') == "1"):
            profile(func)(*args, **kwargs)
        else:
            func(*args, **kwargs)
    return wrapper

def append_id_to_file_name(file_name, identifier):
    '''
    Processing:
        Takes an incoming file name and inserts an identifier into the name
        just ahead of the extension, with an underscore added.
        ie) filename = "/output/myfolder/a_raster.tif"
            indentifer = "13090001"
            Becomes: "/output/myfolder/a_raster_13090001.tif"
        Note: 
            - Can handle a single identifier or a list of identifier
              ie) identifier = ["13090001", "123000001"]
              Becomes: "/output/myfolder/a_raster_13090001_123000001.tif"
            - This allows for file name to not be submitted and will return None
    Inputs:
        file_name: a single file name
        identifier: a value or list of values to be inserted with an underscore 
            added ahead of the extention
    Output:
        out_file_name: A single name with each identifer added at the end before
            the extension, each with an underscore in front of the identifier.
    '''

    if file_name is not None:

        root,extension = os.path.splitext(file_name)

        if isinstance(identifier, list):

            out_file_name = root
            for i in identifier:
                out_file_name += "_{}".format(i)
            out_file_name += extension
        else:
            out_file_name = root + "_{}".format(identifier) + extension
    else:
        out_file_name = None

    return(out_file_name)
