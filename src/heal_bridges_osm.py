import argparse
import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import osmnx as ox
import pandas as pd
import rasterio
from rasterio import features, transform
from rasterstats import zonal_stats
from shapely.geometry import shape


def process_bridges_in_huc(
    resolution, buffer_width, hand_grid_file, osm_file, bridge_lines_raster_filename, updated_hand_filename
):
    if not os.path.exists(hand_grid_file):
        print(f"-- no hand grid, {hand_grid_file}")
        return
    hand_grid = rasterio.open(hand_grid_file)

    if os.path.exists(osm_file):
        osm_gdf = gpd.read_file(osm_file)
    else:
        # skip this huc because it didn't pull in the initial OSM script
        # and could have errors in the data or geometry
        print(f"-- no OSM file, {osm_file}")
        return

    osm_gdf = osm_gdf.to_crs(hand_grid.crs)
    osm_gdf.geometry = osm_gdf.buffer(buffer_width)
    #######################################################################

    ############# get max hand values for each bridge #########
    # find max hand value from raster each linestring intersects
    osm_gdf['max_hand'] = zonal_stats(
        osm_gdf['geometry'], hand_grid.read(1), affine=hand_grid.transform, stats="max"
    )
    # pull the values out of the geopandas columns so we can use them as floats
    osm_gdf['max_hand'] = [x.get('max') for x in osm_gdf.max_hand]
    # sort in case of overlaps; display max hand value at any given location
    osm_gdf = osm_gdf.sort_values(by="max_hand", ascending=False)
    #######################################################

    ########### setup new raster to save bridge max hand values #############
    bbox = hand_grid.bounds
    xmin, ymin, xmax, ymax = bbox
    w = (xmax - xmin) // resolution
    h = (ymax - ymin) // resolution

    out_meta = {
        "driver": "GTiff",
        "dtype": "float32",
        "height": h,
        "width": w,
        "count": 1,
        "crs": hand_grid.crs,
        "nodata": -999999,
        "transform": transform.from_bounds(xmin, ymin, xmax, ymax, w, h),
        "compress": 'lzw',
    }

    ################# rasterize new hand values ####################
    # TODO: This little section and the next heal/update section are where we would
    #   potentially pull lidar data into HAND grid instead of rasterizing the max HAND
    #   data.
    with rasterio.open(bridge_lines_raster_filename, 'w+', **out_meta) as out:
        out_arr = out.read(1)
        # this is where we create a generator of geom, value pairs to use in rasterizing
        shapes = ((geom, value) for geom, value in zip(osm_gdf.geometry, osm_gdf.max_hand))
        # burn in values to any pixel that's touched by polygon and add nodata fill value
        burned = features.rasterize(
            shapes=shapes, fill=-999999, out=out_arr, transform=out.transform, all_touched=True
        )
        out.write_band(1, burned)
    #################################################################

    #################### heal / update hand grid ##########################
    with rasterio.open(bridge_lines_raster_filename) as in_data:
        new_hand_values = in_data.read(1)

    hand_grid_vals = hand_grid.read(1)
    # replace values at all locations where there are healed values available
    combined_hand_values = np.where(new_hand_values == -999999, hand_grid_vals, new_hand_values)

    with rasterio.open(updated_hand_filename, 'w+', **out_meta) as out:
        out.write(combined_hand_values, 1)
    ###################################################################

    return


def burn_bridges(
    huc_shapefile,
    hand_grid_path,
    osm_folder,
    bridge_lines_folder,
    updated_hand_folder,
    resolution,
    buffer_width,
    hucs_of_interest,
):
    if not os.path.exists(bridge_lines_folder):
        os.mkdir(bridge_lines_folder)

    if not os.path.exists(updated_hand_folder):
        os.mkdir(updated_hand_folder)

    print("Opening CONUS HUC8 shapefile")

    if hucs_of_interest:
        hucs_of_interest = hucs_of_interest.split(',')
    elif huc_shapefile:
        gdf = gpd.read_file(huc_shapefile)
        hucs_of_interest = gdf['HUC8']
    else:
        print("-- must specify hucs of interest or a shapefile")
        return
    
    # the following loop can be multiprocessed
    for huc in hucs_of_interest:
        ####################### open up hand grid, huc outline, and get osm bridges #####
        print(f"** Processing {huc}")
        # option to pass in HAND grid file individually for one HUC8 or to set a folder location to be able
        # to process multiple HUCs in one go (FIM pipeline uses one HUC8 at a time)
        if os.path.isdir(hand_grid_path):
            hand_grid_path = os.path.join(hand_grid_path, f"{huc}", "branches", "0", "rem_zeroed_masked_0.tif")
        osm_file = os.path.join(osm_folder, f"huc{huc}_osm_bridges.shp")
        bridge_lines_raster_filename = os.path.join(bridge_lines_folder, f"{huc}_new_bridge_values.tif")
        updated_hand_filename = os.path.join(updated_hand_folder, f"{huc}_final_osm_hand_values.tif")

        process_bridges_in_huc(
            resolution,
            buffer_width,
            hand_grid_path,
            osm_file,
            bridge_lines_raster_filename,
            updated_hand_filename,
        )

    print("... done processing all HUC8s")

    # TODO: cleanup temp folder here if desired

    return


if __name__ == "__main__":
    '''
    Sample usage (min params):
        python3 src/heal_bridges_osm.py
            -g /data/inputs/hand_grids_here or individual hand grid path for single huc
            -s /data/inputs/tx_hucs.shp
            -o /data/inputs/osm/
            -b /data/inputs/temp/
            -u /data/inputs/final_osm_hand/
            -i 12070205,12090301
            -w 10
            -r 10

    Notes:
        - This tool will run best if the pull_osm_bridges.py script is run first.
    '''

    parser = argparse.ArgumentParser(description='Rasterizes max HAND values under OSM lines and heals HAND')

    parser.add_argument(
        '-s',
        '--huc_shapefile',
        help='OPTIONAL: full path of the shapefile, gpkg or gdb files that will'
        ' contain in one layer all the HUC8s to process. Must contain field \'HUC8\'.'
        ' If this file isn\'t specified, then hucs_of_interest must be.',
        required=False,
    )
    parser.add_argument(
        '-g',
        '--hand_grid_path',
        help='REQUIRED: folder location of the HAND grid rasters OR file location of one'
        ' particular HUC and branch combo\'s HAND grid (use this option with one specified'
        ' HUC8 of interest in the hucs_of_interest argument).'
        ' Assumes same file structure as in fim dev folders (should be path'
        ' all the way up to previous_fim/<fim version> folder location).'
        ' Script will access the huc folders and their contained branch 0 folders.'
        ' Files will NOT be modified here.',
        required=True,
    )
    parser.add_argument(
        '-o',
        '--osm_folder',
        help='REQUIRED: folder location of previously-downloaded OSM bridge line'
        ' shapfiles, split by HUC8. Missing HUC8s will be downloaded on the fly.'
        ' Files will NOT be modified here.',
        required=True,
    )
    parser.add_argument(
        '-b',
        '--bridge_lines_folder',
        help='REQUIRED: folder location of bridge lines rasters (can be a temporary folder,'
        ' as these are an intermediate step before being healed into HAND grids).'
        ' Files will be saved to here, and the folder will be created if it doesn\'t exist.',
        required=True,
    )
    parser.add_argument(
        '-u',
        '--updated_hand_folder',
        help='REQUIRED: folder location for final updated HAND grids, saved by HUC8.'
        ' Files will be saved to here, and the folder will be created if it doesn\'t exist.',
        required=True,
    )
    parser.add_argument(
        '-r',
        '--resolution',
        help='OPTIONAL: Resolution of HAND grid. Default value is 10m',
        required=False,
        default=10,
        type=int,
    )
    parser.add_argument(
        '-w',
        '--buffer_width',
        help='OPTIONAL: buffer width for OSM bridge lines. Default value is 10m (on each side)',
        required=False,
        default=10,
        type=int,
    )
    parser.add_argument(
        '-i',
        '--hucs_of_interest',
        help='OPTIONAL: subset of HUC8s of interest as a list. Default is None,'
        ' and all HUC8s in the provided HUC8 shapefile will be processed.'
        ' Pass list of HUCs without brackets and separated by commas.'
        ' Do not pass in an empty list as an argument.',
        required=False,
        default=None,
        type=str,
    )

    args = vars(parser.parse_args())

    burn_bridges(**args)
