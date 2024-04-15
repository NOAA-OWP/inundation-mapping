import osmnx as ox
import geopandas as gpd
from rasterstats import zonal_stats
import rasterio
import os
from rasterio import transform, features
from shapely.geometry import shape
import numpy as np
from pathlib import Path
import pandas as pd
import argparse

def process_bridges_in_huc(
        huc_geom,
        resolution,
        buffer_width,
        hand_grid_file,
        osm_file,
        bridge_lines_raster_filename,
        updated_hand_filename
):
    if not os.path.exists(hand_grid_file):
        print(f"-- no hand grid, {hand_grid_file}")
        return
    hand_grid = rasterio.open(hand_grid_file)

    if os.path.exists(osm_file):
        osm_gdf = gpd.read_file(osm_file)
    else:
        tags = {"bridge": True}
        osm_gdf = ox.features_from_polygon(shape(huc_geom), tags) 
        # may need to check for and fix point geometry and convert 'nodes' lists to strings, or do it before saving to shp
        # ... or just skip

    osm_gdf = osm_gdf.to_crs(hand_grid.crs)
    osm_gdf.geometry = osm_gdf.buffer(buffer_width)
    #######################################################################


    ############# get max hand values for each bridge #########
    # find max hand value from raster each linestring intersects
    osm_gdf['max_hand'] = zonal_stats(osm_gdf['geometry'],
                    hand_grid.read(1),
                    affine=hand_grid.transform,
                    stats="max")
    osm_gdf['max_hand'] = [x.get('max') for x in osm_gdf.max_hand] # pull the values out of the geopandas columns so we can use them as floats
    osm_gdf = osm_gdf.sort_values(by="max_hand", ascending=False) # sort in case of overlaps; display max hand value at any given location
    #######################################################

    ########### burn in bridge max hand values ##################
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
        "compress": 'lzw'
    }

    ################# rasterize new hand values ####################
    with rasterio.open(bridge_lines_raster_filename, 'w+', **out_meta) as out:
        out_arr = out.read(1)
        # this is where we create a generator of geom, value pairs to use in rasterizing
        shapes = ((geom,value) for geom, value in zip(osm_gdf.geometry, osm_gdf.max_hand))
        # burn in values and add a specific nodata fill value and any pixel that's touched by polygon
        burned = features.rasterize(shapes=shapes, 
                                    fill=-999999, 
                                    out=out_arr, 
                                    transform=out.transform, 
                                    all_touched=True)
        out.write_band(1, burned)
    #################################################################

    #################### update hand grid ##########################
    with rasterio.open(bridge_lines_raster_filename) as in_data:
        new_hand_values = in_data.read(1)

    hand_grid_vals = hand_grid.read(1)
    combined_hand_values = np.where(new_hand_values==-999999,hand_grid_vals,new_hand_values)

    with rasterio.open(updated_hand_filename, 'w+', **out_meta) as out:
        out.write(combined_hand_values, 1)
    ###################################################################

    return


def burn_bridges(
        huc_shapefile,
        hand_grid_folder,
        osm_folder,
        bridge_lines_folder,
        updated_hand_folder,
        resolution, 
        buffer_width, 
        hucs_of_interest
):
    ###############################################################

    print("Opening CONUS HUC8 shapefile")

    gdf = gpd.read_file(huc_shapefile)
    for index,row in gdf.iterrows():
    ####################### open up hand grid, huc outline, and get osm bridges #####
        huc = row['HUC8']
        if hucs_of_interest and huc not in hucs_of_interest:
            continue
        print(f"** Processing {huc}")
        hand_grid_file = hand_grid_folder + f"{huc}_branches_0/rem_zeroed_masked_0.tif"
        osm_file = osm_folder + f"huc{huc}_osm_bridges.shp"
        bridge_lines_raster_filename = bridge_lines_folder + f"{huc}_new_bridge_values.tif"
        updated_hand_filename = updated_hand_folder + f"{huc}_final_hand_values.tif"

        process_bridges_in_huc(
            row['geometry'],
            resolution,
            buffer_width,
            hand_grid_file,
            osm_file,
            bridge_lines_raster_filename,
            updated_hand_filename)

    print("... done processing all HUC8s")

    return


if __name__ == "__main__":

    '''
    Sample usage (min params):
        python3 /data/bridges/heal_bridges_osm.py
            -g /data/inputs/hand_grids_here #xxx make this more accurate
            -h /data/inputs/tx_hucs.shp
            -o /data/inputs/osm/
            -b /data/inputs/temp/
            -u /data/inputs/final_osm_hand/

    Notes:
        - This tool will run best if the pull_osm_bridges.py script is run first.
    '''

    parser = argparse.ArgumentParser(description='Rasterizes max HAND values under OSM lines and heals HAND')

    parser.add_argument(
        '-s',
        '--huc_shapefile',
        help='REQUIRED: full path of the shapefile, gpkg or gdb files that will'
        ' contain in one layer all the HUC8s to process.',
        required=True,
    )
    parser.add_argument(
        '-g',
        '--hand_grid_folder',
        help='REQUIRED: folder location of the HAND grid rasters.'
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
        ' Files will be saved to here.',
        required=True,
    )
    parser.add_argument(
        '-u',
        '--updated_hand_folder',
        help='REQUIRED: folder location for final updated HAND grids, saved by HUC8.'
        ' Files will be saved to here.',
        required=True,
    )
    parser.add_argument(
        '-r',
        '--resolution',
        help='OPTIONAL: Resolution of HAND grid. Default value is 10m',
        required=False,
        default=10,
    )
    parser.add_argument(
        '-w',
        '--buffer_width',
        help='OPTIONAL: buffer width for OSM bridge lines. Default value is 10m (on each side)',
        required=False,
        default=10,
    )
    parser.add_argument(
        '-i',
        '--hucs_of_interest',
        help='OPTIONAL: subset of HUC8s of interest as a list. Default is empty list,'
        ' and all HUC8s in the provided HUC8 shapefile will be processed.',
        required=False,
        default=[],
    )
    
    args = vars(parser.parse_args())

    burn_bridges(**args)