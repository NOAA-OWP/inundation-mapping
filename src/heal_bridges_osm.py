import argparse
import os

import geopandas as gpd
import numpy as np
import rasterio
from rasterio import features, transform
from rasterstats import zonal_stats


def process_bridges_in_huc(
    source_hand_raster, bridge_file, output_bridge_lines_raster_file, output_final_burned_file, resolution
):
    """
    Process:
        - Use the osm_file, likely already pre-clipped to a huc, and create a raster from it.
        - using the incomeing source_raster, likely a rem at this point, find the max point value for intersecting from the
          line string. Fundemtally create a z value for the line string based on the touch point of the linestring on both
          banks. TODO: how does it handle higher elevation on one bank? Might already be in this logic.. not sure.
        - That value will be assigned to each cell that covers the bridge in the output raster.
        - We do create a temp raster which we can removed in the "deny" list system. We can use it to show the point values
          assigned to each cell of the bridge.
    """

    if not os.path.exists(source_hand_raster):
        print(f"-- no hand grid, {source_hand_raster}")
        return
    hand_grid = rasterio.open(source_hand_raster)

    if os.path.exists(bridge_file):
        osm_gdf = gpd.read_file(bridge_file)
    else:
        # skip this huc because it didn't pull in the initial OSM script
        # and could have errors in the data or geometry
        print(f"-- no OSM file, {bridge_file}")
        return

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
    with rasterio.open(output_bridge_lines_raster_file, 'w+', **out_meta) as out:
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
    with rasterio.open(output_bridge_lines_raster_file) as in_data:
        new_hand_values = in_data.read(1)

    hand_grid_vals = hand_grid.read(1)
    # replace values at all locations where there are healed values available
    combined_hand_values = np.where(new_hand_values == -999999, hand_grid_vals, new_hand_values)

    with rasterio.open(output_final_burned_file, 'w+', **out_meta) as out:
        out.write(combined_hand_values, 1)

    return


if __name__ == "__main__":
    '''
    Sample usage (min params):
        python3 src/heal_bridges_osm.py
            -u 12090301
            -g /outputs/fim_4_4_15_0/1209301/branches/3763000013/rem_zeroed_masked_3763000013.tif
            -s /outputs/fim_4_4_15_0/1209301/osm_bridges_subset.gpkg
            -t /outputs/fim_4_4_15_0/1209301/branches/3763000013/rem_zeroed_masked_bridge_3763000013.tif
            -o /outputs/fim_4_4_15_0/1209301/branches/3763000013/rem_zeroed_masked_3763000013.tif
            -r 10

    '''

    parser = argparse.ArgumentParser(description='Rasterizes max HAND values under OSM lines and heals HAND')

    parser.add_argument(
        '-g',
        '--source_hand_raster',
        help='REQUIRED: Path and file name of source output raster that we can burn bridges into',
        required=True,
    )

    parser.add_argument(
        '-s', '--bridge_file', help='REQUIRED: A gpkg that contains the bridges vectors', required=True
    )

    parser.add_argument(
        '-t',
        '--output_bridge_lines_raster_file',
        help='REQUIRED: Path and file name of the new raster with just the bridge cell values in it, not yet burned in',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output_bridge_lines_raster_file',
        help='REQUIRED: Path and file name of the new raster with the bridges burned into it',
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

    args = vars(parser.parse_args())

    process_bridges_in_huc(**args)
