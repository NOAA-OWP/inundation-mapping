import argparse
import os
import re
import glob

import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio
from rasterio import features
from rasterstats import zonal_stats
from rasterio.warp import reproject, Resampling
import xarray as xr


threatened_percent = 0.75

def process_non_lidar_osm(osm_gdf,source_hand_raster):
    non_lidar_osm_gdf=osm_gdf[osm_gdf['has_lidar_tif']=='N']

    # apply non-lidar osm bridges into HAND grid
    with rasterio.open(source_hand_raster, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

        # Get max hand values for each bridge
        stats = zonal_stats(
            non_lidar_osm_gdf['geometry'], hand_grid_array, affine=hand_grid.transform, stats="max", nodata=-999
        )
        # pull the values out of the geopandas columns so we can use them as floats
        non_lidar_osm_gdf['threshold_hand'] = [x.get('max') for x in stats]
        # sort in case of overlaps; display max hand value at any given location
        non_lidar_osm_gdf = non_lidar_osm_gdf.sort_values(by="threshold_hand", ascending=False)

        # Burn the bridges into the HAND grid
        shapes = ((geom, value) for geom, value in zip(non_lidar_osm_gdf.geometry, non_lidar_osm_gdf.threshold_hand))
        features.rasterize( shapes=shapes, out=hand_grid_array, transform=hand_grid.transform, all_touched=False )


    return non_lidar_osm_gdf,hand_grid_array,hand_grid_profile


def process_lidar_osm(osm_gdf,hand_grid_array,hand_grid_profile,bridge_elev_diff_raster):
    with rasterio.open(bridge_elev_diff_raster) as diff_grid:
        diff_grid_array = diff_grid.read(1)  # Read the first band
        diff_grid_transform = diff_grid.transform
        diff_grid_crs = diff_grid.crs

        # Ensure CRS and transform match
        if hand_grid_profile['crs'] != diff_grid_crs or hand_grid_profile['transform'] != diff_grid_transform:
            # Reproject the second raster to match the first
            reprojected_diff_grid_array = np.empty_like(hand_grid_array, dtype=diff_grid_array.dtype)
            reproject(
                source=diff_grid_array,
                destination=reprojected_diff_grid_array,
                src_transform=diff_grid_transform,
                src_crs=diff_grid_crs,
                dst_transform=hand_grid_profile['transform'],
                dst_crs=hand_grid_profile['crs'],
                dst_shape=hand_grid_array.shape,
                resampling=Resampling.nearest
            )
        else:
            reprojected_diff_grid_array = diff_grid_array

    # Add the diff and HAND grid rasters 
    nodata_value = hand_grid_profile.get("nodata", None)
    updated_hand_grid_array = np.where(
        (hand_grid_array == nodata_value) | (reprojected_diff_grid_array == nodata_value),  nodata_value,
        hand_grid_array + reprojected_diff_grid_array )


    # Get median hand values for each lidar-informed bridge
    lidar_osm_gdf=osm_gdf[osm_gdf['has_lidar_tif']=='Y']
    stats = zonal_stats(
        lidar_osm_gdf['geometry'], updated_hand_grid_array, affine=hand_grid_profile['transform'], stats="median", nodata=-999
    )
    lidar_osm_gdf['threshold_hand'] = [x.get('median') for x in stats]

    return lidar_osm_gdf,updated_hand_grid_array



def process_bridges_in_huc(
    source_hand_raster,bridge_elev_diff_raster, bridge_vector_file,buffer_width, catchments, bridge_centroids ):
    

    if not os.path.exists(source_hand_raster):
        print(f"-- no hand grid, {source_hand_raster}")
        return

    if os.path.exists(bridge_vector_file):
        # Read the bridge lines file and buffer it by half of the input width
        osm_gdf = gpd.read_file(bridge_vector_file)
        osm_gdf['centroid_geometry'] = osm_gdf.centroid
        osm_gdf['geometry'] = osm_gdf.geometry.buffer(buffer_width, resolution=buffer_width)
    else:
        # skip this huc because it didn't pull in the initial OSM script
        # and could have errors in the data or geometry
        print(f"-- no OSM file, {bridge_vector_file}")
        return

    
    #first process the osm bridges without reliable lidar data using previous method
    non_lidar_osm_gdf,hand_grid_array,hand_grid_profile= process_non_lidar_osm(osm_gdf,source_hand_raster)
    lidar_osm_gdf,updated_hand_grid_array=process_lidar_osm(osm_gdf,hand_grid_array,hand_grid_profile,bridge_elev_diff_raster)

    osm_gdf= pd.concat([non_lidar_osm_gdf, lidar_osm_gdf], ignore_index=True)
    
    # # Write the new HAND grid
    # source_hand_raster="/outputs/Ali_bridges_lidar/outputs/updated_HAND_grid.tif"
    with rasterio.open(source_hand_raster, 'w', **hand_grid_profile) as new_hand_grid:
         new_hand_grid.write(updated_hand_grid_array, 1)


    # Switch the geometry over to the centroid points
    osm_gdf['geometry'] = osm_gdf['centroid_geometry']
    osm_gdf = osm_gdf.drop(columns='centroid_geometry')

    # Join the bridge points to the HAND catchments to get the HydroID and feature_id
    osm_gdf = osm_gdf.loc[osm_gdf.threshold_hand >= 0]
    catchments_df = gpd.read_file(catchments)

    osm_gdf = gpd.sjoin(osm_gdf, catchments_df[['HydroID', 'feature_id', 'order_', 'geometry']], how='inner')
    osm_gdf = osm_gdf.drop(columns='index_right')
    # Calculate threatened stage
    osm_gdf['threshold_hand_75'] = osm_gdf.threshold_hand * threatened_percent
    # Add the branch id to the catchments
    branch_dir = re.search(r'branches/(\d{10}|0)/', catchments).group()
    branch_id = re.search(r'(\d{10}|0)', branch_dir).group()
    osm_gdf['branch'] = branch_id
    osm_gdf['mainstem'] = 0 if branch_id == '0' else 1

    # Check if the GeoDataFrame is empty
    if not osm_gdf.empty:
        # Write the bridge points to a geopackage
        osm_gdf.to_file(bridge_centroids, index=False, engine='fiona')
    else:
        print('The geoDataFrame is empty. File not saved.')
    return


def flow_lookup(stages, hydroid, hydroTable):
    single_hydroTable = hydroTable.loc[hydroTable.HydroID == hydroid]
    return_flows = np.interp(
        stages, single_hydroTable.loc[:, 'stage'], single_hydroTable.loc[:, 'discharge_cms']
    )
    return return_flows


def flows_from_hydrotable(bridge_pnts, hydroTable):
    bridge_pnts[['max_discharge', 'max_discharge75']] = bridge_pnts.apply(
        lambda row: flow_lookup((row.threshold_hand, row.threshold_hand_75), row.HydroID, hydroTable),
        axis=1,
        result_type='expand',
    )
    # Convert stages and dischrages to ft and cfs respectively
    bridge_pnts['threshold_hand_ft'] = bridge_pnts['threshold_hand'] * 3.28084
    bridge_pnts['threshold_hand_75_ft'] = bridge_pnts['threshold_hand_75'] * 3.28084
    bridge_pnts['max_discharge_cfs'] = bridge_pnts['max_discharge'] * 35.3147
    bridge_pnts['max_discharge_75_cfs'] = bridge_pnts['max_discharge75'] * 35.3147

    return bridge_pnts


if __name__ == "__main__":
    '''
    Sample usage (min params):
        python3 src/heal_bridges_osm.py
            -g /outputs/fim_4_4_15_0/1209301/branches/3763000013/rem_zeroed_masked_3763000013.tif
            -s /outputs/fim_4_4_15_0/1209301/osm_bridges_subset.gpkg
            -p /outputs/fim_4_4_15_0/1209301/branches/3763000013/gw_catchments_reaches_filtered_addedAttributes_crosswalked_3763000013.gpkg
            -c /outputs/fim_4_4_15_0/1209301/1209301/branches/3763000013/osm_bridge_centroids_3763000013.tif
            -b 10
            -r 10

    '''

    parser = argparse.ArgumentParser(description='Rasterizes max HAND values under OSM lines and heals HAND')

    parser.add_argument(
        '-g',
        '--source_hand_raster',
        help='REQUIRED: Path and file name of source output raster',
        required=True,
    )

    parser.add_argument(
        '-d', '--bridge_elev_diff_raster', help='REQUIRED: Path of bridge_elev_diff raster file', required=True
    )

    parser.add_argument(
        '-s', '--bridge_vector_file', help='REQUIRED: A gpkg that contains the bridges vectors', required=True
    )

    parser.add_argument(
        '-b',
        '--buffer_width',
        help='OPTIONAL: Buffer to apply to non-lidar OSM bridges. Default value is 10m (on each side)',
        required=False,
        default=10,
        type=float,
    )

    parser.add_argument(
        '-p', '--catchments', help='REQUIRED: Path and file name of the catchments geopackage', required=True
    )

    parser.add_argument(
        '-c',
        '--bridge_centroids',
        help='REQUIRED: Path and file name of the output bridge centroid geopackage',
        required=True,
    )


    # parser.add_argument(
    #     '-r',
    #     '--resolution',
    #     help='OPTIONAL: Resolution of HAND grid. Default value is 10m',
    #     required=False,
    #     default=10,
    #     type=int,
    # )

    args = vars(parser.parse_args())

    process_bridges_in_huc(**args)
