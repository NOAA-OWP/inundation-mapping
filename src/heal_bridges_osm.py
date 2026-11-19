import argparse
# import glob
import os
import re
import shutil
import sys

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
# import xarray as xr
from rasterio import features
from rasterio.warp import Resampling, reproject
from rasterstats import zonal_stats

from utils.io import write_geodataframe

# Debugging
pd.set_option("display.max_rows", None) 
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

threatened_percent = 0.75


def process_non_lidar_osm(osm_gdf, hand_grid_array, hand_grid_profile, non_lidar_buffer):
    non_lidar_osm_gdf = osm_gdf[osm_gdf['has_lidar_tif'] == 'N'].copy()

    if non_lidar_osm_gdf.empty:
        return None, hand_grid_array

    non_lidar_osm_gdf['geometry'] = non_lidar_osm_gdf.geometry.buffer(
        non_lidar_buffer, resolution=non_lidar_buffer
    )
    # Get max hand values for each bridge
    stats = zonal_stats(
        non_lidar_osm_gdf['geometry'],
        hand_grid_array,
        affine=hand_grid_profile['transform'],
        stats="max",
        nodata=hand_grid_profile["nodata"],
        all_touched=True,
        engine="fiona"
    )

    # print("--------------")
    # print(f"type of stats is {type(stats)}")
    # print(f"first 5 recs of stats is {stats[:5]}")
    # print("All done")
    # sys.exit(1)

    # pull the values out of the geopandas columns so we can use them as floats
    non_lidar_osm_gdf.loc[:, 'threshold_hand'] = pd.to_numeric([x.get('max') for x in stats], errors='coerce')
    # sort in case of overlaps; display theshold hand value at any given location
    non_lidar_osm_gdf = non_lidar_osm_gdf.sort_values(by="threshold_hand", ascending=False)

    # Burn the bridges into the HAND grid
    shapes = (
        (geom, value) for geom, value in zip(non_lidar_osm_gdf.geometry, non_lidar_osm_gdf.threshold_hand)
    )
    features.rasterize(
        shapes=shapes, out=hand_grid_array, transform=hand_grid_profile['transform'], all_touched=False
    )

    return non_lidar_osm_gdf, hand_grid_array


def process_lidar_osm(osm_gdf, hand_grid_array, hand_grid_profile, lidar_buffer, bridge_elev_diff_raster):
    # lidar_osm_gdf = osm_gdf[osm_gdf['has_lidar_tif'] == 'Y'].copy()

    # if there are some lidar tif files, then we need to read diff file and apply healing
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
                resampling=Resampling.nearest,
            )
        else:
            reprojected_diff_grid_array = diff_grid_array

    # Add the diff and HAND grid rasters
    nodata_value = hand_grid_profile.get("nodata", None)
    updated_hand_grid_array = np.where(
        (hand_grid_array == nodata_value) | (reprojected_diff_grid_array == nodata_value),
        nodata_value,
        hand_grid_array + reprojected_diff_grid_array,
    )

    # Get median hand values for each lidar-informed bridge
    lidar_osm_gdf = osm_gdf[osm_gdf['has_lidar_tif'] == 'Y'].copy()
    lidar_osm_gdf['geometry'] = lidar_osm_gdf.geometry.buffer(lidar_buffer, resolution=lidar_buffer)
    stats = zonal_stats(
        lidar_osm_gdf['geometry'],
        updated_hand_grid_array,
        affine=hand_grid_profile['transform'],
        stats="median",
        nodata=hand_grid_profile["nodata"],
        all_touched=True,
    )
    lidar_osm_gdf.loc[:, 'threshold_hand'] = pd.to_numeric([x.get('median') for x in stats], errors='coerce')

    return lidar_osm_gdf, updated_hand_grid_array


def process_bridges_in_huc(
    source_hand_raster,
    bridge_elev_diff_raster,
    bridge_vector_file,
    non_lidar_buffer,
    lidar_buffer,
    catchments,
    bridge_centroids,
):

    # TODO make sure to also record the elevation of center of bridge which involves sampling of the DEM and DEM_DIFF.

    if not os.path.exists(source_hand_raster):
        print(f"-- no hand grid, {source_hand_raster}")
        return

    # Temp debug
    # Save off copy of the rem_masked before updating it.
    # Copying a file to a new name/path
    # Save off copy of the rem_masked before updating it.
    # Copying a file to a new name/path
    shutil.copy2(source_hand_raster, source_hand_raster.replace(".tif", "_pre_heal.tif"))

    if os.path.exists(bridge_vector_file):
        # Read the bridge lines file and buffer it by half of the input width
        osm_gdf = gpd.read_file(bridge_vector_file)
        osm_gdf['centroid_geometry'] = osm_gdf.centroid
    else:
        print(f"-- no OSM file, {bridge_vector_file}")
        return

    # read hand grid
    with rasterio.open(source_hand_raster, 'r') as hand_grid:
        hand_grid_profile = hand_grid.profile
        hand_grid_array = hand_grid.read(1)

    # print(hand_grid_profile)

    # print("All done")
    # sys.exit(1)
    


    # first process the osm bridges without reliable lidar data using previous method
    non_lidar_osm_gdf, hand_grid_array = process_non_lidar_osm(
        osm_gdf, hand_grid_array, hand_grid_profile, non_lidar_buffer
    )
    lidar_osm_gdf, updated_hand_grid_array = process_lidar_osm(
        osm_gdf, hand_grid_array, hand_grid_profile, lidar_buffer, bridge_elev_diff_raster
    )

    # print("+++++++++++++++++")
    # print(f"len of non_lidar_osm_gdf is {len(non_lidar_osm_gdf)}")
    # print(non_lidar_osm_gdf.head())

    # print("-----------------")
    # print(f"len of lidar_osm_gdf is {len(lidar_osm_gdf)}")
    # print(lidar_osm_gdf.head())

    # print("All done")
    # sys.exit(1)

    # either of non_lidar_osm_gdf or lidar_osm_gdf can be None (if there are no lidar or non-lidar bridges)-- so handle it
    valid_osm_gdfs = [gdf for gdf in [non_lidar_osm_gdf, lidar_osm_gdf] if gdf is not None]

    if valid_osm_gdfs:
        osm_gdf = pd.concat(valid_osm_gdfs, ignore_index=True)
    else:
        # Return empty GeoDataFrame if no valid bridges found
        osm_gdf = gpd.GeoDataFrame()

    # # Write the new HAND grid
    with rasterio.open(source_hand_raster, 'w', **hand_grid_profile) as new_hand_grid:
        new_hand_grid.write(updated_hand_grid_array, 1)

    del hand_grid_array

    # Switch the geometry over to the centroid points
    osm_gdf['geometry'] = osm_gdf['centroid_geometry']
    osm_gdf = osm_gdf.drop(columns='centroid_geometry')

    # Join the bridge points to the HAND catchments to get the HydroID and feature_id
    # osm_gdf = osm_gdf.loc[osm_gdf.threshold_hand > 0]
    # catchments_df = gpd.read_parquet(catchments)
    # osm_gdf = gpd.sjoin(osm_gdf, catchments_df[['HydroID', 'feature_id', 'order_', 'geometry']], how='inner')

    catchments_df = gpd.read_parquet(
        catchments,
        columns=["HydroID", "feature_id", "order_", "geometry"],
        bbox=tuple(osm_gdf.total_bounds),
    )
    osm_gdf = gpd.sjoin(osm_gdf, catchments_df, how="inner")

    del catchments_df

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
        write_geodataframe(osm_gdf, bridge_centroids, index=False)
    else:
        print('The geoDataFrame is empty. File not saved.')

    del osm_gdf

    return

def flow_lookup(stages, hydroid, hydroTable):
    single_hydroTable = hydroTable.loc[hydroTable.HydroID == hydroid].sort_values(by=["feature_id", "stage"])

    # print("--------------")
    # print(single_hydroTable.head(2)[['feature_id', 'stage', 'discharge_cms']])

    # print(f"In Flow Lookup for hydroid {hydroid} and len of single_hydroTable is {len(single_hydroTable)}")
    # print(single_hydroTable.loc[:, 'stage'], single_hydroTable.loc[:, 'discharge_cms'])

    # print("All done for now")
    # sys.exit()

    return_flows = np.interp(
        stages, single_hydroTable.loc[:, 'stage'], single_hydroTable.loc[:, 'discharge_cms']
    )
    return return_flows


def flows_from_hydrotable(bridge_pnts, hydroTable):

    # hydro_ids = hydroTable['HydroID'].unique().tolist()
    # for hydro_id in hydro_ids:
    #     single_hydroTable = hydroTable.loc[hydroTable.HydroID == hydro_id].sort_values(by="stage")

    #     print("++++++++++++++++++")
    #     print(single_hydroTable.info())
    #     print(single_hydroTable.head(2))

        # print("------- All Done")
        # sys.exit(1)

    bridge_pnts[['threshold_discharge', 'threshold_discharge75']] = bridge_pnts.apply(
        lambda row: flow_lookup((row.threshold_hand, row.threshold_hand_75), row.HydroID, hydroTable),
        axis=1,
        result_type='expand',
    )

    # Convert stages and dischrages to ft and cfs respectively
    bridge_pnts['threshold_hand_ft'] = bridge_pnts['threshold_hand'] * 3.28084
    bridge_pnts['threshold_hand_75_ft'] = bridge_pnts['threshold_hand_75'] * 3.28084
    bridge_pnts['threshold_discharge_cfs'] = bridge_pnts['threshold_discharge'] * 35.3147
    bridge_pnts['threshold_discharge_75_cfs'] = bridge_pnts['threshold_discharge75'] * 35.3147

    return bridge_pnts

# 2. Optimized lookup function using raw numpy arrays from the dictionary
# def fast_flow_lookup(stages, hydroid, lookup_dict):
#     if hydroid in lookup_dict:
#         h_stages, h_flows = lookup_dict[hydroid]
#         return np.interp(stages, h_stages, h_flows)
#     return [np.nan, np.nan]


# def flows_from_hydrotable(bridge_pnts, hydroTable):
#     bridge_pnts[['threshold_discharge', 'threshold_discharge75']] = bridge_pnts.apply(
#         lambda row: flow_lookup((row.threshold_hand, row.threshold_hand_75), row.HydroID, hydroTable),
#         axis=1,
#         result_type='expand',
#     )

    # 1. Pre-sort and pre-build a dictionary mapping HydroID -> (stages, discharges)
    # hydroTable_sorted = hydroTable.sort_values('HydroID', 'stage')
    # hydro_dict = {
    #     hydroid: (sub_df['stage'].to_numpy(), sub_df['discharge_cms'].to_numpy())
    #     for hydroid, sub_df in hydroTable_sorted.groupby('HydroID')
    # }

    # # 3. Execute using list comprehension over itertuples
    # bridge_pnts[['threshold_discharge', 'threshold_discharge75']] = [
    #     fast_flow_lookup(
    #         (row.threshold_hand, row.threshold_hand_75), row.HydroID, hydro_dict
    #     )
    #     for row in bridge_pnts.itertuples(index=False)
    # ]

    # Convert stages and dischrages to ft and cfs respectively
    # bridge_pnts['threshold_hand_ft'] = bridge_pnts['threshold_hand'] * 3.28084
    # bridge_pnts['threshold_hand_75_ft'] = bridge_pnts['threshold_hand_75'] * 3.28084
    # bridge_pnts['threshold_discharge_cfs'] = bridge_pnts['threshold_discharge'] * 35.3147
    # bridge_pnts['threshold_discharge_75_cfs'] = bridge_pnts['threshold_discharge75'] * 35.3147

    # return bridge_pnts


# def flow_lookup(stages, hydroid, hydroTable):
#     # 1. Filter and sort by stage in one step to ensure correct interpolation
#     subset = hydroTable[hydroTable.HydroID == hydroid].sort_values('stage')
    
#     # 2. Extract underlying NumPy arrays directly for a massive speed boost
#     x = subset['stage'].to_numpy()
#     y = subset['discharge_cms'].to_numpy()
    
#     # 3. Perform interpolation
#     return np.interp(stages, x, y)

# def flows_from_hydrotable(bridge_pnts, hydroTable):

#     print("Here 1 in flows_from_hydrotable")
#     if bridge_pnts.empty:
#         raise ValueError("bridge_pnts is empty")

#     # Oct 2026: Breaking this up allows for some opportunities to make it much faster.
#     # Breaking it up also helps debug if something goes wrong without compromising performance.
#     '''
#     # Chat AI's response:
#     Why this is expensive:

#     - bridge_pnts.apply(..., axis=1) calls Python for every row
#     - flow_lookup redoes hydroTable.loc[hydroTable.HydroID == hydroid] each time
#     - That creates a lot of repeated indexing and is the main scalability issue for large bridge lists

#     A better first pass is to build a lookup once, then interpolate in a loop over rows without re-filtering
#     the whole hydrotable each time.
#     '''

#     # bridge_pnts[['threshold_discharge', 'threshold_discharge75']] = bridge_pnts.apply(
#     #     lambda row: flow_lookup((row.threshold_hand, row.threshold_hand_75), row.HydroID, hydroTable),
#     #     axis=1,
#     #     result_type='expand',
#     # )

#     # Build a lookup once per HydroID; sort by stage to keep interpolation monotonic
#     hydro_table_lookup = {}
#     for hydroid, group in hydroTable.sort_values("stage").groupby("HydroID", sort=False):
#         hydro_table_lookup[hydroid] = (
#             group["stage"].to_numpy(dtype=float),
#             group["discharge_cms"].to_numpy(dtype=float),
#         )

#     results = []
#     for row in bridge_pnts.itertuples():
#         hydro_id = row.HydroID
#         if hydro_id not in hydro_table_lookup:
#             results.append((np.nan, np.nan))
#             continue

#         stages = (float(row.threshold_hand), float(row.threshold_hand_75))
#         discharge_vals = flow_lookup(stages, hydro_id, hydro_table_lookup)
#         results.append(tuple(discharge_vals))

#     bridge_pnts[["threshold_discharge", "threshold_discharge75"]] = pd.DataFrame(
#         results,
#         index=bridge_pnts.index,
#         columns=["threshold_discharge", "threshold_discharge75"],
#     )

#     # Convert stages and dischrages to ft and cfs respectively
#     print("Here 2 in flows_from_hydrotable")
    
#     bridge_pnts['threshold_hand_ft'] = bridge_pnts['threshold_hand'] * 3.28084
#     bridge_pnts['threshold_hand_75_ft'] = bridge_pnts['threshold_hand_75'] * 3.28084
#     bridge_pnts['threshold_discharge_cfs'] = bridge_pnts['threshold_discharge'] * 35.3147
#     bridge_pnts['threshold_discharge_75_cfs'] = bridge_pnts['threshold_discharge75'] * 35.3147

#     print("Here 3 in flows_from_hydrotable")    

#     return bridge_pnts


if __name__ == "__main__":
    '''
    Sample usage (min params):
        python3 src/heal_bridges_osm.py
            -g /outputs/fim_4_4_15_0/1209301/branches/3763000013/rem_zeroed_masked_3763000013.tif
            -d /outputs/fim_4_4_15_0/1209301/branches/3763000013/bridge_elev_diff_meters_3763000013.tif
            -s /outputs/fim_4_4_15_0/1209301/osm_bridges_subset.gpkg
            -b1 10
            -b2 1.5
            -p /outputs/fim_4_4_15_0/1209301/branches/3763000013/gw_catchments_reaches_filtered_addedAttributes_crosswalked_3763000013.gpkg
            -c /outputs/fim_4_4_15_0/1209301/1209301/branches/3763000013/osm_bridge_centroids_3763000013.tif

    '''

    parser = argparse.ArgumentParser(description='Heals HAND for osm bridges')

    parser.add_argument(
        '-g',
        '--source_hand_raster',
        help='REQUIRED: Path and file name of source output raster',
        required=True,
    )

    parser.add_argument(
        '-d',
        '--bridge_elev_diff_raster',
        help='REQUIRED: Path of bridge_elev_diff raster file',
        required=True,
    )

    parser.add_argument(
        '-s', '--bridge_vector_file', help='REQUIRED: A gpkg that contains the bridges vectors', required=True
    )

    parser.add_argument(
        '-b1',
        '--non_lidar_buffer',
        help='OPTIONAL: Buffer to apply to non-lidar OSM bridges. Default value is 10m (on each side)',
        required=False,
        default=10,
        type=float,
    )

    parser.add_argument(
        '-b2',
        '--lidar_buffer',
        help='OPTIONAL: Buffer to apply to lidar OSM bridges. Default value is 1.5m (on each side)',
        required=False,
        default=1.5,
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

    args = vars(parser.parse_args())

    process_bridges_in_huc(**args)
