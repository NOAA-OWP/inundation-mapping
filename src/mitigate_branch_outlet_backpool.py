#!/usr/bin/env python3

import argparse
import os
import subprocess
import warnings
from collections import Counter
from os import remove
from os.path import isfile

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from shapely import ops
from shapely.geometry import Point


warnings.simplefilter(action='ignore', category=FutureWarning)


def mitigate_branch_outlet_backpool(
    branch_dir,
    catchment_pixels_filename,
    catchment_pixels_polygonized_filename,
    catchment_reaches_filename,
    split_flows_filename,
    split_points_filename,
    nwm_streams_filename,
    dem_filename,
    slope_min,
    calculate_stats,
    dry_run,
):
    # --------------------------------------------------------------
    # Define functions

    # Test whether there are catchment size outliers (backpool error criteria 1)
    def catch_catchment_size_outliers(catchment_pixels_geom):
        # Quantify the amount of pixels in each catchment
        unique_values = np.unique(catchment_pixels_geom)
        value_counts = Counter(catchment_pixels_geom.ravel())

        vals, counts = [], []
        for value in unique_values:
            vals.append(value)
            counts.append(value_counts[value])

        # Create a structured array from the two lists and convert to pd df
        catchments_array = np.array(list(zip(vals, counts)), dtype=[('catchment_id', int), ('counts', int)])
        catchments_df = pd.DataFrame(catchments_array)

        # Remove row for a catchment_id of zero
        catchments_df = catchments_df[catchments_df['catchment_id'] > 0]

        # Calculate the mean and standard deviation of the 'counts' column
        mean_counts = catchments_df['counts'].mean()
        std_dev_counts = catchments_df['counts'].std()

        # Define the threshold for outliers (1 standard deviation from the mean)
        threshold = 1 * std_dev_counts

        # Create a new column 'outlier' with True for outliers and False for non-outliers
        catchments_df['outlier'] = abs(catchments_df['counts'] - mean_counts) > threshold

        # Quantify outliers
        if catchments_df['outlier'].any():
            num_outlier = catchments_df['outlier'].value_counts()[True]
        elif ~catchments_df['outlier'].all():
            num_outlier = 0

        if num_outlier == 0:
            print('No outliers detected in catchment size.')

            flagged_catchment = False
        elif num_outlier >= 1:
            print(f'{num_outlier} outlier catchment(s) found in catchment size.')

            flagged_catchment = True
        else:
            print('WARNING: Unable to check outlier count.')

        # Make a list of outlier catchment ID's
        catchments_df['outlier'] = catchments_df['outlier'].astype('string')
        outlier_catchment_ids = catchments_df[catchments_df['outlier'] == 'True']['catchment_id'].tolist()

        return flagged_catchment, outlier_catchment_ids

    # Extract raster catchment ID for the last point
    def get_raster_value(point):
        row, col = src.index(point.geometry.x, point.geometry.y)
        value = catchment_pixels_geom[row, col]
        return value

    # Function to test whether the catchment occurs at the outlet (Error Criteria 2)
    def check_if_outlet(last_point_geom, outlier_catchment_ids):
        # Get the catchment ID of the last_point_geom
        last_point_geom['catchment_id'] = last_point_geom.apply(get_raster_value, axis=1)

        # Check if values in 'catchment_id' column of the snapped point are in outlier_catchments_df
        outlet_flag = last_point_geom['catchment_id'].isin(outlier_catchment_ids)
        outlet_flag = any(outlet_flag)

        outlet_catchment_id = last_point_geom['catchment_id']

        return outlet_flag, outlet_catchment_id

    # Function to extract the last point from the line
    def extract_last_point(line):
        return line.coords[-1]

    # Function to extract the third-to-last point from the line
    def extract_pt_3tl(line):
        return line.coords[-3]

    # Function to count coordinates in a linestring
    def count_coordinates(line_string):
        return len(line_string.coords)

    # Function to trim the flow to the specified outlet point
    # Based on snap_and_trim_flow() from split_flows.py (as of 10/20/23)
    def snap_and_trim_splitflow(outlet_point, flows):
        if len(flows.index) == 1:
            flow = flows

        else:
            # Get the nearest flowline to the outlet point
            near_flows = []
            for index, point in outlet_point.iterrows():
                nearest_line = flows.loc[flows.distance(point['geometry']).idxmin()]
                near_flows.append(nearest_line)

            # Create a new GeoDataFrame with the closest flowline(s)
            near_flows_gdf = gpd.GeoDataFrame(near_flows, crs=flows.crs)

            # Trim the near flows to get the furthest downstream one
            if len(near_flows_gdf) == 1:
                flow = near_flows_gdf

            elif len(near_flows_gdf) > 1:
                # Get the highest node value (i.e. furthest down)
                last_node = near_flows_gdf['From_Node'].max()

                # Subset flows to get furthest down flow
                flow = near_flows_gdf[near_flows_gdf['From_Node'] == last_node]

        # Calculate flowline initial length
        toMetersConversion = 1e-3
        initial_length_km = flow.geometry.length.iloc[0] * toMetersConversion

        # Reset index if there is an index mismatch
        if flow.index != outlet_point.index:
            print('WARNING: Index mismatch detected')
            print(f'flow.index: {flow.index}; outlet_point.index: {outlet_point.index}')
            print('Resetting index of flow and outlet_point geometries.')

            flow = flow.reset_index()
            outlet_point = outlet_point.reset_index()

        # Snap the point to the line
        outlet_point['geometry'] = flow.interpolate(flow.project(outlet_point))

        # Split the flows at the snapped point
        # Note: Buffering is to account for python precision issues
        outlet_point_buffer = outlet_point.iloc[0]['geometry'].buffer(1)
        split_lines = ops.split(flow.iloc[0]['geometry'], outlet_point_buffer)

        # Get a list of the split line object indices
        split_lines_indices = list(range(0, len(split_lines.geoms), 1))

        # Produce a table of the geometry length of each splitlines geometries
        linestring_lengths = []
        linestring_geoms = []

        for index in split_lines_indices:
            linestring_geoms.append(split_lines.geoms[index])
            linestring_lengths.append(split_lines.geoms[index].length)

        split_lines_df = pd.DataFrame(
            {
                'split_lines_indices': split_lines_indices,
                'geometry': linestring_geoms,
                'len_flow': linestring_lengths,
            }
        )

        # Select the longest line segment from the split
        longest_split_line_df = split_lines_df[split_lines_df.len_flow == split_lines_df.len_flow.max()]

        # Convert the longest split line into a geodataframe
        # This removes the bits that got cut off
        longest_split_line_gdf = gpd.GeoDataFrame(
            longest_split_line_df, geometry=longest_split_line_df['geometry'], crs=flows.crs
        )

        # Get the new flow geometry
        flow_geometry = longest_split_line_gdf.iloc[0]['geometry']

        # Replace geometry in merged flowine
        if len(flows) > 1:
            flows.loc[flows['NextDownID'] == '-1', 'geometry'] = flow_geometry
        else:
            flows['geometry'] = flow_geometry

        return flows, initial_length_km

    def calculate_length_and_slope(flows, dem, slope_min):
        print('Recalculating length and slope of outlet segment...')

        # Select the last flowline segment (if there are multiple segments)
        if len(flows) > 1:
            flow = flows[flows['NextDownID'] == '-1']
        else:
            flow = flows

        # Calculate channel slope (adapted from split_flows.py on 11/21/23)
        start_point = flow.geometry.iloc[0].coords[0]
        end_point = flow.geometry.iloc[0].coords[-1]

        # Get start and end elevation
        start_elev, end_elev = [i[0] for i in rasterio.sample.sample_gen(dem, [start_point, end_point])]

        # Calculate the slope by differencing the elevations and dividing it by the length
        slope = float(abs(start_elev - end_elev) / flow.length)

        if slope < slope_min:
            slope = slope_min

        # Calculate length
        toMetersConversion = 1e-3
        LengthKm = flow.geometry.length * toMetersConversion

        # Update flows with the updated flow object
        if len(flows) > 1:
            flows.loc[flows['NextDownID'] == '-1', 'S0'] = slope
            flows.loc[flows['NextDownID'] == '-1', 'LengthKm'] = LengthKm

        else:
            flows['S0'] = slope
            flows['LengthKm'] = LengthKm

        return flows, LengthKm

    # Convert the geodataframe into a json format compatible to rasterio
    def gdf_to_json(gdf):
        import json

        return [json.loads(gdf.to_json())['features'][0]['geometry']]

    # Mask a raster to a json boundary and save the new raster
    def mask_raster_to_boundary(raster_path, boundary_json, save_path):
        with rasterio.open(raster_path) as raster:
            # Copy profile
            raster_profile = raster.profile.copy()

            # Mask catchment reaches to new boundary
            raster_masked, _ = mask(raster, boundary_json)

            if isfile(save_path):
                remove(save_path)

            # Save new catchment reaches TODO: update output path
            with rasterio.open(save_path, "w", **raster_profile, BIGTIFF='YES') as dest:
                dest.write(raster_masked[0, :, :], indexes=1)

    # --------------------------------------------------------------
    # Read in nwm lines, explode to ensure linestrings are the only geometry
    nwm_streams = gpd.read_file(nwm_streams_filename).explode(index_parts=True)

    # Check whether it's branch zero
    if 'levpa_id' in nwm_streams.columns:
        # --------------------------------------------------------------
        # If it's NOT branch zero, check for the two criteria and mitigate issue if needed

        # Read in data and check if the files exist
        print()
        print('Non-branch zero, loading data for test ...')

        # Read in the catchment pixels tif
        if isfile(catchment_pixels_filename):
            with rasterio.open(catchment_pixels_filename) as src:
                catchment_pixels_geom = src.read(1)
        else:
            catchment_pixels_geom = None
            print(f'WARNING: No catchment pixels geom at {catchment_pixels_filename}.')

        # # Read in the catchment reaches tif
        # if isfile(catchment_reaches_filename):
        #     with rasterio.open(catchment_reaches_filename) as src:
        #         catchment_reaches_geom = src.read(1)
        # else:
        #     catchment_reaches_geom = None
        #     print(f'No catchment pixels geome at {catchment_reaches_filename}.')

        # Read in split_flows_file and split_points_filename
        split_flows_geom = gpd.read_file(split_flows_filename)
        split_points_geom = gpd.read_file(split_points_filename)

        # Subset the split flows to get the last one
        split_flows_last_geom = split_flows_geom[split_flows_geom['NextDownID'] == '-1']

        # Check whether there are multiple NextDownID's of -1
        if len(split_flows_last_geom.index) == 1:
            one_neg1_nextdownid = True
        elif len(split_flows_last_geom.index) > 1:
            print('WARNING: Multiple stream segments found with NextDownID of -1.')
            one_neg1_nextdownid = False
        elif len(split_flows_last_geom.index) == 0:
            print('WARNING: Zero stream segments found with NextDownID of -1.')
            one_neg1_nextdownid = False

        # Check whether catchment_pixels_geom exists
        if (catchment_pixels_geom is not None) and (one_neg1_nextdownid is True):
            print(
                'A catchment geom file and only one NextDownID of -1 were found, testing for backpool criteria...'
            )  # verbose

            # Check whether any pixel catchment is substantially larger than other catchments
            # (Backpool Error Criteria 1)
            flagged_catchment, outlier_catchment_ids = catch_catchment_size_outliers(catchment_pixels_geom)

            # --------------------------------------------------------------
            # If there are outlier catchments, test whether the catchment occurs at the outlet
            # (Backpool Error Criteria 2)

            if flagged_catchment is True:
                # Apply the function to create a new GeoDataFrame
                last_point = split_flows_last_geom['geometry'].apply(extract_last_point).apply(Point)
                last_point_geom = gpd.GeoDataFrame(last_point, columns=['geometry'], crs=split_flows_geom.crs)

                # Check whether the last vertex corresponds with any of the outlier catchment ID's
                print('Flagged catchment(s) detected. Testing for second criteria.')
                outlet_flag, outlet_catchment_id = check_if_outlet(last_point_geom, outlier_catchment_ids)

            else:
                # If the catchment flag is False, just set the outlet flag to False automatically
                outlet_flag = False

            # If there is an outlier catchment at the outlet, set the snapped point as penultimate vertex
            if outlet_flag is True:
                # --------------------------------------------------------------
                # Trim flowline and flow points to penultimate vertex

                print(
                    'Incorrectly-large outlet pixel catchment detected. \
                      Snapping line points to penultimate vertex.'
                )

                # Count coordinates in 'geometry' column
                split_flows_last_geom['num_coordinates'] = split_flows_last_geom['geometry'].apply(
                    lambda x: count_coordinates(x) if x.geom_type == 'LineString' else None
                )

                # Get the last geometry and check the length of the last geometry
                if split_flows_last_geom['num_coordinates'].iloc[0] < 3:
                    # If the length is shorter than 3, extract the first point of the 2nd-to-last geometry
                    if len(split_flows_geom.index) > 1:
                        print('Extract first point of second-to-last geometry.')

                        # Get "from_node" of last segment
                        node_2tl = split_flows_last_geom['From_Node'].iloc[0]

                        # Subset second-to-last segment by selecting by the node connection
                        split_flows_2tl_geom = split_flows_geom[split_flows_geom['To_Node'] == node_2tl]

                        # Get last point of second-to-last segment
                        pt_3tl = split_flows_2tl_geom['geometry'].apply(extract_last_point).apply(Point)
                        trim_flowlines_proceed = True

                    else:
                        print('Geom length is shorter than 3 coords and no second-to-last geom available.')
                        print('Skipping branch outlet backpool mitigation for this branch.')
                        trim_flowlines_proceed = False

                else:
                    # If the length is 3 coords or greater, extract the third-to-last point of the last geom
                    print('Extract third-to-last point of last geometry.')
                    pt_3tl = split_flows_last_geom['geometry'].apply(extract_pt_3tl).apply(Point)
                    trim_flowlines_proceed = True

                if trim_flowlines_proceed is True:
                    # Apply the function to create a new GeoDataFrame
                    pt_3tl_geom = gpd.GeoDataFrame(pt_3tl, columns=['geometry'], crs=split_flows_geom.crs)

                    # Get the catchment ID of the new snapped point
                    pt_3tl_geom['catchment_id'] = pt_3tl_geom.apply(get_raster_value, axis=1)

                    # Snap and trim the flowline to the selected point
                    trimmed_flows, inital_length_km = snap_and_trim_splitflow(pt_3tl_geom, split_flows_geom)

                    # Create buffer around the updated flows geodataframe (and make sure it's all one shape)
                    buffer = trimmed_flows.buffer(10).geometry.union_all()

                    # Remove flowpoints that don't intersect with the trimmed flow line
                    split_points_filtered_geom = split_points_geom[split_points_geom.geometry.within(buffer)]

                    # --------------------------------------------------------------
                    # Calculate the slope and length of the newly trimmed flows
                    dem = rasterio.open(dem_filename, 'r')
                    output_flows, new_length_km = calculate_length_and_slope(trimmed_flows, dem, slope_min)

                    # --------------------------------------------------------------
                    # Polygonize pixel catchments using subprocess

                    # print('Polygonizing pixel catchments...')  # verbose

                    gdal_args = [
                        f'gdal_polygonize.py -8 -f GPKG {catchment_pixels_filename} \
                                 {catchment_pixels_polygonized_filename} catchments HydroID'
                    ]
                    return_code = subprocess.call(gdal_args, shell=True)

                    if return_code != 0:
                        print("gdal_polygonize failed with return code", return_code)
                    # else:
                    # print("gdal_polygonize executed successfully.")  # verbose

                    # Read in the polygonized catchment pixels
                    cp_poly_geom = gpd.read_file(catchment_pixels_polygonized_filename)

                    # --------------------------------------------------------------
                    # Mask problematic pixel catchment from the catchments rasters

                    # Convert series to number object
                    outlet_catchment_id = outlet_catchment_id.iloc[0]

                    # Filter out the flagged pixel catchment
                    cp_poly_filt_geom = cp_poly_geom[cp_poly_geom['HydroID'] != outlet_catchment_id]

                    # Dissolve the filtered pixel catchments into one geometry (the new boundary)
                    cp_new_boundary_geom = cp_poly_filt_geom.dissolve()

                    # Convert the geodataframe into a format compatible to rasterio
                    catchment_pixels_new_boundary_json = gdf_to_json(cp_new_boundary_geom)

                    if dry_run is True:
                        print('Dry run: Skipping raster masking!')

                    elif dry_run is False:
                        # Mask catchment reaches raster
                        mask_raster_to_boundary(
                            catchment_reaches_filename,
                            catchment_pixels_new_boundary_json,
                            catchment_reaches_filename,
                        )

                        # Mask catchment pixels raster
                        mask_raster_to_boundary(
                            catchment_pixels_filename,
                            catchment_pixels_new_boundary_json,
                            catchment_pixels_filename,
                        )

                        # print('Finished masking!')  # verbose

                    # --------------------------------------------------------------
                    if calculate_stats is True:
                        print('Calculating stats...')  # verbose

                        # Get the area of the old and new catchment boundaries
                        catchment_pixels_old_boundary_geom = cp_poly_geom.dissolve()

                        old_boundary_area = catchment_pixels_old_boundary_geom.area
                        new_boundary_area = cp_new_boundary_geom.area

                        # Calculate the km and percent differences of the catchment area
                        boundary_area_km_diff = float(old_boundary_area - new_boundary_area)
                        boundary_area_percent_diff = float(
                            ((boundary_area_km_diff) / old_boundary_area) * 100
                        )

                        # Calculate the difference (km) of the flowlines
                        flowlength_km_diff = float(inital_length_km - new_length_km)

                        # Create a dataframe with this data
                        backpool_stats_df = pd.DataFrame(
                            {
                                'flowlength_km_diff': [flowlength_km_diff],
                                'area_km_diff': [boundary_area_km_diff],
                                'area_percent_diff': [boundary_area_percent_diff],
                            }
                        )

                        backpool_stats_filepath = os.path.join(branch_dir, 'backpool_stats.csv')

                        # Save stats
                        backpool_stats_df.to_csv(backpool_stats_filepath, index=False)
                        print(f'Saved backpool stats to {backpool_stats_filepath}')

                    # --------------------------------------------------------------
                    # Save the outputs

                    if dry_run is True:
                        print('Test run... not saving outputs!')

                    elif dry_run is False:
                        if isfile(split_flows_filename):
                            remove(split_flows_filename)
                        if isfile(split_points_filename):
                            remove(split_points_filename)

                        output_flows.to_file(split_flows_filename, driver='GPKG', index=False)
                        split_points_filtered_geom.to_file(split_points_filename, driver='GPKG', index=False)

            else:
                print('Incorrectly-large outlet pixel catchment was NOT detected.')

        else:
            print('Will not test for outlet backpool problem.')

    else:
        print('Will not test for outlet backpool problem in branch zero.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Detect and mitigate branch outlet backpools issue.')
    parser.add_argument('-b', '--branch-dir', help='branch directory', required=True)
    parser.add_argument('-cp', '--catchment-pixels-filename', help='catchment-pixels-filename', required=True)
    parser.add_argument(
        '-cpp',
        '--catchment-pixels-polygonized-filename',
        help='catchment-pixels-polygonized-filename',
        required=True,
    )
    parser.add_argument(
        '-cr', '--catchment-reaches-filename', help='catchment-reaches-filename', required=True
    )
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename', required=True)
    parser.add_argument('-p', '--split-points-filename', help='split-points-filename', required=True)
    parser.add_argument('-n', '--nwm-streams-filename', help='nwm-streams-filename', required=True)
    parser.add_argument('-d', '--dem-filename', help='dem-filename', required=True)
    parser.add_argument('-t', '--slope-min', help='Minimum slope', required=True)
    parser.add_argument(
        '--calculate-stats', help='Optional flag to calculate stats', required=False, action='store_true'
    )
    parser.add_argument(
        '--dry-run', help='Optional flag to run without changing files.', required=False, action='store_true'
    )

    # Extract to dictionary and assign to variables
    args = vars(parser.parse_args())
    args['slope_min'] = float(args['slope_min'])
    args['calculate_stats'] = bool(args['calculate_stats'])

    mitigate_branch_outlet_backpool(**args)
