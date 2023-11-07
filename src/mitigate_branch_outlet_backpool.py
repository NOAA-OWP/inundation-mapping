#!/usr/bin/env python3

import argparse
import sys
from collections import OrderedDict
from os import remove
from os.path import isfile

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from shapely import ops, wkt
from shapely.geometry import LineString, Point
from shapely.ops import split as shapely_ops_split
from tqdm import tqdm
from collections import Counter


import build_stream_traversal
from utils.fim_enums import FIM_exit_codes
from utils.shared_functions import getDriver, mem_profile
from utils.shared_variables import FIM_ID

@mem_profile
def mitigate_branch_outlet_backpool(
    catchment_pixels_filename,
    split_flows_filename,
    split_points_filename,
    nwm_streams_filename,
):

    # --------------------------------------------------------------
    # Define functions

    # Test whether there are catchment size outliers (backpool error criteria 1)
    def catch_catchment_size_outliers(catchments_geom):
        # Quantify the amount of pixels in each catchment
        unique_values = np.unique(catchments_geom)
        value_counts = Counter(catchments_geom.ravel())

        vals, counts = [], []
        for value in unique_values:
            vals.append(value)
            counts.append(value_counts[value])

        # Create a structured array from the two lists and convert to pandas dataframe
        catchments_array = np.array(list(zip(vals, counts)), dtype=[('catchment_id', int), ('counts', int)])
        catchments_df = pd.DataFrame(catchments_array)

        # Remove row for a catchment_id of zero
        catchments_df = catchments_df[catchments_df['catchment_id'] > 0]

        # Calculate the mean and standard deviation of the 'counts' column
        mean_counts = catchments_df['counts'].mean()
        std_dev_counts = catchments_df['counts'].std()

        # Define the threshold for outliers (2 standard deviations from the mean)
        threshold = 2 * std_dev_counts

        # Create a new column 'outlier' with True for outliers and False for non-outliers
        catchments_df['outlier'] = abs(catchments_df['counts'] - mean_counts) > threshold

        # Quantify outliers
        num_outlier = catchments_df['outlier'].value_counts()[True]

        if num_outlier == 0:
            print('No outliers detected in catchment size.')

            flagged_catchment = False
        elif num_outlier >= 1:
            print(f'{num_outlier} outlier catchment(s) found in catchment size.') 

            flagged_catchment = True
        else:
            print('WARNING: Unable to check outlier count.')

        # Make a list of outlier catchment ID's
        outlier_catchment_ids = catchments_df[catchments_df['outlier'] == True]['catchment_id'].tolist()

        return flagged_catchment, outlier_catchment_ids

    # Extract raster catchment ID for the last point
    def get_raster_value(point):
        row, col = src.index(point.geometry.x, point.geometry.y)
        value = catchments_geom[row, col]
        return value

    # Test whether the catchment occurs at the outlet (backpool error criteria 2)
    def check_if_ID_is_outlet(last_point_geom, outlier_catchment_ids):
        # Get the catchment ID of the last_point_geom
        last_point_geom['catchment_id'] = last_point_geom.apply(get_raster_value, axis=1)

        # Check if values in 'catchment_id' column of the snapped point are in outlier_catchments_df
        outlet_flag = last_point_geom['catchment_id'].isin(outlier_catchment_ids)
        outlet_flag = any(outlet_flag)

        return outlet_flag

    # Create a function to extract the last point from the line
    def extract_last_point(line):
        return line.coords[-1]

    # Extract the third-to-last point from the line
    def extract_3rdtolast_point(line):
        return line.coords[-3]

    # Trim the flow to the specified outlet point
    # Based on snap_and_trim_flow() from split_flows.py (as of 10/20/23)
    def snap_and_trim_splitflow(outlet_point, flows):

        if len(flows) > 1:
            flow = flows[flows['NextDownID'] == '-1' ] # selects last one
        else: 
            flow = flows

        # Snap the point to the line
        outlet_point['geometry'] = flow.interpolate(flow.project(outlet_point))

        # Split the flows at the snapped point
        # Note: Buffering is to account for python precision issues
        outlet_point_buffer = outlet_point.iloc[0]['geometry'].buffer(1)
        split_lines = ops.split(flow.iloc[0]['geometry'], outlet_point_buffer) 

        # Get a list of the split line object indices
        split_lines_indices = list(range(0,len(split_lines.geoms),1))

        # Produce a table of the geometry length of each splitlines geometries
        linestring_lengths = []
        linestring_geoms = []

        for index in split_lines_indices:
            linestring_geoms.append(split_lines.geoms[index])
            linestring_lengths.append(split_lines.geoms[index].length)            

        split_lines_df = pd.DataFrame(
            {'split_lines_indices': split_lines_indices,
            'geometry': linestring_geoms,
            'linestring_lengths': linestring_lengths
            })

        # Select the longest split line    
        longest_split_line_df = split_lines_df[split_lines_df.linestring_lengths == split_lines_df.linestring_lengths.max()]

        # Convert the longest split line into a geodataframe (this removes the bits that got cut off)
        longest_split_line_gdf = gpd.GeoDataFrame(
            longest_split_line_df, 
            geometry=longest_split_line_df['geometry'], 
            crs=flows.crs
        )

        filename = '/branch_outlet_backpools/code_test_outputs/longest_split_line_gdf.gpkg' ## debug
        longest_split_line_gdf.to_file(filename, driver=getDriver(filename), index=False) ## debug

        # Get the new flow geometry
        flow_geometry = longest_split_line_gdf.iloc[0]['geometry']

        # Replace geometry in merged flowine
        if len(flows) > 1:
            print('Len flows is greater than 1....') ## debug
            flows.loc[flows['NextDownID'] == '-1', 'geometry'] = flow_geometry
        else: 
            print('Len flows is LESS than 1....') ## debug
            flows['geometry'] = flow_geometry

        filename = '/branch_outlet_backpools/code_test_outputs/flows.gpkg' ## debug
        flows.to_file(filename, driver=getDriver(filename), index=False) ## debug

        return flows

    # --------------------------------------------------------------

    # Read in nwm lines, explode to ensure linestrings are the only geometry
    nwm_streams = gpd.read_file(nwm_streams_filename).explode(index_parts=True)

    # Check whether it's branch zero 
    if 'levpa_id' in nwm_streams.columns:
        # If it's NOT branch zero, check for the two criteria and mitigate issue if needed

        # Read in data and check if the files exist
        print()
        print('Non-branch zero, loading data for test ...')

        if isfile(catchment_pixels_filename):
            with rasterio.open(catchment_pixels_filename) as src:
                catchments_geom = src.read(1)
        else:
            catchments_geom = None
            print(f'No catchment pixels geometry found at {catchment_pixels_filename}.') ## debug

        # Read in split_flows_file and split_points_filename
        split_flows_geom = gpd.read_file(split_flows_filename)
        split_points_geom = gpd.read_file(split_points_filename)

        # Check whether catchments_geom exists
        if catchments_geom is not None:
            print('Catchment geom found, testing for backpool criteria...') ## debug

            # Check whether any pixel catchment is substantially larger than other catchments (backpool error criteria 1)
            flagged_catchment, outlier_catchment_ids = catch_catchment_size_outliers(catchments_geom)

            # If there are outlier catchments, test whether the catchment occurs at the outlet (backpool error criteria 2)
            if flagged_catchment == True:
                # Subset the split flows to get the last one ## TODO: Check to make sure this is working as expected (test on a larger dataset)
                split_flows_last_geom = split_flows_geom[split_flows_geom['NextDownID'] == '-1' ]

                # Apply the function to create a new GeoDataFrame
                last_point = split_flows_last_geom['geometry'].apply(extract_last_point).apply(Point)
                last_point_geom = gpd.GeoDataFrame(last_point, columns=['geometry'], crs=split_flows_geom.crs)

                # Check whether the last vertex corresponds with any of the outlier catchment ID's
                print('Flagged catchment(s) detected. Testing for second criteria.') 
                outlet_flag = check_if_ID_is_outlet(last_point_geom, outlier_catchment_ids)

            else: 
                # If the catchment flag is False, just set the outlet flag to False automatically
                outlet_flag = False
                
            # If there is an outlier catchment at the outlet, set the snapped point to be the penultimate (second-to-last) vertex
            if outlet_flag == True:
                print('Incorrectly-large outlet pixel catchment detected. Snapping line points to penultimate vertex.')

                # Apply the function to create a new GeoDataFrame
                thirdtolast_point = split_flows_last_geom['geometry'].apply(extract_3rdtolast_point).apply(Point)
                thirdtolast_point_geom = gpd.GeoDataFrame(thirdtolast_point, columns=['geometry'], crs=split_flows_geom.crs)

                # Get the catchment ID of the new snapped_point
                thirdtolast_point_geom['catchment_id'] = thirdtolast_point_geom.apply(get_raster_value, axis=1) ## mainly for debug (but could be good to keep)

                # Snap and trim the flowline to the selected point
                output_flows = snap_and_trim_splitflow(thirdtolast_point_geom, split_flows_geom)

                # Create  buffer around the updated flows geodataframe (and make sure it's all one shape)
                flows_buffer = output_flows.buffer(10).geometry.unary_union

                # Remove flowpoints that don't intersect with the trimmed flow line
                split_points_filtered_geom = split_points_geom[split_points_geom.geometry.within(flows_buffer)]

            else:
                print('Incorrectly-large outlet pixel catchment was NOT detected.')
                output_flows = split_flows_geom
                split_points_filtered_geom = split_points_geom

        else:
            print('Catchment geom file not found, unable to test for backpool error...')
            output_flows = split_flows_geom
            split_points_filtered_geom = split_points_geom

        # TODO: figure out if I need to recalculate this section: "Iterate through flows and calculate channel slope, manning's n, and LengthKm for each segment"

        # Save the outputs
        print('Writing outputs ...')

        # ## temp output filenames
        # split_flows_filename = 'branch_outlet_backpools/test_outputs/gms_branch_backpool_BEFORE_allintermeds_copy/13080002/branches/6077000088/demDerived_reaches_split_flows_6077000088_TEST.gpkg' ## debug -> remove later once I know this is WORKING 
        # split_points_filename = 'branch_outlet_backpools/test_outputs/gms_branch_backpool_BEFORE_allintermeds_copy/13080002/branches/6077000088/demDerived_reaches_split_points_6077000088_TEST.gpkg' ## debug -> remove later once I know this is WORKING 

        if isfile(split_flows_filename):
            remove(split_flows_filename)
        if isfile(split_points_filename):
            remove(split_points_filename)

        output_flows.to_file(split_flows_filename, driver=getDriver(split_flows_filename), index=False)
        split_points_filtered_geom.to_file(split_points_filename, driver=getDriver(split_points_filename), index=False)

    else:
        print('Will not test for branch outlet backpool error in branch zero.')

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='mitigate_branch_outlet_backpool.py')
    parser.add_argument('-c', '--catchment-pixels-filename', help='catchment-pixels-filename', required=True)
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename', required=True)
    parser.add_argument('-p', '--split-points-filename', help='split-points-filename', required=True)
    parser.add_argument('-n', '--nwm-streams-filename', help='nwm-streams-filename', required=True)


    # Extract to dictionary and assign to variables
    args = vars(parser.parse_args())

    mitigate_branch_outlet_backpool(**args)





