#!/usr/bin/env python3

import argparse
import sys
from collections import OrderedDict
import os
from os import remove
from os.path import isfile

from osgeo import gdal, ogr
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from shapely import ops, wkt
from shapely.geometry import LineString, Point
from shapely.ops import split as shapely_ops_split
from tqdm import tqdm
from collections import Counter
import fiona

import build_stream_traversal
from utils.fim_enums import FIM_exit_codes
from utils.shared_functions import getDriver, mem_profile
from utils.shared_variables import FIM_ID

import subprocess

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
        value = catchment_pixels_geom[row, col]
        return value

    # Test whether the catchment occurs at the outlet (backpool error criteria 2)
    def check_if_ID_is_outlet(last_point_geom, outlier_catchment_ids):
        # Get the catchment ID of the last_point_geom
        last_point_geom['catchment_id'] = last_point_geom.apply(get_raster_value, axis=1)

        # Check if values in 'catchment_id' column of the snapped point are in outlier_catchments_df
        outlet_flag = last_point_geom['catchment_id'].isin(outlier_catchment_ids)
        outlet_flag = any(outlet_flag)

        outlet_catchment_id = last_point_geom['catchment_id']

        return outlet_flag, outlet_catchment_id

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
            flow = flows[flows['NextDownID'] == '-1' ] # selects last flowline segment
        else: 
            flow = flows

        # Calculate flowline initial length
        toMetersConversion = 1e-3
        initial_length_km = flow.geometry.length * toMetersConversion

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

        # Select the longest line segment from the split     
        longest_split_line_df = split_lines_df[split_lines_df.linestring_lengths == split_lines_df.linestring_lengths.max()]

        # Convert the longest split line into a geodataframe (this removes the bits that got cut off)
        longest_split_line_gdf = gpd.GeoDataFrame(
            longest_split_line_df, 
            geometry=longest_split_line_df['geometry'], 
            crs=flows.crs
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
            flow = flows[flows['NextDownID'] == '-1' ]
        else: 
            flow = flows

        # Calculate channel slope (adapted from split_flows.py on 11/21/23)
        start_point = flow.geometry.iloc[0].coords[0]
        end_point = flow.geometry.iloc[0].coords[-1]

        # Get start and end elevation
        start_elev, end_elev = [
            i[0] for i in rasterio.sample.sample_gen(dem, [start_point, end_point])
        ]

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

            # Save new catchment reaches 
            with rasterio.open(save_path, "w", **raster_profile, BIGTIFF='YES') as dest: ## TODO: update output path
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
            print(f'No catchment pixels geometry found at {catchment_pixels_filename}.') 

        # Read in the catchment reaches tif
        if isfile(catchment_reaches_filename):
            with rasterio.open(catchment_reaches_filename) as src:
                catchment_reaches_geom = src.read(1)
        else:
            catchment_reaches_geom = None
            print(f'No catchment pixels geometry found at {catchment_reaches_filename}.') 

        # Read in split_flows_file and split_points_filename
        split_flows_geom = gpd.read_file(split_flows_filename)
        split_points_geom = gpd.read_file(split_points_filename)

        # Check whether catchment_pixels_geom exists
        if catchment_pixels_geom is not None:
            print('Catchment geom found, testing for backpool criteria...') ## verbose

            # Check whether any pixel catchment is substantially larger than other catchments (backpool error criteria 1)
            flagged_catchment, outlier_catchment_ids = catch_catchment_size_outliers(catchment_pixels_geom)

            # --------------------------------------------------------------
            # If there are outlier catchments, test whether the catchment occurs at the outlet (backpool error criteria 2)

            if flagged_catchment == True:
                # Subset the split flows to get the last one ## TODO: Check to make sure this is working as expected (test on a larger dataset)
                split_flows_last_geom = split_flows_geom[split_flows_geom['NextDownID'] == '-1' ]

                # Apply the function to create a new GeoDataFrame
                last_point = split_flows_last_geom['geometry'].apply(extract_last_point).apply(Point)
                last_point_geom = gpd.GeoDataFrame(last_point, columns=['geometry'], crs=split_flows_geom.crs)

                # Check whether the last vertex corresponds with any of the outlier catchment ID's
                print('Flagged catchment(s) detected. Testing for second criteria.') 
                outlet_flag, outlet_catchment_id = check_if_ID_is_outlet(last_point_geom, outlier_catchment_ids)

            else: 
                # If the catchment flag is False, just set the outlet flag to False automatically
                outlet_flag = False
                
            # If there is an outlier catchment at the outlet, set the snapped point to be the penultimate (second-to-last) vertex
            if outlet_flag == True:
                # --------------------------------------------------------------
                # Trim flowline and flow points to penultimate vertex

                print('Incorrectly-large outlet pixel catchment detected. Snapping line points to penultimate vertex.')

                # Apply the function to create a new GeoDataFrame
                thirdtolast_point = split_flows_last_geom['geometry'].apply(extract_3rdtolast_point).apply(Point)
                thirdtolast_point_geom = gpd.GeoDataFrame(thirdtolast_point, columns=['geometry'], crs=split_flows_geom.crs)

                # Get the catchment ID of the new snapped point
                thirdtolast_point_geom['catchment_id'] = thirdtolast_point_geom.apply(get_raster_value, axis=1) ## mainly for debug (but could be good to keep)

                # Snap and trim the flowline to the selected point
                trimmed_flows, inital_length_km = snap_and_trim_splitflow(thirdtolast_point_geom, split_flows_geom)

                # Create buffer around the updated flows geodataframe (and make sure it's all one shape)
                flows_buffer = trimmed_flows.buffer(10).geometry.unary_union

                # Remove flowpoints that don't intersect with the trimmed flow line
                split_points_filtered_geom = split_points_geom[split_points_geom.geometry.within(flows_buffer)]

                # --------------------------------------------------------------
                # Calculate the slope and length of the newly trimmed flows
                dem = rasterio.open(dem_filename, 'r')
                output_flows, new_length_km = calculate_length_and_slope(trimmed_flows, dem, slope_min)

                # --------------------------------------------------------------
                # Polygonize pixel catchments using subprocess

                # print('Polygonizing pixel catchments...')  ## verbose

                gdal_args = [f'gdal_polygonize.py -8 -f GPKG {catchment_pixels_filename} {catchment_pixels_polygonized_filename} catchments HydroID']
                return_code = subprocess.call(gdal_args, shell=True) 

                if return_code != 0:
                    print("gdal_polygonize failed with return code", return_code)
                # else:
                    # print("gdal_polygonize executed successfully.")  ## verbose

                # Read in the polygonized catchment pixels
                catchment_pixels_poly_geom = gpd.read_file(catchment_pixels_polygonized_filename)

                # --------------------------------------------------------------
                # Mask problematic pixel catchment from the catchments rasters

                # print('Masking problematic pixel catchment from catchment reaches raster...')  ## verbose

                # Convert series to number object
                outlet_catchment_id = outlet_catchment_id.iloc[0]

                # Filter out the flagged pixel catchment
                catchment_pixels_poly_filt_geom = catchment_pixels_poly_geom[catchment_pixels_poly_geom['HydroID']!=outlet_catchment_id]

                # Dissolve the filtered pixel catchments into one geometry (the new boundary)
                catchment_pixels_new_boundary_geom = catchment_pixels_poly_filt_geom.dissolve()

                # Convert the geodataframe into a format compatible to rasterio
                catchment_pixels_new_boundary_json = gdf_to_json(catchment_pixels_new_boundary_geom)

                # Mask catchment reaches raster
                mask_raster_to_boundary(catchment_reaches_filename, catchment_pixels_new_boundary_json, catchment_reaches_filename)

                # Mask catchment pixels raster
                mask_raster_to_boundary(catchment_pixels_filename, catchment_pixels_new_boundary_json, catchment_pixels_filename)

                # print('Finished masking!') ## verbose

                # --------------------------------------------------------------
                if calculate_stats == True:

                    print('Calculating stats...') ## verbose

                    # Get the area of the old and new catchment boundaries
                    catchment_pixels_old_boundary_geom = catchment_pixels_poly_geom.dissolve()

                    old_boundary_area = catchment_pixels_old_boundary_geom.area
                    new_boundary_area = catchment_pixels_new_boundary_geom.area

                    # Calculate the km and percent differences of the catchment area
                    boundary_area_km_diff = old_boundary_area-new_boundary_area 
                    boundary_area_percent_diff = ((old_boundary_area-new_boundary_area)/old_boundary_area)*100

                    # Calculate the difference (km) of the flowlines
                    flowlength_km_diff = inital_length_km - new_length_km

                    # Create a dataframe with this data (TODO: Write a script that goes into a runfile and compiles these. That's where I'll add branch and HUC ID to this data.)
                    backpool_stats_df = pd.DataFrame({'flowlength_km_diff': [flowlength_km_diff],
                                             'boundary_area_km_diff': [boundary_area_km_diff],
                                             'boundary_area_percent_diff': [boundary_area_percent_diff]
                                             })

                    # Save stats
                    backpool_stats_filepath = os.path.join(branch_dir, 'backpool_stats.csv')
                    backpool_stats_df.to_csv(backpool_stats_filepath, index=False)



                # --------------------------------------------------------------
                # Save the outputs

                # Toggle whether it's a test run or not
                test_run = True

                # Save outputs
                if test_run == False:
                    # print('Writing outputs ...') ## verbose

                    if isfile(split_flows_filename): 
                        remove(split_flows_filename)
                    if isfile(split_points_filename):
                        remove(split_points_filename)

                    output_flows.to_file(split_flows_filename, driver=getDriver(split_flows_filename), index=False)
                    split_points_filtered_geom.to_file(split_points_filename, driver=getDriver(split_points_filename), index=False)
                
                elif test_run == True:

                    print('Test run... not saving outputs!')

            else:
                print('Incorrectly-large outlet pixel catchment was NOT detected.') 

        else:
            print('Catchment geom file not found, unable to test for backpool error...') 

    else:
        print('Will not test for branch outlet backpool error in branch zero.') 

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='mitigate_branch_outlet_backpool.py')
    parser.add_argument('-b', '--branch-dir', help='branch directory', required=True)
    parser.add_argument('-cp', '--catchment-pixels-filename', help='catchment-pixels-filename', required=True)
    parser.add_argument('-cpp', '--catchment-pixels-polygonized-filename', help='catchment-pixels-polygonized-filename', required=True)
    parser.add_argument('-cr', '--catchment-reaches-filename', help='catchment-reaches-filename', required=True)
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename', required=True)
    parser.add_argument('-p', '--split-points-filename', help='split-points-filename', required=True)
    parser.add_argument('-n', '--nwm-streams-filename', help='nwm-streams-filename', required=True)
    parser.add_argument('-d', '--dem-filename', help='dem-filename', required=True)
    parser.add_argument('-t', '--slope-min', help='Minimum slope', required=True)
    parser.add_argument('-cs', '--calculate-stats', help='Caclulate stats (boolean)', required=True)

    # Extract to dictionary and assign to variables
    args = vars(parser.parse_args())
    args['slope_min'] = float(args['slope_min'])
    args['calculate-stats'] = bool(args['calculate-stats'])

    mitigate_branch_outlet_backpool(**args)