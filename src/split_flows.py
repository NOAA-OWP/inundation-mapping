#!/usr/bin/env python3

'''
Description

    ARGUMENTS

        flows_filename: 
            Filename of an existing input file. <current_branch_folder>/demDerived_reaches_<current_branch_id>.shp
        
        dem_filename: 
            Filename of an existing input file. <current_branch_folder>/dem_thalwegCond_<current_branch_id>.tif 
        
        catchment_pixels_filename: 
            Filename of an existing input file. <current_branch_folder>/gw_catchments_pixels_<current_branch_id>.tif
        
        levelpaths_filename: 
            Filename of an existing input file. <HUC_data_folder>/nwm_subset_streams_levelPaths_dissolved.gpkg
        
        split_flows_filename: 
            Filename of a split_flows.py output product. <current_branch_folder>/demDerived_reaches_split_<current_branch_id>.gpkg
        
        split_points_filename: 
            Filename of a split_flows.py output product. <current_branch_folder>/demDerived_reaches_split_points_<current_branch_id>.gpkg
        
        wbd8_clp_filename: 
            <HUC_data_folder>/wbd8_clp.gpkg 
        
        lakes_filename: 
            <HUC_data_folder>/nwm_lakes_proj_subset.gpkg 
        
        nwm_streams_filename: 

        
        max_length: 
            Constant that describes ___. 
        
        slope_min: 
            Constant that describes ___. 
        
        lakes_buffer_input: 
            Constant that describes ___. 


        PROCESSING STEPS

        1) Split stream segments based on lake boundaries and input threshold distance
        2) Calculate channel slope, manning's n, and LengthKm for each segment
        3) Create unique ids using HUC8 boundaries (and unique FIM_ID column)
        4) Create network traversal attribute columns (To_Node, From_Node, NextDownID)
        5) Create points layer with segment verticies encoded with HydroID's (used for catchment delineation in next step)

'''

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
def split_flows(
    flows_filename,
    dem_filename,
    catchment_pixels_filename, 
    levelpaths_filename,
    split_flows_filename,
    split_points_filename,
    wbd8_clp_filename,
    lakes_filename,
    nwm_streams_filename,
    max_length,
    slope_min,
    lakes_buffer_input,
):
    # --------------------------------------------------------------
    # Define functions

    def snap_and_trim_flow(snapped_point, flows):

        # Find the flowline nearest to the snapped point (if there's multiple flowlines)
        if len(flows) > 1:
            sjoin_nearest = gpd.sjoin_nearest(snapped_point, flows, max_distance=100)
            if sjoin_nearest.empty:
                return flows

            if len(sjoin_nearest) > 1:
                sjoin_nearest = sjoin_nearest[sjoin_nearest['LINKNO'].isin(sjoin_nearest['DSLINKNO'])]

            nearest_index = int(sjoin_nearest['LINKNO'].iloc[0])
            flow = flows[flows['LINKNO'] == nearest_index]
            flow.index = [0]

        else:
            flow = flows
            nearest_index = None

        # Snap to DEM flows
        snapped_point['geometry'] = flow.interpolate(flow.project(snapped_point.geometry))[0]

        # Trim flows to snapped point
        trimmed_line = shapely_ops_split(
            flow.iloc[0]['geometry'], snapped_point.iloc[0]['geometry'].buffer(1)
        )
        # Note: Buffering is to account for python precision issues, print(demDerived_reaches.distance(snapped_point) < 1e-8)

        # Edge cases: line string not split?, nothing is returned, split does not preserve linestring order?
        # Note to dear reader: last here is really the most upstream segment (see caveats above).
        # When we split we should get 3 segments, the most downstream one
        # the tiny 1 meter segment that falls within the snapped point buffer, and the most upstream one.
        # We want that last one which is why we trimmed_line[len(trimmed_line)-1]

        last_line_segment = pd.DataFrame(
            {'id': ['first'], 'geometry': [trimmed_line.geoms[len(trimmed_line.geoms) - 1].wkt]}
        )

        # Note: When we update geopandas verison: last_line_segment = gpd.GeoSeries.from_wkt(last_line_segment)
        last_line_segment['geometry'] = last_line_segment['geometry'].apply(wkt.loads)
        last_line_segment_geodataframe = gpd.GeoDataFrame(last_line_segment).set_crs(flow.crs)

        # Replace geometry in merged flowine
        flow_geometry = last_line_segment_geodataframe.iloc[0]['geometry']

        if nearest_index is not None:
            # Update geometry of line closest to snapped_point
            flows.loc[flows['LINKNO'] == nearest_index, 'geometry'] = flow_geometry
        else:
            flows['geometry'] = flow_geometry

        return flows

    # Check whether the catchment is substantially larger than other catchments (backpool error criteria 1)
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
        catchments_df = catchments_df[catchments_df['catchment_id']>0]

        # Calculate the mean and standard deviation of the 'counts' column
        mean_counts = catchments_df['counts'].mean()
        std_dev_counts = catchments_df['counts'].std()

        # Define the threshold for outliers (2 standard deviations from the mean)
        threshold = 2 * std_dev_counts

        # Create a new column 'outlier' with True for outliers and False for non-outliers
        catchments_df['outlier'] = (abs(catchments_df['counts'] - mean_counts) > threshold)

        # Quantify outliers
        num_outlier = catchments_df['outlier'].value_counts()[True]

        if num_outlier == 0:
            print('No outliers detected in catchment size.') ## debug
            flagged_catchment = False
        elif num_outlier >= 1:
            print(f'{num_outlier} outlier catchment(s) found in catchment size.') ## debug
            flagged_catchment = True
        else:
            print('WARNING: Unable to check outlier count.') ## debug

        print(f'Outlier catchments present? {flagged_catchment}') ## debug

        # Make a list of outlier catchment ID's
        outlier_catchment_ids = catchments_df[catchments_df['outlier']==True]['catchment_id'].tolist()

        return flagged_catchment, outlier_catchment_ids

    # Extract raster catchment ID for the last point
    def get_raster_value(point):
        row, col = src.index(point.geometry.x, point.geometry.y)
        value = catchments_geom[row, col]
        return value

    # Test whether the catchment occurs at the outlet (backpool error criteria 2)
    def check_if_ID_is_outlet(snapped_point, outlier_catchment_ids): 

        # Get the catchment ID of the snapped_point
        snapped_point['catchment_id'] = snapped_point.apply(get_raster_value, axis=1)

        # Check if values in 'catchment_id' column of the snapped point are in outlier_catchments_df
        outlet_flag = snapped_point['catchment_id'].isin(outlier_catchment_ids)
        outlet_flag = any(outlet_flag)

        return outlet_flag

    # --------------------------------------------------------------
    # Read in data and set constants

    print('Loading data ...')
    
    toMetersConversion = 1e-3

    # Read in flows data and check for relevant streams within HUC boundary
    flows = gpd.read_file(flows_filename)
    if len(flows) == 0:
        # Note: This is not an exception, but a custom exit code that can be trapped
        print("No relevant streams within HUC boundaries.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # will send a 61 back

    # Read in and format other data
    wbd8 = gpd.read_file(wbd8_clp_filename)
    dem = rasterio.open(dem_filename, 'r')

    with rasterio.open(catchment_pixels_filename) as src:
        catchments_geom = src.read(1) 

    if isfile(lakes_filename):
        lakes = gpd.read_file(lakes_filename)
    else:
        lakes = None

    wbd8 = wbd8.filter(items=[FIM_ID, 'geometry'])
    wbd8 = wbd8.set_index(FIM_ID)

    # Note: We don't index parts because the new index format causes problems later on
    flows = flows.explode(index_parts=False)

    flows = flows.to_crs(wbd8.crs) # Note: temporary solution

    split_flows = []
    slopes = []
    hydro_id = 'HydroID'

    # backpool_error_log = [] ## debug -- TODO: Remove once the issue is well-tested (ED)

    # --------------------------------------------------------------
    # Trim DEM streams to NWM branch terminus
    # If loop addressing: https://github.com/NOAA-OWP/inundation-mapping/issues/560
    
    print('Trimming DEM stream to NWM branch terminus...')

    # Read in nwm lines, explode to ensure linestrings are the only geometry
    nwm_streams = gpd.read_file(nwm_streams_filename).explode(index_parts=True)

    # If it's NOT branch 0: Dissolve levelpath
    if 'levpa_id' in nwm_streams.columns:
        if len(nwm_streams) > 1:
            # Dissolve the linestring TODO: How much faith should I hold that these are digitized with flow? (JC)
            linestring_geo = ops.linemerge(nwm_streams.dissolve(by='levpa_id').iloc[0]['geometry'])
        else:
            linestring_geo = nwm_streams.iloc[0]['geometry']

        # If the linesting is in MultiLineString format, get the last LineString
        if linestring_geo.geom_type == 'MultiLineString':
            linestring_geo = linestring_geo.geoms[-1]

        # Identify the end vertex (most downstream, should be last), transform into geodataframe
        terminal_nwm_point = []
        last = Point(linestring_geo.coords[-1])
        terminal_nwm_point.append({'ID': 'terminal', 'geometry': last})
        snapped_point = gpd.GeoDataFrame(terminal_nwm_point).set_crs(nwm_streams.crs)

        # Check whether any pixel catchment is substantially larger than other catchments (backpool error criteria 1)
        flagged_catchment, outlier_catchment_ids = catch_catchment_size_outliers(catchments_geom)

        # If there are outlier catchments, test whether the catchment occurs at the outlet (backpool error criteria 2)
        if flagged_catchment == True:
            print('Flagged catchment(s) detected. Testing for second criteria.') ## debug
            outlet_flag = check_if_ID_is_outlet(snapped_point, outlier_catchment_ids)

        # If there is an outlier catchment at the outlet, set the snapped point to be the penultimate (second-to-last) vertex
        if outlet_flag == True:
            print('Incorrectly-large outlet pixel catchment detected. Snapping line to penultimate vertex.')

            # Initialize snapped_point object (so we can make a new one)
            snapped_point=[]

            # Identify the penultimate vertex (second-to-most downstream, should be second-to-last), transform into geodataframe
            penultimate_nwm_point = []
            second_to_last = Point(linestring_geo.coords[-2])
            penultimate_nwm_point.append({'ID': 'terminal', 'geometry': second_to_last})
            snapped_point = gpd.GeoDataFrame(penultimate_nwm_point).set_crs(nwm_streams.crs)

            # Get the catchment ID of the new snapped_point
            snapped_point['catchment_id'] = snapped_point.apply(get_raster_value, axis=1)

            # Log instances of the backpool error (TODO: Remove this once this issue has been thoroughly tested) ED
            # backpool_error_log.append(levpa_id)

        # Snap and trim the flowline to the snapped point 
        flows = snap_and_trim_flow(snapped_point, flows)

        # print('snapped_point: ') ## debug
        # print(snapped_point) ## debug
        # print('flows: ') ## debug
        # print(flows) ## debug

    # If it is branch 0: Loop over NWM terminal segments
    else:
        nwm_streams_terminal = nwm_streams[nwm_streams['to'] == 0]
        if not nwm_streams_terminal.empty:
            for i, row in nwm_streams_terminal.iterrows():
                linestring_geo = row['geometry']

                # Identify the end vertex (most downstream, should be last), transform into geodataframe
                terminal_nwm_point = []
                last = Point(linestring_geo.coords[-1])
                terminal_nwm_point.append({'ID': 'terminal', 'geometry': last})
                snapped_point = gpd.GeoDataFrame(terminal_nwm_point).set_crs(nwm_streams.crs)

                # Snap and trim the flowline to the snapped point
                flows = snap_and_trim_flow(snapped_point, flows)

    # Split stream segments at HUC8 boundaries
    print('Splitting stream segments at HUC8 boundaries...')
    flows = (
        gpd.overlay(flows, wbd8, how='union', keep_geom_type=True)
        .explode(index_parts=True)
        .reset_index(drop=True)
    )
    flows = flows[~flows.is_empty]

    # Make sure flows object doesn't have a length of zero
    if len(flows) == 0:
        # Note: This is not an exception, but a custom exit code that can be trapped
        print("No relevant streams within HUC boundaries.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # Note: Will send a 61 back
    
    # Check for lake features and split flows at lake boundaries, if needed
    if lakes is not None and len(flows) > 0:
        if len(lakes) > 0:
            print('Splitting stream segments at ' + str(len(lakes)) + ' waterbodies...')

            # Create splits at lake boundaries
            lakes = lakes.filter(items=['newID', 'geometry'])
            lakes = lakes.set_index('newID')
            flows = (
                gpd.overlay(flows, lakes, how='union', keep_geom_type=True)
                .explode(index_parts=True)
                .reset_index(drop=True)
            )
            lakes_buffer = lakes.copy()
            lakes_buffer['geometry'] = lakes.buffer(
                lakes_buffer_input
            )  # Note: adding X meter buffer for spatial join comparison (currently using 20meters)

    print('Splitting ' + str(len(flows)) + ' stream segments using on max length of ' + str(max_length) + ' meters')

    # Remove empty flow geometries
    flows = flows.loc[~flows.is_empty, :]

    # Exit processing if length of flows is zero
    if len(flows) == 0:
        # Note: This is not an exception, but a custom exit code that can be trapped
        print("No relevant streams within HUC boundaries.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # will send a 61 back

    # Iterate through flows and calculate channel slope, manning's n, and LengthKm for each segment
    for i, lineString in tqdm(enumerate(flows.geometry), total=len(flows.geometry)):

        # Reverse geometry order (necessary for BurnLines)
        lineString = LineString(lineString.coords[::-1])

        # Skip lines of zero length
        if lineString.length == 0:
            continue

        # Process existing reaches that are less than the max_length
        if lineString.length < max_length:
            split_flows = split_flows + [lineString]
            line_points = [point for point in zip(*lineString.coords.xy)]

            # Calculate channel slope
            start_point = line_points[0]
            end_point = line_points[-1]
            start_elev, end_elev = [i[0] for i in rasterio.sample.sample_gen(dem, [start_point, end_point])]
            slope = float(abs(start_elev - end_elev) / lineString.length)
            if slope < slope_min:
                slope = slope_min
            slopes = slopes + [slope]
            continue

        # Calculate the split length
        splitLength = lineString.length / np.ceil(lineString.length / max_length)

        cumulative_line = []
        line_points = []
        last_point = []

        last_point_in_entire_lineString = list(zip(*lineString.coords.xy))[-1]

        # Calculate cumulative length and channel slope, 
        for point in zip(*lineString.coords.xy):
            cumulative_line = cumulative_line + [point]
            line_points = line_points + [point]
            numberOfPoints_in_cumulative_line = len(cumulative_line)

            if last_point:
                cumulative_line = [last_point] + cumulative_line
                numberOfPoints_in_cumulative_line = len(cumulative_line)
            elif numberOfPoints_in_cumulative_line == 1:
                continue

            cumulative_length = LineString(cumulative_line).length

            if cumulative_length >= splitLength:
                splitLineString = LineString(cumulative_line)
                split_flows = split_flows + [splitLineString]

                # Calculate channel slope
                start_point = cumulative_line[0]
                end_point = cumulative_line[-1]
                start_elev, end_elev = [
                    i[0] for i in rasterio.sample.sample_gen(dem, [start_point, end_point])
                ]
                slope = float(abs(start_elev - end_elev) / splitLineString.length)
                if slope < slope_min:
                    slope = slope_min
                slopes = slopes + [slope]

                last_point = end_point

                if last_point == last_point_in_entire_lineString:
                    continue

                cumulative_line = []
                line_points = []

        splitLineString = LineString(cumulative_line)
        split_flows = split_flows + [splitLineString]

        # Calculate channel slope
        start_point = cumulative_line[0]
        end_point = cumulative_line[-1]
        start_elev, end_elev = [i[0] for i in rasterio.sample.sample_gen(dem, [start_point, end_point])]
        slope = float(abs(start_elev - end_elev) / splitLineString.length)
        if slope < slope_min:
            slope = slope_min
        slopes = slopes + [slope]

    # Assemble the slopes and split flows into a geodataframe
    split_flows_gdf = gpd.GeoDataFrame(
        {'S0': slopes, 'geometry': split_flows}, crs=flows.crs, geometry='geometry'
    )
    split_flows_gdf['LengthKm'] = split_flows_gdf.geometry.length * toMetersConversion
    if lakes is not None:
        split_flows_gdf = gpd.sjoin(
            split_flows_gdf, lakes_buffer, how='left', predicate='within'
        )  # Note: Options include intersects, within, contains, crosses
        split_flows_gdf = split_flows_gdf.rename(columns={"index_right": "LakeID"}).fillna(-999)
    else:
        split_flows_gdf['LakeID'] = -999

    # Drop duplicate stream segments
    split_flows_gdf = split_flows_gdf.drop_duplicates() # TODO: Need to figure out why so many duplicate stream segments for 04010101 FR (JC)

    # Create IDs and Network Traversal Columns
    addattributes = build_stream_traversal.build_stream_traversal_columns()
    tResults = None
    tResults = addattributes.execute(split_flows_gdf, wbd8, hydro_id)
    if tResults[0] == 'OK':
        split_flows_gdf = tResults[1]
    else:
        print('Error: Could not add network attributes to stream segments')

    # Remove single node segments
    split_flows_gdf = split_flows_gdf.query("From_Node != To_Node")

    split_points = OrderedDict()

    # Iterate through split flows line segments and create the points along each segment
    for index, segment in split_flows_gdf.iterrows():

        # Get the points of the linestring geometry
        lineString = segment.geometry

        for point in zip(*lineString.coords.xy):
            if point in split_points:
                if segment.NextDownID == split_points[point]:
                    pass
                else:
                    split_points[point] = segment[hydro_id]
            else:
                split_points[point] = segment[hydro_id]

    hydroIDs_points = [hidp for hidp in split_points.values()]
    split_points = [Point(*point) for point in split_points]

    split_points_gdf = gpd.GeoDataFrame(
        {'id': hydroIDs_points, 'geometry': split_points}, crs=flows.crs, geometry='geometry'
    )

    # --------------------------------------------------------------
    # Save the outputs
    print('Writing outputs ...')

    if isfile(split_flows_filename):
        remove(split_flows_filename)
    if isfile(split_points_filename):
        remove(split_points_filename)

    if len(split_flows_gdf) == 0:
        # this is not an exception, but a custom exit code that can be trapped
        print("There are no flowlines after stream order filtering.")
        sys.exit(FIM_exit_codes.NO_FLOWLINES_EXIST.value)  # Note: Will send a 61 back

    split_flows_gdf.to_file(split_flows_filename, driver=getDriver(split_flows_filename), index=False)

    if len(split_points_gdf) == 0:
        raise Exception("No points exist.")
    split_points_gdf.to_file(split_points_filename, driver=getDriver(split_points_filename), index=False)


    # ----
    # # Save backpool error log ## debug, TODO: Remove after the backpool issue has been well-tested
    # backpool_error_log_filename = 'branch_outlet_backpools/test_outputs/backpool_error_log.txt'

    # if isfile(backpool_error_log_filename): 
    #     remove(backpool_error_log_filename) 

    # print(backpool_error_log)

    # with open(backpool_error_log_filename, 'w') as f:
    #     for line in backpool_error_log:
    #         f.write(line)
    #         f.write('\n')



if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(description='split_flows.py')
    parser.add_argument('-f', '--flows-filename', help='flows-filename', required=True)
    parser.add_argument('-d', '--dem-filename', help='dem-filename', required=True)
    parser.add_argument('-c', '--catchment-pixels-filename', help='catchment-pixels-filename', required=True)
    parser.add_argument('-lp', '--levelpaths-filename', help='levelpaths-filename', required=True)
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename', required=True)
    parser.add_argument('-p', '--split-points-filename', help='split-points-filename', required=True)
    parser.add_argument('-w', '--wbd8-clp-filename', help='wbd8-clp-filename', required=True)
    parser.add_argument('-l', '--lakes-filename', help='lakes-filename', required=True)
    parser.add_argument('-n', '--nwm-streams-filename', help='nwm-streams-filename', required=True)
    parser.add_argument('-m', '--max-length', help='Maximum split distance (meters)', required=True)
    parser.add_argument('-t', '--slope-min', help='Minimum slope', required=True)
    parser.add_argument('-b', '--lakes-buffer-input', help='Lakes buffer distance (meters)', required=True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    args['max_length'] = float(args['max_length'])
    args['slope_min'] = float(args['slope_min'])
    args['lakes_buffer_input'] = float(args['lakes_buffer_input'])

    split_flows(**args)
