#!/usr/bin/env python3

from src.split_flows import snap_and_trim_flow


@mem_profile
def mitigate_branch_outlet_backpool(
    catchment_pixels_filename,
    split_flows_filename,
    split_points_filename,

):

    # --------------------------------------------------------------
    # Define functions

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
    def check_if_ID_is_outlet(snapped_point, outlier_catchment_ids):
        # Get the catchment ID of the snapped_point
        snapped_point['catchment_id'] = snapped_point.apply(get_raster_value, axis=1)

        # Check if values in 'catchment_id' column of the snapped point are in outlier_catchments_df
        outlet_flag = snapped_point['catchment_id'].isin(outlier_catchment_ids)
        outlet_flag = any(outlet_flag)

        return outlet_flag


    # --------------------------------------------------------------
    # Read in data (and if the files exist)

    print('Loading data ...')


    if isfile(catchment_pixels_filename):
        with rasterio.open(catchment_pixels_filename) as src:
            catchments_geom = src.read(1)
    else:
        catchments_geom = None
        print(f'No catchment pixels geometry found at {catchment_pixels_filename}.') ## debug

    # Read in split_flows_file and split_points_filename
    split_flows_geom = gpd.read_file = split_flows_filename
    split_points_geom = gpd.read_file = split_points_filename



    # --------------------------------------------------------------

    # If it's NOT branch zero, check for the two criteria and mitiate issue if needed
    if 'levpa_id' in nwm_streams.columns: # TODO: Should I be iterating through split_flows and/or split_points here?


        # Check whether catchments_geom exists
        if catchments_geom is not None:
            print('Catchment geom found, testing for backpool criteria...') ## debug

            # Check whether any pixel catchment is substantially larger than other catchments (backpool error criteria 1)
            flagged_catchment, outlier_catchment_ids = catch_catchment_size_outliers(catchments_geom)

            # If there are outlier catchments, test whether the catchment occurs at the outlet (backpool error criteria 2)
            if flagged_catchment == True:
                print('Flagged catchment(s) detected. Testing for second criteria.') 
                outlet_flag = check_if_ID_is_outlet(snapped_point, outlier_catchment_ids)
            else: 
                outlet_flag = False
                
            # If there is an outlier catchment at the outlet, set the snapped point to be the penultimate (second-to-last) vertex
            if outlet_flag == True:
                print('Incorrectly-large outlet pixel catchment detected. Snapping line to penultimate vertex.')

                ## TODO: Update the mitigation to work on the split point/split flow objects!

                # # Initialize snapped_point object
                # snapped_point = []

                # # Identify the penultimate vertex (second-to-most downstream, should be second-to-last), transform into geodataframe
                # penultimate_nwm_point = []
                # second_to_last = Point(linestring_geo.coords[-2])
                # penultimate_nwm_point.append({'ID': 'terminal', 'geometry': second_to_last})
                # snapped_point = gpd.GeoDataFrame(penultimate_nwm_point).set_crs(nwm_streams.crs)

                # # Get the catchment ID of the new snapped_point
                # snapped_point['catchment_id'] = snapped_point.apply(get_raster_value, axis=1)




    # If it IS branch zero, skip this 
    else:
        continue


    # TODO: figure out if I need to recalculate this section: "Iterate through flows and calculate channel slope, manning's n, and LengthKm for each segment"

    # --------------------------------------------------------------
    # Save the outputs
    print('Writing outputs ...')

    if isfile(split_flows_filename):
        remove(split_flows_filename)
    if isfile(split_points_filename):
        remove(split_points_filename)


    split_flows_gdf.to_file(split_flows_filename, driver=getDriver(split_flows_filename), index=False)
    split_points_gdf.to_file(split_points_filename, driver=getDriver(split_points_filename), index=False)

if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(description='mitigate_branch_outlet_backpool.py')
    parser.add_argument('-c', '--catchment-pixels-filename', help='catchment-pixels-filename', required=True)
    parser.add_argument('-s', '--split-flows-filename', help='split-flows-filename', required=True)
    parser.add_argument('-p', '--split-points-filename', help='split-points-filename', required=True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())


    mitigate_branch_outlet_backpool(**args)





