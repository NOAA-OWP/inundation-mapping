import argparse
import logging
import os
import re

import geopandas as gpd
import pandas as pd


def find_max_elevation(interpolated_flows_path, attributes_path):
    """
    Finds the maximum elevation from a flows CSV file, associates it with attributes, and updates paths.

    Parameters:
    - interpolated_flows_path (str): Path to the CSV file containing interpolated flows data.
    - attributes_path (str): Path to the CSV file containing attributes data.

    Returns:
    - tuple: A tuple containing the maximum elevation, updated path, study limit file path, levee file path,
             and a DataFrame row with additional attributes. If no valid rows or an error occurs, returns (None, None, None, None, None).
    """
    try:
        # Read the _interpolated_flows.csv file
        df_flows = pd.read_csv(interpolated_flows_path)
        # Read the attributes file
        df_attributes = pd.read_csv(attributes_path)

        # Filter rows with valid numeric "flow" and non-empty "path" string
        df_valid = df_flows[
            (pd.to_numeric(df_flows['flow'], errors='coerce').notnull())
            & (df_flows['path'].astype(str) != '')
        ]

        # Check if the dataframe is empty after filtering
        if df_valid.empty:
            logging.warning(f"No valid rows in {interpolated_flows_path}.")
            return None, None, None, None, None

        # Find the maximum "elevation" value
        df_find_max = df_valid.loc[df_valid['elevation'].idxmax()].copy()

        # Extract first instance of specific columns from df_attributes
        columns_to_extract = ['nws_lid', 'wfo', 'rfc', 'state', 'huc', 'grid_flow_source']
        for col in columns_to_extract:
            df_find_max[col] = df_attributes[col].dropna().iloc[0] if col in df_attributes.columns else None

        # Assign specific values to certain columns
        df_find_max['magnitude'] = 'maximum'  # Set the magnitude to "maximum"

        # Assign null values to certain columns
        columns_to_nullify = [
            'magnitude_stage',
            'magnitude_elev_navd88',
            'grid_name',
            'grid_stage',
            'grid_elev_navd88',
            'grid_flow_cfs',
        ]
        for col in columns_to_nullify:
            df_find_max[col] = None

        # Update the path using the new function
        updated_path = update_path(df_find_max['path'])
        if updated_path is None:
            return None, None, None, None, None

        # Update the row with the new path
        df_find_max['path'] = updated_path

        # Back out one more directory level
        base_dir = os.path.dirname(os.path.dirname(updated_path))

        # Find shapefiles in the directory
        study_limit_file, levee_file = find_shapefiles_in_directory(base_dir)

        logging.debug(f"Max elevation found in {interpolated_flows_path}: {df_find_max['elevation']}")

        # Return all relevant information including df_joined
        return (df_find_max['elevation'], updated_path, study_limit_file, levee_file, df_find_max)
    except Exception as e:
        logging.error(f"Error processing find_max_elevation function for {interpolated_flows_path}: {e}")
        return None, None, None, None, None


def update_path(original_path):
    """
    Updates the given path string by replacing certain patterns and checking if the file exists.

    Parameters:
    - original_path (str): The original file path to update.

    Returns:
    - str or None: The updated path if it exists, otherwise None.
    """
    try:
        # Replace "ahps_inundation_libraries" with "nws"
        updated_path = original_path.replace("ahps_inundation_libraries", "nws")

        # Replace "depth_grids" (case-insensitive) with "polygons"
        updated_path = re.sub(r"depth_grids", "polygons", updated_path, flags=re.IGNORECASE)

        # Replace the .tif with .shp
        updated_path = updated_path.replace(".tif", ".shp")

        # Check if the updated path file exists
        if os.path.exists(updated_path):
            logging.debug(f"File exists: {updated_path}")
            return updated_path
        else:
            updated_path = re.sub(r"_0.shp", ".shp", updated_path, flags=re.IGNORECASE)
            if os.path.exists(updated_path):
                logging.debug(f"File exists: {updated_path}")
                return updated_path
            else:
                logging.warning(f"Unexpected - File does not exist: {updated_path}")
                return None
    except Exception as e:
        logging.error(f"Error updating path {original_path}: {e}")
        return None


def find_shapefiles_in_directory(base_dir):
    """
    Search for specific shapefiles in the given directory and its subdirectories.

    Parameters:
    - base_dir (str): The base directory to start the search.

    Returns:
    - tuple: A tuple containing the paths of the 'study_limit.shp' and 'levee' shapefiles.
    """
    try:
        # Initialize variables for the search results
        study_limit_file = None
        levee_file = None

        # Search for shapefiles
        for root, dirs, files in os.walk(base_dir):
            for file in files:
                if re.search(r"study_limit\.shp$", file, re.IGNORECASE):
                    study_limit_file = os.path.join(root, file)
                elif re.search(r"levee.*\.shp$", file, re.IGNORECASE):
                    levee_file = os.path.join(root, file)

                # Break out of the loop if both files are found
                if study_limit_file and levee_file:
                    break
            if study_limit_file and levee_file:
                break

        if study_limit_file:
            logging.info(f"Found 'study_limit.shp' file: {study_limit_file}")
        else:
            logging.info(f"No 'study_limit.shp' file found in the directories under {base_dir}.")

        if levee_file:
            logging.info(f"Found 'levee' shapefile: {levee_file}")
        else:
            logging.info(f"No 'levee' shapefile found in the directories under {base_dir}.")

        return study_limit_file, levee_file
    except Exception as e:
        logging.error(f"Error searching for shapefiles in {base_dir}: {e}")
        return None, None


def check_max_elevation(csv_path, max_elevation):
    """
    Checks if the maximum elevation in a flows file is greater than the maximum elevation in an attributes file.

    Parameters:
    - csv_path (str): Path to the CSV file containing attributes data.
    - max_elevation (float): The maximum elevation value from the flows data.

    Returns:
    - bool: True if the max elevation in the flows file is greater than in the attributes file, False otherwise.
    """
    try:
        # Read the _attributes.csv file
        df_attributes = pd.read_csv(csv_path)

        # Find the maximum value in the "magnitude_elev_navd88" column
        max_magnitude_elev = df_attributes['magnitude_elev_navd88'].max()

        logging.debug(f"Max magnitude_elev_navd88 in {csv_path}: {max_magnitude_elev}")

        # Compare the elevations
        if max_elevation > max_magnitude_elev:
            logging.info(
                f"Max elevation {max_elevation} in flows file is greater than {max_magnitude_elev} in attributes file {csv_path}."
            )
            return True
        logging.info(
            f"Max elevation {max_elevation} in flows file is NOT greater than {max_magnitude_elev} in attributes file {csv_path}."
        )
        return False
    except Exception as e:
        logging.error(f"Error processing check_max_elevation function for {csv_path}: {e}")
        return False


def find_flood_magnitude_shapes(attributes_path, interpolated_flows_path):
    """
    Finds flood magnitude polygon shapefiles by joining attributes and flows data, and updates paths.

    Parameters:
    - attributes_path (str): Path to the CSV file containing attributes data.
    - interpolated_flows_path (str): Path to the CSV file containing interpolated flows data.

    Returns:
    - list of dict: A list of dictionaries containing flood magnitude data including updated paths and shapefiles.
                    Returns an empty list if no valid data is found or an error occurs.
    """
    try:
        # Read the attributes file
        df_attributes = pd.read_csv(attributes_path)
        df_flows = pd.read_csv(interpolated_flows_path)

        # Log the types of columns found
        logging.debug(f"Columns in {attributes_path}: {df_attributes.columns.tolist()}")

        # Check if the required columns exist
        if 'magnitude' not in df_attributes.columns or 'grid_name' not in df_attributes.columns:
            logging.error(f"Required columns 'magnitude' or 'grid_name' not found in {attributes_path}.")
            return {}

        if 'name' not in df_flows.columns:
            logging.error(f"Required column 'name' not found in {interpolated_flows_path}.")
            return {}

        # Filter rows with non-null and non-empty 'magnitude' values
        df_valid = df_attributes[
            df_attributes['magnitude'].notnull() & (df_attributes['magnitude'].astype(str).str.strip() != '')
        ]
        df_valid = df_attributes[
            df_attributes['grid_name'].notnull() & (df_attributes['grid_name'].astype(str).str.strip() != '')
        ]

        # Remove all ".tif" extensions from the 'grid_name' column
        df_valid['grid_name'] = df_valid['grid_name'].str.replace('.tif', '', regex=False)

        if df_valid.empty:
            logging.warning(f"No valid entries found in the 'magnitude' column of {attributes_path}.")
            return {}

        # Join df_valid and df_flows using 'grid_name' from df_valid and 'name' from df_flows
        df_joined = df_valid.merge(df_flows, left_on='grid_name', right_on='name', how='inner')

        if df_joined.empty:
            logging.warning(f"No matching entries found between attributes and flows data: {attributes_path}")
            return {}

        # Prepare a list to store the results
        results = []

        # Prepare the dictionary with 'magnitude' and grid paths
        flood_cat_paths = {}
        for _, row in df_joined.iterrows():
            magnitude = row['magnitude']
            path = row['path']  # Assuming path is the desired column to return

            # Store in dictionary with magnitude as key and path as value
            flood_cat_paths[magnitude] = path

            # Update the path using the new function
            updated_path = update_path(path)
            if updated_path is None:
                return None, None, None, None, None

            # Update the row with the new path
            row['path'] = updated_path

            # Back out one more directory level
            base_dir = os.path.dirname(os.path.dirname(updated_path))

            # Find shapefiles in the directory
            study_limit_file, levee_file = find_shapefiles_in_directory(base_dir)

            # Append all relevant information to the results list
            results.append(
                {
                    'magnitude': magnitude,
                    'elevation': row['elevation'],
                    'path': updated_path,
                    'study_limit_file': study_limit_file,
                    'levee_file': levee_file,
                    'attributes': row,
                }
            )

            logging.debug(f"Flood category map {magnitude} found in {attributes_path}: {row['elevation']}")
            print(f"Flood category map {magnitude} found in {attributes_path}: {row['elevation']}")

        return results

    except Exception as e:
        logging.error(f"Error processing attributes file {attributes_path}: {e}")
        return []


def process_shapefile_to_parquet_with_sampling(
    shapefile_path, study_limit_file, levee_file, point_attributes, output_dir, step=5
):
    """
    Processes a shapefile to extract edge vertices with sampling, applies buffer operations using other shapefiles,
    and converts the output to a Parquet file.

    Parameters:
    - shapefile_path (str): Path to the input shapefile containing polygon or multipolygon geometries.
    - study_limit_file (str): Path to the study limit shapefile, which will be used for buffering.
    - levee_file (str): Path to the levee shapefile, which will also be used for buffering.
    - point_attributes (dict): Dictionary containing point attributes like 'nws_lid', 'flow', 'magnitude', etc.,
                               to be added to each extracted point.
    - output_dir (str): Directory where the output files (Parquet and optional buffer polygons) will be saved.
    - step (int, optional): Sampling step for extracting vertices from polygon edges. Defaults to 5.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame containing sampled points with associated attributes.
                            Returns None if an error occurs.

    The function performs the following steps:
    1. Reads the input shapefile using GeoPandas.
    2. Logs and checks the types of geometries present in the shapefile.
    3. If the 'nws_lid' is 'tarn7', it dissolves multipolygons into single polygons.
    4. Extracts edge vertices from polygons and multipolygons using a specified sampling step.
    5. Creates a GeoDataFrame of the sampled points, transforms its CRS to EPSG:5070, and attaches point attributes.
    6. Buffers geometries from the provided study limit and levee shapefiles and removes points within these buffer areas.
    7. Returns the GeoDataFrame of sampled points with attached attributes.

    Any errors encountered during processing are logged.
    """
    try:
        try:
            gdf = gpd.read_file(shapefile_path)
        except Exception as e:
            logging.error(f"Failed to read shapefile {shapefile_path}: {e}")
            return

        # Log the types of geometries found
        logging.debug(f"Geometries found in {shapefile_path}: {gdf.geometry.type.unique()}")

        # Check if the shapefile contains valid polygons or multipolygons
        if gdf.empty or not any(gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])):
            logging.warning(f"No valid polygons found in {shapefile_path}.")
            return

        # Dissolve MultiPolygons into single Polygons if nws_lid is "tarn7"
        if point_attributes.get('nws_lid') == 'tarn7':
            if 'MultiPolygon' in gdf.geometry.geom_type.unique():
                # Unify all geometries into a single geometry and then break into polygons
                unified_geom = gdf.geometry.unary_union
                gdf = gpd.GeoDataFrame(geometry=[unified_geom], crs=gdf.crs)
                gdf = gdf.explode(index_parts=True)

        # Extract edge vertices with sampling
        all_points = []
        for geom in gdf.geometry:
            if geom.geom_type == 'Polygon':
                coords = list(geom.exterior.coords)
                sampled_coords = coords[::step]  # Take every `step`th point
                all_points.extend(sampled_coords)
            elif geom.geom_type == 'MultiPolygon':
                # In case any MultiPolygon is left, iterate over each Polygon
                for poly in geom.geoms:
                    coords = list(poly.exterior.coords)
                    sampled_coords = coords[::step]  # Take every `step`th point
                    all_points.extend(sampled_coords)
            else:
                logging.warning(f"Geometry type {geom.geom_type} is not handled in {shapefile_path}.")

        if not all_points:
            logging.warning(f"No points extracted from {shapefile_path}.")
            return

        # Create a GeoDataFrame of the points with EPSG:5070 CRS
        gdf_points = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy([p[0] for p in all_points], [p[1] for p in all_points]), crs=gdf.crs
        )

        # Transform the CRS of points to EPSG:5070
        gdf_points = gdf_points.to_crs(epsg=5070)

        # Convert "flow" from cubic feet to cubic meters
        point_attributes['flow'] = point_attributes['flow'] * 0.0283168
        point_attributes['flow_unit'] = 'cms'  # Set the magnitude to "maximum"

        # Rename 'huc' to 'HUC8' if it exists
        if 'huc' in point_attributes:
            point_attributes['HUC8'] = point_attributes.pop('huc')

        # Define the list of attributes to keep
        attributes_to_keep = [
            'nws_lid',
            'magnitude',
            'path',
            'name',
            'elevation',
            'stage',
            'flow',
            'flow_unit',
            'flow_source',
            'wfo',
            'HUC8',
        ]

        # Filter point_attributes to include only the desired attributes
        filtered_attributes = {
            attr: value for attr, value in point_attributes.items() if attr in attributes_to_keep
        }

        # Add point attributes to each point in gdf_points
        for attr, value in filtered_attributes.items():
            gdf_points[attr] = value

        # Initialize an empty GeoDataFrame for buffer polygons with proper CRS
        buffer_polygons = gpd.GeoDataFrame(geometry=[], crs=gdf.crs)

        # Read and buffer study limit and levee files if they are provided
        for line_shapefile in [study_limit_file, levee_file]:
            if line_shapefile:
                gdf_lines = gpd.read_file(line_shapefile)

                # Log the types of geometries found
                logging.debug(f"Geometries found in {line_shapefile}: {gdf_lines.geometry.type.unique()}")

                # Check if the shapefile contains valid LineString or MultiLineString
                if not any(gdf_lines.geometry.type.isin(['LineString', 'MultiLineString'])):
                    logging.warning(f"No valid LineString or MultiLineString found in {line_shapefile}.")
                    continue

                # Buffer the lines and concatenate to buffer_polygons
                buffered_lines = gdf_lines.buffer(20)  # Buffer by 20 meters
                if buffered_lines.empty:
                    logging.warning(f"No valid buffered polygons created from {line_shapefile}.")
                    continue

                # Create a new GeoDataFrame for buffered lines
                gdf_buffered_lines = gpd.GeoDataFrame(geometry=buffered_lines, crs=gdf.crs)

                # Concatenate buffered lines to buffer_polygons
                buffer_polygons = pd.concat([buffer_polygons, gdf_buffered_lines], ignore_index=True)

        if buffer_polygons.empty:
            logging.info("No buffer polygons created from line shapefiles.")
        else:
            # Ensure buffer_polygons has valid geometry
            buffer_polygons = gpd.GeoDataFrame(geometry=buffer_polygons.geometry, crs=gdf.crs)

            # Remove points within the buffer polygons
            if not buffer_polygons.geometry.is_empty.any():
                gdf_points = gdf_points[~gdf_points.geometry.within(buffer_polygons.unary_union)]

            # Export buffer polygons to GeoPackage
            # buffer_gpkg_path = os.path.join(output_dir, 'buffer_polygons.gpkg')
            # buffer_polygons.to_file(buffer_gpkg_path, driver="GPKG")
            # logging.info(f"Buffer polygons saved to {buffer_gpkg_path}")

        # Define the output Parquet file path
        # shapefile_filename = os.path.splitext(os.path.basename(shapefile_path))[0]
        # parquet_path = os.path.join(output_dir, f"{shapefile_filename}_vertices.parquet")

        # Export points to Parquet
        # gdf_points.to_parquet(parquet_path)
        # logging.info(f"Points from {shapefile_path} saved to {parquet_path}")

        # Return the list
        return gdf_points

    except Exception as e:
        logging.error(f"Error processing shapefile {shapefile_path}: {e}")


def process_directory(root_dir, output_dir, log_file):
    logging.basicConfig(
        filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info(f"Starting process in directory: {root_dir}")
    accumulated_data = []  # List to accumulate nws_lid and magnitude data
    # Loop through each first-level directory under root_dir
    for huc_dir in reversed(sorted(os.listdir(root_dir))):
        print(huc_dir)
        first_level_path = os.path.join(root_dir, huc_dir)
        if os.path.isdir(first_level_path):
            all_points = gpd.GeoDataFrame()  # Initialize an empty GeoDataFrame to accumulate points
            # Now walk through each subdirectory within the first-level directory
            for subdir, dirs, files in os.walk(first_level_path):
                for file in files:
                    if file.endswith('_interpolated_flows.csv'):
                        interpolated_flows_path = os.path.join(subdir, file)
                        print(f"Processing {interpolated_flows_path}.")
                        logging.debug(f"Processing {interpolated_flows_path}.")

                        # Find the corresponding _attributes.csv file
                        attributes_file = file.replace('_interpolated_flows.csv', '_attributes.csv')
                        attributes_path = os.path.join(subdir, attributes_file)

                        if not os.path.exists(attributes_path):
                            logging.warning(
                                f"Attributes file {attributes_file} not found for {file} in {subdir}."
                            )
                        else:
                            max_elevation, path_value, study_limit_file, levee_file, point_attributes = (
                                find_max_elevation(interpolated_flows_path, attributes_path)
                            )

                            if max_elevation is None:
                                logging.warning(
                                    f"Skipping {interpolated_flows_path} due to no valid max elevation."
                                )
                                continue
                            if check_max_elevation(attributes_path, max_elevation):
                                print(
                                    f"Path with max elevation greater than magnitude_elev_navd88: {path_value}"
                                )

                                # Process the shapefile and collect points
                                gdf_points = process_shapefile_to_parquet_with_sampling(
                                    path_value,
                                    study_limit_file,
                                    levee_file,
                                    point_attributes,
                                    output_dir,
                                    step=10,
                                )

                                if gdf_points is not None:
                                    # Check and align CRS before concatenation
                                    if all_points.empty:
                                        all_points = gdf_points
                                    else:
                                        gdf_points = gdf_points.to_crs(all_points.crs)
                                    print('successfully created points for:')
                                    print(
                                        point_attributes['nws_lid'] + ' --> ' + point_attributes['magnitude']
                                    )
                                    # Append the collected points to all_points GeoDataFrame
                                    all_points = pd.concat([all_points, gdf_points], ignore_index=True)
                                    # Accumulate nws_lid and magnitude for CSV export
                                    accumulated_data.append(
                                        {
                                            'nws_lid': point_attributes['nws_lid'],
                                            'HUC8': point_attributes['HUC8'],
                                            'magnitude': point_attributes['magnitude'],
                                        }
                                    )
                            else:
                                logging.info(
                                    f"Max elevation in {interpolated_flows_path} is not greater than max magnitude_elev_navd88 in {attributes_file}."
                                )

                            # Find flood magnitude shapes and process them
                            print('Checking for flood category data files...')
                            flood_cat_paths = find_flood_magnitude_shapes(
                                attributes_path, interpolated_flows_path
                            )
                            if flood_cat_paths:
                                print('Processing flood category data files...')
                                for flood_data in flood_cat_paths:
                                    # Check if flood_data is None
                                    if flood_data is None:
                                        logging.error(f"Flood data could not be loaded for {attributes_path}")
                                        continue  # Skip processing this shapefile
                                    path_value = flood_data['path']
                                    study_limit_file = flood_data['study_limit_file']
                                    levee_file = flood_data['levee_file']
                                    point_attributes = flood_data[
                                        'attributes'
                                    ]  # Use the full dictionary as attributes if needed

                                    # Process the shapefile and collect points
                                    gdf_points = process_shapefile_to_parquet_with_sampling(
                                        path_value,
                                        study_limit_file,
                                        levee_file,
                                        point_attributes,
                                        output_dir,
                                        step=10,
                                    )

                                    if gdf_points is not None:
                                        # Check and align CRS before concatenation
                                        if all_points.empty:
                                            all_points = gdf_points
                                        else:
                                            gdf_points = gdf_points.to_crs(all_points.crs)
                                        # Append the collected points to all_points GeoDataFrame
                                        print('successfully created points for:')
                                        print(
                                            point_attributes['nws_lid']
                                            + ' --> '
                                            + point_attributes['magnitude']
                                        )
                                        all_points = pd.concat([all_points, gdf_points], ignore_index=True)
                                        # Accumulate nws_lid and magnitude for CSV export
                                        accumulated_data.append(
                                            {
                                                'nws_lid': point_attributes['nws_lid'],
                                                'HUC8': point_attributes['HUC8'],
                                                'magnitude': point_attributes['magnitude'],
                                            }
                                        )
                            else:
                                logging.info(
                                    f"Did not find any flood category maps to process in {attributes_file} or could not find corresponding data in {attributes_file}."
                                )

            if not all_points.empty:
                # Define the output Parquet file path using the 8-digit subdirectory name
                parquet_path = os.path.join(output_dir, f"{huc_dir}.parquet")

                # Export the accumulated points to a Parquet file
                all_points.to_parquet(parquet_path)
                print(parquet_path)
                logging.info(f"Points from all files in subdirectory {subdir} saved to {parquet_path}")

    # After processing all directories, group the accumulated data by nws_lid and export to CSV
    if accumulated_data:
        df_accumulated = pd.DataFrame(accumulated_data)
        # Group by nws_lid and concatenate HUC8 and magnitude values
        grouped_df = (
            df_accumulated.groupby('nws_lid')
            .agg(
                {
                    'HUC8': lambda x: ', '.join(map(str, x.unique())),
                    'magnitude': lambda x: ', '.join(map(str, x)),
                }
            )
            .reset_index()
        )

        # Define the CSV output path (e.g., in the output_dir)
        csv_output_path = os.path.join(output_dir, 'nws_lid_magnitudes_summary.csv')
        grouped_df.to_csv(csv_output_path, index=False)
        logging.info(f"Summary CSV saved to {csv_output_path}")

    logging.info("Processing complete.")
    logging.shutdown()  # Ensure all logs are flushed and the log file is properly closed
    print('Script completed!!')


if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Process directories for flood data.')
    parser.add_argument(
        '-i',
        '--root_directory_path',
        type=str,
        help='Path to the root directory containing the HUC directories.',
    )
    parser.add_argument(
        '-o',
        '--output_directory_path',
        type=str,
        help='Path to the directory where output files will be saved.',
    )

    args = parser.parse_args()
    root_directory_path = args.root_directory_path
    output_directory_path = args.output_directory_path
    log_file = os.path.join(root_directory_path, "processing_log.log")

    # root_directory_path = r'B:\FIM_development\fim_assessment\FOSS\ahps_benchmark\5_5_2024\nws'
    # output_directory_path = r'C:\GID\FOSS_FIM\ahps_max_stage_flow_preprocess\calibration_points\5_5_2024'
    process_directory(root_directory_path, output_directory_path, log_file)
